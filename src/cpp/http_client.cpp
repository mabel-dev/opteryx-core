/**
 * Pure C++ HTTP client implementation.
 *
 * No Python.h. No PyObject. No PyErr_SetString.
 * Errors are std::runtime_error. The Cython layer translates to Python.
 *
 * Thread-safety:
 *   get() / head()  — curl_easy_perform(), each call owns its CURL* easy handle.
 *                     CURLSH provides shared connection/DNS cache with mutex.
 *   get_many()      — local CURLM* per call, all N transfers on calling thread.
 *                     CURLOPT_SHARE set so warm connections from get() are reused.
 */

#include "http_client.hpp"

#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <random>
#include <sstream>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

namespace {

// ── WP-5 retry / per-request-timeout configuration (env, read once) ──────────
long http_timeout_floor_ms() {
    static long v = []() {
        const char* e = std::getenv("OPTERYX_HTTP_TIMEOUT_FLOOR_MS");
        return e ? std::atol(e) : 10000L;   // 10s floor
    }();
    return v;
}
double http_min_bw_bytes_per_s() {
    static double v = []() {
        const char* e = std::getenv("OPTERYX_HTTP_MIN_BW_MBPS");
        double mbps = e ? std::atof(e) : 20.0;   // assume ≥20 Mbps/stream
        return mbps * 1.0e6 / 8.0;
    }();
    return v;
}
int http_max_retries() {
    static int v = []() {
        const char* e = std::getenv("OPTERYX_HTTP_MAX_RETRIES");
        return e ? std::atoi(e) : 2;
    }();
    return v;
}

std::atomic<uint64_t> g_http_retries{0};

// A CURL transport result is retryable if it is a transient connection/timeout/
// partial-transfer condition (vs a definitive protocol/SSL error).
bool curl_result_retryable(CURLcode c) {
    switch (c) {
        case CURLE_OPERATION_TIMEDOUT:
        case CURLE_COULDNT_CONNECT:
        case CURLE_COULDNT_RESOLVE_HOST:
        case CURLE_GOT_NOTHING:
        case CURLE_RECV_ERROR:
        case CURLE_SEND_ERROR:
        case CURLE_PARTIAL_FILE:
            return true;
        default:
            return false;
    }
}

// HTTP status is retryable for 5xx (server-side transient) and 429 (rate limit);
// 4xx (except 429) is a hard client error — never retried.
bool http_status_retryable(long code) {
    return code >= 500 || code == 429;
}

// Derive a per-request timeout from the Range header's byte span: a small chunk
// that stalls should time out in ~floor seconds, not the 60s client default, so
// it can be retried promptly. Returns the client default when no Range present.
long request_timeout_ms(const std::map<std::string, std::string>& headers, long fallback_ms) {
    auto it = headers.find("Range");
    if (it == headers.end()) return fallback_ms;
    // "bytes=START-END"
    const std::string& r = it->second;
    size_t eq = r.find('=');
    size_t dash = r.find('-', eq == std::string::npos ? 0 : eq + 1);
    if (eq == std::string::npos || dash == std::string::npos) return fallback_ms;
    long start = std::atol(r.c_str() + eq + 1);
    long end   = std::atol(r.c_str() + dash + 1);
    long size  = (end >= start) ? (end - start + 1) : 0;
    double bw  = http_min_bw_bytes_per_s();
    long derived = bw > 0 ? static_cast<long>(size * 1000.0 / bw) : fallback_ms;
    long floor   = http_timeout_floor_ms();
    return std::max(floor, derived);
}

// Backoff with full jitter: random in [0, base * 2^attempt], capped.
unsigned backoff_ms(int attempt) {
    static thread_local std::mt19937 rng{std::random_device{}()};
    unsigned base = 50u << std::min(attempt, 6);    // 50,100,200,... capped
    if (base > 2000u) base = 2000u;
    std::uniform_int_distribution<unsigned> d(0, base);
    return d(rng);
}

}  // namespace

uint64_t HttpClient::total_retries() {
    return g_http_retries.load(std::memory_order_relaxed);
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

namespace {

struct ResponseBuffer {
    std::vector<uint8_t> body;
    std::string          headers_raw;

    static size_t write_body(void* ptr, size_t size, size_t nmemb, void* userp) {
        size_t bytes = size * nmemb;
        auto* buf = static_cast<ResponseBuffer*>(userp);
        const uint8_t* src = static_cast<const uint8_t*>(ptr);
        buf->body.insert(buf->body.end(), src, src + bytes);
        return bytes;
    }

    static size_t write_headers(char* ptr, size_t size, size_t nmemb, void* userp) {
        size_t bytes = size * nmemb;
        auto* buf = static_cast<ResponseBuffer*>(userp);
        buf->headers_raw.append(ptr, bytes);
        return bytes;
    }
};

/** Parse raw HTTP header block into a map. Keys are lowercased. */
std::map<std::string, std::string> parse_headers(const std::string& raw) {
    std::map<std::string, std::string> result;
    std::istringstream stream(raw);
    std::string line;

    while (std::getline(stream, line)) {
        // Strip \r
        if (!line.empty() && line.back() == '\r') line.pop_back();

        // Skip status line and blank lines
        size_t colon = line.find(':');
        if (colon == std::string::npos || colon == 0) continue;

        std::string key = line.substr(0, colon);
        std::string val = line.substr(colon + 1);

        // Lowercase key
        std::transform(key.begin(), key.end(), key.begin(), ::tolower);

        // Trim leading space from value
        size_t start = val.find_first_not_of(" \t");
        if (start != std::string::npos) val = val.substr(start);

        result[key] = val;
    }
    return result;
}

} // namespace

// ---------------------------------------------------------------------------
// CURLSH lock/unlock callbacks
// ---------------------------------------------------------------------------

void HttpClient::_share_lock(CURL*, curl_lock_data data, curl_lock_access, void* userp) {
    // Index by data type so libcurl can independently lock DNS while holding
    // a CONNECT lock. A single mutex would deadlock when libcurl acquires
    // two data types on the same thread without releasing between them.
    static_cast<HttpClient*>(userp)->share_mutexes_[data].lock();
}

void HttpClient::_share_unlock(CURL*, curl_lock_data data, void* userp) {
    static_cast<HttpClient*>(userp)->share_mutexes_[data].unlock();
}

// ---------------------------------------------------------------------------
// HttpClient
// ---------------------------------------------------------------------------

std::string HttpClient::find_ca_bundle() {
    // Check SSL_CERT_FILE env var first (user/container override)
    const char* env = ::getenv("SSL_CERT_FILE");
    if (env && ::access(env, R_OK) == 0) return std::string(env);

    // Common CA bundle paths, in order of likelihood
    static const char* candidates[] = {
        "/etc/ssl/certs/ca-certificates.crt",                   // Debian/Ubuntu
        "/etc/pki/tls/certs/ca-bundle.crt",                     // RHEL/CentOS/Amazon Linux
        "/etc/pki/ca-trust/extracted/pem/tls-ca-bundle.pem",    // RHEL 7+
        "/etc/ssl/ca-bundle.pem",                               // openSUSE
        "/etc/ssl/cert.pem",                                    // Alpine, macOS
        "/usr/local/etc/openssl@3/cert.pem",                    // macOS Homebrew OpenSSL 3
        "/usr/local/etc/openssl/cert.pem",                      // macOS Homebrew OpenSSL
        nullptr
    };

    for (int i = 0; candidates[i]; ++i) {
        if (::access(candidates[i], R_OK) == 0) {
            return std::string(candidates[i]);
        }
    }
    return "";
}

void HttpClient::configure_ssl(void* easy_handle) const {
    CURL* curl = static_cast<CURL*>(easy_handle);
    if (!ca_bundle_.empty()) {
        curl_easy_setopt(curl, CURLOPT_CAINFO, ca_bundle_.c_str());
    }
    // Always verify peer and host — never silently disable SSL
    curl_easy_setopt(curl, CURLOPT_SSL_VERIFYPEER, 1L);
    curl_easy_setopt(curl, CURLOPT_SSL_VERIFYHOST, 2L);
}

void HttpClient::configure_share(void* easy_handle) const {
    curl_easy_setopt(
        static_cast<CURL*>(easy_handle),
        CURLOPT_SHARE,
        static_cast<CURLSH*>(share_handle_)
    );
}

HttpClient::HttpClient(int max_connections, long timeout_ms)
    : share_handle_(nullptr),
      max_connections_(max_connections),
      timeout_ms_(timeout_ms),
      user_agent_("opteryx/1.0"),
      ca_bundle_(find_ca_bundle()) {

    CURLSH* share = curl_share_init();
    if (!share) throw std::runtime_error("curl_share_init() failed");

    // Share connection cache and DNS across threads.
    // The lock/unlock callbacks serialize access — libcurl calls them
    // whenever it needs to touch shared data structures.
    curl_share_setopt(share, CURLSHOPT_SHARE,     CURL_LOCK_DATA_CONNECT);
    curl_share_setopt(share, CURLSHOPT_SHARE,     CURL_LOCK_DATA_DNS);
    curl_share_setopt(share, CURLSHOPT_LOCKFUNC,  &HttpClient::_share_lock);
    curl_share_setopt(share, CURLSHOPT_UNLOCKFUNC,&HttpClient::_share_unlock);
    curl_share_setopt(share, CURLSHOPT_USERDATA,  this);

    share_handle_ = share;
}

HttpClient::~HttpClient() {
    if (share_handle_) {
        curl_share_cleanup(static_cast<CURLSH*>(share_handle_));
        share_handle_ = nullptr;
    }
}

// ---------------------------------------------------------------------------
// get() — thread-safe single GET via curl_easy_perform()
// ---------------------------------------------------------------------------

std::vector<uint8_t> HttpClient::get(
    const std::string& url,
    const std::map<std::string, std::string>& headers)
{
    CURL* easy = curl_easy_init();
    if (!easy) throw std::runtime_error("curl_easy_init() failed");

    ResponseBuffer buf;

    curl_easy_setopt(easy, CURLOPT_URL,            url.c_str());
    curl_easy_setopt(easy, CURLOPT_USERAGENT,       user_agent_.c_str());
    curl_easy_setopt(easy, CURLOPT_TIMEOUT_MS,      timeout_ms_);
    curl_easy_setopt(easy, CURLOPT_FOLLOWLOCATION,  1L);
    curl_easy_setopt(easy, CURLOPT_MAXREDIRS,       5L);
    curl_easy_setopt(easy, CURLOPT_WRITEFUNCTION,   ResponseBuffer::write_body);
    curl_easy_setopt(easy, CURLOPT_WRITEDATA,       &buf);
    configure_ssl(easy);
    configure_share(easy);

    // Build custom header list
    struct curl_slist* hlist = nullptr;
    for (const auto& kv : headers) {
        std::string line = kv.first + ": " + kv.second;
        hlist = curl_slist_append(hlist, line.c_str());
    }
    if (hlist) curl_easy_setopt(easy, CURLOPT_HTTPHEADER, hlist);

    CURLcode res = curl_easy_perform(easy);

    curl_slist_free_all(hlist);

    if (res != CURLE_OK) {
        curl_easy_cleanup(easy);
        throw std::runtime_error(
            std::string("CURL error: ") + curl_easy_strerror(res));
    }

    long http_code = 0;
    curl_easy_getinfo(easy, CURLINFO_RESPONSE_CODE, &http_code);
    curl_easy_cleanup(easy);

    if (http_code >= 400) {
        throw std::runtime_error(
            std::string("HTTP ") + std::to_string(http_code) + ": " + url);
    }

    return buf.body;
}

// ---------------------------------------------------------------------------
// head() — thread-safe HEAD via curl_easy_perform()
// ---------------------------------------------------------------------------

std::map<std::string, std::string> HttpClient::head(
    const std::string& url,
    const std::map<std::string, std::string>& headers)
{
    CURL* easy = curl_easy_init();
    if (!easy) throw std::runtime_error("curl_easy_init() failed");

    ResponseBuffer buf;

    curl_easy_setopt(easy, CURLOPT_URL,            url.c_str());
    curl_easy_setopt(easy, CURLOPT_USERAGENT,       user_agent_.c_str());
    curl_easy_setopt(easy, CURLOPT_TIMEOUT_MS,      timeout_ms_);
    curl_easy_setopt(easy, CURLOPT_NOBODY,          1L);  // HEAD
    curl_easy_setopt(easy, CURLOPT_FOLLOWLOCATION,  1L);
    curl_easy_setopt(easy, CURLOPT_MAXREDIRS,       5L);
    curl_easy_setopt(easy, CURLOPT_HEADERFUNCTION,  ResponseBuffer::write_headers);
    curl_easy_setopt(easy, CURLOPT_HEADERDATA,      &buf);
    configure_ssl(easy);
    configure_share(easy);

    // Build custom header list
    struct curl_slist* hlist = nullptr;
    for (const auto& kv : headers) {
        std::string line = kv.first + ": " + kv.second;
        hlist = curl_slist_append(hlist, line.c_str());
    }
    if (hlist) curl_easy_setopt(easy, CURLOPT_HTTPHEADER, hlist);

    CURLcode res = curl_easy_perform(easy);

    long http_code = 0;
    curl_easy_getinfo(easy, CURLINFO_RESPONSE_CODE, &http_code);
    curl_slist_free_all(hlist);
    curl_easy_cleanup(easy);

    if (res != CURLE_OK) {
        throw std::runtime_error(
            std::string("CURL error: ") + curl_easy_strerror(res));
    }
    if (http_code >= 400) {
        throw std::runtime_error(
            std::string("HTTP ") + std::to_string(http_code) + ": " + url);
    }

    return parse_headers(buf.headers_raw);
}

// ---------------------------------------------------------------------------
// get_many() — concurrent batch GET via a local CURLM event loop
//
// Design:
//   - Creates a local CURLM* for this call only; never shared across threads.
//   - All N easy handles are added at once; CURLM multiplexes them concurrently.
//   - Sets CURLOPT_SHARE on every easy handle so warm connections from
//     previous get() / get_many() calls can be reused.
//   - Results are returned in the same order as the input requests vector.
//   - On any error (CURL, HTTP 4xx/5xx), throws std::runtime_error and cleans up.
// ---------------------------------------------------------------------------

std::vector<std::vector<uint8_t>> HttpClient::get_many(
    const std::vector<std::pair<std::string, std::map<std::string, std::string>>>& requests)
{
    const size_t n = requests.size();
    if (n == 0) return {};

    // Per-request context: response buffer + last curl result/status.
    struct RequestCtx {
        ResponseBuffer buf;
        CURLcode       res      = CURLE_OK;
        long           http_code = 0;
    };
    std::vector<RequestCtx> ctx(n);

    // Fetch a subset of requests (by index) concurrently via one local CURLM,
    // harvesting result + status into ctx[idx]. Resets each buffer first so a
    // retry does not append to a partial body. Throws only on CURLM-setup
    // failures (not per-request transport errors — those land in ctx).
    auto fetch = [&](const std::vector<size_t>& idxs) {
        std::vector<CURL*>       handles(idxs.size(), nullptr);
        std::vector<curl_slist*> hlists(idxs.size(), nullptr);

        CURLM* multi = curl_multi_init();
        if (!multi) throw std::runtime_error("get_many: curl_multi_init() failed");
        curl_multi_setopt(multi, CURLMOPT_MAX_HOST_CONNECTIONS, (long)max_connections_);
        curl_multi_setopt(multi, CURLMOPT_MAXCONNECTS,          (long)(max_connections_ * 2));

        auto cleanup = [&]() {
            for (size_t j = 0; j < idxs.size(); ++j) {
                if (handles[j]) {
                    curl_multi_remove_handle(multi, handles[j]);
                    curl_easy_cleanup(handles[j]);
                    handles[j] = nullptr;
                }
                if (hlists[j]) { curl_slist_free_all(hlists[j]); hlists[j] = nullptr; }
            }
            curl_multi_cleanup(multi);
        };

        for (size_t j = 0; j < idxs.size(); ++j) {
            const size_t i = idxs[j];
            ctx[i].buf.body.clear();   // fresh body for this (re)attempt
            const auto& req_url  = requests[i].first;
            const auto& req_hdrs = requests[i].second;

            CURL* easy = curl_easy_init();
            if (!easy) { cleanup(); throw std::runtime_error("get_many: curl_easy_init() failed"); }
            handles[j] = easy;

            curl_easy_setopt(easy, CURLOPT_URL,            req_url.c_str());
            curl_easy_setopt(easy, CURLOPT_USERAGENT,       user_agent_.c_str());
            // Per-request timeout derived from the Range size (WP-5): a stalled
            // small request times out near the floor, not the 60s client default.
            curl_easy_setopt(easy, CURLOPT_TIMEOUT_MS,      request_timeout_ms(req_hdrs, timeout_ms_));
            curl_easy_setopt(easy, CURLOPT_FOLLOWLOCATION,  1L);
            curl_easy_setopt(easy, CURLOPT_MAXREDIRS,       5L);
            curl_easy_setopt(easy, CURLOPT_WRITEFUNCTION,   ResponseBuffer::write_body);
            curl_easy_setopt(easy, CURLOPT_WRITEDATA,       &ctx[i].buf);
            curl_easy_setopt(easy, CURLOPT_PRIVATE,         reinterpret_cast<void*>(i));
            configure_ssl(easy);
            // No CURLOPT_SHARE: the local CURLM reuses connections within the
            // batch; CURLSH from multi+other-thread-get() could deadlock.

            for (const auto& kv : req_hdrs) {
                std::string line = kv.first + ": " + kv.second;
                hlists[j] = curl_slist_append(hlists[j], line.c_str());
            }
            if (hlists[j]) curl_easy_setopt(easy, CURLOPT_HTTPHEADER, hlists[j]);

            CURLMcode mc = curl_multi_add_handle(multi, easy);
            if (mc != CURLM_OK) {
                cleanup();
                throw std::runtime_error(
                    std::string("get_many: curl_multi_add_handle: ") + curl_multi_strerror(mc));
            }
        }

        int running = static_cast<int>(idxs.size());
        while (running > 0) {
            CURLMcode mc = curl_multi_perform(multi, &running);
            if (mc != CURLM_OK) {
                cleanup();
                throw std::runtime_error(
                    std::string("get_many: curl_multi_perform: ") + curl_multi_strerror(mc));
            }
            if (running > 0) curl_multi_wait(multi, nullptr, 0, 100, nullptr);
        }

        int msgs_left = 0;
        CURLMsg* msg;
        while ((msg = curl_multi_info_read(multi, &msgs_left)) != nullptr) {
            if (msg->msg == CURLMSG_DONE) {
                void* priv = nullptr;
                curl_easy_getinfo(msg->easy_handle, CURLINFO_PRIVATE, &priv);
                size_t i = reinterpret_cast<size_t>(priv);
                ctx[i].res = msg->data.result;
                ctx[i].http_code = 0;
                curl_easy_getinfo(msg->easy_handle, CURLINFO_RESPONSE_CODE, &ctx[i].http_code);
            }
        }
        cleanup();
    };

    // First attempt: all requests. Then retry only the transient failures.
    std::vector<size_t> pending(n);
    for (size_t i = 0; i < n; ++i) pending[i] = i;

    const int max_retries = http_max_retries();
    for (int attempt = 0; ; ++attempt) {
        fetch(pending);

        std::vector<size_t> retry_next;
        for (size_t i : pending) {
            bool curl_ok = (ctx[i].res == CURLE_OK);
            bool http_ok = curl_ok && ctx[i].http_code < 400;
            if (http_ok) continue;

            bool retryable = curl_ok ? http_status_retryable(ctx[i].http_code)
                                     : curl_result_retryable(ctx[i].res);
            if (!retryable) {
                // Hard failure (4xx or definitive transport error) — fail now.
                if (!curl_ok)
                    throw HttpError(std::string("get_many: CURL error: ") +
                        curl_easy_strerror(ctx[i].res) + " url=" + requests[i].first,
                        false, 0);
                throw HttpError(std::string("get_many: HTTP ") +
                    std::to_string(ctx[i].http_code) + ": " + requests[i].first,
                    false, ctx[i].http_code);
            }
            retry_next.push_back(i);
        }

        if (retry_next.empty()) break;   // everything succeeded

        if (attempt >= max_retries) {
            size_t i = retry_next.front();
            std::string cause = (ctx[i].res != CURLE_OK)
                ? std::string("CURL error: ") + curl_easy_strerror(ctx[i].res)
                : std::string("HTTP ") + std::to_string(ctx[i].http_code);
            throw HttpError(
                "get_many: exhausted " + std::to_string(max_retries) + " retries (" +
                cause + ") url=" + requests[i].first + " range=" +
                [&]() { auto it = requests[i].second.find("Range");
                        return it == requests[i].second.end() ? std::string("full") : it->second; }(),
                true, ctx[i].http_code);
        }

        g_http_retries.fetch_add(retry_next.size(), std::memory_order_relaxed);
        std::this_thread::sleep_for(std::chrono::milliseconds(backoff_ms(attempt)));
        pending.swap(retry_next);
    }

    std::vector<std::vector<uint8_t>> results(n);
    for (size_t i = 0; i < n; ++i) results[i] = std::move(ctx[i].buf.body);
    return results;
}
