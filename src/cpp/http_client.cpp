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
#include <cstring>
#include <sstream>
#include <stdexcept>
#include <string>
#include <vector>

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

std::map<std::string, std::string> HttpClient::head(const std::string& url) {
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

    CURLcode res = curl_easy_perform(easy);
    curl_easy_cleanup(easy);

    if (res != CURLE_OK) {
        throw std::runtime_error(
            std::string("CURL error: ") + curl_easy_strerror(res));
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

    // Per-request context: response buffer, header list, curl result
    struct RequestCtx {
        ResponseBuffer buf;
        curl_slist*    hlist    = nullptr;
        CURLcode       res      = CURLE_OK;
        long           http_code = 0;
    };

    std::vector<RequestCtx> ctx(n);
    std::vector<CURL*>      handles(n, nullptr);

    CURLM* multi = curl_multi_init();
    if (!multi) throw std::runtime_error("get_many: curl_multi_init() failed");

    // Limit per-host concurrent connections so we don't flood a single origin.
    // max_connections_ is the value passed to the HttpClient constructor.
    curl_multi_setopt(multi, CURLMOPT_MAX_HOST_CONNECTIONS, (long)max_connections_);
    curl_multi_setopt(multi, CURLMOPT_MAXCONNECTS,          (long)(max_connections_ * 2));

    // Cleanup helper: removes all handles from multi, frees easy handles and hlists.
    // Does NOT touch ctx[i].buf.body — those are still needed after cleanup.
    auto cleanup = [&]() {
        for (size_t i = 0; i < n; ++i) {
            if (handles[i]) {
                curl_multi_remove_handle(multi, handles[i]);
                curl_easy_cleanup(handles[i]);
                handles[i] = nullptr;
            }
            if (ctx[i].hlist) {
                curl_slist_free_all(ctx[i].hlist);
                ctx[i].hlist = nullptr;
            }
        }
        curl_multi_cleanup(multi);
    };

    // Set up all easy handles and add them to the multi handle
    for (size_t i = 0; i < n; ++i) {
        const auto& req_url  = requests[i].first;
        const auto& req_hdrs = requests[i].second;

        CURL* easy = curl_easy_init();
        if (!easy) {
            cleanup();
            throw std::runtime_error("get_many: curl_easy_init() failed");
        }
        handles[i] = easy;

        curl_easy_setopt(easy, CURLOPT_URL,            req_url.c_str());
        curl_easy_setopt(easy, CURLOPT_USERAGENT,       user_agent_.c_str());
        curl_easy_setopt(easy, CURLOPT_TIMEOUT_MS,      timeout_ms_);
        curl_easy_setopt(easy, CURLOPT_FOLLOWLOCATION,  1L);
        curl_easy_setopt(easy, CURLOPT_MAXREDIRS,       5L);
        curl_easy_setopt(easy, CURLOPT_WRITEFUNCTION,   ResponseBuffer::write_body);
        curl_easy_setopt(easy, CURLOPT_WRITEDATA,       &ctx[i].buf);
        // Tag with index so we can map completion messages back to ctx[i]
        curl_easy_setopt(easy, CURLOPT_PRIVATE,         reinterpret_cast<void*>(i));
        configure_ssl(easy);
        // Do NOT set CURLOPT_SHARE here. The local CURLM already reuses connections
        // within this batch (all easy handles share the multi's connection pool).
        // Using CURLSH from inside curl_multi_perform() on one thread while other
        // threads are doing the same via get() would contend on the CURLSH mutexes
        // and could deadlock if libcurl acquires two data-type locks on the same thread.

        // Build per-request header list
        for (const auto& kv : req_hdrs) {
            std::string line = kv.first + ": " + kv.second;
            ctx[i].hlist = curl_slist_append(ctx[i].hlist, line.c_str());
        }
        if (ctx[i].hlist) {
            curl_easy_setopt(easy, CURLOPT_HTTPHEADER, ctx[i].hlist);
        }

        CURLMcode mc = curl_multi_add_handle(multi, easy);
        if (mc != CURLM_OK) {
            cleanup();
            throw std::runtime_error(
                std::string("get_many: curl_multi_add_handle: ") + curl_multi_strerror(mc));
        }
    }

    // Run the CURLM event loop until all transfers complete
    int running = static_cast<int>(n);
    while (running > 0) {
        CURLMcode mc = curl_multi_perform(multi, &running);
        if (mc != CURLM_OK) {
            cleanup();
            throw std::runtime_error(
                std::string("get_many: curl_multi_perform: ") + curl_multi_strerror(mc));
        }
        if (running > 0) {
            curl_multi_wait(multi, nullptr, 0, 100, nullptr);
        }
    }

    // Harvest completion messages — must happen before curl_multi_cleanup()
    int msgs_left = 0;
    CURLMsg* msg;
    while ((msg = curl_multi_info_read(multi, &msgs_left)) != nullptr) {
        if (msg->msg == CURLMSG_DONE) {
            CURL* easy = msg->easy_handle;
            void* priv = nullptr;
            curl_easy_getinfo(easy, CURLINFO_PRIVATE, &priv);
            size_t idx = reinterpret_cast<size_t>(priv);
            ctx[idx].res = msg->data.result;
            curl_easy_getinfo(easy, CURLINFO_RESPONSE_CODE, &ctx[idx].http_code);
        }
    }

    // Free all CURL handles and the multi handle.
    // ctx[i].buf.body vectors remain valid — they're owned by ctx, not handles.
    cleanup();

    // Check for errors and move response bodies into the result vector
    std::vector<std::vector<uint8_t>> results(n);
    for (size_t i = 0; i < n; ++i) {
        if (ctx[i].res != CURLE_OK) {
            throw std::runtime_error(
                std::string("get_many[") + std::to_string(i) + "]: CURL error: " +
                curl_easy_strerror(ctx[i].res) + " url=" + requests[i].first);
        }
        if (ctx[i].http_code >= 400) {
            throw std::runtime_error(
                std::string("get_many[") + std::to_string(i) + "]: HTTP " +
                std::to_string(ctx[i].http_code) + ": " + requests[i].first);
        }
        results[i] = std::move(ctx[i].buf.body);
    }

    return results;
}
