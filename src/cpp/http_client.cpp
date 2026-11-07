/**
 * Pure C++ HTTP client implementation.
 *
 * No Python.h. No PyObject. No PyErr_SetString.
 * Errors are std::runtime_error. The Cython layer translates to Python.
 *
 * Thread-safety:
 *   get() / head()  — curl_easy_perform(), each call owns its CURL* easy handle.
 *                     CURLSH provides shared connection/DNS cache with mutex.
 *   get_many() /    — the calling thread's persistent CURLM* (thread_multi),
 *   head_many()       all N transfers on the calling thread; its connection
 *                     cache survives across batches. No CURLOPT_SHARE, so these
 *                     connections are not shared with get()/head().
 */

#include "http_client.hpp"

#include <sys/socket.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdio>
#include <cstdlib>
#include <deque>
#include <cstring>
#include <mutex>
#include <random>
#include <sstream>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

namespace {

// A URL as it may appear in an ERROR MESSAGE: everything up to the query string.
// A presigned URL carries its credential there - X-Amz-Signature, X-Amz-Credential
// and X-Amz-Security-Token for S3, X-Goog-Signature for GCS - and an error message
// is copied into job documents, service logs and API responses by code that has
// no reason to think it holds one (jobs.opteryx docs/design/secrets.md §3, §8.2).
// The path is kept, so the message still says which object failed.
std::string redact_url(const std::string& url) {
    const auto query = url.find('?');
    if (query == std::string::npos) return url;
    return url.substr(0, query) + "?<redacted>";
}

// ── Socket descriptor ceiling of the linked libcurl ──────────────────────────
// Set by setup.py resolve_libcurl(): the fd number at and above which the linked
// libcurl cannot wait on a socket, 0 when it has none. A libcurl built without
// poll() (every darwin build) waits with select(), and Curl_socket_check() fails
// any fd >= FD_SETSIZE; cf-socket.c reports that as CURLE_COULDNT_CONNECT with
// os_errno=0 — indistinguishable from a refused connection, and retried as one.
// The guard below names it instead.
#ifndef OPTERYX_CURL_SELECT_FD_LIMIT
#error "OPTERYX_CURL_SELECT_FD_LIMIT must be defined by the build (setup.py resolve_libcurl)"
#endif
constexpr long kCurlSelectFdLimit = OPTERYX_CURL_SELECT_FD_LIMIT;

// Per-transfer record of a socket refused for exceeding kCurlSelectFdLimit;
// CURL_SOCKET_BAD when none was.
struct SocketGuard {
    curl_socket_t refused_fd = CURL_SOCKET_BAD;
    bool refused() const { return refused_fd != CURL_SOCKET_BAD; }
    // Set when the client denies non-public peers; refused_address then names
    // the address a connect (first hop, redirect or re-resolve) was refused for.
    bool deny_private = false;
    std::string refused_address;
    bool refused_private() const { return !refused_address.empty(); }
};

// True for any address that is not a public unicast peer: loopback, RFC 1918,
// CGNAT, link-local (cloud metadata 169.254.169.254), unspecified, multicast,
// reserved, IPv6 ULA/link-local, and IPv4-mapped forms of all of those.
bool is_non_public_ipv4(uint32_t a) {
    const uint32_t o1 = a >> 24, o2 = (a >> 16) & 0xFFu;
    return o1 == 0 || o1 == 10 || o1 == 127 || o1 >= 224 ||
           (o1 == 100 && (o2 & 0xC0u) == 64) || (o1 == 169 && o2 == 254) ||
           (o1 == 172 && (o2 & 0xF0u) == 16) || (o1 == 192 && o2 == 168);
}

bool is_non_public_address(const struct sockaddr* sa, std::string& text) {
    char buf[INET6_ADDRSTRLEN] = {0};
    if (sa->sa_family == AF_INET) {
        const auto* in4 = reinterpret_cast<const struct sockaddr_in*>(sa);
        ::inet_ntop(AF_INET, &in4->sin_addr, buf, sizeof(buf));
        text = buf;
        return is_non_public_ipv4(ntohl(in4->sin_addr.s_addr));
    }
    if (sa->sa_family == AF_INET6) {
        const auto* in6 = reinterpret_cast<const struct sockaddr_in6*>(sa);
        const uint8_t* b = in6->sin6_addr.s6_addr;
        ::inet_ntop(AF_INET6, &in6->sin6_addr, buf, sizeof(buf));
        text = buf;
        static const uint8_t mapped[12] = {0,0,0,0,0,0,0,0,0,0,0xFF,0xFF};
        if (std::memcmp(b, mapped, 12) == 0)
            return is_non_public_ipv4((uint32_t(b[12]) << 24) | (uint32_t(b[13]) << 16) |
                                      (uint32_t(b[14]) << 8) | uint32_t(b[15]));
        bool zero_prefix = true;
        for (int i = 0; i < 15; ++i) zero_prefix = zero_prefix && b[i] == 0;
        return zero_prefix ||                       // :: and ::1
               (b[0] & 0xFEu) == 0xFC ||            // fc00::/7 unique local
               (b[0] == 0xFE && (b[1] & 0xC0u) == 0x80) ||  // fe80::/10 link-local
               b[0] == 0xFF;                        // ff00::/8 multicast
    }
    text = "family " + std::to_string(sa->sa_family);
    return true;  // not an IP peer at all: never a public HTTP host
}

// CURLOPT_OPENSOCKETFUNCTION: create the socket as libcurl would, and refuse one
// libcurl could not wait on. Returning CURL_SOCKET_BAD fails the transfer; the
// guard is how the caller learns the real reason.
curl_socket_t open_guarded_socket(void* clientp, curlsocktype, struct curl_sockaddr* addr) {
    auto* guard = static_cast<SocketGuard*>(clientp);
    std::string text;
    if (guard->deny_private && is_non_public_address(&addr->addr, text)) {
        guard->refused_address = text;
        return CURL_SOCKET_BAD;
    }
    curl_socket_t fd = ::socket(addr->family, addr->socktype, addr->protocol);
    if (fd != CURL_SOCKET_BAD && kCurlSelectFdLimit > 0 && fd >= kCurlSelectFdLimit) {
        guard->refused_fd = fd;
        ::close(fd);
        return CURL_SOCKET_BAD;
    }
    return fd;
}

// Install the guard on a handle. No callback at all when libcurl has no ceiling.
void configure_socket_guard(CURL* easy, SocketGuard* guard, bool deny_private) {
    guard->refused_fd = CURL_SOCKET_BAD;
    guard->deny_private = deny_private;
    guard->refused_address.clear();
    if (kCurlSelectFdLimit > 0 || deny_private) {
        curl_easy_setopt(easy, CURLOPT_OPENSOCKETFUNCTION, &open_guarded_socket);
        curl_easy_setopt(easy, CURLOPT_OPENSOCKETDATA,     guard);
    }
}

// Non-retryable: the host resolves to the same non-public address on a retry.
[[noreturn]] void throw_private_address(const char* who, const SocketGuard& guard,
                                        const std::string& url) {
    throw HttpError(
        std::string(who) + ": refused to connect to non-public address " +
            guard.refused_address + " (loopback, private, link-local or reserved). "
            "Set the ALLOW_LOCAL_READS=1 environment variable to allow reads from "
            "this machine's network. url=" + redact_url(url),
        false, 0);
}

// Non-retryable: the same process state hands out the same fd on a retry.
[[noreturn]] void throw_socket_fd_limit(const char* who, const SocketGuard& guard,
                                        const std::string& url) {
    throw HttpError(
        std::string(who) + ": socket fd " + std::to_string(guard.refused_fd) +
            " >= " + std::to_string(kCurlSelectFdLimit) +
            ", the linked libcurl's select() limit (FD_SETSIZE; libcurl has no poll() "
            "on this platform): the process holds too many open descriptors. url=" +
            redact_url(url),
        false, 0);
}

// ── WP-5 retry / per-request-timeout configuration (env, read once) ──────────
// These are the process-wide FALLBACK values only — HttpClient::default_tuning()
// assembles them into an HttpTuning. A caller (Opteryx's query engine) that
// wants a per-query SET override never touches these; it builds its own
// HttpTuning and passes it explicitly to get()/get_many() (see http_client.hpp).
// A deadline of 0 would mean "none" to curl, so a non-positive value is refused.
long positive_ms_env(const char* name, long fallback) {
    const char* e = std::getenv(name);
    if (!e) return fallback;
    const long v = std::atol(e);
    if (v <= 0) throw std::invalid_argument(std::string(name) + " must be a positive number of ms (got '" + e + "')");
    return v;
}
long http_stall_ms_env()   { static const long v = positive_ms_env("OPTERYX_HTTP_STALL_MS", 5000);    return v; }
long http_ceiling_ms_env() { static const long v = positive_ms_env("OPTERYX_HTTP_CEILING_MS", 30000); return v; }
long http_connect_ms_env() { static const long v = positive_ms_env("OPTERYX_HTTP_CONNECT_MS", 2000);  return v; }

int http_max_retries_env() {
    static int v = []() {
        const char* e = std::getenv("OPTERYX_HTTP_MAX_RETRIES");
        return e ? std::atoi(e) : 2;
    }();
    return v;
}
// HTTP/2 multiplexing (CURLOPT_PIPEWAIT) — see HttpTuning::use_multiplexing in
// http_client.hpp for the mechanism and the production measurements. Defaults
// ON: without it, get_many() opens a connection per range against a server that
// would have multiplexed them all onto one. Env var is the DISABLE switch, so
// the name matches the Opteryx variable (`disable_http_multiplexing`) and the
// unset/default state is the fast one.
bool http_use_multiplexing_env() {
    static bool v = []() {
        const char* e = std::getenv("OPTERYX_HTTP_DISABLE_MULTIPLEXING");
        return !(e && (*e == '1' || *e == 't' || *e == 'T' || *e == 'y' || *e == 'Y'));
    }();
    return v;
}
// CURLOPT_PIPEWAIT — SEPARATE from multiplexing above, and OFF by default so the
// default path is byte-for-byte the historical behaviour (libcurl's own
// CURLPIPE_MULTIPLEX default, no pipewait). Opt-in only: see HttpTuning for why
// this is not free on a batch that finds no warm connection to reuse.
bool http_use_pipewait_env() {
    static bool v = []() {
        const char* e = std::getenv("OPTERYX_HTTP_PIPEWAIT");
        return e && (*e == '1' || *e == 't' || *e == 'T' || *e == 'y' || *e == 'Y');
    }();
    return v;
}
// Diagnostic only — pin to HTTP/1.1 to measure what h2 is contributing.
bool http_force_http11_env() {
    static bool v = []() {
        const char* e = std::getenv("OPTERYX_HTTP_DISABLE_HTTP2");
        return e && (*e == '1' || *e == 't' || *e == 'T' || *e == 'y' || *e == 'Y');
    }();
    return v;
}
// get_many()'s per-host connection cap. HttpClient is thread_local (one per BS
// worker thread, each with its own independent CURLM), so this bounds only
// ONE thread's own batch — it does not by itself bound the cross-thread total
// of simultaneous new connections to one host, which is the real constraint:
// an isolated repro (std::thread + curl_multi, no opteryx code) on dev
// hardware (18 cores) found 252 total concurrent new connections to one host
// succeeded, 280 started failing (CURLE_COULDNT_CONNECT, os_errno=0 — vetoed
// above the socket layer, not a real connect() failure) — consistent with a
// local security/VPN network extension enforcing a per-host concurrent-
// connection ceiling. Against the real (higher-fan-out, multi-batch) engine
// workload the safe margin was narrower than that isolated model predicted:
// a per-thread cap of 4 was flaky (occasional failures) and 3 was clean
// across 7/7 runs. Default of 3 trades some throughput for a real safety
// margin below the observed edge, given the ceiling isn't a clean, jitter-
// free cutoff in practice; override for environments with a different (or
// no) such ceiling. Callers still clamp against max_connections_ (the CURLSH
// pool cap) at the call site — this returns the raw configured value only.
long http_max_host_connections_env() {
    static long v = []() {
        const char* e = std::getenv("OPTERYX_HTTP_MAX_HOST_CONNECTIONS");
        return e ? std::atol(e) : 3L;
    }();
    return v;
}


// A CURL transport result is retryable if it is a transient connection/timeout/
// partial-transfer condition (vs a definitive protocol/SSL error).
bool curl_result_retryable(CURLcode c) {
    switch (c) {
        case CURLE_OPERATION_TIMEDOUT:
        case CURLE_COULDNT_CONNECT:
        case CURLE_COULDNT_RESOLVE_HOST:
        case CURLE_SSL_CONNECT_ERROR:
        case CURLE_GOT_NOTHING:
        case CURLE_RECV_ERROR:
        case CURLE_SEND_ERROR:
        case CURLE_PARTIAL_FILE:
            return true;
        default:
            return false;
    }
}

// Only 2xx is success. CURLOPT_FOLLOWLOCATION is set on every handle, so a 3xx
// that still reaches us is one curl could NOT follow -- S3 answers a request sent
// to the wrong regional endpoint with exactly that: a 301 carrying no Location
// header and an XML error document as its body. Accepting `code < 400` handed
// that document back as if it were object bytes, so a wrong-region scan decoded
// an error page as Parquet instead of failing. No caller sends a conditional
// request, so no 3xx is ever legitimate here.
bool http_status_ok(long code) {
    return code >= 200 && code < 300;
}

// HTTP status is retryable for 5xx (server-side transient) and 429 (rate limit);
// 4xx (except 429) is a hard client error — never retried.
bool http_status_retryable(long code) {
    return code >= 500 || code == 429;
}

// Byte span of a "Range: bytes=START-END" header; -1 when there is no Range
// (a whole-object GET, whose size is unknown up front).
long range_span_bytes(const std::map<std::string, std::string>& headers) {
    auto it = headers.find("Range");
    if (it == headers.end()) return -1;
    const std::string& r = it->second;
    size_t eq = r.find('=');
    size_t dash = r.find('-', eq == std::string::npos ? 0 : eq + 1);
    if (eq == std::string::npos || dash == std::string::npos) return -1;
    long start = std::atol(r.c_str() + eq + 1);
    long end   = std::atol(r.c_str() + dash + 1);
    return (end >= start) ? (end - start + 1) : 0;
}

// The stall deadline (HttpTuning::stall_ms), watched from curl's progress callback.
// Not curl's LOW_SPEED options: those average over a ~5 s window, so a transfer that
// went quiet after some bytes was caught 5+ s late (measured 6.4 s for a 1 s stall).
// The clock starts once the request is sent (pretransfer), so a request still queued
// for a connection is never a stall; connect_ms and the ceiling bound that wait.
struct StallWatch {
    CURL*      easy     = nullptr;
    long       stall_ms = 0;
    curl_off_t moved    = -1;   // bytes up + down at the last change
    std::chrono::steady_clock::time_point last;
    bool       stalled  = false;

    static int on_progress(void* p, curl_off_t, curl_off_t dlnow, curl_off_t, curl_off_t ulnow) {
        auto* w = static_cast<StallWatch*>(p);
        curl_off_t sent_us = 0;
        curl_easy_getinfo(w->easy, CURLINFO_PRETRANSFER_TIME_T, &sent_us);
        if (sent_us == 0) return 0;
        const auto now = std::chrono::steady_clock::now();
        if (dlnow + ulnow != w->moved) {
            w->moved = dlnow + ulnow;
            w->last = now;
            return 0;
        }
        if (now - w->last < std::chrono::milliseconds(w->stall_ms)) return 0;
        w->stalled = true;
        return 1;   // aborts the transfer (CURLE_ABORTED_BY_CALLBACK)
    }
};

// Progress deadlines (HttpTuning): connect within connect_ms, never go stall_ms
// without a byte once sent, finish by `ceiling_ms`. `w` must outlive the transfer.
void apply_deadlines(CURL* easy, const HttpTuning& t, long ceiling_ms, StallWatch* w) {
    *w = StallWatch{};
    w->easy = easy;
    w->stall_ms = t.stall_ms;
    curl_easy_setopt(easy, CURLOPT_CONNECTTIMEOUT_MS, t.connect_ms);
    curl_easy_setopt(easy, CURLOPT_TIMEOUT_MS,        ceiling_ms);
    curl_easy_setopt(easy, CURLOPT_XFERINFOFUNCTION,  StallWatch::on_progress);
    curl_easy_setopt(easy, CURLOPT_XFERINFODATA,      w);
    curl_easy_setopt(easy, CURLOPT_NOPROGRESS,        0L);
}

// A stall abort is reported as the timeout it is, so it retries like one.
CURLcode deadline_result(CURLcode res, const StallWatch& w) {
    return w.stalled ? CURLE_OPERATION_TIMEDOUT : res;
}

// Which deadline a CURLE_OPERATION_TIMEDOUT was: curl reports connect and ceiling alike.
std::string timeout_kind(const StallWatch& w, curl_off_t connect_us, const HttpTuning& t, long ceiling_ms) {
    if (w.stalled) return "stalled (no bytes for " + std::to_string(t.stall_ms) + "ms)";
    if (connect_us == 0) return "did not connect within " + std::to_string(t.connect_ms) + "ms";
    return "exceeded the " + std::to_string(ceiling_ms) + "ms ceiling";
}

// Counts a deadline failure into the caller's stats: a stall, or the ceiling (a
// timeout once connected that was not a stall).
void note_deadline(HttpCallStats* stats, CURLcode res, const StallWatch& w, curl_off_t connect_us) {
    if (stats == nullptr || res != CURLE_OPERATION_TIMEDOUT) return;
    if (w.stalled) ++stats->stalls;
    else if (connect_us != 0) ++stats->ceiling_timeouts;
}

// Backoff with full jitter: random in [0, base * 2^attempt], capped.
unsigned backoff_ms(int attempt) {
    static thread_local std::mt19937 rng{std::random_device{}()};
    unsigned base = 50u << std::min(attempt, 6);    // 50,100,200,... capped
    if (base > 2000u) base = 2000u;
    std::uniform_int_distribution<unsigned> d(0, base);
    return d(rng);
}


// curl_easy_init()'s implicit auto-init (CURL_GLOBAL_ALL) is documented as not
// thread-safe. tl_http_client() is thread_local, so multiple worker threads can
// each construct their first HttpClient — and therefore reach curl_easy_init()/
// curl_share_init() — at nearly the same moment. Racing the implicit init
// corrupts libcurl's resolver/TLS-backend state and surfaces as sporadic
// CURLE_COULDNT_CONNECT. std::call_once makes the real curl_global_init() run
// exactly once, single-threaded, before any handle is created.
void ensure_curl_global_init() {
    static std::once_flag flag;
    std::call_once(flag, []() {
        CURLcode rc = curl_global_init(CURL_GLOBAL_ALL);
        if (rc != CURLE_OK) {
            throw std::runtime_error(
                std::string("curl_global_init() failed: ") + curl_easy_strerror(rc));
        }
    });
    // No matching curl_global_cleanup(): other threads' thread_local HttpClient
    // instances (and their live CURLSH/easy handles) have no defined shutdown
    // order relative to each other, so calling global cleanup while any of them
    // could still be mid-transfer is unsafe. The process exiting reclaims the
    // resources; this trades a harmless exit-time leak for never tearing down
    // libcurl out from under a live transfer.
}

// The calling thread's CURLM for get_many()/head_many(). A CURLM owns its own
// connection cache, so a CURLM built per batch (the previous design) closed
// every connection when the batch ended and the next batch — the next row
// group, the next table's scan — dialled fresh: a TCP (+TLS) handshake and a
// cold congestion window on EVERY range request. Keeping one CURLM per thread
// for the thread's lifetime lets consecutive batches reuse warm connections.
//
// Per OS THREAD, not per HttpClient: the Cython HttpClient wrapper may call
// get_many() on one instance from several threads at once (GIL released), and
// a CURLM must only ever be driven by one thread. A thread runs one batch at a
// time, so batches never overlap on it. Per-call settings (host cap, pipelining)
// are re-applied to the multi handle at the start of every batch.
struct ThreadMulti {
    CURLM* handle = nullptr;
    ~ThreadMulti() { if (handle) curl_multi_cleanup(handle); }
};

thread_local ThreadMulti tl_multi;

CURLM* thread_multi(const char* who) {
    if (!tl_multi.handle) {
        tl_multi.handle = curl_multi_init();
        if (!tl_multi.handle)
            throw std::runtime_error(std::string(who) + ": curl_multi_init() failed");
    }
    return tl_multi.handle;
}

// A CURLM-level error (not a per-transfer one) leaves the multi handle in an
// unknown state; drop it, and its connection cache, so the thread's next batch
// starts from a fresh handle rather than reusing a broken one.
void discard_thread_multi() {
    if (tl_multi.handle) {
        curl_multi_cleanup(tl_multi.handle);
        tl_multi.handle = nullptr;
    }
}

}  // namespace

namespace http_link {

unsigned backoff_ms(int attempt) { return ::backoff_ms(attempt); }

}  // namespace http_link

HttpTuning HttpClient::default_tuning() {
    static HttpTuning t = []() {
        HttpTuning c;
        c.max_host_connections     = http_max_host_connections_env();
        c.max_retries               = http_max_retries_env();
        c.stall_ms                  = http_stall_ms_env();
        c.ceiling_ms                = http_ceiling_ms_env();
        c.connect_ms                = http_connect_ms_env();
        c.use_multiplexing          = http_use_multiplexing_env();
        c.use_pipewait              = http_use_pipewait_env();
        c.force_http11              = http_force_http11_env();
        return c;
    }();
    return t;
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

HttpClient::HttpClient(int max_connections, bool deny_private_addresses)
    : share_handle_(nullptr),
      max_connections_(max_connections),
      deny_private_(deny_private_addresses),
      user_agent_("opteryx/1.0"),
      ca_bundle_(find_ca_bundle()) {

    ensure_curl_global_init();

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
    const std::map<std::string, std::string>& headers,
    const HttpTuning* tuning_ptr,
    HttpCallStats* stats)
{
    HttpTuning tuning = tuning_ptr ? *tuning_ptr : default_tuning();

    for (int attempt = 0; ; ++attempt) {
        CURL* easy = curl_easy_init();
        if (!easy) throw std::runtime_error("curl_easy_init() failed");

        ResponseBuffer buf;
        SocketGuard guard;
        StallWatch watch;

        curl_easy_setopt(easy, CURLOPT_URL,            url.c_str());
        curl_easy_setopt(easy, CURLOPT_USERAGENT,       user_agent_.c_str());
        apply_deadlines(easy, tuning, tuning.ceiling_ms, &watch);
        curl_easy_setopt(easy, CURLOPT_FOLLOWLOCATION,  1L);
        curl_easy_setopt(easy, CURLOPT_MAXREDIRS,       5L);
        curl_easy_setopt(easy, CURLOPT_WRITEFUNCTION,   ResponseBuffer::write_body);
        curl_easy_setopt(easy, CURLOPT_WRITEDATA,       &buf);
        configure_ssl(easy);
        configure_share(easy);
        configure_socket_guard(easy, &guard, deny_private_);

        // Build custom header list
        struct curl_slist* hlist = nullptr;
        for (const auto& kv : headers) {
            std::string line = kv.first + ": " + kv.second;
            hlist = curl_slist_append(hlist, line.c_str());
        }
        if (hlist) curl_easy_setopt(easy, CURLOPT_HTTPHEADER, hlist);

        const CURLcode res = deadline_result(curl_easy_perform(easy), watch);
        curl_slist_free_all(hlist);

        bool curl_ok = (res == CURLE_OK);
        long http_code = 0;
        if (curl_ok) curl_easy_getinfo(easy, CURLINFO_RESPONSE_CODE, &http_code);
        long os_errno = 0;
        curl_easy_getinfo(easy, CURLINFO_OS_ERRNO, &os_errno);
        curl_off_t connect_us = 0;
        curl_easy_getinfo(easy, CURLINFO_CONNECT_TIME_T, &connect_us);
        curl_easy_cleanup(easy);

        if (guard.refused_private()) throw_private_address("get", guard, url);
        if (guard.refused()) throw_socket_fd_limit("get", guard, url);
        bool http_ok = curl_ok && http_status_ok(http_code);
        if (http_ok) return std::move(buf.body);

        bool retryable = curl_ok ? http_status_retryable(http_code)
                                  : curl_result_retryable(res);
        note_deadline(stats, res, watch, connect_us);
        if (!retryable || attempt >= tuning.max_retries) {
            if (!curl_ok)
                throw HttpError(
                    std::string("get: CURL error: ") + curl_easy_strerror(res) +
                        (res == CURLE_OPERATION_TIMEDOUT
                             ? " (" + timeout_kind(watch, connect_us, tuning, tuning.ceiling_ms) + ")"
                             : std::string()) +
                        " [os_errno=" + std::to_string(os_errno) + "] url=" + redact_url(url),
                    retryable, 0);
            throw HttpError(
                std::string("get: HTTP ") + std::to_string(http_code) + ": " + redact_url(url),
                retryable, http_code);
        }

        if (stats) ++stats->retries;
        std::this_thread::sleep_for(std::chrono::milliseconds(backoff_ms(attempt)));
    }
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
    SocketGuard guard;

    curl_easy_setopt(easy, CURLOPT_URL,            url.c_str());
    curl_easy_setopt(easy, CURLOPT_USERAGENT,       user_agent_.c_str());
    StallWatch watch;
    apply_deadlines(easy, default_tuning(), default_tuning().ceiling_ms, &watch);
    curl_easy_setopt(easy, CURLOPT_NOBODY,          1L);  // HEAD
    curl_easy_setopt(easy, CURLOPT_FOLLOWLOCATION,  1L);
    curl_easy_setopt(easy, CURLOPT_MAXREDIRS,       5L);
    curl_easy_setopt(easy, CURLOPT_HEADERFUNCTION,  ResponseBuffer::write_headers);
    curl_easy_setopt(easy, CURLOPT_HEADERDATA,      &buf);
    configure_ssl(easy);
    configure_share(easy);
    configure_socket_guard(easy, &guard, deny_private_);

    // Build custom header list
    struct curl_slist* hlist = nullptr;
    for (const auto& kv : headers) {
        std::string line = kv.first + ": " + kv.second;
        hlist = curl_slist_append(hlist, line.c_str());
    }
    if (hlist) curl_easy_setopt(easy, CURLOPT_HTTPHEADER, hlist);

    CURLcode res = deadline_result(curl_easy_perform(easy), watch);

    long http_code = 0;
    curl_easy_getinfo(easy, CURLINFO_RESPONSE_CODE, &http_code);
    curl_slist_free_all(hlist);
    curl_easy_cleanup(easy);

    if (guard.refused_private()) throw_private_address("head", guard, url);
    if (guard.refused()) throw_socket_fd_limit("head", guard, url);
    // HttpError, not a bare runtime_error, so the status survives the trip to
    // Python: callers such as the S3 filesystem's get_file_info must tell a 404
    // (the object really is absent) from a signature or redirect failure (the
    // object's existence is unknown), and the message text is not a contract.
    if (res != CURLE_OK) {
        throw HttpError(
            std::string("CURL error: ") + curl_easy_strerror(res),
            curl_result_retryable(res), 0);
    }
    if (!http_status_ok(http_code)) {
        throw HttpError(
            std::string("HTTP ") + std::to_string(http_code) + ": " + redact_url(url),
            http_status_retryable(http_code), http_code);
    }

    return parse_headers(buf.headers_raw);
}

// ---------------------------------------------------------------------------
// put() — one PUT; the status is the caller's to interpret (see the header)
// ---------------------------------------------------------------------------

HttpClient::PutResponse HttpClient::put(
    const std::string& url,
    const uint8_t* data,
    size_t size,
    const std::map<std::string, std::string>& headers,
    long timeout_ms)
{
    CURL* easy = curl_easy_init();
    if (!easy) throw std::runtime_error("curl_easy_init() failed");

    ResponseBuffer buf;
    SocketGuard guard;
    curl_easy_setopt(easy, CURLOPT_URL,              url.c_str());
    curl_easy_setopt(easy, CURLOPT_USERAGENT,        user_agent_.c_str());
    // The caller's timeout is the ceiling: an upload's size is the caller's, not a read range.
    StallWatch watch;
    apply_deadlines(easy, default_tuning(), timeout_ms, &watch);
    curl_easy_setopt(easy, CURLOPT_CUSTOMREQUEST,    "PUT");
    curl_easy_setopt(easy, CURLOPT_POSTFIELDS,       size ? reinterpret_cast<const char*>(data) : "");
    curl_easy_setopt(easy, CURLOPT_POSTFIELDSIZE_LARGE, static_cast<curl_off_t>(size));
    curl_easy_setopt(easy, CURLOPT_WRITEFUNCTION,    ResponseBuffer::write_body);
    curl_easy_setopt(easy, CURLOPT_WRITEDATA,        &buf);
    curl_easy_setopt(easy, CURLOPT_HEADERFUNCTION,   ResponseBuffer::write_headers);
    curl_easy_setopt(easy, CURLOPT_HEADERDATA,       &buf);
    configure_ssl(easy);
    configure_share(easy);
    configure_socket_guard(easy, &guard, deny_private_);

    struct curl_slist* hlist = nullptr;
    for (const auto& kv : headers) {
        std::string line = kv.first + ": " + kv.second;
        hlist = curl_slist_append(hlist, line.c_str());
    }
    // No `Expect: 100-continue` round trip before a large body: the session URI has
    // already accepted the upload, so waiting for permission only adds latency.
    hlist = curl_slist_append(hlist, "Expect:");
    curl_easy_setopt(easy, CURLOPT_HTTPHEADER, hlist);

    CURLcode res = deadline_result(curl_easy_perform(easy), watch);
    long http_code = 0;
    curl_easy_getinfo(easy, CURLINFO_RESPONSE_CODE, &http_code);
    curl_slist_free_all(hlist);
    curl_easy_cleanup(easy);

    if (guard.refused_private()) throw_private_address("put", guard, url);
    if (guard.refused()) throw_socket_fd_limit("put", guard, url);
    if (res != CURLE_OK) {
        throw HttpError(std::string("CURL error: ") + curl_easy_strerror(res),
                        curl_result_retryable(res), 0);
    }
    PutResponse out;
    out.status = http_code;
    out.headers = parse_headers(buf.headers_raw);
    out.body = std::move(buf.body);
    return out;
}

// ---------------------------------------------------------------------------
// get_many() — concurrent batch GET via the calling thread's CURLM event loop
//
// Design:
//   - Drives the calling thread's persistent CURLM* (thread_multi); never
//     shared across threads. Its connection cache persists across calls, so
//     consecutive batches on a thread reuse warm connections.
//   - All N easy handles are added at once; CURLM multiplexes them concurrently.
//   - No CURLOPT_SHARE: connections are not shared with get()/head().
//   - Results are returned in the same order as the input requests vector.
//   - On any error (CURL, HTTP 4xx/5xx), throws std::runtime_error and cleans up.
// ---------------------------------------------------------------------------

std::vector<std::vector<uint8_t>> HttpClient::get_many(
    const std::vector<std::pair<std::string, std::map<std::string, std::string>>>& requests,
    const HttpTuning* tuning_ptr,
    HttpCallStats* stats)
{
    const size_t n = requests.size();
    if (n == 0) return {};

    HttpTuning tuning = tuning_ptr ? *tuning_ptr : default_tuning();

    // Per-request context: response buffer + last curl result/status.
    struct RequestCtx {
        ResponseBuffer buf;
        CURLcode       res      = CURLE_OK;
        long           http_code = 0;
        long           os_errno  = 0;   // DEBUG: raw connect()/socket() errno, see CURLINFO_OS_ERRNO
        SocketGuard    guard;
        curl_off_t     connect_us = 0;  // CURLINFO_CONNECT_TIME_T on the last attempt
        StallWatch     watch;
        curl_off_t     elapsed_us = 0;  // CURLINFO_TOTAL_TIME_T on the last attempt
    };
    std::vector<RequestCtx> ctx(n);
    // Shape of the last attempt's batch, for the exhausted-retries message.
    size_t last_batch_n     = 0;
    long   last_batch_bytes = 0;

    // Fetch a subset of requests (by index) concurrently via the thread's CURLM,
    // harvesting result + status into ctx[idx]. Resets each buffer first so a
    // retry does not append to a partial body. Throws only on CURLM-setup
    // failures (not per-request transport errors — those land in ctx).
    auto fetch = [&](const std::vector<size_t>& idxs) {
        std::vector<CURL*>       handles(idxs.size(), nullptr);
        std::vector<curl_slist*> hlists(idxs.size(), nullptr);

        CURLM* multi = thread_multi("get_many");
        long host_cap = std::min(tuning.max_host_connections, (long)max_connections_);
        curl_multi_setopt(multi, CURLMOPT_MAX_HOST_CONNECTIONS, host_cap);
        curl_multi_setopt(multi, CURLMOPT_MAXCONNECTS,          (long)(max_connections_ * 2));
        // Enable h2 multiplexing on the multi handle. Default-on in libcurl
        // since 7.62, set explicitly so behaviour does not depend on the linked
        // libcurl's vintage. Paired with CURLOPT_PIPEWAIT per easy handle below
        // — the multi option ALONE is not enough: without PIPEWAIT, handles
        // added before any connection is established each get their own.
        curl_multi_setopt(multi, CURLMOPT_PIPELINING,
                          tuning.use_multiplexing ? (long)CURLPIPE_MULTIPLEX : (long)CURLPIPE_NOTHING);

        long batch_bytes = 0;
        for (size_t i : idxs) {
            long span = range_span_bytes(requests[i].second);
            if (span > 0) batch_bytes += span;
        }
        last_batch_n     = idxs.size();
        last_batch_bytes = batch_bytes;

        auto cleanup = [&]() {
            for (size_t j = 0; j < idxs.size(); ++j) {
                if (handles[j]) {
                    curl_multi_remove_handle(multi, handles[j]);
                    curl_easy_cleanup(handles[j]);
                    handles[j] = nullptr;
                }
                if (hlists[j]) { curl_slist_free_all(hlists[j]); hlists[j] = nullptr; }
            }
        };
        // CURLM-level failure: release this batch's handles, then drop the
        // thread's multi handle (see discard_thread_multi).
        auto fail_multi = [&]() { cleanup(); discard_thread_multi(); };

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
            apply_deadlines(easy, tuning, tuning.ceiling_ms, &ctx[i].watch);
            curl_easy_setopt(easy, CURLOPT_FOLLOWLOCATION,  1L);
            curl_easy_setopt(easy, CURLOPT_MAXREDIRS,       5L);
            curl_easy_setopt(easy, CURLOPT_WRITEFUNCTION,   ResponseBuffer::write_body);
            curl_easy_setopt(easy, CURLOPT_WRITEDATA,       &ctx[i].buf);
            curl_easy_setopt(easy, CURLOPT_PRIVATE,         reinterpret_cast<void*>(i));
            // THE multiplexing enabler: wait for the first connection to reveal
            // whether it can multiplex before opening another. All N handles are
            // added below in this same loop, before any transfer has run, so
            // without this every one of them dials its own TCP+TLS connection.
            // See HttpTuning::use_multiplexing for measurements.
            if (tuning.use_pipewait)
                curl_easy_setopt(easy, CURLOPT_PIPEWAIT,    1L);
            if (tuning.force_http11)
                curl_easy_setopt(easy, CURLOPT_HTTP_VERSION,
                                 (long)CURL_HTTP_VERSION_1_1);
            configure_ssl(easy);
            configure_socket_guard(easy, &ctx[i].guard, deny_private_);
            // No CURLOPT_SHARE: the thread's CURLM keeps its own connection
            // cache; CURLSH from multi+other-thread-get() could deadlock.

            for (const auto& kv : req_hdrs) {
                std::string line = kv.first + ": " + kv.second;
                hlists[j] = curl_slist_append(hlists[j], line.c_str());
            }
            if (hlists[j]) curl_easy_setopt(easy, CURLOPT_HTTPHEADER, hlists[j]);

            CURLMcode mc = curl_multi_add_handle(multi, easy);
            if (mc != CURLM_OK) {
                fail_multi();
                throw std::runtime_error(
                    std::string("get_many: curl_multi_add_handle: ") + curl_multi_strerror(mc));
            }
        }

        int running = static_cast<int>(idxs.size());
        while (running > 0) {
            CURLMcode mc = curl_multi_perform(multi, &running);
            if (mc != CURLM_OK) {
                fail_multi();
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
                ctx[i].res = deadline_result(msg->data.result, ctx[i].watch);
                ctx[i].http_code = 0;
                curl_easy_getinfo(msg->easy_handle, CURLINFO_RESPONSE_CODE, &ctx[i].http_code);
                ctx[i].os_errno = 0;
                curl_easy_getinfo(msg->easy_handle, CURLINFO_OS_ERRNO, &ctx[i].os_errno);
                ctx[i].elapsed_us = 0;
                curl_easy_getinfo(msg->easy_handle, CURLINFO_TOTAL_TIME_T, &ctx[i].elapsed_us);
                ctx[i].connect_us = 0;
                curl_easy_getinfo(msg->easy_handle, CURLINFO_CONNECT_TIME_T, &ctx[i].connect_us);
            }
        }
        cleanup();
    };

    // First attempt: all requests. Then retry only the transient failures.
    std::vector<size_t> pending(n);
    for (size_t i = 0; i < n; ++i) pending[i] = i;

    const int max_retries = tuning.max_retries;
    for (int attempt = 0; ; ++attempt) {
        fetch(pending);

        std::vector<size_t> retry_next;
        for (size_t i : pending) {
            if (ctx[i].guard.refused_private()) throw_private_address("get_many", ctx[i].guard, requests[i].first);
            if (ctx[i].guard.refused()) throw_socket_fd_limit("get_many", ctx[i].guard, requests[i].first);
            bool curl_ok = (ctx[i].res == CURLE_OK);
            bool http_ok = curl_ok && http_status_ok(ctx[i].http_code);
            if (http_ok) continue;

            bool retryable = curl_ok ? http_status_retryable(ctx[i].http_code)
                                     : curl_result_retryable(ctx[i].res);
            note_deadline(stats, ctx[i].res, ctx[i].watch, ctx[i].connect_us);
            if (!retryable) {
                // Hard failure (4xx or definitive transport error) — fail now.
                if (!curl_ok)
                    throw HttpError(std::string("get_many: CURL error: ") +
                        curl_easy_strerror(ctx[i].res) + " url=" + redact_url(requests[i].first),
                        false, 0);
                throw HttpError(std::string("get_many: HTTP ") +
                    std::to_string(ctx[i].http_code) + ": " + redact_url(requests[i].first),
                    false, ctx[i].http_code);
            }
            retry_next.push_back(i);
        }

        if (retry_next.empty()) break;   // everything succeeded

        if (attempt >= max_retries) {
            size_t i = retry_next.front();
            std::string cause = (ctx[i].res != CURLE_OK)
                ? std::string("CURL error: ") + curl_easy_strerror(ctx[i].res) +
                      (ctx[i].res == CURLE_OPERATION_TIMEDOUT
                           ? " (" + timeout_kind(ctx[i].watch, ctx[i].connect_us, tuning, tuning.ceiling_ms) + ")"
                           : std::string()) +
                      " [os_errno=" + std::to_string(ctx[i].os_errno) + " (" +
                      std::strerror(static_cast<int>(ctx[i].os_errno)) + ")]"
                : std::string("HTTP ") + std::to_string(ctx[i].http_code);
            // received vs expected + elapsed says how far the transfer got.
            const long span = range_span_bytes(requests[i].second);
            throw HttpError(
                "get_many: exhausted " + std::to_string(max_retries) + " retries (" +
                cause + ") url=" + redact_url(requests[i].first) + " range=" +
                [&]() { auto it = requests[i].second.find("Range");
                        return it == requests[i].second.end() ? std::string("full") : it->second; }() +
                " received=" + std::to_string(ctx[i].buf.body.size()) + "/" +
                (span < 0 ? std::string("?") : std::to_string(span)) + "B" +
                " elapsed=" + std::to_string(ctx[i].elapsed_us / 1000) + "ms" +
                " batch=" + std::to_string(last_batch_n) + " ranges/" +
                std::to_string(last_batch_bytes) + "B",
                true, ctx[i].http_code);
        }

        if (stats) stats->retries += retry_next.size();
        std::this_thread::sleep_for(std::chrono::milliseconds(backoff_ms(attempt)));
        pending.swap(retry_next);
    }

    std::vector<std::vector<uint8_t>> results(n);
    for (size_t i = 0; i < n; ++i) results[i] = std::move(ctx[i].buf.body);
    return results;
}

// ---------------------------------------------------------------------------
// head_many() — concurrent batch HEAD via the calling thread's CURLM event loop
//
// Deliberate near-mirror of get_many() above (same per-thread CURLM, same
// retry/backoff policy) — see get_many()'s design comment. The only
// differences: CURLOPT_NOBODY + CURLOPT_HEADERFUNCTION instead of a body
// writer, and the per-request result is a parsed header map instead of raw
// bytes. Exists so that a manifest-style fan-out over many objects' metadata
// runs as ONE native batch (one GIL release for the whole call) instead of
// dispatching per-path head() calls onto a Python-level thread pool, which
// would require each worker thread to cross back into the interpreter once
// per request.
// ---------------------------------------------------------------------------

std::vector<std::map<std::string, std::string>> HttpClient::head_many(
    const std::vector<std::pair<std::string, std::map<std::string, std::string>>>& requests,
    const HttpTuning* tuning_ptr)
{
    const size_t n = requests.size();
    if (n == 0) return {};

    HttpTuning tuning = tuning_ptr ? *tuning_ptr : default_tuning();

    struct RequestCtx {
        ResponseBuffer buf;
        CURLcode       res      = CURLE_OK;
        long           http_code = 0;
        long           os_errno  = 0;
        SocketGuard    guard;
        StallWatch     watch;
    };
    std::vector<RequestCtx> ctx(n);

    auto fetch = [&](const std::vector<size_t>& idxs) {
        std::vector<CURL*>       handles(idxs.size(), nullptr);
        std::vector<curl_slist*> hlists(idxs.size(), nullptr);

        CURLM* multi = thread_multi("head_many");
        long host_cap = std::min(tuning.max_host_connections, (long)max_connections_);
        curl_multi_setopt(multi, CURLMOPT_MAX_HOST_CONNECTIONS, host_cap);
        curl_multi_setopt(multi, CURLMOPT_MAXCONNECTS,          (long)(max_connections_ * 2));
        curl_multi_setopt(multi, CURLMOPT_PIPELINING,
                          tuning.use_multiplexing ? (long)CURLPIPE_MULTIPLEX : (long)CURLPIPE_NOTHING);

        auto cleanup = [&]() {
            for (size_t j = 0; j < idxs.size(); ++j) {
                if (handles[j]) {
                    curl_multi_remove_handle(multi, handles[j]);
                    curl_easy_cleanup(handles[j]);
                    handles[j] = nullptr;
                }
                if (hlists[j]) { curl_slist_free_all(hlists[j]); hlists[j] = nullptr; }
            }
        };
        // CURLM-level failure: release this batch's handles, then drop the
        // thread's multi handle (see discard_thread_multi).
        auto fail_multi = [&]() { cleanup(); discard_thread_multi(); };

        for (size_t j = 0; j < idxs.size(); ++j) {
            const size_t i = idxs[j];
            ctx[i].buf.headers_raw.clear();   // fresh header block for this (re)attempt
            const auto& req_url  = requests[i].first;
            const auto& req_hdrs = requests[i].second;

            CURL* easy = curl_easy_init();
            if (!easy) { cleanup(); throw std::runtime_error("head_many: curl_easy_init() failed"); }
            handles[j] = easy;

            curl_easy_setopt(easy, CURLOPT_URL,            req_url.c_str());
            curl_easy_setopt(easy, CURLOPT_USERAGENT,       user_agent_.c_str());
            apply_deadlines(easy, tuning, tuning.ceiling_ms, &ctx[i].watch);
            curl_easy_setopt(easy, CURLOPT_NOBODY,          1L);  // HEAD
            curl_easy_setopt(easy, CURLOPT_FOLLOWLOCATION,  1L);
            curl_easy_setopt(easy, CURLOPT_MAXREDIRS,       5L);
            curl_easy_setopt(easy, CURLOPT_HEADERFUNCTION,  ResponseBuffer::write_headers);
            curl_easy_setopt(easy, CURLOPT_HEADERDATA,      &ctx[i].buf);
            curl_easy_setopt(easy, CURLOPT_PRIVATE,         reinterpret_cast<void*>(i));
            if (tuning.use_pipewait)
                curl_easy_setopt(easy, CURLOPT_PIPEWAIT,    1L);
            if (tuning.force_http11)
                curl_easy_setopt(easy, CURLOPT_HTTP_VERSION,
                                 (long)CURL_HTTP_VERSION_1_1);
            configure_ssl(easy);
            configure_socket_guard(easy, &ctx[i].guard, deny_private_);
            // No CURLOPT_SHARE: same rationale as get_many().

            for (const auto& kv : req_hdrs) {
                std::string line = kv.first + ": " + kv.second;
                hlists[j] = curl_slist_append(hlists[j], line.c_str());
            }
            if (hlists[j]) curl_easy_setopt(easy, CURLOPT_HTTPHEADER, hlists[j]);

            CURLMcode mc = curl_multi_add_handle(multi, easy);
            if (mc != CURLM_OK) {
                fail_multi();
                throw std::runtime_error(
                    std::string("head_many: curl_multi_add_handle: ") + curl_multi_strerror(mc));
            }
        }

        int running = static_cast<int>(idxs.size());
        while (running > 0) {
            CURLMcode mc = curl_multi_perform(multi, &running);
            if (mc != CURLM_OK) {
                fail_multi();
                throw std::runtime_error(
                    std::string("head_many: curl_multi_perform: ") + curl_multi_strerror(mc));
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
                ctx[i].res = deadline_result(msg->data.result, ctx[i].watch);
                ctx[i].http_code = 0;
                curl_easy_getinfo(msg->easy_handle, CURLINFO_RESPONSE_CODE, &ctx[i].http_code);
                ctx[i].os_errno = 0;
                curl_easy_getinfo(msg->easy_handle, CURLINFO_OS_ERRNO, &ctx[i].os_errno);
            }
        }
        cleanup();
    };

    std::vector<size_t> pending(n);
    for (size_t i = 0; i < n; ++i) pending[i] = i;

    const int max_retries = tuning.max_retries;
    for (int attempt = 0; ; ++attempt) {
        fetch(pending);

        std::vector<size_t> retry_next;
        for (size_t i : pending) {
            if (ctx[i].guard.refused_private()) throw_private_address("head_many", ctx[i].guard, requests[i].first);
            if (ctx[i].guard.refused()) throw_socket_fd_limit("head_many", ctx[i].guard, requests[i].first);
            bool curl_ok = (ctx[i].res == CURLE_OK);
            bool http_ok = curl_ok && http_status_ok(ctx[i].http_code);
            if (http_ok) continue;

            bool retryable = curl_ok ? http_status_retryable(ctx[i].http_code)
                                     : curl_result_retryable(ctx[i].res);
            if (!retryable) {
                if (!curl_ok)
                    throw HttpError(std::string("head_many: CURL error: ") +
                        curl_easy_strerror(ctx[i].res) + " url=" + redact_url(requests[i].first),
                        false, 0);
                throw HttpError(std::string("head_many: HTTP ") +
                    std::to_string(ctx[i].http_code) + ": " + redact_url(requests[i].first),
                    false, ctx[i].http_code);
            }
            retry_next.push_back(i);
        }

        if (retry_next.empty()) break;

        if (attempt >= max_retries) {
            size_t i = retry_next.front();
            std::string cause = (ctx[i].res != CURLE_OK)
                ? std::string("CURL error: ") + curl_easy_strerror(ctx[i].res) +
                      " [os_errno=" + std::to_string(ctx[i].os_errno) + " (" +
                      std::strerror(static_cast<int>(ctx[i].os_errno)) + ")]"
                : std::string("HTTP ") + std::to_string(ctx[i].http_code);
            throw HttpError(
                "head_many: exhausted " + std::to_string(max_retries) + " retries (" +
                cause + ") url=" + redact_url(requests[i].first),
                true, ctx[i].http_code);
        }

        std::this_thread::sleep_for(std::chrono::milliseconds(backoff_ms(attempt)));
        pending.swap(retry_next);
    }

    std::vector<std::map<std::string, std::string>> results(n);
    for (size_t i = 0; i < n; ++i) results[i] = parse_headers(ctx[i].buf.headers_raw);
    return results;
}
