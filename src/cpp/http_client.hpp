/**
 * Pure C++ HTTP client for Opteryx IO stack.
 *
 * Intentionally has zero knowledge of Python. Types are std::, errors are
 * std::runtime_error. The Cython layer owns all Python type translation.
 *
 * Thread-safety model:
 * - get() / head(): use curl_easy_perform() — thread-safe, each call owns its
 *   own CURL* easy handle. A CURLSH* share handle (with per-data-type mutex array)
 *   provides shared connection/DNS cache across threads. Per-data-type mutexes are
 *   required because libcurl may lock DNS and CONNECT independently in one operation;
 *   a single mutex would deadlock on the second acquire from the same thread.
 * - get_many(): creates a local CURLM* per call, runs all N transfers on one
 *   thread without CURLOPT_SHARE. The local CURLM already reuses connections
 *   within the batch; CURLSH is not used to avoid any cross-thread mutex contention.
 */

#pragma once

#include <cstdint>
#include <map>
#include <mutex>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include <curl/curl.h>  // needed for curl_lock_data / curl_lock_access in callback signatures

// Thrown on an HTTP/transport failure. `retryable` is true for transient causes
// (connection/timeout/recv errors and 5xx/429); a non-retryable HttpError (4xx)
// is a hard failure. Subclass of std::runtime_error so existing Cython `except +`
// translation and callers that catch std::runtime_error are unaffected.
struct HttpError : std::runtime_error {
    bool retryable;
    long http_status;   // 0 if the failure was a CURL-level error
    HttpError(const std::string& what, bool retryable_, long status)
        : std::runtime_error(what), retryable(retryable_), http_status(status) {}
};

// Per-call tuning for get()/get_many(). A null tuning pointer at the call site
// means "use HttpClient::default_tuning()" (the process-wide env-derived
// values, resolved once). A non-null pointer is a query-scoped override —
// e.g. Opteryx's SET-able http_* variables, resolved fresh per query in
// Python/Cython and passed down by value, NEVER stored on the (thread_local,
// process-lifetime) HttpClient itself. Passing by value here is what makes a
// per-query SET override safe despite HttpClient's thread_local lifetime: no
// mutable client state is touched, so one query's override can never leak
// into the next query serviced by the same worker thread.
struct HttpTuning {
    long   max_host_connections      = 3;               // get_many()'s per-host connection cap
    int    max_retries                = 2;               // transient-failure retry budget
    double min_bandwidth_bytes_per_s  = 20.0e6 / 8.0;     // assumed floor stream bandwidth
    long   timeout_floor_ms           = 10000;            // minimum per-request timeout
};

class HttpClient {
public:
    /**
     * Create HTTP client with shared connection/DNS cache.
     *
     * Detects CA bundle path at construction time to avoid SSL cert errors
     * when running from a manylinux wheel where libcurl has a compiled-in
     * path that may not exist on the deployment system.
     *
     * @throws std::runtime_error if CURLSH init fails
     */
    HttpClient(int max_connections = 128, long timeout_ms = 60000);
    ~HttpClient();

    // Non-copyable — share handle is not copyable
    HttpClient(const HttpClient&) = delete;
    HttpClient& operator=(const HttpClient&) = delete;

    /**
     * Perform HTTP GET request (thread-safe).
     *
     * Uses curl_easy_perform() — each call has its own CURL* easy handle.
     * Sets CURLOPT_SHARE so the shared connection/DNS cache is used. Retries
     * transient failures (per `tuning.max_retries`) and derives the per-request
     * timeout from the Range span (per `tuning.min_bandwidth_bytes_per_s` /
     * `tuning.timeout_floor_ms`) — the same policy get_many() applies.
     *
     * @param url      URL to fetch
     * @param headers  Optional request headers (e.g. Range, Authorization)
     * @param tuning   Optional per-call override; nullptr uses default_tuning()
     * @return Response body bytes
     * @throws HttpError on network error, timeout, or HTTP 4xx/5xx
     */
    std::vector<uint8_t> get(
        const std::string& url,
        const std::map<std::string, std::string>& headers = {},
        const HttpTuning* tuning = nullptr
    );

    /**
     * Perform HTTP HEAD request (thread-safe).
     *
     * Uses curl_easy_perform() — each call has its own CURL* easy handle.
     *
     * @param url     URL to query
     * @param headers Optional request headers (e.g. Authorization)
     * @return Response headers map (lower-case keys)
     * @throws std::runtime_error on network error, timeout, or HTTP 4xx/5xx
     */
    std::map<std::string, std::string> head(
        const std::string& url,
        const std::map<std::string, std::string>& headers = {}
    );

    /**
     * Perform multiple HTTP GET requests concurrently (single-threaded CURLM).
     *
     * Creates a local CURLM* event loop for this call only — never shared
     * across threads. All N transfers run concurrently on the calling thread.
     * This is what CURLM is designed for: one thread, N concurrent transfers.
     *
     * GIL should be released by the Cython caller for the entire duration
     * so Python threads are not blocked while network I/O runs.
     *
     * @param requests  Vector of (url, headers) pairs
     * @param tuning    Optional per-call override; nullptr uses default_tuning()
     * @return Vector of response bodies in the same order as requests
     * @throws HttpError on any network error, timeout, or HTTP 4xx/5xx
     */
    std::vector<std::vector<uint8_t>> get_many(
        const std::vector<std::pair<std::string, std::map<std::string, std::string>>>& requests,
        const HttpTuning* tuning = nullptr
    );

    // Process-wide defaults, each resolved from its OPTERYX_HTTP_* env var
    // exactly once (Meyer's singleton) and cached for the life of the process.
    // This is what get()/get_many() fall back to when called with tuning=nullptr.
    static HttpTuning default_tuning();

    // Process-cumulative count of individual range requests re-issued by
    // get()/get_many()'s transient-failure retry logic. For dev telemetry
    // (surfaced in the IO pipeline diagnostics); not reset per query.
    static uint64_t total_retries();

private:
    void*       share_handle_;                   // CURLSH* — shared connection/DNS cache
    std::mutex  share_mutexes_[CURL_LOCK_DATA_LAST]; // one mutex per curl_lock_data type
    int         max_connections_;                // per-host connection cap used by get_many()
    long        timeout_ms_;
    std::string user_agent_;
    std::string ca_bundle_;                      // Path to CA bundle found at init time

    /**
     * Probe common CA bundle locations and return the first readable one.
     * Returns empty string if none found (curl uses its built-in default).
     */
    static std::string find_ca_bundle();

    /**
     * Apply CA bundle and SSL verification settings to a CURL easy handle.
     */
    void configure_ssl(void* easy_handle) const;

    /**
     * Apply the CURLSH share handle to a CURL easy handle.
     * Only used by get() / head() — NOT by get_many() which uses its own CURLM.
     */
    void configure_share(void* easy_handle) const;

    // CURLSH mutex callbacks — called by libcurl when it needs to serialize
    // access to shared data. Each curl_lock_data type uses its own mutex slot
    // so libcurl can independently lock DNS while holding a CONNECT lock.
    static void _share_lock(CURL*, curl_lock_data data, curl_lock_access, void* userp);
    static void _share_unlock(CURL*, curl_lock_data data, void* userp);
};
