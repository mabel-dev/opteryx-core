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
     * Sets CURLOPT_SHARE so the shared connection/DNS cache is used.
     *
     * @param url      URL to fetch
     * @param headers  Optional request headers (e.g. Range, Authorization)
     * @return Response body bytes
     * @throws std::runtime_error on network error, timeout, or HTTP 4xx/5xx
     */
    std::vector<uint8_t> get(
        const std::string& url,
        const std::map<std::string, std::string>& headers = {}
    );

    /**
     * Perform HTTP HEAD request (thread-safe).
     *
     * Uses curl_easy_perform() — each call has its own CURL* easy handle.
     *
     * @param url  URL to query
     * @return Response headers map (lower-case keys)
     * @throws std::runtime_error on network error or timeout
     */
    std::map<std::string, std::string> head(const std::string& url);

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
     * @return Vector of response bodies in the same order as requests
     * @throws std::runtime_error on any network error, timeout, or HTTP 4xx/5xx
     */
    std::vector<std::vector<uint8_t>> get_many(
        const std::vector<std::pair<std::string, std::map<std::string, std::string>>>& requests
    );

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
