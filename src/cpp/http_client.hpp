/**
 * Pure C++ HTTP client for Opteryx IO stack.
 *
 * Intentionally has zero knowledge of Python. Types are std::, errors are
 * std::runtime_error. The Cython layer owns all Python type translation.
 *
 * Features:
 * - CURLM multi-handle connection pooling
 * - Range request support (GET with arbitrary headers)
 * - HEAD for metadata
 * - Runtime CA bundle detection (fixes SSL cert path issues on Linux)
 */

#pragma once

#include <cstdint>
#include <map>
#include <stdexcept>
#include <string>
#include <vector>

class HttpClient {
public:
    /**
     * Create HTTP client with connection pool.
     *
     * Detects CA bundle path at construction time to avoid SSL cert errors
     * when running from a manylinux wheel where libcurl has a compiled-in
     * path that may not exist on the deployment system.
     *
     * @throws std::runtime_error if CURLM init fails
     */
    HttpClient(int max_connections = 128, long timeout_ms = 60000);
    ~HttpClient();

    // Non-copyable - CURLM handle is not copyable
    HttpClient(const HttpClient&) = delete;
    HttpClient& operator=(const HttpClient&) = delete;

    /**
     * Perform HTTP GET request.
     *
     * @param url   URL to fetch
     * @param headers  Optional request headers (e.g. Range, Authorization)
     * @return Response body bytes
     * @throws std::runtime_error on network error, timeout, or HTTP 4xx/5xx
     */
    std::vector<uint8_t> get(
        const std::string& url,
        const std::map<std::string, std::string>& headers = {}
    );

    /**
     * Perform HTTP HEAD request.
     *
     * @param url  URL to query
     * @return Response headers map (lower-case keys)
     * @throws std::runtime_error on network error or timeout
     */
    std::map<std::string, std::string> head(const std::string& url);

private:
    void* multi_handle_;        // CURLM* — opaque to keep curl.h out of this header
    long  timeout_ms_;
    std::string user_agent_;
    std::string ca_bundle_;     // Path to CA bundle found at init time

    /**
     * Probe common CA bundle locations and return the first readable one.
     * Returns empty string if none found (curl uses its built-in default).
     */
    static std::string find_ca_bundle();

    /**
     * Apply CA bundle to a CURL easy handle if we found one.
     */
    void configure_ssl(void* easy_handle) const;

    /**
     * Run a single easy handle to completion via CURLM and return result code.
     * Removes the handle from multi on return (success or failure).
     */
    int perform(void* easy_handle);
};
