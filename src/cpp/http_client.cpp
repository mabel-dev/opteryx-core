/**
 * Pure C++ HTTP client implementation.
 *
 * No Python.h. No PyObject. No PyErr_SetString.
 * Errors are std::runtime_error. The Cython layer translates to Python.
 */

#include "http_client.hpp"

#include <curl/curl.h>
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

HttpClient::HttpClient(int max_connections, long timeout_ms)
    : multi_handle_(nullptr),
      timeout_ms_(timeout_ms),
      user_agent_("opteryx/1.0"),
      ca_bundle_(find_ca_bundle()) {

    CURLM* multi = curl_multi_init();
    if (!multi) throw std::runtime_error("curl_multi_init() failed");

    curl_multi_setopt(multi, CURLMOPT_MAX_HOST_CONNECTIONS, (long)max_connections);
    curl_multi_setopt(multi, CURLMOPT_MAXCONNECTS,          (long)(max_connections * 2));

    multi_handle_ = multi;
}

HttpClient::~HttpClient() {
    if (multi_handle_) {
        curl_multi_cleanup(static_cast<CURLM*>(multi_handle_));
        multi_handle_ = nullptr;
    }
}

int HttpClient::perform(void* easy_handle) {
    CURLM* multi = static_cast<CURLM*>(multi_handle_);
    CURL*  easy  = static_cast<CURL*>(easy_handle);

    curl_multi_add_handle(multi, easy);

    int running = 1;
    while (running) {
        CURLMcode mc = curl_multi_perform(multi, &running);
        if (mc != CURLM_OK) {
            curl_multi_remove_handle(multi, easy);
            throw std::runtime_error(
                std::string("curl_multi_perform: ") + curl_multi_strerror(mc));
        }
        if (running) curl_multi_wait(multi, nullptr, 0, 100, nullptr);
    }

    // Harvest result code
    CURLcode res = CURLE_OK;
    int msgs_left = 0;
    CURLMsg* msg;
    while ((msg = curl_multi_info_read(multi, &msgs_left)) != nullptr) {
        if (msg->msg == CURLMSG_DONE) res = msg->data.result;
    }

    curl_multi_remove_handle(multi, easy);
    return static_cast<int>(res);
}

std::vector<uint8_t> HttpClient::get(
    const std::string& url,
    const std::map<std::string, std::string>& headers)
{
    CURL* easy = curl_easy_init();
    if (!easy) throw std::runtime_error("curl_easy_init() failed");

    ResponseBuffer buf;

    curl_easy_setopt(easy, CURLOPT_URL,             url.c_str());
    curl_easy_setopt(easy, CURLOPT_USERAGENT,       user_agent_.c_str());
    curl_easy_setopt(easy, CURLOPT_TIMEOUT_MS,      timeout_ms_);
    curl_easy_setopt(easy, CURLOPT_FOLLOWLOCATION,  1L);
    curl_easy_setopt(easy, CURLOPT_MAXREDIRS,       5L);
    curl_easy_setopt(easy, CURLOPT_WRITEFUNCTION,   ResponseBuffer::write_body);
    curl_easy_setopt(easy, CURLOPT_WRITEDATA,       &buf);
    configure_ssl(easy);

    // Build custom header list
    struct curl_slist* hlist = nullptr;
    for (const auto& kv : headers) {
        std::string line = kv.first + ": " + kv.second;
        hlist = curl_slist_append(hlist, line.c_str());
    }
    if (hlist) curl_easy_setopt(easy, CURLOPT_HTTPHEADER, hlist);

    CURLcode res;
    try {
        res = static_cast<CURLcode>(perform(easy));
    } catch (...) {
        curl_slist_free_all(hlist);
        curl_easy_cleanup(easy);
        throw;
    }

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

std::map<std::string, std::string> HttpClient::head(const std::string& url) {
    CURL* easy = curl_easy_init();
    if (!easy) throw std::runtime_error("curl_easy_init() failed");

    ResponseBuffer buf;

    curl_easy_setopt(easy, CURLOPT_URL,             url.c_str());
    curl_easy_setopt(easy, CURLOPT_USERAGENT,       user_agent_.c_str());
    curl_easy_setopt(easy, CURLOPT_TIMEOUT_MS,      timeout_ms_);
    curl_easy_setopt(easy, CURLOPT_NOBODY,          1L);  // HEAD
    curl_easy_setopt(easy, CURLOPT_FOLLOWLOCATION,  1L);
    curl_easy_setopt(easy, CURLOPT_MAXREDIRS,       5L);
    curl_easy_setopt(easy, CURLOPT_HEADERFUNCTION,  ResponseBuffer::write_headers);
    curl_easy_setopt(easy, CURLOPT_HEADERDATA,      &buf);
    configure_ssl(easy);

    CURLcode res;
    try {
        res = static_cast<CURLcode>(perform(easy));
    } catch (...) {
        curl_easy_cleanup(easy);
        throw;
    }

    curl_easy_cleanup(easy);

    if (res != CURLE_OK) {
        throw std::runtime_error(
            std::string("CURL error: ") + curl_easy_strerror(res));
    }

    return parse_headers(buf.headers_raw);
}
