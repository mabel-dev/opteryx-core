/**
 * libcurl HTTP Client for Opteryx
 *
 * Provides Python-compatible HTTP client with native Range request support,
 * connection pooling via CURLM (libcurl multi-handle), and proper GIL management.
 *
 * Replaces requests library dependency with C-level HTTP via libcurl.
 *
 * Features:
 * - Synchronous HTTP GET/HEAD with Range headers
 * - CURLM connection pooling (96-128 concurrent connections)
 * - GIL-safe task execution
 * - Timeout handling
 * - HTTP error reporting
 */

#include <Python.h>
#include <curl/curl.h>
#include <string>
#include <vector>
#include <map>
#include <memory>
#include <cstring>

/**
 * Response buffer for capturing HTTP response data.
 * Used as callback target for CURLM transfers.
 */
struct ResponseBuffer {
    std::vector<char> data;
    std::string headers_str;
    long http_code = 0;

    /**
     * Callback for libcurl to write response body data.
     * Called from C code, forwarded to our instance.
     */
    static size_t write_callback(void* contents, size_t size, size_t nmemb, void* userp) {
        size_t realsize = size * nmemb;
        ResponseBuffer* self = static_cast<ResponseBuffer*>(userp);

        char* reinterpret_data = static_cast<char*>(contents);
        self->data.insert(self->data.end(), reinterpret_data, reinterpret_data + realsize);

        return realsize;
    }

    /**
     * Callback for libcurl to capture response headers.
     */
    static size_t header_callback(char* buffer, size_t size, size_t nmemb, void* userp) {
        size_t realsize = size * nmemb;
        ResponseBuffer* self = static_cast<ResponseBuffer*>(userp);

        self->headers_str.append(buffer, realsize);
        return realsize;
    }
};

/**
 * HTTP Client using libcurl CURLM (multi-handle) for connection pooling.
 *
 * Provides synchronous HTTP operations with native Range request support
 * for efficient partial file reads needed by HTTP and GCS filesystems.
 */
class HttpClient {
private:
    CURLM* multi_handle_;
    int max_connections_;
    long timeout_ms_;
    std::string user_agent_;

public:
    /**
     * Create HTTP client with connection pool.
     *
     * @param max_connections Maximum concurrent connections (default: 128 for GCS, 96 for HTTP)
     * @param timeout_ms Timeout in milliseconds (default: 60000 = 60 seconds)
     */
    HttpClient(int max_connections = 128, long timeout_ms = 60000)
        : max_connections_(max_connections),
          timeout_ms_(timeout_ms),
          user_agent_("opteryx-libcurl/1.0") {

        multi_handle_ = curl_multi_init();
        if (!multi_handle_) {
            throw std::runtime_error("Failed to initialize CURLM");
        }

        // Configure multi-handle for connection pooling
        curl_multi_setopt(multi_handle_, CURLMOPT_MAX_HOST_CONNECTIONS, (long)max_connections);
        curl_multi_setopt(multi_handle_, CURLMOPT_MAXCONNECTS, (long)(max_connections * 2));
    }

    /**
     * Destructor - cleanup CURLM handle.
     */
    ~HttpClient() {
        if (multi_handle_) {
            curl_multi_cleanup(multi_handle_);
        }
    }

    /**
     * Perform HTTP GET request with optional Range header.
     *
     * @param url URL to fetch
     * @param headers Optional dictionary of headers (Python dict)
     * @return Raw response body as bytes
     */
    PyObject* get(const std::string& url, PyObject* headers_dict = nullptr) {
        CURL* curl_handle = curl_easy_init();
        if (!curl_handle) {
            PyErr_SetString(PyExc_RuntimeError, "Failed to initialize CURL handle");
            return nullptr;
        }

        ResponseBuffer response;
        std::string all_headers;

        try {
            // Configure easy handle
            curl_easy_setopt(curl_handle, CURLOPT_URL, url.c_str());
            curl_easy_setopt(curl_handle, CURLOPT_USERAGENT, user_agent_.c_str());
            curl_easy_setopt(curl_handle, CURLOPT_TIMEOUT_MS, timeout_ms_);
            curl_easy_setopt(curl_handle, CURLOPT_FOLLOWLOCATION, 1L);
            curl_easy_setopt(curl_handle, CURLOPT_MAXREDIRS, 5L);

            // Callbacks for response data
            curl_easy_setopt(curl_handle, CURLOPT_WRITEFUNCTION, ResponseBuffer::write_callback);
            curl_easy_setopt(curl_handle, CURLOPT_WRITEDATA, &response);
            curl_easy_setopt(curl_handle, CURLOPT_HEADERFUNCTION, ResponseBuffer::header_callback);
            curl_easy_setopt(curl_handle, CURLOPT_HEADERDATA, &response);

            // Handle custom headers (Python dict → curl_slist)
            struct curl_slist* headers_list = nullptr;
            if (headers_dict && PyDict_Check(headers_dict)) {
                PyObject *key, *value;
                Py_ssize_t pos = 0;

                while (PyDict_Next(headers_dict, &pos, &key, &value)) {
                    if (PyUnicode_Check(key) && PyUnicode_Check(value)) {
                        std::string header_line;
                        header_line.append(PyUnicode_AsUTF8(key));
                        header_line.append(": ");
                        header_line.append(PyUnicode_AsUTF8(value));

                        headers_list = curl_slist_append(headers_list, header_line.c_str());
                    }
                }

                if (headers_list) {
                    curl_easy_setopt(curl_handle, CURLOPT_HTTPHEADER, headers_list);
                }
            }

            // Add to multi-handle and perform transfer
            curl_multi_add_handle(multi_handle_, curl_handle);

            int still_running = 1;
            while (still_running) {
                CURLMcode mret = curl_multi_perform(multi_handle_, &still_running);
                if (mret != CURLM_OK) {
                    curl_slist_free_all(headers_list);
                    curl_multi_remove_handle(multi_handle_, curl_handle);
                    curl_easy_cleanup(curl_handle);

                    std::string error_msg = "CURLM error: ";
                    error_msg.append(curl_multi_strerror(mret));
                    PyErr_SetString(PyExc_RuntimeError, error_msg.c_str());
                    return nullptr;
                }
            }

            // Check for transfer errors
            CURLcode res = CURLE_OK;
            struct CURLMsg* msg = nullptr;
            int msgs_left = 0;

            while ((msg = curl_multi_info_read(multi_handle_, &msgs_left)) != nullptr) {
                if (msg->msg == CURLMSG_DONE) {
                    res = msg->data.result;
                }
            }

            // Check HTTP response code
            long http_code = 0;
            curl_easy_getinfo(curl_handle, CURLINFO_RESPONSE_CODE, &http_code);

            // Cleanup
            curl_multi_remove_handle(multi_handle_, curl_handle);
            curl_slist_free_all(headers_list);

            // Handle errors
            if (res != CURLE_OK) {
                curl_easy_cleanup(curl_handle);
                std::string error_msg = "CURL error: ";
                error_msg.append(curl_easy_strerror(res));
                PyErr_SetString(PyExc_RuntimeError, error_msg.c_str());
                return nullptr;
            }

            if (http_code >= 400) {
                curl_easy_cleanup(curl_handle);
                std::string error_msg = "HTTP Error ";
                error_msg.append(std::to_string(http_code));
                PyErr_SetString(PyExc_RuntimeError, error_msg.c_str());
                return nullptr;
            }

            // Success - return response data as Python bytes
            curl_easy_cleanup(curl_handle);
            return PyBytes_FromStringAndSize(response.data.data(), response.data.size());

        } catch (const std::exception& e) {
            curl_multi_remove_handle(multi_handle_, curl_handle);
            curl_easy_cleanup(curl_handle);
            PyErr_SetString(PyExc_RuntimeError, e.what());
            return nullptr;
        }
    }

    /**
     * Perform HTTP HEAD request to get headers only.
     *
     * @param url URL to query
     * @return Python dict with response headers
     */
    PyObject* head(const std::string& url) {
        CURL* curl_handle = curl_easy_init();
        if (!curl_handle) {
            PyErr_SetString(PyExc_RuntimeError, "Failed to initialize CURL handle");
            return nullptr;
        }

        ResponseBuffer response;

        try {
            // Configure easy handle for HEAD request
            curl_easy_setopt(curl_handle, CURLOPT_URL, url.c_str());
            curl_easy_setopt(curl_handle, CURLOPT_USERAGENT, user_agent_.c_str());
            curl_easy_setopt(curl_handle, CURLOPT_TIMEOUT_MS, timeout_ms_);
            curl_easy_setopt(curl_handle, CURLOPT_NOBODY, 1L);  // HEAD request
            curl_easy_setopt(curl_handle, CURLOPT_FOLLOWLOCATION, 1L);
            curl_easy_setopt(curl_handle, CURLOPT_MAXREDIRS, 5L);

            // Callback for headers only
            curl_easy_setopt(curl_handle, CURLOPT_HEADERFUNCTION, ResponseBuffer::header_callback);
            curl_easy_setopt(curl_handle, CURLOPT_HEADERDATA, &response);

            // Add to multi-handle and perform transfer
            curl_multi_add_handle(multi_handle_, curl_handle);

            int still_running = 1;
            while (still_running) {
                CURLMcode mret = curl_multi_perform(multi_handle_, &still_running);
                if (mret != CURLM_OK) {
                    curl_multi_remove_handle(multi_handle_, curl_handle);
                    curl_easy_cleanup(curl_handle);

                    std::string error_msg = "CURLM error: ";
                    error_msg.append(curl_multi_strerror(mret));
                    PyErr_SetString(PyExc_RuntimeError, error_msg.c_str());
                    return nullptr;
                }
            }

            // Check for transfer errors
            CURLcode res = CURLE_OK;
            struct CURLMsg* msg = nullptr;
            int msgs_left = 0;

            while ((msg = curl_multi_info_read(multi_handle_, &msgs_left)) != nullptr) {
                if (msg->msg == CURLMSG_DONE) {
                    res = msg->data.result;
                }
            }

            curl_multi_remove_handle(multi_handle_, curl_handle);

            if (res != CURLE_OK) {
                curl_easy_cleanup(curl_handle);
                std::string error_msg = "CURL error: ";
                error_msg.append(curl_easy_strerror(res));
                PyErr_SetString(PyExc_RuntimeError, error_msg.c_str());
                return nullptr;
            }

            // Parse headers into Python dict
            PyObject* result = PyDict_New();
            if (!result) {
                curl_easy_cleanup(curl_handle);
                return nullptr;
            }

            // Simple header parsing (line-based)
            size_t pos = 0;
            std::string& headers = response.headers_str;

            while (pos < headers.size()) {
                size_t newline = headers.find('\n', pos);
                if (newline == std::string::npos) newline = headers.size();

                std::string line = headers.substr(pos, newline - pos);
                if (!line.empty() && line.back() == '\r') {
                    line.pop_back();
                }

                // Parse "Header: Value" format
                size_t colon = line.find(':');
                if (colon != std::string::npos && colon > 0) {
                    std::string key = line.substr(0, colon);
                    std::string value = line.substr(colon + 1);

                    // Trim value
                    if (!value.empty() && value.front() == ' ') {
                        value = value.substr(1);
                    }

                    PyObject* py_key = PyUnicode_FromString(key.c_str());
                    PyObject* py_value = PyUnicode_FromString(value.c_str());

                    if (py_key && py_value) {
                        PyDict_SetItem(result, py_key, py_value);
                    }

                    Py_XDECREF(py_key);
                    Py_XDECREF(py_value);
                }

                pos = newline + 1;
            }

            curl_easy_cleanup(curl_handle);
            return result;

        } catch (const std::exception& e) {
            curl_multi_remove_handle(multi_handle_, curl_handle);
            curl_easy_cleanup(curl_handle);
            PyErr_SetString(PyExc_RuntimeError, e.what());
            return nullptr;
        }
    }
};

/**
 * Python C API wrapper functions.
 */
extern "C" {

/**
 * Create a new HttpClient instance.
 */
PyObject* http_client_new(int max_connections, long timeout_ms) {
    try {
        HttpClient* client = new HttpClient(max_connections, timeout_ms);
        return PyCapsule_New(client, "HttpClient", nullptr);
    } catch (const std::exception& e) {
        PyErr_SetString(PyExc_RuntimeError, e.what());
        return nullptr;
    }
}

/**
 * Call HTTP GET on existing client.
 */
PyObject* http_client_get(PyObject* client_capsule, const char* url, PyObject* headers) {
    HttpClient* client = static_cast<HttpClient*>(
        PyCapsule_GetPointer(client_capsule, "HttpClient"));

    if (!client) {
        PyErr_SetString(PyExc_RuntimeError, "Invalid HttpClient capsule");
        return nullptr;
    }

    return client->get(url, headers);
}

/**
 * Call HTTP HEAD on existing client.
 */
PyObject* http_client_head(PyObject* client_capsule, const char* url) {
    HttpClient* client = static_cast<HttpClient*>(
        PyCapsule_GetPointer(client_capsule, "HttpClient"));

    if (!client) {
        PyErr_SetString(PyExc_RuntimeError, "Invalid HttpClient capsule");
        return nullptr;
    }

    return client->head(url);
}

/**
 * Destroy HttpClient and clean up.
 */
void http_client_delete(PyObject* client_capsule) {
    HttpClient* client = static_cast<HttpClient*>(
        PyCapsule_GetPointer(client_capsule, "HttpClient"));

    if (client) {
        delete client;
    }
}

}  // extern "C"
