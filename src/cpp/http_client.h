#ifndef OPTERYX_HTTP_CLIENT_H
#define OPTERYX_HTTP_CLIENT_H

#include <Python.h>

#ifdef __cplusplus
extern "C" {
#endif

/**
 * Create a new HTTP client instance with connection pooling.
 *
 * @param max_connections Maximum number of concurrent connections (96-128 typical)
 * @param timeout_ms Timeout in milliseconds (default: 60000 = 60 seconds)
 * @return PyCapsule containing opaque HttpClient* pointer, or NULL on error
 */
PyObject* http_client_new(int max_connections, long timeout_ms);

/**
 * Perform HTTP GET request with optional custom headers.
 *
 * @param client_capsule PyCapsule from http_client_new()
 * @param url URL to fetch (e.g., "https://example.com/file.bin")
 * @param headers Optional PyDict with HTTP headers (e.g., {"Range": "bytes=0-1023"})
 * @return PyBytes object containing response body, or NULL on error
 */
PyObject* http_client_get(PyObject* client_capsule, const char* url, PyObject* headers);

/**
 * Perform HTTP HEAD request to retrieve headers only.
 *
 * @param client_capsule PyCapsule from http_client_new()
 * @param url URL to query
 * @return PyDict with response headers, or NULL on error
 */
PyObject* http_client_head(PyObject* client_capsule, const char* url);

/**
 * Destroy HTTP client and cleanup connection pool.
 *
 * @param client_capsule PyCapsule from http_client_new()
 */
void http_client_delete(PyObject* client_capsule);

#ifdef __cplusplus
}
#endif

#endif /* OPTERYX_HTTP_CLIENT_H */
