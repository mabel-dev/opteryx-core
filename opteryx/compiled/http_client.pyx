# cython: language_level=3, boundscheck=False, wraparound=False

"""Cython bindings for libcurl HTTP client.

Provides Python wrapper for HTTP GET/HEAD operations with:
- Native libcurl CURLM (multi-handle) connection pooling
- Range request support for efficient byte-range reads
- Custom headers support (Authorization, Range, etc.)
- GIL-safe C++ integration

Used by:
- opteryx/connectors/io_systems/http_filesystem.py (HTTP Range requests)
- opteryx/connectors/io_systems/gcs_filesystem.py (GCS OAuth + Range requests)

Replaces requests library dependency with C-level HTTP for better performance
and zero external dependencies (only system libcurl).
"""

from cpython.ref cimport PyObject
from cpython.bytes cimport PyBytes_FromStringAndSize
from libc.stdint cimport int64_t

# C declarations for vendored libcurl HTTP client
# Linked against third_party/curl/lib/.libs/libcurl.a (see setup.py)
cdef extern from "http_client.h":
    PyObject* http_client_new(int max_connections, long timeout_ms) except NULL
    PyObject* http_client_get(PyObject* client_capsule, const char* url, PyObject* headers) except NULL
    PyObject* http_client_head(PyObject* client_capsule, const char* url) except NULL
    void http_client_delete(PyObject* client_capsule)


cdef class HttpClient:
    """Python wrapper for libcurl HTTP client with connection pooling.

    Provides synchronous HTTP GET/HEAD operations with:
    - CURLM multi-handle for connection pooling (96-128 concurrent connections)
    - Range request support (needed by HTTP and GCS filesystems)
    - Custom headers (Authorization, User-Agent, etc.)
    - GIL-safe C++ backend

    Example:
        client = HttpClient(max_connections=128, timeout_ms=60000)

        # GET with Range header for byte-range read
        data = client.get(
            "https://example.com/file.parquet",
            headers={"Range": "bytes=0-1023"}
        )

        # HEAD for metadata (file size, last-modified, etc.)
        headers = client.head("https://example.com/file.parquet")
        print(headers["Content-Length"])

        client.close()
    """

    cdef PyObject* _client
    cdef bint _closed

    def __init__(self, int max_connections=128, long timeout_ms=60000):
        """Initialize HTTP client with connection pool.

        Args:
            max_connections: Maximum concurrent connections (default: 128)
            timeout_ms: Timeout in milliseconds (default: 60000 = 60 seconds)

        Raises:
            RuntimeError: If client initialization fails
        """
        self._client = http_client_new(max_connections, timeout_ms)
        self._closed = False

        if not self._client:
            raise RuntimeError("Failed to initialize HTTP client")

    def get(self, str url, dict headers=None):
        """Perform HTTP GET request.

        Args:
            url: URL to fetch
            headers: Optional dictionary of HTTP headers
                     (e.g., {"Range": "bytes=0-1023", "Authorization": "Bearer token"})

        Returns:
            bytes: Response body

        Raises:
            RuntimeError: If request fails (network error, HTTP error, timeout)
        """
        if self._closed:
            raise RuntimeError("HttpClient is closed")

        cdef bytes url_bytes = url.encode('utf-8')
        cdef const char* c_url = url_bytes
        cdef PyObject* result
        cdef PyObject* c_headers = <PyObject*>headers if headers is not None else NULL

        # Call C function with Python GIL held
        # except NULL declaration above means Cython auto-propagates any C-set exception
        result = http_client_get(self._client, c_url, c_headers)
        return <object>result

    def head(self, str url):
        """Perform HTTP HEAD request to get headers only.

        Args:
            url: URL to query

        Returns:
            dict: Response headers (e.g., {"Content-Type": "...", "Content-Length": "..."})

        Raises:
            RuntimeError: If request fails (network error, HTTP error, timeout)
        """
        if self._closed:
            raise RuntimeError("HttpClient is closed")

        cdef bytes url_bytes = url.encode('utf-8')
        cdef const char* c_url = url_bytes
        cdef PyObject* result

        # Call C function with Python GIL held
        # except NULL declaration above means Cython auto-propagates any C-set exception
        result = http_client_head(self._client, c_url)
        return <object>result

    def close(self):
        """Close HTTP client and cleanup connection pool.

        Safe to call multiple times.
        """
        if not self._closed and self._client:
            http_client_delete(self._client)
            self._client = NULL
            self._closed = True

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()
        return False

    def __dealloc__(self):
        """Cleanup on garbage collection."""
        if self._client and not self._closed:
            http_client_delete(self._client)
            self._client = NULL
            self._closed = True
