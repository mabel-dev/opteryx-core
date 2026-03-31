# cython: language_level=3, boundscheck=False, wraparound=False

"""Cython bindings for the C++ HTTP client.

This is a pure translation layer. It:
  - Converts Python str/dict arguments to C++ std::string/std::map
  - Calls the C++ HttpClient methods
  - Converts C++ return types back to Python bytes/dict
  - Lets Cython's `except +` automatically translate std::runtime_error
    into Python RuntimeError — no manual PyErr_SetString anywhere

The C++ layer (http_client.hpp / http_client.cpp) has no knowledge of Python.
"""

from libcpp.map    cimport map as cpp_map
from libcpp.string cimport string
from libcpp.vector cimport vector


cdef extern from "http_client.hpp":
    # Alias the C++ class as CHttpClient to avoid collision with the Python cdef class below
    cdef cppclass CHttpClient "HttpClient":
        CHttpClient(int max_connections, long timeout_ms) except +
        vector[unsigned char] get(
            string url,
            cpp_map[string, string] headers
        ) except +
        cpp_map[string, string] head(string url) except +


cdef class HttpClient:
    """Python wrapper for the C++ HTTP client.

    Provides synchronous HTTP GET/HEAD with connection pooling via libcurl CURLM.

    Example:
        client = HttpClient(max_connections=128, timeout_ms=60000)
        data = client.get("https://example.com/file.parquet",
                          headers={"Range": "bytes=0-1023"})
        meta = client.head("https://example.com/file.parquet")
        client.close()
    """

    cdef CHttpClient* _client   # C++ object — no PyObject* anywhere
    cdef bint _closed

    def __init__(self, int max_connections=128, long timeout_ms=60000):
        # except + on the constructor means std::runtime_error → RuntimeError
        self._client = new CHttpClient(max_connections, timeout_ms)
        self._closed = False

    def get(self, str url, dict headers=None):
        """HTTP GET. Returns bytes. Raises RuntimeError on failure."""
        if self._closed:
            raise RuntimeError("HttpClient is closed")

        cdef cpp_map[string, string] cpp_headers
        if headers:
            for k, v in headers.items():
                cpp_headers[k.encode('utf-8')] = v.encode('utf-8')

        cdef vector[unsigned char] result = self._client.get(
            url.encode('utf-8'), cpp_headers
        )
        return bytes(result)

    def head(self, str url):
        """HTTP HEAD. Returns dict of response headers. Raises RuntimeError on failure."""
        if self._closed:
            raise RuntimeError("HttpClient is closed")

        cdef cpp_map[string, string] result = self._client.head(url.encode('utf-8'))
        return {k.decode('utf-8'): v.decode('utf-8') for k, v in result}

    def close(self):
        """Release connection pool. Safe to call multiple times."""
        if not self._closed and self._client != NULL:
            del self._client
            self._client = <CHttpClient*>NULL
            self._closed = True

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False

    def __dealloc__(self):
        if self._client != NULL and not self._closed:
            del self._client
            self._client = <CHttpClient*>NULL
