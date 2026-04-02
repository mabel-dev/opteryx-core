# cython: language_level=3, boundscheck=False, wraparound=False

"""Cython bindings for the C++ HTTP client.

This is a pure translation layer. It:
  - Converts Python str/dict/list arguments to C++ std::string/std::map/std::vector
  - Releases the GIL for all network calls (with nogil:) so Python threads
    are not blocked while C++ does network I/O
  - Lets Cython's `except +` automatically translate std::runtime_error
    into Python RuntimeError — no manual PyErr_SetString anywhere

The C++ layer (http_client.hpp / http_client.cpp) has no knowledge of Python.
"""

from libcpp.map    cimport map as cpp_map
from libcpp.pair   cimport pair
from libcpp.string cimport string
from libcpp.vector cimport vector


cdef extern from "http_client.hpp":
    # Alias the C++ class as CHttpClient to avoid collision with the Python cdef class below
    cdef cppclass CHttpClient "HttpClient":
        CHttpClient(int max_connections, long timeout_ms) except +
        vector[unsigned char] get(
            string url,
            cpp_map[string, string] headers
        ) except + nogil
        cpp_map[string, string] head(
            string url,
            cpp_map[string, string] headers
        ) except + nogil
        vector[vector[unsigned char]] get_many(
            vector[pair[string, cpp_map[string, string]]] requests
        ) except + nogil


cdef class HttpClient:
    """Python wrapper for the C++ HTTP client.

    Provides synchronous HTTP GET/HEAD with connection pooling via libcurl.

    Thread safety:
      - get() / head(): thread-safe via curl_easy_perform(). Each call owns its
        own CURL* easy handle. A CURLSH* share handle provides shared
        connection/DNS cache across threads.
      - get_many(): runs all N transfers concurrently on the calling thread via
        a local CURLM event loop. GIL is released for the entire batch.

    Example:
        client = HttpClient(max_connections=128, timeout_ms=60000)
        data = client.get("https://example.com/file.parquet",
                          headers={"Range": "bytes=0-1023"})
        chunks = client.get_many([
            ("https://example.com/a.parquet", {"Range": "bytes=0-999"}),
            ("https://example.com/b.parquet", {"Range": "bytes=100-199"}),
        ])
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
        """HTTP GET. Returns bytes. Raises RuntimeError on failure.

        GIL is released for the network call — safe to call from many threads.
        """
        if self._closed:
            raise RuntimeError("HttpClient is closed")

        # Convert Python types to C++ before releasing the GIL
        cdef string c_url = url.encode('utf-8')
        cdef cpp_map[string, string] cpp_headers
        if headers:
            for k, v in headers.items():
                cpp_headers[<string>k.encode('utf-8')] = <string>v.encode('utf-8')

        cdef vector[unsigned char] result
        with nogil:
            result = self._client.get(c_url, cpp_headers)
        return bytes(result)

    def head(self, str url, dict headers=None):
        """HTTP HEAD. Returns dict of response headers. Raises RuntimeError on failure.

        GIL is released for the network call — safe to call from many threads.
        """
        if self._closed:
            raise RuntimeError("HttpClient is closed")

        cdef string c_url = url.encode('utf-8')
        cdef cpp_map[string, string] c_headers
        cdef cpp_map[string, string] result
        if headers:
            for k, v in headers.items():
                c_headers[<string>k.encode('utf-8')] = <string>v.encode('utf-8')
        with nogil:
            result = self._client.head(c_url, c_headers)
        return {k.decode('utf-8'): v.decode('utf-8') for k, v in result}

    def get_many(self, list requests):
        """Batch HTTP GET. Returns list of bytes in the same order as requests.

        Args:
            requests: list of (url: str, headers: dict) tuples

        GIL is released for the entire batch — all N transfers run concurrently
        in C++ via a local CURLM event loop. No Python thread-pool overhead.
        """
        if self._closed:
            raise RuntimeError("HttpClient is closed")

        # Convert all Python arguments to C++ types before releasing the GIL
        cdef vector[pair[string, cpp_map[string, string]]] cpp_requests
        cdef pair[string, cpp_map[string, string]] cpp_req
        cdef cpp_map[string, string] cpp_headers

        for url, headers in requests:
            cpp_req.first = <string>(<bytes>url.encode('utf-8'))
            cpp_headers.clear()
            if headers:
                for k, v in headers.items():
                    cpp_headers[<string>k.encode('utf-8')] = <string>v.encode('utf-8')
            cpp_req.second = cpp_headers
            cpp_requests.push_back(cpp_req)

        cdef vector[vector[unsigned char]] results
        with nogil:
            results = self._client.get_many(cpp_requests)

        return [bytes(r) for r in results]

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
