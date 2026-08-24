# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False
# cython: optimize.use_switch=True
# cython: optimize.unpack_method_calls=True

"""Cython bindings for the C++ HTTP client.

This is a pure translation layer. It:
  - Converts Python str/dict/list arguments to C++ std::string/std::map/std::vector
  - Releases the GIL for all network calls (with nogil:) so Python threads
    are not blocked while C++ does network I/O
  - Translates C++ exceptions into Python via `except +_raise_http_error`,
    which preserves HttpError's status code as HttpStatusError.status — no
    manual PyErr_SetString anywhere

The C++ layer (http_client.hpp / http_client.cpp) has no knowledge of Python.
"""

from libcpp.map    cimport map as cpp_map
from libcpp.pair   cimport pair
from libcpp.string cimport string
from libcpp.vector cimport vector


class HttpStatusError(RuntimeError):
    """A failed HTTP call, carrying the status code that caused it.

    Subclasses RuntimeError deliberately: every existing `except RuntimeError`
    around this client keeps working unchanged, and callers that need to
    classify a failure read `.status` instead of parsing the message. `.status`
    is 0 when the call never got a response at all (a CURL-level error), which
    is NOT the same as "no error" - reach it only from an except block.

    The message text is for humans and is not a contract; matching on it is how
    the S3 filesystem used to decide an object was missing.
    """

    def __init__(self, message, long status):
        super().__init__(message)
        self.status = status


cdef extern from *:
    """
    #include <string>
    #include <stdexcept>
    #include "http_client.hpp"

    // Runs inside the catch(...) block Cython generates for `except +translator`,
    // so the C++ exception is still in flight and `throw;` recovers its dynamic
    // type - the only way to read HttpError::http_status from here.
    static void opteryx_http_error_info(std::string& message, long& status) {
        try {
            throw;
        } catch (const HttpError& err) {
            message = err.what();
            status = err.http_status;
        } catch (const std::exception& err) {
            message = err.what();
            status = 0;
        } catch (...) {
            message = "unknown C++ exception";
            status = 0;
        }
    }
    """
    void opteryx_http_error_info(string& message, long& status)


cdef void _raise_http_error() except *:
    """Translate the in-flight C++ exception into HttpStatusError.

    Named in every `except +_raise_http_error` below, replacing Cython's default
    `except +` (which flattens everything to RuntimeError and drops the status).

    `except *` is load-bearing: it is what lets the Python exception raised here
    propagate. Declared `noexcept`, Cython discards it ("Exception ignored in")
    and the caller sees a bare "Error converting c++ exception" instead.
    """
    cdef string message
    cdef long status = 0
    opteryx_http_error_info(message, status)
    raise HttpStatusError(message.decode("utf-8", "replace"), status)


cdef extern from "http_client.hpp":
    # Alias the C++ class as CHttpClient to avoid collision with the Python cdef class below
    cdef cppclass CHttpClient "HttpClient":
        CHttpClient(int max_connections, long timeout_ms) except +_raise_http_error
        vector[unsigned char] get(
            string url,
            cpp_map[string, string] headers
        ) except +_raise_http_error nogil
        cpp_map[string, string] head(
            string url,
            cpp_map[string, string] headers
        ) except +_raise_http_error nogil
        vector[vector[unsigned char]] get_many(
            vector[pair[string, cpp_map[string, string]]] requests
        ) except +_raise_http_error nogil
        vector[cpp_map[string, string]] head_many(
            vector[pair[string, cpp_map[string, string]]] requests
        ) except +_raise_http_error nogil


cdef class HttpClient:
    """Python wrapper for the C++ HTTP client.

    Provides synchronous HTTP GET/HEAD with connection pooling via libcurl.

    Thread safety:
      - get() / head(): thread-safe via curl_easy_perform(). Each call owns its
        own CURL* easy handle. A CURLSH* share handle provides shared
        connection/DNS cache across threads.
      - get_many() / head_many(): run all N requests concurrently on the calling
        thread via a local CURLM event loop. GIL is released for the entire
        batch. Callers resolving many URLs at once (e.g. a manifest fan-out)
        MUST use these instead of looping a thread pool over get()/head() --
        that pattern forces each worker thread to cross back into the
        interpreter once per URL, which is off-limits outside the
        planning/execution hand-off.

    Example:
        client = HttpClient(max_connections=128, timeout_ms=60000)
        data = client.get("https://example.com/file.parquet",
                          headers={"Range": "bytes=0-1023"})
        chunks = client.get_many([
            ("https://example.com/a.parquet", {"Range": "bytes=0-999"}),
            ("https://example.com/b.parquet", {"Range": "bytes=100-199"}),
        ])
        meta = client.head("https://example.com/file.parquet")
        metas = client.head_many([
            ("https://example.com/a.parquet", {}),
            ("https://example.com/b.parquet", {}),
        ])
        client.close()
    """

    cdef CHttpClient* _client   # C++ object — no PyObject* anywhere
    cdef bint _closed

    def __init__(self, int max_connections=128, long timeout_ms=60000):
        # except +_raise_http_error on the constructor means a C++ throw here
        # surfaces as HttpStatusError (a RuntimeError), not a crash
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

    def head_many(self, list requests):
        """Batch HTTP HEAD. Returns list of header dicts, same order as requests.

        Args:
            requests: list of (url: str, headers: dict) tuples

        GIL is released for the entire batch -- all N HEAD requests run
        concurrently in C++ via a local CURLM event loop, same as get_many().
        This is the batch counterpart callers MUST use instead of dispatching
        per-path head() calls onto a Python-level thread pool.
        """
        if self._closed:
            raise RuntimeError("HttpClient is closed")

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

        cdef vector[cpp_map[string, string]] results
        with nogil:
            results = self._client.head_many(cpp_requests)

        return [
            {k.decode('utf-8'): v.decode('utf-8') for k, v in r}
            for r in results
        ]

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
