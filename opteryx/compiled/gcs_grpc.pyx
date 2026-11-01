# cython: language_level=3
# cython: boundscheck=False
# cython: wraparound=False

"""Cython bindings for the native GCS gRPC reader (src/cpp/gcs_grpc).

A translation layer for tests only (design R5): Phase 1 connects through
Google's front end, which no engine path uses. The engine reaches this client
natively in Phase 2, never through Python.
"""

from libc.stdint cimport int64_t, uint8_t, uint32_t
from libcpp.memory cimport unique_ptr
from libcpp.string cimport string
from libcpp.vector cimport vector


class GcsGrpcError(RuntimeError):
    """A failed gRPC read; `.retryable` and `.grpc_status` as the C++ error carries them."""

    def __init__(self, message, bint retryable, int grpc_status):
        super().__init__(message)
        self.retryable = retryable
        self.grpc_status = grpc_status


cdef extern from *:
    """
    #include <string>
    #include <stdexcept>
    #include "gcs_grpc/gcs_grpc_client.hpp"

    // Runs inside the catch(...) Cython generates for `except +translator`.
    static void opteryx_gcs_grpc_error_info(std::string& message, int& retryable, int& status) {
        try {
            throw;
        } catch (const gcs_grpc::GcsGrpcError& err) {
            message = err.what();
            retryable = err.retryable;
            status = err.grpc_status;
        }
    }
    """
    void opteryx_gcs_grpc_error_info(string& message, int& retryable, int& status)


cdef void _raise_gcs_grpc_error() except *:
    cdef string message
    cdef int retryable = 0
    cdef int status = 0
    opteryx_gcs_grpc_error_info(message, retryable, status)
    raise GcsGrpcError(message.decode("utf-8", "replace"), retryable, status)


cdef extern from "gcs_grpc/crc32c.hpp" namespace "gcs_grpc":
    uint32_t crc32c(const uint8_t* p, size_t n) nogil


cdef extern from "gcs_grpc/gcs_grpc_client.hpp" namespace "gcs_grpc":
    string parse_read_object_body(const string& body, int64_t length, size_t chunk,
                                  string& data, int64_t& generation) except +

    cdef cppclass ByteRange:
        int64_t offset
        int64_t length

    cdef cppclass Options "gcs_grpc::GcsGrpcClient::Options":
        string host
        int port
        int connections
        string ca_file

    cdef cppclass CGcsGrpcClient "gcs_grpc::GcsGrpcClient":
        CGcsGrpcClient(Options) except +
        vector[vector[uint8_t]] read_many(const string& bucket, const string& obj,
                                          const vector[ByteRange]& ranges,
                                          const string& authorization, int64_t timeout_ms,
                                          int64_t generation, int64_t* out_generation) except +_raise_gcs_grpc_error nogil


def crc32c_of(const uint8_t[::1] data):
    """CRC32C of `data` (bytes-like)."""
    if data.shape[0] == 0:
        return crc32c(NULL, 0)
    return crc32c(&data[0], data.shape[0])


def parse_read_object_body_for_test(bytes body, int64_t length, size_t chunk):
    """Runs the reader's streaming parser over a ReadObject body; returns (error, data, generation)."""
    cdef string data
    cdef int64_t generation = 0
    cdef string err = parse_read_object_body(body, length, chunk, data, generation)
    return err.decode("utf-8", "replace"), bytes(data), generation


cdef class GcsGrpcClient:
    cdef unique_ptr[CGcsGrpcClient] _client

    def __cinit__(self, str host="storage.googleapis.com", int port=443, int connections=8, str ca_file=""):
        cdef Options o
        o.host = host.encode()
        o.port = port
        o.connections = connections
        o.ca_file = ca_file.encode()
        self._client.reset(new CGcsGrpcClient(o))

    def read_many(self, str bucket, str obj, list ranges, str authorization,
                  int64_t timeout_ms=60000, int64_t generation=0):
        """Read [(offset, length), ...] of gs://bucket/obj; returns (list[bytes], generation)."""
        cdef vector[ByteRange] cr
        cdef ByteRange r
        for off, ln in ranges:
            r.offset = off
            r.length = ln
            cr.push_back(r)
        cdef string b = bucket.encode()
        cdef string o = obj.encode()
        cdef string a = authorization.encode()
        cdef int64_t got_generation = 0
        cdef vector[vector[uint8_t]] out
        with nogil:
            out = self._client.get().read_many(b, o, cr, a, timeout_ms, generation, &got_generation)
        cdef list buffers = []
        cdef size_t i
        for i in range(out.size()):
            buffers.append((<const char*>out[i].data())[:out[i].size()])
        return buffers, got_generation
