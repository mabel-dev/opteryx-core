# cython: language_level=3
# cython: boundscheck=False
# cython: wraparound=False

"""Cython bindings for the native GCS gRPC reader (src/cpp/gcs_grpc).

A translation layer for tests only (design R5): the front-end transport is
test-only, and the engine reaches the client natively, never through Python.
`direct_path=True` runs DirectPath (xDS + RLS discovery, ALTS) and works on GCE only.
"""

from libc.stdint cimport int64_t, uint8_t, uint32_t
from libcpp cimport bool as cbool
from libcpp.map cimport map as cmap
from libcpp.pair cimport pair
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
        cbool direct_path

    cdef cppclass CGcsGrpcClient "gcs_grpc::GcsGrpcClient":
        CGcsGrpcClient(Options) except +
        vector[vector[uint8_t]] read_many(const string& bucket, const string& obj,
                                          const vector[ByteRange]& ranges,
                                          const string& authorization, int64_t timeout_ms,
                                          int64_t generation, int64_t* out_generation) except +_raise_gcs_grpc_error nogil


cdef extern from "gcs_grpc/alts.hpp" namespace "gcs_grpc":
    string alts_client_start_request(const string& target_name) except +
    string alts_gcm_rekey_encrypt_for_test(const string& key_data, const string& nonce,
                                           const string& aad, const string& plaintext) except +
    string alts_socketpair_roundtrip_for_test(const string& key_data, const string& payload,
                                              size_t max_frame_size) except +


cdef extern from "gcs_grpc/directpath.hpp" namespace "gcs_grpc":
    cdef cppclass Endpoint:
        string ip
        int port

    cdef cppclass RlsConfig:
        string lookup_service
        int64_t max_age_s
        string default_target

    cdef cppclass RouteDecision:
        string cluster
        cbool use_rls
        RlsConfig rls

    RouteDecision parse_listener_route(const string& listener, const string& authority,
                                       const string& path) except +
    string parse_cluster_eds_service(const string& cluster) except +
    vector[Endpoint] parse_cluster_load_assignment(const string& cla) except +
    cmap[string, string] rls_key_map(const RlsConfig& cfg, const string& authority, const string& service,
                                     const string& method,
                                     const vector[pair[string, string]]& request_headers) except +
    string rls_request(const cmap[string, string]& key_map) except +
    vector[string] parse_rls_response(const string& response) except +


def alts_client_start_request_for_test(str target_name):
    """The serialized HandshakerReq{client_start} the ALTS handshake sends first."""
    return bytes(alts_client_start_request(target_name.encode()))


def alts_gcm_rekey_encrypt_for_test_(bytes key, bytes nonce, bytes aad, bytes plaintext):
    """Ciphertext + tag from the vendored AES-128-GCM rekeying crypter."""
    return bytes(alts_gcm_rekey_encrypt_for_test(key, nonce, aad, plaintext))


def alts_roundtrip_for_test(bytes key, bytes payload, size_t max_frame_size):
    """Round-trips `payload` both ways through two ALTS transports on a socketpair; "" on success."""
    return alts_socketpair_roundtrip_for_test(key, payload, max_frame_size).decode()


def parse_listener_route_for_test(bytes listener, str authority, str path, str bucket):
    """The route decision for `path`, and the RLS request for `bucket` when the route uses RLS."""
    cdef RouteDecision d = parse_listener_route(listener, authority.encode(), path.encode())
    cdef vector[pair[string, string]] headers
    cdef cmap[string, string] keys
    if not d.use_rls:
        return {"cluster": d.cluster.decode(), "use_rls": False}
    headers.push_back(pair[string, string](b"x-goog-request-params",
                                           ("bucket=projects%2F_%2Fbuckets%2F" + bucket).encode()))
    service, method = path.strip("/").split("/")
    keys = rls_key_map(d.rls, authority.encode(), service.encode(), method.encode(), headers)
    return {
        "cluster": d.cluster.decode(),
        "use_rls": True,
        "lookup_service": d.rls.lookup_service.decode(),
        "max_age_s": d.rls.max_age_s,
        "default_target": d.rls.default_target.decode(),
        "key_map": {k.first.decode(): k.second.decode() for k in keys},
        "rls_request": bytes(rls_request(keys)),
    }


def parse_cluster_for_test(bytes cluster):
    return parse_cluster_eds_service(cluster).decode()


def parse_cla_for_test(bytes cla):
    return [(e.ip.decode(), e.port) for e in parse_cluster_load_assignment(cla)]


def parse_rls_response_for_test(bytes response):
    return [t.decode() for t in parse_rls_response(response)]


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

    def __cinit__(self, str host="storage.googleapis.com", int port=443, int connections=8, str ca_file="",
                  bint direct_path=False):
        cdef Options o
        o.host = host.encode()
        o.port = port
        o.connections = connections
        o.ca_file = ca_file.encode()
        o.direct_path = direct_path
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
