// GCS object range reads over gRPC (design C5): one google.storage.v2
// ReadObject stream per range, all multiplexed on the pooled connections. Each
// response is parsed as its bytes arrive and the object data is written
// straight into the range's buffer; every chunk's CRC32C is verified.
// (BidiReadObject is refused for regional buckets — PERMISSION_DENIED "not yet
// available for general use" — so it is not used.)
//
// One client per process (design R3) owns a pool of GrpcConnections; a call
// goes to the live connection with the fewest open calls, and a dead
// connection is replaced on next use.
//
// Phase 1 connects over TLS to storage.googleapis.com (through Google's front
// end) — a test-only transport (design R5). Phase 2 adds ALTS + xDS discovery.
#pragma once

#include <cstdint>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <vector>

#include "grpc_conn.hpp"

namespace gcs_grpc {

// A failed read. `retryable` follows the same contract as HttpError::retryable.
struct GcsGrpcError : std::runtime_error {
    bool retryable;
    int grpc_status;
    GcsGrpcError(const std::string& what, bool retryable_, int status)
        : std::runtime_error(what), retryable(retryable_), grpc_status(status) {}
};

struct ByteRange {
    int64_t offset;
    int64_t length;  // > 0
};

// Test hook: feeds a ReadObject response body to the reader's parser in
// `chunk`-byte pieces, as the network would. Returns "" on success, else the
// parse error. `data` receives the range bytes.
std::string parse_read_object_body(const std::string& body, int64_t length, size_t chunk, std::string& data,
                                   int64_t& generation);

class GcsGrpcClient {
public:
    struct Options {
        std::string host = "storage.googleapis.com";
        int port = 443;
        int connections = 8;
        std::string ca_file;  // empty = OpenSSL default verify paths
    };

    explicit GcsGrpcClient(Options options);

    // Reads `ranges` of gs://bucket/object, returning one buffer per range in
    // order. `authorization` is the full header value ("Bearer ..."). With
    // `generation` > 0 the read is pinned to that generation; the generation
    // actually read is written to `*out_generation` when non-null. Blocks until
    // done or `timeout_ms` passes. Throws GcsGrpcError.
    std::vector<std::vector<uint8_t>> read_many(const std::string& bucket, const std::string& object,
                                                const std::vector<ByteRange>& ranges,
                                                const std::string& authorization, int64_t timeout_ms,
                                                int64_t generation = 0, int64_t* out_generation = nullptr);

private:
    std::shared_ptr<GrpcConnection> pick_connection();

    Options options_;
    std::mutex mu_;
    std::vector<std::shared_ptr<GrpcConnection>> pool_;
};

}  // namespace gcs_grpc
