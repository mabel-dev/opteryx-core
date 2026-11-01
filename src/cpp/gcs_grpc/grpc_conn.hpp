// One gRPC-over-HTTP/2 connection (design C1): a TCP socket, a TLS session
// (Phase 1; Phase 2 swaps in ALTS), and an nghttp2 client session, all owned by
// ONE native I/O thread (design R3). Other threads only enqueue work for it.
//
// The connection handles the HTTP/2 side of gRPC: `content-type:
// application/grpc`, `te: trailers`, and grpc-status / grpc-message from
// trailers (or a trailers-only response). Response bodies reach the handler as
// raw bytes, still carrying the 5-byte message prefixes, so a handler can parse
// as bytes arrive and write payload straight to its destination.
#pragma once

#include <atomic>
#include <cstdint>
#include <deque>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

struct nghttp2_session;
typedef struct ssl_st SSL;
typedef struct ssl_ctx_st SSL_CTX;

namespace gcs_grpc {

// gRPC status codes the reader classifies.
enum GrpcStatus : int {
    kOk = 0, kCancelled = 1, kUnknown = 2, kInvalidArgument = 3, kDeadlineExceeded = 4,
    kNotFound = 5, kPermissionDenied = 7, kResourceExhausted = 8, kFailedPrecondition = 9,
    kAborted = 10, kOutOfRange = 11, kInternal = 13, kUnavailable = 14, kDataLoss = 15, kUnauthenticated = 16,
};

// Callbacks for one call. Invoked ONLY on the connection's I/O thread. on_close
// is called exactly once per started call, and nothing is called after it, so
// a handler may be destroyed as soon as on_close has returned.
class CallHandler {
public:
    virtual ~CallHandler() = default;
    // The next bytes of the response body (gRPC-framed). Return false to
    // abandon the call: its stream is reset and on_close follows with
    // kCancelled (the handler keeps its own record of why).
    virtual bool on_body(const uint8_t* p, size_t n) = 0;
    // `status` is the grpc-status, or kUnavailable for a transport failure.
    virtual void on_close(int status, const std::string& message) = 0;
};

using Metadata = std::vector<std::pair<std::string, std::string>>;

class GrpcConnection {
public:
    // Connects, completes TLS (ALPN h2, hostname verified against `host`) and
    // starts the I/O thread. Throws std::runtime_error on any failure.
    GrpcConnection(const std::string& host, int port, const std::string& ca_file);
    ~GrpcConnection();

    GrpcConnection(const GrpcConnection&) = delete;
    GrpcConnection& operator=(const GrpcConnection&) = delete;

    // Thread-safe. Starts a call to `path` (e.g. "/google.storage.v2.Storage/BidiReadObject")
    // sending `body` (already gRPC-framed messages) and then half-closing. Returns a
    // call id for cancel(). If the connection is dead, `handler->on_close` is
    // still called (on the I/O thread or, if it has exited, before this returns).
    uint64_t start_call(const std::string& path, Metadata metadata, std::string body, CallHandler* handler);

    // Thread-safe. Resets the call's stream; on_close follows with kCancelled.
    void cancel(uint64_t call_id);

    bool alive() const { return alive_.load(std::memory_order_acquire); }
    size_t open_calls() const { return open_calls_.load(std::memory_order_relaxed); }

    // gRPC message framing (5-byte prefix, uncompressed).
    static void frame_message(std::string& out, const std::string& message);

private:
    struct Call;
    struct Op {
        enum Kind { kStart, kCancel } kind;
        uint64_t call_id;
        std::unique_ptr<Call> call;  // kStart only
    };

    void io_loop();
    void wake();
    void submit_pending();
    void fail_all(const std::string& why);
    void drain_ops_failing(const std::string& why);

    // nghttp2 callbacks (static trampolines in the .cpp reach these).
    friend struct Nghttp2Callbacks;
    long send_bytes(const uint8_t* data, size_t n);
    int on_header(int32_t stream_id, const std::string& name, const std::string& value);
    int on_data(int32_t stream_id, const uint8_t* data, size_t n);
    int on_stream_close(int32_t stream_id, uint32_t error_code);
    void on_goaway() { alive_.store(false, std::memory_order_release); }
    long read_body(int32_t stream_id, uint8_t* buf, size_t len, uint32_t* flags);

    std::string host_;
    int fd_ = -1;
    int wake_r_ = -1, wake_w_ = -1;
    SSL_CTX* ctx_ = nullptr;
    SSL* ssl_ = nullptr;
    nghttp2_session* session_ = nullptr;
    bool want_write_ = false;  // TLS needs the socket writable to make progress

    std::mutex ops_mu_;
    std::deque<Op> ops_;
    std::atomic<bool> stop_{false};
    std::atomic<bool> alive_{true};
    std::atomic<size_t> open_calls_{0};
    std::atomic<uint64_t> next_call_id_{1};

    // I/O thread only.
    std::unordered_map<int32_t, std::unique_ptr<Call>> by_stream_;
    std::unordered_map<uint64_t, int32_t> stream_of_call_;
    std::thread io_;
};

}  // namespace gcs_grpc
