// One gRPC-over-HTTP/2 connection (design C1): a Transport (TLS, plaintext or
// ALTS — transport.hpp) and an nghttp2 client session, both owned by ONE native
// I/O thread (design R3). Other threads only enqueue work for it.
//
// The connection handles the HTTP/2 side of gRPC: `content-type:
// application/grpc`, `te: trailers`, and grpc-status / grpc-message from
// trailers (or a trailers-only response). Response bodies reach the handler as
// raw bytes, still carrying the 5-byte message prefixes, so a handler can parse
// as bytes arrive and write payload straight to its destination.
#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
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

#include "transport.hpp"

struct nghttp2_session;

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
    // Takes over a connected, handshaken transport and starts the I/O thread.
    // `authority` is the ":authority" sent on every call. Throws
    // std::runtime_error on failure.
    GrpcConnection(std::unique_ptr<Transport> transport, std::string authority);
    ~GrpcConnection();

    GrpcConnection(const GrpcConnection&) = delete;
    GrpcConnection& operator=(const GrpcConnection&) = delete;

    // Thread-safe. Starts a call to `path` (e.g. "/google.storage.v2.Storage/ReadObject")
    // sending `body` (already gRPC-framed messages), then half-closing unless
    // `half_close` is false, in which case send() supplies the rest. Returns a
    // call id for cancel()/send(). If the connection is dead, `handler->on_close`
    // is still called (on the I/O thread or, if it has exited, before this returns).
    uint64_t start_call(const std::string& path, Metadata metadata, std::string body, CallHandler* handler,
                        bool half_close = true);

    // Thread-safe. Appends gRPC-framed messages to a call started with
    // half_close = false, half-closing after them if `half_close`. A call that
    // has already ended ignores it (its on_close has been or will be called).
    void send(uint64_t call_id, std::string body, bool half_close);

    // Thread-safe. Resets the call's stream; on_close follows with kCancelled.
    void cancel(uint64_t call_id);

    bool alive() const { return alive_.load(std::memory_order_acquire); }
    size_t open_calls() const { return open_calls_.load(std::memory_order_relaxed); }

    // gRPC message framing (5-byte prefix, uncompressed).
    static void frame_message(std::string& out, const std::string& message);

private:
    struct Call;
    struct Op {
        enum Kind { kStart, kCancel, kSend } kind;
        uint64_t call_id;
        std::unique_ptr<Call> call;  // kStart only
        std::string body;            // kSend only
        bool half_close = false;     // kSend only
    };

    void io_loop();
    void wake();
    void submit_pending();
    void fail_all(const std::string& why);
    void drain_ops_failing(const std::string& why);

    // nghttp2 callbacks (static trampolines in the .cpp reach these).
    friend struct Nghttp2Callbacks;
    long send_bytes(const uint8_t* data, size_t n);
    void drop_call(uint64_t call_id, int32_t stream_id);
    int on_header(int32_t stream_id, const std::string& name, const std::string& value);
    int on_data(int32_t stream_id, const uint8_t* data, size_t n);
    int on_stream_close(int32_t stream_id, uint32_t error_code);
    void on_goaway() { alive_.store(false, std::memory_order_release); }
    long read_body(int32_t stream_id, uint8_t* buf, size_t len, uint32_t* flags);

    std::unique_ptr<Transport> transport_;
    std::string host_;  // :authority
    int wake_r_ = -1, wake_w_ = -1;
    nghttp2_session* session_ = nullptr;
    bool want_write_ = false;  // the transport needs the socket writable to make progress

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

// A CallHandler that reassembles whole gRPC messages and hands them to a
// waiting thread: for the request/response conversations of the ALTS
// handshaker, xDS (ADS) and RLS, none of which is on the data path.
class MessageQueue : public CallHandler {
public:
    bool on_body(const uint8_t* p, size_t n) override;
    void on_close(int status, const std::string& message) override;

    // Blocks for the next message. Returns false when the call has ended with
    // no message left (status()/message() then say why) or on timeout
    // (timed_out() is then true).
    bool next(std::string& out, std::chrono::milliseconds timeout);
    // Blocks until on_close has run (it always runs, see CallHandler).
    void wait_closed();

    bool closed() const;
    bool timed_out() const { return timed_out_; }
    int status() const;
    std::string message() const;

private:
    mutable std::mutex mu_;
    std::condition_variable cv_;
    std::deque<std::string> ready_;
    std::string partial_;  // I/O thread only
    bool closed_ = false;
    bool timed_out_ = false;
    int status_ = kUnknown;
    std::string message_;
};

}  // namespace gcs_grpc
