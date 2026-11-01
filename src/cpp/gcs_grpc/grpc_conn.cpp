#include "grpc_conn.hpp"

#include <algorithm>
#include <cerrno>
#include <cstring>

#include <poll.h>
#include <unistd.h>

#include <nghttp2/nghttp2.h>

namespace gcs_grpc {

namespace {

constexpr size_t kReadChunk = 256 * 1024;
// Flow-control windows: the defaults (64 KiB) cap one stream at window/RTT,
// a few MB/s at GCS latencies — far below what a range read needs.
constexpr int32_t kStreamWindow = 16 * 1024 * 1024;
constexpr int32_t kConnectionWindow = 256 * 1024 * 1024;

// HTTP status → gRPC status, per the gRPC HTTP/2 protocol mapping.
int grpc_status_from_http(int http) {
    switch (http) {
        case 400: return kInternal;
        case 401: return kUnauthenticated;
        case 403: return kPermissionDenied;
        case 404: return 12;  // UNIMPLEMENTED
        case 429: case 502: case 503: case 504: return kUnavailable;
        default: return kUnknown;
    }
}

}  // namespace

struct GrpcConnection::Call {
    uint64_t id = 0;
    CallHandler* handler = nullptr;
    std::string path;
    Metadata metadata;
    std::string body;
    size_t body_sent = 0;
    bool half_closed = true;  // no more body after `body`
    bool deferred = false;    // nghttp2 is waiting for send() to resume the stream
    int32_t stream_id = 0;
    int http_status = 0;
    int grpc_status = -1;
    std::string grpc_message;
    bool cancelled = false;
};

// Static trampolines: nghttp2 hands back the session user_data (the connection).
struct Nghttp2Callbacks {
    static ssize_t send(nghttp2_session*, const uint8_t* data, size_t n, int, void* u) {
        return static_cast<ssize_t>(static_cast<GrpcConnection*>(u)->send_bytes(data, n));
    }
    static int header(nghttp2_session*, const nghttp2_frame* frame, const uint8_t* name, size_t namelen,
                      const uint8_t* value, size_t valuelen, uint8_t, void* u) {
        return static_cast<GrpcConnection*>(u)->on_header(
            frame->hd.stream_id, std::string(reinterpret_cast<const char*>(name), namelen),
            std::string(reinterpret_cast<const char*>(value), valuelen));
    }
    static int data(nghttp2_session*, uint8_t, int32_t stream_id, const uint8_t* data, size_t len, void* u) {
        return static_cast<GrpcConnection*>(u)->on_data(stream_id, data, len);
    }
    static int close(nghttp2_session*, int32_t stream_id, uint32_t error_code, void* u) {
        return static_cast<GrpcConnection*>(u)->on_stream_close(stream_id, error_code);
    }
    static int frame(nghttp2_session*, const nghttp2_frame* frame, void* u) {
        if (frame->hd.type == NGHTTP2_GOAWAY) static_cast<GrpcConnection*>(u)->on_goaway();
        return 0;
    }
    static ssize_t body(nghttp2_session*, int32_t stream_id, uint8_t* buf, size_t len, uint32_t* flags,
                        nghttp2_data_source*, void* u) {
        return static_cast<ssize_t>(static_cast<GrpcConnection*>(u)->read_body(stream_id, buf, len, flags));
    }
};

void GrpcConnection::frame_message(std::string& out, const std::string& message) {
    const uint32_t n = static_cast<uint32_t>(message.size());
    out.push_back(0);  // uncompressed
    out.push_back(static_cast<char>(n >> 24));
    out.push_back(static_cast<char>(n >> 16));
    out.push_back(static_cast<char>(n >> 8));
    out.push_back(static_cast<char>(n));
    out.append(message);
}

GrpcConnection::GrpcConnection(std::unique_ptr<Transport> transport, std::string authority)
    : transport_(std::move(transport)), host_(std::move(authority)) {
    // Constructor failures below must release what was acquired so far.
    auto fail = [this](const std::string& why) {
        if (session_) nghttp2_session_del(session_);
        if (wake_r_ >= 0) ::close(wake_r_);
        if (wake_w_ >= 0) ::close(wake_w_);
        session_ = nullptr;
        throw std::runtime_error("gRPC: " + host_ + ": " + why);
    };

    int pipefd[2];
    if (pipe(pipefd) != 0) fail(std::string("pipe: ") + std::strerror(errno));
    wake_r_ = pipefd[0];
    wake_w_ = pipefd[1];
    set_nonblocking(wake_r_, true);
    set_nonblocking(wake_w_, true);

    nghttp2_session_callbacks* cbs = nullptr;
    nghttp2_session_callbacks_new(&cbs);
    nghttp2_session_callbacks_set_send_callback(cbs, &Nghttp2Callbacks::send);
    nghttp2_session_callbacks_set_on_header_callback(cbs, &Nghttp2Callbacks::header);
    nghttp2_session_callbacks_set_on_data_chunk_recv_callback(cbs, &Nghttp2Callbacks::data);
    nghttp2_session_callbacks_set_on_stream_close_callback(cbs, &Nghttp2Callbacks::close);
    nghttp2_session_callbacks_set_on_frame_recv_callback(cbs, &Nghttp2Callbacks::frame);
    int rc = nghttp2_session_client_new(&session_, cbs, this);
    nghttp2_session_callbacks_del(cbs);
    if (rc != 0) fail(std::string("nghttp2_session_client_new: ") + nghttp2_strerror(rc));

    nghttp2_settings_entry settings[] = {
        {NGHTTP2_SETTINGS_ENABLE_PUSH, 0},
        {NGHTTP2_SETTINGS_INITIAL_WINDOW_SIZE, static_cast<uint32_t>(kStreamWindow)},
    };
    if (nghttp2_submit_settings(session_, NGHTTP2_FLAG_NONE, settings, 2) != 0 ||
        nghttp2_session_set_local_window_size(session_, NGHTTP2_FLAG_NONE, 0, kConnectionWindow) != 0)
        fail("nghttp2 settings");

    io_ = std::thread([this] { io_loop(); });
}

GrpcConnection::~GrpcConnection() {
    stop_.store(true, std::memory_order_release);
    wake();
    if (io_.joinable()) io_.join();
    nghttp2_session_del(session_);
    ::close(wake_r_);
    ::close(wake_w_);
}

void GrpcConnection::wake() {
    const char b = 1;
    // A full pipe already guarantees a wakeup.
    (void)!::write(wake_w_, &b, 1);
}

uint64_t GrpcConnection::start_call(const std::string& path, Metadata metadata, std::string body, CallHandler* handler,
                                    bool half_close) {
    auto call = std::make_unique<Call>();
    call->half_closed = half_close;
    call->id = next_call_id_.fetch_add(1, std::memory_order_relaxed);
    call->handler = handler;
    call->path = path;
    call->metadata = std::move(metadata);
    call->body = std::move(body);
    const uint64_t id = call->id;
    {
        std::lock_guard<std::mutex> g(ops_mu_);
        if (alive_.load(std::memory_order_acquire)) {
            ops_.push_back(Op{Op::kStart, id, std::move(call), {}, false});
        }
    }
    if (call) {  // connection is dead: the I/O thread will never see this call
        handler->on_close(kUnavailable, "gRPC connection to " + host_ + " is closed");
        return id;
    }
    wake();
    return id;
}

void GrpcConnection::cancel(uint64_t call_id) {
    {
        std::lock_guard<std::mutex> g(ops_mu_);
        ops_.push_back(Op{Op::kCancel, call_id, nullptr, {}, false});
    }
    wake();
}

void GrpcConnection::send(uint64_t call_id, std::string body, bool half_close) {
    {
        std::lock_guard<std::mutex> g(ops_mu_);
        Op op{Op::kSend, call_id, nullptr, {}, false};
        op.body = std::move(body);
        op.half_close = half_close;
        ops_.push_back(std::move(op));
    }
    wake();
}

void GrpcConnection::submit_pending() {
    std::deque<Op> ops;
    {
        std::lock_guard<std::mutex> g(ops_mu_);
        ops.swap(ops_);
    }
    for (Op& op : ops) {
        if (op.kind == Op::kCancel) {
            auto it = stream_of_call_.find(op.call_id);
            if (it == stream_of_call_.end()) continue;  // already closed
            by_stream_[it->second]->cancelled = true;
            nghttp2_submit_rst_stream(session_, NGHTTP2_FLAG_NONE, it->second, NGHTTP2_CANCEL);
            continue;
        }
        if (op.kind == Op::kSend) {
            auto it = stream_of_call_.find(op.call_id);
            if (it == stream_of_call_.end()) continue;  // already closed
            Call& c = *by_stream_[it->second];
            if (c.half_closed) continue;  // a send after half-close is the caller's bug; the call is done sending
            c.body.append(op.body);
            c.half_closed = op.half_close;
            if (c.deferred) {
                c.deferred = false;
                nghttp2_session_resume_data(session_, c.stream_id);
            }
            continue;
        }
        Call& c = *op.call;
        std::vector<nghttp2_nv> nva;
        auto add = [&nva](const std::string& k, const std::string& v) {
            nva.push_back(nghttp2_nv{reinterpret_cast<uint8_t*>(const_cast<char*>(k.data())),
                                     reinterpret_cast<uint8_t*>(const_cast<char*>(v.data())),
                                     k.size(), v.size(), NGHTTP2_NV_FLAG_NONE});
        };
        const std::string scheme = transport_->scheme();
        static const std::string kMethod = ":method", kPost = "POST", kScheme = ":scheme",
                                 kPath = ":path", kAuthority = ":authority", kCt = "content-type",
                                 kGrpc = "application/grpc", kTe = "te", kTrailers = "trailers",
                                 kUa = "user-agent", kUaVal = "opteryx-gcs-grpc/1";
        add(kMethod, kPost);
        add(kScheme, scheme);
        add(kPath, c.path);
        add(kAuthority, host_);
        add(kCt, kGrpc);
        add(kTe, kTrailers);
        add(kUa, kUaVal);
        for (const auto& kv : c.metadata) add(kv.first, kv.second);
        nghttp2_data_provider prd;
        prd.source.ptr = nullptr;
        prd.read_callback = &Nghttp2Callbacks::body;
        const int32_t sid = nghttp2_submit_request(session_, nullptr, nva.data(), nva.size(), &prd, nullptr);
        if (sid < 0) {
            c.handler->on_close(kUnavailable, std::string("nghttp2_submit_request: ") + nghttp2_strerror(sid));
            continue;
        }
        c.stream_id = sid;
        stream_of_call_[c.id] = sid;
        by_stream_[sid] = std::move(op.call);
        open_calls_.fetch_add(1, std::memory_order_relaxed);
    }
}

long GrpcConnection::read_body(int32_t stream_id, uint8_t* buf, size_t len, uint32_t* flags) {
    auto it = by_stream_.find(stream_id);
    if (it == by_stream_.end()) return NGHTTP2_ERR_TEMPORAL_CALLBACK_FAILURE;
    Call& c = *it->second;
    const size_t n = std::min(len, c.body.size() - c.body_sent);
    if (n == 0 && !c.half_closed) {
        c.deferred = true;
        return NGHTTP2_ERR_DEFERRED;
    }
    std::memcpy(buf, c.body.data() + c.body_sent, n);
    c.body_sent += n;
    if (c.body_sent == c.body.size()) {
        if (c.half_closed) *flags |= NGHTTP2_DATA_FLAG_EOF;
        c.body.clear();  // a long-lived stream must not keep everything it ever sent
        c.body_sent = 0;
    }
    return static_cast<long>(n);
}

long GrpcConnection::send_bytes(const uint8_t* data, size_t n) {
    const long w = transport_->write(data, n, want_write_);
    if (w > 0) return w;
    return w == 0 ? NGHTTP2_ERR_WOULDBLOCK : NGHTTP2_ERR_CALLBACK_FAILURE;
}

int GrpcConnection::on_header(int32_t stream_id, const std::string& name, const std::string& value) {
    auto it = by_stream_.find(stream_id);
    if (it == by_stream_.end()) return 0;
    Call& c = *it->second;
    if (name == ":status") c.http_status = std::atoi(value.c_str());
    else if (name == "grpc-status") c.grpc_status = std::atoi(value.c_str());
    else if (name == "grpc-message") c.grpc_message = value;
    return 0;
}

int GrpcConnection::on_data(int32_t stream_id, const uint8_t* data, size_t n) {
    auto it = by_stream_.find(stream_id);
    if (it == by_stream_.end() || it->second->cancelled) return 0;
    Call& c = *it->second;
    if (!c.handler->on_body(data, n)) {
        c.cancelled = true;
        nghttp2_submit_rst_stream(session_, NGHTTP2_FLAG_NONE, stream_id, NGHTTP2_CANCEL);
    }
    return 0;
}

int GrpcConnection::on_stream_close(int32_t stream_id, uint32_t error_code) {
    auto it = by_stream_.find(stream_id);
    if (it == by_stream_.end()) return 0;
    std::unique_ptr<Call> call = std::move(it->second);
    by_stream_.erase(it);
    stream_of_call_.erase(call->id);
    open_calls_.fetch_sub(1, std::memory_order_relaxed);

    int status;
    std::string message = call->grpc_message;
    if (call->cancelled) {
        status = kCancelled;
        message = "cancelled";
    } else if (call->grpc_status >= 0) {
        status = call->grpc_status;
    } else if (error_code != NGHTTP2_NO_ERROR) {
        status = kUnavailable;
        message = std::string("HTTP/2 stream reset: ") + nghttp2_http2_strerror(error_code);
    } else if (call->http_status != 200) {
        status = grpc_status_from_http(call->http_status);
        message = "HTTP status " + std::to_string(call->http_status);
    } else {
        status = kInternal;
        message = "gRPC: response without grpc-status";
    }
    call->handler->on_close(status, message);
    return 0;
}

void GrpcConnection::fail_all(const std::string& why) {
    alive_.store(false, std::memory_order_release);
    auto calls = std::move(by_stream_);
    by_stream_.clear();
    stream_of_call_.clear();
    open_calls_.store(0, std::memory_order_relaxed);
    for (auto& kv : calls) kv.second->handler->on_close(kUnavailable, why);
}

void GrpcConnection::drain_ops_failing(const std::string& why) {
    std::deque<Op> ops;
    {
        std::lock_guard<std::mutex> g(ops_mu_);
        alive_.store(false, std::memory_order_release);
        ops.swap(ops_);
    }
    for (Op& op : ops)
        if (op.kind == Op::kStart) op.call->handler->on_close(kUnavailable, why);
}

void GrpcConnection::io_loop() {
    std::vector<uint8_t> rbuf(kReadChunk);
    std::string why = "gRPC connection to " + host_ + " closed";
    const int fd = transport_->fd();
    while (!stop_.load(std::memory_order_acquire)) {
        submit_pending();
        want_write_ = false;
        int rc = nghttp2_session_send(session_);
        if (rc != 0) {
            why = rc == NGHTTP2_ERR_CALLBACK_FAILURE && !transport_->error().empty()
                      ? transport_->error()
                      : std::string("gRPC send failed: ") + nghttp2_strerror(rc);
            break;
        }
        if (!nghttp2_session_want_read(session_) && !nghttp2_session_want_write(session_)) {
            why = "gRPC connection to " + host_ + " finished (GOAWAY)";
            break;
        }
        pollfd fds[2] = {{fd, static_cast<short>(POLLIN | (want_write_ ? POLLOUT : 0)), 0}, {wake_r_, POLLIN, 0}};
        // The transport may already hold decrypted bytes the socket will never signal.
        const int timeout = transport_->pending() ? 0 : 1000;
        if (poll(fds, 2, timeout) < 0 && errno != EINTR) {
            why = std::string("gRPC poll: ") + std::strerror(errno);
            break;
        }
        if (fds[1].revents & POLLIN) {
            char sink[64];
            while (::read(wake_r_, sink, sizeof(sink)) > 0) {}
        }
        if (!(fds[0].revents & (POLLIN | POLLHUP | POLLERR)) && !transport_->pending()) continue;
        bool broken = false;
        for (;;) {
            const long r = transport_->read(rbuf.data(), rbuf.size(), want_write_);
            if (r > 0) {
                const ssize_t used = nghttp2_session_mem_recv(session_, rbuf.data(), static_cast<size_t>(r));
                if (used < 0) {
                    why = std::string("gRPC receive failed: ") + nghttp2_strerror(static_cast<int>(used));
                    broken = true;
                    break;
                }
                continue;
            }
            if (r == 0) break;
            why = transport_->error();
            broken = true;
            break;
        }
        if (broken) break;
    }
    fail_all(stop_.load() ? "gRPC connection shut down" : why);
    drain_ops_failing(why);
}

// ── MessageQueue ────────────────────────────────────────────────────────────

bool MessageQueue::on_body(const uint8_t* p, size_t n) {
    partial_.append(reinterpret_cast<const char*>(p), n);
    size_t at = 0;
    std::deque<std::string> whole;
    while (partial_.size() - at >= 5) {
        const auto* h = reinterpret_cast<const uint8_t*>(partial_.data() + at);
        if (h[0] != 0) {
            std::lock_guard<std::mutex> g(mu_);
            message_ = "gRPC: unexpected compressed message";
            return false;
        }
        const size_t len = (size_t(h[1]) << 24) | (size_t(h[2]) << 16) | (size_t(h[3]) << 8) | size_t(h[4]);
        if (partial_.size() - at - 5 < len) break;
        whole.push_back(partial_.substr(at + 5, len));
        at += 5 + len;
    }
    partial_.erase(0, at);
    if (!whole.empty()) {
        std::lock_guard<std::mutex> g(mu_);
        for (auto& m : whole) ready_.push_back(std::move(m));
        cv_.notify_all();
    }
    return true;
}

void MessageQueue::on_close(int status, const std::string& message) {
    std::lock_guard<std::mutex> g(mu_);
    status_ = status;
    // A parse failure recorded by on_body explains a kCancelled better than "cancelled".
    if (message_.empty() || status != kCancelled) message_ = message;
    if (status_ == kOk && !partial_.empty()) {
        status_ = kInternal;
        message_ = "gRPC: stream ended inside a message";
    }
    closed_ = true;
    cv_.notify_all();
}

bool MessageQueue::next(std::string& out, std::chrono::milliseconds timeout) {
    std::unique_lock<std::mutex> lk(mu_);
    timed_out_ = !cv_.wait_for(lk, timeout, [this] { return !ready_.empty() || closed_; });
    if (ready_.empty()) return false;
    out = std::move(ready_.front());
    ready_.pop_front();
    return true;
}

void MessageQueue::wait_closed() {
    std::unique_lock<std::mutex> lk(mu_);
    cv_.wait(lk, [this] { return closed_; });
}

bool MessageQueue::closed() const {
    std::lock_guard<std::mutex> g(mu_);
    return closed_;
}

int MessageQueue::status() const {
    std::lock_guard<std::mutex> g(mu_);
    return status_;
}

std::string MessageQueue::message() const {
    std::lock_guard<std::mutex> g(mu_);
    return message_;
}

}  // namespace gcs_grpc
