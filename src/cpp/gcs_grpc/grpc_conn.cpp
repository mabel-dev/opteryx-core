#include "grpc_conn.hpp"

#include <cerrno>
#include <cstring>

#include <fcntl.h>
#include <netdb.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <nghttp2/nghttp2.h>
#include <openssl/err.h>
#include <openssl/ssl.h>
#include <openssl/x509v3.h>

namespace gcs_grpc {

namespace {

constexpr int kConnectTimeoutMs = 10000;
constexpr size_t kReadChunk = 256 * 1024;
// Flow-control windows: the defaults (64 KiB) cap one stream at window/RTT,
// a few MB/s at GCS latencies — far below what a range read needs.
constexpr int32_t kStreamWindow = 16 * 1024 * 1024;
constexpr int32_t kConnectionWindow = 256 * 1024 * 1024;

std::string ssl_error_string() {
    char buf[256];
    unsigned long e = ERR_get_error();
    if (e == 0) return "unknown TLS error";
    ERR_error_string_n(e, buf, sizeof(buf));
    ERR_clear_error();
    return buf;
}

void set_nonblocking(int fd, bool on) {
    int fl = fcntl(fd, F_GETFL, 0);
    fcntl(fd, F_SETFL, on ? (fl | O_NONBLOCK) : (fl & ~O_NONBLOCK));
}

int connect_tcp(const std::string& host, int port) {
    addrinfo hints{};
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    addrinfo* res = nullptr;
    const std::string service = std::to_string(port);
    int rc = getaddrinfo(host.c_str(), service.c_str(), &hints, &res);
    if (rc != 0) throw std::runtime_error("gRPC: cannot resolve " + host + ": " + gai_strerror(rc));
    std::string last = "no addresses";
    for (addrinfo* ai = res; ai; ai = ai->ai_next) {
        int fd = socket(ai->ai_family, ai->ai_socktype, ai->ai_protocol);
        if (fd < 0) { last = std::strerror(errno); continue; }
        set_nonblocking(fd, true);
        int c = ::connect(fd, ai->ai_addr, ai->ai_addrlen);
        if (c != 0 && errno == EINPROGRESS) {
            pollfd p{fd, POLLOUT, 0};
            c = poll(&p, 1, kConnectTimeoutMs) == 1 ? 0 : -1;
            int err = 0;
            socklen_t len = sizeof(err);
            if (c == 0 && (getsockopt(fd, SOL_SOCKET, SO_ERROR, &err, &len) != 0 || err != 0)) {
                errno = err;
                c = -1;
            } else if (c != 0) {
                errno = ETIMEDOUT;
            }
        }
        if (c == 0) {
            set_nonblocking(fd, false);
            int one = 1;
            setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
            freeaddrinfo(res);
            return fd;
        }
        last = std::strerror(errno);
        ::close(fd);
    }
    freeaddrinfo(res);
    throw std::runtime_error("gRPC: cannot connect to " + host + ":" + service + ": " + last);
}

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

GrpcConnection::GrpcConnection(const std::string& host, int port, const std::string& ca_file) : host_(host) {
    fd_ = connect_tcp(host, port);
    // Constructor failures below must release what was acquired so far.
    auto fail = [this](const std::string& why) {
        if (ssl_) SSL_free(ssl_);
        if (ctx_) SSL_CTX_free(ctx_);
        if (session_) nghttp2_session_del(session_);
        if (wake_r_ >= 0) ::close(wake_r_);
        if (wake_w_ >= 0) ::close(wake_w_);
        ::close(fd_);
        ssl_ = nullptr; ctx_ = nullptr; session_ = nullptr;
        throw std::runtime_error("gRPC: " + host_ + ": " + why);
    };

    ctx_ = SSL_CTX_new(TLS_client_method());
    if (!ctx_) fail("SSL_CTX_new: " + ssl_error_string());
    SSL_CTX_set_min_proto_version(ctx_, TLS1_2_VERSION);
    SSL_CTX_set_verify(ctx_, SSL_VERIFY_PEER, nullptr);
    if (!ca_file.empty() ? SSL_CTX_load_verify_locations(ctx_, ca_file.c_str(), nullptr) != 1
                         : SSL_CTX_set_default_verify_paths(ctx_) != 1)
        fail("loading CA certificates: " + ssl_error_string());
    static const unsigned char kAlpn[] = {2, 'h', '2'};
    if (SSL_CTX_set_alpn_protos(ctx_, kAlpn, sizeof(kAlpn)) != 0) fail("ALPN setup failed");

    ssl_ = SSL_new(ctx_);
    if (!ssl_) fail("SSL_new: " + ssl_error_string());
    SSL_set_fd(ssl_, fd_);
    SSL_set_tlsext_host_name(ssl_, host_.c_str());
    if (SSL_set1_host(ssl_, host_.c_str()) != 1) fail("SSL_set1_host failed");
    // nghttp2 retries a short write from a possibly moved buffer.
    SSL_set_mode(ssl_, SSL_MODE_ACCEPT_MOVING_WRITE_BUFFER | SSL_MODE_ENABLE_PARTIAL_WRITE);
    if (SSL_connect(ssl_) != 1) fail("TLS handshake: " + ssl_error_string());
    const unsigned char* proto = nullptr;
    unsigned int proto_len = 0;
    SSL_get0_alpn_selected(ssl_, &proto, &proto_len);
    if (proto_len != 2 || std::memcmp(proto, "h2", 2) != 0) fail("server did not negotiate HTTP/2 (ALPN)");
    set_nonblocking(fd_, true);

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
    SSL_free(ssl_);
    SSL_CTX_free(ctx_);
    ::close(fd_);
    ::close(wake_r_);
    ::close(wake_w_);
}

void GrpcConnection::wake() {
    const char b = 1;
    // A full pipe already guarantees a wakeup.
    (void)!::write(wake_w_, &b, 1);
}

uint64_t GrpcConnection::start_call(const std::string& path, Metadata metadata, std::string body, CallHandler* handler) {
    auto call = std::make_unique<Call>();
    call->id = next_call_id_.fetch_add(1, std::memory_order_relaxed);
    call->handler = handler;
    call->path = path;
    call->metadata = std::move(metadata);
    call->body = std::move(body);
    const uint64_t id = call->id;
    {
        std::lock_guard<std::mutex> g(ops_mu_);
        if (alive_.load(std::memory_order_acquire)) {
            ops_.push_back(Op{Op::kStart, id, std::move(call)});
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
        ops_.push_back(Op{Op::kCancel, call_id, nullptr});
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
        Call& c = *op.call;
        std::vector<nghttp2_nv> nva;
        auto add = [&nva](const std::string& k, const std::string& v) {
            nva.push_back(nghttp2_nv{reinterpret_cast<uint8_t*>(const_cast<char*>(k.data())),
                                     reinterpret_cast<uint8_t*>(const_cast<char*>(v.data())),
                                     k.size(), v.size(), NGHTTP2_NV_FLAG_NONE});
        };
        static const std::string kMethod = ":method", kPost = "POST", kScheme = ":scheme", kHttps = "https",
                                 kPath = ":path", kAuthority = ":authority", kCt = "content-type",
                                 kGrpc = "application/grpc", kTe = "te", kTrailers = "trailers",
                                 kUa = "user-agent", kUaVal = "opteryx-gcs-grpc/1";
        add(kMethod, kPost);
        add(kScheme, kHttps);
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
    std::memcpy(buf, c.body.data() + c.body_sent, n);
    c.body_sent += n;
    if (c.body_sent == c.body.size()) *flags |= NGHTTP2_DATA_FLAG_EOF;
    return static_cast<long>(n);
}

long GrpcConnection::send_bytes(const uint8_t* data, size_t n) {
    const int w = SSL_write(ssl_, data, static_cast<int>(std::min<size_t>(n, 1 << 30)));
    if (w > 0) return w;
    const int e = SSL_get_error(ssl_, w);
    if (e == SSL_ERROR_WANT_WRITE || e == SSL_ERROR_WANT_READ) {
        want_write_ = (e == SSL_ERROR_WANT_WRITE);
        return NGHTTP2_ERR_WOULDBLOCK;
    }
    return NGHTTP2_ERR_CALLBACK_FAILURE;
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
    while (!stop_.load(std::memory_order_acquire)) {
        submit_pending();
        want_write_ = false;
        int rc = nghttp2_session_send(session_);
        if (rc != 0) {
            why = std::string("gRPC send failed: ") + nghttp2_strerror(rc);
            break;
        }
        if (!nghttp2_session_want_read(session_) && !nghttp2_session_want_write(session_)) {
            why = "gRPC connection to " + host_ + " finished (GOAWAY)";
            break;
        }
        pollfd fds[2] = {{fd_, static_cast<short>(POLLIN | (want_write_ ? POLLOUT : 0)), 0}, {wake_r_, POLLIN, 0}};
        // SSL may already hold decrypted bytes the socket will never signal.
        const int timeout = SSL_pending(ssl_) > 0 ? 0 : 1000;
        if (poll(fds, 2, timeout) < 0 && errno != EINTR) {
            why = std::string("gRPC poll: ") + std::strerror(errno);
            break;
        }
        if (fds[1].revents & POLLIN) {
            char sink[64];
            while (::read(wake_r_, sink, sizeof(sink)) > 0) {}
        }
        if (!(fds[0].revents & (POLLIN | POLLHUP | POLLERR)) && SSL_pending(ssl_) == 0) continue;
        bool broken = false;
        for (;;) {
            const int r = SSL_read(ssl_, rbuf.data(), static_cast<int>(rbuf.size()));
            if (r > 0) {
                const ssize_t used = nghttp2_session_mem_recv(session_, rbuf.data(), static_cast<size_t>(r));
                if (used < 0) {
                    why = std::string("gRPC receive failed: ") + nghttp2_strerror(static_cast<int>(used));
                    broken = true;
                    break;
                }
                continue;
            }
            const int e = SSL_get_error(ssl_, r);
            if (e == SSL_ERROR_WANT_READ) break;
            if (e == SSL_ERROR_WANT_WRITE) {
                want_write_ = true;
                break;
            }
            why = e == SSL_ERROR_ZERO_RETURN ? "gRPC peer closed the connection" : "gRPC TLS read: " + ssl_error_string();
            broken = true;
            break;
        }
        if (broken) break;
    }
    fail_all(stop_.load() ? "gRPC connection shut down" : why);
    drain_ops_failing(why);
}

}  // namespace gcs_grpc
