#include "transport.hpp"

#include <algorithm>
#include <cerrno>
#include <cstring>
#include <stdexcept>

#include <fcntl.h>
#include <netdb.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <openssl/err.h>
#include <openssl/ssl.h>
#include <openssl/x509v3.h>

#ifndef MSG_NOSIGNAL
#define MSG_NOSIGNAL 0  // Apple: SO_NOSIGPIPE is set on the socket instead
#endif

namespace gcs_grpc {

namespace {

std::string ssl_error_string() {
    char buf[256];
    unsigned long e = ERR_get_error();
    if (e == 0) return "unknown TLS error";
    ERR_error_string_n(e, buf, sizeof(buf));
    ERR_clear_error();
    return buf;
}

}  // namespace

void set_nonblocking(int fd, bool on) {
    int fl = fcntl(fd, F_GETFL, 0);
    fcntl(fd, F_SETFL, on ? (fl | O_NONBLOCK) : (fl & ~O_NONBLOCK));
}

int connect_tcp(const std::string& host, int port, int timeout_ms) {
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
            c = poll(&p, 1, timeout_ms) == 1 ? 0 : -1;
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
#ifdef SO_NOSIGPIPE
            setsockopt(fd, SOL_SOCKET, SO_NOSIGPIPE, &one, sizeof(one));
#endif
            freeaddrinfo(res);
            return fd;
        }
        last = std::strerror(errno);
        ::close(fd);
    }
    freeaddrinfo(res);
    throw std::runtime_error("gRPC: cannot connect to " + host + ":" + service + ": " + last);
}

// ── TLS ─────────────────────────────────────────────────────────────────────

TlsTransport::TlsTransport(const std::string& host, int port, const std::string& ca_file) {
    fd_ = connect_tcp(host, port, 10000);
    auto fail = [&](const std::string& why) {
        if (ssl_) SSL_free(ssl_);
        if (ctx_) SSL_CTX_free(ctx_);
        ::close(fd_);
        throw std::runtime_error("gRPC: " + host + ": " + why);
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
    SSL_set_tlsext_host_name(ssl_, host.c_str());
    if (SSL_set1_host(ssl_, host.c_str()) != 1) fail("SSL_set1_host failed");
    // nghttp2 retries a short write from a possibly moved buffer.
    SSL_set_mode(ssl_, SSL_MODE_ACCEPT_MOVING_WRITE_BUFFER | SSL_MODE_ENABLE_PARTIAL_WRITE);
    if (SSL_connect(ssl_) != 1) fail("TLS handshake: " + ssl_error_string());
    const unsigned char* proto = nullptr;
    unsigned int proto_len = 0;
    SSL_get0_alpn_selected(ssl_, &proto, &proto_len);
    if (proto_len != 2 || std::memcmp(proto, "h2", 2) != 0) fail("server did not negotiate HTTP/2 (ALPN)");
    set_nonblocking(fd_, true);
}

TlsTransport::~TlsTransport() {
    SSL_free(ssl_);
    SSL_CTX_free(ctx_);
    ::close(fd_);
}

long TlsTransport::write(const uint8_t* p, size_t n, bool& want_write) {
    const int w = SSL_write(ssl_, p, static_cast<int>(std::min<size_t>(n, 1 << 30)));
    if (w > 0) return w;
    const int e = SSL_get_error(ssl_, w);
    if (e == SSL_ERROR_WANT_WRITE || e == SSL_ERROR_WANT_READ) {
        want_write = (e == SSL_ERROR_WANT_WRITE);
        return 0;
    }
    error_ = "gRPC TLS write: " + ssl_error_string();
    return -1;
}

long TlsTransport::read(uint8_t* p, size_t n, bool& want_write) {
    const int r = SSL_read(ssl_, p, static_cast<int>(std::min<size_t>(n, 1 << 30)));
    if (r > 0) return r;
    const int e = SSL_get_error(ssl_, r);
    if (e == SSL_ERROR_WANT_READ) return 0;
    if (e == SSL_ERROR_WANT_WRITE) {
        want_write = true;
        return 0;
    }
    error_ = e == SSL_ERROR_ZERO_RETURN ? "gRPC peer closed the connection" : "gRPC TLS read: " + ssl_error_string();
    return -1;
}

bool TlsTransport::pending() const { return SSL_pending(ssl_) > 0; }

// ── Plaintext ───────────────────────────────────────────────────────────────

PlainTransport::PlainTransport(const std::string& host, int port) {
    fd_ = connect_tcp(host, port, 10000);
    set_nonblocking(fd_, true);
}

PlainTransport::~PlainTransport() { ::close(fd_); }

long PlainTransport::write(const uint8_t* p, size_t n, bool& want_write) {
    const ssize_t w = ::send(fd_, p, n, MSG_NOSIGNAL);
    if (w > 0) return w;
    if (w < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
        want_write = true;
        return 0;
    }
    error_ = std::string("gRPC write: ") + std::strerror(errno);
    return -1;
}

long PlainTransport::read(uint8_t* p, size_t n, bool&) {
    const ssize_t r = ::recv(fd_, p, n, 0);
    if (r > 0) return r;
    if (r < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) return 0;
    error_ = r == 0 ? "gRPC peer closed the connection" : std::string("gRPC read: ") + std::strerror(errno);
    return -1;
}

}  // namespace gcs_grpc
