// The byte stream under one GrpcConnection: TLS (to Google front ends and
// directpath-pa), plaintext (the ALTS handshaker on the metadata server), or
// ALTS (DirectPath backends, alts.hpp). Construction completes any handshake;
// after that every call is non-blocking and runs on the connection's I/O
// thread only.
#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>

typedef struct ssl_st SSL;
typedef struct ssl_ctx_st SSL_CTX;

namespace gcs_grpc {

class Transport {
public:
    virtual ~Transport() = default;

    virtual int fd() const = 0;
    // The ":scheme" pseudo-header for requests on this transport.
    virtual const char* scheme() const = 0;

    // Moves up to `n` bytes. Returns > 0 for bytes moved, 0 when the socket
    // would block (`want_write` is set when it must become WRITABLE to make
    // progress), < 0 on failure or end of stream (see error()).
    virtual long write(const uint8_t* p, size_t n, bool& want_write) = 0;
    virtual long read(uint8_t* p, size_t n, bool& want_write) = 0;

    // Bytes already decrypted/buffered that a read() returns without the
    // socket becoming readable.
    virtual bool pending() const = 0;

    // Pushes bytes a write() accepted but could not yet put on the socket.
    // Same return contract as write(); 0 also means "nothing left".
    virtual long flush(bool&) { return 0; }

    const std::string& error() const { return error_; }

protected:
    std::string error_;
};

// Blocking TCP connect with a timeout, TCP_NODELAY on. Throws std::runtime_error.
int connect_tcp(const std::string& host, int port, int timeout_ms);
void set_nonblocking(int fd, bool on);

// TLS 1.2+, peer verified against `host`, ALPN h2 required.
class TlsTransport : public Transport {
public:
    TlsTransport(const std::string& host, int port, const std::string& ca_file);
    ~TlsTransport() override;
    int fd() const override { return fd_; }
    const char* scheme() const override { return "https"; }
    long write(const uint8_t* p, size_t n, bool& want_write) override;
    long read(uint8_t* p, size_t n, bool& want_write) override;
    bool pending() const override;

private:
    int fd_ = -1;
    SSL_CTX* ctx_ = nullptr;
    SSL* ssl_ = nullptr;
};

// h2c with prior knowledge. Used ONLY for the ALTS handshaker service, which
// gRPC itself reaches with insecure channel credentials (design §10).
class PlainTransport : public Transport {
public:
    PlainTransport(const std::string& host, int port);
    ~PlainTransport() override;
    int fd() const override { return fd_; }
    const char* scheme() const override { return "http"; }
    long write(const uint8_t* p, size_t n, bool& want_write) override;
    long read(uint8_t* p, size_t n, bool& want_write) override;
    bool pending() const override { return false; }

private:
    int fd_ = -1;
};

}  // namespace gcs_grpc
