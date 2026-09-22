// src/cpp/pg/pg_client.cpp — see pg_client.hpp.

#include "pg/pg_client.hpp"

#include <openssl/err.h>
#include <openssl/evp.h>
#include <openssl/hmac.h>
#include <openssl/rand.h>
#include <openssl/sha.h>
#include <openssl/ssl.h>

#include <arpa/inet.h>
#include <netdb.h>
#include <poll.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cerrno>
#include <cstring>

namespace opteryx::pg {

// ---------------------------------------------------------------------------
// Config / errors
// ---------------------------------------------------------------------------

std::string PgConfig::key() const {
    return host + "\x1f" + std::to_string(port) + "\x1f" + dbname + "\x1f" + user + "\x1f" +
           password + "\x1f" + sslmode;
}

[[noreturn]] static void fail(const std::string& what) { throw PgError(what); }

// The socket died: the read or write itself failed, which says nothing about
// the statement and everything about the connection. Flagged so the retry path
// can tell it apart from a server error or a protocol/auth failure, both of
// which would fail identically on a fresh connection. See PgError::transport.
[[noreturn]] static void fail_transport(const std::string& what) {
    PgError e(what);
    e.transport = true;
    throw e;
}

static std::string ssl_err_text() {
    char buf[256];
    unsigned long e = ERR_get_error();
    if (e == 0) return "unknown OpenSSL error";
    ERR_error_string_n(e, buf, sizeof buf);
    return buf;
}

// ---------------------------------------------------------------------------
// Type map — the single source of truth for OID -> Draken
// ---------------------------------------------------------------------------

DrakenType pg_oid_to_draken(uint32_t oid) {
    switch (oid) {
        case 16:   return DRAKEN_BOOL;
        case 21:   return DRAKEN_INT16;
        case 23:   return DRAKEN_INT32;
        case 20:   return DRAKEN_INT64;
        case 26:   return DRAKEN_UINT32;      // oid
        case 700:  return DRAKEN_FLOAT32;
        case 701:  return DRAKEN_FLOAT64;
        case 1082: return DRAKEN_DATE32;
        case 1114: case 1184: return DRAKEN_TIMESTAMP64;   // timestamp, timestamptz
        case 1700: return DRAKEN_DECIMAL128; // numeric; the plan narrows to DECIMAL when p <= 18
        case 17:   return DRAKEN_VARBINARY;  // bytea
        case 114: case 3802: return DRAKEN_VARIANT;   // json, jsonb
        case 18: case 19: case 25: case 1042: case 1043: case 2950:
                   return DRAKEN_VARCHAR;    // "char", name, text, bpchar, varchar, uuid
        default:   return DRAKEN_NULL;
    }
}

std::string pg_oid_name(uint32_t oid) {
    switch (oid) {
        case 1186: return "interval";
        case 1083: return "time";
        case 1266: return "timetz";
        case 790:  return "money";
        case 869:  return "inet";
        case 650:  return "cidr";
        case 829:  return "macaddr";
        case 142:  return "xml";
        case 1560: return "bit";
        case 1562: return "varbit";
        case 600:  return "point";
        case 3904: return "int4range";
        case 3926: return "int8range";
        case 3908: return "tsrange";
        case 3910: return "tstzrange";
        case 3912: return "daterange";
        case 1000: return "bool[]";
        case 1005: return "int2[]";
        case 1007: return "int4[]";
        case 1016: return "int8[]";
        case 1009: return "text[]";
        case 1015: return "varchar[]";
        case 1021: return "float4[]";
        case 1022: return "float8[]";
        case 1231: return "numeric[]";
        case 1182: return "date[]";
        case 1115: return "timestamp[]";
        case 1185: return "timestamptz[]";
        case 2951: return "uuid[]";
        case 199:  return "json[]";
        case 3807: return "jsonb[]";
        default:   return "oid " + std::to_string(oid);
    }
}

// ---------------------------------------------------------------------------
// Byte helpers
// ---------------------------------------------------------------------------

static inline uint32_t be32(const uint8_t* p) {
    return ((uint32_t)p[0] << 24) | ((uint32_t)p[1] << 16) | ((uint32_t)p[2] << 8) | (uint32_t)p[3];
}
static inline uint16_t be16(const uint8_t* p) { return (uint16_t)((p[0] << 8) | p[1]); }

struct Out {
    std::vector<uint8_t> b;
    void u8(uint8_t v) { b.push_back(v); }
    void i16(int16_t v) { b.push_back((uint8_t)(v >> 8)); b.push_back((uint8_t)v); }
    void i32(int32_t v) {
        b.push_back((uint8_t)(v >> 24)); b.push_back((uint8_t)(v >> 16));
        b.push_back((uint8_t)(v >> 8));  b.push_back((uint8_t)v);
    }
    void cstr(const std::string& s) { b.insert(b.end(), s.begin(), s.end()); b.push_back(0); }
    void bytes(const std::string& s) { b.insert(b.end(), s.begin(), s.end()); }
};

static std::vector<uint8_t> frame(char type, const Out& body) {
    Out o;
    o.u8((uint8_t)type);
    o.i32((int32_t)(body.b.size() + 4));
    o.b.insert(o.b.end(), body.b.begin(), body.b.end());
    return o.b;
}

// A message as read: the type byte plus a VIEW of its payload inside the
// transport's read buffer. Nothing is copied per message, so the view is live
// only until the next read (see Transport::peek). A payload that must outlive
// the next read - an ErrorResponse held back until ReadyForQuery - is copied
// into an owning vector at that one call site.
struct PgConnection::Msg {
    char type = 0;
    const uint8_t* payload = nullptr;
    size_t len = 0;
};

struct Reader {
    const uint8_t* p;
    size_t n, pos = 0;
    Reader(const uint8_t* data, size_t len) : p(data), n(len) {}
    explicit Reader(const std::vector<uint8_t>& v) : p(v.data()), n(v.size()) {}
    void need(size_t k) const { if (pos + k > n) fail("postgres protocol: truncated message"); }
    uint8_t u8() { need(1); return p[pos++]; }
    int16_t i16() { need(2); int16_t v = (int16_t)be16(p + pos); pos += 2; return v; }
    int32_t i32() { need(4); int32_t v = (int32_t)be32(p + pos); pos += 4; return v; }
    std::string cstr() {
        size_t start = pos;
        while (pos < n && p[pos] != 0) pos++;
        if (pos >= n) fail("postgres protocol: unterminated string");
        std::string s((const char*)p + start, pos - start);
        pos++;
        return s;
    }
    const uint8_t* bytes(size_t k) { need(k); const uint8_t* q = p + pos; pos += k; return q; }
    bool done() const { return pos >= n; }
};

// ---------------------------------------------------------------------------
// base64 (SCRAM)
// ---------------------------------------------------------------------------

static const char B64[] = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

static std::string b64_encode(const uint8_t* d, size_t n) {
    std::string out;
    out.reserve((n + 2) / 3 * 4);
    size_t i = 0;
    for (; i + 2 < n; i += 3) {
        uint32_t v = (d[i] << 16) | (d[i + 1] << 8) | d[i + 2];
        out += B64[(v >> 18) & 63]; out += B64[(v >> 12) & 63];
        out += B64[(v >> 6) & 63];  out += B64[v & 63];
    }
    if (i < n) {
        uint32_t v = d[i] << 16;
        if (i + 1 < n) v |= d[i + 1] << 8;
        out += B64[(v >> 18) & 63]; out += B64[(v >> 12) & 63];
        out += (i + 1 < n) ? B64[(v >> 6) & 63] : '=';
        out += '=';
    }
    return out;
}

static std::vector<uint8_t> b64_decode(const std::string& s) {
    auto val = [](char c) -> int {
        if (c >= 'A' && c <= 'Z') return c - 'A';
        if (c >= 'a' && c <= 'z') return c - 'a' + 26;
        if (c >= '0' && c <= '9') return c - '0' + 52;
        if (c == '+') return 62;
        if (c == '/') return 63;
        return -1;
    };
    std::vector<uint8_t> out;
    uint32_t acc = 0; int bits = 0;
    for (char c : s) {
        if (c == '=') break;
        int v = val(c);
        if (v < 0) fail("postgres scram: malformed base64 from server");
        acc = (acc << 6) | (uint32_t)v; bits += 6;
        if (bits >= 8) { bits -= 8; out.push_back((uint8_t)((acc >> bits) & 0xFF)); }
    }
    return out;
}

// ---------------------------------------------------------------------------
// Transport
// ---------------------------------------------------------------------------

// Writing to a socket whose peer is gone raises SIGPIPE, and SIGPIPE's default
// action is to kill the process. Every such write here is one this client
// already handles -- it wants the EPIPE, not the signal -- so the signal is
// suppressed at the two places it can be, WITHOUT touching the process-wide
// disposition: changing that is the embedder's call, not a database client's.
//
//   * SO_NOSIGPIPE (macOS/BSD) covers every write on the socket, TLS included.
//   * MSG_NOSIGNAL (Linux) covers the plaintext send() below.
//
// That leaves ONE combination uncovered: Linux + TLS, where the write happens
// inside OpenSSL's own BIO with flags this code does not supply. Under CPython
// that is already harmless -- the interpreter sets SIGPIPE to SIG_IGN at
// startup, which is why a reset connection surfaced as an error rather than a
// dead container -- so it is a real gap only for an embedder that both runs on
// Linux and restores the default disposition. Closing it properly means giving
// OpenSSL a custom BIO; it is called out here rather than papered over with a
// global sigaction.
#ifdef MSG_NOSIGNAL
static constexpr int kSendFlags = MSG_NOSIGNAL;
#else
static constexpr int kSendFlags = 0;
#endif

class Transport {
public:
    ~Transport() { close(); }

    void close() {
        if (ssl_) { SSL_shutdown(ssl_); SSL_free(ssl_); ssl_ = nullptr; }
        if (ctx_) { SSL_CTX_free(ctx_); ctx_ = nullptr; }
        if (fd_ >= 0) { ::close(fd_); fd_ = -1; }
    }

    void connect_tcp(const std::string& host, int port, int timeout_s) {
        addrinfo hints{}; hints.ai_family = AF_UNSPEC; hints.ai_socktype = SOCK_STREAM;
        addrinfo* res = nullptr;
        const std::string port_s = std::to_string(port);
        int rc = getaddrinfo(host.c_str(), port_s.c_str(), &hints, &res);
        if (rc != 0) fail("postgres: cannot resolve host '" + host + "': " + gai_strerror(rc));
        int last_errno = 0;
        for (addrinfo* ai = res; ai; ai = ai->ai_next) {
            int s = socket(ai->ai_family, ai->ai_socktype, ai->ai_protocol);
            if (s < 0) { last_errno = errno; continue; }
            timeval tv{}; tv.tv_sec = timeout_s;
            setsockopt(s, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof tv);
            setsockopt(s, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof tv);
            int one = 1;
            setsockopt(s, IPPROTO_TCP, TCP_NODELAY, &one, sizeof one);
            setsockopt(s, SOL_SOCKET, SO_KEEPALIVE, &one, sizeof one);
#ifdef SO_NOSIGPIPE
            // macOS/BSD: EPIPE instead of SIGPIPE, for every write on this
            // socket including the ones OpenSSL makes. See kSendFlags.
            setsockopt(s, SOL_SOCKET, SO_NOSIGPIPE, &one, sizeof one);
#endif
            // SO_KEEPALIVE alone inherits the system idle time -- two hours on
            // Linux -- which is useless for noticing that a pooled connection's
            // peer has gone away. Probing after a minute idle, then three
            // probes ten seconds apart, means a connection dropped by a pooler
            // or a NAT is detected by the kernel in about 90 seconds rather
            // than on next use. That does not replace the liveness check and
            // retry in PgPool::acquire -- keepalive cannot cover the gap
            // between a check and the write that follows it -- it just means
            // fewer dead connections are still in the pool to be checked.
            int keep_idle = 60, keep_intvl = 10, keep_cnt = 3;
            (void)keep_intvl; (void)keep_cnt;
#if defined(TCP_KEEPIDLE)
            setsockopt(s, IPPROTO_TCP, TCP_KEEPIDLE, &keep_idle, sizeof keep_idle);
#elif defined(TCP_KEEPALIVE)
            // macOS spells the idle time TCP_KEEPALIVE; same units, same meaning.
            setsockopt(s, IPPROTO_TCP, TCP_KEEPALIVE, &keep_idle, sizeof keep_idle);
#endif
#ifdef TCP_KEEPINTVL
            setsockopt(s, IPPROTO_TCP, TCP_KEEPINTVL, &keep_intvl, sizeof keep_intvl);
#endif
#ifdef TCP_KEEPCNT
            setsockopt(s, IPPROTO_TCP, TCP_KEEPCNT, &keep_cnt, sizeof keep_cnt);
#endif
            if (::connect(s, ai->ai_addr, ai->ai_addrlen) == 0) { fd_ = s; break; }
            last_errno = errno;
            ::close(s);
        }
        freeaddrinfo(res);
        if (fd_ < 0)
            fail("postgres: cannot connect to " + host + ":" + port_s + ": " + strerror(last_errno));
    }

    void start_tls(const std::string& host, bool verify) {
        static const uint8_t req[8] = {0, 0, 0, 8, 0x04, 0xd2, 0x16, 0x2f};  // SSLRequest
        write_all(req, 8);
        uint8_t reply;
        read_exact(&reply, 1);
        if (reply != 'S') fail("postgres: server refused TLS but sslmode requires it");
        ctx_ = SSL_CTX_new(TLS_client_method());
        if (!ctx_) fail("postgres TLS: SSL_CTX_new: " + ssl_err_text());
        SSL_CTX_set_min_proto_version(ctx_, TLS1_2_VERSION);
        if (verify) {
            SSL_CTX_set_default_verify_paths(ctx_);
            SSL_CTX_set_verify(ctx_, SSL_VERIFY_PEER, nullptr);
        }
        ssl_ = SSL_new(ctx_);
        if (!ssl_) fail("postgres TLS: SSL_new: " + ssl_err_text());
        SSL_set_tlsext_host_name(ssl_, host.c_str());
        if (verify) SSL_set1_host(ssl_, host.c_str());
        SSL_set_fd(ssl_, fd_);
        if (SSL_connect(ssl_) != 1) fail("postgres TLS handshake failed: " + ssl_err_text());
    }

    void write_all(const uint8_t* p, size_t n) {
        while (n > 0) {
            ssize_t w =
                ssl_ ? (ssize_t)SSL_write(ssl_, p, (int)n) : ::send(fd_, p, n, kSendFlags);
            if (w <= 0)
                fail_transport(ssl_ ? "postgres TLS write failed: " + ssl_err_text()
                                    : std::string("postgres write failed: ") + strerror(errno));
            p += w; n -= (size_t)w;
        }
    }

    // UNBUFFERED exact read. The ONLY caller is start_tls, which must take the
    // single-byte SSLRequest reply without reading a byte further: the bytes
    // after it are the TLS handshake, and a buffered read that swallowed them
    // would hand plaintext-side bytes to a session that is now encrypted. Every
    // read after the handshake goes through peek/consume below.
    void read_exact(uint8_t* p, size_t n) {
        while (n > 0) {
            ssize_t r = ssl_ ? (ssize_t)SSL_read(ssl_, p, (int)n) : ::recv(fd_, p, n, 0);
            if (r <= 0) read_failed(r);
            p += r; n -= (size_t)r;
        }
    }

    // Ensure `n` bytes are buffered contiguously and return a pointer to them.
    // The buffer is refilled with whole socket reads rather than one read per
    // message, which is what keeps a 1.5M-row stream from costing two syscalls
    // and one allocation per row. The returned pointer is invalidated by the
    // next peek (which may compact the buffer or refill over the consumed
    // prefix), so a caller that needs bytes to outlive its next read copies
    // them.
    const uint8_t* peek(size_t n) {
        if (rend_ - rpos_ >= n) return rbuf_.data() + rpos_;
        // Slide the unread tail to the front so the free space is one run.
        if (rpos_ > 0) {
            const size_t left = rend_ - rpos_;
            if (left > 0) std::memmove(rbuf_.data(), rbuf_.data() + rpos_, left);
            rpos_ = 0;
            rend_ = left;
        }
        // One message larger than the buffer (a wide row, a big bytea): grow to
        // fit it. The buffer keeps that capacity for the rest of the stream.
        if (rbuf_.size() < n) rbuf_.resize(n);
        while (rend_ - rpos_ < n) {
            const size_t space = rbuf_.size() - rend_;
            ssize_t r = ssl_ ? (ssize_t)SSL_read(ssl_, rbuf_.data() + rend_, (int)space)
                             : ::recv(fd_, rbuf_.data() + rend_, space, 0);
            if (r <= 0) read_failed(r);
            rend_ += (size_t)r;
        }
        return rbuf_.data() + rpos_;
    }

    void consume(size_t n) {
        rpos_ += n;
        if (rpos_ == rend_) { rpos_ = 0; rend_ = 0; }
    }

    void send(const std::vector<uint8_t>& b) { write_all(b.data(), b.size()); }

    // Is this socket still usable? For an IDLE connection only.
    //
    // A connection parked in the pool should have nothing to say: the last
    // statement drained to ReadyForQuery, and this client never issues LISTEN,
    // so the server has no reason to send unprompted. Anything readable on it
    // is therefore either the close itself (recv returns 0 on FIN, -1/ECONNRESET
    // on RST) or bytes that mean the protocol state is not what we believe --
    // and a connection whose state is in doubt is worth no more than a dead one.
    // Both answer false.
    //
    // MSG_PEEK leaves whatever it saw in the kernel buffer, so this does not
    // disturb the byte stream, TLS included: the peek reads ciphertext, which
    // OpenSSL still gets to read for itself afterwards.
    bool alive() const {
        if (fd_ < 0) return false;
        if (rend_ > rpos_) return false;  // buffered leftovers: state in doubt
        pollfd pfd{};
        pfd.fd = fd_;
        pfd.events = POLLIN;
        const int rc = ::poll(&pfd, 1, 0);
        if (rc < 0) return false;
        if (rc == 0) return true;  // nothing pending: the healthy idle case
        if (pfd.revents & (POLLHUP | POLLERR | POLLNVAL)) return false;
        uint8_t probe = 0;
        const ssize_t r = ::recv(fd_, &probe, 1, MSG_PEEK | MSG_DONTWAIT);
        if (r == 0) return false;  // orderly close
        if (r < 0) return errno == EAGAIN || errno == EWOULDBLOCK;
        return false;  // unexpected data on an idle connection
    }

private:
    [[noreturn]] void read_failed(ssize_t r) {
        fail_transport(ssl_ ? "postgres TLS read failed: " + ssl_err_text()
                            : std::string("postgres read failed: ") +
                                  (r == 0 ? "connection closed by server" : strerror(errno)));
    }

    // 256 KiB: large enough that a narrow-row stream refills a few times per
    // thousand rows, small enough to be irrelevant next to a morsel.
    static constexpr size_t kReadBufBytes = 256u * 1024u;

    int fd_ = -1;
    SSL_CTX* ctx_ = nullptr;
    SSL* ssl_ = nullptr;
    std::vector<uint8_t> rbuf_ = std::vector<uint8_t>(kReadBufBytes);
    size_t rpos_ = 0;   // first unread byte
    size_t rend_ = 0;   // one past the last byte read from the socket
};

// ---------------------------------------------------------------------------
// Messages
// ---------------------------------------------------------------------------

// The protocol's own ceiling: a length field is int32 and the backend never
// sends a message at 1 GB. A bogus length is refused here rather than sizing
// the read buffer from it.
static constexpr int32_t kMaxMsgLen = 1024 * 1024 * 1024;

PgConnection::Msg PgConnection::read_msg() {
    const int32_t len = (int32_t)be32(t_->peek(5) + 1);
    if (len < 4) fail("postgres protocol: bad message length");
    if (len > kMaxMsgLen) fail("postgres protocol: message length out of range");
    const size_t total = 5u + (size_t)len - 4u;
    // Re-peek for the whole message: this may compact or grow the buffer, so
    // the header pointer above cannot be reused.
    const uint8_t* p = t_->peek(total);
    Msg m;
    m.type = (char)p[0];
    m.payload = p + 5;
    m.len = (size_t)len - 4;
    t_->consume(total);
    return m;
}

static void parse_error_fields(const uint8_t* payload, size_t payload_len, std::string& severity,
                               std::string& code, std::string& message, std::string& detail,
                               std::string& position) {
    Reader r(payload, payload_len);
    while (!r.done()) {
        uint8_t f = r.u8();
        if (f == 0) break;
        std::string v = r.cstr();
        switch (f) {
            case 'S': severity = v; break;
            case 'C': code = v; break;
            case 'M': message = v; break;
            case 'D': detail = v; break;
            case 'P': position = v; break;
            default: break;
        }
    }
}

void PgConnection::raise_server_error(const uint8_t* payload, size_t len) {
    std::string severity, code, message, detail, position;
    parse_error_fields(payload, len, severity, code, message, detail, position);
    std::string text = "postgres " + severity + " [" + code + "]: " + message;
    if (!detail.empty()) text += " (" + detail + ")";
    if (!position.empty()) text += " at position " + position;
    throw PgError(text, code);
}

// ---------------------------------------------------------------------------
// Connection / auth
// ---------------------------------------------------------------------------

PgConnection::PgConnection(const PgConfig& config) : t_(std::make_unique<Transport>()) {
    if (config.host.empty()) fail("postgres: host is required");
    if (config.dbname.empty()) fail("postgres: dbname is required");
    if (config.user.empty()) fail("postgres: user is required");
    t_->connect_tcp(config.host, config.port, config.timeout_s);
    if (config.sslmode == "require") t_->start_tls(config.host, false);
    else if (config.sslmode == "verify-full") t_->start_tls(config.host, true);
    else if (config.sslmode != "disable")
        fail("postgres: sslmode must be disable, require or verify-full (got '" + config.sslmode + "')");
    startup(config);
}

bool PgConnection::alive() const {
    // Mid-stream there are bytes in flight by definition, so the idle-socket
    // reasoning in Transport::alive does not hold; and an unhealthy connection
    // is already disqualified.
    if (!healthy_ || streaming_ || !t_) return false;
    return t_->alive();
}

PgConnection::~PgConnection() {
    if (t_ && healthy_) {
        try {
            Out o;
            t_->send(frame('X', o));  // Terminate
        } catch (...) {
        }
    }
}

void PgConnection::send_password(const std::string& pw) {
    Out o; o.cstr(pw);
    t_->send(frame('p', o));
}

static std::string hex(const uint8_t* p, size_t n) {
    static const char* h = "0123456789abcdef";
    std::string s; s.reserve(2 * n);
    for (size_t i = 0; i < n; i++) { s += h[p[i] >> 4]; s += h[p[i] & 15]; }
    return s;
}

static std::string md5_hex(const std::string& s) {
    uint8_t d[16]; unsigned int n = 16;
    EVP_Digest(s.data(), s.size(), d, &n, EVP_md5(), nullptr);
    return hex(d, 16);
}

void PgConnection::scram_sha256(const Msg& first, const PgConfig& config) {
    Reader r(first.payload, first.len);
    r.i32();  // auth code 10
    bool have = false;
    while (!r.done()) { std::string m = r.cstr(); if (m.empty()) break; if (m == "SCRAM-SHA-256") have = true; }
    if (!have) fail("postgres auth: server offers SASL but not SCRAM-SHA-256");

    uint8_t nonce_raw[18];
    if (RAND_bytes(nonce_raw, sizeof nonce_raw) != 1) fail("postgres auth: RAND_bytes failed");
    const std::string cnonce = b64_encode(nonce_raw, sizeof nonce_raw);
    const std::string bare = "n=,r=" + cnonce;
    const std::string client_first = "n,," + bare;

    Out o; o.cstr("SCRAM-SHA-256"); o.i32((int32_t)client_first.size()); o.bytes(client_first);
    t_->send(frame('p', o));

    Msg m = read_msg();
    if (m.type == 'E') raise_server_error(m.payload, m.len);
    if (m.type != 'R') fail("postgres auth: unexpected message during SASL exchange");
    Reader r2(m.payload, m.len);
    if (r2.i32() != 11) fail("postgres auth: expected SASLContinue");
    const std::string server_first((const char*)m.payload + 4, m.len - 4);

    std::string snonce, salt_b64; int iters = 0;
    size_t pos = 0;
    while (pos < server_first.size()) {
        size_t comma = server_first.find(',', pos);
        std::string attr = server_first.substr(pos, comma == std::string::npos ? std::string::npos : comma - pos);
        if (attr.rfind("r=", 0) == 0) snonce = attr.substr(2);
        else if (attr.rfind("s=", 0) == 0) salt_b64 = attr.substr(2);
        else if (attr.rfind("i=", 0) == 0) iters = std::stoi(attr.substr(2));
        if (comma == std::string::npos) break;
        pos = comma + 1;
    }
    if (snonce.rfind(cnonce, 0) != 0) fail("postgres auth: server nonce does not extend the client nonce");
    if (iters <= 0) fail("postgres auth: bad SCRAM iteration count");
    const std::vector<uint8_t> salt = b64_decode(salt_b64);

    uint8_t salted[32];
    if (PKCS5_PBKDF2_HMAC(config.password.data(), (int)config.password.size(), salt.data(),
                          (int)salt.size(), iters, EVP_sha256(), 32, salted) != 1)
        fail("postgres auth: PBKDF2 failed");

    uint8_t client_key[32], stored_key[32], client_sig[32], server_key[32], server_sig[32];
    unsigned int len = 32;
    HMAC(EVP_sha256(), salted, 32, (const uint8_t*)"Client Key", 10, client_key, &len);
    SHA256(client_key, 32, stored_key);

    const std::string client_final_bare = "c=biws,r=" + snonce;
    const std::string auth_message = bare + "," + server_first + "," + client_final_bare;
    HMAC(EVP_sha256(), stored_key, 32, (const uint8_t*)auth_message.data(), auth_message.size(), client_sig, &len);
    uint8_t proof[32];
    for (int i = 0; i < 32; i++) proof[i] = client_key[i] ^ client_sig[i];
    const std::string client_final = client_final_bare + ",p=" + b64_encode(proof, 32);

    Out o2; o2.bytes(client_final);
    t_->send(frame('p', o2));

    Msg f = read_msg();
    if (f.type == 'E') raise_server_error(f.payload, f.len);
    if (f.type != 'R') fail("postgres auth: unexpected message awaiting SASLFinal");
    Reader r3(f.payload, f.len);
    if (r3.i32() != 12) fail("postgres auth: expected SASLFinal");
    const std::string server_final((const char*)f.payload + 4, f.len - 4);
    if (server_final.rfind("v=", 0) != 0) fail("postgres auth: SASLFinal carries no server verifier");
    const std::vector<uint8_t> v = b64_decode(server_final.substr(2));
    HMAC(EVP_sha256(), salted, 32, (const uint8_t*)"Server Key", 10, server_key, &len);
    HMAC(EVP_sha256(), server_key, 32, (const uint8_t*)auth_message.data(), auth_message.size(), server_sig, &len);
    if (v.size() != 32 || memcmp(v.data(), server_sig, 32) != 0)
        fail("postgres auth: server signature mismatch (possible impersonation)");
}

void PgConnection::startup(const PgConfig& config) {
    Out o;
    o.i32(196608);  // protocol 3.0
    o.cstr("user"); o.cstr(config.user);
    o.cstr("database"); o.cstr(config.dbname);
    o.cstr("client_encoding"); o.cstr("UTF8");
    o.cstr("application_name"); o.cstr("opteryx");
    // The engine's timestamps are UTC. Pinning the session zone makes a pushed
    // timestamp literal (sent as text) mean the same instant the engine meant,
    // and keeps timestamptz binary output (always UTC micros) consistent with
    // how the server parsed our parameters.
    o.cstr("TimeZone"); o.cstr("UTC");
    o.cstr("DateStyle"); o.cstr("ISO, YMD");
    o.u8(0);
    Out framed;
    framed.i32((int32_t)(o.b.size() + 4));
    framed.b.insert(framed.b.end(), o.b.begin(), o.b.end());
    t_->send(framed.b);

    for (;;) {
        Msg m = read_msg();
        switch (m.type) {
            case 'R': {
                Reader r(m.payload, m.len);
                const int32_t code = r.i32();
                switch (code) {
                    case 0: break;                                    // AuthenticationOk
                    case 3: send_password(config.password); break;    // cleartext
                    case 5: {                                         // md5
                        const uint8_t* salt = r.bytes(4);
                        const std::string inner = md5_hex(config.password + config.user);
                        const std::string outer = md5_hex(inner + std::string((const char*)salt, 4));
                        send_password("md5" + outer);
                        break;
                    }
                    case 10: scram_sha256(m, config); break;          // SASL
                    default:
                        fail("postgres auth: unsupported authentication method (code " +
                             std::to_string(code) + "); supported: cleartext, md5, SCRAM-SHA-256");
                }
                break;
            }
            case 'E': raise_server_error(m.payload, m.len);
            case 'S': { Reader r(m.payload, m.len); std::string k = r.cstr(); params_[k] = r.cstr(); break; }
            case 'K': break;   // BackendKeyData (cancel is not implemented)
            case 'N': break;   // NoticeResponse
            case 'Z': {
                auto it = params_.find("server_version");
                if (it != params_.end()) server_version_ = it->second;
                return;
            }
            default:
                fail(std::string("postgres protocol: unexpected message '") + m.type + "' during startup");
        }
    }
}

// ---------------------------------------------------------------------------
// Extended query
// ---------------------------------------------------------------------------

void PgConnection::send_extended(const std::string& sql,
                                 const std::vector<std::optional<std::string>>& params,
                                 bool binary_results, bool describe_portal) {
    if (params.size() > 32767) fail("postgres: too many bind parameters");
    Out parse; parse.cstr(""); parse.cstr(sql); parse.i16(0);
    Out bind;
    bind.cstr(""); bind.cstr("");
    bind.i16(0);                                     // parameter format codes: all text
    bind.i16((int16_t)params.size());
    for (const auto& p : params) {
        if (!p.has_value()) { bind.i32(-1); continue; }
        bind.i32((int32_t)p->size());
        bind.bytes(*p);
    }
    bind.i16(1); bind.i16(binary_results ? 1 : 0);   // one result format code for all columns
    Out desc; desc.u8(describe_portal ? 'P' : 'S'); desc.cstr("");
    Out exec; exec.cstr(""); exec.i32(0);
    Out sync;

    std::vector<uint8_t> wire;
    auto add = [&](char t, const Out& body) { auto f = frame(t, body); wire.insert(wire.end(), f.begin(), f.end()); };
    add('P', parse);
    if (describe_portal) {
        add('B', bind);
        add('D', desc);
        add('E', exec);
    } else {
        add('D', desc);  // Describe(statement): no bind, no execute
    }
    add('S', sync);
    t_->send(wire);
}

std::vector<PgField> PgConnection::parse_row_description(const Msg& m) {
    Reader r(m.payload, m.len);
    const int16_t n = r.i16();
    std::vector<PgField> fields;
    fields.reserve((size_t)n);
    for (int16_t i = 0; i < n; i++) {
        PgField f;
        f.name = r.cstr();
        r.i32(); r.i16();                 // table oid, attnum
        f.oid = (uint32_t)r.i32();
        r.i16();                          // typlen
        f.typmod = r.i32();
        r.i16();                          // format code (0 for a statement Describe)
        fields.push_back(std::move(f));
    }
    return fields;
}

std::vector<PgField> PgConnection::describe(const std::string& sql) {
    if (streaming_) fail("postgres: describe() called while a result stream is open");
    healthy_ = false;  // until we see ReadyForQuery again
    send_extended(sql, {}, false, false);
    std::vector<PgField> fields;
    // The error is raised only once ReadyForQuery leaves the session reusable,
    // so its payload has to survive the reads in between: this is one of the
    // four sites that copy a message out of the read buffer.
    std::vector<uint8_t> pending_error;
    bool have_error = false;
    for (;;) {
        Msg m = read_msg();
        switch (m.type) {
            case '1': case 't': case 'n': case 'N': break;   // ParseComplete, ParameterDescription, NoData, Notice
            case 'T': fields = parse_row_description(m); break;
            case 'E': pending_error.assign(m.payload, m.payload + m.len); have_error = true; break;
            case 'Z':
                healthy_ = true;
                if (have_error) raise_server_error(pending_error.data(), pending_error.size());
                return fields;
            default:
                fail(std::string("postgres protocol: unexpected message '") + m.type + "' in describe");
        }
    }
}

std::vector<std::vector<std::optional<std::string>>> PgConnection::query_text(
    const std::string& sql, const std::vector<std::string>& params) {
    if (streaming_) fail("postgres: query_text() called while a result stream is open");
    std::vector<std::optional<std::string>> ps;
    ps.reserve(params.size());
    for (const auto& p : params) ps.emplace_back(p);
    healthy_ = false;
    send_extended(sql, ps, false, true);
    std::vector<std::vector<std::optional<std::string>>> rows;
    std::vector<uint8_t> pending_error;   // see describe(): outlives later reads
    bool have_error = false;
    for (;;) {
        Msg m = read_msg();
        switch (m.type) {
            case '1': case '2': case 'n': case 'N': case 'T': break;
            case 'D': {
                Reader r(m.payload, m.len);
                const int16_t n = r.i16();
                std::vector<std::optional<std::string>> row;
                row.reserve((size_t)n);
                for (int16_t i = 0; i < n; i++) {
                    const int32_t len = r.i32();
                    if (len < 0) row.emplace_back(std::nullopt);
                    else { const uint8_t* b = r.bytes((size_t)len); row.emplace_back(std::string((const char*)b, (size_t)len)); }
                }
                rows.push_back(std::move(row));
                break;
            }
            case 'C': { Reader r(m.payload, m.len); command_tag_ = r.cstr(); break; }
            case 'E': pending_error.assign(m.payload, m.payload + m.len); have_error = true; break;
            case 'Z':
                healthy_ = true;
                if (have_error) raise_server_error(pending_error.data(), pending_error.size());
                return rows;
            default:
                fail(std::string("postgres protocol: unexpected message '") + m.type + "' in query");
        }
    }
}

std::vector<PgField> PgConnection::begin(const std::string& sql,
                                         const std::vector<std::optional<std::string>>& params) {
    if (streaming_) fail("postgres: begin() called while a result stream is open");
    healthy_ = false;
    streaming_ = true;
    command_tag_.clear();
    send_extended(sql, params, true, true);
    // Read up to the RowDescription (or NoData). A server error before the first
    // row is raised here after draining to ReadyForQuery.
    for (;;) {
        Msg m = read_msg();
        switch (m.type) {
            case '1': case '2': case 'N': break;
            case 'T': {
                auto fields = parse_row_description(m);
                Reader r(m.payload, m.len);
                const int16_t n = r.i16();
                for (int16_t i = 0; i < n; i++) {
                    r.cstr(); r.i32(); r.i16(); r.i32(); r.i16(); r.i32();
                    if (r.i16() != 1) {
                        finish();
                        fail("postgres protocol: server did not honour binary result format for column '" +
                             fields[(size_t)i].name + "'");
                    }
                }
                return fields;
            }
            case 'n': return {};                        // NoData: no result columns
            case 'E': {
                // finish() reads on, so the payload has to be copied out first.
                const std::vector<uint8_t> err(m.payload, m.payload + m.len);
                finish();                               // drain to ReadyForQuery
                raise_server_error(err.data(), err.size());
            }
            default:
                streaming_ = false;
                fail(std::string("postgres protocol: unexpected message '") + m.type + "' starting a stream");
        }
    }
}

bool PgConnection::next_row(const uint8_t** payload, size_t* length) {
    if (!streaming_) return false;
    for (;;) {
        Msg m = read_msg();
        switch (m.type) {
            case 'D':
                // Straight out of the read buffer: no copy, no allocation. The
                // pointer is live until the caller's next next_row(), which is
                // exactly what this function documents.
                *payload = m.payload;
                *length = m.len;
                return true;
            case 'N': break;
            case 'C': { Reader r(m.payload, m.len); command_tag_ = r.cstr(); break; }
            case 's': break;                            // PortalSuspended (not used: no row cap)
            case 'E': {
                const std::vector<uint8_t> err(m.payload, m.payload + m.len);
                finish();
                raise_server_error(err.data(), err.size());
            }
            case 'Z':
                streaming_ = false;
                healthy_ = true;
                return false;
            default:
                streaming_ = false;
                fail(std::string("postgres protocol: unexpected message '") + m.type + "' in a result stream");
        }
    }
}

void PgConnection::finish() {
    if (!streaming_) return;
    // The portal keeps producing until the server finishes the statement; there
    // is no cancel here, so an early abandon drains the rest. Row-limited scans
    // put the LIMIT in the SQL, so this is short in practice.
    try {
        for (;;) {
            Msg m = read_msg();
            if (m.type == 'Z') { streaming_ = false; healthy_ = true; return; }
        }
    } catch (...) {
        streaming_ = false;
        healthy_ = false;
        throw;
    }
}

// ---------------------------------------------------------------------------
// Pool
// ---------------------------------------------------------------------------

PgPool& PgPool::instance() {
    static PgPool* pool = new PgPool();  // never destroyed: outlives every worker thread
    return *pool;
}

// Idle connections are vetted on the way OUT rather than on the way in: a
// connection is fine when it is released and may be dead by the time it is
// wanted, so the check is only worth anything at the moment of reuse.
//
// Taking the newest first (back of the bucket) is deliberate -- it is the least
// likely to have aged out, and it keeps the oldest entries ageing quietly
// towards expiry instead of being cycled back into service.
std::unique_ptr<PgConnection> PgPool::acquire(const PgConfig& config) {
    const auto now = std::chrono::steady_clock::now();
    for (;;) {
        std::unique_ptr<PgConnection> candidate;
        {
            std::lock_guard<std::mutex> lock(mtx_);
            auto it = idle_.find(config.key());
            if (it == idle_.end() || it->second.empty()) break;
            candidate = std::move(it->second.back());
            it->second.pop_back();
        }
        // Outside the lock: closing a connection talks to the server (Terminate)
        // and must not hold up every other thread's acquire while it does.
        const bool expired = now - candidate->idle_since >= kMaxIdleSeconds;
        if (expired || !candidate->alive()) {
            candidate.reset();
            continue;  // try the next one down; the bucket may still hold a live one
        }
        candidate->set_pooled(true);
        return candidate;
    }
    return acquire_fresh(config);
}

std::unique_ptr<PgConnection> PgPool::acquire_fresh(const PgConfig& config) {
    auto conn = std::make_unique<PgConnection>(config);
    conn->set_pooled(false);
    return conn;
}

void PgPool::release(const PgConfig& config, std::unique_ptr<PgConnection> conn) {
    if (!conn || !conn->healthy()) return;  // dropped: destructor sends Terminate if it can
    conn->idle_since = std::chrono::steady_clock::now();
    conn->set_pooled(false);  // re-stamped by acquire; never stale from a past hand-out
    std::lock_guard<std::mutex> lock(mtx_);
    auto& bucket = idle_[config.key()];
    if (bucket.size() >= kMaxIdlePerKey) return;
    bucket.push_back(std::move(conn));
}

// A server-side error (bad relation name, permission) leaves the session usable —
// the client drained to ReadyForQuery before raising — so the connection goes
// back to the pool on that path too; pg_with_retry's release drops an unhealthy
// one, and only a transport failure on a pooled connection earns a second try.
std::vector<PgField> pg_describe(const PgConfig& config, const std::string& sql) {
    return pg_with_retry(config, [&](PgConnection& conn) { return conn.describe(sql); });
}

std::vector<std::vector<std::optional<std::string>>> pg_query_text(
    const PgConfig& config, const std::string& sql, const std::vector<std::string>& params) {
    return pg_with_retry(config,
                         [&](PgConnection& conn) { return conn.query_text(sql, params); });
}

}  // namespace opteryx::pg
