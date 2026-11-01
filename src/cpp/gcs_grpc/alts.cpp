#include "alts.hpp"

#include <algorithm>
#include <cerrno>
#include <chrono>
#include <cstring>
#include <stdexcept>

#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

#include "pb_wire.hpp"
#include "grpc_alts/alts_iovec_record_protocol.h"
#include "grpc_alts/gsec.h"

#ifndef MSG_NOSIGNAL
#define MSG_NOSIGNAL 0  // Apple: SO_NOSIGPIPE is set by connect_tcp
#endif

namespace gcs_grpc {

namespace {

constexpr const char* kApplicationProtocol = "grpc";
constexpr const char* kRecordProtocol = "ALTSRP_GCM_AES128_REKEY";
constexpr const char* kHandshakePath = "/grpc.gcp.HandshakerService/DoHandshake";
constexpr size_t kRekeyKeyLength = 44;
// Bound on a received frame; gRPC's own protector refuses larger.
constexpr size_t kMaxReceivedFrame = 16 * 1024 * 1024;

std::string take_error(char* details, const char* what) {
    std::string out = std::string("ALTS ") + what + (details ? std::string(": ") + details : std::string());
    std::free(details);
    return out;
}

// HandshakerReq { next = 3: NextHandshakeMessageReq { in_bytes = 1 } }
std::string next_request(const std::string& in_bytes) {
    std::string next;
    pb::put_bytes(next, 1, in_bytes);
    std::string req;
    pb::put_bytes(req, 3, next);
    return req;
}

struct HandshakerResp {
    std::string out_frames;
    uint64_t bytes_consumed = 0;
    bool has_result = false;
    std::string application_protocol, record_protocol, key_data, peer_service_account;
    uint64_t max_frame_size = 0;
    uint64_t status_code = 0;
    std::string status_details;
};

// HandshakerResp { out_frames=1, bytes_consumed=2, result=3, status=4 }
// HandshakerResult { application_protocol=1, record_protocol=2, key_data=3, peer_identity=4, max_frame_size=8 }
// Identity { service_account=1 }; HandshakerStatus { code=1, details=2 }
HandshakerResp parse_resp(const std::string& msg) {
    HandshakerResp r;
    pb::Reader rd(reinterpret_cast<const uint8_t*>(msg.data()), msg.size());
    pb::Field f;
    while (rd.next(f)) {
        if (f.number == 1 && f.type == pb::kLen) r.out_frames.assign(f.view());
        else if (f.number == 2 && f.type == pb::kVarint) r.bytes_consumed = f.u;
        else if (f.number == 3 && f.type == pb::kLen) {
            r.has_result = true;
            pb::Reader res(f.p, f.n);
            pb::Field g;
            while (res.next(g)) {
                if (g.number == 8 && g.type == pb::kVarint) r.max_frame_size = g.u;
                if (g.type != pb::kLen) continue;
                if (g.number == 1) r.application_protocol.assign(g.view());
                else if (g.number == 2) r.record_protocol.assign(g.view());
                else if (g.number == 3) r.key_data.assign(g.view());
                else if (g.number == 4) {
                    pb::Reader id(g.p, g.n);
                    pb::Field h;
                    while (id.next(h))
                        if (h.number == 1 && h.type == pb::kLen) r.peer_service_account.assign(h.view());
                }
            }
        } else if (f.number == 4 && f.type == pb::kLen) {
            pb::Reader st(f.p, f.n);
            pb::Field g;
            while (st.next(g)) {
                if (g.number == 1 && g.type == pb::kVarint) r.status_code = g.u;
                else if (g.number == 2 && g.type == pb::kLen) r.status_details.assign(g.view());
            }
        }
    }
    return r;
}

void write_all(int fd, const std::string& bytes, int timeout_ms) {
    size_t at = 0;
    while (at < bytes.size()) {
        pollfd p{fd, POLLOUT, 0};
        if (poll(&p, 1, timeout_ms) != 1) throw std::runtime_error("ALTS handshake: timed out writing to the peer");
        const ssize_t w = ::send(fd, bytes.data() + at, bytes.size() - at, MSG_NOSIGNAL);
        if (w < 0 && errno != EAGAIN && errno != EINTR)
            throw std::runtime_error(std::string("ALTS handshake: write to peer: ") + std::strerror(errno));
        if (w > 0) at += static_cast<size_t>(w);
    }
}

std::string read_some(int fd, int timeout_ms) {
    pollfd p{fd, POLLIN, 0};
    if (poll(&p, 1, timeout_ms) != 1) throw std::runtime_error("ALTS handshake: timed out waiting for the peer");
    char buf[16384];
    const ssize_t r = ::recv(fd, buf, sizeof(buf), 0);
    if (r == 0) throw std::runtime_error("ALTS handshake: peer closed the connection");
    if (r < 0) throw std::runtime_error(std::string("ALTS handshake: read from peer: ") + std::strerror(errno));
    return std::string(buf, static_cast<size_t>(r));
}

alts_iovec_record_protocol* make_protocol(const std::string& key_data, bool is_client, bool is_protect) {
    grpc_alts::GsecKeyFactory factory(
        grpc_alts::Span<const uint8_t>(reinterpret_cast<const uint8_t*>(key_data.data()), kRekeyKeyLength),
        /*is_rekey=*/true);
    gsec_aead_crypter* crypter = nullptr;
    char* details = nullptr;
    if (gsec_aes_gcm_aead_crypter_create(factory.Create(), kAesGcmNonceLength, kAesGcmTagLength, &crypter,
                                         &details) != GRPC_STATUS_OK)
        throw std::runtime_error(take_error(details, "crypter"));
    alts_iovec_record_protocol* rp = nullptr;
    if (alts_iovec_record_protocol_create(crypter, kAltsRecordProtocolRekeyFrameLimit, is_client,
                                          /*is_integrity_only=*/false, is_protect, &rp, &details) != GRPC_STATUS_OK) {
        gsec_aead_crypter_destroy(crypter);
        throw std::runtime_error(take_error(details, "record protocol"));
    }
    return rp;
}

}  // namespace

std::string alts_client_start_request(const std::string& target_name) {
    // RpcProtocolVersions { max_rpc_version=1, min_rpc_version=2 }, each Version { major=1, minor=2 } = 2.1
    std::string version;
    pb::put_uint(version, 1, 2);
    pb::put_uint(version, 2, 1);
    std::string versions;
    pb::put_bytes(versions, 1, version);
    pb::put_bytes(versions, 2, version);
    // StartClientHandshakeReq
    std::string start;
    pb::put_uint(start, 1, 2);  // handshake_security_protocol = ALTS
    pb::put_bytes(start, 2, kApplicationProtocol);
    pb::put_bytes(start, 3, kRecordProtocol);
    pb::put_bytes(start, 8, target_name);
    pb::put_bytes(start, 9, versions);
    pb::put_uint(start, 10, kAltsMaxFrameSize);
    std::string req;
    pb::put_bytes(req, 1, start);  // HandshakerReq.client_start
    return req;
}

AltsHandshakeResult alts_client_handshake(int fd, GrpcConnection& handshaker, const std::string& target_name,
                                          int timeout_ms) {
    MessageQueue q;
    std::string body;
    GrpcConnection::frame_message(body, alts_client_start_request(target_name));
    const uint64_t call = handshaker.start_call(kHandshakePath, {}, std::move(body), &q, /*half_close=*/false);
    // The handler must outlive the call: every exit cancels and waits for on_close.
    struct Finish {
        GrpcConnection& h;
        uint64_t id;
        MessageQueue& q;
        ~Finish() {
            h.cancel(id);
            q.wait_closed();
        }
    } finish{handshaker, call, q};

    const auto timeout = std::chrono::milliseconds(timeout_ms);
    std::string last_in;  // the peer bytes the newest handshaker response answers
    for (int round = 0; round < 16; ++round) {
        std::string msg;
        if (!q.next(msg, timeout)) {
            if (q.timed_out()) throw std::runtime_error("ALTS handshake: handshaker service did not answer");
            throw std::runtime_error("ALTS handshake: handshaker stream ended: status " + std::to_string(q.status()) +
                                     " " + q.message());
        }
        HandshakerResp r = parse_resp(msg);
        if (r.status_code != 0)
            throw std::runtime_error("ALTS handshake failed: status " + std::to_string(r.status_code) + " " +
                                     r.status_details);
        if (!r.out_frames.empty()) write_all(fd, r.out_frames, timeout_ms);
        if (r.has_result) {
            if (r.application_protocol != kApplicationProtocol)
                throw std::runtime_error("ALTS handshake: unexpected application protocol '" + r.application_protocol + "'");
            if (r.record_protocol != kRecordProtocol)
                throw std::runtime_error("ALTS handshake: unexpected record protocol '" + r.record_protocol + "'");
            if (r.key_data.size() < kRekeyKeyLength) throw std::runtime_error("ALTS handshake: key data too short");
            if (r.peer_service_account.empty()) throw std::runtime_error("ALTS handshake: peer has no service account");
            AltsHandshakeResult out;
            out.key_data = r.key_data.substr(0, kRekeyKeyLength);
            out.max_frame_size = r.max_frame_size == 0
                                     ? kAltsMinFrameSize
                                     : std::max(kAltsMinFrameSize, std::min<size_t>(r.max_frame_size, kAltsMaxFrameSize));
            out.peer_service_account = r.peer_service_account;
            if (r.bytes_consumed > last_in.size()) throw std::runtime_error("ALTS handshake: consumed more than it was given");
            out.leftover = last_in.substr(static_cast<size_t>(r.bytes_consumed));
            return out;
        }
        if (r.bytes_consumed != last_in.size())
            throw std::runtime_error("ALTS handshake: handshaker left peer bytes unconsumed mid-handshake");
        // The handshaker wants the peer's next bytes.
        last_in = read_some(fd, timeout_ms);
        std::string framed;
        GrpcConnection::frame_message(framed, next_request(last_in));
        handshaker.send(call, std::move(framed), false);
    }
    throw std::runtime_error("ALTS handshake: too many rounds");
}

// ── Record layer ────────────────────────────────────────────────────────────

AltsTransport::AltsTransport(const std::string& ip, int port, GrpcConnection& handshaker,
                             const std::string& target_name) {
    fd_ = connect_tcp(ip, port, 10000);
    try {
        AltsHandshakeResult r = alts_client_handshake(fd_, handshaker, target_name, 10000);
        peer_ = r.peer_service_account;
        max_frame_ = r.max_frame_size;
        in_.assign(r.leftover.begin(), r.leftover.end());
        init_record_layer(r.key_data, /*is_client=*/true);
    } catch (...) {
        ::close(fd_);
        throw;
    }
    set_nonblocking(fd_, true);
}

AltsTransport::AltsTransport(int fd, const std::string& key_data, size_t max_frame_size, bool is_client,
                             std::string leftover)
    : fd_(fd), max_frame_(max_frame_size) {
    in_.assign(leftover.begin(), leftover.end());
    init_record_layer(key_data, is_client);
    set_nonblocking(fd_, true);
}

void AltsTransport::init_record_layer(const std::string& key_data, bool is_client) {
    if (key_data.size() < kRekeyKeyLength) throw std::runtime_error("ALTS: key data too short");
    seal_ = make_protocol(key_data, is_client, /*is_protect=*/true);
    try {
        unseal_ = make_protocol(key_data, is_client, /*is_protect=*/false);
    } catch (...) {
        alts_iovec_record_protocol_destroy(seal_);
        seal_ = nullptr;
        throw;
    }
    header_len_ = alts_iovec_record_protocol_get_header_length();
    tag_len_ = alts_iovec_record_protocol_get_tag_length(seal_);
    max_payload_ = alts_iovec_record_protocol_max_unprotected_data_size(seal_, max_frame_);
    if (max_payload_ == 0) throw std::runtime_error("ALTS: frame size leaves no room for data");
}

AltsTransport::~AltsTransport() {
    alts_iovec_record_protocol_destroy(seal_);
    alts_iovec_record_protocol_destroy(unseal_);
    ::close(fd_);
}

long AltsTransport::flush(bool& want_write) {
    while (out_off_ < out_.size()) {
        const ssize_t w = ::send(fd_, out_.data() + out_off_, out_.size() - out_off_, MSG_NOSIGNAL);
        if (w > 0) {
            out_off_ += static_cast<size_t>(w);
            continue;
        }
        if (w < 0 && (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR)) {
            want_write = true;
            return 0;
        }
        error_ = std::string("ALTS write: ") + std::strerror(errno);
        return -1;
    }
    out_.clear();
    out_off_ = 0;
    return 0;
}

long AltsTransport::write(const uint8_t* p, size_t n, bool& want_write) {
    if (flush(want_write) < 0) return -1;
    if (out_off_ < out_.size()) return 0;  // previous frame still going out
    const size_t k = std::min(n, max_payload_);
    out_.resize(header_len_ + k + tag_len_);
    iovec plain{const_cast<uint8_t*>(p), k};
    char* details = nullptr;
    if (alts_iovec_record_protocol_privacy_integrity_protect(seal_, &plain, 1, iovec{out_.data(), out_.size()},
                                                             &details) != GRPC_STATUS_OK) {
        error_ = take_error(details, "seal");
        return -1;
    }
    out_off_ = 0;
    // The frame is accepted even if it is not all on the socket yet; flush() finishes it.
    if (flush(want_write) < 0) return -1;
    return static_cast<long>(k);
}

size_t AltsTransport::complete_frame() const {
    const size_t avail = in_.size() - in_off_;
    if (avail < 4) return 0;
    uint32_t len;
    std::memcpy(&len, in_.data() + in_off_, 4);  // little-endian on every supported target
    if (len < header_len_ - 4 + tag_len_ || len > kMaxReceivedFrame)
        throw std::runtime_error("ALTS: bad frame length " + std::to_string(len));
    return avail >= 4 + static_cast<size_t>(len) ? 4 + static_cast<size_t>(len) : 0;
}

bool AltsTransport::pending() const {
    if (plain_off_ < plain_.size()) return true;
    try {
        return complete_frame() != 0;
    } catch (const std::exception&) {
        return true;  // read() surfaces the error
    }
}

long AltsTransport::read(uint8_t* p, size_t n, bool&) {
    for (;;) {
        if (plain_off_ < plain_.size()) {
            const size_t k = std::min(n, plain_.size() - plain_off_);
            std::memcpy(p, plain_.data() + plain_off_, k);
            plain_off_ += k;
            return static_cast<long>(k);
        }
        size_t frame;
        try {
            frame = complete_frame();
        } catch (const std::exception& e) {
            error_ = e.what();
            return -1;
        }
        if (frame) {
            const size_t data_len = frame - header_len_ - tag_len_;
            plain_.resize(data_len);
            plain_off_ = 0;
            uint8_t* f = in_.data() + in_off_;
            iovec protected_part{f + header_len_, frame - header_len_};
            char* details = nullptr;
            if (alts_iovec_record_protocol_privacy_integrity_unprotect(unseal_, iovec{f, header_len_}, &protected_part, 1,
                                                                       iovec{plain_.data(), data_len},
                                                                       &details) != GRPC_STATUS_OK) {
                error_ = take_error(details, "unseal");
                return -1;
            }
            in_off_ += frame;
            if (in_off_ == in_.size()) {
                in_.clear();
                in_off_ = 0;
            }
            continue;
        }
        // Need more bytes: compact, then read from the socket.
        if (in_off_) {
            in_.erase(in_.begin(), in_.begin() + static_cast<std::ptrdiff_t>(in_off_));
            in_off_ = 0;
        }
        const size_t have = in_.size();
        in_.resize(have + 256 * 1024);
        const ssize_t r = ::recv(fd_, in_.data() + have, 256 * 1024, 0);
        in_.resize(have + (r > 0 ? static_cast<size_t>(r) : 0));
        if (r > 0) continue;
        if (r < 0 && (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR)) return 0;
        error_ = r == 0 ? "ALTS peer closed the connection" : std::string("ALTS read: ") + std::strerror(errno);
        return -1;
    }
}

std::string alts_gcm_rekey_encrypt_for_test(const std::string& key_data, const std::string& nonce,
                                            const std::string& aad, const std::string& plaintext) {
    grpc_alts::GsecKeyFactory factory(
        grpc_alts::Span<const uint8_t>(reinterpret_cast<const uint8_t*>(key_data.data()), key_data.size()),
        /*is_rekey=*/true);
    gsec_aead_crypter* crypter = nullptr;
    char* details = nullptr;
    if (gsec_aes_gcm_aead_crypter_create(factory.Create(), kAesGcmNonceLength, kAesGcmTagLength, &crypter,
                                         &details) != GRPC_STATUS_OK)
        throw std::runtime_error(take_error(details, "crypter"));
    std::string out(plaintext.size() + kAesGcmTagLength, '\0');
    size_t written = 0;
    const grpc_status_code st = gsec_aead_crypter_encrypt(
        crypter, reinterpret_cast<const uint8_t*>(nonce.data()), nonce.size(),
        reinterpret_cast<const uint8_t*>(aad.data()), aad.size(), reinterpret_cast<const uint8_t*>(plaintext.data()),
        plaintext.size(), reinterpret_cast<uint8_t*>(out.data()), out.size(), &written, &details);
    gsec_aead_crypter_destroy(crypter);
    if (st != GRPC_STATUS_OK) throw std::runtime_error(take_error(details, "encrypt"));
    out.resize(written);
    return out;
}

namespace {

// Pumps `payload` from `from` to `to` with both sockets non-blocking.
std::string pump(AltsTransport& from, AltsTransport& to, const std::string& payload) {
    std::string got;
    size_t sent = 0;
    std::vector<uint8_t> buf(7919);  // odd size: reads split frames
    for (int spins = 0; got.size() < payload.size(); ++spins) {
        if (spins > 1000000) throw std::runtime_error("ALTS round trip made no progress");
        bool ww = false;
        if (sent < payload.size()) {
            const long w = from.write(reinterpret_cast<const uint8_t*>(payload.data()) + sent, payload.size() - sent, ww);
            if (w < 0) throw std::runtime_error(from.error());
            sent += static_cast<size_t>(w);
        } else if (from.flush(ww) < 0) {
            throw std::runtime_error(from.error());
        }
        const long r = to.read(buf.data(), buf.size(), ww);
        if (r < 0) throw std::runtime_error(to.error());
        got.append(reinterpret_cast<const char*>(buf.data()), static_cast<size_t>(r));
    }
    return got;
}

}  // namespace

std::string alts_socketpair_roundtrip_for_test(const std::string& key_data, const std::string& payload,
                                               size_t max_frame_size) {
    int sv[2];
    if (socketpair(AF_UNIX, SOCK_STREAM, 0, sv) != 0) throw std::runtime_error("socketpair failed");
    AltsTransport client(sv[0], key_data, max_frame_size, /*is_client=*/true, "");
    AltsTransport server(sv[1], key_data, max_frame_size, /*is_client=*/false, "");
    if (pump(client, server, payload) != payload) return "client to server: payload changed";
    if (pump(server, client, payload) != payload) return "server to client: payload changed";
    return "";
}

}  // namespace gcs_grpc
