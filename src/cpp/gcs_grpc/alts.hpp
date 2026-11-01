// ALTS (design C3): the client handshake, driven through the ALTS handshaker
// service on the GCE metadata server, and the record layer that protects the
// connection afterwards. The crypto and frame protection are the vendored gRPC
// record protocol (third_party/grpc_alts, ruling R1); this file only drives it.
//
// Wire facts (design §10): handshaker = plaintext h2c to
// metadata.google.internal.:8080, /grpc.gcp.HandshakerService/DoHandshake;
// record protocol ALTSRP_GCM_AES128_REKEY with a 44-byte key; frames are
// `u32le length | u32le type(6) | ciphertext | 16-byte tag`, 16 KiB – 1 MiB.
#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "grpc_conn.hpp"
#include "transport.hpp"

struct alts_iovec_record_protocol;

namespace gcs_grpc {

constexpr const char* kAltsHandshakerHost = "metadata.google.internal.";
constexpr int kAltsHandshakerPort = 8080;
constexpr size_t kAltsMinFrameSize = 16 * 1024;
constexpr size_t kAltsMaxFrameSize = 1024 * 1024;

struct AltsHandshakeResult {
    std::string key_data;             // 44 bytes: KDF key (32) + nonce mask (12)
    size_t max_frame_size = 0;        // negotiated, in [kAltsMinFrameSize, kAltsMaxFrameSize]
    std::string peer_service_account;
    std::string leftover;             // peer bytes after the handshake: the first record-layer bytes
};

// Runs the client side of an ALTS handshake over the connected blocking socket
// `fd`, relaying through `handshaker` (a plaintext GrpcConnection to the
// handshaker service). Throws std::runtime_error on any failure, including a
// result that does not match what was asked for.
AltsHandshakeResult alts_client_handshake(int fd, GrpcConnection& handshaker, const std::string& target_name,
                                          int timeout_ms);

// The handshaker request, exposed for the offline tests.
std::string alts_client_start_request(const std::string& target_name);

class AltsTransport : public Transport {
public:
    // Connects to ip:port and completes the ALTS handshake. Throws on failure.
    AltsTransport(const std::string& ip, int port, GrpcConnection& handshaker, const std::string& target_name);
    // Wraps an already handshaken socket (tests drive both ends with this).
    AltsTransport(int fd, const std::string& key_data, size_t max_frame_size, bool is_client, std::string leftover);
    ~AltsTransport() override;

    int fd() const override { return fd_; }
    const char* scheme() const override { return "https"; }
    long write(const uint8_t* p, size_t n, bool& want_write) override;
    long read(uint8_t* p, size_t n, bool& want_write) override;
    bool pending() const override;
    long flush(bool& want_write) override;

    const std::string& peer_service_account() const { return peer_; }

private:
    void init_record_layer(const std::string& key_data, bool is_client);
    // Next complete frame in in_, as its total size, or 0. Throws on a bad length.
    size_t complete_frame() const;

    int fd_ = -1;
    size_t max_frame_ = kAltsMinFrameSize;
    size_t max_payload_ = 0;
    size_t header_len_ = 0;
    size_t tag_len_ = 0;
    alts_iovec_record_protocol* seal_ = nullptr;
    alts_iovec_record_protocol* unseal_ = nullptr;
    std::string peer_;

    std::vector<uint8_t> out_;  // sealed bytes not yet on the socket
    size_t out_off_ = 0;
    std::vector<uint8_t> in_;   // raw bytes from the socket
    size_t in_off_ = 0;
    std::vector<uint8_t> plain_;  // unsealed bytes not yet returned
    size_t plain_off_ = 0;
};

// Test hooks. One AES-128-GCM rekeying encryption (the vendored crypter) for
// known-answer vectors; and a socketpair round trip of `payload` through a
// client and a server AltsTransport sharing `key_data`, both directions, with
// frames of `max_frame_size`. Return the ciphertext / "" or throw.
std::string alts_gcm_rekey_encrypt_for_test(const std::string& key_data, const std::string& nonce,
                                            const std::string& aad, const std::string& plaintext);
std::string alts_socketpair_roundtrip_for_test(const std::string& key_data, const std::string& payload,
                                               size_t max_frame_size);

}  // namespace gcs_grpc
