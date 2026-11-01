// Hand-written protobuf wire format (design R2): just enough to encode the
// requests and decode the responses the GCS gRPC reader uses. No schema, no
// reflection, no allocation on decode — fields are visited in wire order and
// length-delimited values are views into the caller's buffer.
#pragma once

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <stdexcept>
#include <string>
#include <string_view>

namespace gcs_grpc::pb {

enum WireType : uint32_t { kVarint = 0, kFixed64 = 1, kLen = 2, kFixed32 = 5 };

// ── Encoding ────────────────────────────────────────────────────────────────

inline void put_varint(std::string& out, uint64_t v) {
    while (v >= 0x80) {
        out.push_back(static_cast<char>((v & 0x7F) | 0x80));
        v >>= 7;
    }
    out.push_back(static_cast<char>(v));
}

inline void put_tag(std::string& out, uint32_t field, WireType wt) {
    put_varint(out, (static_cast<uint64_t>(field) << 3) | wt);
}

inline void put_uint(std::string& out, uint32_t field, uint64_t v) {
    put_tag(out, field, kVarint);
    put_varint(out, v);
}

inline void put_bytes(std::string& out, uint32_t field, std::string_view v) {
    put_tag(out, field, kLen);
    put_varint(out, v.size());
    out.append(v.data(), v.size());
}

// ── Decoding ────────────────────────────────────────────────────────────────

struct Field {
    uint32_t number = 0;
    WireType type = kVarint;
    uint64_t u = 0;          // kVarint / kFixed32 / kFixed64 value
    const uint8_t* p = nullptr;  // kLen payload
    size_t n = 0;                // kLen payload size
    std::string_view view() const { return {reinterpret_cast<const char*>(p), n}; }
};

class Reader {
public:
    Reader(const uint8_t* p, size_t n) : p_(p), end_(p + n) {}

    // Next field, or false at the end of the message. Malformed input throws.
    bool next(Field& f) {
        if (p_ == end_) return false;
        const uint64_t key = varint();
        f.number = static_cast<uint32_t>(key >> 3);
        f.type = static_cast<WireType>(key & 7);
        if (f.number == 0) throw std::runtime_error("protobuf: field number 0");
        switch (f.type) {
            case kVarint: f.u = varint(); break;
            case kFixed64: f.u = fixed(8); break;
            case kFixed32: f.u = fixed(4); break;
            case kLen: {
                const uint64_t n = varint();
                if (n > static_cast<uint64_t>(end_ - p_)) throw std::runtime_error("protobuf: truncated length-delimited field");
                f.p = p_;
                f.n = static_cast<size_t>(n);
                p_ += n;
                break;
            }
            default: throw std::runtime_error("protobuf: unsupported wire type " + std::to_string(f.type));
        }
        return true;
    }

private:
    uint64_t varint() {
        uint64_t v = 0;
        for (int shift = 0; shift < 64; shift += 7) {
            if (p_ == end_) throw std::runtime_error("protobuf: truncated varint");
            const uint8_t b = *p_++;
            v |= static_cast<uint64_t>(b & 0x7F) << shift;
            if (!(b & 0x80)) return v;
        }
        throw std::runtime_error("protobuf: varint too long");
    }
    uint64_t fixed(size_t k) {
        if (static_cast<size_t>(end_ - p_) < k) throw std::runtime_error("protobuf: truncated fixed field");
        uint64_t v = 0;
        std::memcpy(&v, p_, k);  // little-endian on every supported target
        p_ += k;
        return v;
    }

    const uint8_t* p_;
    const uint8_t* end_;
};

}  // namespace gcs_grpc::pb
