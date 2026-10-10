#pragma once
// rugo/src/avro/avro_varint.hpp — Avro's binary primitives over a bounded cursor.
//
// Every read is bounds-checked against `end`; a short or malformed input throws
// std::runtime_error (docs/AVRO_READER_DESIGN.md §7). The throw paths are cold.

#include <cstdint>
#include <cstring>
#include <stdexcept>

namespace rugo::avro {

struct Cursor {
    const uint8_t* p;
    const uint8_t* end;
};

[[noreturn, gnu::cold]] inline void truncated() {
    throw std::runtime_error("Avro: the data is truncated (a value runs past the end of its block)");
}

[[noreturn, gnu::cold]] inline void overlong_varint() {
    throw std::runtime_error("Avro: malformed varint (longer than 10 bytes)");
}

// Zigzag varint `long`. At most 10 bytes; the 10th may only carry the top bit.
inline int64_t read_long(Cursor& c) {
    uint64_t v = 0;
    unsigned shift = 0;
    const uint8_t* p = c.p;
    for (;;) {
        if (p == c.end) truncated();
        const uint8_t b = *p++;
        v |= static_cast<uint64_t>(b & 0x7F) << shift;
        if ((b & 0x80) == 0) break;
        shift += 7;
        if (shift > 63) overlong_varint();
    }
    c.p = p;
    return static_cast<int64_t>((v >> 1) ^ (~(v & 1) + 1));
}

// Zigzag varint `int`: a long that must fit int32.
inline int32_t read_int(Cursor& c) {
    const int64_t v = read_long(c);
    if (v < INT32_MIN || v > INT32_MAX)
        throw std::runtime_error("Avro: an int value is out of the 32-bit range");
    return static_cast<int32_t>(v);
}

inline void skip_varint(Cursor& c) {
    const uint8_t* p = c.p;
    for (unsigned n = 0;; ++n) {
        if (p == c.end) truncated();
        if (n == 10) overlong_varint();
        if ((*p++ & 0x80) == 0) break;
    }
    c.p = p;
}

inline void need(const Cursor& c, uint64_t n) {
    if (static_cast<uint64_t>(c.end - c.p) < n) truncated();
}

// A non-negative length prefix (bytes / string / block size).
inline uint64_t read_length(Cursor& c) {
    const int64_t n = read_long(c);
    if (n < 0) throw std::runtime_error("Avro: a negative length");
    need(c, static_cast<uint64_t>(n));
    return static_cast<uint64_t>(n);
}

inline float read_float(Cursor& c) {
    need(c, 4);
    float f;
    std::memcpy(&f, c.p, 4);  // little-endian on every supported target
    c.p += 4;
    return f;
}

inline double read_double(Cursor& c) {
    need(c, 8);
    double d;
    std::memcpy(&d, c.p, 8);
    c.p += 8;
    return d;
}

inline bool read_bool(Cursor& c) {
    need(c, 1);
    const uint8_t b = *c.p++;
    if (b > 1) throw std::runtime_error("Avro: a boolean byte is neither 0 nor 1");
    return b != 0;
}

}  // namespace rugo::avro
