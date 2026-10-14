#pragma once
// draken/ops/uuid_ops.h — kernels for DRAKEN_UUID (docs/UUID_TYPE_DESIGN.md).
//
// Physical layout: 16 opaque bytes per value, in RFC 9562 network order (the order
// the value is written in text). memcmp order of the bytes == canonical-text order,
// which is the type's ordering. A value is loaded as an UNSIGNED 128-bit key whose
// high word is bytes 0..7 big-endian and low word bytes 8..15 big-endian, so a
// single unsigned compare gives memcmp order. Equality and hashing are byte-exact.
//
// UNIFORM PATH ONLY (CLAUDE.md §11): every kernel reads data[selection[i]] for each
// logical row, with no shape discrimination.
//
// MOVEMENT (take/slice/materialize/dictionary_encode) reuses the 16-byte i128_*
// gathers (int128_gather.h): they copy 16 bytes and never interpret them.

#include <stdint.h>
#include <string.h>
#include <stdexcept>

#include "core/buffers.h"
#include "ops/vec_result.h"
#include "ops/int64_compare.h"   // cmp_alloc_bool_buf / cmp_and_validity
#include "simd_hash.h"
#include "core/uuid_text.h"   // uuid_format / uuid_parse

namespace draken { namespace ops {

static inline uint64_t uuid_load_be64(const uint8_t* p) noexcept {
    uint64_t w;
    memcpy(&w, p, 8);
#if defined(__BYTE_ORDER__) && __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
    w = __builtin_bswap64(w);
#endif
    return w;
}

// The ordering key: unsigned compare of this == memcmp of the 16 bytes.
static inline unsigned __int128 uuid_key(const uint8_t* p) noexcept {
    return (static_cast<unsigned __int128>(uuid_load_be64(p)) << 64) | uuid_load_be64(p + 8);
}

// A SIGNED 128-bit key with the same order as memcmp of the bytes (the unsigned key
// with its top bit flipped), for consumers whose 16-byte lanes compare signed (the
// DECIMAL128 MIN/MAX lanes, the sort's int128 lane). uuid_from_order_key inverts it.
static inline __int128 uuid_order_key(const uint8_t* p) noexcept {
    return static_cast<__int128>(uuid_key(p) ^ (static_cast<unsigned __int128>(1) << 127));
}

static inline void uuid_from_order_key(__int128 k, uint8_t* out) noexcept {
    const unsigned __int128 u = static_cast<unsigned __int128>(k) ^ (static_cast<unsigned __int128>(1) << 127);
    uint64_t hi = static_cast<uint64_t>(u >> 64), lo = static_cast<uint64_t>(u);
#if defined(__BYTE_ORDER__) && __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
    hi = __builtin_bswap64(hi);
    lo = __builtin_bswap64(lo);
#endif
    memcpy(out, &hi, 8);
    memcpy(out + 8, &lo, 8);
}

static inline const uint8_t* uuid_at(const DrakenVector& v, uint32_t row) noexcept {
    return static_cast<const uint8_t*>(v.data) + static_cast<size_t>(v.selection[row]) * 16u;
}

// Folds the 16 bytes to one u64 before the shared mixer; null rows get NULL_HASH.
static inline uint64_t uuid_fold(const uint8_t* p) noexcept {
    uint64_t hi, lo;
    memcpy(&hi, p, 8);
    memcpy(&lo, p + 8, 8);
    return hi ^ (lo * 0x9E3779B97F4A7C15ULL);
}

static inline void uuid_hash(const DrakenVector& v, uint64_t* out, uint32_t n) {
    if (n == 0) return;
    const uint8_t* validity = v.validity;
    uint64_t scratch[1024];
    uint32_t i = 0;
    while (i < n) {
        const uint32_t block = (n - i < 1024u) ? (n - i) : 1024u;
        for (uint32_t j = 0; j < block; ++j) {
            if (validity != nullptr && !((validity[(i + j) >> 3] >> ((i + j) & 7)) & 1u))
                scratch[j] = NULL_HASH;
            else
                scratch[j] = uuid_fold(uuid_at(v, i + j));
        }
        simd_hash_i64(scratch, out + i, block);
        i += block;
    }
}

struct UuidEq { static inline bool apply(unsigned __int128 a, unsigned __int128 b) noexcept { return a == b; } };
struct UuidNe { static inline bool apply(unsigned __int128 a, unsigned __int128 b) noexcept { return a != b; } };
struct UuidGt { static inline bool apply(unsigned __int128 a, unsigned __int128 b) noexcept { return a >  b; } };
struct UuidGe { static inline bool apply(unsigned __int128 a, unsigned __int128 b) noexcept { return a >= b; } };
struct UuidLt { static inline bool apply(unsigned __int128 a, unsigned __int128 b) noexcept { return a <  b; } };
struct UuidLe { static inline bool apply(unsigned __int128 a, unsigned __int128 b) noexcept { return a <= b; } };

// Null if EITHER operand row is null (matches int64_compare.h / int128_compare.h).
template<typename Op>
static inline VecResult uuid_compare_vector_impl(const DrakenVector& a, const DrakenVector& b) {
    const uint32_t n = a.length;
    if (b.length != n)
        throw std::invalid_argument("uuid_compare_vector: operand lengths must match");

    uint8_t* out_null = cmp_and_validity(a.validity, b.validity, n);
    uint8_t* dst = nullptr;
    try {
        dst = cmp_alloc_bool_buf(n);
    } catch (...) {
        if (out_null) draken_free(out_null);
        throw;
    }
    for (uint32_t i = 0; i < n; ++i) {
        if (out_null != nullptr && !((out_null[i >> 3] >> (i & 7)) & 1u)) continue;
        if (Op::apply(uuid_key(uuid_at(a, i)), uuid_key(uuid_at(b, i))))
            dst[i >> 3] |= static_cast<uint8_t>(1u << (i & 7));
    }

    VecResult r;
    r.data           = dst;
    r.validity       = out_null;
    r.selection      = draken_identity_sel(n);
    r.owns_selection = false;
    r.data_length    = n;
    r.length         = n;
    r.type           = DRAKEN_BOOL;
    r.flags          = static_cast<uint8_t>(DRAKEN_SEL_IDENTITY | DRAKEN_SEL_PERMUTATION);
    return r;
}

// op: 0=eq 1=ne 2=gt 3=ge 4=lt 5=le.
static inline VecResult uuid_compare_vector(const DrakenVector& a, const DrakenVector& b, int op) {
    switch (op) {
        case 0: return uuid_compare_vector_impl<UuidEq>(a, b);
        case 1: return uuid_compare_vector_impl<UuidNe>(a, b);
        case 2: return uuid_compare_vector_impl<UuidGt>(a, b);
        case 3: return uuid_compare_vector_impl<UuidGe>(a, b);
        case 4: return uuid_compare_vector_impl<UuidLt>(a, b);
        case 5: return uuid_compare_vector_impl<UuidLe>(a, b);
        default: throw std::invalid_argument("uuid_compare_vector: unknown op code");
    }
}

}} // namespace draken::ops
