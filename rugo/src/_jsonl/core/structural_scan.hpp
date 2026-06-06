#ifndef _JSONL_STRUCTURAL_SCAN_HPP_
#define _JSONL_STRUCTURAL_SCAN_HPP_

#include <cstddef>
#include <vector>
#include <cstdint>
#include <cstring>
#include "markers.hpp"

#if defined(__ARM_NEON) || defined(__ARM_NEON__)
#include <arm_neon.h>
#endif

namespace rugo::_jsonl {

// 256-entry LUT: nonzero => structural byte; (value - 1) == MarkerType.
inline const uint8_t* structural_lut() {
    static const struct Lut {
        uint8_t t[256];
        Lut() {
            std::memset(t, 0, 256);
            t[static_cast<uint8_t>('{')]  = static_cast<uint8_t>(MarkerType::BRACE_OPEN)    + 1;
            t[static_cast<uint8_t>('}')]  = static_cast<uint8_t>(MarkerType::BRACE_CLOSE)   + 1;
            t[static_cast<uint8_t>('[')]  = static_cast<uint8_t>(MarkerType::BRACKET_OPEN)  + 1;
            t[static_cast<uint8_t>(']')]  = static_cast<uint8_t>(MarkerType::BRACKET_CLOSE) + 1;
            t[static_cast<uint8_t>(':')]  = static_cast<uint8_t>(MarkerType::COLON)         + 1;
            t[static_cast<uint8_t>(',')]  = static_cast<uint8_t>(MarkerType::COMMA)         + 1;
            t[static_cast<uint8_t>('"')]  = static_cast<uint8_t>(MarkerType::QUOTE)         + 1;
            t[static_cast<uint8_t>('\\')] = static_cast<uint8_t>(MarkerType::BACKSLASH)     + 1;
            t[static_cast<uint8_t>('\n')] = static_cast<uint8_t>(MarkerType::NEWLINE)       + 1;
        }
    } lut;
    return lut.t;
}

// Single-pass structural scan. Calls emit(uint32_t position, uint8_t byte) for every
// structural byte ({ } [ ] : , " \ \n), in ascending position order. The NEON kernel
// finds candidates 16 bytes at a time; emit is inlined into the loop, so the caller can
// either materialise markers or drive a state machine directly with no intermediate
// vector.
template <class Emit>
inline void scan_structural(const uint8_t* data, size_t length, Emit&& emit) {
    const uint8_t* lut = structural_lut();

#if defined(__ARM_NEON) || defined(__ARM_NEON__)
    const uint8x16_t m_bo = vdupq_n_u8('{');
    const uint8x16_t m_bc = vdupq_n_u8('}');
    const uint8x16_t m_so = vdupq_n_u8('[');
    const uint8x16_t m_sc = vdupq_n_u8(']');
    const uint8x16_t m_cl = vdupq_n_u8(':');
    const uint8x16_t m_cm = vdupq_n_u8(',');
    const uint8x16_t m_qu = vdupq_n_u8('"');
    const uint8x16_t m_bs = vdupq_n_u8('\\');
    const uint8x16_t m_nl = vdupq_n_u8('\n');

    size_t i = 0;
    for (; i + 16 <= length; i += 16) {
        uint8x16_t v = vld1q_u8(data + i);
        uint8x16_t any = vorrq_u8(
            vorrq_u8(
                vorrq_u8(vceqq_u8(v, m_bo), vceqq_u8(v, m_bc)),
                vorrq_u8(vceqq_u8(v, m_so), vceqq_u8(v, m_sc))),
            vorrq_u8(
                vorrq_u8(
                    vorrq_u8(vceqq_u8(v, m_cl), vceqq_u8(v, m_cm)),
                    vorrq_u8(vceqq_u8(v, m_qu), vceqq_u8(v, m_bs))),
                vceqq_u8(v, m_nl)));

        uint64_t nibble_mask = vget_lane_u64(
            vreinterpret_u64_u8(vshrn_n_u16(vreinterpretq_u16_u8(any), 4)), 0);

        if (nibble_mask == 0) continue;

        while (nibble_mask) {
            int b = __builtin_ctzll(nibble_mask) >> 2;
            uint32_t pos = static_cast<uint32_t>(i + b);
            emit(pos, data[pos]);
            nibble_mask &= ~(static_cast<uint64_t>(0xF) << (b << 2));
        }
    }
    for (; i < length; ++i) {
        if (lut[data[i]]) emit(static_cast<uint32_t>(i), data[i]);
    }
#else
    for (size_t i = 0; i < length; ++i) {
        if (lut[data[i]]) emit(static_cast<uint32_t>(i), data[i]);
    }
#endif
}

// SIMD-assisted scan that materialises all marker positions into a vector.
// Prefer scan_structural() + a callback when you can avoid materialising.
std::vector<MarkerPosition> scan_structural_markers(
    const uint8_t* data,
    size_t length,
    bool use_simd = true
);

// SPIKE (Mison-style structural index): produce a 1-bit-per-byte structural bitmap instead
// of a position vector — set bit i => byte i is structural. ~8 bytes/marker becomes 1
// bit/byte, a far smaller, bandwidth-cheaper index. Word w (uint64) covers bytes [w*64,
// w*64+64). Drive the document map by iterating set bits (ctz + blsr).
std::vector<uint64_t> scan_structural_bitmap(
    const uint8_t* data,
    size_t length
);

}  // namespace rugo::_jsonl

#endif  // _JSONL_STRUCTURAL_SCAN_HPP_
