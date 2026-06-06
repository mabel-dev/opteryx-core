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

// ---------------------------------------------------------------------------
// In-string masking (simdjson stage-1 style).
// ---------------------------------------------------------------------------

#if defined(__ARM_NEON) || defined(__ARM_NEON__)
// Compress a NEON byte-compare (each lane 0x00/0xFF) to a 16-bit mask.
inline uint16_t _mask_movemask16(uint8x16_t cmp) {
    static const uint8_t kBits[16] = {1,2,4,8,16,32,64,128, 1,2,4,8,16,32,64,128};
    const uint8x16_t bm = vld1q_u8(kBits);
    const uint8x16_t m  = vandq_u8(cmp, bm);
    return static_cast<uint16_t>(vaddv_u8(vget_low_u8(m)))
         | (static_cast<uint16_t>(vaddv_u8(vget_high_u8(m))) << 8);
}
#endif

// Inclusive prefix XOR: out[k] = XOR of in[0..k]. Turns a quote bitmask into an
// "inside a string" mask (each quote toggles the run).
inline uint64_t _mask_prefix_xor(uint64_t x) {
    x ^= x << 1; x ^= x << 2; x ^= x << 4; x ^= x << 8; x ^= x << 16; x ^= x << 32;
    return x;
}

// Bitmask of bytes that are escaped (preceded by an odd run of backslashes). `*prev`
// carries the escape state across 64-bit words (bit 0 = first byte of next word escaped).
// Canonical simdjson find_escaped.
inline uint64_t _mask_find_escaped(uint64_t backslash, uint64_t* prev) {
    backslash &= ~(*prev);
    const uint64_t follows = (backslash << 1) | (*prev);
    const uint64_t even = 0x5555555555555555ULL;
    const uint64_t odd_starts = backslash & ~even & ~follows;
    uint64_t even_seq = 0;
    *prev = __builtin_add_overflow(odd_starts, backslash, &even_seq) ? 1ull : 0ull;
    const uint64_t invert = even_seq << 1;
    return (even ^ invert) & follows;
}

// Structural scan with in-string masking: emit(pos, byte) only for structural characters
// that are NOT inside a string value, plus the real (unescaped) delimiter quotes. In-string
// commas/colons/brackets, backslashes and escaped quotes are dropped, so the document-map
// FSM sees clean structure and a string can never truncate on an escaped quote. Carries the
// escape/in-string state across 64-byte words; safe to start mid-buffer at a record boundary
// (callers split on newlines, which are never inside a string).
//
// SHELVED, NOT THE DEFAULT. Benchmarks showed masking costs ~1.4× scan and only nets out
// above ~40% in-string density (e.g. stringified-JSON fields); below that the fixed scan
// overhead loses to the unmasked scan + the FSM's ~free escaped-quote handling. Retained for
// an adaptive high-density path (gate on sampled in-string density, like the read prefilter).
template <class Emit>
inline void scan_structural_masked(const uint8_t* data, size_t length, Emit&& emit) {
    const uint8_t* lut = structural_lut();
    uint64_t prev_escaped = 0, prev_in_string = 0;
    size_t i = 0;

#if defined(__ARM_NEON) || defined(__ARM_NEON__)
    const uint8x16_t qq = vdupq_n_u8('"'),  bs = vdupq_n_u8('\\');
    const uint8x16_t bo = vdupq_n_u8('{'),  bc = vdupq_n_u8('}');
    const uint8x16_t so = vdupq_n_u8('['),  sc = vdupq_n_u8(']');
    const uint8x16_t cl = vdupq_n_u8(':'),  cm = vdupq_n_u8(',');
    const uint8x16_t nl = vdupq_n_u8('\n');
    for (; i + 64 <= length; i += 64) {
        uint64_t quote = 0, bslash = 0, structb = 0;
        for (int b = 0; b < 4; ++b) {
            const uint8x16_t v = vld1q_u8(data + i + b * 16);
            const uint8x16_t st = vorrq_u8(
                vorrq_u8(vorrq_u8(vceqq_u8(v, bo), vceqq_u8(v, bc)),
                         vorrq_u8(vceqq_u8(v, so), vceqq_u8(v, sc))),
                vorrq_u8(vorrq_u8(vceqq_u8(v, cl), vceqq_u8(v, cm)), vceqq_u8(v, nl)));
            quote  |= static_cast<uint64_t>(_mask_movemask16(vceqq_u8(v, qq))) << (b * 16);
            bslash |= static_cast<uint64_t>(_mask_movemask16(vceqq_u8(v, bs))) << (b * 16);
            structb|= static_cast<uint64_t>(_mask_movemask16(st))             << (b * 16);
        }
        const uint64_t escaped = _mask_find_escaped(bslash, &prev_escaped);
        const uint64_t real_q  = quote & ~escaped;
        const uint64_t in_str  = _mask_prefix_xor(real_q) ^ prev_in_string;
        prev_in_string = static_cast<uint64_t>(0) - (in_str >> 63);   // all-ones if still in string
        uint64_t emit_bits = (structb & ~in_str) | real_q;
        while (emit_bits) {
            const uint32_t pos = static_cast<uint32_t>(i + __builtin_ctzll(emit_bits));
            emit(pos, data[pos]);
            emit_bits &= emit_bits - 1;
        }
    }
#endif
    // Scalar tail (and the whole scan on non-NEON), continuing the carried state.
    bool in_s = prev_in_string != 0;
    bool esc  = (prev_escaped & 1) != 0;
    for (; i < length; ++i) {
        const uint8_t c = data[i];
        if (in_s) {
            if (esc)            { esc = false; continue; }
            if (c == '\\')      { esc = true;  continue; }
            if (c == '"')       { in_s = false; emit(static_cast<uint32_t>(i), c); continue; }
        } else {
            if (c == '"')       { in_s = true;  emit(static_cast<uint32_t>(i), c); continue; }
            if (lut[c])         { emit(static_cast<uint32_t>(i), c); }
        }
    }
}

// SIMD-assisted scan that materialises all marker positions into a vector.
// Prefer scan_structural() + a callback when you can avoid materialising.
std::vector<MarkerPosition> scan_structural_markers(
    const uint8_t* data,
    size_t length,
    bool use_simd = true
);

}  // namespace rugo::_jsonl

#endif  // _JSONL_STRUCTURAL_SCAN_HPP_
