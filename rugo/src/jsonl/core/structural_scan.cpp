#include "structural_scan.hpp"

#include <cstring>

#if defined(__ARM_NEON) || defined(__ARM_NEON__)
#include <arm_neon.h>
#define RUGO_JSONL_SCAN_NEON 1
#elif defined(__AVX2__) && defined(__PCLMUL__)
#include <immintrin.h>
#define RUGO_JSONL_SCAN_AVX2 1
#endif

#if defined(__GNUC__) || defined(__clang__)
#define RUGO_JSONL_SCAN_INLINE inline __attribute__((always_inline))
#else
#define RUGO_JSONL_SCAN_INLINE inline
#endif

namespace rugo::_jsonl {

namespace {

// Bitmask of bytes that are escaped (preceded by an odd run of backslashes). `*prev`
// carries the escape state across 64-byte blocks (bit 0 = first byte of the next block
// escaped). Canonical simdjson find_escaped.
inline uint64_t find_escaped(uint64_t backslash, uint64_t* prev) {
    backslash &= ~(*prev);
    const uint64_t follows = (backslash << 1) | (*prev);
    const uint64_t even = 0x5555555555555555ULL;
    const uint64_t odd_starts = backslash & ~even & ~follows;
    uint64_t even_seq = 0;
    *prev = __builtin_add_overflow(odd_starts, backslash, &even_seq) ? 1ull : 0ull;
    const uint64_t invert = even_seq << 1;
    return (even ^ invert) & follows;
}

#if defined(RUGO_JSONL_SCAN_NEON) || defined(RUGO_JSONL_SCAN_AVX2)

// Inclusive prefix XOR (out[k] = XOR of in[0..k]) as a carry-less multiply by all-ones:
// turns the real-quote mask into the "inside a string" mask.
inline uint64_t prefix_xor(uint64_t x) {
#if defined(RUGO_JSONL_SCAN_NEON)
    return vgetq_lane_u64(vreinterpretq_u64_p128(vmull_p64(x, ~0ULL)), 0);
#else
    return static_cast<uint64_t>(_mm_cvtsi128_si64(
        _mm_clmulepi64_si128(_mm_set_epi64x(0, static_cast<long long>(x)), _mm_set1_epi8(-1), 0)));
#endif
}

// One 64-byte block's four classification masks.
struct BlockMasks {
    uint64_t quote, backslash, newline, structural;
};

#if defined(RUGO_JSONL_SCAN_NEON)
// Four byte-compares (lanes 0x00/0xFF) to one 64-bit mask: weight each lane by its bit,
// then three rounds of pairwise adds fold 64 bytes into 8.
inline uint64_t to_mask64(uint8x16_t a, uint8x16_t b, uint8x16_t c, uint8x16_t d) {
    const uint8x16_t bit = {1, 2, 4, 8, 16, 32, 64, 128, 1, 2, 4, 8, 16, 32, 64, 128};
    uint8x16_t s0 = vpaddq_u8(vandq_u8(a, bit), vandq_u8(b, bit));
    const uint8x16_t s1 = vpaddq_u8(vandq_u8(c, bit), vandq_u8(d, bit));
    s0 = vpaddq_u8(s0, s1);
    s0 = vpaddq_u8(s0, s0);
    return vgetq_lane_u64(vreinterpretq_u64_u8(s0), 0);
}

inline uint8x16_t structural16(uint8x16_t v) {
    // '[' / ']' are '{' / '}' with bit 0x20 clear: one OR folds the bracket pairs.
    const uint8x16_t lower = vorrq_u8(v, vdupq_n_u8(0x20));
    return vorrq_u8(vorrq_u8(vceqq_u8(lower, vdupq_n_u8('{')), vceqq_u8(lower, vdupq_n_u8('}'))),
                    vorrq_u8(vceqq_u8(v, vdupq_n_u8(':')), vceqq_u8(v, vdupq_n_u8(','))));
}

RUGO_JSONL_SCAN_INLINE BlockMasks classify(const uint8_t* p) {
    const uint8x16_t v0 = vld1q_u8(p), v1 = vld1q_u8(p + 16), v2 = vld1q_u8(p + 32), v3 = vld1q_u8(p + 48);
    const uint8x16_t q = vdupq_n_u8('"'), bs = vdupq_n_u8('\\'), nl = vdupq_n_u8('\n');
    BlockMasks m;
    m.quote      = to_mask64(vceqq_u8(v0, q), vceqq_u8(v1, q), vceqq_u8(v2, q), vceqq_u8(v3, q));
    m.backslash  = to_mask64(vceqq_u8(v0, bs), vceqq_u8(v1, bs), vceqq_u8(v2, bs), vceqq_u8(v3, bs));
    m.newline    = to_mask64(vceqq_u8(v0, nl), vceqq_u8(v1, nl), vceqq_u8(v2, nl), vceqq_u8(v3, nl));
    m.structural = to_mask64(structural16(v0), structural16(v1), structural16(v2), structural16(v3));
    return m;
}
#else
inline uint64_t to_mask64(__m256i lo, __m256i hi) {
    return static_cast<uint64_t>(static_cast<uint32_t>(_mm256_movemask_epi8(lo))) |
           (static_cast<uint64_t>(static_cast<uint32_t>(_mm256_movemask_epi8(hi))) << 32);
}

inline __m256i structural32(__m256i v) {
    // '[' / ']' are '{' / '}' with bit 0x20 clear: one OR folds the bracket pairs.
    const __m256i lower = _mm256_or_si256(v, _mm256_set1_epi8(0x20));
    return _mm256_or_si256(
        _mm256_or_si256(_mm256_cmpeq_epi8(lower, _mm256_set1_epi8('{')),
                        _mm256_cmpeq_epi8(lower, _mm256_set1_epi8('}'))),
        _mm256_or_si256(_mm256_cmpeq_epi8(v, _mm256_set1_epi8(':')),
                        _mm256_cmpeq_epi8(v, _mm256_set1_epi8(','))));
}

RUGO_JSONL_SCAN_INLINE BlockMasks classify(const uint8_t* p) {
    const __m256i lo = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(p));
    const __m256i hi = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(p + 32));
    const __m256i q = _mm256_set1_epi8('"'), bs = _mm256_set1_epi8('\\'), nl = _mm256_set1_epi8('\n');
    BlockMasks m;
    m.quote      = to_mask64(_mm256_cmpeq_epi8(lo, q), _mm256_cmpeq_epi8(hi, q));
    m.backslash  = to_mask64(_mm256_cmpeq_epi8(lo, bs), _mm256_cmpeq_epi8(hi, bs));
    m.newline    = to_mask64(_mm256_cmpeq_epi8(lo, nl), _mm256_cmpeq_epi8(hi, nl));
    m.structural = to_mask64(structural32(lo), structural32(hi));
    return m;
}
#endif

// Mask the block, then flatten its set bits to positions. The flatten writes 8 entries
// unconditionally, 8 more when the block holds more than 8, and loops only past 16
// (simdjson flatten_bits): the entry count per block is data-dependent, a branch per
// entry is not. Forced inline: out of line, the per-block call and the BlockMasks
// round-trip through memory cost ~25% of the scan.
RUGO_JSONL_SCAN_INLINE size_t index_block(const BlockMasks& m, uint64_t& prev_escaped, uint64_t& prev_in_string,
                          uint32_t idx, uint32_t* o) {
    const uint64_t escaped = (m.backslash | prev_escaped) ? find_escaped(m.backslash, &prev_escaped) : 0;
    const uint64_t real_q = m.quote & ~escaped;
    uint64_t in_str = prefix_xor(real_q) ^ prev_in_string;
    // A newline still inside a string closes it: flip the in-string run for every byte
    // after it. ~1 block in 8 carries a newline on record-sized lines.
    for (uint64_t nb = m.newline; nb; nb &= nb - 1) {
        const unsigned p = static_cast<unsigned>(__builtin_ctzll(nb));
        in_str ^= ((~0ull << p) << 1) & (0ull - ((in_str >> p) & 1ull));
    }
    // Carry the state AFTER the block's last byte: all-ones if still in a string. A newline
    // as that last byte ends the string — the flip above only reaches bytes after a
    // newline in the same block, so without the mask a line ending inside a string with its
    // newline at bit 63 would start the next block, and the next line, inverted.
    prev_in_string = 0ull - ((in_str & ~m.newline) >> 63);
    uint64_t bits = (m.structural & ~in_str) | real_q | m.newline;

    const int count = __builtin_popcountll(bits);
    for (int k = 0; k < 8; ++k) {
        o[k] = idx + static_cast<uint32_t>(__builtin_ctzll(bits | (1ull << 63)));
        bits &= bits - 1;
    }
    if (count > 8) {
        for (int k = 8; k < 16; ++k) {
            o[k] = idx + static_cast<uint32_t>(__builtin_ctzll(bits | (1ull << 63)));
            bits &= bits - 1;
        }
        for (int k = 16; bits; ++k) {
            o[k] = idx + static_cast<uint32_t>(__builtin_ctzll(bits));
            bits &= bits - 1;
        }
    }
    return static_cast<size_t>(count);
}

#endif  // NEON || AVX2

}  // namespace

size_t scan_structural_index(const uint8_t* data, size_t length, uint32_t base, uint32_t* out) {
    size_t count = 0;
#if defined(RUGO_JSONL_SCAN_NEON) || defined(RUGO_JSONL_SCAN_AVX2)
    uint64_t prev_escaped = 0, prev_in_string = 0;
    size_t i = 0;
    for (; i + 64 <= length; i += 64)
        count += index_block(classify(data + i), prev_escaped, prev_in_string,
                             base + static_cast<uint32_t>(i), out + count);
    if (i < length) {
        // The tail as one block padded with spaces — never a marker, so it carries the
        // escape and string state through unchanged.
        uint8_t tail[64];
        std::memset(tail, ' ', sizeof tail);
        std::memcpy(tail, data + i, length - i);
        count += index_block(classify(tail), prev_escaped, prev_in_string,
                             base + static_cast<uint32_t>(i), out + count);
    }
#else
    // Byte-for-byte the SIMD answer: a backslash escapes the next byte wherever it is
    // (find_escaped), which only ever un-reals a quote; an escaped structural outside a
    // string is still written.
    bool in_s = false, esc = false;
    for (size_t i = 0; i < length; ++i) {
        const uint8_t c = data[i];
        const uint32_t pos = base + static_cast<uint32_t>(i);
        // A newline is always written and always ends the line's string/escape state —
        // checked first so an escape cannot swallow it.
        if (c == '\n')     { in_s = false; esc = false; out[count++] = pos; continue; }
        if (esc) {
            esc = false;
            if (c == '"' || c == '\\') continue;   // escaped: not a delimiter, starts no escape
        } else if (c == '\\') {
            esc = true;
            continue;
        }
        if (in_s) {
            if (c == '"')  { in_s = false; out[count++] = pos; }
            continue;
        }
        switch (c) {
            case '"': in_s = true; out[count++] = pos; break;
            case '{': case '}': case '[': case ']': case ':': case ',': out[count++] = pos; break;
            default: break;
        }
    }
#endif
    return count;
}

}  // namespace rugo::_jsonl
