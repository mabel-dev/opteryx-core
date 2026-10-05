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

// check_line_tail's four classes plus the newline: quote, backslash, an opening bracket
// ('{' / '['), a closing one ('}' / ']').
struct TailMasks {
    uint64_t quote, backslash, open, close, newline;
};

#if defined(RUGO_JSONL_SCAN_NEON)
RUGO_JSONL_SCAN_INLINE TailMasks classify_tail(const uint8_t* p) {
    const uint8x16_t v0 = vld1q_u8(p), v1 = vld1q_u8(p + 16), v2 = vld1q_u8(p + 32), v3 = vld1q_u8(p + 48);
    // '[' / ']' are '{' / '}' with bit 0x20 clear: one OR folds the bracket pairs.
    const uint8x16_t x = vdupq_n_u8(0x20);
    const uint8x16_t l0 = vorrq_u8(v0, x), l1 = vorrq_u8(v1, x), l2 = vorrq_u8(v2, x), l3 = vorrq_u8(v3, x);
    const uint8x16_t q = vdupq_n_u8('"'), bs = vdupq_n_u8('\\'), nl = vdupq_n_u8('\n');
    const uint8x16_t ob = vdupq_n_u8('{'), cb = vdupq_n_u8('}');
    TailMasks m;
    m.quote     = to_mask64(vceqq_u8(v0, q), vceqq_u8(v1, q), vceqq_u8(v2, q), vceqq_u8(v3, q));
    m.backslash = to_mask64(vceqq_u8(v0, bs), vceqq_u8(v1, bs), vceqq_u8(v2, bs), vceqq_u8(v3, bs));
    m.open      = to_mask64(vceqq_u8(l0, ob), vceqq_u8(l1, ob), vceqq_u8(l2, ob), vceqq_u8(l3, ob));
    m.close     = to_mask64(vceqq_u8(l0, cb), vceqq_u8(l1, cb), vceqq_u8(l2, cb), vceqq_u8(l3, cb));
    m.newline   = to_mask64(vceqq_u8(v0, nl), vceqq_u8(v1, nl), vceqq_u8(v2, nl), vceqq_u8(v3, nl));
    return m;
}
#else
RUGO_JSONL_SCAN_INLINE TailMasks classify_tail(const uint8_t* p) {
    const __m256i lo = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(p));
    const __m256i hi = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(p + 32));
    const __m256i x = _mm256_set1_epi8(0x20);
    const __m256i llo = _mm256_or_si256(lo, x), lhi = _mm256_or_si256(hi, x);
    const __m256i q = _mm256_set1_epi8('"'), bs = _mm256_set1_epi8('\\'), nl = _mm256_set1_epi8('\n');
    const __m256i ob = _mm256_set1_epi8('{'), cb = _mm256_set1_epi8('}');
    TailMasks m;
    m.quote     = to_mask64(_mm256_cmpeq_epi8(lo, q), _mm256_cmpeq_epi8(hi, q));
    m.backslash = to_mask64(_mm256_cmpeq_epi8(lo, bs), _mm256_cmpeq_epi8(hi, bs));
    m.open      = to_mask64(_mm256_cmpeq_epi8(llo, ob), _mm256_cmpeq_epi8(lhi, ob));
    m.close     = to_mask64(_mm256_cmpeq_epi8(llo, cb), _mm256_cmpeq_epi8(lhi, cb));
    m.newline   = to_mask64(_mm256_cmpeq_epi8(lo, nl), _mm256_cmpeq_epi8(hi, nl));
    return m;
}
#endif

#endif  // NEON || AVX2

}  // namespace

bool check_line_tail(const uint8_t* data, size_t length, int depth, size_t* close_off, size_t* nl_off) {
    bool closed = false;
    size_t off = 0;
#if defined(RUGO_JSONL_SCAN_NEON) || defined(RUGO_JSONL_SCAN_AVX2)
    uint64_t prev_escaped = 0, prev_in_string = 0;
    for (size_t i = 0; i < length; i += 64) {
        const size_t valid = length - i < 64 ? length - i : 64;
        TailMasks m;
        if (valid == 64) {
            m = classify_tail(data + i);
        } else {
            // The buffer's last bytes, padded with spaces (never a marker).
            uint8_t pad[64];
            std::memset(pad, ' ', sizeof pad);
            std::memcpy(pad, data + i, valid);
            m = classify_tail(pad);
        }
        // `keep`: the bytes of this block that belong to the line (before its newline).
        uint64_t keep = ~0ull;
        bool end = valid < 64;
        if (m.newline) {
            const unsigned stop = static_cast<unsigned>(__builtin_ctzll(m.newline));
            keep = stop ? (~0ull >> (64 - stop)) : 0ull;
            *nl_off = i + stop;
            end = true;
        } else if (end) {
            *nl_off = length;
        }
        m.quote &= keep;
        m.backslash &= keep;
        const uint64_t carry_in = prev_in_string;
        const uint64_t escaped = (m.backslash | prev_escaped) ? find_escaped(m.backslash, &prev_escaped) : 0;
        const uint64_t in_str = prefix_xor(m.quote & ~escaped) ^ prev_in_string;
        prev_in_string = 0ull - (in_str >> 63);
        const uint64_t op = m.open & keep & ~in_str, cl = m.close & keep & ~in_str;
        bool ok = true;
        if ((op | cl) != 0) {
            if (closed) {
                ok = false;  // a bracket after the record closed: a second value on the line
            } else {
                // The depth cannot reach 0 inside this block: count, don't walk.
                const int nc = __builtin_popcountll(cl);
                if (depth > nc) {
                    depth += __builtin_popcountll(op) - nc;
                } else {
                    for (uint64_t b = op | cl; b; b &= b - 1) {
                        if (closed) { ok = false; break; }
                        if (op & b & (0ull - b)) { ++depth; continue; }
                        if (--depth == 0) { closed = true; off = i + static_cast<size_t>(__builtin_ctzll(b)); }
                    }
                }
            }
        }
        if (!ok) {
            // Rejected: only the line's end is still wanted.
            if (!end) {
                const size_t from = i + 64;
                const void* p = std::memchr(data + from, '\n', length - from);
                *nl_off = p ? static_cast<size_t>(static_cast<const uint8_t*>(p) - data) : length;
            }
            return false;
        }
        if (end) {
            // The string state at the line's last byte (the carry-in when the newline is
            // the block's first byte).
            const bool open_str = keep ? ((in_str >> (63 - __builtin_clzll(keep))) & 1ull) != 0
                                       : (carry_in & 1ull) != 0;
            if (open_str || !closed) return false;
            *close_off = off;
            return true;
        }
    }
    *nl_off = length;
    return false;
#else
    // Byte-for-byte the SIMD answer, with scan_structural_index's escape rule.
    bool in_s = false, esc = false, ok = true;
    size_t i = 0;
    for (; i < length; ++i) {
        const uint8_t c = data[i];
        if (c == '\n') break;
        if (!ok) continue;
        if (esc) {
            esc = false;
            if (c == '"' || c == '\\') continue;
        } else if (c == '\\') {
            esc = true;
            continue;
        }
        if (c == '"') { in_s = !in_s; continue; }
        if (in_s) continue;
        if (c == '{' || c == '[') {
            if (closed) ok = false;
            else ++depth;
        } else if (c == '}' || c == ']') {
            if (closed) ok = false;
            else if (--depth == 0) { closed = true; off = i; }
        }
    }
    *nl_off = i;
    if (!ok || in_s || !closed) return false;
    *close_off = off;
    return true;
#endif
}

size_t scan_structural_index(const uint8_t* data, size_t length, uint32_t base, uint32_t* out) {
    uint64_t state[2] = {0, 0};
    return scan_structural_index_cont(data, length, base, out, state);
}

size_t scan_structural_index_cont(const uint8_t* data, size_t length, uint32_t base, uint32_t* out,
                                  uint64_t state[2]) {
    size_t count = 0;
#if defined(RUGO_JSONL_SCAN_NEON) || defined(RUGO_JSONL_SCAN_AVX2)
    uint64_t prev_escaped = state[0], prev_in_string = state[1];
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
    state[0] = prev_escaped;
    state[1] = prev_in_string;
#else
    // Byte-for-byte the SIMD answer: a backslash escapes the next byte wherever it is
    // (find_escaped), which only ever un-reals a quote; an escaped structural outside a
    // string is still written.
    bool in_s = state[1] != 0, esc = state[0] != 0;
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
    state[0] = esc ? 1u : 0u;
    state[1] = in_s ? 1u : 0u;
#endif
    return count;
}

}  // namespace rugo::_jsonl
