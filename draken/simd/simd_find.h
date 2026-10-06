#pragma once
// simd_find.h — case-sensitive substring search primitives, draken-owned.
//
// Part of draken's shared SIMD layer (draken/simd) so draken kernels
// (fk_contains_hit), rugo (the parquet page search, the JSONL prefilter) and
// opteryx (src/cpp/volnitsky.h) share ONE implementation without draken
// depending on opteryx's src/cpp. Moved out of src/cpp/volnitsky.h 2026-10-06.

#include <cstddef>
#include <cstdint>
#include <cstring>

#if defined(__AVX2__)
#  include <immintrin.h>
#elif defined(__ARM_NEON)
#  include <arm_neon.h>
#endif

// ---------------------------------------------------------------------------
// Helper: find first occurrence of a byte (case‑sensitive).
// Returns index in [0, hay_len) or SIZE_MAX if not found.
// ---------------------------------------------------------------------------
static inline size_t _find_first_byte(
    const uint8_t* hay,
    size_t hay_len,
    uint8_t c) noexcept
{
#if defined(__AVX2__)
    const __m256i vp = _mm256_set1_epi8(static_cast<char>(c));
    size_t i = 0;
    for (; i + 32 <= hay_len; i += 32) {
        const __m256i chunk = _mm256_loadu_si256(
            reinterpret_cast<const __m256i*>(hay + i));
        unsigned mask = _mm256_movemask_epi8(_mm256_cmpeq_epi8(chunk, vp));
        if (mask) {
            return i + __builtin_ctz(mask);
        }
    }
    for (; i < hay_len; ++i) {
        if (hay[i] == c) return i;
    }
    return SIZE_MAX;
#elif defined(__ARM_NEON)
    const uint8x16_t vp = vdupq_n_u8(c);
    size_t i = 0;
    for (; i + 16 <= hay_len; i += 16) {
        const uint8x16_t cmp = vceqq_u8(vld1q_u8(hay + i), vp);
        // NEON has no movemask; narrow each 0x00/0xFF lane to a nibble via
        // vshrn (right-shift+narrow), giving a 64-bit value whose (4·j)th nibble
        // is set iff lane j matched. ctzll>>2 = first matching lane — no scalar
        // rescan of the 16 bytes.
        const uint64_t mask = vget_lane_u64(
            vreinterpret_u64_u8(vshrn_n_u16(vreinterpretq_u16_u8(cmp), 4)), 0);
        if (mask) return i + (__builtin_ctzll(mask) >> 2);
    }
    for (; i < hay_len; ++i) {
        if (hay[i] == c) return i;
    }
    return SIZE_MAX;
#else
    const void* p = memchr(hay, c, hay_len);
    return p ? static_cast<size_t>(static_cast<const uint8_t*>(p) - hay) : SIZE_MAX;
#endif
}

// simd_find_cs / simd_contains_cs — case-sensitive substring search,
// first+last-byte SIMD verify.
//
// Measured faster than volnitsky_contains_cs on realistic (non-worst-case)
// data: 1.2-8.6x across common-first-byte and hit workloads at haystack
// lengths 24B-64KB, needles 2-32B (see memory: contains kernel benchmark).
// No bigram table: nothing to allocate, build, or free per call.
//
// Haystacks <= CHUNK: a single first+last-byte SIMD pass over the whole input
// (Muła's algorithm) — compare pat[0] and pat[pat_len-1] at every candidate
// offset in parallel, verify the middle bytes only where both match.
//
// Haystacks > CHUNK: the same pass runs per CHUNK-byte window, but each window
// is skipped via a first-byte-only SIMD sieve first. The sieve is cheaper per
// byte than the two-load first+last pass, so long inputs whose first byte is
// rare (Volnitsky's best case) still complete in close to Volnitsky's time,
// while every other shape stays 1.2-3x faster than Volnitsky. Windows extend
// pat_len-1 bytes past the chunk boundary so a match spanning two chunks is
// still found, and window k only admits match STARTS inside chunk k, so the
// first match found is the leftmost.
//
// simd_find_cs returns the offset of the LEFTMOST occurrence (SIZE_MAX when
// absent) — the parquet page search (rugo/src/parquet/page_search.hpp) needs
// the position to map a hit back to its value. simd_contains_cs is the same
// search reduced to a bool; there is one inner loop, not two.
// ---------------------------------------------------------------------------
static inline size_t _simd_first_last_find(
    const uint8_t* __restrict__ hay, size_t hay_len,
    const uint8_t* __restrict__ pat, size_t pat_len) noexcept
{
    const uint8_t f    = pat[0];
    const uint8_t l    = pat[pat_len - 1];
    const size_t  last = pat_len - 1;
    const size_t  span = hay_len - last;   // hay_len >= pat_len is guaranteed by callers

#if defined(__AVX2__)
    const __m256i vf = _mm256_set1_epi8(static_cast<char>(f));
    const __m256i vl = _mm256_set1_epi8(static_cast<char>(l));
    size_t i = 0;
    for (; i + 32 <= span; i += 32) {
        const __m256i bf = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(hay + i));
        const __m256i bl = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(hay + i + last));
        unsigned mask = _mm256_movemask_epi8(
            _mm256_and_si256(_mm256_cmpeq_epi8(bf, vf), _mm256_cmpeq_epi8(bl, vl)));
        while (mask) {
            const unsigned j = __builtin_ctz(mask);
            if (pat_len <= 2 || memcmp(hay + i + j + 1, pat + 1, pat_len - 2) == 0)
                return i + j;
            mask &= mask - 1;
        }
    }
    for (; i < span; ++i)
        if (hay[i] == f && hay[i + last] == l &&
            (pat_len <= 2 || memcmp(hay + i + 1, pat + 1, pat_len - 2) == 0))
            return i;
    return SIZE_MAX;
#elif defined(__ARM_NEON)
    const uint8x16_t vf = vdupq_n_u8(f);
    const uint8x16_t vl = vdupq_n_u8(l);
    size_t i = 0;
    for (; i + 16 <= span; i += 16) {
        const uint8x16_t bf = vld1q_u8(hay + i);
        const uint8x16_t bl = vld1q_u8(hay + i + last);
        const uint8x16_t eq = vandq_u8(vceqq_u8(bf, vf), vceqq_u8(bl, vl));
        // vshrn nibble-mask (see _find_first_byte): ctzll>>2 = first match lane.
        uint64_t mask = vget_lane_u64(
            vreinterpret_u64_u8(vshrn_n_u16(vreinterpretq_u16_u8(eq), 4)), 0);
        while (mask) {
            const unsigned j = static_cast<unsigned>(__builtin_ctzll(mask) >> 2);
            if (pat_len <= 2 || memcmp(hay + i + j + 1, pat + 1, pat_len - 2) == 0)
                return i + j;
            mask &= ~(0xFull << (j << 2));
        }
    }
    for (; i < span; ++i)
        if (hay[i] == f && hay[i + last] == l &&
            (pat_len <= 2 || memcmp(hay + i + 1, pat + 1, pat_len - 2) == 0))
            return i;
    return SIZE_MAX;
#else
    for (size_t i = 0; i < span; ++i)
        if (hay[i] == f && hay[i + last] == l &&
            (pat_len <= 2 || memcmp(hay + i + 1, pat + 1, pat_len - 2) == 0))
            return i;
    return SIZE_MAX;
#endif
}

static inline size_t simd_find_cs(
    const uint8_t* __restrict__ hay, size_t hay_len,
    const uint8_t* __restrict__ pat, size_t pat_len) noexcept
{
    if (pat_len == 0) return 0;
    if (hay_len < pat_len) return SIZE_MAX;
    if (pat_len == 1) {
        const void* p = memchr(hay, pat[0], hay_len);
        return p != nullptr ? static_cast<size_t>(static_cast<const uint8_t*>(p) - hay) : SIZE_MAX;
    }

    constexpr size_t CHUNK = 1024;
    if (hay_len <= CHUNK)
        return _simd_first_last_find(hay, hay_len, pat, pat_len);

    const uint8_t f = pat[0];
    for (size_t base = 0; base < hay_len; base += CHUNK) {
        const size_t clen = (CHUNK < hay_len - base) ? CHUNK : (hay_len - base);
        if (_find_first_byte(hay + base, clen, f) == SIZE_MAX) continue;
        const size_t wlen = ((clen + pat_len - 1) < (hay_len - base))
                                 ? (clen + pat_len - 1) : (hay_len - base);
        // A final chunk shorter than the pattern holds no match start: every earlier
        // window already reached pat_len-1 bytes past its chunk. Searching it would break
        // _simd_first_last_find's hay_len >= pat_len contract (span underflows and the
        // scan reads past the haystack — a false positive from the next string's bytes).
        if (wlen < pat_len) break;
        const size_t r = _simd_first_last_find(hay + base, wlen, pat, pat_len);
        if (r != SIZE_MAX) return base + r;
    }
    return SIZE_MAX;
}

static inline bool simd_contains_cs(
    const uint8_t* __restrict__ hay, size_t hay_len,
    const uint8_t* __restrict__ pat, size_t pat_len) noexcept
{
    return simd_find_cs(hay, hay_len, pat, pat_len) != SIZE_MAX;
}
