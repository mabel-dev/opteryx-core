#pragma once
// Volnitsky substring search with SIMD-accelerated first-char sieve.
//
// Based on: https://web.archive.org/web/20191021173915/http://volnitsky.com/project/str_search/index.html
// Licence: Public Domain
//
// CS: case-sensitive.
// CI: case-insensitive — caller supplies a pre-lowercased pattern; the
//     haystack is NOT required to be lowercased (folding is done inline).
//
// Usage:
//   VolnitskyTable* t = volnitsky_alloc();
//   volnitsky_build(t, pattern, pattern_len);         // once per query
//   bool hit = volnitsky_contains_cs(hay, len, pat, plen, t);  // per element
//   volnitsky_free(t);

#include <cstdint>
#include <cstdlib>
#include <cstring>

#if defined(__AVX2__)
#  include <immintrin.h>
#elif defined(__ARM_NEON)
#  include <arm_neon.h>
#endif

// ---------------------------------------------------------------------------
// Bigram → 1-based pattern offset table  (128 KB)
//   entries[bigram] = 0          → bigram absent from pattern
//   entries[bigram] = k  (k > 0) → bigram starts at position (k-1) in pattern
//
// With perfect hashing (16-bit value → array index), we store one position
// per bigram: the rightmost occurrence (maximizes skip on mismatch).
// Repeated bigrams are found through exhaustive stepping.
// ---------------------------------------------------------------------------
struct VolnitskyTable {
    uint16_t entries[65536];
};

static inline VolnitskyTable* volnitsky_alloc() noexcept {
    return static_cast<VolnitskyTable*>(malloc(sizeof(VolnitskyTable)));
}

static inline void volnitsky_free(VolnitskyTable* t) noexcept {
    free(t);
}

// ASCII fold: A-Z → a-z, everything else unchanged.
static inline uint8_t _vn_lower(uint8_t b) noexcept {
    return static_cast<uint8_t>((b >= 'A' && b <= 'Z') ? b | 0x20u : b);
}

// ---------------------------------------------------------------------------
// Build the bigram table.
// Fill right-to-left so the rightmost occurrence of each bigram is stored.
// For CI searches: pass the pre-lowercased pattern.
// ---------------------------------------------------------------------------
static inline void volnitsky_build(
    VolnitskyTable* __restrict__ t,
    const uint8_t*  __restrict__ pat,
    size_t len) noexcept
{
    memset(t->entries, 0, sizeof(t->entries));
    if (len < 2) return;
    for (int i = static_cast<int>(len) - 2; i >= 0; --i) {
        const uint16_t h =
            (static_cast<uint16_t>(pat[i]) << 8) | pat[i + 1];
        if (!t->entries[h])
            t->entries[h] = static_cast<uint16_t>(i + 1);   // 1-based
    }
}

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

// ---------------------------------------------------------------------------
// Helper: find first occurrence of a byte (case‑insensitive).
// lo = lowercase version of the pattern's first character.
// hi = uppercase version (may equal lo if not a letter).
// Returns index or SIZE_MAX.
// ---------------------------------------------------------------------------
static inline size_t _find_first_byte_ci(
    const uint8_t* hay,
    size_t hay_len,
    uint8_t lo,
    uint8_t hi) noexcept
{
    const bool two_cases = (lo != hi);
#if defined(__AVX2__)
    const __m256i vlo = _mm256_set1_epi8(static_cast<char>(lo));
    const __m256i vhi = _mm256_set1_epi8(static_cast<char>(hi));
    size_t i = 0;
    for (; i + 32 <= hay_len; i += 32) {
        const __m256i chunk = _mm256_loadu_si256(
            reinterpret_cast<const __m256i*>(hay + i));
        const __m256i mlo = _mm256_cmpeq_epi8(chunk, vlo);
        unsigned mask = two_cases
            ? _mm256_movemask_epi8(_mm256_or_si256(mlo, _mm256_cmpeq_epi8(chunk, vhi)))
            : _mm256_movemask_epi8(mlo);
        if (mask) {
            return i + __builtin_ctz(mask);
        }
    }
    for (; i < hay_len; ++i) {
        if (hay[i] == lo || hay[i] == hi) return i;
    }
    return SIZE_MAX;
#elif defined(__ARM_NEON)
    const uint8x16_t vlo = vdupq_n_u8(lo);
    const uint8x16_t vhi = vdupq_n_u8(hi);
    size_t i = 0;
    for (; i + 16 <= hay_len; i += 16) {
        const uint8x16_t chunk = vld1q_u8(hay + i);
        const uint8x16_t cmp = two_cases
            ? vorrq_u8(vceqq_u8(chunk, vlo), vceqq_u8(chunk, vhi))
            : vceqq_u8(chunk, vlo);
        // vshrn nibble-mask (see _find_first_byte): ctzll>>2 = first match lane.
        const uint64_t mask = vget_lane_u64(
            vreinterpret_u64_u8(vshrn_n_u16(vreinterpretq_u16_u8(cmp), 4)), 0);
        if (mask) return i + (__builtin_ctzll(mask) >> 2);
    }
    for (; i < hay_len; ++i) {
        if (hay[i] == lo || hay[i] == hi) return i;
    }
    return SIZE_MAX;
#else
    for (size_t i = 0; i < hay_len; ++i) {
        if (hay[i] == lo || hay[i] == hi) return i;
    }
    return SIZE_MAX;
#endif
}

// ---------------------------------------------------------------------------
// Case-sensitive contains
// ---------------------------------------------------------------------------
static inline bool volnitsky_contains_cs(
    const uint8_t*        __restrict__ hay,
    size_t                             hay_len,
    const uint8_t*        __restrict__ pat,
    size_t                             pat_len,
    const VolnitskyTable* __restrict__ table) noexcept
{
    if (pat_len == 0) return true;
    if (hay_len < pat_len) return false;

    const uint8_t c0 = pat[0];
    size_t first_idx = _find_first_byte(hay, hay_len, c0);
    if (first_idx == SIZE_MAX) return false;
    if (pat_len == 1) return true;

    // Start Volnitsky at a window ending at least pat_len-1 after first_idx
    size_t pos = first_idx + pat_len - 1;
    if (pos >= hay_len) return false;

    for (; pos < hay_len; ) {
        const uint16_t h =
            (static_cast<uint16_t>(hay[pos - 1]) << 8) | hay[pos];
        const uint16_t k = table->entries[h];
        if (!k) {
            pos += pat_len - 1;
        } else {
            const size_t hay_start = pos - k;
            if (__builtin_expect(hay_start + pat_len <= hay_len, 1)) {
                if (memcmp(hay + hay_start, pat, pat_len) == 0) {
                    return true;
                }
            }
            pos += 1;
        }
    }
    return false;
}

// ---------------------------------------------------------------------------
// simd_contains_cs — case-sensitive contains, first+last-byte SIMD verify.
//
// Measured faster than volnitsky_contains_cs on realistic (non-worst-case)
// data: 1.2-8.6x across common-first-byte and hit workloads at haystack
// lengths 24B-64KB, needles 2-32B (see memory: contains kernel benchmark).
// No bigram table: nothing to allocate, build, or free per call.
//
// Rows <= CHUNK: a single first+last-byte SIMD pass over the whole row
// (Muła's algorithm) — compare pat[0] and pat[pat_len-1] at every candidate
// offset in parallel, verify the middle bytes only where both match.
//
// Rows > CHUNK: the same pass runs per CHUNK-byte window, but each window is
// skipped via a first-byte-only SIMD sieve first. The sieve is cheaper per
// byte than the two-load first+last pass, so long rows whose first byte is
// rare (Volnitsky's best case) still complete in close to Volnitsky's time,
// while every other shape stays 1.2-3x faster than Volnitsky. Windows extend
// pat_len-1 bytes past the chunk boundary so a match spanning two chunks is
// still found.
// ---------------------------------------------------------------------------
static inline bool _simd_first_last_verify(
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
                return true;
            mask &= mask - 1;
        }
    }
    for (; i < span; ++i)
        if (hay[i] == f && hay[i + last] == l &&
            (pat_len <= 2 || memcmp(hay + i + 1, pat + 1, pat_len - 2) == 0))
            return true;
    return false;
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
                return true;
            mask &= ~(0xFull << (j << 2));
        }
    }
    for (; i < span; ++i)
        if (hay[i] == f && hay[i + last] == l &&
            (pat_len <= 2 || memcmp(hay + i + 1, pat + 1, pat_len - 2) == 0))
            return true;
    return false;
#else
    for (size_t i = 0; i < span; ++i)
        if (hay[i] == f && hay[i + last] == l &&
            (pat_len <= 2 || memcmp(hay + i + 1, pat + 1, pat_len - 2) == 0))
            return true;
    return false;
#endif
}

static inline bool simd_contains_cs(
    const uint8_t* __restrict__ hay, size_t hay_len,
    const uint8_t* __restrict__ pat, size_t pat_len) noexcept
{
    if (pat_len == 0) return true;
    if (hay_len < pat_len) return false;
    if (pat_len == 1) return memchr(hay, pat[0], hay_len) != nullptr;

    constexpr size_t CHUNK = 1024;
    if (hay_len <= CHUNK)
        return _simd_first_last_verify(hay, hay_len, pat, pat_len);

    const uint8_t f = pat[0];
    for (size_t base = 0; base < hay_len; base += CHUNK) {
        const size_t clen = (CHUNK < hay_len - base) ? CHUNK : (hay_len - base);
        if (_find_first_byte(hay + base, clen, f) == SIZE_MAX) continue;
        const size_t wlen = ((clen + pat_len - 1) < (hay_len - base))
                                 ? (clen + pat_len - 1) : (hay_len - base);
        if (_simd_first_last_verify(hay + base, wlen, pat, pat_len)) return true;
    }
    return false;
}

// ---------------------------------------------------------------------------
// Case-insensitive contains.
// Haystack is NOT required to be pre-lowercased; folding is inline.
// Pattern MUST be pre-lowercased by the caller.
// ---------------------------------------------------------------------------
static inline bool volnitsky_contains_ci(
    const uint8_t*        __restrict__ hay,
    size_t                             hay_len,
    const uint8_t*        __restrict__ pat_lower,
    size_t                             pat_len,
    const VolnitskyTable* __restrict__ table) noexcept
{
    if (pat_len == 0) return true;
    if (hay_len < pat_len) return false;

    const uint8_t lo0 = pat_lower[0];
    const uint8_t hi0 = static_cast<uint8_t>(
        (lo0 >= 'a' && lo0 <= 'z') ? lo0 - 32u : lo0);

    size_t first_idx = _find_first_byte_ci(hay, hay_len, lo0, hi0);
    if (first_idx == SIZE_MAX) return false;
    if (pat_len == 1) return true;

    size_t pos = first_idx + pat_len - 1;
    if (pos >= hay_len) return false;

    for (; pos < hay_len; ) {
        const uint16_t h =
            (static_cast<uint16_t>(_vn_lower(hay[pos - 1])) << 8) |
            _vn_lower(hay[pos]);
        const uint16_t k = table->entries[h];
        if (!k) {
            pos += pat_len - 1;
        } else {
            const size_t hay_start = pos - k;
            if (__builtin_expect(hay_start + pat_len <= hay_len, 1)) {
                const uint8_t* hs = hay + hay_start;
                bool ok = true;
                for (size_t j = 0; j < pat_len; ++j) {
                    if (_vn_lower(hs[j]) != pat_lower[j]) {
                        ok = false;
                        break;
                    }
                }
                if (ok) return true;
            }
            pos += 1;
        }
    }
    return false;
}