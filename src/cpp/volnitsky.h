#pragma once
// Volnitsky substring search with SIMD-accelerated first-char sieve.
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
// Fill right-to-left so that, for repeated bigrams, the rightmost occurrence
// gets the smallest k (= largest skip opportunity on mismatch).
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

    if (pat_len == 1)
        return memchr(hay, static_cast<int>(pat[0]), hay_len) != nullptr;

    // -----------------------------------------------------------------------
    // SIMD first-char sieve — bail immediately when pat[0] is absent.
    // -----------------------------------------------------------------------
    const uint8_t c0 = pat[0];
    bool has_first = false;

#if defined(__AVX2__)
    {
        const __m256i vp = _mm256_set1_epi8(static_cast<char>(c0));
        size_t i = 0;
        for (; i + 32 <= hay_len; i += 32) {
            const __m256i chunk = _mm256_loadu_si256(
                reinterpret_cast<const __m256i*>(hay + i));
            if (_mm256_movemask_epi8(_mm256_cmpeq_epi8(chunk, vp))) {
                has_first = true;
                break;
            }
        }
        if (!has_first)
            for (; i < hay_len; ++i)
                if (hay[i] == c0) { has_first = true; break; }
    }
#elif defined(__ARM_NEON)
    {
        const uint8x16_t vp = vdupq_n_u8(c0);
        size_t i = 0;
        for (; i + 16 <= hay_len; i += 16) {
            const uint8x16_t cmp = vceqq_u8(vld1q_u8(hay + i), vp);
            if (vgetq_lane_u64(vreinterpretq_u64_u8(cmp), 0) |
                vgetq_lane_u64(vreinterpretq_u64_u8(cmp), 1)) {
                has_first = true;
                break;
            }
        }
        if (!has_first)
            for (; i < hay_len; ++i)
                if (hay[i] == c0) { has_first = true; break; }
    }
#else
    has_first = (memchr(hay, static_cast<int>(c0), hay_len) != nullptr);
#endif

    if (!has_first) return false;

    // -----------------------------------------------------------------------
    // Volnitsky end-window scan.
    //
    // pos  = end position of the current window (0-indexed).
    // bigram (hay[pos-1], hay[pos]) looked up in table → k (1-based).
    // k == 0: bigram absent → skip by pat_len-1.
    // k  > 0: bigram starts at position (k-1) in pattern → candidate match
    //         starts at hay[pos - k]; verify with memcmp.
    // -----------------------------------------------------------------------
    for (size_t pos = pat_len - 1; pos < hay_len; ) {
        const uint16_t h =
            (static_cast<uint16_t>(hay[pos - 1]) << 8) | hay[pos];
        const uint16_t k = table->entries[h];
        if (!k) {
            pos += pat_len - 1;
        } else {
            const size_t hay_start = pos - k;
            // Guard: entire pattern must fit within haystack at this offset.
            if (__builtin_expect(hay_start + pat_len <= hay_len, 1))
                if (memcmp(hay + hay_start, pat, pat_len) == 0) return true;
            pos += 1;
        }
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

    if (pat_len == 1) {
        const uint8_t lo = pat_lower[0];
        const uint8_t hi = static_cast<uint8_t>(
            (lo >= 'a' && lo <= 'z') ? lo - 32u : lo);
        for (size_t i = 0; i < hay_len; ++i)
            if (hay[i] == lo || hay[i] == hi) return true;
        return false;
    }

    // -----------------------------------------------------------------------
    // SIMD sieve: scan for both lowercase and uppercase of pat_lower[0].
    // -----------------------------------------------------------------------
    const uint8_t lo0 = pat_lower[0];
    const uint8_t hi0 = static_cast<uint8_t>(
        (lo0 >= 'a' && lo0 <= 'z') ? lo0 - 32u : lo0);
    const bool two_cases = (lo0 != hi0);
    bool has_first = false;

#if defined(__AVX2__)
    {
        const __m256i vlo = _mm256_set1_epi8(static_cast<char>(lo0));
        const __m256i vhi = _mm256_set1_epi8(static_cast<char>(hi0));
        size_t i = 0;
        for (; i + 32 <= hay_len; i += 32) {
            const __m256i chunk = _mm256_loadu_si256(
                reinterpret_cast<const __m256i*>(hay + i));
            const __m256i mlo = _mm256_cmpeq_epi8(chunk, vlo);
            const int mask = _mm256_movemask_epi8(
                two_cases
                    ? _mm256_or_si256(mlo, _mm256_cmpeq_epi8(chunk, vhi))
                    : mlo);
            if (mask) { has_first = true; break; }
        }
        if (!has_first)
            for (; i < hay_len; ++i)
                if (hay[i] == lo0 || hay[i] == hi0) { has_first = true; break; }
    }
#elif defined(__ARM_NEON)
    {
        const uint8x16_t vlo = vdupq_n_u8(lo0);
        const uint8x16_t vhi = vdupq_n_u8(hi0);
        size_t i = 0;
        for (; i + 16 <= hay_len; i += 16) {
            const uint8x16_t chunk = vld1q_u8(hay + i);
            const uint8x16_t cmp = two_cases
                ? vorrq_u8(vceqq_u8(chunk, vlo), vceqq_u8(chunk, vhi))
                : vceqq_u8(chunk, vlo);
            if (vgetq_lane_u64(vreinterpretq_u64_u8(cmp), 0) |
                vgetq_lane_u64(vreinterpretq_u64_u8(cmp), 1)) {
                has_first = true;
                break;
            }
        }
        if (!has_first)
            for (; i < hay_len; ++i)
                if (hay[i] == lo0 || hay[i] == hi0) { has_first = true; break; }
    }
#else
    for (size_t i = 0; i < hay_len && !has_first; ++i)
        if (hay[i] == lo0 || hay[i] == hi0) has_first = true;
#endif

    if (!has_first) return false;

    // -----------------------------------------------------------------------
    // Volnitsky scan with inline case-fold on the bigram bytes.
    // -----------------------------------------------------------------------
    for (size_t pos = pat_len - 1; pos < hay_len; ) {
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
                    if (_vn_lower(hs[j]) != pat_lower[j]) { ok = false; break; }
                }
                if (ok) return true;
            }
            pos += 1;
        }
    }
    return false;
}
