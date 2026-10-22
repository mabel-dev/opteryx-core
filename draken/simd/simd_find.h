#pragma once
// simd_find.h — case-sensitive substring search primitives, draken-owned.
//
// Part of draken's shared SIMD layer (draken/simd) so draken kernels
// (fk_contains_hit), rugo (the parquet page search, the JSONL prefilter) and
// opteryx (src/cpp/volnitsky.h) share ONE implementation without draken
// depending on opteryx's src/cpp. Moved out of src/cpp/volnitsky.h 2026-10-06.
//
// Algorithm (2026-10-22): a two-anchor SIMD candidate filter. Two bytes of the
// needle at positions a < b are compared against the haystack at every offset
// in parallel (Muła's first+last scheme generalised to any pair); only offsets
// where BOTH match are verified with memcmp. The main loop takes 64 bytes per
// step and tests "any candidate in these 64 bytes" with one horizontal reduce
// before it extracts a single mask, so a candidate-free stretch costs 8 loads,
// 8 compares and one reduce per 64 bytes.
//
// Which pair: the generic entry (simd_find_cs) uses first+last. A caller that
// searches a large region picks the two RAREST needle bytes from a histogram of
// a sample of that region (simd_pick_pair) and passes them to simd_find_cs_pair
// — on URL-like text the first byte of a needle is often common ('g' is 1.3%
// of URL bytes), and a rare pair cuts the candidate rate by orders of
// magnitude. Measured 2026-10-22 on real URL dictionary bytes (M5 Pro, one
// thread): first+last with the old 1 KB first-byte sieve 14.7 GB/s; first+last
// 64-byte loop 37 GB/s; rare pair 64-byte loop 59 GB/s. The old sieve LOST
// whenever the first byte was common (every 1 KB chunk had one) and is gone.
//
// simd_find_cs returns the offset of the LEFTMOST occurrence (SIZE_MAX when
// absent) — the parquet page search (rugo/src/parquet/page_search.hpp) needs
// the position to map a hit back to its value. simd_contains_cs is the same
// search reduced to a bool.

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

// ---------------------------------------------------------------------------
// simd_pick_pair — choose the two anchor positions (a < b) of `pat` whose bytes
// are rarest in `sample` (a histogram of the first `sample_len` bytes of the
// region about to be searched). Ties keep the earlier position; the two
// positions are always distinct. pat_len must be >= 2. Cost is one byte
// histogram of the sample — pass a bounded prefix (page_search uses 16 KiB),
// never the whole region, and pick ONCE per region, not per call.
// ---------------------------------------------------------------------------
static inline void simd_pick_pair(
    const uint8_t* sample, size_t sample_len,
    const uint8_t* pat, size_t pat_len,
    size_t* a, size_t* b) noexcept
{
    uint32_t hist[256];
    std::memset(hist, 0, sizeof(hist));
    for (size_t i = 0; i < sample_len; ++i) hist[sample[i]]++;
    // Start from the ends (the first+last pair), then let any interior position
    // with a strictly rarer byte displace the commoner of the two.
    size_t p0 = 0, p1 = pat_len - 1;
    uint32_t c0 = hist[pat[p0]], c1 = hist[pat[p1]];
    if (c1 < c0) { const size_t tp = p0; p0 = p1; p1 = tp; const uint32_t tc = c0; c0 = c1; c1 = tc; }
    for (size_t i = 1; i + 1 < pat_len; ++i) {
        const uint32_t c = hist[pat[i]];
        if (c < c0)      { p1 = p0; c1 = c0; p0 = i; c0 = c; }
        else if (c < c1) { p1 = i; c1 = c; }
    }
    if (p0 < p1) { *a = p0; *b = p1; } else { *a = p1; *b = p0; }
}

#if defined(__ARM_NEON)
// 0x00/0xFF lanes -> 64-bit nibble mask (lane j matched iff nibble j != 0).
static inline uint64_t _simd_neon_nibble_mask(uint8x16_t eq) noexcept {
    return vget_lane_u64(
        vreinterpret_u64_u8(vshrn_n_u16(vreinterpretq_u16_u8(eq), 4)), 0);
}
#endif

// Two-anchor search core. Preconditions (checked by the public entries):
// pat_len >= 2, hay_len >= pat_len, a < b <= pat_len - 1.
// Window i covers match starts i .. i+W-1; it loads hay[i+a .. i+a+W-1] and
// hay[i+b .. i+b+W-1], which stay inside the haystack whenever i + W <= span
// (span = hay_len - pat_len + 1 and b <= pat_len - 1). The first candidate
// found is the leftmost because windows advance monotonically and each
// window's candidates are taken in ascending lane order.
static inline size_t _simd_pair_find(
    const uint8_t* __restrict__ hay, size_t hay_len,
    const uint8_t* __restrict__ pat, size_t pat_len,
    size_t a, size_t b) noexcept
{
    const size_t span = hay_len - pat_len + 1;
    const uint8_t ba = pat[a];
    const uint8_t bb = pat[b];
    size_t i = 0;

#if defined(__AVX2__)
    const __m256i va = _mm256_set1_epi8(static_cast<char>(ba));
    const __m256i vb = _mm256_set1_epi8(static_cast<char>(bb));
    auto block = [&](size_t off) -> unsigned {
        const __m256i la = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(hay + off + a));
        const __m256i lb = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(hay + off + b));
        return static_cast<unsigned>(_mm256_movemask_epi8(
            _mm256_and_si256(_mm256_cmpeq_epi8(la, va), _mm256_cmpeq_epi8(lb, vb))));
    };
    for (; i + 64 <= span; i += 64) {
        const unsigned m0 = block(i);
        const unsigned m1 = block(i + 32);
        if ((m0 | m1) == 0) continue;
        unsigned m = m0;
        while (m) {
            const unsigned j = __builtin_ctz(m);
            if (memcmp(hay + i + j, pat, pat_len) == 0) return i + j;
            m &= m - 1;
        }
        m = m1;
        while (m) {
            const unsigned j = __builtin_ctz(m);
            if (memcmp(hay + i + 32 + j, pat, pat_len) == 0) return i + 32 + j;
            m &= m - 1;
        }
    }
    for (; i + 32 <= span; i += 32) {
        unsigned m = block(i);
        while (m) {
            const unsigned j = __builtin_ctz(m);
            if (memcmp(hay + i + j, pat, pat_len) == 0) return i + j;
            m &= m - 1;
        }
    }
#elif defined(__ARM_NEON)
    const uint8x16_t va = vdupq_n_u8(ba);
    const uint8x16_t vb = vdupq_n_u8(bb);
    auto block = [&](size_t off) -> uint8x16_t {
        return vandq_u8(vceqq_u8(vld1q_u8(hay + off + a), va),
                        vceqq_u8(vld1q_u8(hay + off + b), vb));
    };
    for (; i + 64 <= span; i += 64) {
        const uint8x16_t e0 = block(i);
        const uint8x16_t e1 = block(i + 16);
        const uint8x16_t e2 = block(i + 32);
        const uint8x16_t e3 = block(i + 48);
        const uint8x16_t any = vorrq_u8(vorrq_u8(e0, e1), vorrq_u8(e2, e3));
        if (vmaxvq_u8(any) == 0) continue;
        const uint8x16_t es[4] = {e0, e1, e2, e3};
        for (int k = 0; k < 4; ++k) {
            uint64_t m = _simd_neon_nibble_mask(es[k]);
            while (m) {
                const unsigned j = static_cast<unsigned>(__builtin_ctzll(m) >> 2);
                const size_t at = i + static_cast<size_t>(16 * k) + j;
                if (memcmp(hay + at, pat, pat_len) == 0) return at;
                m &= ~(0xFull << (j << 2));
            }
        }
    }
    for (; i + 16 <= span; i += 16) {
        uint64_t m = _simd_neon_nibble_mask(block(i));
        while (m) {
            const unsigned j = static_cast<unsigned>(__builtin_ctzll(m) >> 2);
            if (memcmp(hay + i + j, pat, pat_len) == 0) return i + j;
            m &= ~(0xFull << (j << 2));
        }
    }
#endif
    for (; i < span; ++i)
        if (hay[i + a] == ba && hay[i + b] == bb && memcmp(hay + i, pat, pat_len) == 0)
            return i;
    return SIZE_MAX;
}

// Leftmost occurrence with caller-chosen anchors (see simd_pick_pair).
// a and b must be distinct positions inside the needle; order is free.
static inline size_t simd_find_cs_pair(
    const uint8_t* __restrict__ hay, size_t hay_len,
    const uint8_t* __restrict__ pat, size_t pat_len,
    size_t a, size_t b) noexcept
{
    if (pat_len == 0) return 0;
    if (hay_len < pat_len) return SIZE_MAX;
    if (pat_len == 1) {
        const void* p = memchr(hay, pat[0], hay_len);
        return p != nullptr ? static_cast<size_t>(static_cast<const uint8_t*>(p) - hay) : SIZE_MAX;
    }
    if (a > b) { const size_t t = a; a = b; b = t; }
    return _simd_pair_find(hay, hay_len, pat, pat_len, a, b);
}

// Leftmost occurrence, first+last anchors.
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
    return _simd_pair_find(hay, hay_len, pat, pat_len, 0, pat_len - 1);
}

static inline bool simd_contains_cs(
    const uint8_t* __restrict__ hay, size_t hay_len,
    const uint8_t* __restrict__ pat, size_t pat_len) noexcept
{
    return simd_find_cs(hay, hay_len, pat, pat_len) != SIZE_MAX;
}
