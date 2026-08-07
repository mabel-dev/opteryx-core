#pragma once
#include <cstdint>
#include <cstddef>

// Single-word blocked Bloom filter (k=2, both bits in one 64-bit word).
//
// Layout: the low bits of the hash select a 64-bit word; the HIGH bits of a
// golden-ratio multiply select two bit positions WITHIN that word. Insert is
// one RMW, probe is one load + one compare — one cache line touched per key
// instead of the two an unblocked k=2 filter pays. Costs ~0.4pt FPR (word
// occupancy is Poisson, dense words over-collide) for ~2x probe throughput.
//
// The bit positions must come from the multiply's high bits: the low bits of
// h*C depend only on the low bits of h (already spent on the word index), so
// masking the product would correlate the positions with the word choice.
static constexpr uint64_t BLOOM_GOLDEN_RATIO = 0x9E3779B97F4A7C15ULL;

// Two bit positions within the selected word, as a ready-to-use OR/test mask.
// Positions may coincide (1-in-64); the same rule on insert and probe keeps
// that consistent — it slightly raises FPR, never causes a false negative.
static inline uint64_t bloom_pair_mask(const uint64_t h) noexcept {
    const uint64_t mix = h * BLOOM_GOLDEN_RATIO;
    return (uint64_t(1) << (mix >> 58)) | (uint64_t(1) << ((mix >> 52) & 63u));
}

// Insert n hashes into the 64-bit-chunk bit array.
// bit_mask is the whole-filter BIT mask (bits-1); the word mask is derived.
// Caller owns bit_array (calloc-zeroed). Thread-unsafe for concurrent writes.
static inline void bloom_insert_many(
    uint64_t* __restrict__ bit_array,
    const uint64_t* __restrict__ hashes,
    const size_t n,
    const uint64_t bit_mask
) noexcept {
    const uint64_t word_mask = bit_mask >> 6;
    for (size_t i = 0; i < n; ++i) {
        const uint64_t h = hashes[i];
        bit_array[h & word_mask] |= bloom_pair_mask(h);
    }
}

// Probe n hashes against the bit array. Writes a bit-packed result into
// result[] (LSB-first, one bit per hash). Caller must zero result before
// calling; this function only sets bits, never clears them.
//
// Platform dispatch: NEON (ARM64) or SSE2 (x86-64), else scalar.
// No prefetch — scatter access pattern makes it counter-productive.

#if defined(__ARM_NEON) || defined(__aarch64__)
#include <arm_neon.h>

static inline void bloom_query_packed(
    const uint64_t* __restrict__ bit_array,
    const uint64_t* __restrict__ hashes,
    const size_t n,
    const uint64_t bit_mask,
    uint8_t* __restrict__ result
) noexcept {
    static const uint8_t BIT_WEIGHTS_ARR[8] = {1, 2, 4, 8, 16, 32, 64, 128};
    const uint8x8_t BIT_WEIGHTS = vld1_u8(BIT_WEIGHTS_ARR);
    const uint64_t word_mask = bit_mask >> 6;

    size_t i = 0;
    // Process 8 hashes at a time: compute 8 booleans, pack into 1 byte with NEON.
    for (; i + 8 <= n; i += 8) {
        uint8_t hits[8];
        for (int j = 0; j < 8; ++j) {
            const uint64_t h = hashes[i + j];
            const uint64_t m = bloom_pair_mask(h);
            hits[j] = (uint8_t)((bit_array[h & word_mask] & m) == m);
        }
        // hits[j] is 0 or 1; multiply by bit weight, horizontal-add → packed byte.
        result[i >> 3] = vaddv_u8(vmul_u8(vld1_u8(hits), BIT_WEIGHTS));
    }
    // Scalar tail for the remainder.
    for (; i < n; ++i) {
        const uint64_t h = hashes[i];
        const uint64_t m = bloom_pair_mask(h);
        const uint64_t hit = (bit_array[h & word_mask] & m) == m;
        result[i >> 3] |= static_cast<uint8_t>(hit << (i & 7u));
    }
}

#elif defined(__SSE2__)
#include <emmintrin.h>

static inline void bloom_query_packed(
    const uint64_t* __restrict__ bit_array,
    const uint64_t* __restrict__ hashes,
    const size_t n,
    const uint64_t bit_mask,
    uint8_t* __restrict__ result
) noexcept {
    const uint64_t word_mask = bit_mask >> 6;
    size_t i = 0;
    // 8 hashes → 8 bytes (0x00 or 0xFF) → movemask → 1 packed byte.
    for (; i + 8 <= n; i += 8) {
        alignas(8) int8_t hits[8];
        for (int j = 0; j < 8; ++j) {
            const uint64_t h = hashes[i + j];
            const uint64_t m = bloom_pair_mask(h);
            hits[j] = ((bit_array[h & word_mask] & m) == m) ? -1 : 0; // 0xFF/0x00 for movemask
        }
        // _mm_loadl_epi64: load 64-bit, zero-extend to 128-bit (high bytes = 0).
        // movemask picks MSB of all 16 bytes; low 8 are our hits, high 8 are 0.
        __m128i v = _mm_loadl_epi64(reinterpret_cast<const __m128i*>(hits));
        result[i >> 3] = (uint8_t)_mm_movemask_epi8(v);
    }
    for (; i < n; ++i) {
        const uint64_t h = hashes[i];
        const uint64_t m = bloom_pair_mask(h);
        const uint64_t hit = (bit_array[h & word_mask] & m) == m;
        result[i >> 3] |= static_cast<uint8_t>(hit << (i & 7u));
    }
}

#elif defined(__riscv_vector)
#include <riscv_vector.h>

static inline void bloom_query_packed(
    const uint64_t* __restrict__ bit_array,
    const uint64_t* __restrict__ hashes,
    const size_t n,
    const uint64_t bit_mask,
    uint8_t* __restrict__ result
) noexcept {
    static const uint8_t BIT_WEIGHTS_ARR[8] = {1, 2, 4, 8, 16, 32, 64, 128};
    const vuint8m1_t BIT_WEIGHTS = __riscv_vle8_v_u8m1(BIT_WEIGHTS_ARR, 8);
    const vuint8m1_t V_ZERO      = __riscv_vmv_v_x_u8m1(0, 8);
    const uint64_t word_mask = bit_mask >> 6;

    size_t i = 0;
    for (; i + 8 <= n; i += 8) {
        uint8_t hits[8];
        for (int j = 0; j < 8; ++j) {
            const uint64_t h = hashes[i + j];
            const uint64_t m = bloom_pair_mask(h);
            hits[j] = (uint8_t)((bit_array[h & word_mask] & m) == m);
        }
        // hits[j] is 0 or 1; multiply by bit weight, reduce-sum → packed byte.
        vuint8m1_t v = __riscv_vmul_vv_u8m1(__riscv_vle8_v_u8m1(hits, 8), BIT_WEIGHTS, 8);
        result[i >> 3] = __riscv_vmv_x_s_u8m1_u8(__riscv_vredsum_vs_u8m1_u8m1(v, V_ZERO, 8));
    }
    for (; i < n; ++i) {
        const uint64_t h = hashes[i];
        const uint64_t m = bloom_pair_mask(h);
        const uint64_t hit = (bit_array[h & word_mask] & m) == m;
        result[i >> 3] |= static_cast<uint8_t>(hit << (i & 7u));
    }
}

#else
// Scalar fallback for unsupported targets.
static inline void bloom_query_packed(
    const uint64_t* __restrict__ bit_array,
    const uint64_t* __restrict__ hashes,
    const size_t n,
    const uint64_t bit_mask,
    uint8_t* __restrict__ result
) noexcept {
    const uint64_t word_mask = bit_mask >> 6;
    for (size_t i = 0; i < n; ++i) {
        const uint64_t h = hashes[i];
        const uint64_t m = bloom_pair_mask(h);
        const uint64_t hit = (bit_array[h & word_mask] & m) == m;
        result[i >> 3] |= static_cast<uint8_t>(hit << (i & 7u));
    }
}
#endif
