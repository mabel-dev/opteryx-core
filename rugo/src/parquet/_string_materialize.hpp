#pragma once

#include <cstddef>
#include <cstdint>

#ifdef __AVX2__
#include <immintrin.h>
#endif

#ifdef __ARM_NEON
#include <arm_neon.h>
#endif

// SIMD helpers for StringVector materialization from dict-encoded codes.
//
// Pass 1: byte-count via SIMD gather of dict_lens[codes].
//   Equivalent scalar:
//     int64_t total = 0;
//     for (size_t i = 0; i < count; ++i) total += dict_lens[codes[i]];
//
// Compile-time dispatch only (build targets pinned: NEON for ARM dev,
// AVX2 for x86 prod — see CLAUDE.md §6). No runtime CPU detection.
//
// Caller is responsible for bounds-checking codes against dict size at
// entry to the materialization path; the inner loop trusts them.

namespace rugo_strmat {

#if defined(__AVX2__)

// 8 codes/iter via vpgatherdd; horizontal-sum the gathered lengths.
static inline int64_t sum_dict_lens_avx2(
    const int32_t* __restrict__ dict_lens,
    const int32_t* __restrict__ codes,
    size_t count)
{
    __m256i acc_lo = _mm256_setzero_si256();
    __m256i acc_hi = _mm256_setzero_si256();

    size_t full = count & ~size_t(7);
    for (size_t i = 0; i < full; i += 8) {
        __m256i idx = _mm256_loadu_si256(
            reinterpret_cast<const __m256i*>(codes + i));
        // Gather 8 int32 lengths
        __m256i v = _mm256_i32gather_epi32(dict_lens, idx, 4);
        // Widen to int64 to avoid overflow on the running accumulator
        __m256i v_lo = _mm256_cvtepi32_epi64(_mm256_castsi256_si128(v));
        __m256i v_hi = _mm256_cvtepi32_epi64(_mm256_extracti128_si256(v, 1));
        acc_lo = _mm256_add_epi64(acc_lo, v_lo);
        acc_hi = _mm256_add_epi64(acc_hi, v_hi);
    }

    __m256i acc = _mm256_add_epi64(acc_lo, acc_hi);
    alignas(32) int64_t lanes[4];
    _mm256_store_si256(reinterpret_cast<__m256i*>(lanes), acc);
    int64_t total = lanes[0] + lanes[1] + lanes[2] + lanes[3];

    for (size_t i = full; i < count; ++i) {
        total += static_cast<int64_t>(dict_lens[codes[i]]);
    }
    return total;
}

#endif // __AVX2__

#if defined(__ARM_NEON)

// 4 codes/iter via 4× scalar lane-loads (no native gather on NEON), widened
// to int64 for accumulation. Two parallel accumulators for ILP.
static inline int64_t sum_dict_lens_neon(
    const int32_t* __restrict__ dict_lens,
    const int32_t* __restrict__ codes,
    size_t count)
{
    int64x2_t acc0 = vdupq_n_s64(0);
    int64x2_t acc1 = vdupq_n_s64(0);

    size_t full = count & ~size_t(3);
    for (size_t i = 0; i < full; i += 4) {
        // Manual gather: NEON has no scatter/gather, so 4 scalar loads.
        int32x4_t v;
        v = vsetq_lane_s32(dict_lens[codes[i + 0]], v, 0);
        v = vsetq_lane_s32(dict_lens[codes[i + 1]], v, 1);
        v = vsetq_lane_s32(dict_lens[codes[i + 2]], v, 2);
        v = vsetq_lane_s32(dict_lens[codes[i + 3]], v, 3);
        // Widen halves to int64 and accumulate
        int64x2_t lo = vmovl_s32(vget_low_s32(v));
        int64x2_t hi = vmovl_s32(vget_high_s32(v));
        acc0 = vaddq_s64(acc0, lo);
        acc1 = vaddq_s64(acc1, hi);
    }

    int64_t total = vaddvq_s64(vaddq_s64(acc0, acc1));
    for (size_t i = full; i < count; ++i) {
        total += static_cast<int64_t>(dict_lens[codes[i]]);
    }
    return total;
}

#endif // __ARM_NEON

static inline int64_t sum_dict_lens_scalar(
    const int32_t* __restrict__ dict_lens,
    const int32_t* __restrict__ codes,
    size_t count)
{
    int64_t total = 0;
    for (size_t i = 0; i < count; ++i) {
        total += static_cast<int64_t>(dict_lens[codes[i]]);
    }
    return total;
}

// Compile-time dispatch — single entry point used from Cython.
static inline int64_t sum_dict_lens(
    const int32_t* dict_lens,
    const int32_t* codes,
    size_t count)
{
#if defined(__AVX2__)
    return sum_dict_lens_avx2(dict_lens, codes, count);
#elif defined(__ARM_NEON)
    return sum_dict_lens_neon(dict_lens, codes, count);
#else
    return sum_dict_lens_scalar(dict_lens, codes, count);
#endif
}

// Pass 2: build exclusive prefix-sum offsets[] from dict_lens[codes].
//   offsets[0]      = 0
//   offsets[i]      = offsets[i-1] + dict_lens[codes[i-1]]   (i in 1..count)
//   offsets[count]  = total bytes (returned for caller convenience)
//
// Caller is responsible for the int32 capacity guard (sum_dict_lens covers
// this in the existing pipeline). Carry is int32 to match the StringVector
// offsets layout; overflow must be ruled out before calling.

#if defined(__AVX2__)

// 8 codes/iter via vpgatherdd, then unrolled scalar exclusive-prefix on
// the materialized lengths. The dependency chain on `carry` is unavoidable
// (a serial prefix-sum), but the gather of 8 lengths runs in parallel.
static inline int32_t build_offsets_avx2(
    const int32_t* __restrict__ dict_lens,
    const int32_t* __restrict__ codes,
    size_t count,
    int32_t* __restrict__ out_offsets)
{
    int32_t carry = 0;
    size_t full = count & ~size_t(7);
    for (size_t i = 0; i < full; i += 8) {
        __m256i idx = _mm256_loadu_si256(
            reinterpret_cast<const __m256i*>(codes + i));
        __m256i v = _mm256_i32gather_epi32(dict_lens, idx, 4);
        alignas(32) int32_t lens[8];
        _mm256_store_si256(reinterpret_cast<__m256i*>(lens), v);
        out_offsets[i + 0] = carry;
        out_offsets[i + 1] = carry          + lens[0];
        out_offsets[i + 2] = out_offsets[i + 1] + lens[1];
        out_offsets[i + 3] = out_offsets[i + 2] + lens[2];
        out_offsets[i + 4] = out_offsets[i + 3] + lens[3];
        out_offsets[i + 5] = out_offsets[i + 4] + lens[4];
        out_offsets[i + 6] = out_offsets[i + 5] + lens[5];
        out_offsets[i + 7] = out_offsets[i + 6] + lens[6];
        carry              = out_offsets[i + 7] + lens[7];
    }
    for (size_t i = full; i < count; ++i) {
        out_offsets[i] = carry;
        carry += dict_lens[codes[i]];
    }
    out_offsets[count] = carry;
    return carry;
}

#endif // __AVX2__

#if defined(__ARM_NEON)

// 4 codes/iter; NEON has no native gather so the scalar lane-loads dominate.
// Unrolled exclusive-prefix on the 4 lengths gives the compiler ILP across
// the gather + prefix work.
static inline int32_t build_offsets_neon(
    const int32_t* __restrict__ dict_lens,
    const int32_t* __restrict__ codes,
    size_t count,
    int32_t* __restrict__ out_offsets)
{
    int32_t carry = 0;
    size_t full = count & ~size_t(3);
    for (size_t i = 0; i < full; i += 4) {
        int32_t l0 = dict_lens[codes[i + 0]];
        int32_t l1 = dict_lens[codes[i + 1]];
        int32_t l2 = dict_lens[codes[i + 2]];
        int32_t l3 = dict_lens[codes[i + 3]];
        out_offsets[i + 0] = carry;
        out_offsets[i + 1] = carry          + l0;
        out_offsets[i + 2] = out_offsets[i + 1] + l1;
        out_offsets[i + 3] = out_offsets[i + 2] + l2;
        carry              = out_offsets[i + 3] + l3;
    }
    for (size_t i = full; i < count; ++i) {
        out_offsets[i] = carry;
        carry += dict_lens[codes[i]];
    }
    out_offsets[count] = carry;
    return carry;
}

#endif // __ARM_NEON

static inline int32_t build_offsets_scalar(
    const int32_t* __restrict__ dict_lens,
    const int32_t* __restrict__ codes,
    size_t count,
    int32_t* __restrict__ out_offsets)
{
    int32_t carry = 0;
    for (size_t i = 0; i < count; ++i) {
        out_offsets[i] = carry;
        carry += dict_lens[codes[i]];
    }
    out_offsets[count] = carry;
    return carry;
}

static inline int32_t build_offsets(
    const int32_t* dict_lens,
    const int32_t* codes,
    size_t count,
    int32_t* out_offsets)
{
#if defined(__AVX2__)
    return build_offsets_avx2(dict_lens, codes, count, out_offsets);
#elif defined(__ARM_NEON)
    return build_offsets_neon(dict_lens, codes, count, out_offsets);
#else
    return build_offsets_scalar(dict_lens, codes, count, out_offsets);
#endif
}

} // namespace rugo_strmat
