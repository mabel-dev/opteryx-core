#pragma once

#include <cstddef>
#include <cstdint>
#include <atomic>

#ifdef __AVX2__
#include <immintrin.h>
#endif

#include "cpu_features.h"
#include "simd_dispatch.h"

// SIMD-accelerated type widening for parquet decoding.
//
// Pattern: widen int32→int64 or float32→float64 with SIMD acceleration.
// Typical use: after decoding int32 dictionary values, widen to int64 for Draken.
//
// Dispatch flow:
//   - Compile-time: SIMD_STATIC_SELECT picks the target ISA's variant
//   - Fallback: scalar loop

namespace parquet_simd {

// ---------------------------------------------------------------------------
// INT32 → INT64 Widening
// ---------------------------------------------------------------------------

// Scalar fallback
static inline void widen_int32_to_int64_scalar(
    const int32_t* src,
    int64_t* dst,
    size_t count)
{
    for (size_t i = 0; i < count; ++i) {
        dst[i] = (int64_t)src[i];
    }
}

#ifdef __AVX2__
// AVX2: _mm256_cvtepi32_epi64 widens 4 int32 → 4 int64 per instruction
static inline void widen_int32_to_int64_avx2(
    const int32_t* src,
    int64_t* dst,
    size_t count)
{
    // Process 4 int32 values at a time
    size_t full_groups = count / 4;
    for (size_t i = 0; i < full_groups; ++i) {
        // Load 4 int32 values
        __m128i src_v = _mm_loadu_si128((__m128i*)(src + i * 4));
        
        // Widen to 4 int64 values
        __m256i dst_v = _mm256_cvtepi32_epi64(src_v);
        
        // Store 4 int64 values
        _mm256_storeu_si256((__m256i*)(dst + i * 4), dst_v);
    }
    
    // Handle remainder with scalar
    size_t remainder = count % 4;
    size_t tail_start = full_groups * 4;
    for (size_t i = 0; i < remainder; ++i) {
        dst[tail_start + i] = (int64_t)src[tail_start + i];
    }
}
#endif

// Dispatch
using widen_int32_to_int64_fn_t = void(*)(const int32_t*, int64_t*, size_t);

static inline widen_int32_to_int64_fn_t get_widen_int32_fn()
{
    return SIMD_STATIC_SELECT(widen_int32_to_int64_avx2, widen_int32_to_int64_scalar, widen_int32_to_int64_scalar, widen_int32_to_int64_scalar);
}

static inline void widen_int32_to_int64(
    const int32_t* src,
    int64_t* dst,
    size_t count)
{
    return get_widen_int32_fn()(src, dst, count);
}

// ---------------------------------------------------------------------------
// FLOAT32 → FLOAT64 Widening
// ---------------------------------------------------------------------------

// Scalar fallback
static inline void widen_float32_to_float64_scalar(
    const float* src,
    double* dst,
    size_t count)
{
    for (size_t i = 0; i < count; ++i) {
        dst[i] = (double)src[i];
    }
}

#ifdef __AVX2__
// AVX2: _mm256_cvtps_pd widens 4 float32 → 4 float64 per instruction
static inline void widen_float32_to_float64_avx2(
    const float* src,
    double* dst,
    size_t count)
{
    // Process 4 float32 values at a time
    size_t full_groups = count / 4;
    for (size_t i = 0; i < full_groups; ++i) {
        // Load 4 float32 values
        __m128 src_v = _mm_loadu_ps(src + i * 4);
        
        // Widen to 4 float64 values
        __m256d dst_v = _mm256_cvtps_pd(src_v);
        
        // Store 4 float64 values
        _mm256_storeu_pd(dst + i * 4, dst_v);
    }
    
    // Handle remainder with scalar
    size_t remainder = count % 4;
    size_t tail_start = full_groups * 4;
    for (size_t i = 0; i < remainder; ++i) {
        dst[tail_start + i] = (double)src[tail_start + i];
    }
}
#endif

// Dispatch
using widen_float32_to_float64_fn_t = void(*)(const float*, double*, size_t);

static inline widen_float32_to_float64_fn_t get_widen_float32_fn()
{
    return SIMD_STATIC_SELECT(widen_float32_to_float64_avx2, widen_float32_to_float64_scalar, widen_float32_to_float64_scalar, widen_float32_to_float64_scalar);
}

static inline void widen_float32_to_float64(
    const float* src,
    double* dst,
    size_t count)
{
    return get_widen_float32_fn()(src, dst, count);
}

} // namespace parquet_simd
