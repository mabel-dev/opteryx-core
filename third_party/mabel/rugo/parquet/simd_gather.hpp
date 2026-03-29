#pragma once

#include <cstddef>
#include <cstdint>
#include <vector>
#include <atomic>
#include <initializer_list>
#include <utility>

#ifdef __AVX2__
#include <immintrin.h>
#endif

#ifdef __ARM_NEON
#include <arm_neon.h>
#endif

#include "cpu_features.h"
#include "simd_dispatch.h"

// SIMD-accelerated dictionary gather operations for parquet decoding.
//
// Pattern: dst[i] = dict[indices[i]] for all indices
//
// Dispatch flow:
//   - Compile-time: AVX2/NEON support detected
//   - Runtime: simd::select_dispatch() picks best available implementation
//   - Fallback: scalar loop if no SIMD available or OPTERYX_DISABLE_SIMD

namespace parquet_simd {

// ---------------------------------------------------------------------------
// INT32 Dictionary Gather
// ---------------------------------------------------------------------------

// Scalar fallback
static inline void gather_int32_scalar(
    const int32_t* dict,
    const int32_t* indices,
    size_t count,
    std::vector<int32_t>& output)
{
    size_t old_sz = output.size();
    output.resize(old_sz + count);
    int32_t* dst = output.data() + old_sz;

    for (size_t i = 0; i < count; ++i) {
        dst[i] = dict[indices[i]];
    }
}

#ifdef __AVX2__
// AVX2: _mm256_i32gather_epi32 gathers 8 int32 values per instruction
// Note: indices must be valid; no bounds checking at SIMD level
static inline void gather_int32_avx2(
    const int32_t* dict,
    const int32_t* indices,
    size_t count,
    std::vector<int32_t>& output)
{
    size_t old_sz = output.size();
    output.resize(old_sz + count);
    int32_t* dst = output.data() + old_sz;

    // Process 8 indices at a time
    size_t full_groups = count / 8;
    for (size_t i = 0; i < full_groups; ++i) {
        // Load 8 indices: indices[i*8], indices[i*8+1], ..., indices[i*8+7]
        __m256i idx = _mm256_loadu_si256((__m256i*)(indices + i * 8));

        // Gather 8 int32 values from dict using the indices
        // Scale factor = 4 (sizeof(int32_t))
        __m256i vals = _mm256_i32gather_epi32(dict, idx, 4);

        // Store 8 gathered values to output
        _mm256_storeu_si256((__m256i*)(dst + i * 8), vals);
    }

    // Handle remainder with scalar loop
    size_t remainder = count % 8;
    size_t tail_start = full_groups * 8;
    for (size_t i = 0; i < remainder; ++i) {
        dst[tail_start + i] = dict[indices[tail_start + i]];
    }
}
#endif

// Dispatch
using gather_int32_fn_t = void(*)(const int32_t*, const int32_t*, size_t, std::vector<int32_t>&);
static std::atomic<gather_int32_fn_t> s_gather_int32_cache{nullptr};

static inline gather_int32_fn_t get_gather_int32_fn()
{
    return simd::select_dispatch<gather_int32_fn_t>(s_gather_int32_cache, {
#if defined(__AVX2__)
        {&cpu_supports_avx2, gather_int32_avx2},
#endif
    }, gather_int32_scalar);
}

static inline void gather_int32(
    const int32_t* dict,
    const int32_t* indices,
    size_t count,
    std::vector<int32_t>& output)
{
    return get_gather_int32_fn()(dict, indices, count, output);
}

// ---------------------------------------------------------------------------
// INT64 Dictionary Gather
// ---------------------------------------------------------------------------

// Scalar fallback
static inline void gather_int64_scalar(
    const int64_t* dict,
    const int32_t* indices,
    size_t count,
    std::vector<int64_t>& output)
{
    size_t old_sz = output.size();
    output.resize(old_sz + count);
    int64_t* dst = output.data() + old_sz;

    for (size_t i = 0; i < count; ++i) {
        dst[i] = dict[indices[i]];
    }
}

#ifdef __AVX2__
// AVX2: _mm256_i64gather_epi64 gathers 4 int64 values per instruction
static inline void gather_int64_avx2(
    const int64_t* dict,
    const int32_t* indices,
    size_t count,
    std::vector<int64_t>& output)
{
    size_t old_sz = output.size();
    output.resize(old_sz + count);
    int64_t* dst = output.data() + old_sz;

    // Process 4 indices at a time
    size_t full_groups = count / 4;
    for (size_t i = 0; i < full_groups; ++i) {
        // Load 4 indices and zero-extend to int64
        __m128i idx32 = _mm_loadu_si128((__m128i*)(indices + i * 4));
        __m256i idx64 = _mm256_cvtepi32_epi64(idx32);

        // Gather 4 int64 values from dict using the indices
        // Scale factor = 8 (sizeof(int64_t))
        __m256i vals = _mm256_i64gather_epi64(dict, idx64, 8);

        // Store 4 gathered values to output
        _mm256_storeu_si256((__m256i*)(dst + i * 4), vals);
    }

    // Handle remainder with scalar loop
    size_t remainder = count % 4;
    size_t tail_start = full_groups * 4;
    for (size_t i = 0; i < remainder; ++i) {
        dst[tail_start + i] = dict[indices[tail_start + i]];
    }
}
#endif

// Dispatch
using gather_int64_fn_t = void(*)(const int64_t*, const int32_t*, size_t, std::vector<int64_t>&);
static std::atomic<gather_int64_fn_t> s_gather_int64_cache{nullptr};

static inline gather_int64_fn_t get_gather_int64_fn()
{
    return simd::select_dispatch<gather_int64_fn_t>(s_gather_int64_cache, {
#if defined(__AVX2__)
        {&cpu_supports_avx2, gather_int64_avx2},
#endif
    }, gather_int64_scalar);
}

static inline void gather_int64(
    const int64_t* dict,
    const int32_t* indices,
    size_t count,
    std::vector<int64_t>& output)
{
    return get_gather_int64_fn()(dict, indices, count, output);
}

// ---------------------------------------------------------------------------
// FLOAT32 Dictionary Gather
// ---------------------------------------------------------------------------

// Scalar fallback
static inline void gather_float32_scalar(
    const float* dict,
    const int32_t* indices,
    size_t count,
    std::vector<float>& output)
{
    size_t old_sz = output.size();
    output.resize(old_sz + count);
    float* dst = output.data() + old_sz;

    for (size_t i = 0; i < count; ++i) {
        dst[i] = dict[indices[i]];
    }
}

#ifdef __AVX2__
// AVX2: reuse _mm256_i32gather_epi32 and reinterpret as float
static inline void gather_float32_avx2(
    const float* dict,
    const int32_t* indices,
    size_t count,
    std::vector<float>& output)
{
    size_t old_sz = output.size();
    output.resize(old_sz + count);
    float* dst = output.data() + old_sz;

    size_t full_groups = count / 8;
    for (size_t i = 0; i < full_groups; ++i) {
        __m256i idx = _mm256_loadu_si256((__m256i*)(indices + i * 8));
        // Gather as int32, reinterpret as float
        __m256i vals_int = _mm256_i32gather_epi32((const int32_t*)dict, idx, 4);
        __m256 vals_float = _mm256_castsi256_ps(vals_int);
        _mm256_storeu_ps(dst + i * 8, vals_float);
    }

    size_t remainder = count % 8;
    size_t tail_start = full_groups * 8;
    for (size_t i = 0; i < remainder; ++i) {
        dst[tail_start + i] = dict[indices[tail_start + i]];
    }
}
#endif

// Dispatch
using gather_float32_fn_t = void(*)(const float*, const int32_t*, size_t, std::vector<float>&);
static std::atomic<gather_float32_fn_t> s_gather_float32_cache{nullptr};

static inline gather_float32_fn_t get_gather_float32_fn()
{
    return simd::select_dispatch<gather_float32_fn_t>(s_gather_float32_cache, {
#if defined(__AVX2__)
        {&cpu_supports_avx2, gather_float32_avx2},
#endif
    }, gather_float32_scalar);
}

static inline void gather_float32(
    const float* dict,
    const int32_t* indices,
    size_t count,
    std::vector<float>& output)
{
    return get_gather_float32_fn()(dict, indices, count, output);
}

// ---------------------------------------------------------------------------
// FLOAT64 Dictionary Gather
// ---------------------------------------------------------------------------

// Scalar fallback
static inline void gather_float64_scalar(
    const double* dict,
    const int32_t* indices,
    size_t count,
    std::vector<double>& output)
{
    size_t old_sz = output.size();
    output.resize(old_sz + count);
    double* dst = output.data() + old_sz;

    for (size_t i = 0; i < count; ++i) {
        dst[i] = dict[indices[i]];
    }
}

#ifdef __AVX2__
// AVX2: use _mm256_i64gather_epi64 with indices zero-extended to int64
static inline void gather_float64_avx2(
    const double* dict,
    const int32_t* indices,
    size_t count,
    std::vector<double>& output)
{
    size_t old_sz = output.size();
    output.resize(old_sz + count);
    double* dst = output.data() + old_sz;

    size_t full_groups = count / 4;
    for (size_t i = 0; i < full_groups; ++i) {
        __m128i idx32 = _mm_loadu_si128((__m128i*)(indices + i * 4));
        __m256i idx64 = _mm256_cvtepi32_epi64(idx32);
        // Gather as int64, reinterpret as double
        __m256i vals_int = _mm256_i64gather_epi64((const int64_t*)dict, idx64, 8);
        __m256d vals_double = _mm256_castsi256_pd(vals_int);
        _mm256_storeu_pd(dst + i * 4, vals_double);
    }

    size_t remainder = count % 4;
    size_t tail_start = full_groups * 4;
    for (size_t i = 0; i < remainder; ++i) {
        dst[tail_start + i] = dict[indices[tail_start + i]];
    }
}
#endif

// Dispatch
using gather_float64_fn_t = void(*)(const double*, const int32_t*, size_t, std::vector<double>&);
static std::atomic<gather_float64_fn_t> s_gather_float64_cache{nullptr};

static inline gather_float64_fn_t get_gather_float64_fn()
{
    return simd::select_dispatch<gather_float64_fn_t>(s_gather_float64_cache, {
#if defined(__AVX2__)
        {&cpu_supports_avx2, gather_float64_avx2},
#endif
    }, gather_float64_scalar);
}

static inline void gather_float64(
    const double* dict,
    const int32_t* indices,
    size_t count,
    std::vector<double>& output)
{
    return get_gather_float64_fn()(dict, indices, count, output);
}

} // namespace parquet_simd
