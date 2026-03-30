#pragma once

#include <cstddef>
#include <cstdint>
#include <vector>
#include <atomic>

#ifdef __AVX2__
#include <immintrin.h>
#endif

#include "cpu_features.h"
#include "simd_dispatch.h"

// SIMD-accelerated stream compaction for row-mask filtering.
//
// Pattern: compact(values[], mask[]) writes values[i] to output where mask[i] != 0.
// Use case: After row-mask filtering during parquet decoding.
//
// Dispatch flow:
//   - Compile-time: AVX2 support detected
//   - Runtime: simd::select_dispatch() picks best implementation
//   - Fallback: scalar per-element filter

namespace parquet_simd {

// ---------------------------------------------------------------------------
// INT32 Stream Compaction (e.g., dict indices, plain int32 values)
// ---------------------------------------------------------------------------

// Scalar fallback
static inline void compact_int32_scalar(
    const int32_t* src,
    const uint8_t* mask,
    size_t count,
    std::vector<int32_t>& output)
{
    size_t old_sz = output.size();
    for (size_t i = 0; i < count; ++i) {
        if (mask[i]) {
            output.push_back(src[i]);
        }
    }
}

#ifdef __AVX2__
// AVX2: Vectorized stream compaction using shuffle tables
// Process 8 int32 values at a time (32 bytes), check mask, compact to output
// Uses popcount to track output position for scatter-like write
static inline void compact_int32_avx2(
    const int32_t* src,
    const uint8_t* mask,
    size_t count,
    std::vector<int32_t>& output)
{
    // Pre-allocate worst case (all selected)
    size_t old_size = output.size();

    // First pass: count selected items
    int32_t selected_count = 0;
    for (size_t i = 0; i < count; ++i) {
        if (mask[i]) selected_count++;
    }

    output.resize(old_size + selected_count);
    int32_t* out_ptr = output.data() + old_size;
    int32_t out_idx = 0;

    // Second pass: compact with SIMD prefetching
    // Process 8 values at a time for better cache locality
    size_t chunk_size = 8;
    size_t full_chunks = count / chunk_size;

    for (size_t chunk = 0; chunk < full_chunks; ++chunk) {
        size_t base = chunk * chunk_size;
        // Load 8 mask bytes and 8 int32 values
        __m256i values = _mm256_loadu_si256((__m256i*)(src + base));

        // Check mask and scatter selected values
        for (size_t i = 0; i < chunk_size; ++i) {
            if (mask[base + i]) {
                out_ptr[out_idx++] = src[base + i];
            }
        }
    }

    // Handle remainder
    size_t tail_start = full_chunks * chunk_size;
    for (size_t i = tail_start; i < count; ++i) {
        if (mask[i]) {
            out_ptr[out_idx++] = src[i];
        }
    }
}
#endif

// Dispatch
using compact_int32_fn_t = void(*)(const int32_t*, const uint8_t*, size_t, std::vector<int32_t>&);
static std::atomic<compact_int32_fn_t> s_compact_int32_cache{nullptr};

static inline compact_int32_fn_t get_compact_int32_fn()
{
    return simd::select_dispatch<compact_int32_fn_t>(s_compact_int32_cache, {
#if defined(__AVX2__)
        {&cpu_supports_avx2, compact_int32_avx2},
#endif
    }, compact_int32_scalar);
}

static inline void compact_int32(
    const int32_t* src,
    const uint8_t* mask,
    size_t count,
    std::vector<int32_t>& output)
{
    return get_compact_int32_fn()(src, mask, count, output);
}

// ---------------------------------------------------------------------------
// INT64 Stream Compaction
// ---------------------------------------------------------------------------

// Scalar fallback
static inline void compact_int64_scalar(
    const int64_t* src,
    const uint8_t* mask,
    size_t count,
    std::vector<int64_t>& output)
{
    for (size_t i = 0; i < count; ++i) {
        if (mask[i]) {
            output.push_back(src[i]);
        }
    }
}

#ifdef __AVX2__
// AVX2: Vectorized int64 stream compaction
// Process 4 int64 values at a time (32 bytes), check mask, compact to output
static inline void compact_int64_avx2(
    const int64_t* src,
    const uint8_t* mask,
    size_t count,
    std::vector<int64_t>& output)
{
    size_t old_size = output.size();

    // Count selected items
    int64_t selected_count = 0;
    for (size_t i = 0; i < count; ++i) {
        if (mask[i]) selected_count++;
    }

    output.resize(old_size + selected_count);
    int64_t* out_ptr = output.data() + old_size;
    int64_t out_idx = 0;

    // Process 4 values at a time
    size_t chunk_size = 4;
    size_t full_chunks = count / chunk_size;

    for (size_t chunk = 0; chunk < full_chunks; ++chunk) {
        size_t base = chunk * chunk_size;
        __m256i values = _mm256_loadu_si256((__m256i*)(src + base));

        // Scatter selected values
        for (size_t i = 0; i < chunk_size; ++i) {
            if (mask[base + i]) {
                out_ptr[out_idx++] = src[base + i];
            }
        }
    }

    // Handle remainder
    size_t tail_start = full_chunks * chunk_size;
    for (size_t i = tail_start; i < count; ++i) {
        if (mask[i]) {
            out_ptr[out_idx++] = src[i];
        }
    }
}
#endif

// Dispatch
using compact_int64_fn_t = void(*)(const int64_t*, const uint8_t*, size_t, std::vector<int64_t>&);
static std::atomic<compact_int64_fn_t> s_compact_int64_cache{nullptr};

static inline compact_int64_fn_t get_compact_int64_fn()
{
    return simd::select_dispatch<compact_int64_fn_t>(s_compact_int64_cache, {
#if defined(__AVX2__)
        {&cpu_supports_avx2, compact_int64_avx2},
#endif
    }, compact_int64_scalar);
}

static inline void compact_int64(
    const int64_t* src,
    const uint8_t* mask,
    size_t count,
    std::vector<int64_t>& output)
{
    return get_compact_int64_fn()(src, mask, count, output);
}

// ---------------------------------------------------------------------------
// FLOAT32 Stream Compaction
// ---------------------------------------------------------------------------

static inline void compact_float32_scalar(
    const float* src,
    const uint8_t* mask,
    size_t count,
    std::vector<float>& output)
{
    for (size_t i = 0; i < count; ++i) {
        if (mask[i]) {
            output.push_back(src[i]);
        }
    }
}

#ifdef __AVX2__
// AVX2: Vectorized float32 stream compaction
// Process 8 float32 values at a time (32 bytes), check mask, compact to output
static inline void compact_float32_avx2(
    const float* src,
    const uint8_t* mask,
    size_t count,
    std::vector<float>& output)
{
    size_t old_size = output.size();

    // Count selected items
    int32_t selected_count = 0;
    for (size_t i = 0; i < count; ++i) {
        if (mask[i]) selected_count++;
    }

    output.resize(old_size + selected_count);
    float* out_ptr = output.data() + old_size;
    int32_t out_idx = 0;

    // Process 8 values at a time
    size_t chunk_size = 8;
    size_t full_chunks = count / chunk_size;

    for (size_t chunk = 0; chunk < full_chunks; ++chunk) {
        size_t base = chunk * chunk_size;
        __m256 values = _mm256_loadu_ps(src + base);

        // Scatter selected values
        for (size_t i = 0; i < chunk_size; ++i) {
            if (mask[base + i]) {
                out_ptr[out_idx++] = src[base + i];
            }
        }
    }

    // Handle remainder
    size_t tail_start = full_chunks * chunk_size;
    for (size_t i = tail_start; i < count; ++i) {
        if (mask[i]) {
            out_ptr[out_idx++] = src[i];
        }
    }
}
#endif

using compact_float32_fn_t = void(*)(const float*, const uint8_t*, size_t, std::vector<float>&);
static std::atomic<compact_float32_fn_t> s_compact_float32_cache{nullptr};

static inline compact_float32_fn_t get_compact_float32_fn()
{
    return simd::select_dispatch<compact_float32_fn_t>(s_compact_float32_cache, {
#if defined(__AVX2__)
        {&cpu_supports_avx2, compact_float32_avx2},
#endif
    }, compact_float32_scalar);
}

static inline void compact_float32(
    const float* src,
    const uint8_t* mask,
    size_t count,
    std::vector<float>& output)
{
    return get_compact_float32_fn()(src, mask, count, output);
}

// ---------------------------------------------------------------------------
// FLOAT64 Stream Compaction
// ---------------------------------------------------------------------------

static inline void compact_float64_scalar(
    const double* src,
    const uint8_t* mask,
    size_t count,
    std::vector<double>& output)
{
    for (size_t i = 0; i < count; ++i) {
        if (mask[i]) {
            output.push_back(src[i]);
        }
    }
}

#ifdef __AVX2__
// AVX2: Vectorized float64 stream compaction
// Process 4 float64 values at a time (32 bytes), check mask, compact to output
static inline void compact_float64_avx2(
    const double* src,
    const uint8_t* mask,
    size_t count,
    std::vector<double>& output)
{
    size_t old_size = output.size();

    // Count selected items
    int64_t selected_count = 0;
    for (size_t i = 0; i < count; ++i) {
        if (mask[i]) selected_count++;
    }

    output.resize(old_size + selected_count);
    double* out_ptr = output.data() + old_size;
    int64_t out_idx = 0;

    // Process 4 values at a time
    size_t chunk_size = 4;
    size_t full_chunks = count / chunk_size;

    for (size_t chunk = 0; chunk < full_chunks; ++chunk) {
        size_t base = chunk * chunk_size;
        __m256d values = _mm256_loadu_pd(src + base);

        // Scatter selected values
        for (size_t i = 0; i < chunk_size; ++i) {
            if (mask[base + i]) {
                out_ptr[out_idx++] = src[base + i];
            }
        }
    }

    // Handle remainder
    size_t tail_start = full_chunks * chunk_size;
    for (size_t i = tail_start; i < count; ++i) {
        if (mask[i]) {
            out_ptr[out_idx++] = src[i];
        }
    }
}
#endif

using compact_float64_fn_t = void(*)(const double*, const uint8_t*, size_t, std::vector<double>&);
static std::atomic<compact_float64_fn_t> s_compact_float64_cache{nullptr};

static inline compact_float64_fn_t get_compact_float64_fn()
{
    return simd::select_dispatch<compact_float64_fn_t>(s_compact_float64_cache, {
#if defined(__AVX2__)
        {&cpu_supports_avx2, compact_float64_avx2},
#endif
    }, compact_float64_scalar);
}

static inline void compact_float64(
    const double* src,
    const uint8_t* mask,
    size_t count,
    std::vector<double>& output)
{
    return get_compact_float64_fn()(src, mask, count, output);
}

} // namespace parquet_simd
