#pragma once

#include <cstddef>
#include <cstdint>
#include <vector>
#include <atomic>
#include <array>

#ifdef __AVX2__
#include <immintrin.h>
#endif

#if defined(__ARM_NEON) || defined(__aarch64__)
#include <arm_neon.h>
#endif

#if defined(__riscv) && defined(__riscv_vector)
#include <riscv_vector.h>
#endif

#include "cpu_features.h"
#include "simd_dispatch.h"

// SIMD-accelerated stream compaction for row-mask filtering.
//
// Pattern: compact(values[], mask[]) writes values[i] to output where mask[i] != 0.
// Use case: After row-mask filtering during parquet decoding.
//
// Dispatch flow:
//   - Compile-time: per-arch variants compiled when the target supports them.
//   - Runtime: simd::select_dispatch() picks the best available implementation.
//   - Fallback: scalar per-element filter.
//
// Per-arch strategy:
//   - RISC-V RVV: native `vcompress` instruction — packs masked-in lanes to the
//     front in one op, then a masked store of vcpop() elements.
//   - ARM NEON: no compress instruction, so a per-128-bit-block byte shuffle
//     (`vqtbl1q_u8`) driven by a precomputed permutation table indexed by the
//     block's lane mask, advancing the write cursor by the block popcount.
//   - x86 AVX2: kept as-is (note: the existing AVX2 body is effectively scalar).

namespace parquet_simd {

#if defined(__ARM_NEON) || defined(__aarch64__)
// Permutation tables: for a block of N lanes (each `stride` bytes), entry[m]
// gives the byte-shuffle indices (for vqtbl1q_u8) that pack the lanes whose mask
// bit is set into the front of the register, in order. Unused tail bytes are
// 0x80 (vqtbl1q emits 0 for out-of-range indices) and never stored (the write
// cursor only advances by popcount(m)). Built once, correct-by-construction.
inline const std::array<std::array<uint8_t, 16>, 16>& neon_perm4()  // 4 lanes × 4 bytes
{
    static const std::array<std::array<uint8_t, 16>, 16> t = [] {
        std::array<std::array<uint8_t, 16>, 16> tbl{};
        for (int m = 0; m < 16; ++m) {
            int o = 0;
            for (int lane = 0; lane < 4; ++lane)
                if (m & (1 << lane))
                    for (int b = 0; b < 4; ++b) tbl[m][o++] = (uint8_t)(4 * lane + b);
            for (; o < 16; ++o) tbl[m][o] = 0x80;
        }
        return tbl;
    }();
    return t;
}

inline const std::array<std::array<uint8_t, 16>, 4>& neon_perm2()  // 2 lanes × 8 bytes
{
    static const std::array<std::array<uint8_t, 16>, 4> t = [] {
        std::array<std::array<uint8_t, 16>, 4> tbl{};
        for (int m = 0; m < 4; ++m) {
            int o = 0;
            for (int lane = 0; lane < 2; ++lane)
                if (m & (1 << lane))
                    for (int b = 0; b < 8; ++b) tbl[m][o++] = (uint8_t)(8 * lane + b);
            for (; o < 16; ++o) tbl[m][o] = 0x80;
        }
        return tbl;
    }();
    return t;
}
#endif

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

#if defined(__ARM_NEON) || defined(__aarch64__)
// NEON: per-128-bit block (4×int32) byte-shuffle compaction.
static inline void compact_int32_neon(
    const int32_t* src, const uint8_t* mask, size_t count, std::vector<int32_t>& output)
{
    size_t old_size = output.size();
    size_t sel = 0;
    for (size_t i = 0; i < count; ++i) sel += (mask[i] != 0);
    // +4 slack: each block does a full 16-byte (4×int32) vst1q even when fewer
    // lanes are selected; the final block can write up to 4 elements past `sel`.
    output.resize(old_size + sel + 4);
    int32_t* out = output.data() + old_size;
    size_t w = 0;

    const auto& tbl = neon_perm4();
    size_t blocks = count / 4;
    for (size_t b = 0; b < blocks; ++b) {
        size_t base = b * 4;
        unsigned m = (mask[base] != 0) | ((mask[base + 1] != 0) << 1) |
                     ((mask[base + 2] != 0) << 2) | ((mask[base + 3] != 0) << 3);
        uint8x16_t v = vld1q_u8((const uint8_t*)(src + base));
        uint8x16_t shuf = vld1q_u8(tbl[m].data());
        uint8x16_t r = vqtbl1q_u8(v, shuf);
        vst1q_u8((uint8_t*)(out + w), r);
        w += (size_t)__builtin_popcount(m);
    }
    for (size_t i = blocks * 4; i < count; ++i)
        if (mask[i]) out[w++] = src[i];
    output.resize(old_size + sel);  // trim slack back to logical size
}
#endif

#if defined(__riscv) && defined(__riscv_vector)
// RVV: native vcompress — pack masked-in lanes, masked store of vcpop elements.
static inline void compact_int32_rvv(
    const int32_t* src, const uint8_t* mask, size_t count, std::vector<int32_t>& output)
{
    size_t old_size = output.size();
    size_t sel = 0;
    for (size_t i = 0; i < count; ++i) sel += (mask[i] != 0);
    output.resize(old_size + sel);
    int32_t* out = output.data() + old_size;
    size_t i = 0, w = 0;
    while (i < count) {
        size_t vl = __riscv_vsetvl_e32m1(count - i);
        vuint8mf4_t mb = __riscv_vle8_v_u8mf4(mask + i, vl);
        vbool32_t keep = __riscv_vmsne_vx_u8mf4_b32(mb, 0, vl);
        vint32m1_t v = __riscv_vle32_v_i32m1(src + i, vl);
        vint32m1_t c = __riscv_vcompress_vm_i32m1(v, keep, vl);
        size_t k = __riscv_vcpop_m_b32(keep, vl);
        __riscv_vse32_v_i32m1(out + w, c, k);
        w += k;
        i += vl;
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
#if defined(__ARM_NEON) || defined(__aarch64__)
        {&cpu_supports_neon, compact_int32_neon},
#endif
#if defined(__riscv) && defined(__riscv_vector)
        {&cpu_supports_rvv, compact_int32_rvv},
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

#if defined(__ARM_NEON) || defined(__aarch64__)
static inline void compact_int64_neon(
    const int64_t* src, const uint8_t* mask, size_t count, std::vector<int64_t>& output)
{
    size_t old_size = output.size();
    size_t sel = 0;
    for (size_t i = 0; i < count; ++i) sel += (mask[i] != 0);
    // +2 slack: each block does a full 16-byte (2×int64) vst1q.
    output.resize(old_size + sel + 2);
    int64_t* out = output.data() + old_size;
    size_t w = 0;

    const auto& tbl = neon_perm2();
    size_t blocks = count / 2;
    for (size_t b = 0; b < blocks; ++b) {
        size_t base = b * 2;
        unsigned m = (mask[base] != 0) | ((mask[base + 1] != 0) << 1);
        uint8x16_t v = vld1q_u8((const uint8_t*)(src + base));
        uint8x16_t r = vqtbl1q_u8(v, vld1q_u8(tbl[m].data()));
        vst1q_u8((uint8_t*)(out + w), r);
        w += (size_t)__builtin_popcount(m);
    }
    for (size_t i = blocks * 2; i < count; ++i)
        if (mask[i]) out[w++] = src[i];
    output.resize(old_size + sel);  // trim slack back to logical size
}
#endif

#if defined(__riscv) && defined(__riscv_vector)
static inline void compact_int64_rvv(
    const int64_t* src, const uint8_t* mask, size_t count, std::vector<int64_t>& output)
{
    size_t old_size = output.size();
    size_t sel = 0;
    for (size_t i = 0; i < count; ++i) sel += (mask[i] != 0);
    output.resize(old_size + sel);
    int64_t* out = output.data() + old_size;
    size_t i = 0, w = 0;
    while (i < count) {
        size_t vl = __riscv_vsetvl_e64m1(count - i);
        vuint8mf8_t mb = __riscv_vle8_v_u8mf8(mask + i, vl);
        vbool64_t keep = __riscv_vmsne_vx_u8mf8_b64(mb, 0, vl);
        vint64m1_t v = __riscv_vle64_v_i64m1(src + i, vl);
        vint64m1_t c = __riscv_vcompress_vm_i64m1(v, keep, vl);
        size_t k = __riscv_vcpop_m_b64(keep, vl);
        __riscv_vse64_v_i64m1(out + w, c, k);
        w += k;
        i += vl;
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
#if defined(__ARM_NEON) || defined(__aarch64__)
        {&cpu_supports_neon, compact_int64_neon},
#endif
#if defined(__riscv) && defined(__riscv_vector)
        {&cpu_supports_rvv, compact_int64_rvv},
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

#if defined(__ARM_NEON) || defined(__aarch64__)
static inline void compact_float32_neon(
    const float* src, const uint8_t* mask, size_t count, std::vector<float>& output)
{
    // 32-bit lanes: identical byte layout to int32 — reuse via reinterpret.
    compact_int32_neon(reinterpret_cast<const int32_t*>(src), mask, count,
                       reinterpret_cast<std::vector<int32_t>&>(output));
}
#endif

#if defined(__riscv) && defined(__riscv_vector)
static inline void compact_float32_rvv(
    const float* src, const uint8_t* mask, size_t count, std::vector<float>& output)
{
    size_t old_size = output.size();
    size_t sel = 0;
    for (size_t i = 0; i < count; ++i) sel += (mask[i] != 0);
    output.resize(old_size + sel);
    float* out = output.data() + old_size;
    size_t i = 0, w = 0;
    while (i < count) {
        size_t vl = __riscv_vsetvl_e32m1(count - i);
        vuint8mf4_t mb = __riscv_vle8_v_u8mf4(mask + i, vl);
        vbool32_t keep = __riscv_vmsne_vx_u8mf4_b32(mb, 0, vl);
        vfloat32m1_t v = __riscv_vle32_v_f32m1(src + i, vl);
        vfloat32m1_t c = __riscv_vcompress_vm_f32m1(v, keep, vl);
        size_t k = __riscv_vcpop_m_b32(keep, vl);
        __riscv_vse32_v_f32m1(out + w, c, k);
        w += k;
        i += vl;
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
#if defined(__ARM_NEON) || defined(__aarch64__)
        {&cpu_supports_neon, compact_float32_neon},
#endif
#if defined(__riscv) && defined(__riscv_vector)
        {&cpu_supports_rvv, compact_float32_rvv},
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

#if defined(__ARM_NEON) || defined(__aarch64__)
static inline void compact_float64_neon(
    const double* src, const uint8_t* mask, size_t count, std::vector<double>& output)
{
    // 64-bit lanes: identical byte layout to int64 — reuse via reinterpret.
    compact_int64_neon(reinterpret_cast<const int64_t*>(src), mask, count,
                       reinterpret_cast<std::vector<int64_t>&>(output));
}
#endif

#if defined(__riscv) && defined(__riscv_vector)
static inline void compact_float64_rvv(
    const double* src, const uint8_t* mask, size_t count, std::vector<double>& output)
{
    size_t old_size = output.size();
    size_t sel = 0;
    for (size_t i = 0; i < count; ++i) sel += (mask[i] != 0);
    output.resize(old_size + sel);
    double* out = output.data() + old_size;
    size_t i = 0, w = 0;
    while (i < count) {
        size_t vl = __riscv_vsetvl_e64m1(count - i);
        vuint8mf8_t mb = __riscv_vle8_v_u8mf8(mask + i, vl);
        vbool64_t keep = __riscv_vmsne_vx_u8mf8_b64(mb, 0, vl);
        vfloat64m1_t v = __riscv_vle64_v_f64m1(src + i, vl);
        vfloat64m1_t c = __riscv_vcompress_vm_f64m1(v, keep, vl);
        size_t k = __riscv_vcpop_m_b64(keep, vl);
        __riscv_vse64_v_f64m1(out + w, c, k);
        w += k;
        i += vl;
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
#if defined(__ARM_NEON) || defined(__aarch64__)
        {&cpu_supports_neon, compact_float64_neon},
#endif
#if defined(__riscv) && defined(__riscv_vector)
        {&cpu_supports_rvv, compact_float64_rvv},
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
