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
//   - Compile-time: per-arch variants compiled when the target supports them,
//     selected via SIMD_STATIC_SELECT (one ISA per build; no runtime probe).
//   - Fallback: scalar per-element filter.
//
// Per-arch strategy:
//   - RISC-V RVV: native `vcompress` instruction — packs masked-in lanes to the
//     front in one op, then a masked store of vcpop() elements.
//   - ARM NEON: no compress instruction, so a per-128-bit-block byte shuffle
//     (`vqtbl1q_u8`) driven by a precomputed permutation table indexed by the
//     block's lane mask, advancing the write cursor by the block popcount.
//   - x86 AVX2: per-256-bit-block left-pack (`_mm256_permutevar8x32_epi32`)
//     driven by a lane-mask-indexed permutation table, advancing the write
//     cursor by the block popcount — the 8-wide twin of the NEON design.

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
// AVX2 left-pack: same permutation-table design as the NEON bodies below, at
// 256-bit width. For each block, the mask bytes become an N-bit lane mask
// (compare-to-zero + movemask, so any nonzero byte selects), the LUT supplies
// the _mm256_permutevar8x32_epi32 index vector that packs the selected 32-bit
// lanes to the front in order, and the write cursor advances by popcount.
// Single pass: worst-case resize up front, trim to the written count at the
// end — no separate counting pass.

// 256-entry LUT: 8-bit lane mask → packed 32-bit lane indices.
inline const std::array<std::array<uint32_t, 8>, 256>& avx2_perm8()
{
    static const std::array<std::array<uint32_t, 8>, 256> t = [] {
        std::array<std::array<uint32_t, 8>, 256> tbl{};
        for (int m = 0; m < 256; ++m) {
            int o = 0;
            for (int lane = 0; lane < 8; ++lane)
                if (m & (1 << lane)) tbl[m][o++] = (uint32_t)lane;
            for (; o < 8; ++o) tbl[m][o] = 0;  // lanes past popcount are never kept
        }
        return tbl;
    }();
    return t;
}

// 16-entry LUT: 4-bit lane mask → packed 64-bit lanes as 32-bit index pairs.
inline const std::array<std::array<uint32_t, 8>, 16>& avx2_perm4x64()
{
    static const std::array<std::array<uint32_t, 8>, 16> t = [] {
        std::array<std::array<uint32_t, 8>, 16> tbl{};
        for (int m = 0; m < 16; ++m) {
            int o = 0;
            for (int lane = 0; lane < 4; ++lane)
                if (m & (1 << lane)) {
                    tbl[m][o++] = (uint32_t)(2 * lane);
                    tbl[m][o++] = (uint32_t)(2 * lane + 1);
                }
            for (; o < 8; ++o) tbl[m][o] = 0;
        }
        return tbl;
    }();
    return t;
}

// 32-bit-lane core (int32 / float32 — identical byte layout).
template <typename T>
static inline void compact32_avx2_impl(
    const T* src, const uint8_t* mask, size_t count, std::vector<T>& output)
{
    static_assert(sizeof(T) == 4, "32-bit lanes only");
    size_t old_size = output.size();
    // +8 slack: each block stores a full 32-byte vector even when fewer lanes
    // are selected (same slack-and-trim pattern as the NEON bodies).
    output.resize(old_size + count + 8);
    T* out = output.data() + old_size;
    size_t w = 0;
    const auto& tbl = avx2_perm8();
    const __m128i zero = _mm_setzero_si128();
    size_t blocks = count / 8;
    for (size_t b = 0; b < blocks; ++b) {
        size_t base = b * 8;
        __m128i mb = _mm_loadl_epi64((const __m128i*)(mask + base));
        unsigned m = (unsigned)(~_mm_movemask_epi8(_mm_cmpeq_epi8(mb, zero))) & 0xFFu;
        __m256i v = _mm256_loadu_si256((const __m256i*)(src + base));
        __m256i perm = _mm256_loadu_si256((const __m256i*)tbl[m].data());
        _mm256_storeu_si256((__m256i*)(out + w), _mm256_permutevar8x32_epi32(v, perm));
        w += (size_t)__builtin_popcount(m);
    }
    for (size_t i = blocks * 8; i < count; ++i)
        if (mask[i]) out[w++] = src[i];
    output.resize(old_size + w);  // trim slack back to logical size
}

// 64-bit-lane core (int64 / float64).
template <typename T>
static inline void compact64_avx2_impl(
    const T* src, const uint8_t* mask, size_t count, std::vector<T>& output)
{
    static_assert(sizeof(T) == 8, "64-bit lanes only");
    size_t old_size = output.size();
    // +4 slack: full 32-byte store per block (up to 4 extra elements).
    output.resize(old_size + count + 4);
    T* out = output.data() + old_size;
    size_t w = 0;
    const auto& tbl = avx2_perm4x64();
    const __m128i zero = _mm_setzero_si128();
    size_t blocks = count / 4;
    for (size_t b = 0; b < blocks; ++b) {
        size_t base = b * 4;
        uint32_t mb4;
        __builtin_memcpy(&mb4, mask + base, 4);
        __m128i mb = _mm_cvtsi32_si128((int)mb4);
        unsigned m = (unsigned)(~_mm_movemask_epi8(_mm_cmpeq_epi8(mb, zero))) & 0xFu;
        __m256i v = _mm256_loadu_si256((const __m256i*)(src + base));
        __m256i perm = _mm256_loadu_si256((const __m256i*)tbl[m].data());
        _mm256_storeu_si256((__m256i*)(out + w), _mm256_permutevar8x32_epi32(v, perm));
        w += (size_t)__builtin_popcount(m);
    }
    for (size_t i = blocks * 4; i < count; ++i)
        if (mask[i]) out[w++] = src[i];
    output.resize(old_size + w);
}

static inline void compact_int32_avx2(
    const int32_t* src,
    const uint8_t* mask,
    size_t count,
    std::vector<int32_t>& output)
{
    compact32_avx2_impl(src, mask, count, output);
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

static inline compact_int32_fn_t get_compact_int32_fn()
{
    return SIMD_STATIC_SELECT(compact_int32_avx2, compact_int32_neon, compact_int32_rvv, compact_int32_scalar);
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
// AVX2: left-pack via avx2_perm4x64 (see compact64_avx2_impl above).
static inline void compact_int64_avx2(
    const int64_t* src,
    const uint8_t* mask,
    size_t count,
    std::vector<int64_t>& output)
{
    compact64_avx2_impl(src, mask, count, output);
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

static inline compact_int64_fn_t get_compact_int64_fn()
{
    return SIMD_STATIC_SELECT(compact_int64_avx2, compact_int64_neon, compact_int64_rvv, compact_int64_scalar);
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
// AVX2: 32-bit lanes — identical byte layout to int32, same left-pack core.
static inline void compact_float32_avx2(
    const float* src,
    const uint8_t* mask,
    size_t count,
    std::vector<float>& output)
{
    compact32_avx2_impl(src, mask, count, output);
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

static inline compact_float32_fn_t get_compact_float32_fn()
{
    return SIMD_STATIC_SELECT(compact_float32_avx2, compact_float32_neon, compact_float32_rvv, compact_float32_scalar);
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
// AVX2: 64-bit lanes — identical byte layout to int64, same left-pack core.
static inline void compact_float64_avx2(
    const double* src,
    const uint8_t* mask,
    size_t count,
    std::vector<double>& output)
{
    compact64_avx2_impl(src, mask, count, output);
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

static inline compact_float64_fn_t get_compact_float64_fn()
{
    return SIMD_STATIC_SELECT(compact_float64_avx2, compact_float64_neon, compact_float64_rvv, compact_float64_scalar);
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
