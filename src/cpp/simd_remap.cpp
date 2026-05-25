#include "simd_remap.h"

#include <atomic>
#include <cstddef>
#include <cstdint>

#include "cpu_features.h"
#include "simd_dispatch.h"

#if defined(__AVX2__)
#include <immintrin.h>
#elif defined(__ARM_NEON) || defined(__ARM_NEON__)
#include <arm_neon.h>
#endif

#if defined(__riscv) && defined(__riscv_vector)
#include <riscv_vector.h>
#endif

// ── uint8 ────────────────────────────────────────────────────────────────────
//
// AVX2: 16-pass vpshufb over 32 codes at a time.
//
// vpshufb uses bits[3:0] of the selector byte as the index within a 16-byte
// table chunk, and zeroes the output when bit7 of the selector is set.
//
// For chunk k (covering codes [k*16, k*16+15]):
//   adj = code - k*16  (wraps for codes < k*16 → large value → bit7 set → zeroed ✓)
//
// Codes in [k*16+16, k*16+127] produce adj in [16..127] — bit7 is clear but
// adj > 15, so pshufb would use the wrong index.  Fix: set bit7 when adj
// is in [16..127] using the carry trick:
//   carry = (adj + 0x70) & 0x80
//   safe_adj = adj | carry
//
// Proof: adj+0x70 overflows to ≥ 0x80 iff adj ≥ 0x10 (16).
//   adj = 0x0F → 0x7F, &0x80 = 0 ✓
//   adj = 0x10 → 0x80, &0x80 = 0x80 ✓ (bit7 set)
//   adj = 0x7F → 0xEF, &0x80 = 0x80 ✓
//   adj ≥ 0x80 → bit7 already set (zeroed by pshufb) ✓
//
// NEON: 4-pass vqtbl4q_u8 over 16 codes at a time.
//
// vqtbl4q_u8 performs a 16-element lookup from a 64-byte table; output is 0
// for any index ≥ 64.  Four passes cover the full 256-entry table:
//   adj = code - k*64  (wraps for out-of-range → ≥ 64 → zeroed ✓)

namespace {

static void remap_u8_scalar(uint8_t* codes, size_t n, const uint8_t* tbl) {
    for (size_t i = 0; i < n; ++i)
        codes[i] = tbl[codes[i]];
}

#if defined(__AVX2__)
static void remap_u8_avx2(uint8_t* codes, size_t n, const uint8_t* tbl) {
    const __m256i add70   = _mm256_set1_epi8(0x70);
    const __m256i mask80  = _mm256_set1_epi8((int8_t)0x80);

    size_t i = 0;
    for (; i + 32 <= n; i += 32) {
        __m256i c      = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(codes + i));
        __m256i result = _mm256_setzero_si256();

        for (int k = 0; k < 16; ++k) {
            // Broadcast 16-byte sub-table to both 128-bit lanes.
            __m128i chunk = _mm_loadu_si128(reinterpret_cast<const __m128i*>(tbl + k * 16));
            __m256i vtbl  = _mm256_broadcastsi128_si256(chunk);

            __m256i base    = _mm256_set1_epi8((int8_t)(k * 16));
            __m256i adj     = _mm256_sub_epi8(c, base);
            __m256i carry   = _mm256_and_si256(_mm256_add_epi8(adj, add70), mask80);
            __m256i safe    = _mm256_or_si256(adj, carry);
            __m256i looked  = _mm256_shuffle_epi8(vtbl, safe);
            result          = _mm256_or_si256(result, looked);
        }

        _mm256_storeu_si256(reinterpret_cast<__m256i*>(codes + i), result);
    }
    remap_u8_scalar(codes + i, n - i, tbl);
}
#endif  // __AVX2__

#if defined(__ARM_NEON) || defined(__ARM_NEON__)
static void remap_u8_neon(uint8_t* codes, size_t n, const uint8_t* tbl) {
    size_t i = 0;
    for (; i + 16 <= n; i += 16) {
        uint8x16_t c      = vld1q_u8(codes + i);
        uint8x16_t result = vdupq_n_u8(0);

        for (int k = 0; k < 4; ++k) {
            uint8x16x4_t table;
            table.val[0] = vld1q_u8(tbl + k * 64 +  0);
            table.val[1] = vld1q_u8(tbl + k * 64 + 16);
            table.val[2] = vld1q_u8(tbl + k * 64 + 32);
            table.val[3] = vld1q_u8(tbl + k * 64 + 48);

            uint8x16_t base = vdupq_n_u8((uint8_t)(k * 64));
            uint8x16_t adj  = vsubq_u8(c, base);
            // vqtbl4q_u8: index ≥ 64 → output 0 ✓
            uint8x16_t hit  = vqtbl4q_u8(table, adj);
            result          = vorrq_u8(result, hit);
        }

        vst1q_u8(codes + i, result);
    }
    remap_u8_scalar(codes + i, n - i, tbl);
}
#endif  // __ARM_NEON

#if defined(__riscv) && defined(__riscv_vector)
// RVV: vloxei8 indexed gather — code values are byte offsets into the 256-entry
// table directly.  One pass, no multi-pass OR accumulation needed.
// LMUL=m4: 4× register width per iteration.
static void remap_u8_rvv(uint8_t* codes, size_t n, const uint8_t* tbl) {
    size_t i = 0;
    while (i < n) {
        size_t vl = __riscv_vsetvl_e8m4(n - i);
        vuint8m4_t c = __riscv_vle8_v_u8m4(codes + i, vl);
        // vloxei8: result[j] = tbl[ c[j] ]  (c values are byte offsets; tbl is uint8)
        vuint8m4_t r = __riscv_vloxei8_v_u8m4(tbl, c, vl);
        __riscv_vse8_v_u8m4(codes + i, r, vl);
        i += vl;
    }
}
#endif  // __riscv_vector

}  // namespace

void simd_remap_u8(uint8_t* codes, size_t n, const uint8_t* remap_table) {
    using fn_t = void(*)(uint8_t*, size_t, const uint8_t*);
    static std::atomic<fn_t> cache{nullptr};
    fn_t fn = simd::select_dispatch<fn_t>(cache, {
#if defined(__AVX2__)
        { &cpu_supports_avx2, remap_u8_avx2 },
#endif
#if defined(__ARM_NEON) || defined(__ARM_NEON__)
        { &cpu_supports_neon, remap_u8_neon },
#endif
#if defined(__riscv) && defined(__riscv_vector)
        { &cpu_supports_rvv, remap_u8_rvv },
#endif
    }, remap_u8_scalar);
    fn(codes, n, remap_table);
}


// ── uint16 ───────────────────────────────────────────────────────────────────
//
// AVX2: process 8 uint16 codes at a time using _mm256_i32gather_epi32.
//
// The gather instruction loads 32-bit values from base + index * scale.
// With scale=2 and a uint16_t* base, this loads two consecutive uint16
// values as a 32-bit word.  On little-endian x86, the low 16 bits hold
// remap[code], which is what we need.  Masking discards the high 16 bits.
//
// The remap_table must have at least one extra element beyond the last valid
// code to avoid reading past the end of the array.

namespace {

static void remap_u16_scalar(uint16_t* codes, size_t n, const uint16_t* tbl) {
    for (size_t i = 0; i < n; ++i)
        codes[i] = tbl[codes[i]];
}

#if defined(__AVX2__)
static void remap_u16_avx2(uint16_t* codes, size_t n, const uint16_t* tbl) {
    const int32_t* tbl32   = reinterpret_cast<const int32_t*>(tbl);
    const __m256i mask16   = _mm256_set1_epi32(0x0000FFFF);

    size_t i = 0;
    for (; i + 8 <= n; i += 8) {
        // Load 8 uint16 codes, zero-extend to 8 int32 for the gather indices.
        __m128i c8   = _mm_loadu_si128(reinterpret_cast<const __m128i*>(codes + i));
        __m256i idx  = _mm256_cvtepu16_epi32(c8);

        // Gather: addr = (char*)tbl32 + idx[j] * 2 = &tbl[idx[j]] as uint16.
        // Each load reads 32 bits; low 16 bits = tbl[idx[j]] on little-endian.
        __m256i vals = _mm256_i32gather_epi32(tbl32, idx, 2);
        vals         = _mm256_and_si256(vals, mask16);

        // Pack 8 x int32 back to 8 x uint16 using unsigned saturation.
        __m128i lo   = _mm256_castsi256_si128(vals);
        __m128i hi   = _mm256_extracti128_si256(vals, 1);
        __m128i out  = _mm_packus_epi32(lo, hi);
        _mm_storeu_si128(reinterpret_cast<__m128i*>(codes + i), out);
    }
    remap_u16_scalar(codes + i, n - i, tbl);
}
#endif  // __AVX2__

#if defined(__riscv) && defined(__riscv_vector)
// RVV: vloxei32 indexed gather for u16 values.
//
// Byte offset = code * 2.  For codes near 65535, byte offset approaches 131070
// which overflows u16; we widen to u32 before scaling.
//
// EMUL constraint for vloxei32_v_u16m2: data LMUL=m2, index EEW=32,
// so index LMUL = m2 * (32/16) = m4 → vuint32m4_t.
static void remap_u16_rvv(uint16_t* codes, size_t n, const uint16_t* tbl) {
    size_t i = 0;
    while (i < n) {
        size_t vl = __riscv_vsetvl_e16m2(n - i);
        vuint16m2_t c = __riscv_vle16_v_u16m2(codes + i, vl);
        // Zero-extend u16→u32, then shift left by 1 to get byte offsets
        vuint32m4_t boff = __riscv_vsll_vx_u32m4(
            __riscv_vzext_vf2_u32m4(c, vl), 1, vl);
        vuint16m2_t r = __riscv_vloxei32_v_u16m2(tbl, boff, vl);
        __riscv_vse16_v_u16m2(codes + i, r, vl);
        i += vl;
    }
}
#endif  // __riscv_vector

}  // namespace

void simd_remap_u16(uint16_t* codes, size_t n, const uint16_t* remap_table) {
    using fn_t = void(*)(uint16_t*, size_t, const uint16_t*);
    static std::atomic<fn_t> cache{nullptr};
    fn_t fn = simd::select_dispatch<fn_t>(cache, {
#if defined(__AVX2__)
        { &cpu_supports_avx2, remap_u16_avx2 },
#endif
        // NEON: no efficient 16-bit gather; scalar is fast enough (L2-resident table).
#if defined(__riscv) && defined(__riscv_vector)
        { &cpu_supports_rvv, remap_u16_rvv },
#endif
    }, remap_u16_scalar);
    fn(codes, n, remap_table);
}


// ── uint32 ───────────────────────────────────────────────────────────────────
//
// uint32 dictionary codes imply D up to ~4B unique values; the remap table
// would be 16 GB.  In practice uint32 codes are rare and D is still small,
// but there is no effective SIMD strategy for a huge random-access table.
// Scalar is correct and cache-behaviour depends entirely on D.

void simd_remap_u32(uint32_t* codes, size_t n, const uint32_t* remap_table) {
    for (size_t i = 0; i < n; ++i)
        codes[i] = remap_table[codes[i]];
}
