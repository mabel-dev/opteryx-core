#pragma once
// draken/ops/vector_cosine_row.h — cosine similarity of ONE pair of fp16 rows.
//
// The single definition of the engine's cosine arithmetic. Every caller (the column
// kernel in vector_cosine.h, the text overloads in function_vector_distance.cpp, and
// the ANN metric) computes through this function, so the same pair of rows has the
// same score everywhere.
//
// Arithmetic contract — identical bits on every ISA:
//   * fp16 -> fp64 widening is exact, and the product of two fp16 values is exact in
//     fp64 (11-bit significands), so fused and unfused multiply-add agree.
//   * Element k accumulates into lane (k mod 8), in increasing k. The NEON (4 x f64x2),
//     AVX2 (2 x f64x4) and scalar (8 doubles) paths keep exactly these 8 lanes.
//   * A tail shorter than 8 is zero-padded; adding an exact 0.0 changes no lane.
//   * The 8 lanes are reduced in scalar, in one fixed tree.
// Zero norm -> NaN (0/0 is an undefined direction, not "dissimilar").

#include <cmath>
#include <cstdint>
#include <cstring>
#include <limits>

#include "fp16/fp16.h"
#include "simd/simd_dispatch.h"

#if defined(__AVX2__)
#include <immintrin.h>
#elif defined(__ARM_NEON) || defined(__ARM_NEON__)
#include <arm_neon.h>
#endif

namespace draken { namespace ops {

struct CosineLanes { double dot[8]; double aa[8]; double bb[8]; };

static inline double cosine_lanes_finish(const CosineLanes& l) noexcept {
    const double dot = ((l.dot[0] + l.dot[1]) + (l.dot[2] + l.dot[3])) +
                       ((l.dot[4] + l.dot[5]) + (l.dot[6] + l.dot[7]));
    const double aa  = ((l.aa[0] + l.aa[1]) + (l.aa[2] + l.aa[3])) +
                       ((l.aa[4] + l.aa[5]) + (l.aa[6] + l.aa[7]));
    const double bb  = ((l.bb[0] + l.bb[1]) + (l.bb[2] + l.bb[3])) +
                       ((l.bb[4] + l.bb[5]) + (l.bb[6] + l.bb[7]));
    const double denom = std::sqrt(aa) * std::sqrt(bb);
    return (denom == 0.0) ? std::numeric_limits<double>::quiet_NaN() : dot / denom;
}

static inline void cosine_lanes_scalar(const uint16_t* a, const uint16_t* b, uint32_t dims,
                                       CosineLanes& l) noexcept {
    for (int j = 0; j < 8; ++j) { l.dot[j] = 0.0; l.aa[j] = 0.0; l.bb[j] = 0.0; }
    for (uint32_t k = 0; k < dims; ++k) {
        const uint32_t j = k & 7u;
        const double fa = static_cast<double>(fp16_ieee_to_fp32_value(a[k]));
        const double fb = static_cast<double>(fp16_ieee_to_fp32_value(b[k]));
        // Separate statements: each product is exact, so contraction is harmless, but
        // keeping one rounding point per add matches the SIMD paths by construction.
        const double pab = fa * fb;
        const double paa = fa * fa;
        const double pbb = fb * fb;
        l.dot[j] += pab;
        l.aa[j]  += paa;
        l.bb[j]  += pbb;
    }
}

#if defined(__AVX2__)
static inline void cosine_lanes_avx2(const uint16_t* a, const uint16_t* b, uint32_t dims,
                                     CosineLanes& l) noexcept {
    __m256d d0 = _mm256_setzero_pd(), d1 = _mm256_setzero_pd();
    __m256d a0 = _mm256_setzero_pd(), a1 = _mm256_setzero_pd();
    __m256d b0 = _mm256_setzero_pd(), b1 = _mm256_setzero_pd();
    const uint32_t full = dims & ~7u;
    uint16_t ta[8], tb[8];
    for (uint32_t k = 0; k < dims; k += 8) {
        const uint16_t* pa = a + k;
        const uint16_t* pb = b + k;
        if (k >= full) {
            std::memset(ta, 0, sizeof(ta)); std::memset(tb, 0, sizeof(tb));
            std::memcpy(ta, pa, (dims - k) * sizeof(uint16_t));
            std::memcpy(tb, pb, (dims - k) * sizeof(uint16_t));
            pa = ta; pb = tb;
        }
        const __m256 fa = _mm256_cvtph_ps(_mm_loadu_si128(reinterpret_cast<const __m128i*>(pa)));
        const __m256 fb = _mm256_cvtph_ps(_mm_loadu_si128(reinterpret_cast<const __m128i*>(pb)));
        const __m256d alo = _mm256_cvtps_pd(_mm256_castps256_ps128(fa));
        const __m256d ahi = _mm256_cvtps_pd(_mm256_extractf128_ps(fa, 1));
        const __m256d blo = _mm256_cvtps_pd(_mm256_castps256_ps128(fb));
        const __m256d bhi = _mm256_cvtps_pd(_mm256_extractf128_ps(fb, 1));
        d0 = _mm256_fmadd_pd(alo, blo, d0); d1 = _mm256_fmadd_pd(ahi, bhi, d1);
        a0 = _mm256_fmadd_pd(alo, alo, a0); a1 = _mm256_fmadd_pd(ahi, ahi, a1);
        b0 = _mm256_fmadd_pd(blo, blo, b0); b1 = _mm256_fmadd_pd(bhi, bhi, b1);
    }
    _mm256_storeu_pd(l.dot, d0); _mm256_storeu_pd(l.dot + 4, d1);
    _mm256_storeu_pd(l.aa,  a0); _mm256_storeu_pd(l.aa + 4,  a1);
    _mm256_storeu_pd(l.bb,  b0); _mm256_storeu_pd(l.bb + 4,  b1);
}
#endif

#if defined(__ARM_NEON) || defined(__ARM_NEON__)
static inline void cosine_lanes_neon(const uint16_t* a, const uint16_t* b, uint32_t dims,
                                     CosineLanes& l) noexcept {
    float64x2_t d[4], sa[4], sb[4];
    for (int j = 0; j < 4; ++j) { d[j] = vdupq_n_f64(0.0); sa[j] = d[j]; sb[j] = d[j]; }
    const uint32_t full = dims & ~7u;
    uint16_t ta[8], tb[8];
    for (uint32_t k = 0; k < dims; k += 8) {
        const uint16_t* pa = a + k;
        const uint16_t* pb = b + k;
        if (k >= full) {
            std::memset(ta, 0, sizeof(ta)); std::memset(tb, 0, sizeof(tb));
            std::memcpy(ta, pa, (dims - k) * sizeof(uint16_t));
            std::memcpy(tb, pb, (dims - k) * sizeof(uint16_t));
            pa = ta; pb = tb;
        }
        const float16x8_t ha = vreinterpretq_f16_u16(vld1q_u16(pa));
        const float16x8_t hb = vreinterpretq_f16_u16(vld1q_u16(pb));
        const float32x4_t alo = vcvt_f32_f16(vget_low_f16(ha));
        const float32x4_t ahi = vcvt_high_f32_f16(ha);
        const float32x4_t blo = vcvt_f32_f16(vget_low_f16(hb));
        const float32x4_t bhi = vcvt_high_f32_f16(hb);
        // Lanes {0,1} {2,3} {4,5} {6,7}.
        const float64x2_t av[4] = { vcvt_f64_f32(vget_low_f32(alo)), vcvt_high_f64_f32(alo),
                                    vcvt_f64_f32(vget_low_f32(ahi)), vcvt_high_f64_f32(ahi) };
        const float64x2_t bv[4] = { vcvt_f64_f32(vget_low_f32(blo)), vcvt_high_f64_f32(blo),
                                    vcvt_f64_f32(vget_low_f32(bhi)), vcvt_high_f64_f32(bhi) };
        for (int j = 0; j < 4; ++j) {
            d[j]  = vfmaq_f64(d[j],  av[j], bv[j]);
            sa[j] = vfmaq_f64(sa[j], av[j], av[j]);
            sb[j] = vfmaq_f64(sb[j], bv[j], bv[j]);
        }
    }
    for (int j = 0; j < 4; ++j) {
        vst1q_f64(l.dot + 2 * j, d[j]);
        vst1q_f64(l.aa  + 2 * j, sa[j]);
        vst1q_f64(l.bb  + 2 * j, sb[j]);
    }
}
#endif

static inline double cosine_row_fp16_scalar(const uint16_t* a, const uint16_t* b,
                                            uint32_t dims) noexcept {
    CosineLanes l;
    cosine_lanes_scalar(a, b, dims, l);
    return cosine_lanes_finish(l);
}

static inline double cosine_row_fp16(const uint16_t* a, const uint16_t* b,
                                     uint32_t dims) noexcept {
    CosineLanes l;
    SIMD_STATIC_SELECT(cosine_lanes_avx2, cosine_lanes_neon,
                       cosine_lanes_scalar, cosine_lanes_scalar)(a, b, dims, l);
    return cosine_lanes_finish(l);
}

}} // namespace draken::ops
