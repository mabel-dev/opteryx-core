#ifndef DRAKEN_FP16_H
#define DRAKEN_FP16_H

#include <stdint.h>
#include <string.h>

#if defined(__ARM_NEON) && defined(__ARM_FP16_FORMAT_IEEE)
#include <arm_neon.h>
#define DRAKEN_FP16_HAS_NEON 1
#endif

#if defined(__F16C__)
#include <immintrin.h>
#define DRAKEN_FP16_HAS_F16C 1
#endif

/* Scalar IEEE binary16 -> binary32 convert. Handles signed zero, subnormals,
 * infinities, and NaNs. Compilers reliably auto-vectorize tight loops over
 * this on AArch64 and x86 with F16C. */
static inline float draken_fp16_to_fp32(uint16_t h) {
#if DRAKEN_FP16_HAS_NEON
    return (float) vget_lane_f16(vreinterpret_f16_u16(vdup_n_u16(h)), 0);
#elif DRAKEN_FP16_HAS_F16C
    __m128i v = _mm_cvtsi32_si128((int) h);
    return _mm_cvtss_f32(_mm_cvtph_ps(v));
#else
    uint32_t sign = ((uint32_t) (h & 0x8000u)) << 16;
    uint32_t exp  = (h >> 10) & 0x1Fu;
    uint32_t mant = h & 0x3FFu;
    uint32_t out;
    if (exp == 0u) {
        if (mant == 0u) {
            out = sign;
        } else {
            int e = -1;
            do { e++; mant <<= 1; } while ((mant & 0x400u) == 0u);
            mant &= 0x3FFu;
            out = sign | ((uint32_t) (127 - 15 - e) << 23) | (mant << 13);
        }
    } else if (exp == 31u) {
        out = sign | 0x7F800000u | (mant << 13);
    } else {
        out = sign | ((exp + (127u - 15u)) << 23) | (mant << 13);
    }
    float f;
    memcpy(&f, &out, sizeof(f));
    return f;
#endif
}

/* Scalar IEEE binary32 -> binary16 convert with round-to-nearest-even.
 * Auto-vectorizes on AArch64 (FCVTN) and x86 with F16C (VCVTPS2PH). */
static inline uint16_t draken_fp32_to_fp16(float f) {
#if DRAKEN_FP16_HAS_NEON
    return vget_lane_u16(vreinterpret_u16_f16(vcvt_f16_f32(vdupq_n_f32(f))), 0);
#elif DRAKEN_FP16_HAS_F16C
    __m128 v = _mm_set_ss(f);
    __m128i h = _mm_cvtps_ph(v, 0 /* round to nearest, suppress exceptions */);
    return (uint16_t) _mm_cvtsi128_si32(h);
#else
    uint32_t x;
    memcpy(&x, &f, sizeof(x));
    uint32_t sign = (x >> 16) & 0x8000u;
    int32_t  e    = (int32_t)((x >> 23) & 0xFFu) - 127 + 15;
    uint32_t m    = x & 0x7FFFFFu;
    if (e <= 0) {
        if (e < -10) return (uint16_t) sign;
        m |= 0x800000u;
        uint32_t shift = (uint32_t)(14 - e);
        uint32_t r = (m >> shift) + ((m >> (shift - 1)) & 1u);
        return (uint16_t)(sign | r);
    }
    if (e >= 31) {
        if ((x & 0x7FFFFFFFu) > 0x7F800000u) return (uint16_t)(sign | 0x7E00u);
        return (uint16_t)(sign | 0x7C00u);
    }
    uint32_t r = ((uint32_t) e << 10) | (m >> 13);
    r += (m >> 12) & 1u;
    return (uint16_t)(sign | r);
#endif
}

#endif /* DRAKEN_FP16_H */
