#include "simd_hash.h"

#include <cstddef>
#include <cstdint>
#include <atomic>
#include <cstring>

#include "simd_dispatch.h"
#include "cpu_features.h"

#if defined(__AVX2__)
#include <immintrin.h>
#elif defined(__ARM_NEON) || defined(__ARM_NEON__)
#include <arm_neon.h>
#endif

#if defined(_MSC_VER)
#include <intrin.h>
#endif

namespace {

inline void scalar_mix(uint64_t* dest, const uint64_t* values, std::size_t count) {
    for (std::size_t i = 0; i < count; ++i) {
        uint64_t mixed = dest[i] ^ values[i];
        mixed = mixed * MIX_HASH_CONSTANT + 1;
        mixed ^= mixed >> 32;
        dest[i] = mixed;
    }
}

// Provide architecture-specific mullo_u64 overloads.

#if defined(__AVX2__)
inline __m256i mullo_u64(__m256i a, __m256i b) {
    // AVX2 lacks a direct 64-bit integer multiply, so combine 32-bit partials per lane.
    const __m256i mask = _mm256_set1_epi64x(0xFFFFFFFFULL);
    __m256i a_lo = _mm256_and_si256(a, mask);
    __m256i b_lo = _mm256_and_si256(b, mask);
    __m256i a_hi = _mm256_srli_epi64(a, 32);
    __m256i b_hi = _mm256_srli_epi64(b, 32);

    __m256i prod_ll = _mm256_mul_epu32(a_lo, b_lo);
    __m256i prod_lh = _mm256_mul_epu32(a_lo, b_hi);
    __m256i prod_hl = _mm256_mul_epu32(a_hi, b_lo);

    __m256i cross = _mm256_add_epi64(prod_lh, prod_hl);
    cross = _mm256_slli_epi64(cross, 32);

    return _mm256_add_epi64(prod_ll, cross);
}
#endif

#if defined(__ARM_NEON) || defined(__ARM_NEON__)
inline uint64x2_t mullo_u64(uint64x2_t a, uint64x2_t b) {
    uint32x2_t a_lo = vmovn_u64(a);
    uint32x2_t b_lo = vmovn_u64(b);
    uint32x2_t a_hi = vshrn_n_u64(a, 32);
    uint32x2_t b_hi = vshrn_n_u64(b, 32);
    uint64x2_t lo_lo = vmull_u32(a_lo, b_lo);
    uint64x2_t lo_hi = vmull_u32(a_lo, b_hi);
    uint64x2_t hi_lo = vmull_u32(a_hi, b_lo);
    uint64x2_t cross = vaddq_u64(lo_hi, hi_lo);
    return vaddq_u64(lo_lo, vshlq_n_u64(cross, 32));
}
#endif

}  // namespace

static void simd_mix_hash_scalar(uint64_t* dest, const uint64_t* values, std::size_t count) {
    if (dest == nullptr || values == nullptr || count == 0) {
        return;
    }

    scalar_mix(dest, values, count);
}

#if defined(__AVX2__)
static void simd_mix_hash_avx2(uint64_t* dest, const uint64_t* values, std::size_t count) {
    if (dest == nullptr || values == nullptr || count == 0) {
        return;
    }

    const std::size_t stride = 4;
    const __m256i const_vec = _mm256_set1_epi64x(static_cast<long long>(MIX_HASH_CONSTANT));
    std::size_t i = 0;
    for (; i + stride <= count; i += stride) {
        __m256i dst_vec = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(dest + i));
        __m256i val_vec = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(values + i));
        __m256i mixed = _mm256_xor_si256(dst_vec, val_vec);
        __m256i product = mullo_u64(mixed, const_vec);
        product = _mm256_add_epi64(product, _mm256_set1_epi64x(1));
        __m256i shifted = _mm256_srli_epi64(product, 32);
        __m256i combined = _mm256_xor_si256(product, shifted);
        _mm256_storeu_si256(reinterpret_cast<__m256i*>(dest + i), combined);
    }
    if (i < count) {
        scalar_mix(dest + i, values + i, count - i);
    }
}
#endif

#if defined(__ARM_NEON) || defined(__ARM_NEON__)
static void simd_mix_hash_neon(uint64_t* dest, const uint64_t* values, std::size_t count) {
    if (dest == nullptr || values == nullptr || count == 0) {
        return;
    }

    const uint64x2_t const_vec = vdupq_n_u64(MIX_HASH_CONSTANT);
    const uint64x2_t one_vec = vdupq_n_u64(1);
    std::size_t i = 0;
    // Process 4 elements (2 NEON pairs) per iteration to hide latency.
    for (; i + 4 <= count; i += 4) {
        uint64x2_t d0 = vld1q_u64(dest + i);
        uint64x2_t d1 = vld1q_u64(dest + i + 2);
        uint64x2_t v0 = vld1q_u64(values + i);
        uint64x2_t v1 = vld1q_u64(values + i + 2);
        uint64x2_t m0 = veorq_u64(d0, v0);
        uint64x2_t m1 = veorq_u64(d1, v1);
        m0 = vaddq_u64(mullo_u64(m0, const_vec), one_vec);
        m1 = vaddq_u64(mullo_u64(m1, const_vec), one_vec);
        m0 = veorq_u64(m0, vshrq_n_u64(m0, 32));
        m1 = veorq_u64(m1, vshrq_n_u64(m1, 32));
        vst1q_u64(dest + i, m0);
        vst1q_u64(dest + i + 2, m1);
    }
    // Handle remaining pair.
    for (; i + 2 <= count; i += 2) {
        uint64x2_t dst_vec = vld1q_u64(dest + i);
        uint64x2_t val_vec = vld1q_u64(values + i);
        uint64x2_t mixed = veorq_u64(dst_vec, val_vec);
        uint64x2_t product = vaddq_u64(mullo_u64(mixed, const_vec), one_vec);
        vst1q_u64(dest + i, veorq_u64(product, vshrq_n_u64(product, 32)));
    }
    if (i < count) {
        scalar_mix(dest + i, values + i, count - i);
    }
}
#endif

void simd_mix_hash(uint64_t* dest, const uint64_t* values, std::size_t count) {
    using fn_t = void(*)(uint64_t*, const uint64_t*, std::size_t);
    static std::atomic<fn_t> cache{nullptr};

#if defined(__AVX2__)
    // noop - AVX2 candidate included below
#endif
    fn_t fn = simd::select_dispatch<fn_t>(cache, {
#if defined(__AVX2__)
    { &cpu_supports_avx2, simd_mix_hash_avx2 },
#endif
#if defined(__ARM_NEON) || defined(__ARM_NEON__)
        { &cpu_supports_neon, simd_mix_hash_neon },
#endif
    }, simd_mix_hash_scalar);

    return fn(dest, values, count);
}

// ---------------------------------------------------------------------------
// simd_scale_date32: multiply int32 day values by 86400000000 -> int64 µs
// ---------------------------------------------------------------------------

static const int64_t DATE32_SCALE = 86400000000LL;

static void simd_scale_date32_scalar(const int32_t* src, int64_t* dest, std::size_t count) {
    for (std::size_t i = 0; i < count; ++i) {
        dest[i] = static_cast<int64_t>(src[i]) * DATE32_SCALE;
    }
}

#if defined(__AVX2__)
static void simd_scale_date32_avx2(const int32_t* src, int64_t* dest, std::size_t count) {
    // _mm256_cvtepi32_epi64 converts 4×int32 (__m128i) to 4×int64 (__m256i)
    const __m256i scale_vec = _mm256_set1_epi64x(DATE32_SCALE);
    std::size_t i = 0;
    for (; i + 4 <= count; i += 4) {
        __m128i src_vec = _mm_loadu_si128(reinterpret_cast<const __m128i*>(src + i));
        __m256i widened = _mm256_cvtepi32_epi64(src_vec);
        __m256i result  = mullo_u64(widened, scale_vec);
        _mm256_storeu_si256(reinterpret_cast<__m256i*>(dest + i), result);
    }
    if (i < count) {
        simd_scale_date32_scalar(src + i, dest + i, count - i);
    }
}
#endif

#if defined(__ARM_NEON) || defined(__ARM_NEON__)
static void simd_scale_date32_neon(const int32_t* src, int64_t* dest, std::size_t count) {
    const uint64x2_t scale_vec = vdupq_n_u64(static_cast<uint64_t>(DATE32_SCALE));
    std::size_t i = 0;
    // Process 4 elements (2 NEON pairs) per iteration.
    for (; i + 4 <= count; i += 4) {
        int32x2_t s0 = vld1_s32(src + i);
        int32x2_t s1 = vld1_s32(src + i + 2);
        int64x2_t w0 = vmovl_s32(s0);
        int64x2_t w1 = vmovl_s32(s1);
        uint64x2_t r0 = mullo_u64(vreinterpretq_u64_s64(w0), scale_vec);
        uint64x2_t r1 = mullo_u64(vreinterpretq_u64_s64(w1), scale_vec);
        vst1q_s64(dest + i, vreinterpretq_s64_u64(r0));
        vst1q_s64(dest + i + 2, vreinterpretq_s64_u64(r1));
    }
    // Handle remaining pair.
    for (; i + 2 <= count; i += 2) {
        int32x2_t src_vec = vld1_s32(src + i);
        int64x2_t widened = vmovl_s32(src_vec);
        uint64x2_t result = mullo_u64(vreinterpretq_u64_s64(widened), scale_vec);
        vst1q_s64(dest + i, vreinterpretq_s64_u64(result));
    }
    if (i < count) {
        simd_scale_date32_scalar(src + i, dest + i, count - i);
    }
}
#endif

void simd_scale_date32(const int32_t* src, int64_t* dest, std::size_t count) {
    if (src == nullptr || dest == nullptr || count == 0) {
        return;
    }
    using fn_t = void(*)(const int32_t*, int64_t*, std::size_t);
    static std::atomic<fn_t> cache{nullptr};

    fn_t fn = simd::select_dispatch<fn_t>(cache, {
#if defined(__AVX2__)
        { &cpu_supports_avx2, simd_scale_date32_avx2 },
#endif
#if defined(__ARM_NEON) || defined(__ARM_NEON__)
        { &cpu_supports_neon, simd_scale_date32_neon },
#endif
    }, simd_scale_date32_scalar);

    return fn(src, dest, count);
}