#include "simd_hash.h"

#include <cstddef>
#include <cstdint>
#include <atomic>
#include <cstring>
#include <algorithm>  // std::min (RVV path; pulled in transitively elsewhere on x86/ARM)

#include "simd_dispatch.h"
#include "cpu_features.h"

#if defined(__AVX2__)
#include <immintrin.h>
#elif defined(__ARM_NEON) || defined(__ARM_NEON__)
#include <arm_neon.h>
#elif defined(__riscv) && defined(__riscv_vector)
#include <riscv_vector.h>
#endif

#if defined(_MSC_VER)
#include <intrin.h>
#endif

namespace {

// The mix is three arithmetic ops per value (xor, multiply-add, xor-with-shift),
// which AArch64 issues as `eor` / `madd` / `eor ..., lsr #32` — already optimal
// per value. The cost is not instruction count but the ~3-cycle latency of the
// 64-bit multiply, which a single dependent chain cannot hide. Unrolling by 8
// gives the scheduler eight independent chains to interleave and lets the
// loads/stores pair into ldp/stp; measured 1.6x over the un-unrolled loop.
// Do not "simplify" this back into a single-accumulator loop.
inline void scalar_mix(uint64_t* dest, const uint64_t* values, std::size_t count) {
    std::size_t i = 0;
    for (; i + 8 <= count; i += 8) {
        uint64_t m[8];
        for (int k = 0; k < 8; ++k) m[k] = dest[i + k] ^ values[i + k];
        for (int k = 0; k < 8; ++k) m[k] = m[k] * MIX_HASH_CONSTANT + 1;
        for (int k = 0; k < 8; ++k) dest[i + k] = m[k] ^ (m[k] >> 32);
    }
    for (; i < count; ++i) {
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

// ---------------------------------------------------------------------------
// RVV implementations
//
// RVV provides a native 64×64→64 integer multiply (vmul), so no mullo_u64
// emulation is required.  All loops use vsetvl_e64m1 and advance by vl,
// which automatically adapts to any hardware VLEN.
// ---------------------------------------------------------------------------

#if defined(__riscv) && defined(__riscv_vector)

static void simd_mix_hash_rvv(uint64_t* dest, const uint64_t* values, std::size_t count) {
    std::size_t i = 0;
    while (i < count) {
        std::size_t vl   = __riscv_vsetvl_e64m1(count - i);
        vuint64m1_t d    = __riscv_vle64_v_u64m1(dest   + i, vl);
        vuint64m1_t v    = __riscv_vle64_v_u64m1(values + i, vl);
        vuint64m1_t mix  = __riscv_vxor_vv_u64m1(d, v, vl);
        vuint64m1_t prod = __riscv_vadd_vx_u64m1(__riscv_vmul_vx_u64m1(mix, MIX_HASH_CONSTANT, vl), 1, vl);
        vuint64m1_t res  = __riscv_vxor_vv_u64m1(prod, __riscv_vsrl_vx_u64m1(prod, 32, vl), vl);
        __riscv_vse64_v_u64m1(dest + i, res, vl);
        i += vl;
    }
}

static void simd_hash_i64_rvv(const uint64_t* src, uint64_t* dst, std::size_t count) {
    std::size_t i = 0;
    while (i < count) {
        std::size_t vl   = __riscv_vsetvl_e64m1(count - i);
        vuint64m1_t v    = __riscv_vle64_v_u64m1(src + i, vl);
        vuint64m1_t prod = __riscv_vadd_vx_u64m1(__riscv_vmul_vx_u64m1(v, MIX_HASH_CONSTANT, vl), 1, vl);
        vuint64m1_t res  = __riscv_vxor_vv_u64m1(prod, __riscv_vsrl_vx_u64m1(prod, 32, vl), vl);
        __riscv_vse64_v_u64m1(dst + i, res, vl);
        i += vl;
    }
}
#endif  // __riscv && __riscv_vector

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

// NOTE: there is deliberately no NEON mixer.
// AArch64 NEON has no 64x64->64 integer multiply, so a vector mixer must emulate
// it with three vmull_u32 partial products plus shifts and adds. Scalar AArch64
// does the same work in one `madd`. Measured on M-series (byte-identical output,
// interleaved A/B, min of 15, both L2-resident and 64 MiB working sets):
//     NEON (3x vmull emulation)      0.315 ns/value
//     unrolled scalar (scalar_mix)   0.203 ns/value   <- 1.55x faster
// The ARM dispatch slot below therefore selects scalar_mix on purpose. This does
// NOT generalise to x86: AVX2's emulation amortises over 4 lanes, so the AVX2
// mixer is kept. Re-measure before adding a NEON mixer back.

void simd_mix_hash(uint64_t* dest, const uint64_t* values, std::size_t count) {
    using fn_t = void(*)(uint64_t*, const uint64_t*, std::size_t);
    static std::atomic<fn_t> cache{nullptr};

#if defined(__AVX2__)
    // noop - AVX2 candidate included below
#endif
    // ARM slot is scalar_mix by measurement, not by omission - see the note above.
    fn_t fn = SIMD_STATIC_SELECT(simd_mix_hash_avx2, simd_mix_hash_scalar, simd_mix_hash_rvv, simd_mix_hash_scalar);

    return fn(dest, values, count);
}

// ---------------------------------------------------------------------------
// simd_hash_i64: single-column hash, no prior dest state.
//
// Equivalent to memset(dst,0,n*8) + simd_mix_hash(dst,src,n) in one pass.
// Used by COUNT(DISTINCT) where there is no composite key to accumulate into.
// The hash is identical to simd_mix_hash applied to a zeroed destination:
//   dst[i] = (src[i] * CONST + 1) ^ ((src[i] * CONST + 1) >> 32)
// ---------------------------------------------------------------------------

// Unrolled by 8 for the same reason as scalar_mix: the cost here is the
// ~3-cycle latency of the 64-bit multiply, not the instruction count, and only
// independent chains can hide it. Measured 1.9x over the un-unrolled loop.
// Do not re-roll.
static void simd_hash_i64_scalar(const uint64_t* src, uint64_t* dst, std::size_t count) {
    std::size_t i = 0;
    for (; i + 8 <= count; i += 8) {
        uint64_t v[8];
        for (int k = 0; k < 8; ++k) v[k] = src[i + k] * MIX_HASH_CONSTANT + 1;
        for (int k = 0; k < 8; ++k) dst[i + k] = v[k] ^ (v[k] >> 32);
    }
    for (; i < count; ++i) {
        uint64_t v = src[i] * MIX_HASH_CONSTANT + 1;
        dst[i] = v ^ (v >> 32);
    }
}

#if defined(__AVX2__)
static void simd_hash_i64_avx2(const uint64_t* src, uint64_t* dst, std::size_t count) {
    const __m256i kc  = _mm256_set1_epi64x(static_cast<long long>(MIX_HASH_CONSTANT));
    const __m256i one = _mm256_set1_epi64x(1);
    std::size_t i = 0;
    for (; i + 4 <= count; i += 4) {
        __m256i v = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(src + i));
        v = _mm256_add_epi64(mullo_u64(v, kc), one);
        v = _mm256_xor_si256(v, _mm256_srli_epi64(v, 32));
        _mm256_storeu_si256(reinterpret_cast<__m256i*>(dst + i), v);
    }
    if (i < count) simd_hash_i64_scalar(src + i, dst + i, count - i);
}
#endif

// No NEON variant: this is the mixer minus the xor-with-dest, so the same
// measurement applies - NEON must emulate the 64-bit multiply with three
// vmull_u32, scalar does it in one madd. Measured (byte-identical, interleaved,
// min of 15, at 512 KiB and 32 MiB): NEON 0.297 ns/value vs unrolled scalar
// 0.155 ns/value = 1.91x. The ARM slot selects the scalar kernel on purpose.

void simd_hash_i64(const uint64_t* src, uint64_t* dst, std::size_t count) {
    if (!src || !dst || !count) return;
    using fn_t = void(*)(const uint64_t*, uint64_t*, std::size_t);
    static std::atomic<fn_t> cache{nullptr};
    // ARM slot is the scalar kernel by measurement - see the note above.
    fn_t fn = SIMD_STATIC_SELECT(simd_hash_i64_avx2, simd_hash_i64_scalar, simd_hash_i64_rvv, simd_hash_i64_scalar);
    fn(src, dst, count);
}
