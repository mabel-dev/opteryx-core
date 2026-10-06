#pragma once
// draken/ops/int64_addsub_simd.h — SIMD checked INT64 add / sub over CONTIGUOUS data.
//
// The scalar loops in int64_checked.h (`any |= __builtin_add_overflow(...)`) are not
// auto-vectorised by clang (verified in the shipped x86_64 and arm64 wheels: one
// element per iteration, `seto` / `cset vs`). These kernels compute the same wrapped
// result and the same per-batch overflow flag with explicit NEON / AVX2.
//
// Overflow without a flags register — sign-bit identities, OR-accumulated, tested once
// per call (the loop body carries no branch):
//   add: r = x + y   overflow  <=>  ((x ^ r) & (y ^ r)) < 0
//   sub: r = x - y   overflow  <=>  ((x ^ y) & (x ^ r)) < 0
//
// CONTRACT (identical to Op::apply in int64_checked.h):
//   * dst[i] always receives the WRAPPED result, overflow or not.
//   * Returns true iff ANY element overflowed. The caller decides whether that is an
//     error (live row) or a bail-out (dead dict entry / NULL slot) — these kernels
//     know nothing about validity or selection. They read `n` contiguous int64s; the
//     caller must only reach them when the operand is contiguous (physical-value
//     loops, or an operand whose DRAKEN_SEL_IDENTITY hint is set).
//   * Operands are plain pointers or a broadcast scalar; dst must not alias them.
//
// ISA is chosen at compile time (one ISA per wheel, see simd/simd_dispatch.h); a
// target with neither NEON nor AVX2 runs the scalar loop — the same loop the checked
// primitives already use, not a second implementation of the overflow rule.

#include <stdint.h>
#include <stddef.h>

#if defined(__ARM_NEON) || defined(__ARM_NEON__)
#include <arm_neon.h>
#endif
#if defined(__AVX2__)
#include <immintrin.h>
#endif

namespace draken { namespace ops {

// Operand modes for the core: a pointer stream or a broadcast scalar.
enum class AddSubArg { Ptr, Bcast };

namespace addsub_detail {

template <bool Sub>
static inline bool scalar_ovf(int64_t x, int64_t y, int64_t& r) noexcept {
    if constexpr (Sub) return __builtin_sub_overflow(x, y, &r);
    else               return __builtin_add_overflow(x, y, &r);
}

#if defined(__ARM_NEON) || defined(__ARM_NEON__)

template <bool Sub>
static inline int64x2_t vop(int64x2_t x, int64x2_t y) {
    if constexpr (Sub) return vsubq_s64(x, y);
    else               return vaddq_s64(x, y);
}

// Sign bit of the result is set exactly where the lane overflowed.
template <bool Sub>
static inline int64x2_t vovf(int64x2_t x, int64x2_t y, int64x2_t r) {
    if constexpr (Sub) return vandq_s64(veorq_s64(x, y), veorq_s64(x, r));
    else               return vandq_s64(veorq_s64(x, r), veorq_s64(y, r));
}

template <bool Sub, AddSubArg A, AddSubArg B>
static inline bool core(const int64_t* a, int64_t sa, const int64_t* b, int64_t sb,
                        int64_t* dst, uint32_t n) {
    const int64x2_t ba = vdupq_n_s64(sa);
    const int64x2_t bb = vdupq_n_s64(sb);
    int64x2_t acc0 = vdupq_n_s64(0);
    int64x2_t acc1 = vdupq_n_s64(0);
    uint32_t i = 0;
    for (; i + 4u <= n; i += 4u) {
        int64x2_t x0, x1, y0, y1;
        if constexpr (A == AddSubArg::Ptr) { x0 = vld1q_s64(a + i); x1 = vld1q_s64(a + i + 2); }
        else                               { x0 = ba; x1 = ba; }
        if constexpr (B == AddSubArg::Ptr) { y0 = vld1q_s64(b + i); y1 = vld1q_s64(b + i + 2); }
        else                               { y0 = bb; y1 = bb; }
        const int64x2_t r0 = vop<Sub>(x0, y0);
        const int64x2_t r1 = vop<Sub>(x1, y1);
        vst1q_s64(dst + i,     r0);
        vst1q_s64(dst + i + 2, r1);
        acc0 = vorrq_s64(acc0, vovf<Sub>(x0, y0, r0));
        acc1 = vorrq_s64(acc1, vovf<Sub>(x1, y1, r1));
    }
    const int64x2_t acc = vorrq_s64(acc0, acc1);
    bool ovf = (vgetq_lane_s64(acc, 0) | vgetq_lane_s64(acc, 1)) < 0;
    for (; i < n; ++i) {
        const int64_t x = (A == AddSubArg::Ptr) ? a[i] : sa;
        const int64_t y = (B == AddSubArg::Ptr) ? b[i] : sb;
        ovf |= scalar_ovf<Sub>(x, y, dst[i]);
    }
    return ovf;
}

#elif defined(__AVX2__)

template <bool Sub>
static inline __m256i vop(__m256i x, __m256i y) {
    if constexpr (Sub) return _mm256_sub_epi64(x, y);
    else               return _mm256_add_epi64(x, y);
}

template <bool Sub>
static inline __m256i vovf(__m256i x, __m256i y, __m256i r) {
    if constexpr (Sub) return _mm256_and_si256(_mm256_xor_si256(x, y), _mm256_xor_si256(x, r));
    else               return _mm256_and_si256(_mm256_xor_si256(x, r), _mm256_xor_si256(y, r));
}

template <bool Sub, AddSubArg A, AddSubArg B>
static inline bool core(const int64_t* a, int64_t sa, const int64_t* b, int64_t sb,
                        int64_t* dst, uint32_t n) {
    const __m256i ba = _mm256_set1_epi64x(sa);
    const __m256i bb = _mm256_set1_epi64x(sb);
    __m256i acc0 = _mm256_setzero_si256();
    __m256i acc1 = _mm256_setzero_si256();
    uint32_t i = 0;
    for (; i + 8u <= n; i += 8u) {
        __m256i x0, x1, y0, y1;
        if constexpr (A == AddSubArg::Ptr) {
            x0 = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(a + i));
            x1 = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(a + i + 4));
        } else { x0 = ba; x1 = ba; }
        if constexpr (B == AddSubArg::Ptr) {
            y0 = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(b + i));
            y1 = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(b + i + 4));
        } else { y0 = bb; y1 = bb; }
        const __m256i r0 = vop<Sub>(x0, y0);
        const __m256i r1 = vop<Sub>(x1, y1);
        _mm256_storeu_si256(reinterpret_cast<__m256i*>(dst + i),     r0);
        _mm256_storeu_si256(reinterpret_cast<__m256i*>(dst + i + 4), r1);
        acc0 = _mm256_or_si256(acc0, vovf<Sub>(x0, y0, r0));
        acc1 = _mm256_or_si256(acc1, vovf<Sub>(x1, y1, r1));
    }
    const __m256i acc = _mm256_or_si256(acc0, acc1);
    bool ovf = _mm256_movemask_pd(_mm256_castsi256_pd(acc)) != 0;   // sign bit per lane
    for (; i < n; ++i) {
        const int64_t x = (A == AddSubArg::Ptr) ? a[i] : sa;
        const int64_t y = (B == AddSubArg::Ptr) ? b[i] : sb;
        ovf |= scalar_ovf<Sub>(x, y, dst[i]);
    }
    return ovf;
}

#else

template <bool Sub, AddSubArg A, AddSubArg B>
static inline bool core(const int64_t* a, int64_t sa, const int64_t* b, int64_t sb,
                        int64_t* dst, uint32_t n) {
    bool ovf = false;
    for (uint32_t i = 0; i < n; ++i) {
        const int64_t x = (A == AddSubArg::Ptr) ? a[i] : sa;
        const int64_t y = (B == AddSubArg::Ptr) ? b[i] : sb;
        ovf |= scalar_ovf<Sub>(x, y, dst[i]);
    }
    return ovf;
}

#endif

} // namespace addsub_detail

// dst[i] = a[i] OP b[i]
template <bool Sub>
static inline bool i64_addsub_vv(const int64_t* a, const int64_t* b, int64_t* dst, uint32_t n) {
    return addsub_detail::core<Sub, AddSubArg::Ptr, AddSubArg::Ptr>(a, 0, b, 0, dst, n);
}
// dst[i] = a[i] OP s
template <bool Sub>
static inline bool i64_addsub_vs(const int64_t* a, int64_t s, int64_t* dst, uint32_t n) {
    return addsub_detail::core<Sub, AddSubArg::Ptr, AddSubArg::Bcast>(a, 0, nullptr, s, dst, n);
}
// dst[i] = s OP b[i]
template <bool Sub>
static inline bool i64_addsub_sv(int64_t s, const int64_t* b, int64_t* dst, uint32_t n) {
    return addsub_detail::core<Sub, AddSubArg::Bcast, AddSubArg::Ptr>(nullptr, s, b, 0, dst, n);
}

}} // namespace draken::ops
