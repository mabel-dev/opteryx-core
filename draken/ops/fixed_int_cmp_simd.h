#pragma once
// draken/ops/fixed_int_cmp_simd.h — NATIVE-WIDTH SIMD compare for int8/16/32 and
// uint8/16/32 over CONTIGUOUS data, producing the packed 1-bit-per-row result.
//
// The scalar kernels in fixed_int_ops.h widen every value to int64 before comparing,
// which makes every width cost 2 (NEON) / 4 (AVX2) lanes per instruction. These
// kernels compare at the element's own width: 16 / 32 lanes for 8-bit, 8 / 16 for
// 16-bit, 4 / 8 for 32-bit (NEON / AVX2), then pack the lane masks into bits.
//
// CONTRACT
//   * Reads n contiguous elements — the caller reaches these only when the operand's
//     DRAKEN_SEL_IDENTITY hint is set (CLAUDE.md §11, ratified identity fast path).
//   * Writes WHOLE output bytes for rows [0, done) and returns `done`, a multiple of 8
//     (0 = nothing processed: no SIMD target, or the scalar does not fit T). The caller
//     finishes rows [done, n) with the uniform kernel. Same answer either way.
//   * Output bit i of byte b is row 8b+i. Validity is NOT applied here; the caller ANDs.
//   * Scalar: only used when it is representable in T (else the answer for every row is
//     a constant the widened kernel already produces) — see fi_cmp_scalar_fits.
//   * Unsigned: NEON has native unsigned compares; AVX2 has none, so both operands are
//     biased by the sign bit and compared signed (order-preserving).
//   * ISA is compile-time (one ISA per wheel, simd/simd_dispatch.h); others return 0.

#include <stdint.h>
#include <stddef.h>
#include <string.h>
#include <limits>
#include <type_traits>
#include "ops/int64_compare.h"   // CmpEq/CmpNe/CmpGt/CmpGe/CmpLt/CmpLe

#if defined(__ARM_NEON) || defined(__ARM_NEON__)
#include <arm_neon.h>
#endif
#if defined(__AVX2__)
#include <immintrin.h>
#endif

namespace draken { namespace ops {

enum class FiCmp { Eq, Ne, Gt, Ge, Lt, Le };
template <typename Op> struct FiCmpKind;
template <> struct FiCmpKind<CmpEq> { static constexpr FiCmp value = FiCmp::Eq; };
template <> struct FiCmpKind<CmpNe> { static constexpr FiCmp value = FiCmp::Ne; };
template <> struct FiCmpKind<CmpGt> { static constexpr FiCmp value = FiCmp::Gt; };
template <> struct FiCmpKind<CmpGe> { static constexpr FiCmp value = FiCmp::Ge; };
template <> struct FiCmpKind<CmpLt> { static constexpr FiCmp value = FiCmp::Lt; };
template <> struct FiCmpKind<CmpLe> { static constexpr FiCmp value = FiCmp::Le; };

// True iff `s` is representable in T (so the narrow compare is exactly the int64 one).
template <typename T>
static inline bool fi_cmp_scalar_fits(int64_t s) noexcept {
    return s >= static_cast<int64_t>(std::numeric_limits<T>::min()) &&
           s <= static_cast<int64_t>(std::numeric_limits<T>::max());
}

namespace fi_simd_detail {

#if defined(__ARM_NEON) || defined(__ARM_NEON__)

template <typename T> struct Neon;
#define FI_NEON_TRAITS(T, VEC, MVEC, LOAD, DUP, EQ, GT, GE, LT, LE, MVN)            \
    template <> struct Neon<T> {                                                    \
        using vec = VEC; using mvec = MVEC;                                         \
        static constexpr uint32_t kLanes = 16u / sizeof(T);                         \
        static inline vec  load(const T* p) { return LOAD(p); }                     \
        static inline vec  dup(T v) { return DUP(v); }                              \
        template <FiCmp K> static inline mvec cmp(vec x, vec y) {                   \
            if constexpr (K == FiCmp::Eq)      return EQ(x, y);                     \
            else if constexpr (K == FiCmp::Ne) return MVN(EQ(x, y));                \
            else if constexpr (K == FiCmp::Gt) return GT(x, y);                     \
            else if constexpr (K == FiCmp::Ge) return GE(x, y);                     \
            else if constexpr (K == FiCmp::Lt) return LT(x, y);                     \
            else                               return LE(x, y);                     \
        }                                                                           \
    };
FI_NEON_TRAITS(int8_t,   int8x16_t,  uint8x16_t, vld1q_s8,  vdupq_n_s8,  vceqq_s8,  vcgtq_s8,  vcgeq_s8,  vcltq_s8,  vcleq_s8,  vmvnq_u8)
FI_NEON_TRAITS(uint8_t,  uint8x16_t, uint8x16_t, vld1q_u8,  vdupq_n_u8,  vceqq_u8,  vcgtq_u8,  vcgeq_u8,  vcltq_u8,  vcleq_u8,  vmvnq_u8)
FI_NEON_TRAITS(int16_t,  int16x8_t,  uint16x8_t, vld1q_s16, vdupq_n_s16, vceqq_s16, vcgtq_s16, vcgeq_s16, vcltq_s16, vcleq_s16, vmvnq_u16)
FI_NEON_TRAITS(uint16_t, uint16x8_t, uint16x8_t, vld1q_u16, vdupq_n_u16, vceqq_u16, vcgtq_u16, vcgeq_u16, vcltq_u16, vcleq_u16, vmvnq_u16)
FI_NEON_TRAITS(int32_t,  int32x4_t,  uint32x4_t, vld1q_s32, vdupq_n_s32, vceqq_s32, vcgtq_s32, vcgeq_s32, vcltq_s32, vcleq_s32, vmvnq_u32)
FI_NEON_TRAITS(uint32_t, uint32x4_t, uint32x4_t, vld1q_u32, vdupq_n_u32, vceqq_u32, vcgtq_u32, vcgeq_u32, vcltq_u32, vcleq_u32, vmvnq_u32)
#undef FI_NEON_TRAITS

template <typename T, bool Bc>
static inline typename Neon<T>::vec operand(const T* p, typename Neon<T>::vec bc, uint32_t i) {
    if constexpr (Bc) return bc;
    else              return Neon<T>::load(p + i);
}

// 16 rows starting at row `i` → one u8x16 of 0xFF / 0x00 lane masks, in row order.
template <typename T, FiCmp K, bool ABc, bool BBc>
static inline uint8x16_t mask16(const T* a, typename Neon<T>::vec av,
                                const T* b, typename Neon<T>::vec bv, uint32_t i) {
    using N = Neon<T>;
    constexpr uint32_t L = N::kLanes;
    if constexpr (sizeof(T) == 1) {
        return N::template cmp<K>(operand<T, ABc>(a, av, i), operand<T, BBc>(b, bv, i));
    } else if constexpr (sizeof(T) == 2) {
        const auto m0 = N::template cmp<K>(operand<T, ABc>(a, av, i),     operand<T, BBc>(b, bv, i));
        const auto m1 = N::template cmp<K>(operand<T, ABc>(a, av, i + L), operand<T, BBc>(b, bv, i + L));
        return vuzp1q_u8(vreinterpretq_u8_u16(m0), vreinterpretq_u8_u16(m1));
    } else {
        const auto m0 = N::template cmp<K>(operand<T, ABc>(a, av, i),       operand<T, BBc>(b, bv, i));
        const auto m1 = N::template cmp<K>(operand<T, ABc>(a, av, i + L),   operand<T, BBc>(b, bv, i + L));
        const auto m2 = N::template cmp<K>(operand<T, ABc>(a, av, i + 2*L), operand<T, BBc>(b, bv, i + 2*L));
        const auto m3 = N::template cmp<K>(operand<T, ABc>(a, av, i + 3*L), operand<T, BBc>(b, bv, i + 3*L));
        const uint16x8_t lo = vuzp1q_u16(vreinterpretq_u16_u32(m0), vreinterpretq_u16_u32(m1));
        const uint16x8_t hi = vuzp1q_u16(vreinterpretq_u16_u32(m2), vreinterpretq_u16_u32(m3));
        return vuzp1q_u8(vreinterpretq_u8_u16(lo), vreinterpretq_u8_u16(hi));
    }
}

template <typename T, FiCmp K, bool ABc, bool BBc>
static inline uint32_t core(const T* a, T sa, const T* b, T sb, uint8_t* dst, uint32_t n) {
    using N = Neon<T>;
    const typename N::vec av = N::dup(sa);
    const typename N::vec bv = N::dup(sb);
    static const uint8_t kW[16] = {1, 2, 4, 8, 16, 32, 64, 128, 1, 2, 4, 8, 16, 32, 64, 128};
    const uint8x16_t w = vld1q_u8(kW);
    uint32_t i = 0;
    // 64 rows → 8 bytes. Three levels of pairwise add fold each 8 weighted lanes into
    // one byte; the byte order that falls out is the row order.
    for (; i + 64u <= n; i += 64u) {
        const uint8x16_t t0 = vandq_u8(mask16<T, K, ABc, BBc>(a, av, b, bv, i),      w);
        const uint8x16_t t1 = vandq_u8(mask16<T, K, ABc, BBc>(a, av, b, bv, i + 16), w);
        const uint8x16_t t2 = vandq_u8(mask16<T, K, ABc, BBc>(a, av, b, bv, i + 32), w);
        const uint8x16_t t3 = vandq_u8(mask16<T, K, ABc, BBc>(a, av, b, bv, i + 48), w);
        uint8x16_t p = vpaddq_u8(vpaddq_u8(t0, t1), vpaddq_u8(t2, t3));
        p = vpaddq_u8(p, p);
        vst1_u8(dst + (i >> 3), vget_low_u8(p));
    }
    for (; i + 16u <= n; i += 16u) {
        uint8x16_t p = vandq_u8(mask16<T, K, ABc, BBc>(a, av, b, bv, i), w);
        p = vpaddq_u8(p, p);
        p = vpaddq_u8(p, p);
        p = vpaddq_u8(p, p);
        const uint16_t two = vgetq_lane_u16(vreinterpretq_u16_u8(p), 0);
        memcpy(dst + (i >> 3), &two, 2);
    }
    return i;
}

#elif defined(__AVX2__)

// Signed compare only: unsigned operands are biased by the sign bit first.
template <typename T> static inline __m256i bias() {
    if constexpr (std::is_unsigned<T>::value) {
        if constexpr (sizeof(T) == 1)      return _mm256_set1_epi8(static_cast<char>(0x80));
        else if constexpr (sizeof(T) == 2) return _mm256_set1_epi16(static_cast<short>(0x8000));
        else                               return _mm256_set1_epi32(static_cast<int>(0x80000000u));
    } else {
        return _mm256_setzero_si256();
    }
}
template <typename T> static inline __m256i bcast(T v) {
    if constexpr (sizeof(T) == 1)      return _mm256_set1_epi8(static_cast<char>(v));
    else if constexpr (sizeof(T) == 2) return _mm256_set1_epi16(static_cast<short>(v));
    else                               return _mm256_set1_epi32(static_cast<int>(v));
}
template <typename T, bool Bc>
static inline __m256i operand(const T* p, __m256i bc, __m256i bs, uint32_t i) {
    if constexpr (Bc) return bc;
    else {
        const __m256i x = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(p + i));
        if constexpr (std::is_unsigned<T>::value) return _mm256_xor_si256(x, bs);
        else { (void)bs; return x; }
    }
}
// Predicate P: 0 = eq, 1 = gt(x,y), 2 = lt(x,y) i.e. gt(y,x).
template <typename T, int P>
static inline __m256i lane_cmp(__m256i x, __m256i y) {
    if constexpr (P == 2) { __m256i t = x; x = y; y = t; }
    if constexpr (sizeof(T) == 1)      return P == 0 ? _mm256_cmpeq_epi8(x, y)  : _mm256_cmpgt_epi8(x, y);
    else if constexpr (sizeof(T) == 2) return P == 0 ? _mm256_cmpeq_epi16(x, y) : _mm256_cmpgt_epi16(x, y);
    else                               return P == 0 ? _mm256_cmpeq_epi32(x, y) : _mm256_cmpgt_epi32(x, y);
}

// 32 rows → 32-bit mask (bit r = row r).
template <typename T, int P, bool ABc, bool BBc>
static inline uint32_t mask32(const T* a, __m256i av, const T* b, __m256i bv, __m256i bs, uint32_t i) {
    constexpr uint32_t L = 32u / sizeof(T);
    if constexpr (sizeof(T) == 1) {
        const __m256i c = lane_cmp<T, P>(operand<T, ABc>(a, av, bs, i), operand<T, BBc>(b, bv, bs, i));
        return static_cast<uint32_t>(_mm256_movemask_epi8(c));
    } else if constexpr (sizeof(T) == 2) {
        const __m256i c0 = lane_cmp<T, P>(operand<T, ABc>(a, av, bs, i),     operand<T, BBc>(b, bv, bs, i));
        const __m256i c1 = lane_cmp<T, P>(operand<T, ABc>(a, av, bs, i + L), operand<T, BBc>(b, bv, bs, i + L));
        // packs works per 128-bit half; 0xD8 reorders the qwords back into row order.
        const __m256i p = _mm256_permute4x64_epi64(_mm256_packs_epi16(c0, c1), 0xD8);
        return static_cast<uint32_t>(_mm256_movemask_epi8(p));
    } else {
        uint32_t m = 0;
        for (uint32_t k = 0; k < 4; ++k) {
            const __m256i c = lane_cmp<T, P>(operand<T, ABc>(a, av, bs, i + k * L),
                                             operand<T, BBc>(b, bv, bs, i + k * L));
            m |= static_cast<uint32_t>(_mm256_movemask_ps(_mm256_castsi256_ps(c))) << (8 * k);
        }
        return m;
    }
}

template <typename T, FiCmp K, bool ABc, bool BBc>
static inline uint32_t core(const T* a, T sa, const T* b, T sb, uint8_t* dst, uint32_t n) {
    constexpr int  P   = (K == FiCmp::Eq || K == FiCmp::Ne) ? 0
                       : (K == FiCmp::Gt || K == FiCmp::Le) ? 1 : 2;
    constexpr bool inv = (K == FiCmp::Ne || K == FiCmp::Le || K == FiCmp::Ge);
    const __m256i bs = bias<T>();
    __m256i av = bcast<T>(sa), bv = bcast<T>(sb);
    if constexpr (std::is_unsigned<T>::value) { av = _mm256_xor_si256(av, bs); bv = _mm256_xor_si256(bv, bs); }
    uint32_t i = 0;
    for (; i + 32u <= n; i += 32u) {
        uint32_t m = mask32<T, P, ABc, BBc>(a, av, b, bv, bs, i);
        if constexpr (inv) m = ~m;
        memcpy(dst + (i >> 3), &m, 4);      // little-endian: bit r of m = row i + r
    }
    return i;
}

#else

template <typename T, FiCmp K, bool ABc, bool BBc>
static inline uint32_t core(const T*, T, const T*, T, uint8_t*, uint32_t) { return 0; }

#endif

} // namespace fi_simd_detail

// rows [0, done) of  data[i] OP scalar  → dst bytes; returns done (multiple of 8; 0 = none).
template <typename T, typename Op>
static inline uint32_t fi_native_cmp_vs(const T* data, int64_t scalar, uint8_t* dst, uint32_t n) {
    if (!fi_cmp_scalar_fits<T>(scalar)) return 0;
    return fi_simd_detail::core<T, FiCmpKind<Op>::value, false, true>(
        data, T(0), nullptr, static_cast<T>(scalar), dst, n);
}

// rows [0, done) of  a[i] OP b[i]  → dst bytes; returns done (multiple of 8; 0 = none).
template <typename T, typename Op>
static inline uint32_t fi_native_cmp_vv(const T* a, const T* b, uint8_t* dst, uint32_t n) {
    return fi_simd_detail::core<T, FiCmpKind<Op>::value, false, false>(
        a, T(0), b, T(0), dst, n);
}

}} // namespace draken::ops
