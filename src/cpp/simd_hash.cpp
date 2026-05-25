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
#elif defined(__riscv) && defined(__riscv_vector)
#include <riscv_vector.h>
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
        std::size_t vl   = vsetvl_e64m1(count - i);
        vuint64m1_t d    = vle64_v_u64m1(dest   + i, vl);
        vuint64m1_t v    = vle64_v_u64m1(values + i, vl);
        vuint64m1_t mix  = vxor_vv_u64m1(d, v, vl);
        vuint64m1_t prod = vadd_vx_u64m1(vmul_vx_u64m1(mix, MIX_HASH_CONSTANT, vl), 1, vl);
        vuint64m1_t res  = vxor_vv_u64m1(prod, vsrl_vx_u64m1(prod, 32, vl), vl);
        vse64_v_u64m1(dest + i, res, vl);
        i += vl;
    }
}

static void simd_hash_i64_rvv(const uint64_t* src, uint64_t* dst, std::size_t count) {
    std::size_t i = 0;
    while (i < count) {
        std::size_t vl   = vsetvl_e64m1(count - i);
        vuint64m1_t v    = vle64_v_u64m1(src + i, vl);
        vuint64m1_t prod = vadd_vx_u64m1(vmul_vx_u64m1(v, MIX_HASH_CONSTANT, vl), 1, vl);
        vuint64m1_t res  = vxor_vv_u64m1(prod, vsrl_vx_u64m1(prod, 32, vl), vl);
        vse64_v_u64m1(dst + i, res, vl);
        i += vl;
    }
}

static void simd_scale_date32_rvv(const int32_t* src, int64_t* dest, std::size_t count) {
    std::size_t i = 0;
    while (i < count) {
        // vsetvl_e64m1 and vsetvl_e32mf2 yield the same vl (VLEN_bytes/8).
        std::size_t vl      = vsetvl_e64m1(count - i);
        vint32mf2_t narrow  = vle32_v_i32mf2(src + i, vl);
        vint64m1_t  widened = vsext_vf2_i64m1(narrow, vl);
        vint64m1_t  scaled  = vmul_vx_i64m1(widened, static_cast<int64_t>(DATE32_SCALE), vl);
        vse64_v_i64m1(dest + i, scaled, vl);
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
#if defined(__riscv) && defined(__riscv_vector)
        { &cpu_supports_rvv, simd_mix_hash_rvv },
#endif
    }, simd_mix_hash_scalar);

    return fn(dest, values, count);
}

// ---------------------------------------------------------------------------
// simd_hash_i64 / simd_hash_f64: single-column hash, no prior dest state.
//
// Equivalent to memset(dst,0,n*8) + simd_mix_hash(dst,src,n) in one pass.
// Used by COUNT(DISTINCT) where there is no composite key to accumulate into.
// The hash is identical to simd_mix_hash applied to a zeroed destination:
//   dst[i] = (src[i] * CONST + 1) ^ ((src[i] * CONST + 1) >> 32)
// ---------------------------------------------------------------------------

static void simd_hash_i64_scalar(const uint64_t* src, uint64_t* dst, std::size_t count) {
    for (std::size_t i = 0; i < count; ++i) {
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

#if defined(__ARM_NEON) || defined(__ARM_NEON__)
static void simd_hash_i64_neon(const uint64_t* src, uint64_t* dst, std::size_t count) {
    const uint64x2_t kc  = vdupq_n_u64(MIX_HASH_CONSTANT);
    const uint64x2_t one = vdupq_n_u64(1);
    std::size_t i = 0;
    for (; i + 4 <= count; i += 4) {
        uint64x2_t v0 = vld1q_u64(src + i);
        uint64x2_t v1 = vld1q_u64(src + i + 2);
        v0 = vaddq_u64(mullo_u64(v0, kc), one);
        v1 = vaddq_u64(mullo_u64(v1, kc), one);
        v0 = veorq_u64(v0, vshrq_n_u64(v0, 32));
        v1 = veorq_u64(v1, vshrq_n_u64(v1, 32));
        vst1q_u64(dst + i,     v0);
        vst1q_u64(dst + i + 2, v1);
    }
    for (; i + 2 <= count; i += 2) {
        uint64x2_t v = vld1q_u64(src + i);
        v = vaddq_u64(mullo_u64(v, kc), one);
        v = veorq_u64(v, vshrq_n_u64(v, 32));
        vst1q_u64(dst + i, v);
    }
    if (i < count) simd_hash_i64_scalar(src + i, dst + i, count - i);
}
#endif

void simd_hash_i64(const uint64_t* src, uint64_t* dst, std::size_t count) {
    if (!src || !dst || !count) return;
    using fn_t = void(*)(const uint64_t*, uint64_t*, std::size_t);
    static std::atomic<fn_t> cache{nullptr};
    fn_t fn = simd::select_dispatch<fn_t>(cache, {
#if defined(__AVX2__)
        { &cpu_supports_avx2, simd_hash_i64_avx2 },
#endif
#if defined(__ARM_NEON) || defined(__ARM_NEON__)
        { &cpu_supports_neon, simd_hash_i64_neon },
#endif
#if defined(__riscv) && defined(__riscv_vector)
        { &cpu_supports_rvv, simd_hash_i64_rvv },
#endif
    }, simd_hash_i64_scalar);
    fn(src, dst, count);
}

void simd_hash_f64(const double* src, uint64_t* dst, std::size_t count) {
    // Reinterpret double bits as uint64 then apply the same mixer.
    simd_hash_i64(reinterpret_cast<const uint64_t*>(src), dst, count);
}

// ---------------------------------------------------------------------------
// simd_mix_hash_from_dict_*: fused gather + mix for dict-encoded columns.
//
// The scatter+mix pattern used elsewhere first writes K-indexed lookups into a
// per-chunk scratch buffer and then calls simd_mix_hash on that buffer; this
// fused form replaces that two-pass loop with a single pass that reads codes,
// gathers from dict_lookup, and folds directly into dest[]. SIMD-specialized
// per code width (1/2/4 bytes) and per null/non-null. Code-width and null-flag
// are template parameters so the compiler emits straight-line code with no
// per-iteration branches.
// ---------------------------------------------------------------------------

namespace {

// One step of the scalar mixer. Kept inline so the compiler can fuse it into
// each specialized loop body.
inline uint64_t mix_step(uint64_t acc, uint64_t value) {
    uint64_t mixed = acc ^ value;
    mixed = mixed * MIX_HASH_CONSTANT + 1;
    mixed ^= mixed >> 32;
    return mixed;
}

inline bool bitmap_is_set(const uint8_t* bitmap, std::size_t bit_index) {
    return (bitmap[bit_index >> 3] >> (bit_index & 7)) & 1;
}

// Scalar implementation, parameterized by code-pointer type and null flag.
template <typename CodeT, bool Nullable>
void simd_mix_hash_from_dict_scalar_tpl(
        uint64_t* dest, const uint64_t* dict_lookup,
        const CodeT* codes, const uint8_t* null_bitmap,
        std::size_t start_row, std::size_t count) {
    for (std::size_t j = 0; j < count; ++j) {
        uint64_t h;
        if (Nullable) {
            h = bitmap_is_set(null_bitmap, start_row + j)
                    ? dict_lookup[codes[j]]
                    : NULL_HASH;
        } else {
            h = dict_lookup[codes[j]];
        }
        dest[j] = mix_step(dest[j], h);
    }
}

#if defined(__AVX2__)
// Widen 4 packed codes (cw1/cw2/cw4) into 4×64-bit indices for SIMD gather.
template <typename CodeT> inline __m256i load_indices_avx2(const CodeT* codes);

template <> inline __m256i load_indices_avx2<uint8_t>(const uint8_t* codes) {
    // Load 4 bytes into the low 32 bits, then zero-extend each byte to 64 bits.
    __m128i raw = _mm_cvtsi32_si128(
        *reinterpret_cast<const int32_t*>(codes));
    return _mm256_cvtepu8_epi64(raw);
}
template <> inline __m256i load_indices_avx2<uint16_t>(const uint16_t* codes) {
    // Load 4×uint16 (8 bytes), zero-extend each to 64 bits.
    __m128i raw = _mm_loadl_epi64(reinterpret_cast<const __m128i*>(codes));
    return _mm256_cvtepu16_epi64(raw);
}
template <> inline __m256i load_indices_avx2<uint32_t>(const uint32_t* codes) {
    // Load 4×uint32 (16 bytes), zero-extend each to 64 bits.
    __m128i raw = _mm_loadu_si128(reinterpret_cast<const __m128i*>(codes));
    return _mm256_cvtepu32_epi64(raw);
}

template <typename CodeT, bool Nullable>
void simd_mix_hash_from_dict_avx2_tpl(
        uint64_t* dest, const uint64_t* dict_lookup,
        const CodeT* codes, const uint8_t* null_bitmap,
        std::size_t start_row, std::size_t count) {
    const __m256i const_vec = _mm256_set1_epi64x(static_cast<long long>(MIX_HASH_CONSTANT));
    const __m256i one_vec = _mm256_set1_epi64x(1);
    const std::size_t stride = 4;
    std::size_t i = 0;

    for (; i + stride <= count; i += stride) {
        __m256i indices = load_indices_avx2<CodeT>(codes + i);
        // SIMD gather 4 dict hashes (scale = 8 bytes per uint64).
        __m256i val_vec = _mm256_i64gather_epi64(
            reinterpret_cast<const long long*>(dict_lookup), indices, 8);

        if (Nullable) {
            // Patch up null lanes with NULL_HASH. Branch-free: read 4 lanes
            // out, fix any nulls in scalar, reload.
            alignas(32) uint64_t lanes[4];
            _mm256_store_si256(reinterpret_cast<__m256i*>(lanes), val_vec);
            const std::size_t base = start_row + i;
            if (!bitmap_is_set(null_bitmap, base + 0)) lanes[0] = NULL_HASH;
            if (!bitmap_is_set(null_bitmap, base + 1)) lanes[1] = NULL_HASH;
            if (!bitmap_is_set(null_bitmap, base + 2)) lanes[2] = NULL_HASH;
            if (!bitmap_is_set(null_bitmap, base + 3)) lanes[3] = NULL_HASH;
            val_vec = _mm256_load_si256(reinterpret_cast<const __m256i*>(lanes));
        }

        __m256i dst_vec = _mm256_loadu_si256(
            reinterpret_cast<const __m256i*>(dest + i));
        __m256i mixed = _mm256_xor_si256(dst_vec, val_vec);
        __m256i product = mullo_u64(mixed, const_vec);
        product = _mm256_add_epi64(product, one_vec);
        __m256i shifted = _mm256_srli_epi64(product, 32);
        __m256i combined = _mm256_xor_si256(product, shifted);
        _mm256_storeu_si256(reinterpret_cast<__m256i*>(dest + i), combined);
    }

    // Scalar tail (0..3 elements).
    if (i < count) {
        simd_mix_hash_from_dict_scalar_tpl<CodeT, Nullable>(
            dest + i, dict_lookup, codes + i, null_bitmap, start_row + i,
            count - i);
    }
}
#endif  // __AVX2__

#if defined(__ARM_NEON) || defined(__ARM_NEON__)
template <typename CodeT, bool Nullable>
void simd_mix_hash_from_dict_neon_tpl(
        uint64_t* dest, const uint64_t* dict_lookup,
        const CodeT* codes, const uint8_t* null_bitmap,
        std::size_t start_row, std::size_t count) {
    const uint64x2_t const_vec = vdupq_n_u64(MIX_HASH_CONSTANT);
    const uint64x2_t one_vec = vdupq_n_u64(1);
    std::size_t i = 0;

    // 4 elements (2 NEON pairs) per iteration, matching simd_mix_hash_neon.
    // NEON has no gather, so the gather is scalar; the win vs the old code is
    // the eliminated scratch buffer pass on dest[].
    for (; i + 4 <= count; i += 4) {
        uint64_t v[4];
        if (Nullable) {
            const std::size_t base = start_row + i;
            v[0] = bitmap_is_set(null_bitmap, base + 0) ? dict_lookup[codes[i + 0]] : NULL_HASH;
            v[1] = bitmap_is_set(null_bitmap, base + 1) ? dict_lookup[codes[i + 1]] : NULL_HASH;
            v[2] = bitmap_is_set(null_bitmap, base + 2) ? dict_lookup[codes[i + 2]] : NULL_HASH;
            v[3] = bitmap_is_set(null_bitmap, base + 3) ? dict_lookup[codes[i + 3]] : NULL_HASH;
        } else {
            v[0] = dict_lookup[codes[i + 0]];
            v[1] = dict_lookup[codes[i + 1]];
            v[2] = dict_lookup[codes[i + 2]];
            v[3] = dict_lookup[codes[i + 3]];
        }
        uint64x2_t v0 = vld1q_u64(v);
        uint64x2_t v1 = vld1q_u64(v + 2);
        uint64x2_t d0 = vld1q_u64(dest + i);
        uint64x2_t d1 = vld1q_u64(dest + i + 2);
        uint64x2_t m0 = veorq_u64(d0, v0);
        uint64x2_t m1 = veorq_u64(d1, v1);
        m0 = vaddq_u64(mullo_u64(m0, const_vec), one_vec);
        m1 = vaddq_u64(mullo_u64(m1, const_vec), one_vec);
        m0 = veorq_u64(m0, vshrq_n_u64(m0, 32));
        m1 = veorq_u64(m1, vshrq_n_u64(m1, 32));
        vst1q_u64(dest + i, m0);
        vst1q_u64(dest + i + 2, m1);
    }

    // Scalar tail.
    if (i < count) {
        simd_mix_hash_from_dict_scalar_tpl<CodeT, Nullable>(
            dest + i, dict_lookup, codes + i, null_bitmap, start_row + i,
            count - i);
    }
}
#endif  // __ARM_NEON

#if defined(__riscv) && defined(__riscv_vector)
// RVV dict gather+mix.
//
// Non-nullable path: vloxei64 (byte-indexed gather) replaces the two-pass
// scatter+simd_mix_hash pattern.  Code widths are handled with fractional
// LMUL zero-extends (vzext_vfN) so element counts always match vl from
// vsetvl_e64m1.
//
// Nullable path: mirrors the NEON approach — scalar null-guarded gather into
// a fixed-size stack buffer, then vector mixing.  The buffer is capped at
// RVV_MAX_VL_E64M1 (covers VLEN up to 4096 bits at LMUL=1).
static constexpr std::size_t RVV_MAX_VL_E64M1 = 64;

template <typename CodeT, bool Nullable>
void simd_mix_hash_from_dict_rvv_tpl(
        uint64_t* dest, const uint64_t* dict_lookup,
        const CodeT* codes, const uint8_t* null_bitmap,
        std::size_t start_row, std::size_t count) {
    std::size_t i = 0;
    while (i < count) {
        const std::size_t batch =
            Nullable ? std::min(count - i, RVV_MAX_VL_E64M1) : (count - i);
        std::size_t vl = vsetvl_e64m1(batch);

        vuint64m1_t val_vec;

        if constexpr (Nullable) {
            // Scalar null-guarded gather into a stack buffer of known size.
            uint64_t v[RVV_MAX_VL_E64M1];
            const std::size_t base = start_row + i;
            for (std::size_t k = 0; k < vl; ++k) {
                v[k] = bitmap_is_set(null_bitmap, base + k)
                           ? dict_lookup[codes[i + k]]
                           : NULL_HASH;
            }
            val_vec = vle64_v_u64m1(v, vl);
        } else {
            // Fully vectorized: widen codes to byte offsets, indexed gather.
            vuint64m1_t offsets;
            if constexpr (std::is_same_v<CodeT, uint8_t>) {
                // u8 → u64: vzext_vf8 (LMUL u8mf8 → u64m1, same vl)
                offsets = vsll_vx_u64m1(
                    vzext_vf8_u64m1(vle8_v_u8mf8(codes + i, vl), vl), 3, vl);
            } else if constexpr (std::is_same_v<CodeT, uint16_t>) {
                // u16 → u64: vzext_vf4 (LMUL u16mf4 → u64m1, same vl)
                offsets = vsll_vx_u64m1(
                    vzext_vf4_u64m1(vle16_v_u16mf4(codes + i, vl), vl), 3, vl);
            } else {
                // u32 → u64: vzext_vf2 (LMUL u32mf2 → u64m1, same vl)
                offsets = vsll_vx_u64m1(
                    vzext_vf2_u64m1(vle32_v_u32mf2(codes + i, vl), vl), 3, vl);
            }
            // Byte-indexed gather: dict_lookup[code] = *(dict_lookup + offset)
            val_vec = vloxei64_v_u64m1(dict_lookup, offsets, vl);
        }

        vuint64m1_t d    = vle64_v_u64m1(dest + i, vl);
        vuint64m1_t mix  = vxor_vv_u64m1(d, val_vec, vl);
        vuint64m1_t prod = vadd_vx_u64m1(vmul_vx_u64m1(mix, MIX_HASH_CONSTANT, vl), 1, vl);
        vuint64m1_t res  = vxor_vv_u64m1(prod, vsrl_vx_u64m1(prod, 32, vl), vl);
        vse64_v_u64m1(dest + i, res, vl);
        i += vl;
    }
}
#endif  // __riscv && __riscv_vector

// Dispatcher per (CodeT, Nullable) pair. Each template instantiation owns its
// own atomic cache, so the CPU probe runs once per kernel variant.
template <typename CodeT, bool Nullable>
void simd_mix_hash_from_dict_dispatch(
        uint64_t* dest, const uint64_t* dict_lookup,
        const CodeT* codes, const uint8_t* null_bitmap,
        std::size_t start_row, std::size_t count) {
    using fn_t = void (*)(uint64_t*, const uint64_t*, const CodeT*,
                          const uint8_t*, std::size_t, std::size_t);
    static std::atomic<fn_t> cache{nullptr};

    fn_t fn = simd::select_dispatch<fn_t>(cache, {
#if defined(__AVX2__)
        { &cpu_supports_avx2, simd_mix_hash_from_dict_avx2_tpl<CodeT, Nullable> },
#endif
#if defined(__ARM_NEON) || defined(__ARM_NEON__)
        { &cpu_supports_neon, simd_mix_hash_from_dict_neon_tpl<CodeT, Nullable> },
#endif
#if defined(__riscv) && defined(__riscv_vector)
        { &cpu_supports_rvv, simd_mix_hash_from_dict_rvv_tpl<CodeT, Nullable> },
#endif
    }, simd_mix_hash_from_dict_scalar_tpl<CodeT, Nullable>);

    fn(dest, dict_lookup, codes, null_bitmap, start_row, count);
}

}  // namespace

void simd_mix_hash_from_dict_cw1(uint64_t* dest, const uint64_t* dict_lookup,
                                  const uint8_t* codes, std::size_t count) {
    if (dest == nullptr || dict_lookup == nullptr || codes == nullptr || count == 0) {
        return;
    }
    simd_mix_hash_from_dict_dispatch<uint8_t, false>(
        dest, dict_lookup, codes, nullptr, 0, count);
}

void simd_mix_hash_from_dict_cw2(uint64_t* dest, const uint64_t* dict_lookup,
                                  const uint16_t* codes, std::size_t count) {
    if (dest == nullptr || dict_lookup == nullptr || codes == nullptr || count == 0) {
        return;
    }
    simd_mix_hash_from_dict_dispatch<uint16_t, false>(
        dest, dict_lookup, codes, nullptr, 0, count);
}

void simd_mix_hash_from_dict_cw4(uint64_t* dest, const uint64_t* dict_lookup,
                                  const uint32_t* codes, std::size_t count) {
    if (dest == nullptr || dict_lookup == nullptr || codes == nullptr || count == 0) {
        return;
    }
    simd_mix_hash_from_dict_dispatch<uint32_t, false>(
        dest, dict_lookup, codes, nullptr, 0, count);
}

void simd_mix_hash_from_dict_nullable_cw1(uint64_t* dest, const uint64_t* dict_lookup,
                                           const uint8_t* codes, const uint8_t* null_bitmap,
                                           std::size_t start_row, std::size_t count) {
    if (dest == nullptr || dict_lookup == nullptr || codes == nullptr || count == 0) {
        return;
    }
    if (null_bitmap == nullptr) {
        simd_mix_hash_from_dict_cw1(dest, dict_lookup, codes, count);
        return;
    }
    simd_mix_hash_from_dict_dispatch<uint8_t, true>(
        dest, dict_lookup, codes, null_bitmap, start_row, count);
}

void simd_mix_hash_from_dict_nullable_cw2(uint64_t* dest, const uint64_t* dict_lookup,
                                           const uint16_t* codes, const uint8_t* null_bitmap,
                                           std::size_t start_row, std::size_t count) {
    if (dest == nullptr || dict_lookup == nullptr || codes == nullptr || count == 0) {
        return;
    }
    if (null_bitmap == nullptr) {
        simd_mix_hash_from_dict_cw2(dest, dict_lookup, codes, count);
        return;
    }
    simd_mix_hash_from_dict_dispatch<uint16_t, true>(
        dest, dict_lookup, codes, null_bitmap, start_row, count);
}

void simd_mix_hash_from_dict_nullable_cw4(uint64_t* dest, const uint64_t* dict_lookup,
                                           const uint32_t* codes, const uint8_t* null_bitmap,
                                           std::size_t start_row, std::size_t count) {
    if (dest == nullptr || dict_lookup == nullptr || codes == nullptr || count == 0) {
        return;
    }
    if (null_bitmap == nullptr) {
        simd_mix_hash_from_dict_cw4(dest, dict_lookup, codes, count);
        return;
    }
    simd_mix_hash_from_dict_dispatch<uint32_t, true>(
        dest, dict_lookup, codes, null_bitmap, start_row, count);
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
#if defined(__riscv) && defined(__riscv_vector)
        { &cpu_supports_rvv, simd_scale_date32_rvv },
#endif
    }, simd_scale_date32_scalar);

    return fn(src, dest, count);
}