#include "simd_hash.h"

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <algorithm>  // std::min (RVV path; pulled in transitively elsewhere on x86/ARM)

#include "simd_dispatch.h"

#if defined(__riscv) && defined(__riscv_vector)
#include <riscv_vector.h>
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


// NOTE: there is deliberately no hand-written vector mixer on x86 or ARM.
// AVX2 and NEON both lack a 64x64->64 integer multiply, so a hand vector mixer
// must emulate it from 32-bit partial products.
//   ARM (M-series, byte-identical, interleaved, min of 15, L2 and 64 MiB):
//     NEON (3x vmull emulation) 0.315 ns/value vs scalar_mix 0.203 — 1.55x slower.
//   x86 (i5-8500, gcc 12 -O3 -march=haswell, min of 31, 2026-10-04,
//   dev/bench_hash_mix_arch.cpp): hand AVX2 0.66-0.68 ns/value vs scalar_mix
//     0.63 at 1 MiB — the compiler auto-vectorizes scalar_mix itself (vpmuludq)
//     and beats the hand kernel; at 64 MiB they tie. ClickBench suite with the
//     hand kernel removed: 0.9995 (7 rounds). The hand AVX2 mixer was deleted.
// So every ISA except RVV (which has a native vmul) runs scalar_mix, and on x86
// "scalar" means "what the compiler vectorizes". Re-measure before adding a hand
// vector mixer back on either.

void simd_mix_hash(uint64_t* dest, const uint64_t* values, std::size_t count) {
    using fn_t = void(*)(uint64_t*, const uint64_t*, std::size_t);
    // x86 and ARM slots are scalar_mix by measurement, not by omission - see the note above.
    fn_t fn = SIMD_STATIC_SELECT(simd_mix_hash_scalar, simd_mix_hash_scalar, simd_mix_hash_rvv, simd_mix_hash_scalar);

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


// No hand-written x86/ARM variant: this is the mixer minus the xor-with-dest, so
// the same measurements apply. ARM: NEON 0.297 ns/value vs unrolled scalar
// 0.155 (1.91x slower; byte-identical, interleaved, min of 15, 512 KiB and
// 32 MiB). x86 (i5-8500, gcc 12, 2026-10-04): hand AVX2 0.58-0.60 vs the
// compiler-vectorized scalar loop 0.51 at 1 MiB, tied at 64 MiB — the hand
// AVX2 kernel was deleted. Both slots select the scalar kernel on purpose.

void simd_hash_i64(const uint64_t* src, uint64_t* dst, std::size_t count) {
    if (!src || !dst || !count) return;
    using fn_t = void(*)(const uint64_t*, uint64_t*, std::size_t);
    // x86 and ARM slots are the scalar kernel by measurement - see the note above.
    fn_t fn = SIMD_STATIC_SELECT(simd_hash_i64_scalar, simd_hash_i64_scalar, simd_hash_i64_rvv, simd_hash_i64_scalar);
    fn(src, dst, count);
}
