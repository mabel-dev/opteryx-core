// dev/bench/bench_tier1_simd.cpp
//
// Micro-benchmark: scalar vs NEON for the three Tier-1 SIMD targets.
//
//   1. expand_bitmap_byte_to_u64_masks  (simd_bitmap.cpp)
//   2. bm_negate_inplace (no-validity)  (vector_misc.cpp)
//   3. bm_negate_inplace (with-validity)(vector_misc.cpp)
//   4. cmp_and_validity                 (draken/ops/int64_compare.h)
//
// Build (from repo root):
//   c++ -std=c++20 -O2 -march=native \
//       dev/bench/bench_tier1_simd.cpp -o /tmp/bench_tier1 && /tmp/bench_tier1
//
// ARM (NEON) only — that is the dev platform.

#include <cstdint>
#include <cstdio>
#include <cstring>
#include <cstdlib>
#include <time.h>

#if defined(__ARM_NEON) || defined(__ARM_NEON__)
#include <arm_neon.h>
#define HAVE_NEON 1
#else
#define HAVE_NEON 0
#endif

// ============================================================================
// Timing helper
// ============================================================================
static inline double now_ns() {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts.tv_sec * 1e9 + ts.tv_nsec;
}

// Prevent the compiler from optimising away a benchmark result.
static volatile uint64_t sink64 = 0;
static volatile uint8_t  sink8  = 0;

// ============================================================================
// 1. expand_bitmap_byte_to_u64_masks
// ============================================================================
static void expand_scalar(uint8_t bm, uint64_t* out) {
    for (int i = 0; i < 8; ++i) out[i] = -(uint64_t)((bm >> i) & 1);
}

#if HAVE_NEON
static void expand_neon(uint8_t bm, uint64_t* out) {
    static const uint8_t k_sels[8] = {0x01,0x02,0x04,0x08,0x10,0x20,0x40,0x80};
    uint8x8_t bcast  = vdup_n_u8(bm);
    uint8x8_t sels   = vld1_u8(k_sels);
    uint8x8_t bits   = vand_u8(bcast, sels);
    uint8x8_t bytes8 = vcgt_u8(bits, vdup_n_u8(0));

    uint16x8_t  w16    = vmovl_u8(bytes8);
    uint32x4_t  w32_lo = vmovl_u16(vget_low_u16(w16));
    uint32x4_t  w32_hi = vmovl_u16(vget_high_u16(w16));

    vst1q_u64(out + 0, vmovl_u32(vget_low_u32(w32_lo)));
    vst1q_u64(out + 2, vmovl_u32(vget_high_u32(w32_lo)));
    vst1q_u64(out + 4, vmovl_u32(vget_low_u32(w32_hi)));
    vst1q_u64(out + 6, vmovl_u32(vget_high_u32(w32_hi)));
}
#endif

static void bench_expand(int iters) {
    alignas(64) uint64_t out[8];

    // Scalar
    double t0 = now_ns();
    for (int i = 0; i < iters; ++i) {
        expand_scalar((uint8_t)i, out);
        sink64 ^= out[0] ^ out[7];
    }
    double scalar_ns = (now_ns() - t0) / iters;

#if HAVE_NEON
    double t1 = now_ns();
    for (int i = 0; i < iters; ++i) {
        expand_neon((uint8_t)i, out);
        sink64 ^= out[0] ^ out[7];
    }
    double neon_ns = (now_ns() - t1) / iters;
    printf("expand_bitmap_byte_to_u64_masks\n");
    printf("  scalar : %6.2f ns/call\n", scalar_ns);
    printf("  neon   : %6.2f ns/call   speedup %.1fx\n\n",
           neon_ns, scalar_ns / neon_ns);
#else
    printf("expand_bitmap_byte_to_u64_masks\n");
    printf("  scalar : %6.2f ns/call   (NEON not available)\n\n", scalar_ns);
#endif
}

// ============================================================================
// 2 & 3. bm_negate_inplace
// ============================================================================
static void negate_scalar_novalid(uint8_t* d, uint32_t nb) {
    for (uint32_t i = 0; i < nb; ++i) d[i] = ~d[i];
}
static void negate_scalar_valid(uint8_t* d, const uint8_t* v, uint32_t nb) {
    for (uint32_t i = 0; i < nb; ++i) d[i] = ~d[i] & v[i];
}

#if HAVE_NEON
// 4×16 = 64-byte unroll matching the compiler's auto-vectorisation grain.
static void negate_neon_novalid(uint8_t* d, uint32_t nb) {
    uint32_t i = 0;
    for (; i + 64u <= nb; i += 64u) {
        vst1q_u8(d+i+ 0, vmvnq_u8(vld1q_u8(d+i+ 0)));
        vst1q_u8(d+i+16, vmvnq_u8(vld1q_u8(d+i+16)));
        vst1q_u8(d+i+32, vmvnq_u8(vld1q_u8(d+i+32)));
        vst1q_u8(d+i+48, vmvnq_u8(vld1q_u8(d+i+48)));
    }
    for (; i + 16u <= nb; i += 16u) vst1q_u8(d+i, vmvnq_u8(vld1q_u8(d+i)));
    for (; i < nb; ++i) d[i] = ~d[i];
}
static void negate_neon_valid(uint8_t* d, const uint8_t* v, uint32_t nb) {
    uint32_t i = 0;
    for (; i + 64u <= nb; i += 64u) {
        vst1q_u8(d+i+ 0, vandq_u8(vmvnq_u8(vld1q_u8(d+i+ 0)), vld1q_u8(v+i+ 0)));
        vst1q_u8(d+i+16, vandq_u8(vmvnq_u8(vld1q_u8(d+i+16)), vld1q_u8(v+i+16)));
        vst1q_u8(d+i+32, vandq_u8(vmvnq_u8(vld1q_u8(d+i+32)), vld1q_u8(v+i+32)));
        vst1q_u8(d+i+48, vandq_u8(vmvnq_u8(vld1q_u8(d+i+48)), vld1q_u8(v+i+48)));
    }
    for (; i + 16u <= nb; i += 16u)
        vst1q_u8(d+i, vandq_u8(vmvnq_u8(vld1q_u8(d+i)), vld1q_u8(v+i)));
    for (; i < nb; ++i) d[i] = ~d[i] & v[i];
}
#endif

static void bench_negate(int iters, int n_rows) {
    const uint32_t nb = (n_rows + 7) / 8;
    const size_t alloc_nb = ((nb + 127u) / 64u) * 64u;
    uint8_t* data     = (uint8_t*)aligned_alloc(64, alloc_nb);
    uint8_t* validity = (uint8_t*)aligned_alloc(64, alloc_nb);
    memset(data,     0xAA, nb);
    memset(validity, 0xF0, nb);

    // --- no-validity path ---
    double t0 = now_ns();
    for (int i = 0; i < iters; ++i) {
        negate_scalar_novalid(data, nb);
        sink8 ^= data[0];
    }
    double sc_nv = (now_ns() - t0) / iters;

#if HAVE_NEON
    double t1 = now_ns();
    for (int i = 0; i < iters; ++i) {
        negate_neon_novalid(data, nb);
        sink8 ^= data[0];
    }
    double ne_nv = (now_ns() - t1) / iters;
#endif

    // --- with-validity path ---
    double t2 = now_ns();
    for (int i = 0; i < iters; ++i) {
        negate_scalar_valid(data, validity, nb);
        sink8 ^= data[0];
    }
    double sc_v = (now_ns() - t2) / iters;

#if HAVE_NEON
    double t3 = now_ns();
    for (int i = 0; i < iters; ++i) {
        negate_neon_valid(data, validity, nb);
        sink8 ^= data[0];
    }
    double ne_v = (now_ns() - t3) / iters;

    printf("bm_negate_inplace  (%d rows = %u bytes)\n", n_rows, nb);
    printf("  scalar no-validity: %7.1f ns   (%.2f ns/byte)\n", sc_nv, sc_nv/nb);
    printf("  neon   no-validity: %7.1f ns   (%.2f ns/byte)   speedup %.1fx\n",
           ne_nv, ne_nv/nb, sc_nv/ne_nv);
    printf("  scalar   validity : %7.1f ns   (%.2f ns/byte)\n", sc_v,  sc_v/nb);
    printf("  neon     validity : %7.1f ns   (%.2f ns/byte)   speedup %.1fx\n\n",
           ne_v,  ne_v/nb,  sc_v/ne_v);
#else
    printf("bm_negate_inplace  (%d rows = %u bytes)\n", n_rows, nb);
    printf("  scalar no-validity: %7.1f ns   (NEON not available)\n", sc_nv);
    printf("  scalar   validity : %7.1f ns\n\n", sc_v);
#endif

    free(data);
    free(validity);
}

// ============================================================================
// 4. cmp_and_validity
// ============================================================================
static bool cmp_scalar(const uint8_t* va, const uint8_t* vb, uint8_t* dst,
                       uint32_t nb, uint32_t n) {
    bool all_valid = true;
    for (uint32_t k = 0; k < nb; ++k) {
        uint8_t exp = 0xFF;
        if (k == nb - 1u && (n & 7u)) exp = (uint8_t)((1u << (n & 7u)) - 1u);
        dst[k] = va[k] & vb[k];
        if (dst[k] != exp) all_valid = false;
    }
    return all_valid;
}

#if HAVE_NEON
static bool cmp_neon(const uint8_t* va, const uint8_t* vb, uint8_t* dst,
                     uint32_t nb, uint32_t n) {
    const uint32_t nb_full = (n & 7u) ? nb - 1u : nb;
    bool all_valid = true;
    uint32_t k = 0;
    uint8x16_t min_acc = vdupq_n_u8(0xFF);
    for (; k + 16u <= nb_full; k += 16u) {
        uint8x16_t r = vandq_u8(vld1q_u8(va + k), vld1q_u8(vb + k));
        vst1q_u8(dst + k, r);
        min_acc = vminq_u8(min_acc, r);
    }
    if (vminvq_u8(min_acc) < 0xFF) all_valid = false;
    for (; k < nb_full; ++k) {
        dst[k] = va[k] & vb[k];
        if (dst[k] != 0xFF) all_valid = false;
    }
    if (n & 7u) {
        const uint8_t exp = (1u << (n & 7u)) - 1u;
        dst[nb-1u] = va[nb-1u] & vb[nb-1u];
        if (dst[nb-1u] != exp) all_valid = false;
    }
    return all_valid;
}
#endif

static void bench_cmp_validity(int iters, int n_rows) {
    const uint32_t nb = (n_rows + 7) / 8;
    const size_t alloc_nb = ((nb + 127u) / 64u) * 64u;
    uint8_t* va  = (uint8_t*)aligned_alloc(64, alloc_nb);
    uint8_t* vb  = (uint8_t*)aligned_alloc(64, alloc_nb);
    uint8_t* dst = (uint8_t*)aligned_alloc(64, alloc_nb);
    // Mix of valid and null rows so both paths exercise real logic.
    for (uint32_t i = 0; i < nb; ++i) { va[i] = 0xAB; vb[i] = 0xCD; }

    double t0 = now_ns();
    for (int i = 0; i < iters; ++i) {
        bool r = cmp_scalar(va, vb, dst, nb, n_rows);
        sink8 ^= (uint8_t)r ^ dst[0];
    }
    double sc = (now_ns() - t0) / iters;

#if HAVE_NEON
    double t1 = now_ns();
    for (int i = 0; i < iters; ++i) {
        bool r = cmp_neon(va, vb, dst, nb, n_rows);
        sink8 ^= (uint8_t)r ^ dst[0];
    }
    double ne = (now_ns() - t1) / iters;
    printf("cmp_and_validity  (%d rows = %u bytes)\n", n_rows, nb);
    printf("  scalar : %7.1f ns   (%.2f ns/byte)\n", sc, sc/nb);
    printf("  neon   : %7.1f ns   (%.2f ns/byte)   speedup %.1fx\n\n",
           ne, ne/nb, sc/ne);
#else
    printf("cmp_and_validity  (%d rows = %u bytes)\n", n_rows, nb);
    printf("  scalar : %7.1f ns   (NEON not available)\n\n", sc);
#endif

    free(va); free(vb); free(dst);
}

// ============================================================================
// main
// ============================================================================
int main() {
#if HAVE_NEON
    printf("Platform: ARM NEON\n\n");
#else
    printf("Platform: scalar fallback (no NEON)\n\n");
#endif

    // Warm up cache and branch predictor
    { uint64_t tmp[8]; expand_scalar(0xA5, tmp); (void)tmp; }

    bench_expand(20'000'000);
    bench_negate(200'000,  1'000);   // 1K  rows = 125 bytes
    bench_negate(200'000, 65'536);   // 64K rows = 8192 bytes
    bench_cmp_validity(200'000,  1'000);
    bench_cmp_validity(200'000, 65'536);

    (void)sink64; (void)sink8;
    return 0;
}
