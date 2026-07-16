#include "_base16.h"
#include <string.h>

/*
 * Runtime dispatch for base16. NEON is part of the AArch64 baseline; on
 * x86 we runtime-detect AVX2; otherwise scalar.
 */

#if defined(__ARM_NEON) || defined(__aarch64__)
  #define B16_HAVE_NEON 1
#else
  #define B16_HAVE_NEON 0
#endif

#if defined(__riscv) && defined(__riscv_vector)
  #define B16_HAVE_RVV 1
#else
  #define B16_HAVE_RVV 0
#endif

#ifdef __x86_64__
#include <cpuid.h>

static int detected = 0;
static int has_avx2 = 0;

static int x86_avx2(void) {
    unsigned a, b, c, d;
    if (!__get_cpuid(1, &a, &b, &c, &d)) return 0;
    if (!(c & (1u << 27))) return 0;
    if (!(c & (1u << 28))) return 0;
    if (!__get_cpuid_count(7, 0, &a, &b, &c, &d)) return 0;
    return (b & (1u << 5)) != 0;
}

static void detect(void) {
    if (detected) return;
    has_avx2 = x86_avx2();
    detected = 1;
}
#else
static void detect(void) {}
#endif

void* b16tobin_len(void* restrict dest, const char* restrict src, size_t len) {
    detect();
#ifdef __x86_64__
    if (has_avx2 && len >= 32) return b16tobin_avx2(dest, src, len);
#endif
#if B16_HAVE_NEON
    if (len >= 32) return b16tobin_neon(dest, src, len);
#endif
#if B16_HAVE_RVV
    if (len >= 32) return b16tobin_rvv(dest, src, len);
#endif
    return b16tobin_scalar(dest, src, len);
}

void* b16tobin(void* restrict dest, const char* restrict src) {
    return b16tobin_len(dest, src, strlen(src));
}

char* bintob16_lut(char* restrict dest, const void* restrict src, size_t size,
                   const char* restrict lut) {
    detect();
#ifdef __x86_64__
    if (has_avx2 && size >= 16) return bintob16_avx2_lut(dest, src, size, lut);
#endif
#if B16_HAVE_NEON
    if (size >= 16) return bintob16_neon_lut(dest, src, size, lut);
#endif
#if B16_HAVE_RVV
    if (size >= 16) return bintob16_rvv_lut(dest, src, size, lut);
#endif
    return bintob16_scalar_lut(dest, src, size, lut);
}

char* bintob16(char* restrict dest, const void* restrict src, size_t size) {
    return bintob16_lut(dest, src, size, B16_ENCODE_LUT);
}

char* bintob16_lower(char* restrict dest, const void* restrict src, size_t size) {
    return bintob16_lut(dest, src, size, B16_ENCODE_LUT_LC);
}
