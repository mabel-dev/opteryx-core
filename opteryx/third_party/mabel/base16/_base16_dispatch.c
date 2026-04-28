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
    return b16tobin_scalar(dest, src, len);
}

void* b16tobin(void* restrict dest, const char* restrict src) {
    return b16tobin_len(dest, src, strlen(src));
}

char* bintob16(char* restrict dest, const void* restrict src, size_t size) {
    detect();
#ifdef __x86_64__
    if (has_avx2 && size >= 16) return bintob16_avx2(dest, src, size);
#endif
#if B16_HAVE_NEON
    if (size >= 16) return bintob16_neon(dest, src, size);
#endif
    return bintob16_scalar(dest, src, size);
}
