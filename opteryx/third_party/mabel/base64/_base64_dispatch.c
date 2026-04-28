#include "_base64.h"
#include <string.h>

/*
 * Runtime dispatch for base64.
 *
 * On ARM (NEON is part of the AArch64 baseline) we always take the NEON
 * path. On x86 we runtime-detect AVX2; otherwise we use scalar.
 * AVX512 is not supported (production target is GCP Cloud Run x86, which
 * does not reliably expose AVX512).
 */

#if defined(__ARM_NEON) || defined(__aarch64__)
  #define B64_HAVE_NEON 1
#else
  #define B64_HAVE_NEON 0
#endif

#ifdef __x86_64__
#include <cpuid.h>

static int detected = 0;
static int has_avx2 = 0;

static int x86_avx2(void) {
    unsigned a, b, c, d;
    if (!__get_cpuid(1, &a, &b, &c, &d)) return 0;
    if (!(c & (1u << 27))) return 0;  /* OSXSAVE */
    if (!(c & (1u << 28))) return 0;  /* AVX */
    if (!__get_cpuid_count(7, 0, &a, &b, &c, &d)) return 0;
    return (b & (1u << 5)) != 0;      /* AVX2 */
}

static void detect(void) {
    if (detected) return;
    has_avx2 = x86_avx2();
    detected = 1;
}
#else
static void detect(void) {}
#endif

void* b64tobin_len(void* B64_RESTRICT dest, const char* B64_RESTRICT src, size_t len) {
    detect();
#ifdef __x86_64__
    if (has_avx2 && len >= 32) return b64tobin_avx2(dest, src, len);
#endif
#if B64_HAVE_NEON
    if (len >= 64) return b64tobin_neon(dest, src, len);
#endif
    return b64tobin_scalar(dest, src, len);
}

void* b64tobin(void* B64_RESTRICT dest, const char* B64_RESTRICT src) {
    return b64tobin_len(dest, src, strlen(src));
}

char* bintob64(char* B64_RESTRICT dest, const void* B64_RESTRICT src, size_t size) {
    detect();
#ifdef __x86_64__
    if (has_avx2 && size >= 32) return bintob64_avx2(dest, src, size);
#endif
#if B64_HAVE_NEON
    if (size >= 48) return bintob64_neon(dest, src, size);
#endif
    return bintob64_scalar(dest, src, size);
}
