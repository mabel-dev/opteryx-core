#include "_base16.h"

// Placeholder AVX2 implementation: fall back to scalar for now.
// Replace with true AVX2 intrinsics for performance.

void* b16tobin_avx2(void* restrict dest, const char* restrict src, size_t len) {
    return b16tobin_scalar(dest, src, len);
}

char* bintob16_avx2(char* restrict dest, const void* restrict src, size_t size) {
    return bintob16_scalar(dest, src, size);
}
