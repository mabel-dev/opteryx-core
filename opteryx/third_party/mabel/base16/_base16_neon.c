#include "_base16.h"

// Placeholder NEON implementation: fall back to scalar for now.
// Replace with true NEON intrinsics for performance.

void* b16tobin_neon(void* restrict dest, const char* restrict src, size_t len) {
    return b16tobin_scalar(dest, src, len);
}

char* bintob16_neon(char* restrict dest, const void* restrict src, size_t size) {
    return bintob16_scalar(dest, src, size);
}
