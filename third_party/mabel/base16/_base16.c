#include "_base16.h"
#include <string.h>

// Lookup tables - all entries initialized to 255 (invalid marker) except valid hex chars
const uint8_t B16_DECODE_LUT[256] = {
    [0 ... 255] = 255,   // Initialize all to invalid
    ['0'] = 0, ['1'] = 1, ['2'] = 2, ['3'] = 3, ['4'] = 4,
    ['5'] = 5, ['6'] = 6, ['7'] = 7, ['8'] = 8, ['9'] = 9,
    ['A'] = 10, ['B'] = 11, ['C'] = 12, ['D'] = 13, ['E'] = 14,
    ['F'] = 15,
    ['a'] = 10, ['b'] = 11, ['c'] = 12, ['d'] = 13, ['e'] = 14,
    ['f'] = 15
};

const char B16_ENCODE_LUT[16] = "0123456789ABCDEF";
const char B16_ENCODE_LUT_LC[16] = "0123456789abcdef";

#define DIGIT(x) B16_DECODE_LUT[(uint8_t)(x)]
#define NOT_BASE16 255

size_t b16_encoded_size(size_t bin_size) {
    return bin_size * 2;
}

size_t b16_decoded_size(size_t b16_len) {
    return b16_len / 2;
}

// Scalar implementation for HEX encode, parameterized by the 16-char nibble
// alphabet so the uppercase and lowercase encoders share one implementation.
char* bintob16_scalar_lut(char* restrict dest, const void* restrict src, size_t size,
                          const char* restrict lut) {
    const uint8_t* in = (const uint8_t*)src;
    size_t i = 0;

    while (i < size) {
        *dest++ = lut[in[i] >> 4];
        *dest++ = lut[in[i] & 0x0F];
        i++;
    }

    *dest = '\0';
    return dest;
}

char* bintob16_scalar(char* restrict dest, const void* restrict src, size_t size) {
    return bintob16_scalar_lut(dest, src, size, B16_ENCODE_LUT);
}

// Scalar implementation for HEX decode
void* b16tobin_scalar(void* restrict dest, const char* restrict src, size_t len) {
    if (len == 0) return dest;
    if (len % 2 != 0) return NULL;

    uint8_t* out = (uint8_t*)dest;
    const uint8_t* in = (const uint8_t*)src;
    const uint8_t* end = in + len;

    while (end - in >= 2) {
        uint8_t high = DIGIT(in[0]);
        uint8_t low = DIGIT(in[1]);

        if (high == NOT_BASE16 || low == NOT_BASE16) return NULL;

        *out++ = (high << 4) | low;

        in += 2;
    }

    return out;
}

// Unity build: pull the auto-dispatch and SIMD implementations into this single
// translation unit. Each SIMD source self-guards on its target architecture and
// forwards to the scalar path otherwise, so all variants compile on every
// platform. Because b16tobin_len/bintob16 (in _base16_dispatch.c) are now
// compiled here, every consumer only needs _base16.c on its source list — the
// per-arch files must NOT also be listed separately (that double-compiles and
// duplicates symbols).
#include "_base16_dispatch.c"
#include "_base16_avx2.c"
#if defined(__ARM_NEON) || defined(__ARM_NEON__) || defined(__aarch64__)
#include "_base16_neon.c"
#endif
#include "_base16_rvv.c"
