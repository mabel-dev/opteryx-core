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

#define DIGIT(x) B16_DECODE_LUT[(uint8_t)(x)]
#define NOT_BASE16 255

size_t b16_encoded_size(size_t bin_size) {
    return bin_size * 2;
}

size_t b16_decoded_size(size_t b16_len) {
    return b16_len / 2;
}

// Scalar implementation for HEX encode
char* bintob16_scalar(char* restrict dest, const void* restrict src, size_t size) {
    const uint8_t* in = (const uint8_t*)src;
    size_t i = 0;

    while (i < size) {
        *dest++ = B16_ENCODE_LUT[in[i] >> 4];
        *dest++ = B16_ENCODE_LUT[in[i] & 0x0F];
        i++;
    }

    *dest = '\0';
    return dest;
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

// Pull in auto-dispatch and SIMD implementations
#include "_base16_dispatch.c"
#include "_base16_avx2.c"
#include "_base16_neon.c"
