#include "_base85.h"
#include <string.h>
#include <stdint.h>

/*
 * base85 (Mercurial alphabet, matches Python's base64.b85encode/b85decode).
 *
 * Scalar-only by design: encode requires modulo-85 / divide-by-85 on
 * 32-bit words and decode requires multiply-by-85 accumulation. Neither
 * vectorises usefully — see the rationale comment in _base85.h.
 */

const char B85_ENCODE_LUT[85] = {
    '0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
    'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J',
    'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T',
    'U', 'V', 'W', 'X', 'Y', 'Z', 'a', 'b', 'c', 'd',
    'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n',
    'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x',
    'y', 'z', '!', '#', '$', '%', '&', '(', ')', '*',
    '+', '-', ';', '<', '=', '>', '?', '@', '^', '_',
    '`', '{', '|', '}', '~'
};

const uint8_t B85_DECODE_LUT[256] = {
    255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    255,  62, 255,  63,  64,  65,  66, 255,  67,  68,  69,  70, 255,  71, 255, 255,
      0,   1,   2,   3,   4,   5,   6,   7,   8,   9, 255,  72,  73,  74,  75,  76,
     77,  10,  11,  12,  13,  14,  15,  16,  17,  18,  19,  20,  21,  22,  23,  24,
     25,  26,  27,  28,  29,  30,  31,  32,  33,  34,  35, 255, 255, 255,  78,  79,
     80,  36,  37,  38,  39,  40,  41,  42,  43,  44,  45,  46,  47,  48,  49,  50,
     51,  52,  53,  54,  55,  56,  57,  58,  59,  60,  61,  81,  82,  83,  84, 255,
    255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
};

size_t b85_encoded_size(size_t bin_size) {
    return bin_size == 0 ? 0 : (bin_size * 5 + 3) / 4;
}

size_t b85_decoded_size(size_t b85_len) {
    return b85_len / 5 * 4;
}

char* bintob85(char* dest, const void* src, size_t size) {
    const uint8_t* input = (const uint8_t*)src;
    char* output = dest;
    size_t i = 0;

    while (i + 4 <= size) {
        uint32_t w = ((uint32_t)input[i]     << 24) |
                     ((uint32_t)input[i + 1] << 16) |
                     ((uint32_t)input[i + 2] <<  8) |
                      (uint32_t)input[i + 3];

        output[4] = B85_ENCODE_LUT[w % 85]; w /= 85;
        output[3] = B85_ENCODE_LUT[w % 85]; w /= 85;
        output[2] = B85_ENCODE_LUT[w % 85]; w /= 85;
        output[1] = B85_ENCODE_LUT[w % 85]; w /= 85;
        output[0] = B85_ENCODE_LUT[w % 85];

        output += 5;
        i += 4;
    }

    size_t remaining = size - i;
    if (remaining > 0) {
        uint8_t padded[4] = {0, 0, 0, 0};
        memcpy(padded, input + i, remaining);

        uint32_t w = ((uint32_t)padded[0] << 24) |
                     ((uint32_t)padded[1] << 16) |
                     ((uint32_t)padded[2] <<  8) |
                      (uint32_t)padded[3];

        output[4] = B85_ENCODE_LUT[w % 85]; w /= 85;
        output[3] = B85_ENCODE_LUT[w % 85]; w /= 85;
        output[2] = B85_ENCODE_LUT[w % 85]; w /= 85;
        output[1] = B85_ENCODE_LUT[w % 85]; w /= 85;
        output[0] = B85_ENCODE_LUT[w % 85];

        output += remaining + 1;
    }

    return output;
}

void* b85tobin_len(void* dest, const char* src, size_t len) {
    uint8_t* output = (uint8_t*)dest;
    size_t i = 0;

    while (i + 5 <= len) {
        uint32_t acc = 0;
        for (int j = 0; j < 5; j++) {
            uint8_t c = B85_DECODE_LUT[(uint8_t)src[i + j]];
            if (c == 255) return output;
            acc = acc * 85 + c;
        }

        output[0] = (acc >> 24) & 0xFF;
        output[1] = (acc >> 16) & 0xFF;
        output[2] = (acc >>  8) & 0xFF;
        output[3] = acc & 0xFF;

        output += 4;
        i += 5;
    }

    if (i < len) {
        uint8_t chunk[5];
        size_t remainder = len - i;

        memcpy(chunk, src + i, remainder);
        memset(chunk + remainder, '~', 5 - remainder);

        uint32_t acc = 0;
        for (int j = 0; j < 5; j++) {
            uint8_t c = B85_DECODE_LUT[chunk[j]];
            if (c == 255) return output;
            acc = acc * 85 + c;
        }

        if (remainder > 1) {
            for (size_t j = 0; j < remainder - 1; j++) {
                output[j] = (acc >> (24 - 8 * j)) & 0xFF;
            }
            output += remainder - 1;
        }
    }

    return output;
}

void* b85tobin(void* dest, const char* src) {
    const char* p = src;
    while (*p && *p != '~') p++;
    return b85tobin_len(dest, src, p - src);
}
