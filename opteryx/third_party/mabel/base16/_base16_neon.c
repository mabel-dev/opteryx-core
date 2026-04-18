#include <arm_neon.h>
#include <stdint.h>
#include <string.h>

// Precomputed table: maps ASCII hex char -> 4-bit value, or 0x80 for invalid.
// (0-9, A-F, a-f) -> 0..15, else 0x80.
static const uint8_t kHexLUT[256] = {
    ['0'] = 0,  ['1'] = 1,  ['2'] = 2,  ['3'] = 3,  ['4'] = 4,
    ['5'] = 5,  ['6'] = 6,  ['7'] = 7,  ['8'] = 8,  ['9'] = 9,
    ['A'] = 10, ['B'] = 11, ['C'] = 12, ['D'] = 13, ['E'] = 14, ['F'] = 15,
    ['a'] = 10, ['b'] = 11, ['c'] = 12, ['d'] = 13, ['e'] = 14, ['f'] = 15,
    // all others are 0x80
};

static inline uint8x16_t load_hex_lut() {
    static uint8_t lut[256] __attribute__((aligned(16))) = {0};
    static int init = 0;
    if (!init) {
        memcpy((void*)lut, kHexLUT, sizeof(kHexLUT));
        for (int i = 0; i < 256; i++) if (!kHexLUT[i] && i != '0') ((uint8_t*)lut)[i] = 0x80;
        init = 1;
    }
    return vld1q_u8(lut);
}

void* b16tobin_neon(void* restrict dest, const char* restrict src, size_t len) {
    if (len < 64 || (len & 1)) {
        return b16tobin_scalar(dest, src, len);
    }

    uint8_t* out = (uint8_t*)dest;
    const uint8_t* in = (const uint8_t*)src;
    const uint8_t* end = in + len;

    // Process 16 input chars (8 output bytes) per iteration using comparisons
    while (end - in >= 16) {
        uint8x16_t chunk = vld1q_u8(in);

        // digits: '0'..'9'
        uint8x16_t t0 = vsubq_u8(chunk, vdupq_n_u8('0'));
        uint8x16_t mask0 = vcleq_u8(t0, vdupq_n_u8(9));
        uint8x16_t val0 = t0;

        // upper: 'A'..'F'
        uint8x16_t t1 = vsubq_u8(chunk, vdupq_n_u8('A'));
        uint8x16_t mask1 = vcleq_u8(t1, vdupq_n_u8(5));
        uint8x16_t val1 = vaddq_u8(t1, vdupq_n_u8(10));

        // lower: 'a'..'f'
        uint8x16_t t2 = vsubq_u8(chunk, vdupq_n_u8('a'));
        uint8x16_t mask2 = vcleq_u8(t2, vdupq_n_u8(5));
        uint8x16_t val2 = vaddq_u8(t2, vdupq_n_u8(10));

        // combine values using masks: prefer val0, then val1, otherwise val2
        uint8x16_t tmp = vbslq_u8(mask0, val0, val2);
        uint8x16_t nibbles = vbslq_u8(mask1, val1, tmp);

        // validity: any byte where none of masks are true => invalid
        uint8x16_t valid = vorrq_u8(mask0, vorrq_u8(mask1, mask2));
        uint8x16_t inv = vmvnq_u8(valid);
        if (vmaxvq_u8(inv) != 0) {
            return NULL;
        }

        // reinterpret nibbles as 8 16-bit words: word = (high_byte<<8) | low_byte
        uint16x8_t pairs = vreinterpretq_u16_u8(nibbles);
        // low byte contains first nibble, high byte contains second nibble due to little-endian
        uint16x8_t lo = vandq_u16(pairs, vdupq_n_u16(0x00FF));
        uint16x8_t hi = vshrq_n_u16(pairs, 8);
        // combine: (lo << 4) | hi
        uint16x8_t combined = vorrq_u16(vshlq_n_u16(lo, 4), hi);
        uint8x8_t result = vmovn_u16(combined);
        vst1_u8(out, result);

        in += 16;
        out += 8;
    }

    if (end > in) {
        out = b16tobin_scalar(out, (const char*)in, end - in);
        if (!out) return NULL;
    }
    return out;
}

static const uint8_t kHexEncodeLUT[16] = {
    '0','1','2','3','4','5','6','7','8','9','a','b','c','d','e','f'
};

char* bintob16_neon(char* restrict dest, const void* restrict src, size_t size) {
    if (size < 32) {
        return bintob16_scalar(dest, src, size);
    }

    const uint8_t* in = (const uint8_t*)src;
    const uint8_t* end = in + size;
    uint8_t* out = (uint8_t*)dest;

    const uint8x16_t lut = vld1q_u8(kHexEncodeLUT);

    while (end - in >= 16) {
        uint8x16_t bytes = vld1q_u8(in);

        // High nibbles (shift right 4)
        uint8x16_t hi = vshrq_n_u8(bytes, 4);
        // Low nibbles (mask lower 4 bits)
        uint8x16_t lo = vandq_u8(bytes, vdupq_n_u8(0x0F));

        // Lookup ASCII for high and low nibbles
        uint8x16_t ascii_hi = vqtbl1q_u8(lut, hi);
        uint8x16_t ascii_lo = vqtbl1q_u8(lut, lo);

        // Interleave: hi[0], lo[0], hi[1], lo[1], ...
        uint8x16x2_t interleaved = vzipq_u8(ascii_hi, ascii_lo);
        vst1q_u8(out, interleaved.val[0]);
        vst1q_u8(out + 16, interleaved.val[1]);

        in += 16;
        out += 32;
    }

    if (end > in) {
        out = (uint8_t*)bintob16_scalar((char*)out, in, end - in);
    }

    *out = '\0';
    return (char*)out;
}
