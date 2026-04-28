/*
 * NEON base16 (hex) encode/decode.
 *
 * Encode: 16 input bytes -> 32 output chars per iteration (uppercase).
 * Decode: 32 input chars -> 16 output bytes per iteration. Mixed case.
 *         Returns NULL on any non-hex byte or odd length.
 */
#include "_base16.h"

#if defined(__ARM_NEON) || defined(__aarch64__)
#include <arm_neon.h>

char* bintob16_neon(char* restrict dest, const void* restrict src, size_t size) {
    if (size < 16) {
        return bintob16_scalar(dest, src, size);
    }

    const uint8_t* in = (const uint8_t*)src;
    uint8_t* out = (uint8_t*)dest;

    const uint8x16_t lut    = vld1q_u8((const uint8_t*)B16_ENCODE_LUT);
    const uint8x16_t mask0f = vdupq_n_u8(0x0F);

    while (size >= 16) {
        uint8x16_t v        = vld1q_u8(in);
        uint8x16_t hi       = vshrq_n_u8(v, 4);
        uint8x16_t lo       = vandq_u8(v, mask0f);
        uint8x16_t hi_ascii = vqtbl1q_u8(lut, hi);
        uint8x16_t lo_ascii = vqtbl1q_u8(lut, lo);

        /* zip interleaves: result.val[0] = [h0,l0,h1,l1,...,h7,l7]
         *                  result.val[1] = [h8,l8,...,h15,l15]    */
        uint8x16x2_t zipped = vzipq_u8(hi_ascii, lo_ascii);
        vst1q_u8(out,      zipped.val[0]);
        vst1q_u8(out + 16, zipped.val[1]);

        in   += 16;
        out  += 32;
        size -= 16;
    }

    return bintob16_scalar((char*)out, in, size);
}

void* b16tobin_neon(void* restrict dest, const char* restrict src, size_t len) {
    if (len < 32 || (len & 1) != 0) {
        return b16tobin_scalar(dest, src, len);
    }

    uint8_t* out = (uint8_t*)dest;
    const uint8_t* in = (const uint8_t*)src;

    const uint8x16_t mask0f = vdupq_n_u8(0x0F);
    const uint8x16_t nine   = vdupq_n_u8(9);

    while (len >= 32) {
        uint8x16_t v0 = vld1q_u8(in);
        uint8x16_t v1 = vld1q_u8(in + 16);

        /* Validity: byte must be in '0'..'9' or 'A'..'F' or 'a'..'f'. */
        uint8x16_t is_digit_0 = vandq_u8(vcgeq_u8(v0, vdupq_n_u8('0')), vcleq_u8(v0, vdupq_n_u8('9')));
        uint8x16_t is_upper_0 = vandq_u8(vcgeq_u8(v0, vdupq_n_u8('A')), vcleq_u8(v0, vdupq_n_u8('F')));
        uint8x16_t is_lower_0 = vandq_u8(vcgeq_u8(v0, vdupq_n_u8('a')), vcleq_u8(v0, vdupq_n_u8('f')));
        uint8x16_t valid_0    = vorrq_u8(is_digit_0, vorrq_u8(is_upper_0, is_lower_0));

        uint8x16_t is_digit_1 = vandq_u8(vcgeq_u8(v1, vdupq_n_u8('0')), vcleq_u8(v1, vdupq_n_u8('9')));
        uint8x16_t is_upper_1 = vandq_u8(vcgeq_u8(v1, vdupq_n_u8('A')), vcleq_u8(v1, vdupq_n_u8('F')));
        uint8x16_t is_lower_1 = vandq_u8(vcgeq_u8(v1, vdupq_n_u8('a')), vcleq_u8(v1, vdupq_n_u8('f')));
        uint8x16_t valid_1    = vorrq_u8(is_digit_1, vorrq_u8(is_upper_1, is_lower_1));

        if (vminvq_u8(valid_0) == 0 || vminvq_u8(valid_1) == 0) {
            return NULL;
        }

        /* Nibble = (c & 0x0F) + (c > '9' ? 9 : 0). */
        uint8x16_t alpha_0 = vcgtq_u8(v0, vdupq_n_u8('9'));
        uint8x16_t alpha_1 = vcgtq_u8(v1, vdupq_n_u8('9'));
        uint8x16_t n0 = vaddq_u8(vandq_u8(v0, mask0f), vandq_u8(alpha_0, nine));
        uint8x16_t n1 = vaddq_u8(vandq_u8(v1, mask0f), vandq_u8(alpha_1, nine));

        /*
         * Pack pairs of nibbles into bytes. Reinterpret as u16x8: lane i
         * holds (n[2i+1] << 8) | n[2i] in little-endian. The high nibble
         * of the output byte is n[2i] (first char of the hex pair) and
         * the low nibble is n[2i+1] (second char).
         */
        uint16x8_t p0 = vreinterpretq_u16_u8(n0);
        uint16x8_t p1 = vreinterpretq_u16_u8(n1);
        uint16x8_t hi0 = vandq_u16(p0, vdupq_n_u16(0x00FF));
        uint16x8_t lo0 = vshrq_n_u16(p0, 8);
        uint16x8_t hi1 = vandq_u16(p1, vdupq_n_u16(0x00FF));
        uint16x8_t lo1 = vshrq_n_u16(p1, 8);
        uint16x8_t b0  = vorrq_u16(vshlq_n_u16(hi0, 4), lo0);
        uint16x8_t b1  = vorrq_u16(vshlq_n_u16(hi1, 4), lo1);

        uint8x8_t  r0 = vmovn_u16(b0);
        uint8x8_t  r1 = vmovn_u16(b1);
        vst1q_u8(out, vcombine_u8(r0, r1));

        in  += 32;
        out += 16;
        len -= 32;
    }

    if (len > 0) {
        return b16tobin_scalar(out, (const char*)in, len);
    }
    return out;
}

#else
char* bintob16_neon(char* restrict dest, const void* restrict src, size_t size) {
    return bintob16_scalar(dest, src, size);
}
void* b16tobin_neon(void* restrict dest, const char* restrict src, size_t len) {
    return b16tobin_scalar(dest, src, len);
}
#endif
