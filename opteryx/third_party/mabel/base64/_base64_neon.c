/*
 * NEON base64 encode/decode using deinterleaving loads and stores.
 *
 * Encode: vld3q_u8 splits 48 input bytes into three 16-byte vectors of
 * "first/second/third byte of each triplet". A 64-entry table lookup via
 * vqtbl4q_u8 maps 6-bit indices to ASCII.
 *
 * Decode: vld4q_u8 splits 64 input chars into four 16-byte vectors of
 * "first/second/third/fourth char of each quad". Range-based lookups
 * convert each char to a 6-bit value (rejecting invalid bytes), and
 * the 4 vectors are bit-packed into 3 vectors that are stored back via
 * vst3q_u8.
 */
#include "_base64.h"

#if defined(__ARM_NEON) || defined(__aarch64__)
#include <arm_neon.h>

static inline uint8x16x4_t b64_alphabet_table(void) {
    static const uint8_t alphabet[64] =
        "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    uint8x16x4_t lut;
    lut.val[0] = vld1q_u8(alphabet);
    lut.val[1] = vld1q_u8(alphabet + 16);
    lut.val[2] = vld1q_u8(alphabet + 32);
    lut.val[3] = vld1q_u8(alphabet + 48);
    return lut;
}

char* bintob64_neon(char* B64_RESTRICT dest, const void* B64_RESTRICT src, size_t size) {
    if (size < 48) {
        return bintob64_scalar(dest, src, size);
    }

    const uint8_t* in = (const uint8_t*)src;
    char* out = dest;

    const uint8x16x4_t lut = b64_alphabet_table();
    const uint8x16_t mask3f = vdupq_n_u8(0x3F);

    while (size >= 48) {
        uint8x16x3_t triplets = vld3q_u8(in);
        const uint8x16_t a = triplets.val[0];
        const uint8x16_t b = triplets.val[1];
        const uint8x16_t c = triplets.val[2];

        /* Four 6-bit indices per triplet (a, b, c). */
        uint8x16_t i0 = vshrq_n_u8(a, 2);
        uint8x16_t i1 = vandq_u8(vorrq_u8(vshlq_n_u8(a, 4), vshrq_n_u8(b, 4)), mask3f);
        uint8x16_t i2 = vandq_u8(vorrq_u8(vshlq_n_u8(b, 2), vshrq_n_u8(c, 6)), mask3f);
        uint8x16_t i3 = vandq_u8(c, mask3f);

        uint8x16x4_t chars;
        chars.val[0] = vqtbl4q_u8(lut, i0);
        chars.val[1] = vqtbl4q_u8(lut, i1);
        chars.val[2] = vqtbl4q_u8(lut, i2);
        chars.val[3] = vqtbl4q_u8(lut, i3);

        vst4q_u8((uint8_t*)out, chars);

        in   += 48;
        out  += 64;
        size -= 48;
    }

    return bintob64_scalar(out, in, size);
}

/*
 * Range-based decode of one 16-char vector. Returns the 6-bit values; sets
 * `*valid` to 0xFF for in-alphabet bytes and 0x00 otherwise.
 */
static inline uint8x16_t b64_dec_chunk(uint8x16_t c, uint8x16_t* valid) {
    uint8x16_t is_upper = vandq_u8(vcgeq_u8(c, vdupq_n_u8('A')), vcleq_u8(c, vdupq_n_u8('Z')));
    uint8x16_t is_lower = vandq_u8(vcgeq_u8(c, vdupq_n_u8('a')), vcleq_u8(c, vdupq_n_u8('z')));
    uint8x16_t is_digit = vandq_u8(vcgeq_u8(c, vdupq_n_u8('0')), vcleq_u8(c, vdupq_n_u8('9')));
    uint8x16_t is_plus  = vceqq_u8(c, vdupq_n_u8('+'));
    uint8x16_t is_slash = vceqq_u8(c, vdupq_n_u8('/'));

    uint8x16_t v_upper = vsubq_u8(c, vdupq_n_u8('A'));
    uint8x16_t v_lower = vsubq_u8(c, vdupq_n_u8('a' - 26));
    uint8x16_t v_digit = vsubq_u8(c, vdupq_n_u8((uint8_t)('0' - 52)));

    uint8x16_t value = vandq_u8(is_upper, v_upper);
    value = vorrq_u8(value, vandq_u8(is_lower, v_lower));
    value = vorrq_u8(value, vandq_u8(is_digit, v_digit));
    value = vorrq_u8(value, vandq_u8(is_plus,  vdupq_n_u8(62)));
    value = vorrq_u8(value, vandq_u8(is_slash, vdupq_n_u8(63)));

    *valid = vorrq_u8(vorrq_u8(is_upper, is_lower),
                      vorrq_u8(vorrq_u8(is_digit, is_plus), is_slash));
    return value;
}

void* b64tobin_neon(void* B64_RESTRICT dest, const char* B64_RESTRICT src, size_t len) {
    if (len < 64 || (len & 3) != 0) {
        return b64tobin_scalar(dest, src, len);
    }

    uint8_t* out = (uint8_t*)dest;
    const uint8_t* in = (const uint8_t*)src;

    /* Reserve last full quad for scalar so it can resolve '=' padding. */
    while (len >= 68) {
        uint8x16x4_t chars = vld4q_u8(in);

        uint8x16_t valid_a, valid_b, valid_c, valid_d;
        uint8x16_t va = b64_dec_chunk(chars.val[0], &valid_a);
        uint8x16_t vb = b64_dec_chunk(chars.val[1], &valid_b);
        uint8x16_t vc = b64_dec_chunk(chars.val[2], &valid_c);
        uint8x16_t vd = b64_dec_chunk(chars.val[3], &valid_d);

        uint8x16_t valid = vandq_u8(vandq_u8(valid_a, valid_b), vandq_u8(valid_c, valid_d));
        if (vminvq_u8(valid) == 0) {
            break;
        }

        uint8x16x3_t out_triplets;
        out_triplets.val[0] = vorrq_u8(vshlq_n_u8(va, 2), vshrq_n_u8(vb, 4));
        out_triplets.val[1] = vorrq_u8(vshlq_n_u8(vb, 4), vshrq_n_u8(vc, 2));
        out_triplets.val[2] = vorrq_u8(vshlq_n_u8(vc, 6), vd);

        vst3q_u8(out, out_triplets);

        in  += 64;
        out += 48;
        len -= 64;
    }

    return b64tobin_scalar(out, (const char*)in, len);
}

#else
char* bintob64_neon(char* B64_RESTRICT dest, const void* B64_RESTRICT src, size_t size) {
    return bintob64_scalar(dest, src, size);
}
void* b64tobin_neon(void* B64_RESTRICT dest, const char* B64_RESTRICT src, size_t len) {
    return b64tobin_scalar(dest, src, len);
}
#endif
