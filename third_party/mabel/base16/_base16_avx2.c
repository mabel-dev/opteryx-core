/*
 * AVX2 base16 (hex) encode/decode.
 *
 * Encode: 32 input bytes -> 64 output chars per iteration. Output is
 * uppercase hex ("0..9", "A..F"), matching the scalar implementation and
 * Python's base64.b16encode.
 *
 * Decode: 32 input chars -> 16 output bytes per iteration. Accepts mixed
 * case ("0..9", "A..F", "a..f"), matching scalar behaviour. Returns NULL
 * on any non-hex byte or odd length.
 */
#include "_base16.h"

#ifdef __AVX2__
#include <immintrin.h>

char* bintob16_avx2_lut(char* restrict dest, const void* restrict src, size_t size,
                        const char* restrict lut) {
    if (size < 32) {
        return bintob16_scalar_lut(dest, src, size, lut);
    }

    const uint8_t* in = (const uint8_t*)src;
    char* out = dest;

    const __m128i lut128 = _mm_loadu_si128((const __m128i*)lut);
    const __m256i lutv   = _mm256_broadcastsi128_si256(lut128);
    const __m256i mask0f = _mm256_set1_epi8(0x0F);

    while (size >= 32) {
        __m256i v        = _mm256_loadu_si256((const __m256i*)in);
        __m256i hi       = _mm256_and_si256(_mm256_srli_epi16(v, 4), mask0f);
        __m256i lo       = _mm256_and_si256(v, mask0f);
        __m256i hi_ascii = _mm256_shuffle_epi8(lutv, hi);
        __m256i lo_ascii = _mm256_shuffle_epi8(lutv, lo);

        /*
         * Per-lane interleave gives, for lane 0:
         *   out_lo = chars for in[0..7]
         *   out_hi = chars for in[8..15]
         * and for lane 1:
         *   out_lo = chars for in[16..23]
         *   out_hi = chars for in[24..31]
         * Permute lanes so the output is contiguous: bytes 0..63 of out
         * cover input bytes 0..31 in order.
         */
        __m256i out_a    = _mm256_unpacklo_epi8(hi_ascii, lo_ascii);
        __m256i out_b    = _mm256_unpackhi_epi8(hi_ascii, lo_ascii);
        __m256i first    = _mm256_permute2x128_si256(out_a, out_b, 0x20);
        __m256i second   = _mm256_permute2x128_si256(out_a, out_b, 0x31);

        _mm256_storeu_si256((__m256i*)out, first);
        _mm256_storeu_si256((__m256i*)(out + 32), second);

        in   += 32;
        out  += 64;
        size -= 32;
    }

    /* Scalar handles 0..31 trailing bytes and the null terminator. */
    return bintob16_scalar_lut(out, in, size, lut);
}

char* bintob16_avx2(char* restrict dest, const void* restrict src, size_t size) {
    return bintob16_avx2_lut(dest, src, size, B16_ENCODE_LUT);
}

void* b16tobin_avx2(void* restrict dest, const char* restrict src, size_t len) {
    if (len < 32 || (len & 1) != 0) {
        return b16tobin_scalar(dest, src, len);
    }

    uint8_t* out = (uint8_t*)dest;
    const uint8_t* in = (const uint8_t*)src;

    while (len >= 32) {
        __m256i v = _mm256_loadu_si256((const __m256i*)in);

        /*
         * Range checks. cmpgt_epi8 is signed, but all valid hex chars are
         * in [0x30..0x66] which is positive, so high-bit input bytes fail
         * every range — exactly the desired "invalid" behaviour.
         */
        __m256i is_digit = _mm256_and_si256(
            _mm256_cmpgt_epi8(v, _mm256_set1_epi8('0' - 1)),
            _mm256_cmpgt_epi8(_mm256_set1_epi8('9' + 1), v)
        );
        __m256i is_upper = _mm256_and_si256(
            _mm256_cmpgt_epi8(v, _mm256_set1_epi8('A' - 1)),
            _mm256_cmpgt_epi8(_mm256_set1_epi8('F' + 1), v)
        );
        __m256i is_lower = _mm256_and_si256(
            _mm256_cmpgt_epi8(v, _mm256_set1_epi8('a' - 1)),
            _mm256_cmpgt_epi8(_mm256_set1_epi8('f' + 1), v)
        );
        __m256i valid = _mm256_or_si256(is_digit, _mm256_or_si256(is_upper, is_lower));
        if ((uint32_t)_mm256_movemask_epi8(valid) != 0xFFFFFFFFu) {
            return NULL;
        }

        /*
         * Nibble = (c & 0x0F) + (c > '9' ? 9 : 0). Works because
         *   '0'..'9' (0x30..0x39): low nibble 0..9, c <= '9' so +0.
         *   'A'..'F' (0x41..0x46): low nibble 1..6, c >  '9' so +9 -> 10..15.
         *   'a'..'f' (0x61..0x66): low nibble 1..6, c >  '9' so +9 -> 10..15.
         * After validation, no other byte reaches this point.
         */
        __m256i lo_nib = _mm256_and_si256(v, _mm256_set1_epi8(0x0F));
        __m256i alpha  = _mm256_cmpgt_epi8(v, _mm256_set1_epi8('9'));
        __m256i nine   = _mm256_and_si256(alpha, _mm256_set1_epi8(9));
        __m256i nibble = _mm256_add_epi8(lo_nib, nine);

        /*
         * Pack pairs (high, low) -> byte. maddubs with [0x10, 0x01, ...]
         * gives 16 16-bit results = (high<<4)|low; packus narrows them to
         * 8-bit. packus operates per 128-bit lane so the result has the
         * 16 valid bytes split between the two lanes — permute4x64 with
         * 0xD8 (lanes 0,2,1,3) compacts them into the low 128 bits.
         */
        __m256i bytes16 = _mm256_maddubs_epi16(nibble, _mm256_set1_epi16(0x0110));
        __m256i packed  = _mm256_packus_epi16(bytes16, _mm256_setzero_si256());
        __m256i result  = _mm256_permute4x64_epi64(packed, 0xD8);

        _mm_storeu_si128((__m128i*)out, _mm256_castsi256_si128(result));

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
char* bintob16_avx2_lut(char* restrict dest, const void* restrict src, size_t size,
                        const char* restrict lut) {
    return bintob16_scalar_lut(dest, src, size, lut);
}
char* bintob16_avx2(char* restrict dest, const void* restrict src, size_t size) {
    return bintob16_scalar(dest, src, size);
}
void* b16tobin_avx2(void* restrict dest, const char* restrict src, size_t len) {
    return b16tobin_scalar(dest, src, len);
}
#endif
