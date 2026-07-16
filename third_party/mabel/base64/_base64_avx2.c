/*
 * AVX2 base64 encode/decode (Muła/Lemire algorithm).
 *
 * Encode: 24 input bytes -> 32 output base64 chars per iteration.
 * Decode: 32 input chars -> 24 output bytes per iteration.
 *
 * Tail handling defers to the scalar implementation, which also handles
 * '=' padding. The SIMD path therefore reserves the last full quad of
 * input for the scalar so it never needs to validate or emit padding.
 */
#include "_base64.h"

#ifdef __AVX2__
#include <immintrin.h>
#include <string.h>

/*
 * Translate 32 6-bit values (one per byte, range 0..63) to ASCII.
 *
 *   0..25  -> 'A'..'Z' (offset +65)
 *   26..51 -> 'a'..'z' (offset +71)
 *   52..61 -> '0'..'9' (offset -4)
 *   62     -> '+'      (offset -19)
 *   63     -> '/'      (offset -16)
 *
 * Index a 14-entry offset LUT by collapsing the alphabet with saturating
 * subtraction. After `subs_epu8(v, 51)` we have 0 for v in [0..51] and 1..12
 * for v in [52..63]; adding 1 when v > 25 separates [0..25] and [26..51].
 *
 *     v in [0..25]   -> idx 0  -> +65
 *     v in [26..51]  -> idx 1  -> +71
 *     v in [52..61]  -> idx 2..11 -> -4
 *     v == 62        -> idx 12 -> -19
 *     v == 63        -> idx 13 -> -16
 */
static inline __m256i b64_enc_translate(__m256i in) {
    const __m256i lut = _mm256_setr_epi8(
        65, 71, -4, -4, -4, -4, -4, -4, -4, -4, -4, -4, -19, -16, 0, 0,
        65, 71, -4, -4, -4, -4, -4, -4, -4, -4, -4, -4, -19, -16, 0, 0
    );
    __m256i indices = _mm256_subs_epu8(in, _mm256_set1_epi8(51));
    __m256i alpha   = _mm256_cmpgt_epi8(in, _mm256_set1_epi8(25));
    indices         = _mm256_sub_epi8(indices, alpha);  /* +1 when v > 25 */
    return _mm256_add_epi8(in, _mm256_shuffle_epi8(lut, indices));
}

/*
 * Reshuffle 24 input bytes (laid out as bytes 0..11 of lane 0 and bytes 0..11
 * of lane 1) into the 4-byte-per-quad layout the bit manipulation expects.
 * The two lanes use the same in-lane shuffle.
 */
static inline __m256i b64_enc_reshuffle(__m256i in) {
    const __m256i shuf = _mm256_setr_epi8(
        1, 0, 2, 1,  4, 3, 5, 4,  7, 6, 8, 7,  10, 9, 11, 10,
        1, 0, 2, 1,  4, 3, 5, 4,  7, 6, 8, 7,  10, 9, 11, 10
    );
    in = _mm256_shuffle_epi8(in, shuf);

    /* Extract two 6-bit fields from the upper half of each 16-bit lane. */
    const __m256i t0 = _mm256_and_si256(in, _mm256_set1_epi32(0x0fc0fc00));
    const __m256i t1 = _mm256_mulhi_epu16(t0, _mm256_set1_epi32(0x04000040));
    /* And two more from the lower half. */
    const __m256i t2 = _mm256_and_si256(in, _mm256_set1_epi32(0x003f03f0));
    const __m256i t3 = _mm256_mullo_epi16(t2, _mm256_set1_epi32(0x01000010));
    return _mm256_or_si256(t1, t3);
}

char* bintob64_avx2(char* B64_RESTRICT dest, const void* B64_RESTRICT src, size_t size) {
    if (size < 32) {
        return bintob64_scalar(dest, src, size);
    }

    const uint8_t* in = (const uint8_t*)src;
    char* out = dest;

    /*
     * Per iteration: load 32 bytes from `in`, but the encode shuffle wants
     * lane 1 to hold bytes 12..27 of the input. Build that with one extra
     * 16-byte load and inserti128. Each iter consumes 24 bytes.
     */
    while (size >= 32) {
        __m256i v   = _mm256_loadu_si256((const __m256i*)in);
        __m128i hi  = _mm_loadu_si128((const __m128i*)(in + 12));
        __m256i v2  = _mm256_inserti128_si256(v, hi, 1);

        __m256i indices = b64_enc_reshuffle(v2);
        __m256i ascii   = b64_enc_translate(indices);

        _mm256_storeu_si256((__m256i*)out, ascii);

        in   += 24;
        out  += 32;
        size -= 24;
    }

    /* Tail: scalar handles 0..31 remaining bytes (including any padding). */
    out = bintob64_scalar(out, in, size);
    return out;
}

/*
 * Decode 32 base64 chars to 24 bytes per iteration.
 *
 * Validity is checked per chunk via per-byte range tests on five disjoint
 * alphabet ranges; if any byte is outside the alphabet we bail out and let
 * scalar handle the rest (which will return NULL on the same bytes).
 */
static inline __m256i b64_dec_value(__m256i c, __m256i* valid) {
    /* Range masks: cmpgt_epi8 with (lo-1) and (hi+1). */
    __m256i is_upper = _mm256_and_si256(
        _mm256_cmpgt_epi8(c, _mm256_set1_epi8('A' - 1)),
        _mm256_cmpgt_epi8(_mm256_set1_epi8('Z' + 1), c)
    );
    __m256i is_lower = _mm256_and_si256(
        _mm256_cmpgt_epi8(c, _mm256_set1_epi8('a' - 1)),
        _mm256_cmpgt_epi8(_mm256_set1_epi8('z' + 1), c)
    );
    __m256i is_digit = _mm256_and_si256(
        _mm256_cmpgt_epi8(c, _mm256_set1_epi8('0' - 1)),
        _mm256_cmpgt_epi8(_mm256_set1_epi8('9' + 1), c)
    );
    __m256i is_plus  = _mm256_cmpeq_epi8(c, _mm256_set1_epi8('+'));
    __m256i is_slash = _mm256_cmpeq_epi8(c, _mm256_set1_epi8('/'));

    __m256i v_upper = _mm256_sub_epi8(c, _mm256_set1_epi8('A'));
    __m256i v_lower = _mm256_sub_epi8(c, _mm256_set1_epi8('a' - 26));
    __m256i v_digit = _mm256_sub_epi8(c, _mm256_set1_epi8((char)('0' - 52)));
    __m256i v_plus  = _mm256_set1_epi8(62);
    __m256i v_slash = _mm256_set1_epi8(63);

    __m256i value = _mm256_and_si256(is_upper, v_upper);
    value = _mm256_or_si256(value, _mm256_and_si256(is_lower, v_lower));
    value = _mm256_or_si256(value, _mm256_and_si256(is_digit, v_digit));
    value = _mm256_or_si256(value, _mm256_and_si256(is_plus,  v_plus));
    value = _mm256_or_si256(value, _mm256_and_si256(is_slash, v_slash));

    *valid = _mm256_or_si256(
        _mm256_or_si256(is_upper, is_lower),
        _mm256_or_si256(_mm256_or_si256(is_digit, is_plus), is_slash)
    );
    return value;
}

void* b64tobin_avx2(void* B64_RESTRICT dest, const char* B64_RESTRICT src, size_t len) {
    if (len < 32 || (len & 3) != 0) {
        return b64tobin_scalar(dest, src, len);
    }

    uint8_t* out = (uint8_t*)dest;
    const uint8_t* in = (const uint8_t*)src;

    /*
     * Reserve the last full quad for scalar so it can handle '=' padding.
     * That means we only run AVX2 while at least 36 chars remain.
     */
    while (len >= 36) {
        __m256i chunk = _mm256_loadu_si256((const __m256i*)in);

        __m256i valid;
        __m256i values = b64_dec_value(chunk, &valid);

        /* If any byte is invalid, bail and let scalar reproduce the error. */
        if ((uint32_t)_mm256_movemask_epi8(valid) != 0xFFFFFFFFu) {
            break;
        }

        /*
         * Pack 4 6-bit values into 3 bytes per quad.
         * After maddubs with [0x40, 0x01]: each 16-bit lane = (v0<<6)|v1.
         * After madd_epi16 with [0x1000, 0x0001]: each 32-bit lane =
         *   (v0<<18)|(v1<<12)|(v2<<6)|v3, i.e. 24 valid bits in low 3 bytes,
         *   ordered as [out2, out1, out0] in little-endian.
         */
        __m256i merged_pairs = _mm256_maddubs_epi16(
            values, _mm256_set1_epi32(0x01400140));
        __m256i merged_quads = _mm256_madd_epi16(
            merged_pairs, _mm256_set1_epi32(0x00011000));

        /*
         * Reorder each 4-byte group from [out2, out1, out0, 0] to
         * [out0, out1, out2, _]. The 0xFF slots are clobbered by the
         * permute step below.
         */
        const __m256i shuf = _mm256_setr_epi8(
            2, 1, 0,  6, 5, 4,  10, 9, 8,  14, 13, 12,  -1, -1, -1, -1,
            2, 1, 0,  6, 5, 4,  10, 9, 8,  14, 13, 12,  -1, -1, -1, -1
        );
        __m256i ordered = _mm256_shuffle_epi8(merged_quads, shuf);

        /* Compact the 12-valid + 4-junk pattern of each lane into 24
         * contiguous bytes by selecting the six valid 32-bit lanes. */
        __m256i compact = _mm256_permutevar8x32_epi32(
            ordered, _mm256_setr_epi32(0, 1, 2, 4, 5, 6, 7, 7));

        _mm_storeu_si128((__m128i*)out, _mm256_castsi256_si128(compact));
        __m128i upper = _mm256_extracti128_si256(compact, 1);
        _mm_storel_epi64((__m128i*)(out + 16), upper);

        in  += 32;
        out += 24;
        len -= 32;
    }

    /* Tail: scalar handles whatever's left (always >= 4 chars, mod 4). */
    return b64tobin_scalar(out, (const char*)in, len);
}

#else
void* b64tobin_avx2(void* B64_RESTRICT dest, const char* B64_RESTRICT src, size_t len) {
    return b64tobin_scalar(dest, src, len);
}
char* bintob64_avx2(char* B64_RESTRICT dest, const void* B64_RESTRICT src, size_t size) {
    return bintob64_scalar(dest, src, size);
}
#endif
