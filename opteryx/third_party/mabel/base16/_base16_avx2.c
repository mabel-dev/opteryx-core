#include "_base16.h"

#ifdef __AVX2__
#include <immintrin.h>
#include <string.h>

// AVX2-accelerated encode (vectorized using 128-bit lanes). Decode falls back to scalar.

void* b16tobin_avx2(void* restrict dest, const char* restrict src, size_t len) {
    // Decoding to bytes from hex - keep scalar implementation for now
    return b16tobin_scalar(dest, src, len);
}

char* bintob16_avx2(char* restrict dest, const void* restrict src, size_t size) {
    if (size < 16) {
        return bintob16_scalar(dest, src, size);
    }

    const uint8_t* in = (const uint8_t*)src;
    const uint8_t* end = in + size;
    char* out = dest;

    // Process 16 input bytes -> 32 output chars per loop using 128-bit ops
    while (end - in >= 16) {
        __m128i v = _mm_loadu_si128((const __m128i*)in);

        // Extract high nibbles: (v >> 4) & 0x0F
        __m128i v_shr4 = _mm_srli_epi16(v, 4);
        __m128i mask0f = _mm_set1_epi8(0x0F);
        __m128i hi = _mm_and_si128(v_shr4, mask0f);

        // Low nibbles
        __m128i lo = _mm_and_si128(v, mask0f);

        // Map nibble -> ASCII. ascii = nibble + '0'; if nibble > 9 add 7 (so 10 -> 'A')
        const __m128i ascii_zero = _mm_set1_epi8('0');
        const __m128i ten = _mm_set1_epi8(9);
        const __m128i add7 = _mm_set1_epi8(7);

        __m128i hi_ascii = _mm_add_epi8(hi, ascii_zero);
        __m128i lo_ascii = _mm_add_epi8(lo, ascii_zero);

        __m128i hi_mask = _mm_cmpgt_epi8(hi, ten); // hi > 9 ? 0xFF : 0x00
        __m128i lo_mask = _mm_cmpgt_epi8(lo, ten);

        __m128i hi_add = _mm_and_si128(hi_mask, add7);
        __m128i lo_add = _mm_and_si128(lo_mask, add7);

        hi_ascii = _mm_add_epi8(hi_ascii, hi_add);
        lo_ascii = _mm_add_epi8(lo_ascii, lo_add);

        // Interleave hi_ascii and lo_ascii bytes: produce [hi0,lo0,hi1,lo1,...]
        __m128i out_lo = _mm_unpacklo_epi8(hi_ascii, lo_ascii); // bytes 0..7 interleaved
        __m128i out_hi = _mm_unpackhi_epi8(hi_ascii, lo_ascii); // bytes 8..15 interleaved

        // Store 32 bytes
        _mm_storeu_si128((__m128i*)out, out_lo);
        _mm_storeu_si128((__m128i*)(out + 16), out_hi);

        in += 16;
        out += 32;
    }

    // Tail
    if (end > in) {
        out = bintob16_scalar(out, in, end - in);
    }

    // Null-terminate like scalar implementation
    *out = '\0';
    return out;
}

#else
// Stub implementations when AVX2 is not available
void* b16tobin_avx2(void* restrict dest, const char* restrict src, size_t len) {
    return b16tobin_scalar(dest, src, len);
}

char* bintob16_avx2(char* restrict dest, const void* restrict src, size_t size) {
    return bintob16_scalar(dest, src, size);
}
#endif
