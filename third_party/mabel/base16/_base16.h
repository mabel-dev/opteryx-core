#ifndef BASE16_H
#define BASE16_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

extern const uint8_t B16_DECODE_LUT[256];
extern const char B16_ENCODE_LUT[16];     /* "0123456789ABCDEF" — uppercase */
extern const char B16_ENCODE_LUT_LC[16];  /* "0123456789abcdef" — lowercase */

void* b16tobin(void* restrict dest, const char* restrict src);
void* b16tobin_len(void* restrict dest, const char* restrict src, size_t len);
char* bintob16(char* restrict dest, const void* restrict src, size_t size);

/*
 * Lowercase hex encode. Same contract as bintob16 (including the trailing NUL
 * written past the returned pointer) but emits "0..9a..f". Digest consumers
 * (MD5/SHA-*) need lowercase; HEX_ENCODE stays uppercase via bintob16.
 * Both are thin wrappers over bintob16_lut, so the SIMD paths are shared and
 * cannot drift between the two alphabets.
 */
char* bintob16_lower(char* restrict dest, const void* restrict src, size_t size);

/*
 * LUT-parameterized encode. `lut` must point at 16 ASCII chars indexed by
 * nibble value. bintob16 / bintob16_lower are the only intended callers.
 */
char* bintob16_lut(char* restrict dest, const void* restrict src, size_t size,
                   const char* restrict lut);

void* b16tobin_scalar(void* restrict dest, const char* restrict src, size_t len);
void* b16tobin_neon(void* restrict dest, const char* restrict src, size_t len);
void* b16tobin_avx2(void* restrict dest, const char* restrict src, size_t len);
void* b16tobin_rvv(void* restrict dest, const char* restrict src, size_t len);

char* bintob16_scalar(char* restrict dest, const void* restrict src, size_t size);
char* bintob16_neon(char* restrict dest, const void* restrict src, size_t size);
char* bintob16_avx2(char* restrict dest, const void* restrict src, size_t size);
char* bintob16_rvv(char* restrict dest, const void* restrict src, size_t size);

/* Per-arch LUT-parameterized cores; the no-LUT forms above wrap these with
 * B16_ENCODE_LUT. Each falls back to the scalar core below its size threshold. */
char* bintob16_scalar_lut(char* restrict dest, const void* restrict src, size_t size,
                          const char* restrict lut);
char* bintob16_neon_lut(char* restrict dest, const void* restrict src, size_t size,
                        const char* restrict lut);
char* bintob16_avx2_lut(char* restrict dest, const void* restrict src, size_t size,
                        const char* restrict lut);
char* bintob16_rvv_lut(char* restrict dest, const void* restrict src, size_t size,
                       const char* restrict lut);

size_t b16_encoded_size(size_t bin_size);
size_t b16_decoded_size(size_t b16_len);

#ifdef __cplusplus
}
#endif

#endif /* BASE16_H */
