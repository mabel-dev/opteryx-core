#ifndef BASE16_H
#define BASE16_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

extern const uint8_t B16_DECODE_LUT[256];
extern const char B16_ENCODE_LUT[16];

void* b16tobin(void* restrict dest, const char* restrict src);
void* b16tobin_len(void* restrict dest, const char* restrict src, size_t len);
char* bintob16(char* restrict dest, const void* restrict src, size_t size);

void* b16tobin_scalar(void* restrict dest, const char* restrict src, size_t len);
void* b16tobin_neon(void* restrict dest, const char* restrict src, size_t len);
void* b16tobin_avx2(void* restrict dest, const char* restrict src, size_t len);
void* b16tobin_rvv(void* restrict dest, const char* restrict src, size_t len);

char* bintob16_scalar(char* restrict dest, const void* restrict src, size_t size);
char* bintob16_neon(char* restrict dest, const void* restrict src, size_t size);
char* bintob16_avx2(char* restrict dest, const void* restrict src, size_t size);
char* bintob16_rvv(char* restrict dest, const void* restrict src, size_t size);

size_t b16_encoded_size(size_t bin_size);
size_t b16_decoded_size(size_t b16_len);

#ifdef __cplusplus
}
#endif

#endif /* BASE16_H */
