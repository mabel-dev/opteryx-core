#ifndef BASE64_H
#define BASE64_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
  #define B64_RESTRICT __restrict__
extern "C" {
#else
  #define B64_RESTRICT restrict
#endif

extern const uint8_t B64_DECODE_LUT[256];
extern const char B64_ENCODE_LUT[64];

void* b64tobin(void* B64_RESTRICT dest, const char* B64_RESTRICT src);
void* b64tobin_len(void* B64_RESTRICT dest, const char* B64_RESTRICT src, size_t len);
char* bintob64(char* B64_RESTRICT dest, const void* B64_RESTRICT src, size_t size);

void* b64tobin_scalar(void* B64_RESTRICT dest, const char* B64_RESTRICT src, size_t len);
void* b64tobin_neon(void* B64_RESTRICT dest, const char* B64_RESTRICT src, size_t len);
void* b64tobin_avx2(void* B64_RESTRICT dest, const char* B64_RESTRICT src, size_t len);

char* bintob64_scalar(char* B64_RESTRICT dest, const void* B64_RESTRICT src, size_t size);
char* bintob64_neon(char* B64_RESTRICT dest, const void* B64_RESTRICT src, size_t size);
char* bintob64_avx2(char* B64_RESTRICT dest, const void* B64_RESTRICT src, size_t size);

size_t b64_encoded_size(size_t bin_size);
size_t b64_decoded_size(size_t b64_len);

#ifdef __cplusplus
}
#endif

#endif /* BASE64_H */
