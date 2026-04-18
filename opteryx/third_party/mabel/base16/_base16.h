#ifndef BASE16_H
#define BASE16_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

// Lookup tables - all entries initialized to 255 (invalid marker) except valid hex chars
extern const uint8_t B16_DECODE_LUT[256];
extern const char B16_ENCODE_LUT[16];

// Basic functions (with auto-dispatch)
void* b16tobin(void* restrict dest, const char* restrict src);
void* b16tobin_len(void* restrict dest, const char* restrict src, size_t len);
char* bintob16(char* restrict dest, const void* restrict src, size_t size);

// Optimized versions (for direct use if needed)
void* b16tobin_scalar(void* restrict dest, const char* restrict src, size_t len);
void* b16tobin_neon(void* restrict dest, const char* restrict src, size_t len);
void* b16tobin_avx2(void* restrict dest, const char* restrict src, size_t len);

char* bintob16_scalar(char* restrict dest, const void* restrict src, size_t size);
char* bintob16_neon(char* restrict dest, const void* restrict src, size_t size);
char* bintob16_avx2(char* restrict dest, const void* restrict src, size_t size);

// Utility functions
size_t b16_encoded_size(size_t bin_size);
size_t b16_decoded_size(size_t b16_len);

// CPU feature detection
typedef struct {
    int neon;
    int avx2;
    int avx512;
} b16_cpu_features;

b16_cpu_features b16_detect_cpu_features(void);
void b16_force_scalar(void);   // Force scalar implementation
int b16_has_neon(void);   // Check if NEON is available
int b16_has_avx2(void);   // Check if AVX2 is available
int b16_has_avx512(void); // Check if AVX512 is available

#ifdef __cplusplus
}
#endif

#endif /* BASE16_H */