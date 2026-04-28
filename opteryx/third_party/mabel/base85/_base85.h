#ifndef BASE85_H
#define BASE85_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/*
 * base85 (Mercurial alphabet, matches Python's base64.b85encode/b85decode).
 *
 * Scalar-only by design: per-block encode/decode requires modulo-85 and
 * divide-by-85 on 32-bit words. SIMD has no efficient lane-parallel
 * modular reduction by a non-power-of-two constant; any "vectorised"
 * implementation collapses back to per-lane scalar work plus shuffle
 * overhead, yielding worse throughput than a tight scalar loop with
 * good ILP. Do not add NEON/AVX2 entrypoints here.
 */

extern const char B85_ENCODE_LUT[85];
extern const uint8_t B85_DECODE_LUT[256];

void* b85tobin(void* dest, const char* src);
void* b85tobin_len(void* dest, const char* src, size_t len);
char* bintob85(char* dest, const void* src, size_t size);

size_t b85_encoded_size(size_t bin_size);
size_t b85_decoded_size(size_t b85_len);

#ifdef __cplusplus
}
#endif

#endif /* BASE85_H */
