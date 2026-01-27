/* Public-domain MD5 implementation header (RFC 1321 style) */
#ifndef OPTERYX_MD5_H
#define OPTERYX_MD5_H

#ifdef __cplusplus
extern "C" {
#endif

#include <stdint.h>
#include <stddef.h>

typedef struct {
    uint32_t state[4];
    uint64_t count; /* number of bits, modulo 2^64 */
    unsigned char buffer[64];
} MD5_CTX;

int MD5_Init(MD5_CTX *c);
int MD5_Update(MD5_CTX *c, const void *data, size_t len);
int MD5_Final(unsigned char *md, MD5_CTX *c);

#ifdef __cplusplus
}
#endif

#endif /* OPTERYX_MD5_H */