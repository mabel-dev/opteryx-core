#ifndef OPTERYX_SHA2_H
#define OPTERYX_SHA2_H

#ifdef __cplusplus
extern "C" {
#endif

#include <stdint.h>
#include <stddef.h>

/* SHA-256 context */
typedef struct {
    uint32_t state[8];
    uint64_t count; /* bits */
    unsigned char buffer[64];
} SHA256_CTX;

/* For SHA-512 */
typedef struct {
    uint64_t state[8];
    unsigned long long count; /* bits */
    unsigned char buffer[128];
} SHA512_CTX;

int SHA256_Init(SHA256_CTX *c);
int SHA256_Update(SHA256_CTX *c, const void *data, size_t len);
int SHA256_Final(unsigned char *md, SHA256_CTX *c);

int SHA512_Init(SHA512_CTX *c);
int SHA512_Update(SHA512_CTX *c, const void *data, size_t len);
int SHA512_Final(unsigned char *md, SHA512_CTX *c);

#ifdef __cplusplus
}
#endif

#endif /* OPTERYX_SHA2_H */