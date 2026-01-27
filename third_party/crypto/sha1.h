#ifndef OPTERYX_SHA1_H
#define OPTERYX_SHA1_H

#ifdef __cplusplus
extern "C" {
#endif

#include <stdint.h>
#include <stddef.h>

typedef struct {
    uint32_t state[5];
    uint64_t count; /* bits */
    unsigned char buffer[64];
} SHA_CTX;

int SHA1_Init(SHA_CTX *c);
int SHA1_Update(SHA_CTX *c, const void *data, size_t len);
int SHA1_Final(unsigned char *md, SHA_CTX *c);

#ifdef __cplusplus
}
#endif

#endif /* OPTERYX_SHA1_H */