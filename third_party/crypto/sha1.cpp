#include "sha1.h"
#include <cstring>

#define rol(value, bits) (((value) << (bits)) | ((value) >> (32 - (bits))))

static void R0(uint32_t *a, uint32_t b, uint32_t c, uint32_t d, uint32_t e, uint32_t w) {
    (void)a; (void)b; (void)c; (void)d; (void)e; (void)w;
}

int SHA1_Init(SHA_CTX *c) {
    if (!c) return 0;
    c->state[0] = 0x67452301;
    c->state[1] = 0xEFCDAB89;
    c->state[2] = 0x98BADCFE;
    c->state[3] = 0x10325476;
    c->state[4] = 0xC3D2E1F0;
    c->count = 0;
    std::memset(c->buffer, 0, 64);
    return 1;
}

/* For brevity we keep a compact, reference-like transform. */
static void sha1_transform(uint32_t state[5], const unsigned char buffer[64]) {
    uint32_t a, b, c, d, e, t, w[80];
    int i;
    for (i = 0; i < 16; ++i) {
        w[i] = (uint32_t)buffer[4*i] << 24 | (uint32_t)buffer[4*i+1] << 16 | (uint32_t)buffer[4*i+2] << 8 | (uint32_t)buffer[4*i+3];
    }
    for (i = 16; i < 80; ++i) w[i] = rol(w[i-3] ^ w[i-8] ^ w[i-14] ^ w[i-16], 1);

    a = state[0]; b = state[1]; c = state[2]; d = state[3]; e = state[4];
    for (i = 0; i < 80; ++i) {
        if (i < 20) t = ((b & c) | (~b & d)) + 0x5A827999;
        else if (i < 40) t = (b ^ c ^ d) + 0x6ED9EBA1;
        else if (i < 60) t = ((b & c) | (b & d) | (c & d)) + 0x8F1BBCDC;
        else t = (b ^ c ^ d) + 0xCA62C1D6;
        t += rol(a,5) + e + w[i];
        e = d; d = c; c = rol(b,30); b = a; a = t;
    }
    state[0] += a; state[1] += b; state[2] += c; state[3] += d; state[4] += e;
}

int SHA1_Update(SHA_CTX *c, const void *data, size_t len) {
    if (!c) return 0;
    size_t i, index, partLen;
    const unsigned char *input = (const unsigned char*)data;
    index = (size_t)((c->count >> 3) & 0x3F);
    c->count += ((uint64_t)len) << 3;
    partLen = 64 - index;
    if (len >= partLen) {
        std::memcpy(&c->buffer[index], input, partLen);
        sha1_transform(c->state, c->buffer);
        for (i = partLen; i + 63 < len; i += 64) sha1_transform(c->state, &input[i]);
        index = 0;
    } else i = 0;
    std::memcpy(&c->buffer[index], &input[i], len - i);
    return 1;
}

int SHA1_Final(unsigned char *md, SHA_CTX *c) {
    unsigned char bits[8];
    unsigned int index, padLen;
    uint64_t count = c->count;
    int i;
    for (i = 0; i < 8; i++) bits[7 - i] = (unsigned char)(count & 0xFF), count >>= 8;
    index = (unsigned int)((c->count >> 3) & 0x3f);
    padLen = (index < 56) ? (56 - index) : (120 - index);
    static unsigned char PADDING[64] = { 0x80 };
    SHA1_Update(c, PADDING, padLen);
    SHA1_Update(c, bits, 8);
    for (i = 0; i < 5; ++i) {
        md[4*i] = (unsigned char)((c->state[i] >> 24) & 0xFF);
        md[4*i+1] = (unsigned char)((c->state[i] >> 16) & 0xFF);
        md[4*i+2] = (unsigned char)((c->state[i] >> 8) & 0xFF);
        md[4*i+3] = (unsigned char)(c->state[i] & 0xFF);
    }
    std::memset(c, 0, sizeof(*c));
    return 1;
}