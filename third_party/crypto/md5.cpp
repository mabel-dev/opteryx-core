/* Minimal public-domain MD5 implementation (RFC 1321 reference inspired)
   Sufficient for non-cryptographic hashing purposes in this project.
*/

#include "md5.h"
#include <cstring>

/* Constants for MD5Transform routine. */
#define S11 7
#define S12 12
#define S13 17
#define S14 22
#define S21 5
#define S22 9
#define S23 14
#define S24 20
#define S31 4
#define S32 11
#define S33 16
#define S34 23
#define S41 6
#define S42 10
#define S43 15
#define S44 21

static void MD5Transform(uint32_t state[4], const unsigned char block[64]);
static void Encode(unsigned char *output, const uint32_t *input, size_t len);
static void Decode(uint32_t *output, const unsigned char *input, size_t len);

int MD5_Init(MD5_CTX *context) {
    if (!context) return 0;
    context->count = 0;
    context->state[0] = 0x67452301;
    context->state[1] = 0xEFCDAB89;
    context->state[2] = 0x98BADCFE;
    context->state[3] = 0x10325476;
    std::memset(context->buffer, 0, 64);
    return 1;
}

int MD5_Update(MD5_CTX *context, const void *input, size_t inputLen) {
    size_t i, index, partLen;
    const unsigned char *buf = (const unsigned char *)input;

    if (!context) return 0;

    index = (size_t)((context->count / 8) % 64);
    context->count += ((uint64_t)inputLen << 3);

    partLen = 64 - index;

    if (inputLen >= partLen) {
        std::memcpy(&context->buffer[index], buf, partLen);
        MD5Transform(context->state, context->buffer);

        for (i = partLen; i + 63 < inputLen; i += 64)
            MD5Transform(context->state, &buf[i]);

        index = 0;
    } else {
        i = 0;
    }

    std::memcpy(&context->buffer[index], &buf[i], inputLen - i);
    return 1;
}

int MD5_Final(unsigned char digest[16], MD5_CTX *context) {
    unsigned char bits[8];
    size_t index, padLen;
    static unsigned char PADDING[64] = { 0x80 };

    if (!context || !digest) return 0;

    Encode(bits, (uint32_t *)&context->count, 8);

    index = (size_t)((context->count / 8) % 64);
    padLen = (index < 56) ? (56 - index) : (120 - index);
    MD5_Update(context, PADDING, padLen);

    MD5_Update(context, bits, 8);

    Encode(digest, context->state, 16);

    /* Zeroize sensitive information. */
    std::memset(context, 0, sizeof(*context));
    return 1;
}

/* Basic MD5 functions and macros */
#define F(x, y, z) (((x) & (y)) | ((~(x)) & (z)))
#define G(x, y, z) (((x) & (z)) | ((y) & (~(z))))
#define H(x, y, z) ((x) ^ (y) ^ (z))
#define I(x, y, z) ((y) ^ ((x) | (~(z))))

#define ROTATE_LEFT(x, n) (((x) << (n)) | ((x) >> (32 - (n))))

#define FF(a, b, c, d, x, s, ac) { \
    (a) += F((b),(c),(d)) + (x) + (uint32_t)(ac); \
    (a) = ROTATE_LEFT((a),(s)); \
    (a) += (b); \
}
#define GG(a, b, c, d, x, s, ac) { \
    (a) += G((b),(c),(d)) + (x) + (uint32_t)(ac); \
    (a) = ROTATE_LEFT((a),(s)); \
    (a) += (b); \
}
#define HH(a, b, c, d, x, s, ac) { \
    (a) += H((b),(c),(d)) + (x) + (uint32_t)(ac); \
    (a) = ROTATE_LEFT((a),(s)); \
    (a) += (b); \
}
#define II(a, b, c, d, x, s, ac) { \
    (a) += I((b),(c),(d)) + (x) + (uint32_t)(ac); \
    (a) = ROTATE_LEFT((a),(s)); \
    (a) += (b); \
}

static void MD5Transform(uint32_t state[4], const unsigned char block[64]) {
    uint32_t a = state[0], b = state[1], c = state[2], d = state[3], x[16];
    Decode(x, block, 64);

    /* Round 1 */
    FF (a, b, c, d, x[ 0], S11, 0xd76aa478);
    FF (d, a, b, c, x[ 1], S12, 0xe8c7b756);
    FF (c, d, a, b, x[ 2], S13, 0x242070db);
    FF (b, c, d, a, x[ 3], S14, 0xc1bdceee);
    FF (a, b, c, d, x[ 4], S11, 0xf57c0faf);
    FF (d, a, b, c, x[ 5], S12, 0x4787c62a);
    FF (c, d, a, b, x[ 6], S13, 0xa8304613);
    FF (b, c, d, a, x[ 7], S14, 0xfd469501);
    FF (a, b, c, d, x[ 8], S11, 0x698098d8);
    FF (d, a, b, c, x[ 9], S12, 0x8b44f7af);
    FF (c, d, a, b, x[10], S13, 0xffff5bb1);
    FF (b, c, d, a, x[11], S14, 0x895cd7be);
    FF (a, b, c, d, x[12], S11, 0x6b901122);
    FF (d, a, b, c, x[13], S12, 0xfd987193);
    FF (c, d, a, b, x[14], S13, 0xa679438e);
    FF (b, c, d, a, x[15], S14, 0x49b40821);

    /* Round 2 */
    GG (a, b, c, d, x[ 1], S21, 0xf61e2562);
    GG (d, a, b, c, x[ 6], S22, 0xc040b340);
    GG (c, d, a, b, x[11], S23, 0x265e5a51);
    GG (b, c, d, a, x[ 0], S24, 0xe9b6c7aa);
    GG (a, b, c, d, x[ 5], S21, 0xd62f105d);
    GG (d, a, b, c, x[10], S22,  0x2441453);
    GG (c, d, a, b, x[15], S23, 0xd8a1e681);
    GG (b, c, d, a, x[ 4], S24, 0xe7d3fbc8);
    GG (a, b, c, d, x[ 9], S21, 0x21e1cde6);
    GG (d, a, b, c, x[14], S22, 0xc33707d6);
    GG (c, d, a, b, x[ 3], S23, 0xf4d50d87);
    GG (b, c, d, a, x[ 8], S24, 0x455a14ed);
    GG (a, b, c, d, x[13], S21, 0xa9e3e905);
    GG (d, a, b, c, x[ 2], S22, 0xfcefa3f8);
    GG (c, d, a, b, x[ 7], S23, 0x676f02d9);
    GG (b, c, d, a, x[12], S24, 0x8d2a4c8a);

    /* Round 3 */
    HH (a, b, c, d, x[ 5], S31, 0xfffa3942);
    HH (d, a, b, c, x[ 8], S32, 0x8771f681);
    HH (c, d, a, b, x[11], S33, 0x6d9d6122);
    HH (b, c, d, a, x[14], S34, 0xfde5380c);
    HH (a, b, c, d, x[ 1], S31, 0xa4beea44);
    HH (d, a, b, c, x[ 4], S32, 0x4bdecfa9);
    HH (c, d, a, b, x[ 7], S33, 0xf6bb4b60);
    HH (b, c, d, a, x[10], S34, 0xbebfbc70);
    HH (a, b, c, d, x[13], S31, 0x289b7ec6);
    HH (d, a, b, c, x[ 0], S32, 0xeaa127fa);
    HH (c, d, a, b, x[ 3], S33, 0xd4ef3085);
    HH (b, c, d, a, x[ 6], S34,  0x4881d05);
}

/* Helper: encode/decode between uint32_t arrays and byte arrays */
static void Encode(unsigned char *output, const uint32_t *input, size_t len) {
    size_t i, j;
    for (i = 0, j = 0; j < len; i++, j += 4) {
        output[j]   = (unsigned char)(input[i] & 0xFF);
        output[j+1] = (unsigned char)((input[i] >> 8) & 0xFF);
        output[j+2] = (unsigned char)((input[i] >> 16) & 0xFF);
        output[j+3] = (unsigned char)((input[i] >> 24) & 0xFF);
    }
}

static void Decode(uint32_t *output, const unsigned char *input, size_t len) {
    size_t i, j;
    for (i = 0, j = 0; j < len; i++, j += 4) {
        output[i] = ((uint32_t)input[j]) |
                    ((uint32_t)input[j+1] << 8) |
                    ((uint32_t)input[j+2] << 16) |
                    ((uint32_t)input[j+3] << 24);
    }
}
