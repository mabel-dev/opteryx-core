/*
 * SHA-512 implementation (public-domain / MIT-style)
 * Based on Brad Conte's crypto-algorithms (https://github.com/B-Con/crypto-algorithms)
 * Adapted to provide the OpenSSL-style API expected by the codebase:
 *   typedef struct SHA512_CTX { ... } SHA512_CTX;
 *   int SHA512_Init(SHA512_CTX *c);
 *   int SHA512_Update(SHA512_CTX *c, const void *data, size_t len);
 *   int SHA512_Final(unsigned char *md, SHA512_CTX *c);
 */

#include "sha2.h"
#include <cstring>
#include <cstdint>

/* Hardware acceleration: ARMv8.2 SHA-512 (FEAT_SHA512) crypto extensions when
 * present at runtime, scalar core otherwise. Same dispatch pattern as
 * sha2.cpp/sha1.cpp. The NEON single-block transform below is transcribed from
 * Botan's public ARMv8 implementation (see attribution above sha512_transform_neon)
 * and verified against NIST/hashlib vectors. */
#if defined(__aarch64__)
#include <arm_neon.h>
#if defined(__linux__)
#include <sys/auxv.h>
#include <asm/hwcap.h>
#endif
#endif

/* Constants */
static const uint64_t K512[80] = {
    0x428a2f98d728ae22ULL,0x7137449123ef65cdULL,0xb5c0fbcfec4d3b2fULL,0xe9b5dba58189dbbcULL,
    0x3956c25bf348b538ULL,0x59f111f1b605d019ULL,0x923f82a4af194f9bULL,0xab1c5ed5da6d8118ULL,
    0xd807aa98a3030242ULL,0x12835b0145706fbeULL,0x243185be4ee4b28cULL,0x550c7dc3d5ffb4e2ULL,
    0x72be5d74f27b896fULL,0x80deb1fe3b1696b1ULL,0x9bdc06a725c71235ULL,0xc19bf174cf692694ULL,
    0xe49b69c19ef14ad2ULL,0xefbe4786384f25e3ULL,0x0fc19dc68b8cd5b5ULL,0x240ca1cc77ac9c65ULL,
    0x2de92c6f592b0275ULL,0x4a7484aa6ea6e483ULL,0x5cb0a9dcbd41fbd4ULL,0x76f988da831153b5ULL,
    0x983e5152ee66dfabULL,0xa831c66d2db43210ULL,0xb00327c898fb213fULL,0xbf597fc7beef0ee4ULL,
    0xc6e00bf33da88fc2ULL,0xd5a79147930aa725ULL,0x06ca6351e003826fULL,0x142929670a0e6e70ULL,
    0x27b70a8546d22ffcULL,0x2e1b21385c26c926ULL,0x4d2c6dfc5ac42aedULL,0x53380d139d95b3dfULL,
    0x650a73548baf63deULL,0x766a0abb3c77b2a8ULL,0x81c2c92e47edaee6ULL,0x92722c851482353bULL,
    0xa2bfe8a14cf10364ULL,0xa81a664bbc423001ULL,0xc24b8b70d0f89791ULL,0xc76c51a30654be30ULL,
    0xd192e819d6ef5218ULL,0xd69906245565a910ULL,0xf40e35855771202aULL,0x106aa07032bbd1b8ULL,
    0x19a4c116b8d2d0c8ULL,0x1e376c085141ab53ULL,0x2748774cdf8eeb99ULL,0x34b0bcb5e19b48a8ULL,
    0x391c0cb3c5c95a63ULL,0x4ed8aa4ae3418acbULL,0x5b9cca4f7763e373ULL,0x682e6ff3d6b2b8a3ULL,
    0x748f82ee5defb2fcULL,0x78a5636f43172f60ULL,0x84c87814a1f0ab72ULL,0x8cc702081a6439ecULL,
    0x90befffa23631e28ULL,0xa4506cebde82bde9ULL,0xbef9a3f7b2c67915ULL,0xc67178f2e372532bULL,
    0xca273eceea26619cULL,0xd186b8c721c0c207ULL,0xeada7dd6cde0eb1eULL,0xf57d4f7fee6ed178ULL,
    0x06f067aa72176fbaULL,0x0a637dc5a2c898a6ULL,0x113f9804bef90daeULL,0x1b710b35131c471bULL,
    0x28db77f523047d84ULL,0x32caab7b40c72493ULL,0x3c9ebe0a15c9bebcULL,0x431d67c49c100d4cULL,
    0x4cc5d4becb3e42b6ULL,0x597f299cfc657e2aULL,0x5fcb6fab3ad6faecULL,0x6c44198c4a475817ULL
};

static inline uint64_t rotr64(uint64_t x, unsigned n) { return (x >> n) | (x << (64 - n)); }

extern "C" {

/* Use `SHA512_CTX` as declared in sha2.h; do not redeclare the struct here. */
static void sha512_transform_scalar(uint64_t state[8], const unsigned char block[128]) {
    uint64_t W[80];
    uint64_t a,b,c,d,e,f,g,h,t1,t2;
    int t;
    for (t = 0; t < 16; ++t) {
        W[t] = ((uint64_t)block[t*8] << 56) | ((uint64_t)block[t*8+1] << 48) | ((uint64_t)block[t*8+2] << 40) | ((uint64_t)block[t*8+3] << 32)
             | ((uint64_t)block[t*8+4] << 24) | ((uint64_t)block[t*8+5] << 16) | ((uint64_t)block[t*8+6] << 8) | ((uint64_t)block[t*8+7]);
    }
    for (t = 16; t < 80; ++t) {
        uint64_t s0 = rotr64(W[t-15],1) ^ rotr64(W[t-15],8) ^ (W[t-15] >> 7);
        uint64_t s1 = rotr64(W[t-2],19) ^ rotr64(W[t-2],61) ^ (W[t-2] >> 6);
        W[t] = W[t-16] + s0 + W[t-7] + s1;
    }
    a = state[0]; b = state[1]; c = state[2]; d = state[3];
    e = state[4]; f = state[5]; g = state[6]; h = state[7];
    for (t = 0; t < 80; ++t) {
        uint64_t S1 = rotr64(e,14) ^ rotr64(e,18) ^ rotr64(e,41);
        uint64_t ch = (e & f) ^ ((~e) & g);
        t1 = h + S1 + ch + K512[t] + W[t];
        uint64_t S0 = rotr64(a,28) ^ rotr64(a,34) ^ rotr64(a,39);
        uint64_t maj = (a & b) ^ (a & c) ^ (b & c);
        t2 = S0 + maj;
        h = g; g = f; f = e; e = d + t1; d = c; c = b; b = a; a = t1 + t2;
    }
    state[0] += a; state[1] += b; state[2] += c; state[3] += d;
    state[4] += e; state[5] += f; state[6] += g; state[7] += h;
}

#if defined(__aarch64__)
/* ARMv8.2 SHA-512 single-block transform (FEAT_SHA512 crypto extensions).
 *
 * Transcribed from Botan's public ARMv8 SHA-512 implementation:
 *   src/lib/hash/sha2_64/sha2_64_armv8/sha2_64_armv8.cpp
 *   (C) 2023 René Fischer — Botan, Simplified BSD License (BSD-2-Clause).
 * Adapted to a single 128-byte block over our SHA512_CTX state layout; uses the
 * file's existing K512 constants. Verified against NIST/hashlib test vectors. */
__attribute__((target("arch=armv8.2-a+sha3")))
static void sha512_transform_neon(uint64_t state[8], const unsigned char block[128]) {
    uint64x2_t STATE0 = vld1q_u64(&state[0]);  // ab
    uint64x2_t STATE1 = vld1q_u64(&state[2]);  // cd
    uint64x2_t STATE2 = vld1q_u64(&state[4]);  // ef
    uint64x2_t STATE3 = vld1q_u64(&state[6]);  // gh

    const uint64x2_t AB_SAVE = STATE0;
    const uint64x2_t CD_SAVE = STATE1;
    const uint64x2_t EF_SAVE = STATE2;
    const uint64x2_t GH_SAVE = STATE3;

    // Load message (big-endian: byte-reverse each 64-bit word).
    uint64x2_t MSG0 = vreinterpretq_u64_u8(vrev64q_u8(vld1q_u8(block +   0)));
    uint64x2_t MSG1 = vreinterpretq_u64_u8(vrev64q_u8(vld1q_u8(block +  16)));
    uint64x2_t MSG2 = vreinterpretq_u64_u8(vrev64q_u8(vld1q_u8(block +  32)));
    uint64x2_t MSG3 = vreinterpretq_u64_u8(vrev64q_u8(vld1q_u8(block +  48)));
    uint64x2_t MSG4 = vreinterpretq_u64_u8(vrev64q_u8(vld1q_u8(block +  64)));
    uint64x2_t MSG5 = vreinterpretq_u64_u8(vrev64q_u8(vld1q_u8(block +  80)));
    uint64x2_t MSG6 = vreinterpretq_u64_u8(vrev64q_u8(vld1q_u8(block +  96)));
    uint64x2_t MSG7 = vreinterpretq_u64_u8(vrev64q_u8(vld1q_u8(block + 112)));

    uint64x2_t MSG_K, TSTATE0, TSTATE1;

    #define SHA512_2ROUNDS(M0,M1,M2,M3,M4,M5,M7, Sa,Sb,Sc,Sd, KI)                       \
        MSG_K   = vaddq_u64(M0, vld1q_u64(&K512[2 * (KI)]));                            \
        TSTATE0 = vaddq_u64(vextq_u64(MSG_K, MSG_K, 1), Sd);                            \
        TSTATE1 = vsha512hq_u64(TSTATE0, vextq_u64(Sc, Sd, 1), vextq_u64(Sb, Sc, 1));   \
        Sd      = vsha512h2q_u64(TSTATE1, Sb, Sa);                                      \
        Sb      = vaddq_u64(Sb, TSTATE1);                                               \
        M0      = vsha512su1q_u64(vsha512su0q_u64(M0, M1), M7, vextq_u64(M4, M5, 1));

    // Rounds with message schedule (0-63).
    SHA512_2ROUNDS(MSG0,MSG1,MSG2,MSG3,MSG4,MSG5,MSG7, STATE0,STATE1,STATE2,STATE3,  0)
    SHA512_2ROUNDS(MSG1,MSG2,MSG3,MSG4,MSG5,MSG6,MSG0, STATE3,STATE0,STATE1,STATE2,  1)
    SHA512_2ROUNDS(MSG2,MSG3,MSG4,MSG5,MSG6,MSG7,MSG1, STATE2,STATE3,STATE0,STATE1,  2)
    SHA512_2ROUNDS(MSG3,MSG4,MSG5,MSG6,MSG7,MSG0,MSG2, STATE1,STATE2,STATE3,STATE0,  3)
    SHA512_2ROUNDS(MSG4,MSG5,MSG6,MSG7,MSG0,MSG1,MSG3, STATE0,STATE1,STATE2,STATE3,  4)
    SHA512_2ROUNDS(MSG5,MSG6,MSG7,MSG0,MSG1,MSG2,MSG4, STATE3,STATE0,STATE1,STATE2,  5)
    SHA512_2ROUNDS(MSG6,MSG7,MSG0,MSG1,MSG2,MSG3,MSG5, STATE2,STATE3,STATE0,STATE1,  6)
    SHA512_2ROUNDS(MSG7,MSG0,MSG1,MSG2,MSG3,MSG4,MSG6, STATE1,STATE2,STATE3,STATE0,  7)
    SHA512_2ROUNDS(MSG0,MSG1,MSG2,MSG3,MSG4,MSG5,MSG7, STATE0,STATE1,STATE2,STATE3,  8)
    SHA512_2ROUNDS(MSG1,MSG2,MSG3,MSG4,MSG5,MSG6,MSG0, STATE3,STATE0,STATE1,STATE2,  9)
    SHA512_2ROUNDS(MSG2,MSG3,MSG4,MSG5,MSG6,MSG7,MSG1, STATE2,STATE3,STATE0,STATE1, 10)
    SHA512_2ROUNDS(MSG3,MSG4,MSG5,MSG6,MSG7,MSG0,MSG2, STATE1,STATE2,STATE3,STATE0, 11)
    SHA512_2ROUNDS(MSG4,MSG5,MSG6,MSG7,MSG0,MSG1,MSG3, STATE0,STATE1,STATE2,STATE3, 12)
    SHA512_2ROUNDS(MSG5,MSG6,MSG7,MSG0,MSG1,MSG2,MSG4, STATE3,STATE0,STATE1,STATE2, 13)
    SHA512_2ROUNDS(MSG6,MSG7,MSG0,MSG1,MSG2,MSG3,MSG5, STATE2,STATE3,STATE0,STATE1, 14)
    SHA512_2ROUNDS(MSG7,MSG0,MSG1,MSG2,MSG3,MSG4,MSG6, STATE1,STATE2,STATE3,STATE0, 15)
    SHA512_2ROUNDS(MSG0,MSG1,MSG2,MSG3,MSG4,MSG5,MSG7, STATE0,STATE1,STATE2,STATE3, 16)
    SHA512_2ROUNDS(MSG1,MSG2,MSG3,MSG4,MSG5,MSG6,MSG0, STATE3,STATE0,STATE1,STATE2, 17)
    SHA512_2ROUNDS(MSG2,MSG3,MSG4,MSG5,MSG6,MSG7,MSG1, STATE2,STATE3,STATE0,STATE1, 18)
    SHA512_2ROUNDS(MSG3,MSG4,MSG5,MSG6,MSG7,MSG0,MSG2, STATE1,STATE2,STATE3,STATE0, 19)
    SHA512_2ROUNDS(MSG4,MSG5,MSG6,MSG7,MSG0,MSG1,MSG3, STATE0,STATE1,STATE2,STATE3, 20)
    SHA512_2ROUNDS(MSG5,MSG6,MSG7,MSG0,MSG1,MSG2,MSG4, STATE3,STATE0,STATE1,STATE2, 21)
    SHA512_2ROUNDS(MSG6,MSG7,MSG0,MSG1,MSG2,MSG3,MSG5, STATE2,STATE3,STATE0,STATE1, 22)
    SHA512_2ROUNDS(MSG7,MSG0,MSG1,MSG2,MSG3,MSG4,MSG6, STATE1,STATE2,STATE3,STATE0, 23)
    SHA512_2ROUNDS(MSG0,MSG1,MSG2,MSG3,MSG4,MSG5,MSG7, STATE0,STATE1,STATE2,STATE3, 24)
    SHA512_2ROUNDS(MSG1,MSG2,MSG3,MSG4,MSG5,MSG6,MSG0, STATE3,STATE0,STATE1,STATE2, 25)
    SHA512_2ROUNDS(MSG2,MSG3,MSG4,MSG5,MSG6,MSG7,MSG1, STATE2,STATE3,STATE0,STATE1, 26)
    SHA512_2ROUNDS(MSG3,MSG4,MSG5,MSG6,MSG7,MSG0,MSG2, STATE1,STATE2,STATE3,STATE0, 27)
    SHA512_2ROUNDS(MSG4,MSG5,MSG6,MSG7,MSG0,MSG1,MSG3, STATE0,STATE1,STATE2,STATE3, 28)
    SHA512_2ROUNDS(MSG5,MSG6,MSG7,MSG0,MSG1,MSG2,MSG4, STATE3,STATE0,STATE1,STATE2, 29)
    SHA512_2ROUNDS(MSG6,MSG7,MSG0,MSG1,MSG2,MSG3,MSG5, STATE2,STATE3,STATE0,STATE1, 30)
    SHA512_2ROUNDS(MSG7,MSG0,MSG1,MSG2,MSG3,MSG4,MSG6, STATE1,STATE2,STATE3,STATE0, 31)
    #undef SHA512_2ROUNDS

    // Final rounds 64-79: no further message schedule.
    #define SHA512_2ROUNDS_NOMSG(M0, Sa,Sb,Sc,Sd, KI)                                   \
        MSG_K   = vaddq_u64(M0, vld1q_u64(&K512[2 * (KI)]));                            \
        TSTATE0 = vaddq_u64(vextq_u64(MSG_K, MSG_K, 1), Sd);                            \
        TSTATE1 = vsha512hq_u64(TSTATE0, vextq_u64(Sc, Sd, 1), vextq_u64(Sb, Sc, 1));   \
        Sd      = vsha512h2q_u64(TSTATE1, Sb, Sa);                                      \
        Sb      = vaddq_u64(Sb, TSTATE1);

    SHA512_2ROUNDS_NOMSG(MSG0, STATE0,STATE1,STATE2,STATE3, 32)
    SHA512_2ROUNDS_NOMSG(MSG1, STATE3,STATE0,STATE1,STATE2, 33)
    SHA512_2ROUNDS_NOMSG(MSG2, STATE2,STATE3,STATE0,STATE1, 34)
    SHA512_2ROUNDS_NOMSG(MSG3, STATE1,STATE2,STATE3,STATE0, 35)
    SHA512_2ROUNDS_NOMSG(MSG4, STATE0,STATE1,STATE2,STATE3, 36)
    SHA512_2ROUNDS_NOMSG(MSG5, STATE3,STATE0,STATE1,STATE2, 37)
    SHA512_2ROUNDS_NOMSG(MSG6, STATE2,STATE3,STATE0,STATE1, 38)
    SHA512_2ROUNDS_NOMSG(MSG7, STATE1,STATE2,STATE3,STATE0, 39)
    #undef SHA512_2ROUNDS_NOMSG

    STATE0 = vaddq_u64(STATE0, AB_SAVE);
    STATE1 = vaddq_u64(STATE1, CD_SAVE);
    STATE2 = vaddq_u64(STATE2, EF_SAVE);
    STATE3 = vaddq_u64(STATE3, GH_SAVE);

    vst1q_u64(&state[0], STATE0);
    vst1q_u64(&state[2], STATE1);
    vst1q_u64(&state[4], STATE2);
    vst1q_u64(&state[6], STATE3);
}

static bool sha512_hw_detect(void) {
#if defined(__APPLE__)
    return true;  /* Apple Silicon (M1+) implements FEAT_SHA512 */
#elif defined(__linux__)
    return (getauxval(AT_HWCAP) & HWCAP_SHA512) != 0;
#else
    return false;
#endif
}
static const bool g_sha512_hw = sha512_hw_detect();
#endif  /* __aarch64__ */

static inline void sha512_transform(uint64_t state[8], const unsigned char block[128]) {
#if defined(__aarch64__)
    if (g_sha512_hw) { sha512_transform_neon(state, block); return; }
#endif
    sha512_transform_scalar(state, block);
}

int SHA512_Init(SHA512_CTX *c) {
    if (!c) return 0;
    c->state[0]=0x6a09e667f3bcc908ULL; c->state[1]=0xbb67ae8584caa73bULL; c->state[2]=0x3c6ef372fe94f82bULL; c->state[3]=0xa54ff53a5f1d36f1ULL;
    c->state[4]=0x510e527fade682d1ULL; c->state[5]=0x9b05688c2b3e6c1fULL; c->state[6]=0x1f83d9abfb41bd6bULL; c->state[7]=0x5be0cd19137e2179ULL;
    c->count = 0;
    std::memset(c->buffer, 0, 128);
    return 1;
}

int SHA512_Update(SHA512_CTX *c, const void *data, size_t len) {
    if (!c) return 0;
    const unsigned char *input = (const unsigned char*)data;
    size_t index = (size_t)((c->count >> 3) & 0x7F);
    c->count += ((uint64_t)len) << 3;
    size_t partLen = 128 - index;
    size_t i = 0;

    if (len >= partLen) {
        std::memcpy(&c->buffer[index], input, partLen);
        sha512_transform(c->state, c->buffer);
        for (i = partLen; i + 127 < len; i += 128) {
            sha512_transform(c->state, &input[i]);
        }
        index = 0;
    } else i = 0;

    std::memcpy(&c->buffer[index], &input[i], len - i);
    return 1;
}

int SHA512_Final(unsigned char *md, SHA512_CTX *c) {
    if (!c || !md) return 0;
    unsigned char bits[16];
    uint64_t cnt = c->count;
    for (int i = 0; i < 16; ++i) {
        bits[15 - i] = (unsigned char)(cnt & 0xFF);
        cnt >>= 8;
    }
    unsigned int index = (unsigned int)((c->count >> 3) & 0x7F);
    unsigned int padLen = (index < 112) ? (112 - index) : (240 - index);
    static unsigned char PADDING[128] = { 0x80 };
    SHA512_Update(c, PADDING, padLen);
    SHA512_Update(c, bits, 16);
    for (int i = 0; i < 8; ++i) {
        md[8*i]   = (unsigned char)((c->state[i] >> 56) & 0xFF);
        md[8*i+1] = (unsigned char)((c->state[i] >> 48) & 0xFF);
        md[8*i+2] = (unsigned char)((c->state[i] >> 40) & 0xFF);
        md[8*i+3] = (unsigned char)((c->state[i] >> 32) & 0xFF);
        md[8*i+4] = (unsigned char)((c->state[i] >> 24) & 0xFF);
        md[8*i+5] = (unsigned char)((c->state[i] >> 16) & 0xFF);
        md[8*i+6] = (unsigned char)((c->state[i] >> 8) & 0xFF);
        md[8*i+7] = (unsigned char)(c->state[i] & 0xFF);
    }
    std::memset(c, 0, sizeof(*c));
    return 1;
}

} // extern "C"
