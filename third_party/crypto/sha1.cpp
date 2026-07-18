#include "sha1.h"
#include <cstring>

/* Hardware acceleration: ARMv8 SHA-1 crypto extensions when present at runtime,
 * scalar core otherwise. Same structure as sha2.cpp — function-level target
 * attribute so it builds without crypto in the global -march, runtime-detected,
 * scalar fallback for crypto-less ARM (Raspberry Pi). */
#if defined(__aarch64__)
#include <arm_neon.h>
#if defined(__linux__)
#include <sys/auxv.h>
#include <asm/hwcap.h>
#endif
#endif

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
static void sha1_transform_scalar(uint32_t state[5], const unsigned char buffer[64]) {
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

#if defined(__aarch64__)
/* ARMv8 SHA-1 single-block transform (crypto extensions). State {a,b,c,d} in a
 * vector + e scalar; messages loaded little-endian then byte-reversed. */
__attribute__((target("+crypto")))
static void sha1_transform_neon(uint32_t state[5], const unsigned char data[64]) {
    uint32x4_t ABCD, ABCD_SAVED, TMP0, TMP1, MSG0, MSG1, MSG2, MSG3;
    uint32_t   E0, E0_SAVED, E1;

    ABCD = vld1q_u32(&state[0]);
    E0   = state[4];
    ABCD_SAVED = ABCD;
    E0_SAVED   = E0;

    MSG0 = vreinterpretq_u32_u8(vrev32q_u8(vld1q_u8(data +  0)));
    MSG1 = vreinterpretq_u32_u8(vrev32q_u8(vld1q_u8(data + 16)));
    MSG2 = vreinterpretq_u32_u8(vrev32q_u8(vld1q_u8(data + 32)));
    MSG3 = vreinterpretq_u32_u8(vrev32q_u8(vld1q_u8(data + 48)));

    TMP0 = vaddq_u32(MSG0, vdupq_n_u32(0x5A827999));
    TMP1 = vaddq_u32(MSG1, vdupq_n_u32(0x5A827999));

    /* Rounds 0-3 */
    E1 = vsha1h_u32(vgetq_lane_u32(ABCD, 0));
    ABCD = vsha1cq_u32(ABCD, E0, TMP0);
    TMP0 = vaddq_u32(MSG2, vdupq_n_u32(0x5A827999));
    MSG0 = vsha1su0q_u32(MSG0, MSG1, MSG2);

    /* Rounds 4-7 */
    E0 = vsha1h_u32(vgetq_lane_u32(ABCD, 0));
    ABCD = vsha1cq_u32(ABCD, E1, TMP1);
    TMP1 = vaddq_u32(MSG3, vdupq_n_u32(0x5A827999));
    MSG0 = vsha1su1q_u32(MSG0, MSG3);
    MSG1 = vsha1su0q_u32(MSG1, MSG2, MSG3);

    /* Rounds 8-11 */
    E1 = vsha1h_u32(vgetq_lane_u32(ABCD, 0));
    ABCD = vsha1cq_u32(ABCD, E0, TMP0);
    TMP0 = vaddq_u32(MSG0, vdupq_n_u32(0x5A827999));
    MSG1 = vsha1su1q_u32(MSG1, MSG0);
    MSG2 = vsha1su0q_u32(MSG2, MSG3, MSG0);

    /* Rounds 12-15 */
    E0 = vsha1h_u32(vgetq_lane_u32(ABCD, 0));
    ABCD = vsha1cq_u32(ABCD, E1, TMP1);
    TMP1 = vaddq_u32(MSG1, vdupq_n_u32(0x6ED9EBA1));
    MSG2 = vsha1su1q_u32(MSG2, MSG1);
    MSG3 = vsha1su0q_u32(MSG3, MSG0, MSG1);

    /* Rounds 16-19 */
    E1 = vsha1h_u32(vgetq_lane_u32(ABCD, 0));
    ABCD = vsha1cq_u32(ABCD, E0, TMP0);
    TMP0 = vaddq_u32(MSG2, vdupq_n_u32(0x6ED9EBA1));
    MSG3 = vsha1su1q_u32(MSG3, MSG2);
    MSG0 = vsha1su0q_u32(MSG0, MSG1, MSG2);

    /* Rounds 20-23 */
    E0 = vsha1h_u32(vgetq_lane_u32(ABCD, 0));
    ABCD = vsha1pq_u32(ABCD, E1, TMP1);
    TMP1 = vaddq_u32(MSG3, vdupq_n_u32(0x6ED9EBA1));
    MSG0 = vsha1su1q_u32(MSG0, MSG3);
    MSG1 = vsha1su0q_u32(MSG1, MSG2, MSG3);

    /* Rounds 24-27 */
    E1 = vsha1h_u32(vgetq_lane_u32(ABCD, 0));
    ABCD = vsha1pq_u32(ABCD, E0, TMP0);
    TMP0 = vaddq_u32(MSG0, vdupq_n_u32(0x6ED9EBA1));
    MSG1 = vsha1su1q_u32(MSG1, MSG0);
    MSG2 = vsha1su0q_u32(MSG2, MSG3, MSG0);

    /* Rounds 28-31 */
    E0 = vsha1h_u32(vgetq_lane_u32(ABCD, 0));
    ABCD = vsha1pq_u32(ABCD, E1, TMP1);
    TMP1 = vaddq_u32(MSG1, vdupq_n_u32(0x6ED9EBA1));
    MSG2 = vsha1su1q_u32(MSG2, MSG1);
    MSG3 = vsha1su0q_u32(MSG3, MSG0, MSG1);

    /* Rounds 32-35 */
    E1 = vsha1h_u32(vgetq_lane_u32(ABCD, 0));
    ABCD = vsha1pq_u32(ABCD, E0, TMP0);
    TMP0 = vaddq_u32(MSG2, vdupq_n_u32(0x8F1BBCDC));
    MSG3 = vsha1su1q_u32(MSG3, MSG2);
    MSG0 = vsha1su0q_u32(MSG0, MSG1, MSG2);

    /* Rounds 36-39 */
    E0 = vsha1h_u32(vgetq_lane_u32(ABCD, 0));
    ABCD = vsha1pq_u32(ABCD, E1, TMP1);
    TMP1 = vaddq_u32(MSG3, vdupq_n_u32(0x8F1BBCDC));
    MSG0 = vsha1su1q_u32(MSG0, MSG3);
    MSG1 = vsha1su0q_u32(MSG1, MSG2, MSG3);

    /* Rounds 40-43 */
    E1 = vsha1h_u32(vgetq_lane_u32(ABCD, 0));
    ABCD = vsha1mq_u32(ABCD, E0, TMP0);
    TMP0 = vaddq_u32(MSG0, vdupq_n_u32(0x8F1BBCDC));
    MSG1 = vsha1su1q_u32(MSG1, MSG0);
    MSG2 = vsha1su0q_u32(MSG2, MSG3, MSG0);

    /* Rounds 44-47 */
    E0 = vsha1h_u32(vgetq_lane_u32(ABCD, 0));
    ABCD = vsha1mq_u32(ABCD, E1, TMP1);
    TMP1 = vaddq_u32(MSG1, vdupq_n_u32(0x8F1BBCDC));
    MSG2 = vsha1su1q_u32(MSG2, MSG1);
    MSG3 = vsha1su0q_u32(MSG3, MSG0, MSG1);

    /* Rounds 48-51 */
    E1 = vsha1h_u32(vgetq_lane_u32(ABCD, 0));
    ABCD = vsha1mq_u32(ABCD, E0, TMP0);
    TMP0 = vaddq_u32(MSG2, vdupq_n_u32(0x8F1BBCDC));
    MSG3 = vsha1su1q_u32(MSG3, MSG2);
    MSG0 = vsha1su0q_u32(MSG0, MSG1, MSG2);

    /* Rounds 52-55 */
    E0 = vsha1h_u32(vgetq_lane_u32(ABCD, 0));
    ABCD = vsha1mq_u32(ABCD, E1, TMP1);
    TMP1 = vaddq_u32(MSG3, vdupq_n_u32(0xCA62C1D6));
    MSG0 = vsha1su1q_u32(MSG0, MSG3);
    MSG1 = vsha1su0q_u32(MSG1, MSG2, MSG3);

    /* Rounds 56-59 */
    E1 = vsha1h_u32(vgetq_lane_u32(ABCD, 0));
    ABCD = vsha1mq_u32(ABCD, E0, TMP0);
    TMP0 = vaddq_u32(MSG0, vdupq_n_u32(0xCA62C1D6));
    MSG1 = vsha1su1q_u32(MSG1, MSG0);
    MSG2 = vsha1su0q_u32(MSG2, MSG3, MSG0);

    /* Rounds 60-63 */
    E0 = vsha1h_u32(vgetq_lane_u32(ABCD, 0));
    ABCD = vsha1pq_u32(ABCD, E1, TMP1);
    TMP1 = vaddq_u32(MSG1, vdupq_n_u32(0xCA62C1D6));
    MSG2 = vsha1su1q_u32(MSG2, MSG1);
    MSG3 = vsha1su0q_u32(MSG3, MSG0, MSG1);

    /* Rounds 64-67 */
    E1 = vsha1h_u32(vgetq_lane_u32(ABCD, 0));
    ABCD = vsha1pq_u32(ABCD, E0, TMP0);
    TMP0 = vaddq_u32(MSG2, vdupq_n_u32(0xCA62C1D6));
    MSG3 = vsha1su1q_u32(MSG3, MSG2);
    MSG0 = vsha1su0q_u32(MSG0, MSG1, MSG2);

    /* Rounds 68-71 */
    E0 = vsha1h_u32(vgetq_lane_u32(ABCD, 0));
    ABCD = vsha1pq_u32(ABCD, E1, TMP1);
    TMP1 = vaddq_u32(MSG3, vdupq_n_u32(0xCA62C1D6));
    MSG0 = vsha1su1q_u32(MSG0, MSG3);

    /* Rounds 72-75 */
    E1 = vsha1h_u32(vgetq_lane_u32(ABCD, 0));
    ABCD = vsha1pq_u32(ABCD, E0, TMP0);

    /* Rounds 76-79 */
    E0 = vsha1h_u32(vgetq_lane_u32(ABCD, 0));
    ABCD = vsha1pq_u32(ABCD, E1, TMP1);

    E0 += E0_SAVED;
    ABCD = vaddq_u32(ABCD_SAVED, ABCD);

    vst1q_u32(&state[0], ABCD);
    state[4] = E0;
}

static bool sha1_hw_detect(void) {
#if defined(__APPLE__)
    return true;  /* Apple Silicon implements FEAT_SHA1 */
#elif defined(__linux__)
    return (getauxval(AT_HWCAP) & HWCAP_SHA1) != 0;
#else
    return false;
#endif
}
static const bool g_sha1_hw = sha1_hw_detect();
#endif  /* __aarch64__ */

static inline void sha1_transform(uint32_t state[5], const unsigned char data[64]) {
#if defined(__aarch64__)
    if (g_sha1_hw) { sha1_transform_neon(state, data); return; }
#endif
    sha1_transform_scalar(state, data);
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