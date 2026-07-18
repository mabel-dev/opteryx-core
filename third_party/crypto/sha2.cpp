#include "sha2.h"
#include <cstring>

/* Hardware acceleration: ARMv8 SHA-256 crypto extensions when available at
 * runtime, scalar core otherwise. The accelerated transform is compiled with a
 * function-level target attribute so it builds even when crypto is not in the
 * global -march (manylinux aarch64), and is selected only after a runtime
 * feature check — Raspberry Pi cores (Cortex-A72/A76) lack the extensions and
 * fall back to scalar. The AVX2 multi-buffer x86 path lives separately and is
 * CI-verified (cannot be built or run on the ARM dev host). */
#if defined(__aarch64__)
#include <arm_neon.h>
#if defined(__linux__)
#include <sys/auxv.h>
#include <asm/hwcap.h>
#endif
#endif

/* SHA-256 implementation (adapted for C++) */
static const uint32_t K256[64] = {
    0x428a2f98,0x71374491,0xb5c0fbcf,0xe9b5dba5,0x3956c25b,0x59f111f1,0x923f82a4,0xab1c5ed5,
    0xd807aa98,0x12835b01,0x243185be,0x550c7dc3,0x72be5d74,0x80deb1fe,0x9bdc06a7,0xc19bf174,
    0xe49b69c1,0xefbe4786,0x0fc19dc6,0x240ca1cc,0x2de92c6f,0x4a7484aa,0x5cb0a9dc,0x76f988da,
    0x983e5152,0xa831c66d,0xb00327c8,0xbf597fc7,0xc6e00bf3,0xd5a79147,0x06ca6351,0x14292967,
    0x27b70a85,0x2e1b2138,0x4d2c6dfc,0x53380d13,0x650a7354,0x766a0abb,0x81c2c92e,0x92722c85,
    0xa2bfe8a1,0xa81a664b,0xc24b8b70,0xc76c51a3,0xd192e819,0xd6990624,0xf40e3585,0x106aa070,
    0x19a4c116,0x1e376c08,0x2748774c,0x34b0bcb5,0x391c0cb3,0x4ed8aa4a,0x5b9cca4f,0x682e6ff3,
    0x748f82ee,0x78a5636f,0x84c87814,0x8cc70208,0x90befffa,0xa4506ceb,0xbef9a3f7,0xc67178f2
};

static uint32_t rotr32(uint32_t x, unsigned n) { return (x >> n) | (x << (32 - n)); }

static void sha256_transform_scalar(uint32_t state[8], const unsigned char data[64]) {
    uint32_t W[64];
    uint32_t a,b,c,d,e,f,g,h,t1,t2;
    int t;
    for (t = 0; t < 16; ++t) {
        W[t] = (uint32_t)data[t*4] << 24 | (uint32_t)data[t*4+1] << 16 | (uint32_t)data[t*4+2] << 8 | (uint32_t)data[t*4+3];
    }
    for (t = 16; t < 64; ++t) {
        uint32_t s0 = rotr32(W[t-15],7) ^ rotr32(W[t-15],18) ^ (W[t-15] >> 3);
        uint32_t s1 = rotr32(W[t-2],17) ^ rotr32(W[t-2],19) ^ (W[t-2] >> 10);
        W[t] = W[t-16] + s0 + W[t-7] + s1;
    }
    a = state[0]; b = state[1]; c = state[2]; d = state[3];
    e = state[4]; f = state[5]; g = state[6]; h = state[7];
    for (t = 0; t < 64; ++t) {
        uint32_t S1 = rotr32(e,6) ^ rotr32(e,11) ^ rotr32(e,25);
        uint32_t ch = (e & f) ^ (~e & g);
        t1 = h + S1 + ch + K256[t] + W[t];
        uint32_t S0 = rotr32(a,2) ^ rotr32(a,13) ^ rotr32(a,22);
        uint32_t maj = (a & b) ^ (a & c) ^ (b & c);
        t2 = S0 + maj;
        h = g; g = f; f = e; e = d + t1; d = c; c = b; b = a; a = t1 + t2;
    }
    state[0] += a; state[1] += b; state[2] += c; state[3] += d;
    state[4] += e; state[5] += f; state[6] += g; state[7] += h;
}

#if defined(__aarch64__)
/* ARMv8 SHA-256 single-block transform using the crypto extensions. State is the
 * canonical {a,b,c,d} / {e,f,g,h} layout, matching the scalar core. Messages are
 * loaded little-endian then byte-reversed (SHA-256 is big-endian). */
__attribute__((target("+crypto")))
static void sha256_transform_neon(uint32_t state[8], const unsigned char data[64]) {
    uint32x4_t STATE0 = vld1q_u32(&state[0]);
    uint32x4_t STATE1 = vld1q_u32(&state[4]);
    const uint32x4_t ABEF_SAVE = STATE0;
    const uint32x4_t CDGH_SAVE = STATE1;

    uint32x4_t MSG0 = vreinterpretq_u32_u8(vrev32q_u8(vld1q_u8(data +  0)));
    uint32x4_t MSG1 = vreinterpretq_u32_u8(vrev32q_u8(vld1q_u8(data + 16)));
    uint32x4_t MSG2 = vreinterpretq_u32_u8(vrev32q_u8(vld1q_u8(data + 32)));
    uint32x4_t MSG3 = vreinterpretq_u32_u8(vrev32q_u8(vld1q_u8(data + 48)));
    uint32x4_t TMP0, TMP1, TMP2;

    TMP0 = vaddq_u32(MSG0, vld1q_u32(&K256[0]));

    /* Rounds 0-3 */
    MSG0 = vsha256su0q_u32(MSG0, MSG1);
    TMP2 = STATE0;
    TMP1 = vaddq_u32(MSG1, vld1q_u32(&K256[4]));
    STATE0 = vsha256hq_u32(STATE0, STATE1, TMP0);
    STATE1 = vsha256h2q_u32(STATE1, TMP2, TMP0);
    MSG0 = vsha256su1q_u32(MSG0, MSG2, MSG3);

    /* Rounds 4-7 */
    MSG1 = vsha256su0q_u32(MSG1, MSG2);
    TMP2 = STATE0;
    TMP0 = vaddq_u32(MSG2, vld1q_u32(&K256[8]));
    STATE0 = vsha256hq_u32(STATE0, STATE1, TMP1);
    STATE1 = vsha256h2q_u32(STATE1, TMP2, TMP1);
    MSG1 = vsha256su1q_u32(MSG1, MSG3, MSG0);

    /* Rounds 8-11 */
    MSG2 = vsha256su0q_u32(MSG2, MSG3);
    TMP2 = STATE0;
    TMP1 = vaddq_u32(MSG3, vld1q_u32(&K256[12]));
    STATE0 = vsha256hq_u32(STATE0, STATE1, TMP0);
    STATE1 = vsha256h2q_u32(STATE1, TMP2, TMP0);
    MSG2 = vsha256su1q_u32(MSG2, MSG0, MSG1);

    /* Rounds 12-15 */
    MSG3 = vsha256su0q_u32(MSG3, MSG0);
    TMP2 = STATE0;
    TMP0 = vaddq_u32(MSG0, vld1q_u32(&K256[16]));
    STATE0 = vsha256hq_u32(STATE0, STATE1, TMP1);
    STATE1 = vsha256h2q_u32(STATE1, TMP2, TMP1);
    MSG3 = vsha256su1q_u32(MSG3, MSG1, MSG2);

    /* Rounds 16-19 */
    MSG0 = vsha256su0q_u32(MSG0, MSG1);
    TMP2 = STATE0;
    TMP1 = vaddq_u32(MSG1, vld1q_u32(&K256[20]));
    STATE0 = vsha256hq_u32(STATE0, STATE1, TMP0);
    STATE1 = vsha256h2q_u32(STATE1, TMP2, TMP0);
    MSG0 = vsha256su1q_u32(MSG0, MSG2, MSG3);

    /* Rounds 20-23 */
    MSG1 = vsha256su0q_u32(MSG1, MSG2);
    TMP2 = STATE0;
    TMP0 = vaddq_u32(MSG2, vld1q_u32(&K256[24]));
    STATE0 = vsha256hq_u32(STATE0, STATE1, TMP1);
    STATE1 = vsha256h2q_u32(STATE1, TMP2, TMP1);
    MSG1 = vsha256su1q_u32(MSG1, MSG3, MSG0);

    /* Rounds 24-27 */
    MSG2 = vsha256su0q_u32(MSG2, MSG3);
    TMP2 = STATE0;
    TMP1 = vaddq_u32(MSG3, vld1q_u32(&K256[28]));
    STATE0 = vsha256hq_u32(STATE0, STATE1, TMP0);
    STATE1 = vsha256h2q_u32(STATE1, TMP2, TMP0);
    MSG2 = vsha256su1q_u32(MSG2, MSG0, MSG1);

    /* Rounds 28-31 */
    MSG3 = vsha256su0q_u32(MSG3, MSG0);
    TMP2 = STATE0;
    TMP0 = vaddq_u32(MSG0, vld1q_u32(&K256[32]));
    STATE0 = vsha256hq_u32(STATE0, STATE1, TMP1);
    STATE1 = vsha256h2q_u32(STATE1, TMP2, TMP1);
    MSG3 = vsha256su1q_u32(MSG3, MSG1, MSG2);

    /* Rounds 32-35 */
    MSG0 = vsha256su0q_u32(MSG0, MSG1);
    TMP2 = STATE0;
    TMP1 = vaddq_u32(MSG1, vld1q_u32(&K256[36]));
    STATE0 = vsha256hq_u32(STATE0, STATE1, TMP0);
    STATE1 = vsha256h2q_u32(STATE1, TMP2, TMP0);
    MSG0 = vsha256su1q_u32(MSG0, MSG2, MSG3);

    /* Rounds 36-39 */
    MSG1 = vsha256su0q_u32(MSG1, MSG2);
    TMP2 = STATE0;
    TMP0 = vaddq_u32(MSG2, vld1q_u32(&K256[40]));
    STATE0 = vsha256hq_u32(STATE0, STATE1, TMP1);
    STATE1 = vsha256h2q_u32(STATE1, TMP2, TMP1);
    MSG1 = vsha256su1q_u32(MSG1, MSG3, MSG0);

    /* Rounds 40-43 */
    MSG2 = vsha256su0q_u32(MSG2, MSG3);
    TMP2 = STATE0;
    TMP1 = vaddq_u32(MSG3, vld1q_u32(&K256[44]));
    STATE0 = vsha256hq_u32(STATE0, STATE1, TMP0);
    STATE1 = vsha256h2q_u32(STATE1, TMP2, TMP0);
    MSG2 = vsha256su1q_u32(MSG2, MSG0, MSG1);

    /* Rounds 44-47 */
    MSG3 = vsha256su0q_u32(MSG3, MSG0);
    TMP2 = STATE0;
    TMP0 = vaddq_u32(MSG0, vld1q_u32(&K256[48]));
    STATE0 = vsha256hq_u32(STATE0, STATE1, TMP1);
    STATE1 = vsha256h2q_u32(STATE1, TMP2, TMP1);
    MSG3 = vsha256su1q_u32(MSG3, MSG1, MSG2);

    /* Rounds 48-51 */
    TMP2 = STATE0;
    TMP1 = vaddq_u32(MSG1, vld1q_u32(&K256[52]));
    STATE0 = vsha256hq_u32(STATE0, STATE1, TMP0);
    STATE1 = vsha256h2q_u32(STATE1, TMP2, TMP0);

    /* Rounds 52-55 */
    TMP2 = STATE0;
    TMP0 = vaddq_u32(MSG2, vld1q_u32(&K256[56]));
    STATE0 = vsha256hq_u32(STATE0, STATE1, TMP1);
    STATE1 = vsha256h2q_u32(STATE1, TMP2, TMP1);

    /* Rounds 56-59 */
    TMP2 = STATE0;
    TMP1 = vaddq_u32(MSG3, vld1q_u32(&K256[60]));
    STATE0 = vsha256hq_u32(STATE0, STATE1, TMP0);
    STATE1 = vsha256h2q_u32(STATE1, TMP2, TMP0);

    /* Rounds 60-63 */
    TMP2 = STATE0;
    STATE0 = vsha256hq_u32(STATE0, STATE1, TMP1);
    STATE1 = vsha256h2q_u32(STATE1, TMP2, TMP1);

    STATE0 = vaddq_u32(STATE0, ABEF_SAVE);
    STATE1 = vaddq_u32(STATE1, CDGH_SAVE);

    vst1q_u32(&state[0], STATE0);
    vst1q_u32(&state[4], STATE1);
}

static bool sha256_hw_detect(void) {
#if defined(__APPLE__)
    return true;  /* every Apple Silicon core implements FEAT_SHA256 */
#elif defined(__linux__)
    return (getauxval(AT_HWCAP) & HWCAP_SHA2) != 0;
#else
    return false;  /* unknown OS: take the portable scalar path */
#endif
}
static const bool g_sha256_hw = sha256_hw_detect();
#endif  /* __aarch64__ */

static inline void sha256_transform(uint32_t state[8], const unsigned char data[64]) {
#if defined(__aarch64__)
    if (g_sha256_hw) { sha256_transform_neon(state, data); return; }
#endif
    sha256_transform_scalar(state, data);
}

int SHA256_Init(SHA256_CTX *c) {
    if (!c) return 0;
    c->state[0]=0x6a09e667; c->state[1]=0xbb67ae85; c->state[2]=0x3c6ef372; c->state[3]=0xa54ff53a;
    c->state[4]=0x510e527f; c->state[5]=0x9b05688c; c->state[6]=0x1f83d9ab; c->state[7]=0x5be0cd19;
    c->count=0; std::memset(c->buffer,0,64);
    return 1;
}

int SHA256_Update(SHA256_CTX *c, const void *data, size_t len) {
    if (!c) return 0;
    size_t index = (size_t)((c->count >> 3) & 0x3f);
    c->count += ((uint64_t)len) << 3;
    size_t partLen = 64 - index;
    size_t i=0;
    const unsigned char *input = (const unsigned char*)data;
    if (len >= partLen) {
        std::memcpy(&c->buffer[index], input, partLen);
        sha256_transform(c->state, c->buffer);
        for (i = partLen; i + 63 < len; i += 64) sha256_transform(c->state, &input[i]);
        index = 0;
    } else i = 0;
    std::memcpy(&c->buffer[index], &input[i], len - i);
    return 1;
}

int SHA256_Final(unsigned char *md, SHA256_CTX *c) {
    unsigned char bits[8]; uint64_t cnt = c->count; int i;
    for (i=0;i<8;i++) { bits[7-i] = cnt & 0xFF; cnt >>= 8; }
    unsigned int index = (unsigned int)((c->count >> 3) & 0x3f);
    unsigned int padLen = (index < 56) ? (56 - index) : (120 - index);
    static unsigned char PADDING[64] = { 0x80 };
    SHA256_Update(c, PADDING, padLen);
    SHA256_Update(c, bits, 8);
    for (i = 0; i < 8; ++i) {
        md[4*i] = (unsigned char)((c->state[i] >> 24) & 0xFF);
        md[4*i+1] = (unsigned char)((c->state[i] >> 16) & 0xFF);
        md[4*i+2] = (unsigned char)((c->state[i] >> 8) & 0xFF);
        md[4*i+3] = (unsigned char)(c->state[i] & 0xFF);
    }
    memset(c,0,sizeof(*c));
    return 1;
}
