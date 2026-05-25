/*
 * RVV base64 encode/decode (RISC-V Vector extension).
 *
 * Encode: vlseg3e8 splits 3*vl input bytes into three vl-element vectors
 * (a, b, c — one per byte of each triplet). Four 6-bit indices are extracted
 * via vsrl/vsll/vand/vor and mapped to ASCII with vloxei8 (byte-indexed
 * gather from the 64-entry alphabet). vsseg4e8 writes the four interleaved
 * output chars.
 *
 * Decode: vlseg4e8 splits 4*vl input chars. Each char is mapped to its 6-bit
 * value with vloxei8 from B64_DECODE_LUT (256-byte table). A vredmaxu check
 * detects invalid bytes (value >= 64) and bails to scalar. vsseg3e8 writes
 * the three interleaved output bytes.
 *
 * Both paths reserve the tail for the scalar implementation, which also
 * handles '=' padding.
 *
 * VLEN-agnostic: vl is determined at runtime by vsetvl_e8m1; no hard-coded
 * vector width is assumed.
 */
#include "_base64.h"

#if defined(__riscv) && defined(__riscv_vector)
#include <riscv_vector.h>

static const uint8_t b64_rvv_alphabet[64] =
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

char* bintob64_rvv(char* B64_RESTRICT dest, const void* B64_RESTRICT src, size_t size) {
    if (size < 48) {
        return bintob64_scalar(dest, src, size);
    }

    const uint8_t* in = (const uint8_t*)src;
    char* out = dest;

    while (size >= 48) {
        /* vl = number of triplets to process this iteration. */
        size_t vl = vsetvl_e8m1(size / 3);

        /* Deinterleaved load: a[i]=in[3i], b[i]=in[3i+1], c[i]=in[3i+2] */
        vuint8m1_t a, b, c;
        vlseg3e8_v_u8m1(&a, &b, &c, in, vl);

        /* Four 6-bit indices per input triplet. */
        vuint8m1_t i0 = vsrl_vx_u8m1(a, 2, vl);
        vuint8m1_t i1 = vand_vx_u8m1(
            vor_vv_u8m1(vsll_vx_u8m1(a, 4, vl), vsrl_vx_u8m1(b, 4, vl), vl),
            0x3F, vl);
        vuint8m1_t i2 = vand_vx_u8m1(
            vor_vv_u8m1(vsll_vx_u8m1(b, 2, vl), vsrl_vx_u8m1(c, 6, vl), vl),
            0x3F, vl);
        vuint8m1_t i3 = vand_vx_u8m1(c, 0x3F, vl);

        /* Gather ASCII chars from the 64-entry alphabet via byte-indexed load. */
        vuint8m1_t c0 = vloxei8_v_u8m1(b64_rvv_alphabet, i0, vl);
        vuint8m1_t c1 = vloxei8_v_u8m1(b64_rvv_alphabet, i1, vl);
        vuint8m1_t c2 = vloxei8_v_u8m1(b64_rvv_alphabet, i2, vl);
        vuint8m1_t c3 = vloxei8_v_u8m1(b64_rvv_alphabet, i3, vl);

        /* Interleaved store: out[4i+k] = ck[i] */
        vsseg4e8_v_u8m1((uint8_t*)out, c0, c1, c2, c3, vl);

        in   += 3 * vl;
        out  += 4 * vl;
        size -= 3 * vl;
    }

    /* Tail (0..47 bytes): scalar handles remainder and any padding. */
    return bintob64_scalar(out, in, size);
}

void* b64tobin_rvv(void* B64_RESTRICT dest, const char* B64_RESTRICT src, size_t len) {
    if (len < 64 || (len & 3) != 0) {
        return b64tobin_scalar(dest, src, len);
    }

    uint8_t* out = (uint8_t*)dest;
    const uint8_t* in = (const uint8_t*)src;

    /*
     * Reserve the last quad (4 chars) for scalar so it can handle '=' padding.
     * vl is set to process at most (len - 4) / 4 complete quads per iteration.
     */
    while (len >= 68) {
        size_t vl = vsetvl_e8m1((len - 4) / 4);
        if (vl == 0) break;

        /* Deinterleaved load: c0[i]=in[4i], c1[i]=in[4i+1], etc. */
        vuint8m1_t ch0, ch1, ch2, ch3;
        vlseg4e8_v_u8m1(&ch0, &ch1, &ch2, &ch3, in, vl);

        /* Decode each char to its 6-bit value via the 256-entry LUT. */
        vuint8m1_t v0 = vloxei8_v_u8m1(B64_DECODE_LUT, ch0, vl);
        vuint8m1_t v1 = vloxei8_v_u8m1(B64_DECODE_LUT, ch1, vl);
        vuint8m1_t v2 = vloxei8_v_u8m1(B64_DECODE_LUT, ch2, vl);
        vuint8m1_t v3 = vloxei8_v_u8m1(B64_DECODE_LUT, ch3, vl);

        /*
         * Validity check: any decoded value >= 64 means an invalid char or
         * padding in the bulk region. Bail out and let scalar report the error.
         */
        vuint8m1_t combined = vmax_vv_u8m1(
            vmax_vv_u8m1(v0, v1, vl),
            vmax_vv_u8m1(v2, v3, vl),
            vl);
        vuint8m1_t init = vmv_s_x_u8m1(0, vl);
        vuint8m1_t max_vec = vredmaxu_vs_u8m1(combined, init, vl);
        if (vmv_x_s_u8m1(max_vec) >= 64) break;

        /* Bit-pack: four 6-bit values → three 8-bit bytes per quad. */
        vuint8m1_t out0 = vor_vv_u8m1(vsll_vx_u8m1(v0, 2, vl), vsrl_vx_u8m1(v1, 4, vl), vl);
        vuint8m1_t out1 = vor_vv_u8m1(vsll_vx_u8m1(v1, 4, vl), vsrl_vx_u8m1(v2, 2, vl), vl);
        vuint8m1_t out2 = vor_vv_u8m1(vsll_vx_u8m1(v2, 6, vl), v3, vl);

        /* Interleaved store: out[3i+k] = outk[i] */
        vsseg3e8_v_u8m1(out, out0, out1, out2, vl);

        in  += 4 * vl;
        out += 3 * vl;
        len -= 4 * vl;
    }

    /* Tail: scalar resolves remaining chars and '=' padding. */
    return b64tobin_scalar(out, (const char*)in, len);
}

#else  /* no __riscv_vector */

char* bintob64_rvv(char* B64_RESTRICT dest, const void* B64_RESTRICT src, size_t size) {
    return bintob64_scalar(dest, src, size);
}
void* b64tobin_rvv(void* B64_RESTRICT dest, const char* B64_RESTRICT src, size_t len) {
    return b64tobin_scalar(dest, src, len);
}

#endif
