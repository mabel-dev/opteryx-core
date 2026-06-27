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
        size_t vl = __riscv_vsetvl_e8m1(size / 3);

        /* Deinterleaved load: a[i]=in[3i], b[i]=in[3i+1], c[i]=in[3i+2] */
        vuint8m1x3_t abc = __riscv_vlseg3e8_v_u8m1x3(in, vl);
        vuint8m1_t a = __riscv_vget_v_u8m1x3_u8m1(abc, 0);
        vuint8m1_t b = __riscv_vget_v_u8m1x3_u8m1(abc, 1);
        vuint8m1_t c = __riscv_vget_v_u8m1x3_u8m1(abc, 2);

        /* Four 6-bit indices per input triplet. */
        vuint8m1_t i0 = __riscv_vsrl_vx_u8m1(a, 2, vl);
        vuint8m1_t i1 = __riscv_vand_vx_u8m1(
            __riscv_vor_vv_u8m1(__riscv_vsll_vx_u8m1(a, 4, vl), __riscv_vsrl_vx_u8m1(b, 4, vl), vl),
            0x3F, vl);
        vuint8m1_t i2 = __riscv_vand_vx_u8m1(
            __riscv_vor_vv_u8m1(__riscv_vsll_vx_u8m1(b, 2, vl), __riscv_vsrl_vx_u8m1(c, 6, vl), vl),
            0x3F, vl);
        vuint8m1_t i3 = __riscv_vand_vx_u8m1(c, 0x3F, vl);

        /* Gather ASCII chars from the 64-entry alphabet via byte-indexed load. */
        vuint8m1_t c0 = __riscv_vloxei8_v_u8m1(b64_rvv_alphabet, i0, vl);
        vuint8m1_t c1 = __riscv_vloxei8_v_u8m1(b64_rvv_alphabet, i1, vl);
        vuint8m1_t c2 = __riscv_vloxei8_v_u8m1(b64_rvv_alphabet, i2, vl);
        vuint8m1_t c3 = __riscv_vloxei8_v_u8m1(b64_rvv_alphabet, i3, vl);

        /* Interleaved store: out[4i+k] = ck[i] */
        vuint8m1x4_t outv = __riscv_vundefined_u8m1x4();
        outv = __riscv_vset_v_u8m1_u8m1x4(outv, 0, c0);
        outv = __riscv_vset_v_u8m1_u8m1x4(outv, 1, c1);
        outv = __riscv_vset_v_u8m1_u8m1x4(outv, 2, c2);
        outv = __riscv_vset_v_u8m1_u8m1x4(outv, 3, c3);
        __riscv_vsseg4e8_v_u8m1x4((uint8_t*)out, outv, vl);

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
        size_t vl = __riscv_vsetvl_e8m1((len - 4) / 4);
        if (vl == 0) break;

        /* Deinterleaved load: c0[i]=in[4i], c1[i]=in[4i+1], etc. */
        vuint8m1x4_t chs = __riscv_vlseg4e8_v_u8m1x4(in, vl);
        vuint8m1_t ch0 = __riscv_vget_v_u8m1x4_u8m1(chs, 0);
        vuint8m1_t ch1 = __riscv_vget_v_u8m1x4_u8m1(chs, 1);
        vuint8m1_t ch2 = __riscv_vget_v_u8m1x4_u8m1(chs, 2);
        vuint8m1_t ch3 = __riscv_vget_v_u8m1x4_u8m1(chs, 3);

        /* Decode each char to its 6-bit value via the 256-entry LUT. */
        vuint8m1_t v0 = __riscv_vloxei8_v_u8m1(B64_DECODE_LUT, ch0, vl);
        vuint8m1_t v1 = __riscv_vloxei8_v_u8m1(B64_DECODE_LUT, ch1, vl);
        vuint8m1_t v2 = __riscv_vloxei8_v_u8m1(B64_DECODE_LUT, ch2, vl);
        vuint8m1_t v3 = __riscv_vloxei8_v_u8m1(B64_DECODE_LUT, ch3, vl);

        /*
         * Validity check: any decoded value >= 64 means an invalid char or
         * padding in the bulk region. Bail out and let scalar report the error.
         * Values are 0..65 (≤127) so unsigned reduction is well-defined.
         */
        vuint8m1_t combined = __riscv_vmaxu_vv_u8m1(
            __riscv_vmaxu_vv_u8m1(v0, v1, vl),
            __riscv_vmaxu_vv_u8m1(v2, v3, vl),
            vl);
        vuint8m1_t init = __riscv_vmv_s_x_u8m1(0, vl);
        vuint8m1_t max_vec = __riscv_vredmaxu_vs_u8m1_u8m1(combined, init, vl);
        if (__riscv_vmv_x_s_u8m1_u8(max_vec) >= 64) break;

        /* Bit-pack: four 6-bit values → three 8-bit bytes per quad. */
        vuint8m1_t out0 = __riscv_vor_vv_u8m1(__riscv_vsll_vx_u8m1(v0, 2, vl), __riscv_vsrl_vx_u8m1(v1, 4, vl), vl);
        vuint8m1_t out1 = __riscv_vor_vv_u8m1(__riscv_vsll_vx_u8m1(v1, 4, vl), __riscv_vsrl_vx_u8m1(v2, 2, vl), vl);
        vuint8m1_t out2 = __riscv_vor_vv_u8m1(__riscv_vsll_vx_u8m1(v2, 6, vl), v3, vl);

        /* Interleaved store: out[3i+k] = outk[i] */
        vuint8m1x3_t outv = __riscv_vundefined_u8m1x3();
        outv = __riscv_vset_v_u8m1_u8m1x3(outv, 0, out0);
        outv = __riscv_vset_v_u8m1_u8m1x3(outv, 1, out1);
        outv = __riscv_vset_v_u8m1_u8m1x3(outv, 2, out2);
        __riscv_vsseg3e8_v_u8m1x3(out, outv, vl);

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
