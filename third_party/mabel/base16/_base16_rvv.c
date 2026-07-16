/*
 * RVV base16 (hex) encode/decode (RISC-V Vector extension).
 *
 * Encode: 16-entry B16_ENCODE_LUT gathered via vloxei8. Each input byte is
 * split into high/low nibbles; vsseg2e8 writes them interleaved as two hex
 * chars per byte. vl bytes in -> 2*vl chars out per iteration.
 *
 * Decode: vlseg2e8 splits each pair of hex chars into two vl-element vectors
 * (high and low char of each pair). vloxei8 from B16_DECODE_LUT converts each
 * char to its nibble (invalid chars map to 255). vredmaxu detects any invalid
 * byte; if found we return NULL immediately. Valid nibbles are packed into
 * output bytes via vsll/vor and stored with vse8.
 */
#include "_base16.h"

#if defined(__riscv) && defined(__riscv_vector)
#include <riscv_vector.h>

char* bintob16_rvv_lut(char* restrict dest, const void* restrict src, size_t size,
                       const char* restrict lut) {
    if (size < 16) {
        return bintob16_scalar_lut(dest, src, size, lut);
    }

    const uint8_t* in  = (const uint8_t*)src;
    uint8_t*       out = (uint8_t*)dest;

    while (size >= 16) {
        size_t vl = __riscv_vsetvl_e8m1(size);

        vuint8m1_t v  = __riscv_vle8_v_u8m1(in, vl);
        vuint8m1_t hi = __riscv_vsrl_vx_u8m1(v, 4, vl);           /* high nibble: 0..15 */
        vuint8m1_t lo = __riscv_vand_vx_u8m1(v, 0x0F, vl);        /* low  nibble: 0..15 */

        /* Gather ASCII hex digit for each nibble from the 16-entry LUT. */
        vuint8m1_t hi_ascii = __riscv_vloxei8_v_u8m1((const uint8_t*)lut, hi, vl);
        vuint8m1_t lo_ascii = __riscv_vloxei8_v_u8m1((const uint8_t*)lut, lo, vl);

        /* Interleaved store: out[2i] = hi_ascii[i], out[2i+1] = lo_ascii[i] */
        vuint8m1x2_t outv = __riscv_vundefined_u8m1x2();
        outv = __riscv_vset_v_u8m1_u8m1x2(outv, 0, hi_ascii);
        outv = __riscv_vset_v_u8m1_u8m1x2(outv, 1, lo_ascii);
        __riscv_vsseg2e8_v_u8m1x2(out, outv, vl);

        in   += vl;
        out  += 2 * vl;
        size -= vl;
    }

    return bintob16_scalar_lut((char*)out, in, size, lut);
}

char* bintob16_rvv(char* restrict dest, const void* restrict src, size_t size) {
    return bintob16_rvv_lut(dest, src, size, B16_ENCODE_LUT);
}

void* b16tobin_rvv(void* restrict dest, const char* restrict src, size_t len) {
    if (len < 32 || (len & 1) != 0) {
        return b16tobin_scalar(dest, src, len);
    }

    uint8_t*       out = (uint8_t*)dest;
    const uint8_t* in  = (const uint8_t*)src;

    while (len >= 32) {
        size_t vl = __riscv_vsetvl_e8m1(len / 2);

        /* Deinterleaved load: c0[i]=in[2i] (high char), c1[i]=in[2i+1] (low char) */
        vuint8m1x2_t pair = __riscv_vlseg2e8_v_u8m1x2(in, vl);
        vuint8m1_t c0 = __riscv_vget_v_u8m1x2_u8m1(pair, 0);
        vuint8m1_t c1 = __riscv_vget_v_u8m1x2_u8m1(pair, 1);

        /* Decode each char to its nibble via the 256-entry LUT (invalid -> 255). */
        vuint8m1_t n_hi = __riscv_vloxei8_v_u8m1(B16_DECODE_LUT, c0, vl);
        vuint8m1_t n_lo = __riscv_vloxei8_v_u8m1(B16_DECODE_LUT, c1, vl);

        /*
         * Validity check: any nibble >= 16 means an invalid or non-hex char.
         * Invalid chars decode to 255 (>127), so UNSIGNED max/reduction is
         * required — a signed max would treat 255 as -1 and miss it.
         */
        vuint8m1_t combined = __riscv_vmaxu_vv_u8m1(n_hi, n_lo, vl);
        vuint8m1_t init     = __riscv_vmv_s_x_u8m1(0, vl);
        vuint8m1_t max_vec  = __riscv_vredmaxu_vs_u8m1_u8m1(combined, init, vl);
        if (__riscv_vmv_x_s_u8m1_u8(max_vec) >= 16) return NULL;

        /* Pack: high nibble into bits [7:4], low nibble into bits [3:0]. */
        vuint8m1_t packed = __riscv_vor_vv_u8m1(__riscv_vsll_vx_u8m1(n_hi, 4, vl), n_lo, vl);
        __riscv_vse8_v_u8m1(out, packed, vl);

        in  += 2 * vl;
        out += vl;
        len -= 2 * vl;
    }

    if (len > 0) {
        return b16tobin_scalar(out, (const char*)in, len);
    }
    return out;
}

#else  /* no __riscv_vector */

char* bintob16_rvv_lut(char* restrict dest, const void* restrict src, size_t size,
                       const char* restrict lut) {
    return bintob16_scalar_lut(dest, src, size, lut);
}
char* bintob16_rvv(char* restrict dest, const void* restrict src, size_t size) {
    return bintob16_scalar(dest, src, size);
}
void* b16tobin_rvv(void* restrict dest, const char* restrict src, size_t len) {
    return b16tobin_scalar(dest, src, len);
}

#endif
