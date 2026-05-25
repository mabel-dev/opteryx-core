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

char* bintob16_rvv(char* restrict dest, const void* restrict src, size_t size) {
    if (size < 16) {
        return bintob16_scalar(dest, src, size);
    }

    const uint8_t* in  = (const uint8_t*)src;
    uint8_t*       out = (uint8_t*)dest;

    while (size >= 16) {
        size_t vl = vsetvl_e8m1(size);

        vuint8m1_t v  = vle8_v_u8m1(in, vl);
        vuint8m1_t hi = vsrl_vx_u8m1(v, 4, vl);           /* high nibble: 0..15 */
        vuint8m1_t lo = vand_vx_u8m1(v, 0x0F, vl);        /* low  nibble: 0..15 */

        /* Gather ASCII hex digit for each nibble from the 16-entry LUT. */
        vuint8m1_t hi_ascii = vloxei8_v_u8m1((const uint8_t*)B16_ENCODE_LUT, hi, vl);
        vuint8m1_t lo_ascii = vloxei8_v_u8m1((const uint8_t*)B16_ENCODE_LUT, lo, vl);

        /* Interleaved store: out[2i] = hi_ascii[i], out[2i+1] = lo_ascii[i] */
        vsseg2e8_v_u8m1(out, hi_ascii, lo_ascii, vl);

        in   += vl;
        out  += 2 * vl;
        size -= vl;
    }

    return bintob16_scalar((char*)out, in, size);
}

void* b16tobin_rvv(void* restrict dest, const char* restrict src, size_t len) {
    if (len < 32 || (len & 1) != 0) {
        return b16tobin_scalar(dest, src, len);
    }

    uint8_t*       out = (uint8_t*)dest;
    const uint8_t* in  = (const uint8_t*)src;

    while (len >= 32) {
        size_t vl = vsetvl_e8m1(len / 2);

        /* Deinterleaved load: c0[i]=in[2i] (high char), c1[i]=in[2i+1] (low char) */
        vuint8m1_t c0, c1;
        vlseg2e8_v_u8m1(&c0, &c1, in, vl);

        /* Decode each char to its nibble via the 256-entry LUT (invalid -> 255). */
        vuint8m1_t n_hi = vloxei8_v_u8m1(B16_DECODE_LUT, c0, vl);
        vuint8m1_t n_lo = vloxei8_v_u8m1(B16_DECODE_LUT, c1, vl);

        /*
         * Validity check: any nibble >= 16 means an invalid or non-hex char.
         * vredmaxu reduces to the maximum value across both vectors.
         */
        vuint8m1_t combined = vmax_vv_u8m1(n_hi, n_lo, vl);
        vuint8m1_t init     = vmv_s_x_u8m1(0, vl);
        vuint8m1_t max_vec  = vredmaxu_vs_u8m1(combined, init, vl);
        if (vmv_x_s_u8m1(max_vec) >= 16) return NULL;

        /* Pack: high nibble into bits [7:4], low nibble into bits [3:0]. */
        vuint8m1_t packed = vor_vv_u8m1(vsll_vx_u8m1(n_hi, 4, vl), n_lo, vl);
        vse8_v_u8m1(out, packed, vl);

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

char* bintob16_rvv(char* restrict dest, const void* restrict src, size_t size) {
    return bintob16_scalar(dest, src, size);
}
void* b16tobin_rvv(void* restrict dest, const char* restrict src, size_t len) {
    return b16tobin_scalar(dest, src, len);
}

#endif
