#pragma once

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/*
 * simd_remap — in-place dictionary code remapping.
 *
 * Each function replaces every code in the input array with the value from
 * the corresponding remap table entry:  codes[i] = remap_table[codes[i]].
 *
 * This converts insertion-ordered dictionary codes into rank-ordered codes,
 * making radix sort on the remapped values semantically correct for ORDER BY.
 *
 * Implementation notes:
 *   simd_remap_u8  — AVX2: 16-pass vpshufb (32 codes/iter)
 *                    NEON: 4-pass vqtbl4q_u8 (16 codes/iter)
 *   simd_remap_u16 — AVX2: i32gather with scale=2 (8 codes/iter)
 *                    NEON: scalar (no efficient 16-bit gather in NEON)
 *   simd_remap_u32 — scalar (table too large for SIMD benefit)
 *
 * The remap_table for simd_remap_u16 must have at least one extra element
 * past the highest valid code (the gather loads 32-bit values at scale=2).
 */

void simd_remap_u8(uint8_t* codes, size_t n, const uint8_t* remap_table);
void simd_remap_u16(uint16_t* codes, size_t n, const uint16_t* remap_table);
void simd_remap_u32(uint32_t* codes, size_t n, const uint32_t* remap_table);

#ifdef __cplusplus
}
#endif
