#pragma once

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

// Shared mixing constant used by scalar and SIMD mixers.
#ifndef MIX_HASH_CONSTANT
#define MIX_HASH_CONSTANT ((uint64_t)0x9e3779b97f4a7c15ULL)
#endif

// Sentinel hash assigned to null cells across vector implementations.
#ifndef NULL_HASH
#define NULL_HASH ((uint64_t)0x4c3f95a36ab8eccaULL)
#endif

void simd_mix_hash(uint64_t* dest, const uint64_t* values, size_t count);

// Single-column hash: dst[i] = hash(src[i]), no prior dest state required.
// Equivalent to memset(dst,0) + simd_mix_hash(dst,src,n) but in one pass.
// Use for COUNT(DISTINCT) where there is no composite key to combine.
void simd_hash_i64(const uint64_t* src, uint64_t* dst, size_t count);
void simd_hash_f64(const double*   src, uint64_t* dst, size_t count);

// Fused gather-and-mix for dict-encoded columns. Reads a packed-code per
// row, indexes into a K-entry uint64 lookup table, and mixes the result
// into dest[] in place — eliminating the per-chunk scratch buffer used
// by the scatter+simd_mix_hash pattern.
//
// Three specializations by code width (bytes per packed code):
//   cw1: 1-byte codes (dict_size <= 256)
//   cw2: 2-byte codes (dict_size <= 65536)
//   cw4: 4-byte codes (larger dictionaries)
//
// Non-null variants assume every row carries a valid code. Null variants
// consult `null_bitmap` (one bit per row, little-endian) and substitute
// NULL_HASH for rows whose bit is 0. `start_row` is the absolute row
// index of dest[0]/codes[0] so the bitmap can be indexed correctly when
// dest is a sub-slice of a larger buffer.
void simd_mix_hash_from_dict_cw1(uint64_t* dest, const uint64_t* dict_lookup,
                                  const uint8_t* codes, size_t count);
void simd_mix_hash_from_dict_cw2(uint64_t* dest, const uint64_t* dict_lookup,
                                  const uint16_t* codes, size_t count);
void simd_mix_hash_from_dict_cw4(uint64_t* dest, const uint64_t* dict_lookup,
                                  const uint32_t* codes, size_t count);

void simd_mix_hash_from_dict_nullable_cw1(uint64_t* dest, const uint64_t* dict_lookup,
                                           const uint8_t* codes, const uint8_t* null_bitmap,
                                           size_t start_row, size_t count);
void simd_mix_hash_from_dict_nullable_cw2(uint64_t* dest, const uint64_t* dict_lookup,
                                           const uint16_t* codes, const uint8_t* null_bitmap,
                                           size_t start_row, size_t count);
void simd_mix_hash_from_dict_nullable_cw4(uint64_t* dest, const uint64_t* dict_lookup,
                                           const uint32_t* codes, const uint8_t* null_bitmap,
                                           size_t start_row, size_t count);

// Scale int32 day-offsets to int64 microseconds (multiply by 86400000000).
// dest[i] = (int64_t)src[i] * 86400000000LL  for i in [0, count)
void simd_scale_date32(const int32_t* src, int64_t* dest, size_t count);

#ifdef __cplusplus
}
#endif
