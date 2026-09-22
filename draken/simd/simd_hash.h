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

#ifdef __cplusplus
}
#endif
