#pragma once
// Compile-time single-byte search dispatch for the DFA extractor.
//
// The build targets a fixed architecture, so the NEON-vs-AVX choice is a
// compile-time fact, not a per-call runtime decision. Resolving it here lets
// the compiler emit a direct call (and inline this wrapper) instead of the
// per-call indirect jump through a module-level function pointer.
#include <cstddef>
#include "simd_search.h"

static inline int dfa_find_char(const char* data, size_t length, char target) {
#if defined(__ARM_NEON) || defined(__ARM_NEON__)
    return neon_search(data, length, target);
#else
    return avx_search(data, length, target);
#endif
}
