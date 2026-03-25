#pragma once

// cpu_features.h
//
// Compile-time SIMD capability probes.
//
// These functions are intentionally static inline so every translation unit
// that includes this header gets a self-contained copy with no external
// linkage dependency.  The results are fully determined at compile time by
// the architecture macros set via compiler flags (-mavx2, -march=armv8-a,
// etc.), which is correct: we target a fixed ABI per build and do not need
// to probe the host CPU at runtime.
//
// Rationale: runtime CPU probing was used historically to dispatch between
// SIMD paths inside carchar and other hot-path code.  Because the build
// system already selects the appropriate arch flags at configuration time
// (x86_64 gets -mavx2, aarch64 gets implicit NEON), a runtime probe adds
// latency and a link-time dependency on cpu_features.cpp without providing
// any additional correctness guarantee.  Removing the runtime probe also
// eliminates the only reason carchar_native needed cpu_features.cpp as a
// compiled source.

#include <stdbool.h>

#ifdef __cplusplus
extern "C" {
#endif

static inline bool cpu_supports_avx2(void) {
#if defined(__AVX2__)
    return true;
#else
    return false;
#endif
}

static inline bool cpu_supports_neon(void) {
#if defined(__ARM_NEON) || defined(__ARM_NEON__)
    return true;
#else
    return false;
#endif
}

#ifdef __cplusplus
}
#endif