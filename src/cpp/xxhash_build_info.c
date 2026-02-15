#include "xxhash_build_info.h"

/* Probe compile-time macros / xxHash selection and return a short backend name. */
const char* opteryx_xxhash_compiled_vector(void)
{
#if defined(XXH_VECTOR)
    /* Prefer xxHash's XXH_VECTOR if available */
    #if XXH_VECTOR == XXH_AVX2
        return "avx2";
    #elif XXH_VECTOR == XXH_NEON
        return "neon";
    #else
        return "scalar";
    #endif
#else
    /* Fallback to compiler feature checks */
    #if defined(__AVX2__)
        return "avx2";
    #elif defined(__ARM_NEON) || defined(__ARM_NEON__) || defined(__aarch64__)
        return "neon";
    #else
        return "scalar";
    #endif
#endif
}
