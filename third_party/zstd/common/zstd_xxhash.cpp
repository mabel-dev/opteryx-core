/*
 * Instantiates zstd's ZSTD_-namespaced XXH64 from the canonical vendored
 * xxHash (third_party/cyan4973) — see zstd_xxhash.h.
 */

#define XXH_STATIC_LINKING_ONLY /* access advanced declarations */
#define XXH_IMPLEMENTATION      /* access definitions */

#include "zstd_xxhash.h"
