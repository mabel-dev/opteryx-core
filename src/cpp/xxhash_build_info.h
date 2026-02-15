#pragma once

#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

/* Return a short string describing the vector backend compiled into xxhash
 * ("avx2", "neon", "scalar"). This is intended only for diagnostics / CI
 * verification and does not affect runtime output.
 */
const char* opteryx_xxhash_compiled_vector(void);

#ifdef __cplusplus
}
#endif
