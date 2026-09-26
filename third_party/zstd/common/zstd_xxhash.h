/*
 * Local adaptation for Zstandard.
 *
 * zstd does not carry its own xxHash: it uses the single vendored copy in
 * third_party/cyan4973, configured the way upstream zstd configures its
 * bundled copy — XXH64 only (no XXH3), with every public symbol prefixed
 * ZSTD_ so it can never collide with another instantiation.
 *
 * zstd sources include THIS header, never "xxhash.h" directly: zstd/common is
 * on several extensions' include paths, so a bare "xxhash.h" here would shadow
 * or be shadowed by the canonical header depending on -I order.
 */
#ifndef ZSTD_XXHASH_H
#define ZSTD_XXHASH_H

#ifndef XXH_NO_XXH3
# define XXH_NO_XXH3
#endif

#ifndef XXH_NAMESPACE
# define XXH_NAMESPACE ZSTD_
#endif

#include "../../cyan4973/xxhash.h"

#endif /* ZSTD_XXHASH_H */
