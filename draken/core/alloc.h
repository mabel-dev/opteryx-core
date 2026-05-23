// Draken allocation surface.
//
// Single entry point for every transferable buffer the new draken vector model
// owns. The per-vector RAII ownership layer (unique_ptr + stateless deleter,
// Milestone B) frees through draken_free; everything that allocates an owned
// buffer goes through draken_malloc / draken_aligned_malloc. One allocator,
// called explicitly — mimalloc's process-wide malloc override is OFF (see
// setup.py and 01_ownership.md), so draken_old and the rest of the engine keep
// the system allocator untouched.
#ifndef DRAKEN_CORE_ALLOC_H
#define DRAKEN_CORE_ALLOC_H

#include <mimalloc.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

static inline void* draken_malloc(size_t size) { return mi_malloc(size); }

static inline void* draken_aligned_malloc(size_t size, size_t alignment) {
    return mi_malloc_aligned(size, alignment);
}

static inline void draken_free(void* ptr) { mi_free(ptr); }

#ifdef __cplusplus
}  // extern "C"
#endif

#endif  // DRAKEN_CORE_ALLOC_H
