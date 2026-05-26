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
#include <stdlib.h>
#include <stdio.h>
#include <execinfo.h>
#include <unistd.h>

#ifdef __cplusplus
extern "C" {
#endif

static inline void* draken_malloc(size_t size) { return mi_malloc(size); }

static inline void* draken_aligned_malloc(size_t size, size_t alignment) {
    return mi_malloc_aligned(size, alignment);
}

static inline void draken_free(void* ptr) {
    if (ptr == nullptr) {
        return;
    }

    /* Optional diagnostic tracing: set OPTERYX_FREE_TRACE=1 to enable.
     * Further filtering is supported via OPTERYX_FREE_TRACE_MIN / _MAX (bytes).
     * When enabled we print the allocation size (mi_usable_size) and a backtrace
     * to stderr to help identify who freed a buffer. This is temporary debug
     * instrumentation and should be removed after the bug is found.
     */
    const char* trace_env = getenv("OPTERYX_FREE_TRACE");
    if (trace_env && trace_env[0]) {
        /* Gather size (mimalloc) and apply optional min/max filters. */
        size_t asize = 0;
        /* mi_usable_size is available from mimalloc.h */
        asize = mi_usable_size(ptr);

        const char* min_env = getenv("OPTERYX_FREE_TRACE_MIN");
        const char* max_env = getenv("OPTERYX_FREE_TRACE_MAX");
        size_t min_sz = 0;
        size_t max_sz = (size_t)-1;
        if (min_env) min_sz = (size_t)strtoull(min_env, NULL, 10);
        if (max_env) max_sz = (size_t)strtoull(max_env, NULL, 10);

        if (asize >= min_sz && asize <= max_sz) {
            /* Print header + size */
            fprintf(stderr, "DRAKEN_FREE TRACE: ptr=%p size=%zu\n", ptr, asize);

            /* Backtrace — use execinfo if available. */
            void* bt[32];
            int bt_sz = backtrace(bt, 32);
            backtrace_symbols_fd(bt, bt_sz, STDERR_FILENO);
            fprintf(stderr, "-- end DRAKEN_FREE TRACE --\n");
            fflush(stderr);
        }
    }

    mi_free(ptr);
}

#ifdef __cplusplus
}  // extern "C"
#endif

#endif  // DRAKEN_CORE_ALLOC_H
