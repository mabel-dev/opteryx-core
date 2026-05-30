// Draken allocation surface.
//
// Single entry point for every transferable buffer the new draken vector model
// owns. The per-vector RAII ownership layer (unique_ptr + stateless deleter,
// Milestone B) frees through draken_free; everything that allocates an owned
// buffer goes through draken_malloc / draken_aligned_malloc.
//
// Allocator: the system allocator (malloc/free). draken buffers cross extension
// boundaries (e.g. column_deserializer allocates, a draken Vector frees on GC),
// so the allocator MUST be a single process-wide instance shared by every
// extension. The system allocator is exactly that, and — unlike a bundled
// mimalloc — it coexists with foreign native libraries (pandas/pyarrow/…)
// loaded into the same process at any point. mimalloc may return later as an
// opt-in, measured prod build behind a flag.
#ifndef DRAKEN_CORE_ALLOC_H
#define DRAKEN_CORE_ALLOC_H

#include <stddef.h>
#include <stdlib.h>
#include <stdio.h>
#include <execinfo.h>
#include <unistd.h>

#if defined(__APPLE__)
#include <malloc/malloc.h>   // malloc_size
#define DRAKEN_USABLE_SIZE(p) malloc_size(p)
#elif defined(__linux__)
#include <malloc.h>          // malloc_usable_size
#define DRAKEN_USABLE_SIZE(p) malloc_usable_size(p)
#else
#define DRAKEN_USABLE_SIZE(p) ((size_t)0)
#endif

#ifdef __cplusplus
extern "C" {
#endif

static inline void* draken_malloc(size_t size) {
    void* p = malloc(size);
    const char* trace_env = getenv("OPTERYX_FREE_TRACE");
    if (trace_env && trace_env[0] && p != nullptr) {
        size_t asize = DRAKEN_USABLE_SIZE(p);
        const char* min_env = getenv("OPTERYX_FREE_TRACE_MIN");
        const char* max_env = getenv("OPTERYX_FREE_TRACE_MAX");
        size_t min_sz = 0;
        size_t max_sz = (size_t)-1;
        if (min_env) min_sz = (size_t)strtoull(min_env, NULL, 10);
        if (max_env) max_sz = (size_t)strtoull(max_env, NULL, 10);
        if (asize >= min_sz && asize <= max_sz) {
            fprintf(stderr, "DRAKEN_MALLOC TRACE: ptr=%p req=%zu size=%zu\n", p, size, asize);
            void* bt[32];
            int bt_sz = backtrace(bt, 32);
            backtrace_symbols_fd(bt, bt_sz, STDERR_FILENO);
            fprintf(stderr, "-- end DRAKEN_MALLOC TRACE --\n");
            fflush(stderr);
        }
    }
    return p;
}

static inline void* draken_aligned_malloc(size_t size, size_t alignment) {
    // posix_memalign requires alignment to be a power of two and a multiple of
    // sizeof(void*); clamp small alignments up (over-alignment is harmless).
    if (alignment < sizeof(void*)) alignment = sizeof(void*);
    void* p = nullptr;
    if (posix_memalign(&p, alignment, size) != 0) p = nullptr;
    const char* trace_env = getenv("OPTERYX_FREE_TRACE");
    if (trace_env && trace_env[0] && p != nullptr) {
        size_t asize = DRAKEN_USABLE_SIZE(p);
        const char* min_env = getenv("OPTERYX_FREE_TRACE_MIN");
        const char* max_env = getenv("OPTERYX_FREE_TRACE_MAX");
        size_t min_sz = 0;
        size_t max_sz = (size_t)-1;
        if (min_env) min_sz = (size_t)strtoull(min_env, NULL, 10);
        if (max_env) max_sz = (size_t)strtoull(max_env, NULL, 10);
        if (asize >= min_sz && asize <= max_sz) {
            fprintf(stderr, "DRAKEN_ALIGNED_MALLOC TRACE: ptr=%p req=%zu align=%zu size=%zu\n", p, size, alignment, asize);
            void* bt[32];
            int bt_sz = backtrace(bt, 32);
            backtrace_symbols_fd(bt, bt_sz, STDERR_FILENO);
            fprintf(stderr, "-- end DRAKEN_ALIGNED_MALLOC TRACE --\n");
            fflush(stderr);
        }
    }
    return p;
}

static inline void draken_free(void* ptr) {
    if (ptr == nullptr) {
        return;
    }

    /* Optional diagnostic tracing: set OPTERYX_FREE_TRACE=1 to enable.
     * Further filtering is supported via OPTERYX_FREE_TRACE_MIN / _MAX (bytes).
     */
    const char* trace_env = getenv("OPTERYX_FREE_TRACE");
    if (trace_env && trace_env[0]) {
        size_t asize = DRAKEN_USABLE_SIZE(ptr);
        const char* min_env = getenv("OPTERYX_FREE_TRACE_MIN");
        const char* max_env = getenv("OPTERYX_FREE_TRACE_MAX");
        size_t min_sz = 0;
        size_t max_sz = (size_t)-1;
        if (min_env) min_sz = (size_t)strtoull(min_env, NULL, 10);
        if (max_env) max_sz = (size_t)strtoull(max_env, NULL, 10);

        if (asize >= min_sz && asize <= max_sz) {
            fprintf(stderr, "DRAKEN_FREE TRACE: ptr=%p size=%zu\n", ptr, asize);
            void* bt[32];
            int bt_sz = backtrace(bt, 32);
            backtrace_symbols_fd(bt, bt_sz, STDERR_FILENO);
            fprintf(stderr, "-- end DRAKEN_FREE TRACE --\n");
            fflush(stderr);
        }
    }

    free(ptr);
}

#ifdef __cplusplus
}  // extern "C"
#endif

#endif  // DRAKEN_CORE_ALLOC_H
