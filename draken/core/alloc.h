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

// DIAGNOSTIC-ONLY, TEMPORARY: allocator-scope measurement pass. See
// alloc_trace.h for what this records and why it's return-address-based
// rather than a propagated owner tag. No-ops entirely unless
// OPTERYX_ALLOC_TRACE is defined at compile time.
#include "alloc_trace.h"

#ifdef OPTERYX_ALLOC_TRACE
// Must not be inlined into callers in this build — see alloc_trace.h's
// header comment: __builtin_return_address(0) needs draken_malloc/
// draken_aligned_malloc/draken_free to be real call frames to attribute
// allocations to their actual caller. No effect on the normal build.
#define DRAKEN_ALLOC_NOINLINE __attribute__((noinline))
#else
#define DRAKEN_ALLOC_NOINLINE
#endif

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

// Diagnostic-trace configuration, resolved from the environment EXACTLY ONCE
// per process (magic-static init is thread-safe). getenv is a linear scan of
// `environ`; calling it on every malloc/free — the hottest allocation path in
// the engine — cost 2–6 environ scans per vector op. Mid-process toggling of
// OPTERYX_FREE_TRACE* is no longer honoured (it never needed to be); the vars
// must be set at process start.
struct DrakenTraceConfig {
    int    enabled;
    size_t min_sz;
    size_t max_sz;
};

static inline const DrakenTraceConfig* draken_trace_config(void) {
    static const DrakenTraceConfig cfg = []() {
        DrakenTraceConfig c;
        const char* trace_env = getenv("OPTERYX_FREE_TRACE");
        c.enabled = (trace_env && trace_env[0]) ? 1 : 0;
        const char* min_env = getenv("OPTERYX_FREE_TRACE_MIN");
        const char* max_env = getenv("OPTERYX_FREE_TRACE_MAX");
        c.min_sz = min_env ? (size_t)strtoull(min_env, NULL, 10) : (size_t)0;
        c.max_sz = max_env ? (size_t)strtoull(max_env, NULL, 10) : (size_t)-1;
        return c;
    }();
    return &cfg;
}

static inline DRAKEN_ALLOC_NOINLINE void* draken_malloc(size_t size) {
    void* p = malloc(size);
#ifdef OPTERYX_ALLOC_TRACE
    opteryx_alloc_trace::record_alloc(p, size, __builtin_return_address(0));
#endif
    const DrakenTraceConfig* tc = draken_trace_config();
    if (tc->enabled && p != nullptr) {
        size_t asize = DRAKEN_USABLE_SIZE(p);
        size_t min_sz = tc->min_sz;
        size_t max_sz = tc->max_sz;
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

static inline DRAKEN_ALLOC_NOINLINE void* draken_aligned_malloc(size_t size, size_t alignment) {
    // posix_memalign requires alignment to be a power of two and a multiple of
    // sizeof(void*); clamp small alignments up (over-alignment is harmless).
    if (alignment < sizeof(void*)) alignment = sizeof(void*);
    void* p = nullptr;
    if (posix_memalign(&p, alignment, size) != 0) p = nullptr;
#ifdef OPTERYX_ALLOC_TRACE
    opteryx_alloc_trace::record_alloc(p, size, __builtin_return_address(0));
#endif
    const DrakenTraceConfig* tc = draken_trace_config();
    if (tc->enabled && p != nullptr) {
        size_t asize = DRAKEN_USABLE_SIZE(p);
        size_t min_sz = tc->min_sz;
        size_t max_sz = tc->max_sz;
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

static inline DRAKEN_ALLOC_NOINLINE void draken_free(void* ptr) {
    if (ptr == nullptr) {
        return;
    }

#ifdef OPTERYX_ALLOC_TRACE
    opteryx_alloc_trace::record_free(ptr, __builtin_return_address(0));
#endif

    /* Optional diagnostic tracing: set OPTERYX_FREE_TRACE=1 to enable.
     * Further filtering is supported via OPTERYX_FREE_TRACE_MIN / _MAX (bytes).
     * Config is resolved once at process start (see draken_trace_config).
     */
    const DrakenTraceConfig* tc = draken_trace_config();
    if (tc->enabled) {
        size_t asize = DRAKEN_USABLE_SIZE(ptr);
        size_t min_sz = tc->min_sz;
        size_t max_sz = tc->max_sz;

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
