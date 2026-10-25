// Draken allocation surface.
//
// Single entry point for every transferable buffer the new draken vector model
// owns. The per-vector RAII ownership layer (unique_ptr + stateless deleter,
// Milestone B) frees through draken_free; everything that allocates an owned
// buffer goes through draken_malloc / draken_calloc / draken_aligned_malloc.
//
// Allocator: two tiers, one process-wide instance each. Requests below
// DRAKEN_SLAB_MIN (64 KiB) are the system allocator's (malloc/free); requests
// from DRAKEN_SLAB_MIN up come from the draken slab pool (slab_pool.h): a
// reservation of 2 MiB slots faulted once and recycled for the life of the
// process, huge-page backed on Linux. draken buffers cross extension boundaries
// (e.g. column_deserializer allocates, a draken Vector frees on GC), so both
// tiers MUST be single process-wide instances shared by every extension: the
// system allocator is that by nature, and the pool lives in draken_native only.
// A pointer's tier is one range compare against the reservation (draken_slab_owns),
// so draken_free needs no header and no hint. The system tier coexists with
// foreign native libraries (pandas/pyarrow/...) loaded into the same process at
// any point; mimalloc may return later as an opt-in, measured prod build behind
// a flag.
#ifndef DRAKEN_CORE_ALLOC_H
#define DRAKEN_CORE_ALLOC_H

#include <stddef.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
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

#include "mem_account.h"
#include "slab_pool.h"

#ifdef __cplusplus
extern "C" {
#endif

// 1 when p was handed out by the slab pool. Two loads and one compare; false
// for NULL and before the pool exists (base == end == 0).
static inline int draken_slab_owns(const void* p) {
    return ((uintptr_t)p - draken_slab_base) < (draken_slab_end - draken_slab_base);
}

// Usable size of any draken-allocated block, whichever tier holds it.
static inline size_t draken_usable_size(const void* p) {
    return draken_slab_owns(p) ? draken_slab_usable_size(p) : DRAKEN_USABLE_SIZE((void*)p);
}

// Process memory account (mem_account.h): system-tier blocks of at least
// DRAKEN_MEM_ACCOUNT_MIN usable bytes are charged when allocated and uncharged
// when freed. Both sides use the allocator's usable size, so the pair is exact
// without the free side knowing the request. Pool blocks are NOT noted here:
// the pool charges its slots when it faults them in and uncharges when it gives
// them back (slab_pool.h) - memory the process holds, used or retained.
static inline void draken_mem_note_alloc(void* p) {
    if (p == nullptr) return;
    const int64_t n = (int64_t)DRAKEN_USABLE_SIZE(p);
    if (n >= DRAKEN_MEM_ACCOUNT_MIN) draken_mem_charge(n);
}

static inline void draken_mem_note_free(void* p) {
    if (p == nullptr) return;
    const int64_t n = (int64_t)DRAKEN_USABLE_SIZE(p);
    if (n >= DRAKEN_MEM_ACCOUNT_MIN) draken_mem_uncharge(n);
}

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

static inline void draken_alloc_trace(const char* what, void* p, size_t req) {
    const DrakenTraceConfig* tc = draken_trace_config();
    if (!tc->enabled || p == nullptr) return;
    const size_t asize = draken_usable_size(p);
    if (asize < tc->min_sz || asize > tc->max_sz) return;
    fprintf(stderr, "%s TRACE: ptr=%p req=%zu size=%zu%s\n", what, p, req, asize,
            draken_slab_owns(p) ? " tier=slab" : "");
    void* bt[32];
    int bt_sz = backtrace(bt, 32);
    backtrace_symbols_fd(bt, bt_sz, STDERR_FILENO);
    fprintf(stderr, "-- end %s TRACE --\n", what);
    fflush(stderr);
}

static inline void* draken_malloc(size_t size) {
    void* p;
    if (size >= DRAKEN_SLAB_MIN) {
        p = draken_slab_alloc(size);
    } else {
        p = malloc(size);
        draken_mem_note_alloc(p);
    }
    draken_alloc_trace("DRAKEN_MALLOC", p, size);
    return p;
}

// Zero-initialised allocation. Below the pool threshold it delegates to calloc
// (fresh pages from the OS are already zero, so the system allocator can skip
// the write). A pool block is recycled memory and is zeroed explicitly: the
// write over an already-faulted block is what replaces the page faults the
// block used to cost. Returns NULL on failure, like draken_malloc - it does not
// throw. Pairs with draken_free.
static inline void* draken_calloc(size_t count, size_t size) {
    void* p;
    const size_t total = count * size;
    if (size != 0 && total / size != count) {
        p = nullptr;   // overflow, as calloc would refuse
    } else if (total >= DRAKEN_SLAB_MIN) {
        p = draken_slab_alloc(total);
        if (p != nullptr) memset(p, 0, total);
    } else {
        p = calloc(count, size);
        draken_mem_note_alloc(p);
    }
    draken_alloc_trace("DRAKEN_CALLOC", p, total);
    return p;
}

// Resize a draken_malloc'd block, preserving its contents up to the smaller
// size; new bytes are uninitialised. Within a tier the allocator may grow the
// block in place (a pool run extends over free slots after it; the system
// allocator may remap) rather than copy; a block that crosses the threshold
// moves tier with one copy. Returns NULL on failure with the original block
// untouched, like realloc. Pairs with draken_free.
static inline void* draken_realloc(void* ptr, size_t size) {
    void* p;
    if (ptr == nullptr) {
        p = draken_malloc(size);
    } else if (draken_slab_owns(ptr)) {
        if (size >= DRAKEN_SLAB_MIN) {
            p = draken_slab_realloc(ptr, size);
        } else {
            p = malloc(size);
            if (p != nullptr) {
                memcpy(p, ptr, size);
                draken_mem_note_alloc(p);
                draken_slab_free(ptr);
            }
        }
    } else if (size >= DRAKEN_SLAB_MIN) {
        p = draken_slab_alloc(size);
        if (p != nullptr) {
            const size_t old = DRAKEN_USABLE_SIZE(ptr);
            memcpy(p, ptr, old < size ? old : size);
            draken_mem_note_free(ptr);
            free(ptr);
        }
    } else {
        draken_mem_note_free(ptr);
        p = realloc(ptr, size);
        // On failure realloc leaves the original block untouched: re-charge it.
        // realloc(ptr, 0) may free ptr and return NULL - then there is nothing left.
        draken_mem_note_alloc(p != nullptr ? p : (size != 0 ? ptr : nullptr));
    }
    draken_alloc_trace("DRAKEN_REALLOC", p, size);
    return p;
}

static inline void* draken_aligned_malloc(size_t size, size_t alignment) {
    void* p = nullptr;
    if (size >= DRAKEN_SLAB_MIN) {
        // Pool blocks are DRAKEN_SLAB_MIN-aligned (class blocks) or slot-aligned
        // (runs): an alignment up to the slot is met by a run, above it refused.
        if (alignment > DRAKEN_SLAB_SLOT) return nullptr;
        p = draken_slab_alloc(alignment > DRAKEN_SLAB_MIN && size < DRAKEN_SLAB_SLOT ? DRAKEN_SLAB_SLOT : size);
    } else {
        // posix_memalign requires alignment to be a power of two and a multiple of
        // sizeof(void*); clamp small alignments up (over-alignment is harmless).
        if (alignment < sizeof(void*)) alignment = sizeof(void*);
        if (posix_memalign(&p, alignment, size) != 0) p = nullptr;
        draken_mem_note_alloc(p);
    }
    draken_alloc_trace("DRAKEN_ALIGNED_MALLOC", p, size);
    return p;
}

static inline void draken_free(void* ptr) {
    if (ptr == nullptr) {
        return;
    }
    /* Optional diagnostic tracing: set OPTERYX_FREE_TRACE=1 to enable.
     * Further filtering is supported via OPTERYX_FREE_TRACE_MIN / _MAX (bytes).
     * Config is resolved once at process start (see draken_trace_config).
     */
    draken_alloc_trace("DRAKEN_FREE", ptr, 0);
    if (draken_slab_owns(ptr)) {
        draken_slab_free(ptr);
        return;
    }
    draken_mem_note_free(ptr);
    free(ptr);
}

#ifdef __cplusplus
}  // extern "C"
#endif

#endif  // DRAKEN_CORE_ALLOC_H
