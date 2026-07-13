#pragma once
// draken/core/alloc_trace.h — DIAGNOSTIC-ONLY, TEMPORARY.
//
// Measurement pass for the arena-allocator scoping question: does an arena
// belong at operator scope, morsel scope, or something else? This file
// records every draken_malloc / draken_aligned_malloc / draken_free call as
// a flat event tagged with the immediate caller's return address, so
// allocations can be attributed to a call site OFFLINE (addr2line / atos)
// without any state shared across the separately-compiled extension .so's
// that include draken/core/alloc.h.
//
// Why return-address capture, not a propagated owner/morsel tag: this
// codebase already hit the cross-.so thread_local trap once
// (draken/ops/kernels/error_handling.cpp's g_error_message is compiled into
// more than one extension .so, so each gets its own private copy and a
// shared accessor silently reads the wrong instance — see that file's
// header comment). The operator/morsel execution context lives in
// src/cpp/engine/executor.hpp, compiled into a DIFFERENT .so than most
// draken_malloc call sites, so a propagated thread-local owner stack would
// hit the identical failure mode. Return-address capture needs no shared
// state: it is resolved entirely from the CPU's own return-address
// register/stack slot at the call site, so it works identically no matter
// which .so calls draken_malloc. The trade-off, accepted explicitly: this
// data attributes allocations to a call site (kernel/function), not to a
// query-plan operator or morsel.
//
// Header-only, `static inline` — like alloc.h itself, this is duplicated
// per translation-unit by design. Each `.so`'s thread-local event buffer is
// independent and flushed to its own file at thread/process exit; nothing
// here relies on cross-.so symbol resolution.
//
// Gate: compiled in only when OPTERYX_ALLOC_TRACE is defined (see
// OPTERYX_ENABLE_ALLOC_TRACE in build_common.py) — every call site in
// alloc.h is wrapped in #ifdef, so an unset macro compiles to nothing.
// Within an OPTERYX_ALLOC_TRACE build, recording is further gated by the
// OPTERYX_ALLOC_TRACE=1 environment variable so an instrumented build is
// not silently always-recording.
//
// Correctness note: draken_malloc/draken_aligned_malloc/draken_free MUST be
// marked noinline in an OPTERYX_ALLOC_TRACE build (see DRAKEN_ALLOC_NOINLINE
// in alloc.h) — if the optimizer inlines them into a caller,
// __builtin_return_address(0) evaluated inside would resolve to whatever
// called THAT caller (one level too far up), non-uniformly across call
// sites depending on the inliner's per-site heuristics. Forcing noinline in
// this build only makes attribution reliable and uniform; it does not
// affect the normal (macro-off) build at all.
//
// TEMPORARY: this file exists only to collect allocator-scope data for one
// throwaway measurement pass. Delete it, do not maintain it.

#ifdef OPTERYX_ALLOC_TRACE

#if !defined(__GNUC__) && !defined(__clang__)
#error "OPTERYX_ALLOC_TRACE requires __builtin_return_address (GCC/Clang); unsupported compiler"
#endif

#include <atomic>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <dlfcn.h>
#include <sys/stat.h>
#include <unistd.h>

#if defined(__APPLE__)
#include <pthread.h>
#elif defined(__linux__)
#include <sys/syscall.h>
#endif

namespace opteryx_alloc_trace {

enum EventKind : uint8_t { FREE = 0, ALLOC = 1 };

// 32 bytes, POD, cheap to append — no ctor/dtor work per record.
struct Event {
    uint64_t ts_ns;
    uint64_t ptr;
    uint64_t retaddr;
    uint32_t size;
    uint8_t  kind;
    uint8_t  _pad[3] = {0, 0, 0};
};
static_assert(sizeof(Event) == 32, "Event layout drifted; update analyze_alloc_scope.py");

// Runtime activation, resolved once (magic-static init is thread-safe),
// same pattern as draken_trace_config() in alloc.h. getenv is a linear
// environ scan; doing it once and caching avoids paying that cost per call.
inline bool trace_enabled() {
    static const bool e = []() {
        const char* v = getenv("OPTERYX_ALLOC_TRACE");
        return v != nullptr && v[0] != '\0' && v[0] != '0';
    }();
    return e;
}

inline const char* trace_dir() {
    static const char* d = []() -> const char* {
        const char* v = getenv("OPTERYX_ALLOC_TRACE_DIR");
        return (v != nullptr && v[0] != '\0') ? v : "/tmp/opteryx_alloc_trace";
    }();
    return d;
}

inline uint64_t now_ns() {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return static_cast<uint64_t>(ts.tv_sec) * 1000000000ull + static_cast<uint64_t>(ts.tv_nsec);
}

inline int current_tid() {
#if defined(__APPLE__)
    uint64_t tid64 = 0;
    pthread_threadid_np(nullptr, &tid64);
    return static_cast<int>(tid64);
#elif defined(__linux__)
    return static_cast<int>(syscall(SYS_gettid));
#else
    return 0;
#endif
}

// Events per thread. ~4M * 32B = 128MB/thread — generous for a single
// TPC-H-scale query without reallocation mid-run. Overflow fails loud
// (stops recording, flags truncated) rather than silently wrapping/dropping.
constexpr size_t kCapacity = 4ull * 1024 * 1024;

inline std::atomic<uint64_t>& buffer_seq() {
    static std::atomic<uint64_t> seq{0};
    return seq;
}

struct ThreadBuffer {
    Event* events = nullptr;
    size_t count = 0;
    bool truncated = false;
    uint64_t seq = 0;

    void ensure() {
        if (events == nullptr) {
            events = static_cast<Event*>(malloc(kCapacity * sizeof(Event)));
            seq = buffer_seq().fetch_add(1, std::memory_order_relaxed);
        }
    }

    void record(EventKind kind, const void* ptr, size_t size, void* retaddr) {
        ensure();
        if (events == nullptr) return;  // OOM allocating the trace buffer itself; nothing to record into
        if (count >= kCapacity) {
            truncated = true;
            return;
        }
        events[count++] = Event{now_ns(), reinterpret_cast<uint64_t>(ptr),
                                 reinterpret_cast<uint64_t>(retaddr), static_cast<uint32_t>(size),
                                 static_cast<uint8_t>(kind), {0, 0, 0}};
    }

    // Identify which .so this translation unit was compiled into, and its
    // load base, so the offline symbolizer can turn a runtime return
    // address into a file-relative offset for addr2line/atos (PIC/PIE
    // shared objects are loaded at an ASLR-randomized base).
    static void module_info(const char** so_path, uintptr_t* so_base) {
        Dl_info info;
        // Any address inside this TU's own code resolves to this .so.
        if (dladdr(reinterpret_cast<void*>(&ThreadBuffer::module_info), &info) && info.dli_fname) {
            *so_path = info.dli_fname;
            *so_base = reinterpret_cast<uintptr_t>(info.dli_fbase);
        } else {
            *so_path = "";
            *so_base = 0;
        }
    }

    void flush() {
        if (count == 0 && !truncated) {
            free(events);
            events = nullptr;
            return;
        }
        mkdir(trace_dir(), 0755);  // best-effort; ignore EEXIST/errors, open() below fails loud instead

        const char* so_path = "";
        uintptr_t so_base = 0;
        module_info(&so_path, &so_base);

        char path[1024];
        snprintf(path, sizeof(path), "%s/alloc_trace.%d.%llu.%d.bin", trace_dir(), getpid(),
                  static_cast<unsigned long long>(seq), current_tid());

        FILE* f = fopen(path, "wb");
        if (f == nullptr) {
            fprintf(stderr, "OPTERYX_ALLOC_TRACE: failed to open %s for writing (%d events lost)\n",
                    path, static_cast<int>(count));
            free(events);
            events = nullptr;
            return;
        }
        fprintf(f, "OPTERYX_ALLOC_TRACE v1\n");
        fprintf(f, "so_path=%s\n", so_path);
        fprintf(f, "so_base=0x%llx\n", static_cast<unsigned long long>(so_base));
        fprintf(f, "pid=%d\n", getpid());
        fprintf(f, "tid=%d\n", current_tid());
        fprintf(f, "count=%zu\n", count);
        fprintf(f, "truncated=%d\n", truncated ? 1 : 0);
        fprintf(f, "---BINARY---\n");
        if (count > 0) {
            fwrite(events, sizeof(Event), count, f);
        }
        fclose(f);

        free(events);
        events = nullptr;
    }

    // Flushes at thread-local storage duration end (thread exit, or process
    // exit for buffers owned by long-lived worker-pool threads). Never
    // flushed mid-run — I/O here would perturb the very timing being
    // measured, so it only happens once, after all recording for this
    // thread is done.
    ~ThreadBuffer() { flush(); }
};

inline ThreadBuffer& tls_buffer() {
    static thread_local ThreadBuffer buf;
    return buf;
}

inline void record_alloc(const void* ptr, size_t size, void* retaddr) {
    if (!trace_enabled() || ptr == nullptr) return;
    tls_buffer().record(ALLOC, ptr, size, retaddr);
}

inline void record_free(const void* ptr, void* retaddr) {
    if (!trace_enabled() || ptr == nullptr) return;
    tls_buffer().record(FREE, ptr, 0, retaddr);
}

}  // namespace opteryx_alloc_trace

#endif  // OPTERYX_ALLOC_TRACE
