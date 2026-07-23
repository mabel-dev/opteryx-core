#pragma once
// draken/core/trace.hpp — the shared execution-tracing primitive.
//
// See docs/EXECUTION_TRACING_DESIGN.md (opteryx-core root). A trace is a flat
// stream of fixed-layout POD spans on ONE shared monotonic clock, recorded
// into per-thread arenas with no lock/atomics on the append path, and drained
// once by whichever subsystem owns the Python boundary crossing.
//
// This lives in draken/core, not src/cpp/engine, because BOTH rugo (IO/decode
// spans) and the opteryx engine (operator spans) need to record onto the same
// timeline, and rugo/ is required to build standalone, opteryx-free (see the
// repo structure rules in the root CLAUDE.md) — it cannot depend on opteryx's
// src/cpp/engine headers. draken is the zero-dependency base both already sit
// on, so the shared clock/span/arena mechanism belongs here. opteryx's
// src/cpp/engine/trace.hpp is a thin re-export of this header into the
// opteryx::engine namespace; rugo's decode/IO probes (a later phase) include
// this header directly.
//
// Each subsystem keeps its OWN arena registry (draken_trace_registry() below
// is one process-wide registry shared by every includer — engine and rugo
// arenas are registered into the SAME list, since they share the same gate/
// query_seq and must all be walked by one drain() call at query teardown).

#include <atomic>
#include <cstdint>
#include <cstdlib>
#include <ctime>
#include <mutex>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

namespace draken_trace {

// Span categories are pure vocabulary (compile-time constants, no state) and
// are defined ONCE, canonically, in trace_bridge_c.h's DrakenTraceCategory —
// not re-declared here — so nothing can drift out of sync with it. This
// header only ever stores/passes a plain uint16_t category.

// One cache line. No strings, no heap touches on the append path — node_id/
// corr_id are the only identity carried; string resolution happens once at
// drain time (Engine::collect_trace_symbols on the opteryx side).
struct TraceSpan {
    uint64_t t_start_ns;
    uint64_t t_end_ns;    // 0 == still open; every trace_begin() on the
                           // same-thread path is closed by trace_end() before
                           // the owning call returns, so a drained span is
                           // never observed open.
    uint32_t query_seq;
    uint16_t category;    // TraceCategory
    uint16_t worker_id;
    uint32_t node_id;     // engine-assigned plan-node id; 0 == untagged
    uint32_t corr_id;     // row-group gather correlation; 0 == n/a
    uint32_t rg_idx;      // row-group index; 0xFFFFFFFF == n/a
    uint32_t rows;
    uint32_t bytes;
    uint32_t detail;
    uint32_t file_id;     // interned file path id (trace_intern_file); 0 == n/a
    uint32_t _reserved0[2];  // pad to 64B; future widening
};
static_assert(sizeof(TraceSpan) == 64, "TraceSpan is sized to stay one cache line");

// The single monotonic epoch every span — engine and rugo alike — timestamps
// against. CLOCK_MONOTONIC. Both subsystems MUST read time through this one
// function, not their own std::chrono::steady_clock/CLOCK_MONOTONIC reads, or
// their spans merely happen to agree by coincidence rather than by contract.
inline uint64_t trace_now_ns() {
    timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return static_cast<uint64_t>(ts.tv_sec) * 1000000000ull
         + static_cast<uint64_t>(ts.tv_nsec);
}

// ---- Runtime gate, driven from the surface (OPTERYX_TRACE) ----------------------
// A process-wide atomic, not a thread_local: engine worker threads AND rugo's
// io_pipeline threads are pooled and persist across queries, so there is no
// single per-thread "query start" hook to stamp a thread_local flag from. One
// relaxed atomic load per trace_begin() call is the practical equivalent of
// the runtime-gate contract — still a single, well-predicted branch when
// tracing is off. Set once, single-threaded, before a traced query's workers
// (engine AND rugo) are dispatched.
inline std::atomic<bool>     g_trace_enabled{false};
inline std::atomic<uint32_t> g_trace_query_seq{0};

inline void trace_set_enabled(bool on) {
    g_trace_enabled.store(on, std::memory_order_relaxed);
}
inline bool trace_enabled() {
    return g_trace_enabled.load(std::memory_order_relaxed);
}
// Bumps the trace generation. Every thread's arena (engine or rugo) lazily
// resets to empty the next time IT records a span tagged with the new
// generation (ThreadArena::maybe_reset) — no cross-thread reach-in required.
// Returns the new query_seq; the caller threads it through to trace_drain()
// at teardown.
inline void trace_intern_reset();  // forward decl; defined below with the intern table

// Query-wide row-group gather correlation id. MUST be minted here, not by
// each rugo::ParquetIOPipeline instance locally (that was the bug: a
// per-instance counter restarting at 1 for every new pipeline object — and a
// single query can open more than one, e.g. across scan passes/retries —
// collided corr_id 1..N from one instance with 1..N from another, silently
// conflating unrelated row groups' spans in interpret_trace()/dev/io_waterfall's
// grouping). One shared counter, reset alongside query_seq, guarantees
// uniqueness across every pipeline instance touched by one query, in
// whichever .so recorded them (this lives in the same bridge-owned state as
// trace_intern_* — see docs/EXECUTION_TRACING_DESIGN.md).
inline std::atomic<uint32_t> g_trace_next_corr_id{1};

inline uint32_t trace_start_query() {
    trace_intern_reset();
    g_trace_next_corr_id.store(1, std::memory_order_relaxed);
    return g_trace_query_seq.fetch_add(1, std::memory_order_relaxed) + 1;
}
inline uint32_t trace_current_query_seq() {
    return g_trace_query_seq.load(std::memory_order_relaxed);
}
// 1-based (0 == "no correlation" sentinel, matching WorkItem::corr_id's
// existing convention). Thread-safe; called once per row-group submission.
inline uint32_t trace_next_corr_id() {
    return g_trace_next_corr_id.fetch_add(1, std::memory_order_relaxed);
}

// ---- File-path interning ----------------------------------------------------------
// Spans carry only a small file_id (never a string, to stay POD/hot-path-safe).
// The intern table maps that id back to the real path, resolved once at drain
// time — same shape as Engine::collect_trace_symbols() for node_id/identity,
// just for paths instead of plan-node identities. Reset per query (cleared at
// trace_start_query(), mirroring node_id's per-Engine-instance reset) — like
// the existing WP-INSTR GIL instrument, this makes the tracer a single-query
// diagnostic, not correct across concurrent traced queries in one process.
// That is an accepted, already-established limitation of this opt-in tracer,
// not new scope.
inline std::mutex& trace_intern_mutex() {
    static std::mutex m;
    return m;
}
inline std::unordered_map<std::string, uint32_t>& trace_intern_map() {
    static std::unordered_map<std::string, uint32_t> m;
    return m;
}
inline std::vector<std::string>& trace_intern_list() {
    static std::vector<std::string> v;
    return v;
}

// 1-based (0 == "no file"). Thread-safe; called once per row-group submission
// (rugo), not per-row — a mutexed map lookup is negligible at that rate.
inline uint32_t trace_intern_file(const std::string& path) {
    std::lock_guard<std::mutex> lock(trace_intern_mutex());
    auto it = trace_intern_map().find(path);
    if (it != trace_intern_map().end()) return it->second;
    trace_intern_list().push_back(path);
    uint32_t id = static_cast<uint32_t>(trace_intern_list().size());
    trace_intern_map().emplace(path, id);
    return id;
}

inline std::vector<std::pair<uint32_t, std::string>> trace_file_symbols() {
    std::lock_guard<std::mutex> lock(trace_intern_mutex());
    std::vector<std::pair<uint32_t, std::string>> out;
    out.reserve(trace_intern_list().size());
    for (size_t i = 0; i < trace_intern_list().size(); ++i)
        out.emplace_back(static_cast<uint32_t>(i + 1), trace_intern_list()[i]);
    return out;
}

inline void trace_intern_reset() {
    std::lock_guard<std::mutex> lock(trace_intern_mutex());
    trace_intern_map().clear();
    trace_intern_list().clear();
}

// Per-thread arena capacity. A diagnostic knob, read once per process
// (OPTERYX_TRACE_ARENA_SPANS), not re-read per query.
inline size_t trace_arena_capacity() {
    static const size_t cap = [] {
        const char* v = std::getenv("OPTERYX_TRACE_ARENA_SPANS");
        if (v && *v) {
            long long parsed = std::atoll(v);
            if (parsed > 0) return static_cast<size_t>(parsed);
        }
        return static_cast<size_t>(1000000);  // ~64MB/thread ceiling at default
    }();
    return cap;
}

// ---- Per-thread arena: lock-free bump-index append -------------------------------
// Exactly one per OS thread that ever records a span — an opteryx engine
// worker, an io_pipeline decode/fetch worker, or a drive thread. Touched ONLY
// by its owning thread while recording — no atomics needed there. Registered
// into the shared global list on first use (mutex-guarded, but once per
// thread's lifetime, not once per query) so a single draining thread can walk
// every arena — engine's and rugo's alike — after every worker has joined.
struct ThreadArena {
    std::vector<TraceSpan> spans;
    uint32_t local_query_seq = 0;
    bool truncated = false;

    void maybe_reset(uint32_t query_seq) {
        if (local_query_seq != query_seq) {
            spans.clear();
            truncated = false;
            local_query_seq = query_seq;
        }
    }
};

inline std::mutex& trace_registry_mutex() {
    static std::mutex m;
    return m;
}
inline std::vector<ThreadArena*>& trace_registry() {
    static std::vector<ThreadArena*> reg;
    return reg;
}

// Deliberately heap-allocated and NEVER freed — this is not a leak in the
// "forgot to clean up" sense, it's the fix for a real bug: row-group decode
// work (rugo/src/parquet/io_pipeline.hpp) does not necessarily run on a
// small, persistent pool of OS threads. Some of the threads that touch this
// tracer are short-lived (created for one unit of work, then exit). A
// thread_local ThreadArena VALUE would be destroyed when such a thread exits
// — but trace_registry() keeps a raw pointer to it for the rest of the
// process's life, to be walked by trace_drain() long after. That pointer
// would dangle the moment its owning thread exited, and trace_drain() reading
// it is undefined behavior — observed in practice as silently-empty arenas
// (freed memory happening to read back as zero), which is exactly why most
// of one large query's row-group spans were vanishing before this fix: the
// short-lived decode threads that recorded them had already exited by drain
// time. Heap-allocating and never deleting ties the arena's lifetime to the
// registry (process lifetime) instead of to whichever thread first touched
// it, so it stays valid no matter how long that thread lives.
inline ThreadArena& trace_thread_arena() {
    thread_local ThreadArena* arena_ptr = nullptr;
    if (arena_ptr == nullptr) {
        arena_ptr = new ThreadArena();
        std::lock_guard<std::mutex> lock(trace_registry_mutex());
        trace_registry().push_back(arena_ptr);
    }
    return *arena_ptr;
}

// ---- Recording API ----------------------------------------------------------------
// Handle for spans whose open/close happen on the SAME thread — operator/
// source/sink self-time, and rugo's fetch/decode phases within one
// decode_row_group() call. IO spans that cross threads (issue on the drive
// thread, completion on an io_pipeline worker) correlate via corr_id instead
// of sharing a handle.
struct TraceHandle {
    ThreadArena* arena = nullptr;
    size_t       slot  = SIZE_MAX;
};

inline TraceHandle trace_begin(uint16_t category, uint32_t node_id, uint32_t corr_id,
                                uint32_t rg_idx, int worker_id, uint32_t file_id = 0) {
    if (!trace_enabled()) return TraceHandle{};
    uint32_t seq = trace_current_query_seq();
    ThreadArena& arena = trace_thread_arena();
    arena.maybe_reset(seq);
    if (arena.spans.size() >= trace_arena_capacity()) {
        arena.truncated = true;
        return TraceHandle{};
    }
    arena.spans.push_back(TraceSpan{
        trace_now_ns(), 0, seq, category, static_cast<uint16_t>(worker_id),
        node_id, corr_id, rg_idx, 0, 0, 0, file_id, {0, 0}});
    return TraceHandle{&arena, arena.spans.size() - 1};
}

inline void trace_end(const TraceHandle& h, uint32_t rows, uint32_t bytes, uint32_t detail = 0) {
    if (h.arena == nullptr) return;
    TraceSpan& s = h.arena->spans[h.slot];
    s.t_end_ns = trace_now_ns();
    s.rows = rows;
    s.bytes = bytes;
    s.detail = detail;
}

// Record a span whose [t_start_ns, t_end_ns) is already known — e.g. rugo's
// row-group fetch/decode phases, whose durations are computed as running
// totals (total_read_ns/total_decode_ns) rather than bracketed live. Same
// gate/arena/truncation path as trace_begin/trace_end; just skips the "open
// now, close later" two-call shape when both timestamps are already in hand.
inline void trace_record(uint16_t category, uint32_t node_id, uint32_t corr_id,
                          uint32_t rg_idx, int worker_id,
                          uint64_t t_start_ns, uint64_t t_end_ns,
                          uint32_t rows, uint32_t bytes, uint32_t detail = 0,
                          uint32_t file_id = 0) {
    if (!trace_enabled()) return;
    uint32_t seq = trace_current_query_seq();
    ThreadArena& arena = trace_thread_arena();
    arena.maybe_reset(seq);
    if (arena.spans.size() >= trace_arena_capacity()) {
        arena.truncated = true;
        return;
    }
    arena.spans.push_back(TraceSpan{
        t_start_ns, t_end_ns, seq, category, static_cast<uint16_t>(worker_id),
        node_id, corr_id, rg_idx, rows, bytes, detail, file_id, {0, 0}});
}

// RAII scope for the common (same-thread) case.
struct TraceScope {
    TraceHandle h;
    uint32_t rows_ = 0, bytes_ = 0, detail_ = 0;
    TraceScope(uint16_t category, uint32_t node_id, int worker_id,
               uint32_t corr_id = 0, uint32_t rg_idx = 0xFFFFFFFFu, uint32_t file_id = 0)
        : h(trace_begin(category, node_id, corr_id, rg_idx, worker_id, file_id)) {}
    void set_result(uint32_t rows, uint32_t bytes, uint32_t detail = 0) {
        rows_ = rows; bytes_ = bytes; detail_ = detail;
    }
    ~TraceScope() { trace_end(h, rows_, bytes_, detail_); }
};

// ---- Drain — the single Python crossing, called once at query teardown -----------
// Precondition: every worker (engine AND rugo) that could still be writing
// spans has already joined — arenas are read here with no synchronization of
// their own, relying on the caller's existing join/queue-drain to establish
// happens-before (the same precondition Engine::collect_op_stats documents).
struct DrainResult {
    std::vector<TraceSpan> spans;
    bool truncated = false;
};

inline DrainResult trace_drain(uint32_t query_seq) {
    DrainResult out;
    std::lock_guard<std::mutex> lock(trace_registry_mutex());
    for (ThreadArena* a : trace_registry()) {
        if (a->local_query_seq != query_seq) continue;
        out.spans.insert(out.spans.end(), a->spans.begin(), a->spans.end());
        out.truncated = out.truncated || a->truncated;
    }
    return out;
}

}  // namespace draken_trace
