#pragma once
// draken/core/trace_bridge_c.h — cross-.so bridge for the shared execution
// tracer (draken/core/trace.hpp). See that header and
// docs/EXECUTION_TRACING_DESIGN.md for the tracing design; see
// src/cpp/bs_pool_bridge_c.h for the precedent this mirrors.
//
// draken/core/trace.hpp's gate/registry/arenas are process-wide state that
// BOTH rugo (compiled into pool_reader.so) and the opteryx engine (compiled
// into _operators.so) must share — but each is a SEPARATELY LINKED shared
// library. Header-only inline/static C++ state does not merge across .so
// boundaries; this codebase already hit exactly this trap with
// BS::thread_pool (see bs_pool_bridge_c.h's header comment) and fixed it with
// "one compiled home + extern C bridge, loaded RTLD_GLOBAL". This header is
// that treatment for tracing.
//
// The real state lives in EXACTLY ONE place — draken/core/trace_bridge.cpp,
// compiled into draken.draken_native (draken_native.so, the same .so
// draken_bridge.h already uses for this purpose). Every OTHER .so —
// including src/cpp/engine/trace.hpp and rugo/src/parquet/io_pipeline.hpp —
// only DECLARES these functions and calls through them; neither includes
// draken/core/trace.hpp directly.
//
// draken/__init__.py loads draken_native.so with RTLD_GLOBAL so these symbols
// resolve at import time from consumer extensions linked with
// -undefined dynamic_lookup, exactly like draken_vector_unwrap et al.

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

// The canonical, single-source vocabulary of span categories — defined here
// (not in draken/core/trace.hpp, not redefined per-consumer) so opteryx::
// engine's TraceCategory and rugo's category constants cannot drift apart.
typedef enum {
    DRAKEN_TC_SOURCE_PULL     = 1,
    DRAKEN_TC_OP_EXEC         = 2,
    DRAKEN_TC_SINK            = 3,
    DRAKEN_TC_COMBINE         = 4,
    DRAKEN_TC_QUEUE_WAIT      = 5,
    DRAKEN_TC_IO_REQUEST      = 6,
    DRAKEN_TC_IO_WAIT         = 7,
    DRAKEN_TC_BUFFER_RESIDENT = 8,
    DRAKEN_TC_DECODE          = 9,
    DRAKEN_TC_DECODE_PHASE    = 10,
} DrakenTraceCategory;

// One cache line. Layout MUST match draken_trace::TraceSpan (draken/core/
// trace.hpp) field-for-field — draken_trace_drain() memcpy's directly from
// one to the other, verified by a static_assert in trace_bridge.cpp.
typedef struct {
    uint64_t t_start_ns;
    uint64_t t_end_ns;
    uint32_t query_seq;
    uint16_t category;
    uint16_t worker_id;
    uint32_t node_id;
    uint32_t corr_id;
    uint32_t rg_idx;
    uint32_t rows;
    uint32_t bytes;
    uint32_t detail;
    uint32_t file_id;      // interned file path id (draken_trace_intern_file); 0 == n/a
    uint32_t _reserved0[2];
} DrakenTraceSpanC;

// Runtime gate, driven from the surface (opteryx config.OPTERYX_TRACE).
void draken_trace_set_enabled(int on);
int draken_trace_enabled(void);

// Bump/read the trace generation. Call draken_trace_start_query() once,
// single-threaded, before a traced query's workers (engine AND rugo) are
// dispatched.
uint32_t draken_trace_start_query(void);
uint32_t draken_trace_current_query_seq(void);

// The single shared monotonic clock every span — engine and rugo alike —
// timestamps against.
uint64_t draken_trace_now_ns(void);

// Record a span whose [t_start_ns, t_end_ns) is already known. No clock read
// happens here; callers get their timestamps from draken_trace_now_ns().
// No-op (branches out immediately) when tracing is disabled. file_id is 0 for
// spans with no associated file (e.g. engine operator spans).
void draken_trace_record(uint16_t category, uint32_t node_id, uint32_t corr_id,
                          uint32_t rg_idx, uint16_t worker_id,
                          uint64_t t_start_ns, uint64_t t_end_ns,
                          uint32_t rows, uint32_t bytes, uint32_t detail,
                          uint32_t file_id);

// Intern a file path for this query, returning a stable small id (1-based; 0
// == "no file") to embed in spans instead of the string itself. Idempotent
// per path per query — repeated calls with the same path return the same id.
// The intern table resets on every draken_trace_start_query() call, same as
// node_id's per-query reset — like the rest of this tracer, it is a
// single-query diagnostic, not correct across concurrent traced queries in
// one process (an accepted limitation, matching the existing WP-INSTR GIL
// instrument's documented scope).
uint32_t draken_trace_intern_file(const char* path, size_t len);

// One entry of the file_id -> path symbol table, resolved at drain time —
// same shape as Engine::collect_trace_symbols() for node_id -> identity.
typedef struct {
    uint32_t file_id;
    char*    path;   // draken_malloc'd, NUL-terminated; caller draken_free's it
} DrakenFileSymbolC;

// Drain the file-path intern table into one draken_malloc'd
// DrakenFileSymbolC[*out_count] array. Returns NULL (*out_count = 0) if
// nothing was interned. Ownership: the caller must draken_free() each
// entry's `.path`, then draken_free() the returned array itself.
DrakenFileSymbolC* draken_trace_drain_file_symbols(size_t* out_count);

// Drain every registered thread arena (engine's and rugo's alike) tagged with
// `query_seq` into one draken_malloc'd contiguous DrakenTraceSpanC[*out_count]
// array. Returns NULL (with *out_count set to 0) if there is nothing to
// drain. *out_truncated is set to 1 if any arena hit its capacity this query.
//
// Precondition: every worker (engine AND rugo) that could still be recording
// spans for query_seq has already joined — same precondition
// Engine::collect_op_stats documents at its own call site.
//
// Ownership: the caller must draken_free() (draken/core/alloc.h) the returned
// pointer when non-NULL.
DrakenTraceSpanC* draken_trace_drain(uint32_t query_seq, size_t* out_count, int* out_truncated);

#ifdef __cplusplus
}
#endif
