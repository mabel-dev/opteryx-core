#pragma once
// src/cpp/engine/trace.hpp — opteryx::engine's C++ convenience wrapper over the
// shared execution-tracer bridge (draken/core/trace_bridge_c.h).
//
// The actual tracer state (arenas, registry, gate) lives in EXACTLY ONE place
// — draken/core/trace_bridge.cpp, compiled only into draken_native.so —
// because rugo (compiled into pool_reader.so) and this engine (compiled into
// _operators.so) are SEPARATE shared libraries, and header-only inline/static
// C++ state does not merge across .so boundaries (this codebase already hit
// exactly this trap with BS::thread_pool; see src/cpp/bs_pool_bridge_c.h).
// This header therefore never includes draken/core/trace.hpp — only the
// extern "C" bridge — and neither should any other engine file. See
// docs/EXECUTION_TRACING_DESIGN.md for the full design.

#include "core/trace_bridge_c.h"  // build: -Idraken

namespace opteryx::engine {

// Aliases of DrakenTraceCategory (trace_bridge_c.h) — the canonical, single-
// source vocabulary shared with rugo's span categories. Not redeclared with
// independent values; these just give call sites (executor.hpp) the
// unqualified TC_* names they already use.
constexpr uint16_t TC_SOURCE_PULL     = DRAKEN_TC_SOURCE_PULL;
constexpr uint16_t TC_OP_EXEC         = DRAKEN_TC_OP_EXEC;
constexpr uint16_t TC_SINK            = DRAKEN_TC_SINK;
constexpr uint16_t TC_COMBINE         = DRAKEN_TC_COMBINE;
constexpr uint16_t TC_QUEUE_WAIT      = DRAKEN_TC_QUEUE_WAIT;
constexpr uint16_t TC_IO_REQUEST      = DRAKEN_TC_IO_REQUEST;
constexpr uint16_t TC_IO_WAIT         = DRAKEN_TC_IO_WAIT;
constexpr uint16_t TC_BUFFER_RESIDENT = DRAKEN_TC_BUFFER_RESIDENT;
constexpr uint16_t TC_DECODE          = DRAKEN_TC_DECODE;
constexpr uint16_t TC_DECODE_PHASE    = DRAKEN_TC_DECODE_PHASE;
constexpr uint16_t TC_QUEUE_STALL     = DRAKEN_TC_QUEUE_STALL;
constexpr uint16_t TC_FINALIZE        = DRAKEN_TC_FINALIZE;

inline uint64_t trace_now_ns() { return draken_trace_now_ns(); }
inline void trace_set_enabled(bool on) { draken_trace_set_enabled(on ? 1 : 0); }
inline bool trace_enabled() { return draken_trace_enabled() != 0; }
inline uint32_t trace_start_query() { return draken_trace_start_query(); }
inline uint32_t trace_current_query_seq() { return draken_trace_current_query_seq(); }

// Handle for spans opened/closed on the same thread (operator/source/sink
// self-time — Phase 1's only user). Carries the start timestamp + span
// identity locally; trace_end() closes it with one bridge call, so no live
// arena pointer/slot ever needs to cross the .so boundary.
struct TraceHandle {
    bool     open       = false;
    uint64_t t_start_ns = 0;
    uint16_t category   = 0;
    uint32_t node_id    = 0;
    uint32_t corr_id    = 0;
    uint32_t rg_idx     = 0xFFFFFFFFu;
    uint16_t worker_id  = 0;
};

inline TraceHandle trace_begin(uint16_t category, uint32_t node_id, uint32_t corr_id,
                                uint32_t rg_idx, int worker_id) {
    if (!trace_enabled()) return TraceHandle{};
    TraceHandle h;
    h.open = true;
    h.t_start_ns = draken_trace_now_ns();
    h.category = category;
    h.node_id = node_id;
    h.corr_id = corr_id;
    h.rg_idx = rg_idx;
    h.worker_id = static_cast<uint16_t>(worker_id);
    return h;
}

inline void trace_end(const TraceHandle& h, uint32_t rows, uint32_t bytes, uint32_t detail = 0) {
    if (!h.open) return;
    // file_id=0: no scan/file concept on the engine's operator spans (Phase 1).
    draken_trace_record(h.category, h.node_id, h.corr_id, h.rg_idx, h.worker_id,
                         h.t_start_ns, draken_trace_now_ns(), rows, bytes, detail, 0);
}

// RAII scope for the common (same-thread) case.
struct TraceScope {
    TraceHandle h;
    uint32_t rows_ = 0, bytes_ = 0, detail_ = 0;
    TraceScope(uint16_t category, uint32_t node_id, int worker_id,
               uint32_t corr_id = 0, uint32_t rg_idx = 0xFFFFFFFFu)
        : h(trace_begin(category, node_id, corr_id, rg_idx, worker_id)) {}
    void set_result(uint32_t rows, uint32_t bytes, uint32_t detail = 0) {
        rows_ = rows; bytes_ = bytes; detail_ = detail;
    }
    ~TraceScope() { trace_end(h, rows_, bytes_, detail_); }
};

}  // namespace opteryx::engine
