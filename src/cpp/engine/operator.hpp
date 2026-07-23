#pragma once
// src/cpp/engine/operator.hpp — the operator API the morsel-driven engine DICTATES.
//
// See docs/MORSEL_DRIVEN_ENGINE_DESIGN.md. This is a C++ engine: operators, states,
// pipelines and the task loop are C++; no PyObject lives on the execution path. The
// carrier is std::shared_ptr<CxxMorsel>. Errors propagate by STATUS CODE via ErrCtx
// (the boundary cannot carry C++ exceptions across the Cython edge) — code 0 == OK.
//
// Three roles. State is split GLOBAL (shared across worker threads, synchronised only at
// combine/finalize) vs LOCAL (per worker, lock-free on the hot path) — the split that
// makes morsel-driven parallelism scale. Virtual dispatch is PER-MORSEL (coarse, ~thousands
// of rows amortise it); the per-ROW hot path lives inside execute()/sink() as static,
// vectorised kernel calls.

#include <atomic>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "morsels/cxx_morsel.h"  // CxxMorsel, ErrCtx, MorselState  (build: -Idraken)

namespace opteryx::engine {

using MorselPtr = std::shared_ptr<CxxMorsel>;

// ---- Per-operator telemetry (basic, always-on) -----------------------------------
// One Source/Operator/Sink instance serves EVERY worker thread, so these counters are
// shared and therefore atomic. Accumulation is PER-MORSEL (never per-row): the atomic
// add + the timing clock read amortise over the thousands of rows in a morsel, the same
// granularity as the engine's per-morsel virtual dispatch. `identity` ties the readings
// back to the plan node the compiler lowered into this operator (the harvest keys on it).
// The counters are inclusive of downstream work only for the source loop's own timing;
// each operator/sink times just its own execute()/sink() call (the driver excludes the
// recursive forward), so exec_ns is this operator's SELF time.
struct OpStats {
    std::string           identity;      // plan-node identity; empty = untagged (demos).
                                          // An opaque correlation key (several rows can
                                          // share one), NOT for display — see display_name.
    std::string           display_name;  // human-readable plan-node kind (e.g. "FilterNode",
                                          // "ScanNode") — what trace.hpp span consumers show,
                                          // via Engine::collect_trace_symbols(). Empty falls
                                          // back to identity (untagged demos/call sites that
                                          // never call set_current_display_name).
    uint32_t               node_id{0};   // trace.hpp span identity; assigned once at plan
                                          // build time (Engine::add_op_/set_sink_/set_
                                          // source_), read-only during execution — safe
                                          // without atomics. 0 = untagged.
    std::atomic<uint64_t> calls{0};      // input morsels handled
    std::atomic<uint64_t> rows_in{0};
    std::atomic<uint64_t> rows_out{0};
    std::atomic<uint64_t> bytes_in{0};   // rows * columns * 8, matching the Python model
    std::atomic<uint64_t> bytes_out{0};
    std::atomic<uint64_t> exec_ns{0};    // wall time inside this operator's own call(s)
    std::atomic<uint64_t> cpu_ns{0};     // CPU time actually consumed (CLOCK_THREAD_CPUTIME_ID);
                                          // excludes time blocked/asleep, unlike exec_ns
};

// ---- Opaque per-operator state. The engine owns the lifetimes. -------------------
struct GlobalSourceState { virtual ~GlobalSourceState() = default; };
struct LocalSourceState  { virtual ~LocalSourceState()  = default; };
struct OperatorState     { virtual ~OperatorState()     = default; };
struct GlobalSinkState   { virtual ~GlobalSinkState()   = default; };
struct LocalSinkState    { virtual ~LocalSinkState()    = default; };

enum class SourceResult { HAVE_MORE, FINISHED };
// EMIT: `out` holds one output morsel. NEED_INPUT: input consumed, no output (e.g. fully
// filtered) — pull the next input. HAVE_MORE: `out` holds an output AND more remain from
// this input — re-call execute() with the SAME input until it returns EMIT/NEED_INPUT.
enum class OpResult     { EMIT, NEED_INPUT, HAVE_MORE };
enum class SinkResult   { CONTINUE };

// ---- Source: produces morsels; parallel via dynamic morsel assignment ------------
struct Source {
    OpStats stats;
    virtual ~Source() = default;
    virtual std::unique_ptr<GlobalSourceState> make_global() = 0;
    virtual std::unique_ptr<LocalSourceState>  make_local(GlobalSourceState&) = 0;
    // Fill `out` with this worker's next morsel; FINISHED at exhaustion. Disjoint across
    // workers, load-balanced (a free worker asks the global for the next morsel).
    virtual SourceResult get_morsel(GlobalSourceState&, LocalSourceState&,
                                    MorselPtr& out, ErrCtx&) = 0;
};

// ---- Operator: in-pipeline transform (filter, projection, expression eval) -------
struct Operator {
    OpStats stats;
    virtual ~Operator() = default;
    virtual std::unique_ptr<OperatorState> make_state() = 0;
    virtual OpResult execute(const MorselPtr& in, OperatorState&,
                             MorselPtr& out, ErrCtx&) = 0;
};

// ---- Sink: pipeline terminal / breaker -------------------------------------------
struct Sink {
    OpStats stats;
    virtual ~Sink() = default;
    virtual std::unique_ptr<GlobalSinkState> make_global() = 0;
    virtual std::unique_ptr<LocalSinkState>  make_local(GlobalSinkState&) = 0;
    // Accumulate `in` into the worker's LOCAL state (lock-free).
    virtual SinkResult sink(const MorselPtr& in, GlobalSinkState&, LocalSinkState&,
                            ErrCtx&) = 0;
    // Merge this worker's LOCAL state into GLOBAL — once per worker after the source is
    // exhausted (the one synchronised contact).
    virtual void combine(GlobalSinkState&, LocalSinkState&, ErrCtx&) = 0;
    // Produce the breaker's result from GLOBAL — once, after all combines. The result is
    // exposed as a Source for the dependent pipeline (or streamed to the output queue).
    virtual void finalize(GlobalSinkState&, ErrCtx&) = 0;
};

// ---- Pipeline: SOURCE -> OPERATOR* -> SINK ---------------------------------------
struct Pipeline {
    Source*                source = nullptr;
    std::vector<Operator*> operators;
    Sink*                  sink   = nullptr;
    // Optional early-termination signal (LIMIT quota filled): workers stop claiming
    // new morsels when set. Checked between morsels only — never mid-push, so no
    // partial results. nullptr = no early termination possible.
    std::atomic<bool>*     halt   = nullptr;
};

}  // namespace opteryx::engine
