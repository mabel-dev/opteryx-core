#pragma once
// src/cpp/engine/real_filter_pipeline.hpp — the FIRST piece of the REAL (non-demo)
// engine cutover: scan -> filter -> exit, sourced via the genuinely streaming
// StreamingScanSource (on-demand pulls, memory-bounded — not slice 5a's pre-materialized
// vector), reusing the proven NumericFilterOperator (scan_filter_demo.hpp) and the real
// production MorselQueue sink. This is the shape `execute()` will route to the native
// engine for, gated narrowly, with everything else still falling back to the existing
// scheduler — the actual cutover starts here, not a proof harness.

#include <cstdint>

#include "executor.hpp"
#include "morsel_queue.hpp"
#include "scan_filter_demo.hpp"     // NumericFilterOperator, QueueSink
#include "streaming_scan_source.hpp"

namespace opteryx::engine {

struct RealFilterStats {
    int64_t rows_out = 0;
};

// Runs scan -> filter -> out_q at degree `dop`, pulling morsels ON DEMAND from `scan_ptr`
// via `pull_fn` (no pre-materialization). Pure C++ for the run itself; the only
// Python/Cython touch is INSIDE `pull_fn`'s trampoline body, called once per morsel pull
// across however many workers `dop` requires.
//
// `pool` is the caller's persistent BSThreadPoolBridge, passed through OPAQUELY (see
// executor.hpp's pool-backed `run_pipeline` overload for why it's `void*` and not
// `BSThreadPoolBridge*`) — `dop` worker tasks are submitted to it rather than spawned
// as fresh std::threads, which is what avoids the free-threaded-CPython thread-attach
// deadlock documented in executor.hpp.
inline RealFilterStats run_real_filter_to_queue(void* scan_ptr, ScanPullFn pull_fn,
                                                size_t col_idx, double threshold, int dop,
                                                MorselQueue* out_q, ErrCtx& err,
                                                void* pool) {
    StreamingScanSource src(scan_ptr, pull_fn);
    NumericFilterOperator filt(col_idx, threshold);
    QueueSink snk(out_q);
    Pipeline p;
    p.source = &src;
    p.operators = {&filt};
    p.sink = &snk;

    auto gsink = run_pipeline(p, dop, err, pool);
    RealFilterStats stats;
    stats.rows_out = static_cast<QueueSinkGlobal*>(gsink.get())->rows_out.load();
    return stats;
}

}  // namespace opteryx::engine
