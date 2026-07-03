#pragma once
// src/cpp/engine/real_aggregate_pipeline.hpp — REAL CUTOVER PIECE 2: scan -> [filter] ->
// ungrouped SUM/COUNT aggregate, sourced via the genuinely streaming StreamingScanSource
// (on-demand pulls, memory-bounded — matches real_filter_pipeline.hpp's pattern exactly,
// not slice 5b's pre-materialized vector), reusing the proven SumCountSink
// (scan_aggregate_demo.hpp) and NumericFilterOperator (scan_filter_demo.hpp) for an
// optional ANDed predicate chain. Pure C++ for the run itself; the only Python/Cython
// touch is INSIDE `pull_fn`'s trampoline body, called once per morsel pull across
// however many workers `dop` requires.

#include <cstdint>
#include <vector>

#include "executor.hpp"
#include "scan_aggregate_demo.hpp"  // SumCountSink, AggDemoStats, agg_type_supported
#include "scan_filter_demo.hpp"     // NumericFilterOperator, SimplePredicate
#include "streaming_scan_source.hpp"

namespace opteryx::engine {

// Runs scan -> [filter]* -> SUM/COUNT at degree `dop`, pulling morsels ON DEMAND from
// `scan_ptr` via `pull_fn` (no pre-materialization). `predicates` may be empty (no
// filter operator inserted at all — straight scan -> aggregate).
//
// `pool` is the caller's persistent BSThreadPoolBridge, passed through OPAQUELY (see
// executor.hpp's pool-backed `run_pipeline` overload) — `dop` worker tasks are submitted
// to it rather than spawned as fresh std::threads.
inline AggDemoStats run_real_aggregate_to_result(void* scan_ptr, ScanPullFn pull_fn,
                                                 std::vector<SimplePredicate> predicates,
                                                 size_t col_idx, int dop, ErrCtx& err,
                                                 void* pool) {
    StreamingScanSource src(scan_ptr, pull_fn);
    NumericFilterOperator filt(std::move(predicates));
    SumCountSink snk(col_idx);
    Pipeline p;
    p.source = &src;
    if (!filt.predicates.empty()) {
        p.operators = {&filt};
    }
    p.sink = &snk;

    auto gsink = run_pipeline(p, dop, err, pool);
    auto& g = *static_cast<SumCountGlobal*>(gsink.get());
    return AggDemoStats{g.result_sum, g.result_count};
}

}  // namespace opteryx::engine
