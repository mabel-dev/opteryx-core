#pragma once
// src/cpp/engine/real_groupby_pipeline.hpp — REAL CUTOVER PIECE 3: scan -> [filter]* ->
// GROUP BY key -> SUM/COUNT, sourced via the genuinely streaming StreamingScanSource
// (on-demand pulls, memory-bounded — matches real_filter_pipeline.hpp /
// real_aggregate_pipeline.hpp's pattern), reusing the proven GroupSumCountSink
// (scan_groupby_demo.hpp) and NumericFilterOperator (scan_filter_demo.hpp) for an
// optional ANDed predicate chain. Pure C++ for the run itself; the only Python/Cython
// touch is INSIDE `pull_fn`'s trampoline body, called once per morsel pull across
// however many workers `dop` requires.

#include <cstdint>
#include <vector>

#include "executor.hpp"
#include "scan_filter_demo.hpp"     // NumericFilterOperator, SimplePredicate
#include "scan_groupby_demo.hpp"    // GroupSumCountSink, GroupRow
#include "streaming_scan_source.hpp"

namespace opteryx::engine {

// Runs scan -> [filter]* -> GROUP BY key_col_idx SUM/COUNT(val_col_idx) at degree `dop`,
// pulling morsels ON DEMAND from `scan_ptr` via `pull_fn` (no pre-materialization).
// `predicates` may be empty (no filter operator inserted — straight scan -> group-by).
// Flattens the merged per-key map into `out` (Cython-friendly POD rows) on the single
// calling thread after the parallel run completes.
//
// `pool` is the caller's persistent BSThreadPoolBridge, passed through OPAQUELY (see
// executor.hpp's pool-backed `run_pipeline` overload) — `dop` worker tasks are submitted
// to it rather than spawned as fresh std::threads.
inline void run_real_groupby_to_result(void* scan_ptr, ScanPullFn pull_fn,
                                       std::vector<SimplePredicate> predicates,
                                       size_t key_col_idx, size_t val_col_idx, int dop,
                                       ErrCtx& err, void* pool,
                                       std::vector<GroupRow>& out) {
    StreamingScanSource src(scan_ptr, pull_fn);
    NumericFilterOperator filt(std::move(predicates));
    GroupSumCountSink snk(key_col_idx, val_col_idx);
    Pipeline p;
    p.source = &src;
    if (!filt.predicates.empty()) {
        p.operators = {&filt};
    }
    p.sink = &snk;

    auto gsink = run_pipeline(p, dop, err, pool);
    if (err.code != 0) return;
    auto& g = *static_cast<GroupSumCountGlobal*>(gsink.get());
    out.reserve(g.result.size());
    for (auto& kv : g.result) {
        out.push_back(GroupRow{kv.first, kv.second.sum, kv.second.count});
    }
}

}  // namespace opteryx::engine
