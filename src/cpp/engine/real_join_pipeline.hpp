#pragma once
// src/cpp/engine/real_join_pipeline.hpp — REAL CUTOVER PIECE 4: scan -> [filter]* ->
// build hash table; scan -> [filter]* -> probe -> SUM/COUNT over one join-output column.
// Sourced via the genuinely streaming StreamingScanSource on BOTH sides (on-demand pulls,
// memory-bounded — matches real_filter_pipeline.hpp / real_aggregate_pipeline.hpp /
// real_groupby_pipeline.hpp's pattern), reusing the proven arena-aware HashJoinBuildSink /
// JoinProbeOperator (native_hash_join.hpp) and SumCountSink (scan_aggregate_demo.hpp).
// Pure C++ for the run itself; the only Python/Cython touch is INSIDE each side's
// `pull_fn` trampoline body, called once per morsel pull across however many workers
// `dop` requires.
//
// Scope: same as native_hash_join.hpp (INNER equi-join, single INT64/DECIMAL key,
// fixed-width or VARCHAR payload columns, no NULL payloads) — deliberately narrower than
// native_hash_join_pipeline.hpp's NativeAggregateSink/AggregateSpec expression-tree
// aggregate: this cutover piece aggregates a single raw join-output column via
// SumCountSink, matching pieces 2/3's simplicity. A caller needing a computed expression
// (e.g. TPC-H Q14's CASE/LIKE) is out of this piece's scope, same as pieces 2/3 don't
// support expression trees either.

#include <cstdint>
#include <vector>

#include "executor.hpp"
#include "native_hash_join.hpp"     // HashJoinBuildSink, JoinProbeOperator
#include "scan_aggregate_demo.hpp"  // SumCountSink, AggDemoStats
#include "scan_filter_demo.hpp"     // NumericFilterOperator, SimplePredicate
#include "streaming_scan_source.hpp"

namespace opteryx::engine {

// Runs build_scan -> [filter]* -> HashJoinBuildSink, then probe_scan -> [filter]* ->
// JoinProbeOperator -> SUM/COUNT(agg_col_idx), at degree `dop`. `agg_col_idx` indexes the
// JOIN OUTPUT's combined column space: build payload columns first (in
// `build_payload_col_idx` order), then probe payload columns (in `probe_payload_col_idx`
// order) — same convention as native_hash_join_pipeline.hpp / the (now-deleted) Python
// eligibility gate that used to build this index space.
inline AggDemoStats run_real_join_aggregate_to_result(
        void* build_scan_ptr, ScanPullFn build_pull_fn,
        std::vector<SimplePredicate> build_predicates,
        size_t build_key_col_idx, std::vector<size_t> build_payload_col_idx,
        void* probe_scan_ptr, ScanPullFn probe_pull_fn,
        std::vector<SimplePredicate> probe_predicates,
        size_t probe_key_col_idx, std::vector<size_t> probe_payload_col_idx,
        size_t agg_col_idx, int dop, ErrCtx& err, void* pool) {
    StreamingScanSource bsrc(build_scan_ptr, build_pull_fn);
    NumericFilterOperator bfilt(std::move(build_predicates));
    HashJoinBuildSink bsink(build_key_col_idx, build_payload_col_idx);
    Pipeline bp;
    bp.source = &bsrc;
    if (!bfilt.predicates.empty()) {
        bp.operators = {&bfilt};
    }
    bp.sink = &bsink;
    auto bg = run_pipeline(bp, dop, err, pool);
    if (err.code != 0) return AggDemoStats{};
    auto* build_global = static_cast<HashJoinBuildGlobal*>(bg.get());

    StreamingScanSource psrc(probe_scan_ptr, probe_pull_fn);
    JoinProbeOperator join(probe_key_col_idx, &build_global->key_to_rows,
                          &build_global->payload, std::move(probe_payload_col_idx));
    NumericFilterOperator pfilt(std::move(probe_predicates));
    SumCountSink asink(agg_col_idx);
    Pipeline pp;
    pp.source = &psrc;
    if (!pfilt.predicates.empty()) {
        pp.operators = {&pfilt, &join};
    } else {
        pp.operators = {&join};
    }
    pp.sink = &asink;
    auto pg = run_pipeline(pp, dop, err, pool);
    if (err.code != 0) return AggDemoStats{};
    auto& g = *static_cast<SumCountGlobal*>(pg.get());
    return AggDemoStats{g.result_sum, g.result_count};
}

}  // namespace opteryx::engine
