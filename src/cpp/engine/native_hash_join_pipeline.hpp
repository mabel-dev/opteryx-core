#pragma once
// src/cpp/engine/native_hash_join_pipeline.hpp — scan -> [filter] -> build hash
// table; scan -> [filter] -> probe -> aggregate. Fully native two-pipeline
// driver: runs the build pipeline (NativeParquetScanSource ->
// [NumericFilterOperator] -> HashJoinBuildSink) to completion, then wires its
// finalized row-store/hash-index into the probe pipeline
// (NativeParquetScanSource -> [NumericFilterOperator] -> JoinProbeOperator ->
// NativeAggregateSink). No Python anywhere in either run. Filtering is
// supported on EITHER side (or both) — a WHERE clause lands on whichever
// physical relation the join-ordering optimizer put it on, which is not
// always the probe side (verified directly: TPC-H Q14's `l_shipdate` filter
// ends up on the BUILD side, since the optimizer put post-filter-smaller
// `lineitem` on the left/build side, not `part`).
//
// This formalizes what scan_join_demo.hpp's run_hash_join_count already did by
// hand (two sequential run_pipeline calls, a raw pointer threaded from the
// first pipeline's finalized GlobalSinkState into the second pipeline's
// Operator) — see native_hash_join.hpp's header comment for why no new
// Source/Sink interface is needed for this.
//
// First-landing proof harness: takes two ALREADY-OPENED NativeScanPlans (one
// per side) and drives build-then-probe-then-aggregate. Plan-shape eligibility
// (which relation is build vs probe, join-key/payload resolution from a real
// query plan) is intentionally NOT built here yet — see the memory note this
// change is paired with for what's proven vs what's still a follow-up.

#include <cstdint>
#include <vector>

#include "executor.hpp"
#include "scan_filter_demo.hpp"           // NumericFilterOperator, SimplePredicate
#include "native_parquet_scan_source.hpp"
#include "native_hash_join.hpp"           // HashJoinBuildSink, JoinProbeOperator
#include "native_aggregate.hpp"           // NativeAggregateSink, AggregateSpec
#include "memory_pool.hpp"                // opteryx::MemoryPool (decimal pool-path)

namespace opteryx::engine {

struct NativeHashJoinAggregateStats {
    std::vector<double> result;
    std::vector<int64_t> decimal_hi;
    std::vector<uint64_t> decimal_lo;
    std::vector<uint8_t> decimal_scale;
};

// Flat parameter list (no intermediate struct — avoids constructing a C++
// struct value across the Cython FFI boundary; mirrors
// run_native_aggregate_to_result's own flat convention, just doubled per side).
inline NativeHashJoinAggregateStats run_native_hash_join_aggregate_to_result(
        rugo::ParquetIOPipeline* build_pipeline,
        const std::unordered_map<std::string, FileStats>* build_footer_map,
        const std::vector<std::pair<std::string, int>>* build_work_items,
        const std::vector<std::string>* build_column_names,
        int build_in_flight_limit,
        MemoryPool* build_decimal_pool, const std::vector<uint8_t>* build_decimal_columns,
        const std::vector<uint8_t>* build_varchar_columns,
        size_t build_key_col_idx, std::vector<size_t> build_payload_col_idx,
        std::vector<SimplePredicate> build_predicates,
        rugo::ParquetIOPipeline* probe_pipeline,
        const std::unordered_map<std::string, FileStats>* probe_footer_map,
        const std::vector<std::pair<std::string, int>>* probe_work_items,
        const std::vector<std::string>* probe_column_names,
        int probe_in_flight_limit,
        MemoryPool* probe_decimal_pool, const std::vector<uint8_t>* probe_decimal_columns,
        const std::vector<uint8_t>* probe_varchar_columns,
        size_t probe_key_col_idx, std::vector<size_t> probe_payload_col_idx,
        std::vector<SimplePredicate> probe_predicates,
        std::vector<AggregateSpec> specs, int dop, ErrCtx& err, void* thread_pool) {
    NativeHashJoinAggregateStats out;

    NativeParquetScanSource bsrc(build_pipeline, build_footer_map, build_work_items,
                                build_column_names, build_in_flight_limit,
                                build_decimal_pool, build_decimal_columns, build_varchar_columns);
    HashJoinBuildSink bsink(build_key_col_idx, build_payload_col_idx);
    NumericFilterOperator bfilt(std::move(build_predicates));
    Pipeline bp;
    bp.source = &bsrc;
    if (!bfilt.predicates.empty()) {
        bp.operators = {&bfilt};
    }
    bp.sink = &bsink;
    auto bg = run_pipeline(bp, dop, err, thread_pool);
    if (err.code != 0) return out;
    auto* build_global = static_cast<HashJoinBuildGlobal*>(bg.get());

    NativeParquetScanSource psrc(probe_pipeline, probe_footer_map, probe_work_items,
                                probe_column_names, probe_in_flight_limit,
                                probe_decimal_pool, probe_decimal_columns, probe_varchar_columns);
    JoinProbeOperator join(probe_key_col_idx, &build_global->key_to_rows, &build_global->payload,
                           std::move(probe_payload_col_idx));
    NativeAggregateSink asink(std::move(specs));

    NumericFilterOperator filt(std::move(probe_predicates));
    Pipeline pp;
    pp.source = &psrc;
    if (!filt.predicates.empty()) {
        pp.operators = {&filt, &join};
    } else {
        pp.operators = {&join};
    }
    pp.sink = &asink;

    auto pg = run_pipeline(pp, dop, err, thread_pool);
    if (err.code != 0) return out;
    auto* agg_global = static_cast<NativeAggregateGlobal*>(pg.get());
    out.result = agg_global->result;
    out.decimal_hi = agg_global->decimal_hi;
    out.decimal_lo = agg_global->decimal_lo;
    out.decimal_scale = agg_global->decimal_scale;
    return out;
}

}  // namespace opteryx::engine
