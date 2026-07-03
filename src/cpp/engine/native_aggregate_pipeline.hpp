#pragma once
// src/cpp/engine/native_aggregate_pipeline.hpp — scan -> [filter] -> ungrouped
// aggregate, fully native. Same zero-Python contract as native_filter_pipeline.hpp:
// no PyObject anywhere in this pipeline's run, because there is no Python on this
// path at all — worker threads submitted to the caller's BSThreadPoolBridge never
// touch the interpreter.

#include <cstdint>
#include <vector>

#include "executor.hpp"
#include "scan_filter_demo.hpp"          // NumericFilterOperator, SimplePredicate
#include "native_parquet_scan_source.hpp"
#include "native_aggregate.hpp"          // NativeAggregateSink, AggregateSpec
#include "memory_pool.hpp"               // opteryx::MemoryPool (decimal pool-path)

namespace opteryx::engine {

struct NativeAggregateStats {
    std::vector<double> result;        // finalized SUM/COUNT/AVG, non-decimal specs
    std::vector<int64_t> decimal_hi;   // decimal specs only — see NativeAggregateGlobal
    std::vector<uint64_t> decimal_lo;
    std::vector<uint8_t> decimal_scale;
};

// `predicates` may be empty (bare scan -> aggregate, no WHERE clause reachable by
// this path). See native_filter_pipeline.hpp / native_parquet_scan_source.hpp for
// what `pipeline`/`footer_map`/`work_items`/`column_names` mean and their
// lifetime contract — identical here. `thread_pool` is the caller's
// BSThreadPoolBridge (opaque void*, see executor.hpp::run_pipeline); it is
// unrelated to `decimal_pool` (opteryx::MemoryPool*), which is only read from
// for columns flagged in `decimal_columns` (parallel to column_names) — see
// native_decimal_pool_decode.hpp. Both decimal params may be nullptr/empty
// when no spec in `specs` is a decimal spec.
inline NativeAggregateStats run_native_aggregate_to_result(
        rugo::ParquetIOPipeline* pipeline,
        const std::unordered_map<std::string, FileStats>* footer_map,
        const std::vector<std::pair<std::string, int>>* work_items,
        const std::vector<std::string>* column_names,
        int in_flight_limit, std::vector<SimplePredicate> predicates,
        std::vector<AggregateSpec> specs, int dop, ErrCtx& err, void* thread_pool,
        MemoryPool* decimal_pool = nullptr,
        const std::vector<uint8_t>* decimal_columns = nullptr) {
    NativeParquetScanSource src(pipeline, footer_map, work_items, column_names, in_flight_limit,
                                decimal_pool, decimal_columns);
    NativeAggregateSink snk(std::move(specs));
    Pipeline p;
    p.source = &src;
    p.sink = &snk;

    NumericFilterOperator filt(std::move(predicates));
    if (!filt.predicates.empty()) {
        p.operators = {&filt};
    }

    auto gsink = run_pipeline(p, dop, err, thread_pool);
    auto* g = static_cast<NativeAggregateGlobal*>(gsink.get());
    NativeAggregateStats stats;
    stats.result = g->result;
    stats.decimal_hi = g->decimal_hi;
    stats.decimal_lo = g->decimal_lo;
    stats.decimal_scale = g->decimal_scale;
    return stats;
}

}  // namespace opteryx::engine
