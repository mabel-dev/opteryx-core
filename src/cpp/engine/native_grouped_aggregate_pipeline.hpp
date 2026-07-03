#pragma once
// src/cpp/engine/native_grouped_aggregate_pipeline.hpp — scan -> [filter] ->
// GROUP BY aggregate, fully native. Same zero-Python contract as
// native_aggregate_pipeline.hpp: worker threads submitted to the caller's
// BSThreadPoolBridge never touch the interpreter, because there is no Python
// on this path at all.

#include <cstdint>
#include <vector>

#include "executor.hpp"
#include "scan_filter_demo.hpp"           // NumericFilterOperator, SimplePredicate
#include "native_parquet_scan_source.hpp"
#include "native_grouped_aggregate.hpp"   // NativeGroupedAggregateSink, AggregateSpec
#include "memory_pool.hpp"                // opteryx::MemoryPool (decimal pool-path)

namespace opteryx::engine {

// See native_aggregate_pipeline.hpp for what `pipeline`/`footer_map`/
// `work_items`/`column_names`/`thread_pool`/`decimal_pool`/`decimal_columns`
// mean — identical contract here. `group_col_idx` indexes into `column_names`
// (same convention as SimplePredicate.col_idx), naming the VARCHAR columns
// that form the group-by key, in SELECT-list order.
inline NativeGroupedAggregateStats run_native_grouped_aggregate_to_result(
        rugo::ParquetIOPipeline* pipeline,
        const std::unordered_map<std::string, FileStats>* footer_map,
        const std::vector<std::pair<std::string, int>>* work_items,
        const std::vector<std::string>* column_names,
        int in_flight_limit, std::vector<SimplePredicate> predicates,
        std::vector<size_t> group_col_idx,
        std::vector<AggregateSpec> specs, int dop, ErrCtx& err, void* thread_pool,
        MemoryPool* decimal_pool = nullptr,
        const std::vector<uint8_t>* decimal_columns = nullptr,
        const std::vector<uint8_t>* varchar_columns = nullptr) {
    NativeParquetScanSource src(pipeline, footer_map, work_items, column_names, in_flight_limit,
                                decimal_pool, decimal_columns, varchar_columns);
    NativeGroupedAggregateSink snk(std::move(group_col_idx), std::move(specs));
    Pipeline p;
    p.source = &src;
    p.sink = &snk;

    NumericFilterOperator filt(std::move(predicates));
    if (!filt.predicates.empty()) {
        p.operators = {&filt};
    }

    run_pipeline(p, dop, err, thread_pool);
    if (err.code != 0) return NativeGroupedAggregateStats{};
    return snk.stats_;
}

}  // namespace opteryx::engine
