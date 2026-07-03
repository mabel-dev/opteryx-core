#pragma once
// src/cpp/engine/native_filter_pipeline.hpp — scan -> filter -> queue, fully native.
//
// Unlike real_filter_pipeline.hpp (which streams from the EXISTING Cython scan via a
// GIL-bridged trampoline — a deliberate, narrow cutover, not a rewrite), this pairs
// NativeParquetScanSource with NumericFilterOperator and the real MorselQueue sink:
// there is no Python/Cython anywhere in this pipeline's run. Worker threads submitted
// to the caller's BSThreadPoolBridge never touch the interpreter — not because the
// GIL is released cleverly, but because there is no PyObject on this path at all.

#include <cstdint>

#include "executor.hpp"
#include "morsel_queue.hpp"
#include "scan_filter_demo.hpp"          // NumericFilterOperator, QueueSink
#include "native_parquet_scan_source.hpp"

namespace opteryx::engine {

struct NativeFilterStats {
    int64_t rows_out = 0;
};

// `pipeline`/`footer_map`/`work_items`/`column_names` are borrowed from the caller's
// NativeScanPlan (pool_reader.pyx), which the Python planning frame keeps alive for
// the run's duration. `pool` is the caller's persistent BSThreadPoolBridge, passed
// through opaquely — see executor.hpp's pool-backed `run_pipeline` overload.
// `predicates` are ANDed together (see NumericFilterOperator / SimplePredicate);
// `_find_native_filter_eligible` (parallel_engine.py) is what walks a WHERE clause's
// AND-tree into this flat list.
inline NativeFilterStats run_native_filter_to_queue(
        rugo::ParquetIOPipeline* pipeline,
        const std::unordered_map<std::string, FileStats>* footer_map,
        const std::vector<std::pair<std::string, int>>* work_items,
        const std::vector<std::string>* column_names,
        int in_flight_limit, std::vector<SimplePredicate> predicates, int dop,
        MorselQueue* out_q, ErrCtx& err, void* pool) {
    NativeParquetScanSource src(pipeline, footer_map, work_items, column_names, in_flight_limit);
    NumericFilterOperator filt(std::move(predicates));
    QueueSink snk(out_q);
    Pipeline p;
    p.source = &src;
    p.operators = {&filt};
    p.sink = &snk;

    auto gsink = run_pipeline(p, dop, err, pool);
    NativeFilterStats stats;
    stats.rows_out = static_cast<QueueSinkGlobal*>(gsink.get())->rows_out.load();
    return stats;
}

}  // namespace opteryx::engine
