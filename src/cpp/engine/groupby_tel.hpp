#pragma once
// Process-wide timing accumulators for GroupBySink's three per-morsel passes.
//
// Mirrors rugo_tel (rugo/src/parquet/telemetry.hpp): global atomics, not thread_local
// — GroupBySink::sink() runs on worker threads, so a thread_local counter would never
// be observed by the harvesting (Python) thread. Accumulation is per-morsel (never
// per-row), so the relaxed fetch_add is negligible against the pass it measures.
//
// Diagnostic only, for the "where does Grouped Aggregate time go" profiling question
// — not wired into OpStats/collect_op_stats, which stays the stable per-plan-node
// contract every operator shares. Reset before a traced query, read after; safe to
// leave permanently instrumented (matches OpStats.exec_ns's own always-on cost class).
//
// Usage (C++):
//   GROUPBY_TEL_START(t0);
//   ... Pass A work ...
//   GROUPBY_TEL_ACCUM(groupby_tel::hash_ns, t0);
//
// Usage (Cython): see reset_groupby_telemetry()/get_groupby_telemetry() in _operators.pyx.

#include <atomic>
#include <chrono>

namespace opteryx::engine::groupby_tel {

// Accumulators in nanoseconds — zero them via reset()
inline std::atomic<long long> hash_ns  {0};  // Pass A: compute_row_hashes over GROUP BY keys
inline std::atomic<long long> probe_ns {0};  // Pass B: find_or_insert_id + partition lane growth
inline std::atomic<long long> apply_ns {0};  // Pass C: per-aggregate-function state update
inline std::atomic<long long> calls    {0};  // GroupBySink::sink() calls (morsels)
// Parvi low-card gate engagement (per-event, never per-row):
inline std::atomic<long long> parvi_sinks    {0};  // GroupBySink locals armed with parvi partitions
inline std::atomic<long long> parvi_promotes {0};  // partition's parvi front map overflowed (estimate misfire)
inline std::atomic<long long> mid_promotes {0};    // partition outgrew the bounded mid tier -> carchar
inline std::atomic<long long> distinct_parvi_sinks    {0};  // DistinctSink locals armed with a parvi front set
inline std::atomic<long long> distinct_parvi_promotes {0};  // front set overflowed (estimate misfire)
// Adaptive raw mode and the radix-partitioned merge (per-event, never per-row):
inline std::atomic<long long> raw_switches {0};     // worker stopped probing its local tables
inline std::atomic<long long> merge_bucketed {0};   // partitions merged through radix buckets
inline std::atomic<long long> merge_buckets {0};    // buckets those partitions were split into
// GROUP BY -> ORDER BY <aggregate> LIMIT k fusion (docs/GROUPBY_TOPK_FUSION_DESIGN.md):
inline std::atomic<long long> topk_pruned {0};      // merged partitions/buckets cut to their top k
inline std::atomic<long long> da_morsels {0};          // morsels aggregated by the direct-array path
inline std::atomic<long long> da_layouts_stats {0};    // direct-array layouts fixed from planner bounds
inline std::atomic<long long> da_layouts_prescan {0};  // ...fixed from a first-morsel prescan
inline std::atomic<long long> da_rejects {0};          // layout over the slot budget: worker hashes
inline std::atomic<long long> da_fallbacks {0};        // key outside the layout: state spilled to hashing
inline std::atomic<long long> da_dict_first {0};       // morsels routed to the dict path ahead of the direct array
inline std::atomic<long long> dict_codepass {0};       // dict-path morsels aggregated through codes

inline void reset() {
    hash_ns.store(0, std::memory_order_relaxed);
    probe_ns.store(0, std::memory_order_relaxed);
    apply_ns.store(0, std::memory_order_relaxed);
    calls.store(0, std::memory_order_relaxed);
    parvi_sinks.store(0, std::memory_order_relaxed);
    parvi_promotes.store(0, std::memory_order_relaxed);
    mid_promotes.store(0, std::memory_order_relaxed);
    distinct_parvi_sinks.store(0, std::memory_order_relaxed);
    distinct_parvi_promotes.store(0, std::memory_order_relaxed);
    raw_switches.store(0, std::memory_order_relaxed);
    merge_bucketed.store(0, std::memory_order_relaxed);
    merge_buckets.store(0, std::memory_order_relaxed);
    topk_pruned.store(0, std::memory_order_relaxed);
    da_morsels.store(0, std::memory_order_relaxed);
    da_layouts_stats.store(0, std::memory_order_relaxed);
    da_layouts_prescan.store(0, std::memory_order_relaxed);
    da_rejects.store(0, std::memory_order_relaxed);
    da_fallbacks.store(0, std::memory_order_relaxed);
    da_dict_first.store(0, std::memory_order_relaxed);
    dict_codepass.store(0, std::memory_order_relaxed);
}

using Clock = std::chrono::steady_clock;
using TP    = std::chrono::time_point<Clock>;

inline TP now() { return Clock::now(); }

inline long long elapsed_ns(TP t0) {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now() - t0).count();
}

// Seconds accessors for the Cython surface (ns -> s).
inline double hash_s()  { return hash_ns.load(std::memory_order_relaxed)  * 1e-9; }
inline double probe_s() { return probe_ns.load(std::memory_order_relaxed) * 1e-9; }
inline double apply_s() { return apply_ns.load(std::memory_order_relaxed) * 1e-9; }
inline long long calls_count() { return calls.load(std::memory_order_relaxed); }
inline long long parvi_sinks_count()    { return parvi_sinks.load(std::memory_order_relaxed); }
inline long long parvi_promotes_count() { return parvi_promotes.load(std::memory_order_relaxed); }
inline long long mid_promotes_count()   { return mid_promotes.load(std::memory_order_relaxed); }
inline long long distinct_parvi_sinks_count()    { return distinct_parvi_sinks.load(std::memory_order_relaxed); }
inline long long distinct_parvi_promotes_count() { return distinct_parvi_promotes.load(std::memory_order_relaxed); }
inline long long raw_switches_count()   { return raw_switches.load(std::memory_order_relaxed); }
inline long long merge_bucketed_count() { return merge_bucketed.load(std::memory_order_relaxed); }
inline long long merge_buckets_count()  { return merge_buckets.load(std::memory_order_relaxed); }
inline long long topk_pruned_count()    { return topk_pruned.load(std::memory_order_relaxed); }
inline long long da_morsels_count()         { return da_morsels.load(std::memory_order_relaxed); }
inline long long da_layouts_stats_count()   { return da_layouts_stats.load(std::memory_order_relaxed); }
inline long long da_layouts_prescan_count() { return da_layouts_prescan.load(std::memory_order_relaxed); }
inline long long da_rejects_count()         { return da_rejects.load(std::memory_order_relaxed); }
inline long long da_fallbacks_count()       { return da_fallbacks.load(std::memory_order_relaxed); }
inline long long da_dict_first_count()      { return da_dict_first.load(std::memory_order_relaxed); }
inline long long dict_codepass_count()      { return dict_codepass.load(std::memory_order_relaxed); }

}  // namespace opteryx::engine::groupby_tel

#define GROUPBY_TEL_START(name)      auto name = opteryx::engine::groupby_tel::now()
#define GROUPBY_TEL_ACCUM(acc, name) (acc).fetch_add(opteryx::engine::groupby_tel::elapsed_ns(name), \
                                                       std::memory_order_relaxed)
