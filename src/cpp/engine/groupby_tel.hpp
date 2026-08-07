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
inline std::atomic<long long> distinct_parvi_sinks    {0};  // DistinctSink locals armed with a parvi front set
inline std::atomic<long long> distinct_parvi_promotes {0};  // front set overflowed (estimate misfire)

inline void reset() {
    hash_ns.store(0, std::memory_order_relaxed);
    probe_ns.store(0, std::memory_order_relaxed);
    apply_ns.store(0, std::memory_order_relaxed);
    calls.store(0, std::memory_order_relaxed);
    parvi_sinks.store(0, std::memory_order_relaxed);
    parvi_promotes.store(0, std::memory_order_relaxed);
    distinct_parvi_sinks.store(0, std::memory_order_relaxed);
    distinct_parvi_promotes.store(0, std::memory_order_relaxed);
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
inline long long distinct_parvi_sinks_count()    { return distinct_parvi_sinks.load(std::memory_order_relaxed); }
inline long long distinct_parvi_promotes_count() { return distinct_parvi_promotes.load(std::memory_order_relaxed); }

}  // namespace opteryx::engine::groupby_tel

#define GROUPBY_TEL_START(name)      auto name = opteryx::engine::groupby_tel::now()
#define GROUPBY_TEL_ACCUM(acc, name) (acc).fetch_add(opteryx::engine::groupby_tel::elapsed_ns(name), \
                                                       std::memory_order_relaxed)
