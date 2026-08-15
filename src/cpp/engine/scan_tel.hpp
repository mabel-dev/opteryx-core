#pragma once
// Process-wide counters for the SHAPE a native parquet scan hands the engine.
//
// Mirrors groupby_tel / rugo_tel: global atomics, not thread_local — build_column()
// runs on the scan's worker threads, so a thread_local counter would never be seen
// by the harvesting (Python) thread. Accumulation is per COLUMN per MORSEL, never
// per row, so the relaxed fetch_add is noise against the decode it follows.
//
// WHY this exists, and why it is NOT redundant with rugo_tel's ba_* counters:
// rugo_tel measures what the DECODER emits (rugo/src/parquet/telemetry.hpp).
// This measures what the ENGINE receives. The two are separated by
// direct_kind_for()'s classification and the pool/direct split, and the whole
// question this instrumentation exists to answer is whether a dictionary that
// the decoder built survives that boundary.
//
// It also deliberately does NOT read the shape back through Python. Session
// .execute_to_morsels() merges and splits morsels to an output-boundary row
// target (query_session.py's max_size, default 65,536) — so a dictionary whose
// entry count exceeds that target reads as "dense" at the Python boundary even
// when the engine never saw it that way. Measuring here is measuring the thing
// itself; measuring there measured the harness.
//
// Diagnostic only — not wired into OpStats, which stays the stable per-plan-node
// contract. Reset before a query, read after.

#include <atomic>

namespace opteryx::engine::scan_tel {

// String columns only (VARCHAR / NVARCHAR / VARBINARY), keyed on the DirectKind
// the scan source received. Rows are logical rows in the morsel.
inline std::atomic<long long> str_dict_cols  {0};  // DK_VARCHAR_DICT — dict-shaped direct
inline std::atomic<long long> str_dense_cols {0};  // DK_VARCHAR — plain dense direct
inline std::atomic<long long> str_pool_cols  {0};  // DK_POOL — via MemoryPool wire format
inline std::atomic<long long> str_dict_rows  {0};
inline std::atomic<long long> str_dense_rows {0};
inline std::atomic<long long> str_pool_rows  {0};
inline std::atomic<long long> str_dict_entries {0};  // unique-value slots on dict arrivals
// Non-string columns, so a scan's total column count is recoverable and a
// string-only rate is not silently computed against the wrong denominator.
inline std::atomic<long long> other_cols     {0};

inline void reset() {
    str_dict_cols.store(0, std::memory_order_relaxed);
    str_dense_cols.store(0, std::memory_order_relaxed);
    str_pool_cols.store(0, std::memory_order_relaxed);
    str_dict_rows.store(0, std::memory_order_relaxed);
    str_dense_rows.store(0, std::memory_order_relaxed);
    str_pool_rows.store(0, std::memory_order_relaxed);
    str_dict_entries.store(0, std::memory_order_relaxed);
    other_cols.store(0, std::memory_order_relaxed);
}

inline long long str_dict_cols_count()  { return str_dict_cols.load(std::memory_order_relaxed); }
inline long long str_dense_cols_count() { return str_dense_cols.load(std::memory_order_relaxed); }
inline long long str_pool_cols_count()  { return str_pool_cols.load(std::memory_order_relaxed); }
inline long long str_dict_rows_count()  { return str_dict_rows.load(std::memory_order_relaxed); }
inline long long str_dense_rows_count() { return str_dense_rows.load(std::memory_order_relaxed); }
inline long long str_pool_rows_count()  { return str_pool_rows.load(std::memory_order_relaxed); }
inline long long str_dict_entries_count() { return str_dict_entries.load(std::memory_order_relaxed); }
inline long long other_cols_count()     { return other_cols.load(std::memory_order_relaxed); }

} // namespace opteryx::engine::scan_tel
