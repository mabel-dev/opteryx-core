#pragma once
// src/cpp/engine/topn_boundary.hpp — the Top-N runtime boundary: a running n-th
// best leading-key value, produced by whatever sees the rows that reach a Top-N and
// consumed by the scan feeding it, which skips row groups that provably cannot
// contribute. See docs/TOPN_RUNTIME_BOUNDARY_DESIGN.md.
//
// Three pieces, each defined exactly once:
//   TopNBoundary          the shared value (one atomic) + the consumers' skip count
//   TopNBoundaryTracker   the producer: the n best non-null ordinals one thread saw
//   topn_excludes_row_group   THE parquet row-group skip test (§2.2) — defined in
//                         native_parquet_scan_source.hpp, next to the stat converter it
//                         uses, so this header (included by native_sort.hpp) stays
//                         free of any parquet dependency.
//
// ── Why there is no barrier (§2.4) ───────────────────────────────────────────────
// The runtime min/max JOIN filter (runtime_bound.hpp) is complete before its scan
// starts because pipelines run serially. Here the producer and the scan run
// CONCURRENTLY, in the same pipeline — and that is fine, because the boundary only
// ever TIGHTENS: offer() installs a value only when it is strictly better. A reader
// holding a stale value tests against a LOOSER boundary and therefore skips a SUBSET
// of what the current value would skip. Every read, at any moment, is sound. So the
// value is one relaxed atomic: nothing else is published through it.
//
// ── The ordinal space is draken's ───────────────────────────────────────────────
// The tracker ordinalizes rows through cxx_ordinal_topn_c (draken's one ops table)
// and the skip test converts footer statistics through stat_bytes_to_ordinal — the
// SAME space runtime_bound.hpp and skene's zone map use. sort_num_key is a different
// order-preserving mapping and is deliberately NOT used here: translating between the
// two would be a second dialect.
//
// ── "Nothing published" needs no flag ───────────────────────────────────────────
// The value starts at the LOOSEST ordinal: INT64_MAX ascending, INT64_MIN
// descending. The skip test is strict (row-group min > boundary, or max < boundary),
// which no row group can satisfy against those, so an unpublished boundary skips
// nothing — the `valid == 0` posture of runtime_bound.hpp, without a second atomic
// that a reader could observe out of step with the first.

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <limits>
#include <string>
#include <vector>

#include "morsels/cxx_ordinal.h"     // cxx_ordinal_topn_c — draken owns the ordinal

namespace opteryx::engine {

struct TopNBoundary {
    explicit TopNBoundary(bool asc)
        : ascending(asc),
          value(asc ? std::numeric_limits<int64_t>::max()
                    : std::numeric_limits<int64_t>::min()) {}

    // The direction of the Top-N's LEADING key. Fixed at construction; the engine
    // checks it against the sink's / Source's own spec when wiring, so a producer
    // and a consumer can never disagree about which way "tighter" is.
    const bool ascending;
    std::atomic<int64_t> value;
    // Row groups the consumers skipped because of this boundary — the marginal
    // share, counted after plan-time and runtime-join pruning (§7).
    std::atomic<int64_t> row_groups_skipped{0};

    // Install `v` iff it is strictly tighter than the current value.
    void offer(int64_t v) {
        int64_t cur = value.load(std::memory_order_relaxed);
        if (ascending) {
            while (v < cur &&
                   !value.compare_exchange_weak(cur, v, std::memory_order_relaxed)) {
            }
        } else {
            while (v > cur &&
                   !value.compare_exchange_weak(cur, v, std::memory_order_relaxed)) {
            }
        }
    }
    int64_t load() const { return value.load(std::memory_order_relaxed); }
};

// One producer thread's view: the n best non-null leading-key ordinals it has seen.
// NULL rows are ignored, which is sound under both placements (§2.2): under NULLS
// LAST a NULL is the worst row and cannot be in the top n unless fewer than n
// non-null rows exist (then this never fills); under NULLS FIRST a NULL beats every
// value, so the true n-th best row is at least as good as the n-th best non-null
// value, and the boundary this computes is merely looser.
struct TopNBoundaryTracker {
    std::vector<int64_t> heap;
    uint32_t len = 0;

    // Fold the rows of `m` (column `col`) in, and publish once n values are held.
    // The heap grows only as rows arrive — never to `n` up front, so a large LIMIT
    // costs memory in proportion to the rows actually seen (bounded by n, the same
    // order as TopNSink's own candidate set).
    void observe(const CxxMorsel* m, int32_t col, uint32_t n, TopNBoundary& b) {
        if (n == 0u || m == nullptr) return;
        const size_t need = std::min<size_t>(
            n, static_cast<size_t>(len) + static_cast<size_t>(m->num_rows()));
        if (heap.size() < need) heap.resize(need);
        if (cxx_ordinal_topn_c(m, col, n, b.ascending ? 1 : 0, heap.data(), &len) != 1)
            return;   // no ordinal for this column: never publishes, skips nothing
        if (len == n) b.offer(heap[0]);
    }
};

}  // namespace opteryx::engine
