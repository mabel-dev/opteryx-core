// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// See the License at http://www.apache.org/licenses/LICENSE-2.0
// Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

// src/cpp/planner/stats_store.hpp — one query's estimated statistics, native
// (native plan graph P5, architect rulings 2026-09-27/28).
//
// The statistics refresh records, for every plan node it reaches, the
// estimated statistics of that node's OUTPUT relation:
//   - a relation's statistics are its row count - a METRIC (a number we claim
//     to know) or an ESTIMATE, exactly one of the two - its pre-filter base row
//     count, and one ColumnStats per column it carries;
//   - columns are keyed by ROOT SLOT (ColumnRows::root): every row of one
//     identity shares a root, so the key means what the identity bytes meant;
//   - nodes are keyed by NodeId (probe 2026-09-27: no read ever reaches a node
//     object other than the one last refreshed under its id);
//   - a shared CTE's output statistics are keyed by its cte_key.
// Everything is immutable once recorded and shared by pointer: a propagator
// that changes one column builds one new ColumnStats and shares the rest.
//
// Row counts and NDVs, which the Python planner carried unbounded, are int64
// here, saturated at INT64_MAX - the cap the join estimator already applies to
// row counts (architect ruling 2026-09-24); an NDV is capped by its row count.
// Byte totals are the exception (ColumnStats::total_bytes).

#pragma once

#include <cmath>
#include <cstdint>
#include <limits>
#include <memory>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "opteryx/third_party/maki_nage/_distogram.hpp"
#include "planner/column_table.hpp"

namespace opteryx::planner {

// A plan node's id (plan_graph.hpp's NodeId; not included here - the store
// holds no Python, and the graph holds each node's Python step).
using StatsNodeId = uint32_t;

inline constexpr int64_t kNoStat = INT64_MIN;   // an absent optional integer statistic

// ---------------------------------------------------------------------------
// A value_range bound: a NUMBER that keeps the kind it arrived as (an integer
// or a float), because the kind is part of its meaning - only an integer range
// implies a distinct-value span. Nothing else is a bound (strings, decimals and
// booleans never were: the refresh refused them at both inlets). The integer is
// 128 bits wide so it holds every integer the planner meets exactly - int64
// literals and bounds, and UINT64 columns' bounds past INT64_MAX.
// ---------------------------------------------------------------------------
struct StatBound {
    enum Kind : uint8_t { NONE = 0, INT = 1, FLOAT = 2 };
    Kind kind = NONE;
    __int128 i = 0;
    double d = 0.0;

    static StatBound of_int(int64_t v) {
        StatBound b;
        b.kind = INT;
        b.i = v;
        return b;
    }
    static StatBound of_uint(uint64_t v) {
        StatBound b;
        b.kind = INT;
        b.i = static_cast<__int128>(v);
        return b;
    }
    static StatBound of_float(double v) {
        StatBound b;
        b.kind = FLOAT;
        b.d = v;
        return b;
    }
    bool present() const { return kind != NONE; }
    // Python's float(): an integer correctly rounded
    double as_double() const { return kind == INT ? static_cast<double>(i) : d; }
};

namespace stat_detail {

// Exact ordering of an integer against a double (no rounding of the integer):
// -1, 0, 1, or 2 when unordered (NaN). Python compares int and float exactly.
inline int compare_int_double(__int128 i, double d) {
    if (std::isnan(d)) return 2;
    constexpr double kTwo127 = 170141183460469231731687303715884105728.0;   // 2^127
    if (d >= kTwo127) return -1;     // above every int128
    if (d < -kTwo127) return 1;      // below every int128
    const double t = std::trunc(d);  // in [-2^127, 2^127): fits
    const __int128 ti = static_cast<__int128>(t);
    if (i < ti) return -1;
    if (i > ti) return 1;
    if (d > t) return -1;            // i == trunc(d): the fraction decides
    if (d < t) return 1;
    return 0;
}

// a < b over present bounds, Python semantics (unordered compares false).
inline bool less(const StatBound& a, const StatBound& b) {
    if (a.kind == StatBound::INT && b.kind == StatBound::INT) return a.i < b.i;
    if (a.kind == StatBound::FLOAT && b.kind == StatBound::FLOAT) return a.d < b.d;
    if (a.kind == StatBound::INT) return compare_int_double(a.i, b.d) == -1;
    return compare_int_double(b.i, a.d) == 1;
}

}  // namespace stat_detail

// Python's max(a, b) / min(a, b): the FIRST argument unless the second is
// strictly greater / smaller - so a tie keeps a's kind.
inline StatBound bound_max(const StatBound& a, const StatBound& b) {
    return stat_detail::less(a, b) ? b : a;
}
inline StatBound bound_min(const StatBound& a, const StatBound& b) {
    return stat_detail::less(b, a) ? b : a;
}

// ---------------------------------------------------------------------------
// One column's statistics at one plan node.
// ---------------------------------------------------------------------------
inline constexpr int kCharClasses = 8;   // selectivity's _CHAR_CLASSES, in its order

struct ColumnStats {
    int64_t distinct_count = kNoStat;
    // PRE-filter distinct count - the key domain; kNoStat means "same as
    // distinct_count". Filters shrink the live count, never the domain.
    int64_t base_distinct_count = kNoStat;
    StatBound lower;
    StatBound upper;
    std::shared_ptr<const maki_nage::Distogram> histogram;
    bool has_null_fraction = false;
    double null_fraction = 0.0;
    // LIKE '%needle%' inputs: each class's share of the column's bytes, and
    // the mean non-null value length in bytes. Present together or not at all.
    bool has_char_class = false;
    double class_proportions[kCharClasses] = {};
    double avg_length = 0.0;
    // Relation-wide ordinal-key range (STARTS_WITH), and observed byte-length
    // range (the containment estimators' hard impossibility guard).
    bool has_ordinal_bounds = false;
    int64_t ordinal_lo = 0;
    int64_t ordinal_hi = 0;
    bool has_length_bounds = false;
    int64_t length_lo = 0;
    int64_t length_hi = 0;
    // Dense logical bytes of the column's values at this node (a relation
    // total, rescaled wherever the row count changes). 128 bits and NOT
    // saturated: a cross join's row count saturates at INT64_MAX, and the
    // filters above it bring the count back into range - bytes saturated
    // alongside would lose the per-row width they carry (8 bytes for a row of
    // int64 came back as 1). Bounded by the widest bytes-per-row x INT64_MAX.
    bool has_total_bytes = false;
    __int128 total_bytes = 0;

    int64_t domain_distinct_count() const {
        return base_distinct_count == kNoStat ? distinct_count : base_distinct_count;
    }
};

using ColumnStatsPtr = std::shared_ptr<const ColumnStats>;

// ---------------------------------------------------------------------------
// One relation's statistics at one plan node.
// ---------------------------------------------------------------------------
struct RelationStats {
    // Exactly one is set (the other kNoStat): a count with no provenance, or
    // two competing ones, is the dishonesty the split exists to stop.
    int64_t row_count_metric = kNoStat;
    int64_t row_count_estimate = kNoStat;
    // Pre-filter row count of the largest base relation underneath - a DOMAIN
    // size; kNoStat means "same as row_count".
    int64_t base_row_count = kNoStat;
    // (root slot, stats) in the order the columns were recorded.
    std::vector<std::pair<Slot, ColumnStatsPtr>> columns;

    static RelationStats with_metric(int64_t rows) {
        RelationStats r;
        r.row_count_metric = rows;
        return r;
    }
    static RelationStats with_estimate(int64_t rows) {
        RelationStats r;
        r.row_count_estimate = rows;
        return r;
    }

    void check() const {
        if ((row_count_metric == kNoStat) == (row_count_estimate == kNoStat)) {
            throw std::logic_error("exactly one of row_count_metric / row_count_estimate must be set");
        }
    }
    int64_t row_count() const { return row_count_metric != kNoStat ? row_count_metric : row_count_estimate; }
    bool row_count_is_metric() const { return row_count_metric != kNoStat; }
    int64_t domain_row_count() const { return base_row_count == kNoStat ? row_count() : base_row_count; }

    // The column rooted at `root`, or nullptr.
    const ColumnStats* column(Slot root) const {
        for (const auto& entry : columns) {
            if (entry.first == root) return entry.second.get();
        }
        return nullptr;
    }
    ColumnStatsPtr column_ptr(Slot root) const {
        for (const auto& entry : columns) {
            if (entry.first == root) return entry.second;
        }
        return nullptr;
    }
    // Record `stats` for `root`, replacing an existing entry in place (its
    // position kept) or appending.
    void set_column(Slot root, ColumnStatsPtr stats) {
        for (auto& entry : columns) {
            if (entry.first == root) {
                entry.second = std::move(stats);
                return;
            }
        }
        columns.emplace_back(root, std::move(stats));
    }
};

using RelationStatsPtr = std::shared_ptr<const RelationStats>;

// ---------------------------------------------------------------------------
// The scan base memo's key: everything a scan's pre-narrowing statistics are
// computed from. The manifest is named by its identity token (held, so never
// reused as a freed manifest's address can be), with the schema it was bound
// under; `all_columns` stands for "no
// referenced column could be established" (every schema column is walked).
// ---------------------------------------------------------------------------
struct ScanBaseKey {
    std::shared_ptr<const void> manifest;      // NativeManifest::identity(); null: no manifest
    std::vector<Slot> manifest_schema_slots;
    bool has_schema = false;
    std::vector<Slot> schema_slots;
    int64_t schema_row_count_metric = kNoStat;
    int64_t schema_row_count_estimate = kNoStat;
    bool all_columns = false;
    std::vector<Slot> wanted;                  // referenced root slots, sorted

    bool operator==(const ScanBaseKey& o) const {
        return manifest.get() == o.manifest.get() && manifest_schema_slots == o.manifest_schema_slots &&
               has_schema == o.has_schema && schema_slots == o.schema_slots &&
               schema_row_count_metric == o.schema_row_count_metric &&
               schema_row_count_estimate == o.schema_row_count_estimate && all_columns == o.all_columns &&
               wanted == o.wanted;
    }
};

struct ScanBaseKeyHash {
    static void mix(size_t& h, uint64_t v) { h ^= std::hash<uint64_t>{}(v) + 0x9e3779b97f4a7c15ULL + (h << 6) + (h >> 2); }
    size_t operator()(const ScanBaseKey& k) const {
        size_t h = 0;
        mix(h, reinterpret_cast<uintptr_t>(k.manifest.get()));
        for (Slot s : k.manifest_schema_slots) mix(h, s);
        mix(h, k.has_schema);
        for (Slot s : k.schema_slots) mix(h, s);
        mix(h, static_cast<uint64_t>(k.schema_row_count_metric));
        mix(h, static_cast<uint64_t>(k.schema_row_count_estimate));
        mix(h, k.all_columns);
        for (Slot s : k.wanted) mix(h, s);
        return h;
    }
};

// ---------------------------------------------------------------------------
// The query's statistics: per node, per shared CTE, and the scan base memo
// that every refresh (and the billing meter) of the query shares.
// ---------------------------------------------------------------------------
class StatsStore {
public:
    const RelationStats* node(StatsNodeId id) const {
        auto it = nodes_.find(id);
        return it == nodes_.end() ? nullptr : it->second.get();
    }
    RelationStatsPtr node_ptr(StatsNodeId id) const {
        auto it = nodes_.find(id);
        return it == nodes_.end() ? nullptr : it->second;
    }
    void set_node(StatsNodeId id, RelationStatsPtr stats) {
        stats->check();
        nodes_[id] = std::move(stats);
    }

    const RelationStats* cte(const std::string& key) const {
        auto it = ctes_.find(key);
        return it == ctes_.end() ? nullptr : it->second.get();
    }
    RelationStatsPtr cte_ptr(const std::string& key) const {
        auto it = ctes_.find(key);
        return it == ctes_.end() ? nullptr : it->second;
    }
    void set_cte(const std::string& key, RelationStatsPtr stats) {
        stats->check();
        ctes_[key] = std::move(stats);
    }

    RelationStatsPtr scan_base(const ScanBaseKey& key) const {
        auto it = scan_bases_.find(key);
        return it == scan_bases_.end() ? nullptr : it->second;
    }
    void set_scan_base(ScanBaseKey key, RelationStatsPtr stats) {
        stats->check();
        scan_bases_[std::move(key)] = std::move(stats);
    }

private:
    std::unordered_map<StatsNodeId, RelationStatsPtr> nodes_;
    std::unordered_map<std::string, RelationStatsPtr> ctes_;
    std::unordered_map<ScanBaseKey, RelationStatsPtr, ScanBaseKeyHash> scan_bases_;
};

}  // namespace opteryx::planner
