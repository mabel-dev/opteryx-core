// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// See the License at http://www.apache.org/licenses/LICENSE-2.0
// Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

// src/cpp/planner/statistics_refresh.hpp — the statistics refresh, native
// (native plan graph P5, S5).
//
// A bottom-up walk of a logical plan recording, in the query's StatsStore, the
// estimated statistics of every node's output. Ported rule for rule from the
// Python optimizer/statistics_refresh.py (replaced at the P5 cut-over; the port
// was verified identical over every refresh of the planner corpus and test
// suites). In brief:
//   Scan       manifest/schema base statistics (memoized per query), narrowed
//              by the leaf-local Filter conjuncts it can claim, by its pushed
//              predicates, by a pushed aggregate or DISTINCT, and by a pushed
//              LIMIT;
//   Filter     the conjuncts no scan claimed: selectivity, range narrowing,
//              NDV scaling and capping;
//   Join       cross / semi-anti / ASOF / keyless / equi-key (key classes,
//              occupancy bound, the native join estimator);
//   Aggregate  group-key NDV product (one row with no keys);
//   Limit      min(rows - offset, limit);  Distinct, Project (distinctness-
//   preserving CASTs), Union, Intersect / Except, shared CTE references;
//   anything else passes its first input through.
// Plan steps are read ONLY through their native rows (step_row.hpp), gathered
// once per refresh; the plan graph's edges give the walk.
//
// Where the Python caught an estimator's exception and priced the predicate at
// 1.0, the estimator here throws SelectivityRefused and the refresh prices it
// at 1.0 - the one refusal the estimators make. Everything else that fails,
// fails.

#pragma once

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <memory>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "core/buffers.h"
#include "planner/column_table.hpp"
#include "planner/column_type.hpp"
#include "planner/expr_arena.hpp"
#include "planner/join_estimator.hpp"
#include "planner/manifest_estimates.hpp"
#include "planner/native_manifest.hpp"
#include "planner/plan_graph.hpp"
#include "planner/py_numeric.hpp"
#include "planner/selectivity.hpp"
#include "planner/stats_store.hpp"
#include "planner/step_row.hpp"

namespace opteryx::planner {

// A scan with no manifest count and no schema estimate: obviously synthetic,
// never zero (a zero-row side collapses every join above it).
inline constexpr int64_t kUnknownRowCount = 1000000;

// opteryx.planner.logical_planner.LogicalPlanStepType values, handed over once.
struct StepKinds {
    int32_t scan = 0, filter = 0, join = 0, aggregate_and_group = 0, aggregate = 0, limit = 0;
    int32_t heap_sort = 0, distinct = 0, project = 0, union_ = 0, intersect = 0, except_ = 0;
    int32_t materialized_cte_ref = 0, order = 0;
};

// One predicate's estimate, for telemetry (the condition's text is rendered by
// the caller). `estimator` is null for a predicate kind with no tiers.
struct PredicateNote {
    NodeId nid;
    bool scan;                 // Scan, else Filter
    std::string relation;      // the scan's relation (a Filter's is none)
    ExprId condition;
    double selectivity;
    bool has_cost;
    double cost;
    const char* estimator;
};

struct JoinNote {
    NodeId nid;
    bool has_join_type;
    std::string join_type;
    int64_t left_rows;
    int64_t right_rows;
    int64_t out_rows;
    int64_t key_count;
};

struct RefreshTelemetry {
    std::vector<PredicateNote> predicates;
    std::vector<JoinNote> joins;
};

struct RefreshInputs {
    const PlanGraph* graph = nullptr;
    const std::vector<const StepRow*>* rows = nullptr;   // by NodeId; null where absent
    SelectivityInputs selectivity;
    const StepKinds* steps = nullptr;
    const std::unordered_map<std::string, double>* function_costs = nullptr;   // telemetry only
    StatsStore* store = nullptr;
    RefreshTelemetry* telemetry = nullptr;   // null: none recorded
};

namespace refresh_detail {

using Cols = std::vector<std::pair<Slot, ColumnStatsPtr>>;

inline constexpr int64_t kInt64Max = INT64_MAX;

inline const ColumnStats* find(const Cols& cols, Slot key) {
    for (const auto& e : cols) {
        if (e.first == key) return e.second.get();
    }
    return nullptr;
}

inline bool contains(const Cols& cols, Slot key) { return find(cols, key) != nullptr; }

// dict[key] = value: replaced in place, else appended.
inline void put(Cols& cols, Slot key, ColumnStatsPtr value) {
    for (auto& e : cols) {
        if (e.first == key) {
            e.second = std::move(value);
            return;
        }
    }
    cols.emplace_back(key, std::move(value));
}

inline std::shared_ptr<ColumnStats> copy_of(const ColumnStats& c) { return std::make_shared<ColumnStats>(c); }

inline int64_t saturate(__int128 v) {
    if (v > static_cast<__int128>(kInt64Max)) return kInt64Max;
    if (v < static_cast<__int128>(INT64_MIN + 1)) return INT64_MIN + 1;   // INT64_MIN is kNoStat
    return static_cast<int64_t>(v);
}

// `int(x)` of a float statistic, saturated like every other integer here.
inline int64_t int_of(double x) {
    if (!(x < 9223372036854775808.0)) return kInt64Max;
    if (!(x >= -9223372036854775807.0)) return INT64_MIN + 1;
    return static_cast<int64_t>(x);
}

// `int(x)` of a float byte total (never near 2^127: see ColumnStats).
inline __int128 int_of_128(double x) {
    if (!(x < 170141183460469231731687303715884105728.0) || !(x > -170141183460469231731687303715884105728.0)) {
        throw std::overflow_error("a byte total outgrew 128 bits");
    }
    return static_cast<__int128>(x);
}

inline RelationStatsPtr make_relation(bool metric, int64_t rows, int64_t base_rows, Cols columns) {
    auto r = std::make_shared<RelationStats>();
    if (metric) r->row_count_metric = rows;
    else r->row_count_estimate = rows;
    r->base_row_count = base_rows;
    r->columns = std::move(columns);
    return r;
}

inline RelationStatsPtr empty_stats(int64_t rows = 0, bool metric = false) {
    return make_relation(metric, std::max<int64_t>(0, rows), kNoStat, Cols{});
}

// A statistics object with its count demoted to an estimate (itself when it
// already is one).
inline RelationStatsPtr as_estimate(const RelationStatsPtr& r) {
    if (!r->row_count_is_metric()) return r;
    return make_relation(false, r->row_count_metric, r->base_row_count, r->columns);
}

// new_rows / old_rows (1.0 or 0.0 when there was nothing to scale).
inline double ratio(int64_t new_rows, int64_t old_rows) {
    if (old_rows <= 0) return new_rows == old_rows ? 1.0 : 0.0;
    return py::true_divide(new_rows, old_rows);
}

// Every column's bytes scaled by `ratio`.
inline Cols scale_total_bytes(const Cols& cols, double r) {
    if (r == 1.0) return cols;
    Cols out;
    out.reserve(cols.size());
    for (const auto& e : cols) {
        if (!e.second->has_total_bytes) {
            out.push_back(e);
            continue;
        }
        auto c = copy_of(*e.second);
        c->total_bytes = std::max<__int128>(0, int_of_128(static_cast<double>(e.second->total_bytes) * r));
        out.emplace_back(e.first, std::move(c));
    }
    return out;
}

// A join's columns scaled by the ratio of the side each came from.
inline Cols scale_total_bytes_by_origin(const Cols& merged, const RelationStats& left, const RelationStats& right,
                                        int64_t out_rows) {
    const double ratio_left = ratio(out_rows, left.row_count());
    const double ratio_right = ratio(out_rows, right.row_count());
    if (ratio_left == 1.0 && ratio_right == 1.0) return merged;
    Cols out;
    out.reserve(merged.size());
    for (const auto& e : merged) {
        if (!e.second->has_total_bytes) {
            out.push_back(e);
            continue;
        }
        const double r = contains(left.columns, e.first) ? ratio_left : ratio_right;
        auto c = copy_of(*e.second);
        c->total_bytes = std::max<__int128>(0, int_of_128(static_cast<double>(e.second->total_bytes) * r));
        out.emplace_back(e.first, std::move(c));
    }
    return out;
}

// A relation holds no more distinct values than rows.
inline Cols cap_ndvs(const Cols& cols, int64_t row_count) {
    Cols out;
    out.reserve(cols.size());
    for (const auto& e : cols) {
        if (e.second->distinct_count != kNoStat && e.second->distinct_count > row_count) {
            auto c = copy_of(*e.second);
            c->distinct_count = std::max<int64_t>(1, row_count);
            out.emplace_back(e.first, std::move(c));
        } else {
            out.push_back(e);
        }
    }
    return out;
}

// NDV after a filter, by surviving_distinct_count over the PRE-filter count,
// never above what range narrowing already set; the domain carried forward.
inline Cols scale_ndvs(const Cols& cols, const RelationStats& base, double selectivity) {
    if (selectivity >= 1.0) return cols;
    Cols out;
    out.reserve(cols.size());
    for (const auto& e : cols) {
        const ColumnStats& col = *e.second;
        const ColumnStats* source = find(base.columns, e.first);
        const int64_t domain = source == nullptr ? col.domain_distinct_count() : source->domain_distinct_count();
        const int64_t pre_filter = source == nullptr ? col.distinct_count : source->distinct_count;
        if (pre_filter == kNoStat) {
            if (domain == kNoStat) {
                out.push_back(e);
            } else {
                auto c = copy_of(col);
                c->base_distinct_count = domain;
                out.emplace_back(e.first, std::move(c));
            }
            continue;
        }
        int64_t scaled = surviving_distinct_count(pre_filter, base.row_count(), selectivity);
        if (col.distinct_count != kNoStat) scaled = std::min(scaled, col.distinct_count);
        auto c = copy_of(col);
        c->distinct_count = scaled;
        c->base_distinct_count = domain;
        out.emplace_back(e.first, std::move(c));
    }
    return out;
}

inline Cols drop_histograms(const Cols& cols) {
    bool any = false;
    for (const auto& e : cols) any = any || static_cast<bool>(e.second->histogram);
    if (!any) return cols;
    Cols out;
    out.reserve(cols.size());
    for (const auto& e : cols) {
        if (!e.second->histogram) {
            out.push_back(e);
            continue;
        }
        auto c = copy_of(*e.second);
        c->histogram.reset();
        out.emplace_back(e.first, std::move(c));
    }
    return out;
}

// left's columns, then right's that left does not hold.
inline Cols merge_columns(const RelationStats& left, const RelationStats& right) {
    Cols merged = left.columns;
    for (const auto& e : right.columns) {
        if (!contains(merged, e.first)) merged.push_back(e);
    }
    return merged;
}

// Distinct-value upper bound implied by an INTEGER value range (none else).
inline bool value_range_span(const ColumnStats& col, __int128& span) {
    if (col.lower.kind != StatBound::INT || col.upper.kind != StatBound::INT) return false;
    if (col.upper.i < col.lower.i) return false;
    span = col.upper.i - col.lower.i + 1;
    return true;
}

// composite_key_ndv: the max of the known ones.
inline int64_t composite_key_ndv(const std::vector<int64_t>& ndvs) {
    bool found = false;
    int64_t best = 0;
    for (int64_t v : ndvs) {
        if (!found || v > best) {
            best = v;
            found = true;
        }
    }
    return found ? best : kNoStat;
}

// Python's max() over a non-empty list of floats.
inline double list_max(const std::vector<double>& values) {
    double best = values[0];
    for (size_t i = 1; i < values.size(); ++i) {
        if (values[i] > best) best = values[i];
    }
    return best;
}

}  // namespace refresh_detail

namespace refresh_detail {

// A column expression's key: its bound column's root slot, or kNoSlot.
inline Slot expr_key(const SelectivityInputs& in, ExprId id) {
    if (id == kNoExpr) return kNoSlot;
    const uint32_t slot = in.exprs->row(id).column_slot;
    return slot == kNoColumnSlot ? kNoSlot : in.columns->root(slot);
}

// Every IDENTIFIER's column in the subtree.
inline void identifier_keys(const SelectivityInputs& in, ExprId id, std::vector<Slot>& out) {
    if (id == kNoExpr) throw std::logic_error("a predicate holds no expression");
    const ExprRow& r = in.exprs->row(id);
    if (r.kind == in.kinds->identifier) {
        const Slot k = expr_key(in, id);
        if (k != kNoSlot) out.push_back(k);
    }
    std::vector<ExprId> kids;
    expression_children(r, *in.kinds, kids);
    for (ExprId child : kids) identifier_keys(in, child, out);
}

// The column keys the query can consult on a scan (its output columns and
// every column its pushed predicates read), sorted; `all` when none is.
inline void referenced_keys(const SelectivityInputs& in, const StepRow& r, std::vector<Slot>& wanted, bool& all) {
    wanted.clear();
    for (ExprId c : r.columns) {
        const Slot k = expr_key(in, c);
        if (k != kNoSlot) wanted.push_back(k);
    }
    for (ExprId p : r.predicates) identifier_keys(in, p, wanted);
    std::sort(wanted.begin(), wanted.end());
    wanted.erase(std::unique(wanted.begin(), wanted.end()), wanted.end());
    all = wanted.empty();
}

// Manifest._identity_category: the first column of the manifest's schema with
// this name is an INTEGER or DATE.
inline bool identity_category(const SelectivityInputs& in, const std::vector<Slot>& manifest_schema_slots,
                              const std::string& name) {
    for (Slot slot : manifest_schema_slots) {
        const ColumnRow& c = in.columns->row(slot);
        if (c.name != name) continue;
        if (c.type_id == kNoColumnType) return false;
        switch (in.types->entry(c.type_id).physical) {
            case DRAKEN_INT8: case DRAKEN_INT16: case DRAKEN_INT32: case DRAKEN_INT64:
            case DRAKEN_UINT8: case DRAKEN_UINT16: case DRAKEN_UINT32: case DRAKEN_UINT64:
            case DRAKEN_DATE32:
                return true;
            default:
                return false;
        }
    }
    return false;
}

inline StatBound bound_of(const estimate_detail::End& e) {
    if (e.tag == DECODED_INT64) return StatBound::of_int(e.i);
    if (e.tag == DECODED_UINT64) return StatBound::of_uint(e.u());
    return StatBound::of_float(e.d);
}

// One column's statistics as the manifest records them: NDV (a sketch, else
// the range-derived costing fallback), histogram, numeric value range (when
// asked for), null fraction (when any file counts nulls), byte classes,
// ordinal and length bounds; `encoded_bytes` the footer's encoded size.
inline void manifest_column_stats(const NativeManifest& m, size_t pos, bool identity, bool null_counts,
                                  bool value_ranges, ColumnStats& c, int64_t& encoded_bytes) {
    double estimate = -1.0;
    const int64_t count = estimate_cardinality(m, pos, estimate);
    if (estimate >= 0.0) c.distinct_count = int_of(estimate);
    else if (count != kUnknown) c.distinct_count = count;
    if (c.distinct_count == kNoStat) {
        const int64_t range = estimate_range_cardinality(m, pos, identity);
        if (range != kUnknown) c.distinct_count = range;
    }
    c.histogram = column_distogram(m, pos);
    if (value_ranges) {
        estimate_detail::End lo, hi;
        if (value_range(m, pos, identity, lo, hi)) {
            c.lower = bound_of(lo);
            c.upper = bound_of(hi);
        }
    }
    if (null_counts) {
        double fraction = 0.0;
        if (null_fraction(m, pos, fraction)) {
            c.has_null_fraction = true;
            c.null_fraction = fraction;
        }
    }
    int64_t totals[8];
    int64_t non_null_rows = 0;
    if (char_class_stats(m, pos, totals, non_null_rows)) {
        __int128 total = 0;
        for (int k = 0; k < 8; ++k) total += totals[k];
        if (total > 0) {
            c.has_char_class = true;
            for (int k = 0; k < 8; ++k) c.class_proportions[k] = py::true_divide(totals[k], total);
            c.avg_length = py::true_divide(total, std::max<int64_t>(1, non_null_rows));
        }
    }
    int64_t lo_i = 0, hi_i = 0;
    if (ordinal_bounds(m, pos, lo_i, hi_i)) {
        c.has_ordinal_bounds = true;
        c.ordinal_lo = lo_i;
        c.ordinal_hi = hi_i;
    }
    if (length_bounds(m, pos, lo_i, hi_i)) {
        c.has_length_bounds = true;
        c.length_lo = lo_i;
        c.length_hi = hi_i;
    }
    const int64_t encoded = total_uncompressed_size(m, pos);
    encoded_bytes = encoded == kUnknown ? kNoStat : encoded;
}

// The manifest's count (metric only when its statistics are authoritative: a
// hint manifest's count is the best number there is, not a known one).
inline bool manifest_rows(const NativeManifest& m, int64_t& rows, bool& metric) {
    const int64_t count = m.record_count();
    if (count == kUnknown) return false;
    rows = count;
    metric = m.stats_are_authoritative();
    return true;
}

}  // namespace refresh_detail

// A scan's statistics BEFORE any predicate or limit narrows them - the rows
// and bytes it reads - from its manifest and schema, over the columns the
// query consults.
inline RelationStatsPtr compute_scan_base(const SelectivityInputs& in, const StepRow& r, const ScanBaseKey& key) {
    using namespace refresh_detail;
    const NativeManifest* m = r.manifest;
    int64_t rows = kNoStat;
    bool metric = false;
    if (m != nullptr) manifest_rows(*m, rows, metric);
    if (rows == kNoStat && r.has_schema) {
        rows = r.schema_row_count_metric;
        metric = rows != kNoStepValue;
        if (rows == kNoStepValue) rows = r.schema_row_count_estimate;
    }
    if (rows == kNoStat || rows <= 0) {
        rows = kUnknownRowCount;
        metric = false;
    }

    Cols cols;
    const bool null_counts = m != nullptr && has_null_counts(*m);
    if (r.has_schema) {
        for (Slot slot : r.schema_slots) {
            const ColumnRow& column = in.columns->row(slot);
            const Slot root = in.columns->root(slot);
            if (!key.all_columns && !std::binary_search(key.wanted.begin(), key.wanted.end(), root)) continue;
            if (column.name.empty()) continue;
            auto c = std::make_shared<ColumnStats>();
            int64_t encoded_bytes = kNoStat;
            if (m != nullptr) {
                auto found = m->positions().find(column.name);
                if (found != m->positions().end()) {
                    manifest_column_stats(*m, found->second, identity_category(in, r.manifest_schema_slots, column.name),
                                          null_counts, true, *c, encoded_bytes);
                }
            }
            // dense logical bytes: fixed width x rows, else average length x
            // rows, else (last resort) the footer's encoded size
            if (column.type_id != kNoColumnType) {
                const uint64_t width = draken_type_fixed_itemsize(in.types->entry(column.type_id).physical);
                if (width != 0) {
                    c->has_total_bytes = true;
                    c->total_bytes = static_cast<__int128>(width) * rows;
                }
            }
            if (!c->has_total_bytes && c->has_char_class) {
                c->has_total_bytes = true;
                c->total_bytes = int_of_128(c->avg_length * static_cast<double>(rows));
            }
            if (!c->has_total_bytes && encoded_bytes != kNoStat) {
                c->has_total_bytes = true;
                c->total_bytes = encoded_bytes;
            }
            put(cols, root, std::move(c));
        }
    }
    // the pre-filter size is the domain every narrowing shrinks
    return make_relation(metric, rows, rows, std::move(cols));
}

// compute_scan_base, memoized in the query's store: the base depends only on
// the manifest, the schemas and the consulted columns.
inline RelationStatsPtr scan_base_statistics(const SelectivityInputs& in, StatsStore& store, const StepRow& r) {
    ScanBaseKey key;
    if (r.manifest != nullptr) {
        key.manifest = r.manifest->identity();
        key.manifest_schema_slots = r.manifest_schema_slots;
    }
    key.has_schema = r.has_schema;
    key.schema_slots = r.schema_slots;
    key.schema_row_count_metric = r.schema_row_count_metric;
    key.schema_row_count_estimate = r.schema_row_count_estimate;
    refresh_detail::referenced_keys(in, r, key.wanted, key.all_columns);
    if (RelationStatsPtr cached = store.scan_base(key)) return cached;
    RelationStatsPtr base = compute_scan_base(in, r, key);
    store.set_scan_base(std::move(key), base);
    return base;
}

// A manifest's statistics over the schema it is bound with, for estimating a
// predicate against the manifest alone (Manifest.estimate_selectivity): no
// value ranges and no byte sizes - only what selectivity reads.
inline RelationStatsPtr manifest_statistics(const SelectivityInputs& in, const NativeManifest& m,
                                            const std::vector<Slot>& schema_slots) {
    using namespace refresh_detail;
    int64_t rows = kNoStat;
    bool metric = false;
    if (!manifest_rows(m, rows, metric)) {
        rows = kUnknownRowCount;
        metric = false;
    }
    Cols cols;
    const bool null_counts = has_null_counts(m);
    for (Slot slot : schema_slots) {
        const ColumnRow& column = in.columns->row(slot);
        if (column.name.empty()) continue;
        auto c = std::make_shared<ColumnStats>();
        auto found = m.positions().find(column.name);
        if (found != m.positions().end()) {
            int64_t encoded_bytes = kNoStat;
            manifest_column_stats(m, found->second, identity_category(in, schema_slots, column.name), null_counts,
                                  false, *c, encoded_bytes);
        }
        put(cols, in.columns->root(slot), std::move(c));
    }
    return make_relation(metric, rows, kNoStat, std::move(cols));
}

class StatisticsRefresh {
public:
    explicit StatisticsRefresh(const RefreshInputs& in) : in_(in) {}

    void run() {
        for (NodeId nid : in_.graph->exit_points()) visit(nid);
    }

    // Node `nid` alone, from the statistics its inputs already hold in the
    // store (none are recomputed); an input with none has none.
    void run_one(NodeId nid) {
        std::vector<Child> children;
        for (const Edge& edge : in_.graph->node(nid).in) {
            children.push_back(Child{in_.store->node_ptr(edge.other), edge.role});
        }
        in_.store->set_node(nid, compute(nid, row(nid), children));
    }

private:
    using Cols = refresh_detail::Cols;
    struct Child {
        RelationStatsPtr stats;
        EdgeRole role;
    };

    const RefreshInputs& in_;
    std::unordered_set<NodeId> visited_;
    // conjuncts a scan has folded into its statistics (by ExprId): a Filter
    // above skips exactly these
    std::unordered_set<ExprId> folded_;

    const ExprTable& exprs() const { return *in_.selectivity.exprs; }
    const ColumnRows& columns() const { return *in_.selectivity.columns; }
    const NodeKinds& nk() const { return *in_.selectivity.kinds; }
    const StepKinds& sk() const { return *in_.steps; }
    const ExprRow& expr(ExprId id) const { return exprs().row(id); }

    const StepRow& row(NodeId nid) const {
        const StepRow* r = nid < in_.rows->size() ? (*in_.rows)[nid] : nullptr;
        if (r == nullptr) throw std::logic_error("statistics refresh: no step row for node " + std::to_string(nid));
        if (r->arena_conflict || (r->arena != nullptr && r->arena != in_.selectivity.exprs)) {
            throw std::logic_error("statistics refresh: node " + std::to_string(nid) +
                                   " holds expressions of another query");
        }
        return *r;
    }

    // ---- walking -----------------------------------------------------------

    void visit(NodeId nid) {
        if (!visited_.insert(nid).second) return;
        std::vector<Child> children;
        for (const Edge& edge : in_.graph->node(nid).in) {
            visit(edge.other);
            children.push_back(Child{in_.store->node_ptr(edge.other), edge.role});
        }
        in_.store->set_node(nid, compute(nid, row(nid), children));
    }

    RelationStatsPtr compute(NodeId nid, const StepRow& r, const std::vector<Child>& children) {
        const int32_t kind = r.kind;
        if (kind == sk().scan) return scan_stats(nid, r);
        if (kind == sk().materialized_cte_ref) {
            RelationStatsPtr stamped = in_.store->cte_ptr(r.cte_key);
            return stamped ? stamped : refresh_detail::empty_stats(kUnknownRowCount);
        }
        if (kind == sk().filter) return filter_stats(nid, r, children);
        if (kind == sk().join) return join_stats(nid, r, children);
        if (kind == sk().aggregate_and_group) return aggregate_stats(r, children);
        if (kind == sk().aggregate) return refresh_detail::empty_stats(1, true);
        if (kind == sk().limit || kind == sk().heap_sort) return limit_stats(r, children);
        if (kind == sk().distinct) return distinct_stats(nid, children);
        if (kind == sk().project) return project_stats(r, children);
        if (kind == sk().union_) return union_stats(children);
        if (kind == sk().intersect || kind == sk().except_) return set_op_stats(children);
        return first_or_empty(children);
    }

    static RelationStatsPtr first_child(const std::vector<Child>& children) {
        for (const Child& c : children) {
            if (c.stats) return c.stats;
        }
        return nullptr;
    }

    static RelationStatsPtr first_or_empty(const std::vector<Child>& children) {
        RelationStatsPtr first = first_child(children);
        return first ? first : refresh_detail::empty_stats();
    }

    // (left, right) by edge role: one input is the left alone; two must be
    // labelled LEFT and RIGHT.
    static void split_join_children(const std::vector<Child>& children, RelationStatsPtr& left,
                                    RelationStatsPtr& right) {
        if (children.size() == 1) {
            left = children[0].stats;
            right = nullptr;
            return;
        }
        if (children.size() != 2 || children[0].role != EDGE_LEFT || children[1].role != EDGE_RIGHT) {
            throw std::logic_error("A two-input node's legs must be labelled LEFT and RIGHT.");
        }
        left = children[0].stats;
        right = children[1].stats;
    }

    // ---- expressions -------------------------------------------------------

    // A column expression's key: its bound column's root slot, or kNoSlot.
    Slot expr_key(ExprId id) const { return refresh_detail::expr_key(in_.selectivity, id); }

    // A join / group key: an expression's column, or an identity given directly.
    Slot step_key(const StepKey& key) const {
        if (key.expr != kNoExpr) return expr_key(key.expr);
        if (key.identity.empty()) return kNoSlot;
        const Slot root = columns().root_of(key.identity);
        if (root == kNoSlot) throw std::logic_error("a plan key names a column this query never bound");
        return root;
    }

    std::vector<Slot> step_keys(const std::vector<StepKey>& keys) const {
        std::vector<Slot> out;
        for (const StepKey& key : keys) {
            const Slot k = step_key(key);
            if (k != kNoSlot) out.push_back(k);
        }
        return out;
    }

    // _inner_split: NESTED unwrapped, a DNF's terms, an AND's two sides.
    void split_conjuncts(ExprId id, std::vector<ExprId>& out) const {
        if (id == kNoExpr) throw std::logic_error("a conjunction holds no expression");
        while (expr(id).kind == nk().nested) {
            id = expr(id).centre;
            if (id == kNoExpr) throw std::logic_error("a nested expression holds nothing");
        }
        const ExprRow& r = expr(id);
        if (r.kind == nk().dnf) {
            for (ExprId term : r.parameters) split_conjuncts(term, out);
            return;
        }
        if (r.kind != nk().and_) {
            out.push_back(id);
            return;
        }
        split_conjuncts(r.left, out);
        split_conjuncts(r.right, out);
    }

    // Every identifier's source relation in the subtree.
    void identifier_sources(ExprId id, std::unordered_set<std::string>& out) const {
        if (id == kNoExpr) return;
        const ExprRow& r = expr(id);
        if (r.kind == nk().identifier) {
            if (!r.source.empty()) out.insert(r.source);
            return;
        }
        std::vector<ExprId> kids;
        expression_children(r, nk(), kids);
        for (ExprId child : kids) identifier_sources(child, out);
    }

    // ---- selectivity -------------------------------------------------------

    // A predicate the estimator refuses is priced at 1.0 (no reduction).
    double selectivity_of(ExprId predicate, const RelationStats& stats) const {
        try {
            return estimate_selectivity(in_.selectivity, predicate, stats);
        } catch (const SelectivityRefused&) {
            return 1.0;
        }
    }

    void note_predicate(NodeId nid, bool scan, const std::string& relation, ExprId condition, double s,
                        const RelationStats& stats) {
        if (in_.telemetry == nullptr) return;
        PredicateNote note{nid, scan, scan ? relation : std::string(), condition, s, false, 0.0, nullptr};
        try {
            note.cost = predicate_cost(in_.selectivity, condition, *in_.function_costs);
            note.has_cost = true;
        } catch (const SelectivityRefused&) {
        }
        try {
            note.estimator = predicate_estimator_tag(in_.selectivity, condition, stats);
        } catch (const SelectivityRefused&) {
        }
        in_.telemetry->predicates.push_back(note);
    }

    // _narrow_filter_columns: a conjunction's comparison / BETWEEN / IN bounds
    // intersected into the columns' ranges, equality cardinality capping NDV.
    // `changed` is set when the Python returned a new dict (any constraint).
    struct Constraint {
        StatBound lower;
        StatBound upper;
        int64_t eq_card = kNoStat;
    };
    using Constraints = std::vector<std::pair<Slot, Constraint>>;

    static void merge_constraint(Constraints& sink, Slot key, const StatBound& lower, const StatBound& upper,
                                 int64_t eq_card) {
        Constraint* cur = nullptr;
        for (auto& e : sink) {
            if (e.first == key) cur = &e.second;
        }
        if (cur == nullptr) {
            sink.emplace_back(key, Constraint{});
            cur = &sink.back().second;
        }
        if (lower.present()) cur->lower = cur->lower.present() ? bound_max(cur->lower, lower) : lower;
        if (upper.present()) cur->upper = cur->upper.present() ? bound_min(cur->upper, upper) : upper;
        if (eq_card != kNoStat) cur->eq_card = cur->eq_card == kNoStat ? eq_card : std::min(cur->eq_card, eq_card);
    }

    // A literal as a bound: only a plain int or float orders against one.
    static StatBound orderable_bound(const LiteralValue& v) {
        switch (v.tag) {
            case LITERAL_INT64: return StatBound::of_int(v.i);
            case LITERAL_UINT64: return StatBound::of_uint(static_cast<uint64_t>(v.i));
            case LITERAL_DOUBLE: return StatBound::of_float(v.d);
            default: return StatBound{};
        }
    }

    Slot identifier_key(ExprId id) const {
        if (id == kNoExpr || expr(id).kind != nk().identifier) return kNoSlot;
        return expr_key(id);
    }

    const LiteralValue* literal_of(ExprId id) const {
        if (id == kNoExpr || expr(id).kind != nk().literal) return nullptr;
        const LiteralValue& v = expr(id).literal;
        return v.tag == LITERAL_NULL ? nullptr : &v;   // a NULL literal's value is None
    }

    void collect_constraints(ExprId id, Constraints& sink) const {
        if (id == kNoExpr) return;
        const ExprRow& r = expr(id);
        if (r.kind == nk().and_) {
            collect_constraints(r.left, sink);
            collect_constraints(r.right, sink);
            return;
        }
        if (r.kind == nk().or_ || r.kind == nk().not_) return;
        if (r.kind == nk().between) {
            const Slot key = identifier_key(r.left);
            if (key == kNoSlot) return;
            const LiteralValue* a_lit = literal_of(r.right);
            const LiteralValue* b_lit = literal_of(r.centre);
            const StatBound a = a_lit ? orderable_bound(*a_lit) : StatBound{};
            const StatBound b = b_lit ? orderable_bound(*b_lit) : StatBound{};
            if (!a.present() || !b.present()) return;
            // `(a, b) if a <= b else (b, a)`: a <= b is not(b < a) only when ordered
            const bool a_le_b = stat_detail::less(a, b) || (!stat_detail::less(b, a) && ordered_equal(a, b));
            merge_constraint(sink, key, a_le_b ? a : b, a_le_b ? b : a, kNoStat);
            return;
        }
        if (r.kind != nk().comparison) return;
        std::string op = r.value;
        Slot key = identifier_key(r.left);
        ExprId literal = r.right;
        if (key == kNoSlot) {
            key = identifier_key(r.right);
            literal = r.left;
            if (op == "Lt") op = "Gt";
            else if (op == "LtEq") op = "GtEq";
            else if (op == "Gt") op = "Lt";
            else if (op == "GtEq") op = "LtEq";
        }
        if (key == kNoSlot) return;
        const LiteralValue* value = literal_of(literal);
        if (value == nullptr) return;
        if (op == "InList") {
            // an IN list (a tuple: an ARRAY's items, or an INTERVAL's two
            // parts) caps NDV whatever the members; a range needs every
            // member orderable
            std::vector<StatBound> bounds;
            size_t members = 0;
            if (value->tag == LITERAL_ITEMS) {
                members = value->items.size();
                for (const LiteralValue& m : value->items) {
                    const StatBound b = m.tag == LITERAL_NULL ? StatBound{} : orderable_bound(m);
                    if (b.present()) bounds.push_back(b);
                }
            } else if (value->tag == LITERAL_INTERVAL) {
                members = 2;
                bounds.push_back(StatBound::of_int(value->i));
                bounds.push_back(StatBound::of_int(value->j));
            } else {
                return;
            }
            if (members == 0) return;
            StatBound lower, upper;
            if (bounds.size() == members) {
                lower = bounds[0];
                upper = bounds[0];
                for (size_t i = 1; i < bounds.size(); ++i) {
                    lower = bound_min(lower, bounds[i]);
                    upper = bound_max(upper, bounds[i]);
                }
            }
            merge_constraint(sink, key, lower, upper, static_cast<int64_t>(members));
            return;
        }
        const StatBound bound = orderable_bound(*value);
        if (op == "Eq") {
            merge_constraint(sink, key, bound, bound, 1);
        } else if (!bound.present()) {
            return;
        } else if (op == "Lt" || op == "LtEq") {
            merge_constraint(sink, key, StatBound{}, bound, kNoStat);
        } else if (op == "Gt" || op == "GtEq") {
            merge_constraint(sink, key, bound, StatBound{}, kNoStat);
        }
    }

    static bool ordered_equal(const StatBound& a, const StatBound& b) {
        if (a.kind == StatBound::INT && b.kind == StatBound::INT) return a.i == b.i;
        if (a.kind == StatBound::FLOAT && b.kind == StatBound::FLOAT) return a.d == b.d;
        if (a.kind == StatBound::INT) return stat_detail::compare_int_double(a.i, b.d) == 0;
        return stat_detail::compare_int_double(b.i, a.d) == 0;
    }

    Cols narrow_filter_columns(const Cols& cols, ExprId condition, bool& changed) const {
        if (condition == kNoExpr) return cols;
        Constraints constraints;
        collect_constraints(condition, constraints);
        if (constraints.empty()) return cols;
        changed = true;
        Cols out = cols;
        for (const auto& [key, c] : constraints) {
            const ColumnStats* col = refresh_detail::find(out, key);
            if (col == nullptr) continue;
            StatBound lower = col->lower;
            StatBound upper = col->upper;
            if (c.lower.present()) lower = lower.present() ? bound_max(lower, c.lower) : c.lower;
            if (c.upper.present()) upper = upper.present() ? bound_min(upper, c.upper) : c.upper;
            int64_t ndv = col->distinct_count;
            if (c.eq_card != kNoStat) ndv = ndv == kNoStat ? c.eq_card : std::min(ndv, c.eq_card);
            auto updated = refresh_detail::copy_of(*col);
            updated->lower = lower;
            updated->upper = upper;
            updated->distinct_count = ndv;
            refresh_detail::put(out, key, std::move(updated));
        }
        return out;
    }

    // ---- Scan --------------------------------------------------------------

    // Walk up from the scan through transparent steps (Filter, Project, Order,
    // HeapSort, cross joins) collecting the Filter conjuncts whose every
    // identifier binds to one of `names`.
    std::vector<ExprId> leaf_local_conjuncts(NodeId scan, const std::vector<std::string>& names) const {
        std::vector<ExprId> out;
        std::unordered_set<NodeId> seen;
        std::vector<NodeId> frontier{scan};
        while (!frontier.empty()) {
            const NodeId nid = frontier.back();
            frontier.pop_back();
            for (const Edge& edge : in_.graph->node(nid).out) {
                const NodeId parent = edge.other;
                if (!seen.insert(parent).second) continue;
                const StepRow& p = row(parent);
                const bool transparent = p.kind == sk().filter || p.kind == sk().project || p.kind == sk().order ||
                                         p.kind == sk().heap_sort ||
                                         (p.kind == sk().join && p.has_join_type && p.join_type == "cross join");
                if (!transparent) continue;
                if (p.kind == sk().filter && p.condition != kNoExpr) {
                    std::vector<ExprId> conjuncts;
                    split_conjuncts(p.condition, conjuncts);
                    for (ExprId conj : conjuncts) {
                        std::unordered_set<std::string> sources;
                        identifier_sources(conj, sources);
                        if (sources.empty()) continue;
                        bool subset = true;
                        for (const std::string& s : sources) {
                            subset = subset && std::find(names.begin(), names.end(), s) != names.end();
                        }
                        if (subset) out.push_back(conj);
                    }
                }
                frontier.push_back(parent);
            }
        }
        return out;
    }

    // Predicates applied at a scan: selectivity, narrowing, scaling, capping.
    RelationStatsPtr apply_scan_predicates(const RelationStatsPtr& base, const std::vector<ExprId>& predicates,
                                           NodeId nid, const std::string& relation) {
        double selectivity = 1.0;
        bool changed = false;
        Cols narrowed = base->columns;
        for (ExprId conj : predicates) {
            const double s = selectivity_of(conj, *base);
            selectivity *= s;
            note_predicate(nid, true, relation, conj, s, *base);
            narrowed = narrow_filter_columns(narrowed, conj, changed);
        }
        int64_t new_rows = base->row_count();
        if (selectivity != 1.0) {
            new_rows = std::max<int64_t>(1, refresh_detail::int_of(static_cast<double>(base->row_count()) * selectivity));
        }
        narrowed = refresh_detail::scale_total_bytes(narrowed, refresh_detail::ratio(new_rows, base->row_count()));
        narrowed = refresh_detail::scale_ndvs(narrowed, *base, selectivity);
        narrowed = refresh_detail::cap_ndvs(narrowed, new_rows);
        // the capped columns are always a new mapping: the count is an estimate
        return refresh_detail::make_relation(false, new_rows, base->domain_row_count(), std::move(narrowed));
    }

    RelationStatsPtr scan_stats(NodeId nid, const StepRow& r) {
        RelationStatsPtr base = scan_base(r);

        std::vector<std::string> names;
        if (!r.relation.empty()) names.push_back(r.relation);
        if (!r.alias.empty()) names.push_back(r.alias);
        if (!names.empty()) {
            std::vector<ExprId> claimed;
            for (ExprId conj : leaf_local_conjuncts(nid, names)) {
                // a self-join's twin scan matches the same conjunct: the first
                // scan visited claims it
                if (folded_.count(conj) != 0) continue;
                claimed.push_back(conj);
            }
            for (ExprId conj : claimed) folded_.insert(conj);
            if (!claimed.empty()) base = apply_scan_predicates(base, claimed, nid, r.relation);
        }
        if (!r.predicates.empty()) base = apply_scan_predicates(base, r.predicates, nid, r.relation);

        // an aggregate or DISTINCT absorbed into the scan: one row, or one per group
        if (r.has_pushed_aggregates || r.pushed_distinct) {
            std::vector<Slot> keys;
            if (r.has_pushed_aggregates) {
                keys = step_keys(r.pushed_groups);
            } else {
                for (ExprId c : r.columns) {
                    const Slot k = expr_key(c);
                    if (k != kNoSlot) keys.push_back(k);
                }
            }
            if (keys.empty()) return refresh_detail::empty_stats(1, true);
            std::vector<int64_t> ndvs;
            std::vector<uint8_t> has;
            for (Slot k : keys) {
                const ColumnStats* c = refresh_detail::find(base->columns, k);
                const int64_t v = c == nullptr ? kNoStat : c->distinct_count;
                has.push_back(v != kNoStat);
                ndvs.push_back(v == kNoStat ? 0 : v);
            }
            const int64_t out_rows = estimate_group_by_cardinality(base->row_count(), ndvs.data(), has.data(), ndvs.size());
            base = refresh_detail::make_relation(false, out_rows, base->domain_row_count(),
                                                 refresh_detail::cap_ndvs(base->columns, out_rows));
        }

        // a LIMIT pushed into the scan caps what it emits
        if (r.limit != kNoStepValue && r.limit >= 0) {
            const int64_t capped = std::min(base->row_count(), r.limit);
            if (capped != base->row_count()) {
                Cols cols = refresh_detail::cap_ndvs(
                    refresh_detail::scale_total_bytes(base->columns, refresh_detail::ratio(capped, base->row_count())),
                    capped);
                base = refresh_detail::make_relation(base->row_count_is_metric(), capped, base->domain_row_count(),
                                                     std::move(cols));
            }
        }
        return base;
    }

    RelationStatsPtr scan_base(const StepRow& r) { return scan_base_statistics(in_.selectivity, *in_.store, r); }

    // ---- Filter ------------------------------------------------------------

    RelationStatsPtr filter_stats(NodeId nid, const StepRow& r, const std::vector<Child>& children) {
        RelationStatsPtr base = first_or_empty(children);
        if (r.condition == kNoExpr) return base;
        double selectivity = 1.0;
        bool changed = false;
        bool applied_any = false;
        Cols narrowed = base->columns;
        std::vector<ExprId> conjuncts;
        split_conjuncts(r.condition, conjuncts);
        for (ExprId conj : conjuncts) {
            if (folded_.count(conj) != 0) continue;   // already applied at a scan
            applied_any = true;
            const double s = selectivity_of(conj, *base);
            selectivity *= s;
            note_predicate(nid, false, std::string(), conj, s, *base);
            narrowed = narrow_filter_columns(narrowed, conj, changed);
        }
        if (!applied_any) return base;
        if (selectivity == 1.0 && !changed) return refresh_detail::as_estimate(base);
        int64_t new_rows = base->row_count();
        if (selectivity != 1.0) new_rows = estimate_after_filter(base->row_count(), selectivity);
        narrowed = refresh_detail::scale_total_bytes(narrowed, refresh_detail::ratio(new_rows, base->row_count()));
        narrowed = refresh_detail::scale_ndvs(narrowed, *base, selectivity);
        narrowed = refresh_detail::cap_ndvs(narrowed, new_rows);
        return refresh_detail::make_relation(false, new_rows, base->domain_row_count(), std::move(narrowed));
    }

    // ---- Join --------------------------------------------------------------

    void note_join(NodeId nid, const StepRow& r, int64_t left_rows, int64_t right_rows, int64_t out_rows,
                   int64_t key_count) {
        if (in_.telemetry == nullptr) return;
        in_.telemetry->joins.push_back(
            JoinNote{nid, r.has_join_type, r.join_type, left_rows, right_rows, out_rows, key_count});
    }

    // One KeyStats pair per equivalence class of the join's key pairs.
    std::vector<KeyPair> equi_key_classes(const std::vector<Slot>& left_keys, const std::vector<Slot>& right_keys,
                                          const RelationStats& left, const RelationStats& right) const {
        using namespace refresh_detail;
        const size_t n = std::min(left_keys.size(), right_keys.size());
        // union-find over (side, key); side 0 = left, 1 = right
        std::vector<std::pair<int, Slot>> nodes;
        std::vector<size_t> parent;
        auto index_of = [&](int side, Slot key) {
            for (size_t i = 0; i < nodes.size(); ++i) {
                if (nodes[i].first == side && nodes[i].second == key) return i;
            }
            nodes.emplace_back(side, key);
            parent.push_back(parent.size());
            return nodes.size() - 1;
        };
        auto find_root = [&](size_t i) {
            size_t root = i;
            while (parent[root] != root) root = parent[root];
            while (parent[i] != root) {
                const size_t next = parent[i];
                parent[i] = root;
                i = next;
            }
            return root;
        };
        for (size_t p = 0; p < n; ++p) {
            const size_t a = find_root(index_of(0, left_keys[p]));
            const size_t b = find_root(index_of(1, right_keys[p]));
            if (a != b) parent[b] = a;
        }
        // classes in order of first appearance
        std::vector<size_t> class_roots;
        std::vector<std::vector<size_t>> class_pairs;
        for (size_t p = 0; p < n; ++p) {
            const size_t root = find_root(index_of(0, left_keys[p]));
            size_t c = 0;
            while (c < class_roots.size() && class_roots[c] != root) ++c;
            if (c == class_roots.size()) {
                class_roots.push_back(root);
                class_pairs.emplace_back();
            }
            class_pairs[c].push_back(p);
        }

        std::vector<KeyPair> out;
        for (const auto& members : class_pairs) {
            std::vector<int64_t> known[2], live[2];
            std::vector<__int128> spans[2];
            std::vector<double> nulls[2];
            for (size_t p : members) {
                const ColumnStats* cols[2] = {find(left.columns, left_keys[p]), find(right.columns, right_keys[p])};
                for (int side = 0; side < 2; ++side) {
                    const ColumnStats* col = cols[side];
                    if (col == nullptr) continue;
                    const int64_t domain = col->domain_distinct_count();
                    if (domain != kNoStat) known[side].push_back(domain);
                    if (col->distinct_count != kNoStat) live[side].push_back(col->distinct_count);
                    __int128 span = 0;
                    if (value_range_span(*col, span)) spans[side].push_back(span);
                }
                for (int side = 0; side < 2; ++side) {
                    if (cols[side] != nullptr && cols[side]->has_null_fraction) nulls[side].push_back(cols[side]->null_fraction);
                }
            }
            const int64_t fallback = std::min(left.domain_row_count(), right.domain_row_count());
            KeyStats sides[2];
            for (int side = 0; side < 2; ++side) {
                const int64_t side_ndv = composite_key_ndv(known[side]);
                bool measured = side_ndv != kNoStat;
                int64_t tdom = side_ndv != kNoStat ? side_ndv : fallback;
                if (!spans[side].empty()) {
                    __int128 narrowest = spans[side][0];
                    for (__int128 s : spans[side]) narrowest = std::min(narrowest, s);
                    const int64_t capped = narrowest < static_cast<__int128>(tdom) ? static_cast<int64_t>(narrowest) : tdom;
                    if (capped != tdom) measured = false;
                    tdom = capped;
                }
                if (tdom < 1) {
                    tdom = 1;
                    measured = false;
                }
                const int64_t live_ndv = composite_key_ndv(live[side]);
                KeyStats& k = sides[side];
                k.ndv = tdom;
                k.has_ndv = true;
                k.live_ndv = live_ndv == kNoStat ? 0 : live_ndv;
                k.has_live_ndv = live_ndv != kNoStat;
                k.has_null_fraction = !nulls[side].empty();
                k.null_fraction = nulls[side].empty() ? 0.0 : list_max(nulls[side]);
                k.provenance = measured ? NDV_MEASURED : NDV_DOMAIN_STANDIN;
            }
            out.push_back(KeyPair{sides[0], sides[1]});
        }
        return out;
    }

    // Which sides of an equi-join have their keys narrowed to the match.
    static void narrowable_sides(const std::string& estimator_type, bool& left, bool& right) {
        if (estimator_type == "left") { left = false; right = true; }
        else if (estimator_type == "right") { left = true; right = false; }
        else if (estimator_type == "outer" || estimator_type == "anti") { left = false; right = false; }
        else if (estimator_type == "semi") { left = true; right = false; }
        else { left = true; right = true; }   // inner, and the default
    }

    static Cols intersect_join_keys(const Cols& merged, const RelationStats& left, const RelationStats& right,
                                    const std::vector<Slot>& left_keys, const std::vector<Slot>& right_keys,
                                    const std::string& estimator_type) {
        using namespace refresh_detail;
        bool narrow_left = false, narrow_right = false;
        narrowable_sides(estimator_type, narrow_left, narrow_right);
        if (!narrow_left && !narrow_right) return merged;
        Cols out = merged;
        const size_t n = std::min(left_keys.size(), right_keys.size());
        for (size_t p = 0; p < n; ++p) {
            const ColumnStats* l = find(left.columns, left_keys[p]);
            const ColumnStats* r = find(right.columns, right_keys[p]);
            if (l == nullptr || r == nullptr) continue;
            // the two ranges' intersection
            StatBound lower = !l->lower.present() ? r->lower : (!r->lower.present() ? l->lower : bound_max(l->lower, r->lower));
            StatBound upper = !l->upper.present() ? r->upper : (!r->upper.present() ? l->upper : bound_min(l->upper, r->upper));
            int64_t ndv = kNoStat;
            if (l->distinct_count != kNoStat && r->distinct_count != kNoStat) ndv = std::min(l->distinct_count, r->distinct_count);
            else if (l->distinct_count != kNoStat) ndv = l->distinct_count;
            else if (r->distinct_count != kNoStat) ndv = r->distinct_count;
            std::vector<Slot> keys;
            if (narrow_left && contains(out, left_keys[p])) keys.push_back(left_keys[p]);
            if (narrow_right && contains(out, right_keys[p])) keys.push_back(right_keys[p]);
            for (Slot k : keys) {
                auto c = copy_of(*find(out, k));
                c->lower = lower;
                c->upper = upper;
                c->distinct_count = ndv;
                put(out, k, std::move(c));
            }
        }
        return out;
    }

    static JoinType estimator_join_type(const std::string& name) {
        if (name == "inner") return JT_INNER;
        if (name == "left outer") return JT_LEFT_OUTER;
        if (name == "right outer") return JT_RIGHT_OUTER;
        if (name == "full outer") return JT_FULL_OUTER;
        if (name == "semi") return JT_SEMI;
        if (name == "anti") return JT_ANTI;
        if (name == "semi not-distinct") return JT_SEMI_NOT_DISTINCT;
        if (name == "anti not-distinct") return JT_ANTI_NOT_DISTINCT;
        if (name == "anti null-aware") return JT_ANTI_NULL_AWARE;
        throw std::invalid_argument("unknown join_type: " + name);
    }

    RelationStatsPtr cross_product(NodeId nid, const StepRow& r, const RelationStats& left, const RelationStats& right,
                                   bool metric_if_both) {
        using namespace refresh_detail;
        const int64_t out_rows = std::max<int64_t>(
            1, saturate(std::min<__int128>(static_cast<__int128>(left.row_count()) * right.row_count(), kInt64Max)));
        note_join(nid, r, left.row_count(), right.row_count(), out_rows, 0);
        Cols merged = drop_histograms(merge_columns(left, right));
        merged = scale_total_bytes_by_origin(merged, left, right, out_rows);
        const bool metric = metric_if_both && left.row_count_is_metric() && right.row_count_is_metric();
        return make_relation(metric, out_rows, kNoStat, cap_ndvs(merged, out_rows));
    }

    RelationStatsPtr join_stats(NodeId nid, const StepRow& r, const std::vector<Child>& children) {
        using namespace refresh_detail;
        RelationStatsPtr left_ptr, right_ptr;
        split_join_children(children, left_ptr, right_ptr);
        if (!left_ptr) left_ptr = empty_stats();
        if (!right_ptr) right_ptr = empty_stats();
        const RelationStats& left = *left_ptr;
        const RelationStats& right = *right_ptr;
        const std::string& join_type = r.join_type;

        if (!r.has_join_type || join_type == "cross join") return cross_product(nid, r, left, right, true);

        std::string estimator_type = "inner";
        std::string semi_anti;
        if (join_type == "left outer" || join_type == "left") estimator_type = "left";
        else if (join_type == "right outer" || join_type == "right") estimator_type = "right";
        else if (join_type == "full outer" || join_type == "outer") estimator_type = "outer";
        else if (join_type == "left semi") semi_anti = "semi";
        else if (join_type == "left anti") semi_anti = "anti";
        else if (join_type == "left anti null-aware") semi_anti = "anti null-aware";
        else if (join_type == "left semi not-distinct") semi_anti = "semi not-distinct";
        else if (join_type == "left anti not-distinct") semi_anti = "anti not-distinct";

        if (!semi_anti.empty()) {
            // semi/anti emit only left columns; the match fraction sets the count
            const std::vector<Slot> left_keys = step_keys(r.left_keys);
            const std::vector<Slot> right_keys = step_keys(r.right_keys);
            Cols cols = left.columns;
            int64_t out_rows;
            int64_t key_classes = 0;
            if (left_keys.empty() || right_keys.empty()) {
                out_rows = left.row_count();
            } else {
                const std::vector<KeyPair> equi = equi_key_classes(left_keys, right_keys, left, right);
                key_classes = static_cast<int64_t>(equi.size());
                out_rows = checked_join_estimate(left, right, estimator_join_type(semi_anti), equi);
                const std::string narrowing = semi_anti.rfind("semi", 0) == 0 ? "semi" : "anti";
                cols = intersect_join_keys(cols, left, right, left_keys, right_keys, narrowing);
            }
            note_join(nid, r, left.row_count(), right.row_count(), out_rows, key_classes);
            return make_relation(false, out_rows, left.domain_row_count(), cap_ndvs(cols, out_rows));
        }

        if (join_type == "asof") {
            // exactly one row per left row, matched or not
            const int64_t out_rows = std::max<int64_t>(0, left.row_count());
            note_join(nid, r, left.row_count(), right.row_count(), out_rows, 0);
            Cols merged = drop_histograms(merge_columns(left, right));
            merged = scale_total_bytes_by_origin(merged, left, right, out_rows);
            return make_relation(left.row_count_is_metric(), out_rows, kNoStat, cap_ndvs(merged, out_rows));
        }

        const std::vector<Slot> left_keys = step_keys(r.left_keys);
        const std::vector<Slot> right_keys = step_keys(r.right_keys);
        if (left_keys.empty() || right_keys.empty()) {
            // no equi key: the cross product bounds it - a bound, an estimate
            return cross_product(nid, r, left, right, false);
        }

        std::vector<KeyPair> equi = equi_key_classes(left_keys, right_keys, left, right);
        const int64_t key_classes = static_cast<int64_t>(equi.size());
        KeyPair collapsed;
        if (apply_occupancy_bound(equi.data(), equi.size(), left.domain_row_count(), right.domain_row_count(),
                                  &collapsed)) {
            equi.assign(1, collapsed);
        }
        JoinType cardinality_type = JT_INNER;
        if (estimator_type == "left") cardinality_type = JT_LEFT_OUTER;
        else if (estimator_type == "right") cardinality_type = JT_RIGHT_OUTER;
        else if (estimator_type == "outer") cardinality_type = JT_FULL_OUTER;
        const int64_t out_rows = checked_join_estimate(left, right, cardinality_type, equi);
        note_join(nid, r, left.row_count(), right.row_count(), out_rows, key_classes);
        Cols merged = drop_histograms(merge_columns(left, right));
        merged = intersect_join_keys(merged, left, right, left_keys, right_keys, estimator_type);
        merged = scale_total_bytes_by_origin(merged, left, right, out_rows);
        // a later join keys off one of the base relations under this one: keep
        // the larger domain
        return make_relation(false, out_rows, std::max(left.domain_row_count(), right.domain_row_count()),
                             cap_ndvs(merged, out_rows));
    }

    static int64_t checked_join_estimate(const RelationStats& left, const RelationStats& right, JoinType type,
                                         const std::vector<KeyPair>& equi) {
        if (left.row_count() < 0 || right.row_count() < 0) {
            throw std::invalid_argument("row counts must be non-negative");
        }
        return estimate_join_cardinality(left.row_count(), right.row_count(), type, equi.data(), equi.size(), 1.0);
    }

    // ---- Aggregate / Limit / Distinct / Project / Union / set operations ---

    RelationStatsPtr aggregate_stats(const StepRow& r, const std::vector<Child>& children) {
        using namespace refresh_detail;
        RelationStatsPtr base = first_or_empty(children);
        const std::vector<Slot> keys = step_keys(r.groups);
        if (keys.empty()) return empty_stats(1, true);
        std::vector<int64_t> ndvs;
        std::vector<uint8_t> has;
        for (Slot k : keys) {
            const ColumnStats* c = find(base->columns, k);
            const int64_t v = c == nullptr ? kNoStat : c->distinct_count;
            has.push_back(v != kNoStat);
            ndvs.push_back(v == kNoStat ? 0 : v);
        }
        const int64_t out_rows = estimate_group_by_cardinality(base->row_count(), ndvs.data(), has.data(), ndvs.size());
        // one key: its NDV IS the output count; several: capped by it below
        const int64_t single_key_ndv = keys.size() == 1 ? out_rows : kNoStat;
        Cols out;
        for (Slot k : keys) {
            const ColumnStats* col = find(base->columns, k);
            if (col == nullptr) continue;
            auto c = copy_of(*col);
            c->histogram.reset();
            c->distinct_count = single_key_ndv != kNoStat ? single_key_ndv : col->distinct_count;
            put(out, k, std::move(c));
        }
        out = scale_total_bytes(out, ratio(out_rows, base->row_count()));
        return make_relation(false, out_rows, kNoStat, cap_ndvs(out, out_rows));
    }

    RelationStatsPtr limit_stats(const StepRow& r, const std::vector<Child>& children) {
        using namespace refresh_detail;
        RelationStatsPtr base = first_or_empty(children);
        const bool has_limit = r.limit != kNoStepValue;
        const bool has_offset = r.kind == sk().limit && r.offset != kNoStepValue;   // HeapSort has none
        if (!has_limit && !has_offset) return base;
        const __int128 available = static_cast<__int128>(base->row_count()) - (has_offset ? r.offset : 0);
        const __int128 capped = has_limit ? std::min<__int128>(r.limit, available) : available;
        const int64_t new_rows = saturate(std::max<__int128>(0, capped));
        Cols cols = cap_ndvs(scale_total_bytes(base->columns, ratio(new_rows, base->row_count())), new_rows);
        // min() over a metric is exact arithmetic: the input's provenance stands
        return make_relation(base->row_count_is_metric(), new_rows, kNoStat, std::move(cols));
    }

    RelationStatsPtr distinct_stats(NodeId nid, const std::vector<Child>& children) {
        using namespace refresh_detail;
        RelationStatsPtr base = first_or_empty(children);
        if (base->columns.empty()) return as_estimate(base);
        // the NDV product over the columns the one child actually outputs
        std::vector<Slot> scoped;
        const auto& in_edges = in_.graph->node(nid).in;
        if (in_edges.size() == 1) {
            for (ExprId c : row(in_edges[0].other).columns) {
                const Slot k = expr_key(c);
                if (k != kNoSlot) scoped.push_back(k);
            }
        }
        std::vector<int64_t> ndvs;
        std::vector<uint8_t> has;
        auto take = [&](const ColumnStats& c) {
            has.push_back(c.distinct_count != kNoStat);
            ndvs.push_back(c.distinct_count == kNoStat ? 0 : c.distinct_count);
        };
        bool relevant = false;
        if (!scoped.empty()) {
            for (const auto& e : base->columns) {
                if (std::find(scoped.begin(), scoped.end(), e.first) != scoped.end()) {
                    relevant = true;
                    take(*e.second);
                }
            }
        }
        if (!relevant) {
            ndvs.clear();
            has.clear();
            for (const auto& e : base->columns) take(*e.second);
        }
        const int64_t out_rows = estimate_group_by_cardinality(base->row_count(), ndvs.data(), has.data(), ndvs.size());
        Cols cols = scale_total_bytes(drop_histograms(base->columns), ratio(out_rows, base->row_count()));
        return make_relation(false, out_rows, kNoStat, cap_ndvs(cols, out_rows));
    }

    // A CAST that cannot map two distinct values onto one: integer -> VARCHAR,
    // or a value-preserving numeric widening; never TRY_, never with a FORMAT.
    bool cast_preserves_distinctness(ExprId cast) const {
        const ExprRow& c = expr(cast);
        const std::string& name = c.value;
        if (name.size() >= 4 && std::toupper(static_cast<unsigned char>(name[0])) == 'T' &&
            std::toupper(static_cast<unsigned char>(name[1])) == 'R' &&
            std::toupper(static_cast<unsigned char>(name[2])) == 'Y' && name[3] == '_') {
            return false;
        }
        if (c.format != kNoExpr) return false;
        if (c.left == kNoExpr) return false;
        DrakenType source, target;
        if (!physical_of(c.left, source) || !physical_of(cast, target)) return false;
        const bool source_integer = source == DRAKEN_INT8 || source == DRAKEN_INT16 || source == DRAKEN_INT32 ||
                                    source == DRAKEN_INT64 || source == DRAKEN_UINT8 || source == DRAKEN_UINT16 ||
                                    source == DRAKEN_UINT32 || source == DRAKEN_UINT64;
        if (source_integer && target == DRAKEN_VARCHAR) return true;
        auto in = [&](std::initializer_list<DrakenType> set) {
            return std::find(set.begin(), set.end(), target) != set.end();
        };
        switch (source) {
            case DRAKEN_INT8: return in({DRAKEN_INT8, DRAKEN_INT16, DRAKEN_INT32, DRAKEN_INT64, DRAKEN_FLOAT32, DRAKEN_FLOAT64});
            case DRAKEN_INT16: return in({DRAKEN_INT16, DRAKEN_INT32, DRAKEN_INT64, DRAKEN_FLOAT32, DRAKEN_FLOAT64});
            case DRAKEN_INT32: return in({DRAKEN_INT32, DRAKEN_INT64, DRAKEN_FLOAT64});   // not FLOAT32: 2^31 > 2^24
            case DRAKEN_INT64: return in({DRAKEN_INT64});
            case DRAKEN_UINT8: return in({DRAKEN_UINT8, DRAKEN_UINT16, DRAKEN_UINT32, DRAKEN_UINT64, DRAKEN_INT16,
                                          DRAKEN_INT32, DRAKEN_INT64, DRAKEN_FLOAT32, DRAKEN_FLOAT64});
            case DRAKEN_UINT16: return in({DRAKEN_UINT16, DRAKEN_UINT32, DRAKEN_UINT64, DRAKEN_INT32, DRAKEN_INT64,
                                           DRAKEN_FLOAT32, DRAKEN_FLOAT64});
            case DRAKEN_UINT32: return in({DRAKEN_UINT32, DRAKEN_UINT64, DRAKEN_INT64, DRAKEN_FLOAT64});
            case DRAKEN_UINT64: return in({DRAKEN_UINT64});
            case DRAKEN_FLOAT32: return in({DRAKEN_FLOAT32, DRAKEN_FLOAT64});
            case DRAKEN_FLOAT64: return in({DRAKEN_FLOAT64});
            default: return false;
        }
    }

    bool physical_of(ExprId id, DrakenType& out) const {
        const uint32_t slot = expr(id).column_slot;
        if (slot == kNoColumnSlot) return false;
        const ColumnTypeId type_id = columns().row(slot).type_id;
        if (type_id == kNoColumnType) return false;
        out = in_.selectivity.types->entry(type_id).physical;
        return true;
    }

    RelationStatsPtr project_stats(const StepRow& r, const std::vector<Child>& children) {
        using namespace refresh_detail;
        RelationStatsPtr base = first_or_empty(children);
        if (r.columns.empty() || base->columns.empty()) return base;
        Cols derived;
        for (ExprId column : r.columns) {
            if (column == kNoExpr || expr(column).kind != nk().cast) continue;
            const Slot key = expr_key(column);
            // an identity already carrying statistics is the child's own column
            if (key == kNoSlot || contains(base->columns, key) || contains(derived, key)) continue;
            const Slot source_key = expr_key(expr(column).left);
            if (source_key == kNoSlot) continue;
            const ColumnStats* source = find(base->columns, source_key);
            if (source == nullptr || source->distinct_count == kNoStat) continue;
            if (!cast_preserves_distinctness(column)) continue;
            auto c = std::make_shared<ColumnStats>();
            c->distinct_count = source->distinct_count;
            c->has_null_fraction = source->has_null_fraction;
            c->null_fraction = source->null_fraction;
            derived.emplace_back(key, std::move(c));
        }
        if (derived.empty()) return base;
        Cols merged = base->columns;
        for (auto& e : derived) put(merged, e.first, std::move(e.second));
        return make_relation(base->row_count_is_metric(), base->row_count(), base->base_row_count, std::move(merged));
    }

    RelationStatsPtr union_stats(const std::vector<Child>& children) {
        using namespace refresh_detail;
        int64_t rows = 0;
        bool all_metric = !children.empty();
        Cols cols;
        for (const Child& child : children) {
            if (!child.stats) {
                all_metric = false;
                continue;
            }
            const RelationStats& cs = *child.stats;
            if (!cs.row_count_is_metric()) all_metric = false;
            rows = saturate(std::min<__int128>(kInt64Max, static_cast<__int128>(rows) + cs.row_count()));
            for (const auto& e : cs.columns) {
                const ColumnStats* existing = find(cols, e.first);
                if (existing == nullptr) {
                    auto c = copy_of(*e.second);
                    c->histogram.reset();
                    put(cols, e.first, std::move(c));
                    continue;
                }
                const ColumnStats& v = *e.second;
                auto c = copy_of(*existing);
                // widen the range (lower = min, upper = max), sum NDV and bytes
                c->lower = !existing->lower.present() ? v.lower : (!v.lower.present() ? existing->lower : bound_min(existing->lower, v.lower));
                c->upper = !existing->upper.present() ? v.upper : (!v.upper.present() ? existing->upper : bound_max(existing->upper, v.upper));
                c->distinct_count = existing->distinct_count != kNoStat && v.distinct_count != kNoStat
                                        ? saturate(static_cast<__int128>(existing->distinct_count) + v.distinct_count)
                                        : kNoStat;
                c->histogram.reset();
                c->has_total_bytes = existing->has_total_bytes && v.has_total_bytes;
                c->total_bytes = c->has_total_bytes ? existing->total_bytes + v.total_bytes : 0;
                put(cols, e.first, std::move(c));
            }
        }
        return make_relation(all_metric, rows, kNoStat, cap_ndvs(cols, rows));
    }

    // INTERSECT / EXCEPT: bounded by the left input - an estimate.
    RelationStatsPtr set_op_stats(const std::vector<Child>& children) {
        RelationStatsPtr left, right;
        if (children.size() >= 2) split_join_children(children, left, right);
        if (!left) return refresh_detail::as_estimate(first_or_empty(children));
        return refresh_detail::as_estimate(left);
    }
};

inline void refresh_statistics(const RefreshInputs& in) {
    StatisticsRefresh(in).run();
}

// One node's statistics from its inputs' (see StatisticsRefresh::run_one).
inline void compute_node_statistics(const RefreshInputs& in, NodeId nid) {
    StatisticsRefresh(in).run_one(nid);
}

// A node's total dense bytes over the columns that know theirs; false when none
// does.
inline bool node_total_bytes(const RelationStats& stats, __int128& out) {
    out = 0;
    bool known = false;
    for (const auto& e : stats.columns) {
        if (!e.second->has_total_bytes) continue;
        out += e.second->total_bytes;
        known = true;
    }
    return known;
}

}  // namespace opteryx::planner
