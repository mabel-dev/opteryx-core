#pragma once
// src/cpp/engine/stats_coverage.hpp — does EVERY row of a unit (a row group)
// satisfy the scan's predicate, and if so, what do its statistics say about the
// aggregate over it? (docs/MANIFEST_SUM_STATISTIC_DESIGN.md §6-7, P3.)
//
// Every other statistics test in the engine proves EXCLUSION ("no row can
// match": pruning, zone maps, runtime bounds, Top-N). This one proves
// INCLUSION, and a unit it calls COVERED is answered from its statistics at
// plan time and never read: the planner folds its partial into the aggregate's
// seed and drops it from the scan's work list. So every rule here is the
// strict one:
//   - a term is covered only when the unit's null count is KNOWN to be 0 - a
//     NULL satisfies no value predicate (IS NULL aside);
//   - bounds must be exact values (the terms' columns are the ordinal == value
//     domains: coverage_terms.py refuses every other one);
//   - unknown is never covered: a missing bound, a missing null count or a
//     missing statistic an aggregate needs makes the unit BOUNDARY (read it).
// A conjunction is DISJOINT when any term is, COVERED when every term is, and
// BOUNDARY otherwise.
//
// Format-neutral: the parquet and skene adapters fill a CoverageColumn per
// named column from their own footer statistics.

#include <algorithm>
#include <cstdint>
#include <map>
#include <string>
#include <utility>
#include <vector>

namespace opteryx::engine {

// Op codes — the SAME numbering as opteryx/connectors/parquet_io/coverage_terms.py.
enum CoverageOp : uint8_t {
    kCoverEq = 0, kCoverNotEq = 1, kCoverLt = 2, kCoverLtEq = 3, kCoverGt = 4, kCoverGtEq = 5,
    kCoverIn = 6, kCoverIsNull = 7, kCoverIsNotNull = 8,
};

struct CoverageTerm {
    size_t column = 0;               // index into the unit's CoverageColumn list
    uint8_t op = kCoverEq;
    std::vector<int64_t> ordinals;   // one (IN: sorted, distinct; IS [NOT] NULL: none)
};

// One column of one unit, as its footer records it. Unknown is never zero.
struct CoverageColumn {
    bool has_bounds = false;         // min/max over the NON-NULL values, exact
    int64_t min = 0, max = 0;        // ordinals (== values for the admitted domains)
    int64_t null_count = -1;         // -1: unknown
    bool has_sum = false;            // the exact integer sum of the non-null values
    __int128 sum = 0;
};

enum class Coverage : uint8_t { kDisjoint, kCovered, kBoundary };

namespace coverage_detail {

inline Coverage term(const CoverageTerm& t, const CoverageColumn& c, int64_t rows) {
    const bool nulls_known = c.null_count >= 0;
    const bool no_nulls = c.null_count == 0;
    const bool all_null = nulls_known && c.null_count == rows;
    if (t.op == kCoverIsNull) {
        if (!nulls_known) return Coverage::kBoundary;
        if (c.null_count == rows) return Coverage::kCovered;
        return c.null_count == 0 ? Coverage::kDisjoint : Coverage::kBoundary;
    }
    if (t.op == kCoverIsNotNull) {
        if (!nulls_known) return Coverage::kBoundary;
        if (c.null_count == 0) return Coverage::kCovered;
        return all_null ? Coverage::kDisjoint : Coverage::kBoundary;
    }
    // Every other op is a value predicate: a NULL row never satisfies it.
    if (all_null) return Coverage::kDisjoint;
    if (!c.has_bounds) return Coverage::kBoundary;
    const int64_t lo = c.min, hi = c.max;
    bool disjoint = false, inside = false;
    switch (t.op) {
        case kCoverEq: {
            const int64_t v = t.ordinals.at(0);
            disjoint = v < lo || v > hi;
            inside = lo == v && hi == v;
            break;
        }
        case kCoverNotEq: {
            const int64_t v = t.ordinals.at(0);
            disjoint = lo == v && hi == v;
            inside = v < lo || v > hi;
            break;
        }
        case kCoverLt:   disjoint = lo >= t.ordinals.at(0); inside = hi < t.ordinals.at(0); break;
        case kCoverLtEq: disjoint = lo > t.ordinals.at(0);  inside = hi <= t.ordinals.at(0); break;
        case kCoverGt:   disjoint = hi <= t.ordinals.at(0); inside = lo > t.ordinals.at(0); break;
        case kCoverGtEq: disjoint = hi < t.ordinals.at(0);  inside = lo >= t.ordinals.at(0); break;
        case kCoverIn: {
            const std::vector<int64_t>& vs = t.ordinals;
            // first member >= lo; disjoint when none lies in [lo, hi]
            auto it = std::lower_bound(vs.begin(), vs.end(), lo);
            disjoint = it == vs.end() || *it > hi;
            inside = lo == hi && it != vs.end() && *it == lo;
            break;
        }
        default:
            return Coverage::kBoundary;
    }
    if (disjoint) return Coverage::kDisjoint;
    if (inside && no_nulls) return Coverage::kCovered;
    return Coverage::kBoundary;
}

}  // namespace coverage_detail

// The unit's verdict over the whole conjunction. An empty conjunction is
// covered (no predicate: every row qualifies).
inline Coverage classify_unit(const std::vector<CoverageTerm>& terms,
                              const std::vector<CoverageColumn>& columns, int64_t rows) {
    bool all_covered = true;
    for (const CoverageTerm& t : terms) {
        const Coverage c = coverage_detail::term(t, columns.at(t.column), rows);
        if (c == Coverage::kDisjoint) return Coverage::kDisjoint;
        if (c != Coverage::kCovered) all_covered = false;
    }
    return all_covered ? Coverage::kCovered : Coverage::kBoundary;
}

// ---- the aggregate's partial ----------------------------------------------

// What each aggregate needs from a covered unit. AVG needs a SUM partial (its
// sum and valid count); the kinds are the facts, not the SQL function names.
enum CoverageNeed : uint8_t {
    kNeedRows = 0,    // COUNT(*)
    kNeedValid = 1,   // COUNT(col)
    kNeedSum = 2,     // SUM/AVG(col): sum + valid
    kNeedMin = 3,     // MIN(col): min + valid
    kNeedMax = 4,     // MAX(col): max + valid
};

struct CoverageAgg {
    uint8_t need = kNeedRows;
    size_t column = 0;               // index into the unit's CoverageColumn list (unused for rows)
};

// One aggregate's partial over the covered units folded so far.
struct CoveragePartial {
    int64_t rows = 0;
    int64_t valid = 0;
    __int128 sum = 0;
    bool any_extreme = false;
    int64_t min = 0, max = 0;
};

// What the planner asks of one scan: the named columns the terms and the
// aggregates refer to (resolved per file by each format's adapter), the terms,
// and the aggregates. Indices are into `names`.
struct CoverageSpec {
    std::vector<std::string> names;
    std::vector<CoverageTerm> terms;
    std::vector<CoverageAgg> aggs;
    // GROUP BY (P4): the key columns, indices into `names`. Empty: ungrouped.
    std::vector<size_t> keys;
};

// Can the covered unit answer every aggregate? False when any needed statistic
// is unknown - the unit is then read like a boundary one.
inline bool unit_answers(const std::vector<CoverageAgg>& aggs, const std::vector<CoverageColumn>& columns) {
    for (const CoverageAgg& a : aggs) {
        if (a.need == kNeedRows) continue;
        const CoverageColumn& c = columns.at(a.column);
        if (c.null_count < 0) return false;
        if (a.need == kNeedSum && !c.has_sum) return false;
        // a unit with non-null values must carry its bounds for MIN/MAX
        if ((a.need == kNeedMin || a.need == kNeedMax) && !c.has_bounds) return false;
    }
    return true;
}

// Fold a covered unit into the partials. False (partials untouched) when a
// running sum would leave __int128.
inline bool fold_unit(const std::vector<CoverageAgg>& aggs, const std::vector<CoverageColumn>& columns,
                      int64_t rows, std::vector<CoveragePartial>& partials) {
    std::vector<CoveragePartial> next = partials;
    for (size_t k = 0; k < aggs.size(); ++k) {
        const CoverageAgg& a = aggs[k];
        CoveragePartial& p = next[k];
        p.rows += rows;
        if (a.need == kNeedRows) continue;
        const CoverageColumn& c = columns.at(a.column);
        const int64_t valid = rows - c.null_count;
        p.valid += valid;
        if (a.need == kNeedSum && __builtin_add_overflow(p.sum, c.sum, &p.sum)) return false;
        if ((a.need == kNeedMin || a.need == kNeedMax) && valid > 0) {
            if (!p.any_extreme) {
                p.min = c.min;
                p.max = c.max;
                p.any_extreme = true;
            } else {
                p.min = std::min(p.min, c.min);
                p.max = std::max(p.max, c.max);
            }
        }
    }
    partials.swap(next);
    return true;
}

// ---- GROUP BY (P4) ----------------------------------------------------------

// One group key column's value in a unit: NULL, or a value (ordinal == value for
// the key types coverage admits - coverage_terms.py / the compiler's gate).
using CoverageKeyValue = std::pair<bool, int64_t>;   // (is_null, value)
using CoverageKey = std::vector<CoverageKeyValue>;

// The unit's single group - every key column holds ONE value throughout it (min
// == max over its non-null values and no nulls, or nothing but nulls) - or false
// when any key column varies (or its statistics cannot say): the unit spans
// several groups and must be read.
inline bool unit_group(const std::vector<size_t>& keys, const std::vector<CoverageColumn>& columns,
                       int64_t rows, CoverageKey& out) {
    out.clear();
    out.reserve(keys.size());
    for (size_t k : keys) {
        const CoverageColumn& c = columns.at(k);
        if (c.null_count < 0) return false;
        if (c.null_count == rows) {
            out.emplace_back(true, 0);
            continue;
        }
        if (c.null_count != 0 || !c.has_bounds || c.min != c.max) return false;
        out.emplace_back(false, c.min);
    }
    return true;
}

// The covered, single-group units folded per group. Ordered so the seed a plan
// produces is deterministic.
using CoverageGroups = std::map<CoverageKey, std::vector<CoveragePartial>>;

// Fold a covered unit into its group's partials: false (nothing changed) when the
// unit is not single-group, or a sum would leave __int128.
inline bool fold_grouped_unit(const CoverageSpec& spec, const std::vector<CoverageColumn>& columns,
                              int64_t rows, CoverageGroups& groups) {
    CoverageKey key;
    if (!unit_group(spec.keys, columns, rows, key)) return false;
    auto it = groups.find(key);
    if (it == groups.end()) {
        std::vector<CoveragePartial> fresh(spec.aggs.size());
        if (!fold_unit(spec.aggs, columns, rows, fresh)) return false;
        groups.emplace(std::move(key), std::move(fresh));
        return true;
    }
    return fold_unit(spec.aggs, columns, rows, it->second);
}

// ---- one unit, either shape ------------------------------------------------

// What a plan folds covered units into: the ungrouped partial per aggregate, or
// (GROUP BY) the partials per group.
struct CoverageAccumulator {
    std::vector<CoveragePartial> partials;   // ungrouped: one per aggregate
    CoverageGroups groups;                   // grouped
};

// One unit's decision, the one every format adapter calls: 0 = disjoint (drop
// it), 1 = covered and folded into `acc` (drop it: the seed answers it), 2 =
// read it (boundary, a statistic an aggregate needs is unknown, a GROUP BY key
// varies within it, or a sum would overflow).
inline int cover_unit(const CoverageSpec& spec, const std::vector<CoverageColumn>& columns,
                      int64_t rows, CoverageAccumulator& acc) {
    const Coverage verdict = classify_unit(spec.terms, columns, rows);
    if (verdict == Coverage::kDisjoint) return 0;
    if (verdict != Coverage::kCovered) return 2;
    if (!unit_answers(spec.aggs, columns)) return 2;
    if (spec.keys.empty()) return fold_unit(spec.aggs, columns, rows, acc.partials) ? 1 : 2;
    return fold_grouped_unit(spec, columns, rows, acc.groups) ? 1 : 2;
}

}  // namespace opteryx::engine
