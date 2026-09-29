#pragma once
// src/cpp/engine/stats_coverage_request.hpp — one scan's statistics-coverage
// request (P3/P4, docs/MANIFEST_SUM_STATISTIC_DESIGN.md §7) as the Cython glue
// builds it and reads it back: the spec (named columns, exact terms, aggregates,
// GROUP BY keys) and the accumulator the format adapter folds covered units into.
//
// A plain holder, shared by both formats' glue (pool_reader.pyx for parquet,
// _operators.pyx for skene) so the build/read-back is written once, and so
// Cython never spells the int128 sums: everything crosses as int64 words.

#include <cstdint>
#include <string>
#include <utility>
#include <vector>

#include "stats_coverage.hpp"

namespace opteryx::engine {

struct CoverageRequest {
    CoverageSpec spec;
    CoverageAccumulator acc;
    // GROUP BY: acc.groups flattened once, after the fold, for indexed read-back.
    std::vector<const std::pair<const CoverageKey, std::vector<CoveragePartial>>*> flat;
    int64_t covered = 0, disjoint = 0;
    std::string err;

    size_t name(const std::string& n) {
        for (size_t k = 0; k < spec.names.size(); ++k)
            if (spec.names[k] == n) return k;
        spec.names.push_back(n);
        return spec.names.size() - 1;
    }
    void term(size_t column, int op, const std::vector<int64_t>& ordinals) {
        CoverageTerm t;
        t.column = column;
        t.op = static_cast<uint8_t>(op);
        t.ordinals = ordinals;
        spec.terms.push_back(std::move(t));
    }
    void agg(int need, size_t column) {
        CoverageAgg a;
        a.need = static_cast<uint8_t>(need);
        a.column = column;
        spec.aggs.push_back(a);
        acc.partials.emplace_back();
    }
    void key(size_t column) { spec.keys.push_back(column); }

    // ungrouped read-back
    const CoveragePartial& partial(size_t k) const { return acc.partials.at(k); }

    // grouped read-back
    size_t group_count() {
        if (flat.size() != acc.groups.size()) {
            flat.clear();
            for (const auto& entry : acc.groups) flat.push_back(&entry);
        }
        return flat.size();
    }
    bool group_key_null(size_t g, size_t k) const { return flat.at(g)->first.at(k).first; }
    int64_t group_key_value(size_t g, size_t k) const { return flat.at(g)->first.at(k).second; }
    const CoveragePartial& group_partial(size_t g, size_t k) const { return flat.at(g)->second.at(k); }
};

// int64 words of a partial, for the glue
inline int64_t coverage_sum_hi(const CoveragePartial& p) { return static_cast<int64_t>(p.sum >> 64); }
inline uint64_t coverage_sum_lo(const CoveragePartial& p) { return static_cast<uint64_t>(p.sum); }

}  // namespace opteryx::engine
