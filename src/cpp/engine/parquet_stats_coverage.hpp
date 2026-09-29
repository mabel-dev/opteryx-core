#pragma once
// src/cpp/engine/parquet_stats_coverage.hpp — the parquet adapter for
// stats_coverage.hpp: one row group's footer statistics as CoverageColumns,
// classified at PLAN time (open_native_scan_plan), a covered row group folded
// into the aggregate's seed instead of being scanned (P3,
// docs/MANIFEST_SUM_STATISTIC_DESIGN.md §7).
//
// Trust: a row group's bounds are used only in a file rugo wrote
// (ColumnStats::writer_is_rugo). Parquet's legacy min/max fields were written
// signed-ordered for unsigned columns by older writers - harmless for pruning's
// rare wrong keep, a wrong ANSWER for coverage. Null counts and row counts are
// taken from any writer (exact when present; absent is -1, never 0), and the sum
// is rugo's own statistic, already trust-gated by the reader.

#include <cstdint>
#include <string>
#include <vector>

#include "metadata.hpp"                     // FileStats, RowGroupStats, ColumnStats
#include "parquet_stat_ordinal.hpp"   // stat_bytes_to_ordinal
#include "stats_coverage.hpp"

namespace opteryx::engine {

inline void parquet_coverage_column(const RowGroupStats& rg, const std::string& name, CoverageColumn& out) {
    out = CoverageColumn{};
    for (const ColumnStats& cs : rg.columns) {
        if (cs.name != name) continue;
        out.null_count = cs.null_count >= 0 ? cs.null_count : -1;
        out.has_sum = cs.has_sum;
        out.sum = cs.has_sum ? cs.sum : 0;
        if (cs.writer_is_rugo && cs.has_min && cs.has_max) {
            int64_t lo = 0, hi = 0;
            if (stat_bytes_to_ordinal(cs.physical_type, cs.logical_type, cs.min, &lo)
                    && stat_bytes_to_ordinal(cs.physical_type, cs.logical_type, cs.max, &hi)) {
                out.has_bounds = true;
                out.min = lo;
                out.max = hi;
            }
        }
        return;
    }
}

// Row group `rg_index` of `footer`: 0 = disjoint (nothing matches - drop it),
// 1 = covered and folded into `partials` (drop it: the seed answers it),
// 2 = boundary (scan it).
inline int parquet_cover_row_group(const FileStats& footer, size_t rg_index, const CoverageSpec& spec,
                                   CoverageAccumulator& acc) {
    if (rg_index >= footer.row_groups.size()) return 2;
    const RowGroupStats& rg = footer.row_groups[rg_index];
    if (rg.num_rows < 0) return 2;
    std::vector<CoverageColumn> columns(spec.names.size());
    for (size_t k = 0; k < spec.names.size(); ++k) parquet_coverage_column(rg, spec.names[k], columns[k]);
    return cover_unit(spec, columns, rg.num_rows, acc);
}

}  // namespace opteryx::engine
