// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// See the License at http://www.apache.org/licenses/LICENSE-2.0
// Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

// src/cpp/planner/skene_stats.hpp — a .skene file's footer statistics,
// aggregated from its row groups to FILE level, into a NativeManifest row
// (native plan graph Q8; architect ruling 2026-09-27 (5d)).
//
// The native port of opteryx/connectors/skene_io's
// skene_aggregate_row_group_statistics; see its docstring for why each rule is
// what it is. Per column, three INDEPENDENT aggregations, each with its own
// "unknown":
//   - bounds (ordinal keys): the UNION, only when EVERY row group bounds it;
//   - null count: the SUM, only when every row group tracks it;
//   - NDV: SUM over provably disjoint ranges, MAX otherwise (never exact after a
//     MAX), unknown if any row group lacks it; and the largest EXACT per-row-group
//     count as a hard floor.
// Plus the file's own KMV sketch per column, and its hash family. Statistics
// slots are depth first over the columns, ARRAY children included; a child
// maps to no column (an element's bounds are not the array's), and a top-level
// column maps by NAME, never by footer position.

#pragma once

#include <algorithm>
#include <cstdint>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <vector>

#include "skene/format.h"
#include "skene/reader.h"
#include "planner/native_manifest.hpp"

namespace opteryx::planner {

namespace skene_detail {

inline void walk(const skene::ColumnSchema& column, bool top_level,
                 const std::unordered_map<std::string, size_t>& position_by_name, std::vector<int64_t>& out) {
    if (top_level) {
        auto found = position_by_name.find(column.name);
        out.push_back(found == position_by_name.end() ? -1 : static_cast<int64_t>(found->second));
    } else {
        out.push_back(-1);
    }
    for (const skene::ColumnSchema& child : column.children) walk(child, false, position_by_name, out);
}

}  // namespace skene_detail

// The statistics slot -> manifest column position map (-1: no column).
inline std::vector<int64_t> skene_slot_positions(const skene::FileMetadata& meta,
                                                 const std::unordered_map<std::string, size_t>& position_by_name) {
    std::vector<int64_t> positions;
    for (const skene::ColumnSchema& column : meta.columns) skene_detail::walk(column, true, position_by_name, positions);
    return positions;
}

// What a footer gave a file: whether any column got bounds, and any a null count.
struct SkeneApplied {
    bool any_bounds = false;
    bool any_nulls = false;
};

// File `row`'s record count, row group count and per-column statistics from the
// footer `meta`. `physical` is each manifest column's DrakenType; `positions`
// each statistics slot's column (skene_slot_positions).
inline SkeneApplied apply_skene_footer(NativeManifest& m, size_t row, const skene::FileMetadata& meta,
                                       const std::vector<DrakenType>& physical,
                                       const std::vector<int64_t>& positions) {
    SkeneApplied applied;
    ManifestFile& file = m.file(row);
    file.record_count = static_cast<int64_t>(meta.row_count);
    file.row_group_count = static_cast<int64_t>(meta.row_groups.size());

    // One file, one hash family: every sketch a footer carries came from one writer.
    int32_t family = 0;
    for (const skene::ColumnSketch& sketch : meta.sketches) {
        if (!sketch.present()) continue;
        if (family != 0 && family != sketch.hash_family) {
            throw std::invalid_argument("a skene footer reported sketches in more than one hash family");
        }
        family = sketch.hash_family;
    }

    bool any_sketch = false;
    for (size_t slot = 0; slot < positions.size(); ++slot) {
        if (positions[slot] < 0) continue;
        const size_t position = static_cast<size_t>(positions[slot]);
        int64_t low = 0, high = 0;
        bool have_bounds = false, bounded = true;
        uint64_t null_total = 0;
        bool nulls_known = true;
        bool ndv_have = false, ndv_exact = true, ndv_known = true;
        int64_t ndv_total = 0;
        bool ndv_range = false;
        int64_t ndv_lo = 0, ndv_hi = 0;
        int64_t ndv_floor = 0;

        for (const skene::RowGroupSummary& group : meta.row_groups) {
            if (slot >= group.column_statistics.size() || !group.column_statistics[slot].present) {
                bounded = false;
                nulls_known = false;
                ndv_known = false;
                break;
            }
            const skene::ColumnStatistics& s = group.column_statistics[slot].statistics;
            const bool has_bounds = (s.flags & (skene::kStatMin | skene::kStatMax)) == (skene::kStatMin | skene::kStatMax);
            if (!has_bounds) {
                bounded = false;
            } else if (bounded) {
                if (!have_bounds) {
                    low = s.min_ordinal;
                    high = s.max_ordinal;
                    have_bounds = true;
                } else {
                    low = std::min(low, s.min_ordinal);
                    high = std::max(high, s.max_ordinal);
                }
            }
            if (nulls_known) {
                if (s.flags & skene::kStatNullCount) null_total += s.null_count;
                else nulls_known = false;
            }
            const bool tracked = (s.flags & skene::kStatNdv) != 0;
            const bool exact = (s.flags & skene::kStatNdvExact) != 0;
            const int64_t ndv = static_cast<int64_t>(s.ndv);
            if (tracked && exact) ndv_floor = std::max(ndv_floor, ndv);
            if (!tracked) {
                ndv_known = false;
            } else if (ndv_known) {
                if (!ndv_have) {
                    ndv_total = ndv;
                    ndv_exact = exact;
                    ndv_have = true;
                    ndv_range = has_bounds;
                    ndv_lo = s.min_ordinal;
                    ndv_hi = s.max_ordinal;
                } else {
                    const bool disjoint = has_bounds && ndv_range && (s.min_ordinal > ndv_hi || s.max_ordinal < ndv_lo);
                    if (disjoint) {
                        ndv_total += ndv;
                        ndv_exact = ndv_exact && exact;
                    } else {
                        ndv_total = std::max(ndv_total, ndv);
                        ndv_exact = false;
                    }
                    if (!has_bounds || !ndv_range) {
                        ndv_range = false;
                    } else {
                        ndv_lo = std::min(ndv_lo, s.min_ordinal);
                        ndv_hi = std::max(ndv_hi, s.max_ordinal);
                    }
                }
            }
        }

        ManifestCell& cell = m.cell(row, position);
        if (bounded && have_bounds) {
            set_ordinal_bound(cell.bounds, physical.at(position), true, low);
            set_ordinal_bound(cell.bounds, physical.at(position), false, high);
            applied.any_bounds = true;
        }
        if (nulls_known) {
            cell.null_count = static_cast<int64_t>(null_total);
            applied.any_nulls = true;
        }
        if (slot < meta.sketches.size() && meta.sketches[slot].present()) {
            cell.distinct_sketch = meta.sketches[slot].hashes;
            cell.has_distinct_sketch = true;
            any_sketch = true;
        }
        if (ndv_known && ndv_have) {
            cell.distinct_count = ndv_total;
            cell.distinct_exact = ndv_exact;
        }
        if (ndv_floor > 0) cell.distinct_floor = ndv_floor;
    }
    if (any_sketch) file.distinct_sketch_family = family;
    return applied;
}

// Parse the .skene file `file` (its bytes, or at least through its footer) and
// apply its statistics to file `row`. Returns "" or skene's error message.
inline std::string read_skene_footer_into(NativeManifest& m, size_t row, const void* file, size_t bytes,
                                          const std::vector<DrakenType>& physical, SkeneApplied& applied) {
    skene::FileMetadata meta;
    const skene::Status status = skene::read_metadata(file, bytes, &meta);
    if (!status.is_ok()) return status.message().empty() ? std::string("unreadable skene footer") : status.message();
    applied = apply_skene_footer(m, row, meta, physical, skene_slot_positions(meta, m.positions()));
    return std::string();
}

}  // namespace opteryx::planner
