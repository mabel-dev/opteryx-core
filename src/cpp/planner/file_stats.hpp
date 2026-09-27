// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// See the License at http://www.apache.org/licenses/LICENSE-2.0
// Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

// src/cpp/planner/file_stats.hpp — a data file's statistics, accumulated row
// group by row group as a writer writes it (native plan graph Q8, writers
// native; architect rulings 2026-09-27 (4)).
//
// Per column: the ORDINAL min / max (draken's own ordinalize kernels - the keys
// ANALYZE records, so the two producers write one dialect) and the null count.
// A column whose type has no ordinal key (ARRAY, VECTOR, DECIMAL128) gets no
// bounds, and neither does one with no non-null value; its null count is still
// kept.

#pragma once

#include <cstdint>
#include <stdexcept>
#include <vector>

#include "core/buffers.h"
#include "ops/hash.h"
#include "ops/ordinalize.h"
#include "planner/native_manifest.hpp"

namespace opteryx::planner {

class FileStatsAccumulator {
public:
    explicit FileStatsAccumulator(std::vector<DrakenType> physical)
        : physical_(std::move(physical)), columns_(physical_.size()) {}

    size_t column_count() const { return columns_.size(); }

    // One row group's vector for the column at `position`.
    void add(size_t position, const DrakenVector& v) {
        Column& c = columns_.at(position);
        const uint32_t n = v.length;
        if (v.type == DRAKEN_NULL) {
            c.nulls += n;
            return;
        }
        if (v.validity != nullptr) {
            uint32_t valid = 0;
            const uint32_t full = n >> 3;
            for (uint32_t k = 0; k < full; ++k) valid += static_cast<uint32_t>(__builtin_popcount(v.validity[k]));
            for (uint32_t i = full << 3; i < n; ++i) valid += (v.validity[i >> 3] >> (i & 7u)) & 1u;
            c.nulls += n - valid;
        }
        const unsigned idx = static_cast<unsigned>(v.type);
        if (idx >= OpsTable::kSize || g_ops_table().entries[idx].ordinalize == nullptr) {
            c.ordered = false;
            return;
        }
        if (!c.ordered || n == 0) return;
        scratch_.resize(n);
        draken_ordinalize(v, scratch_.data(), n);
        for (uint32_t i = 0; i < n; ++i) {
            const int64_t key = scratch_[i];
            if (key == draken::ops::ORDINAL_NULL) continue;
            if (!c.any || key < c.lo) c.lo = key;
            if (!c.any || key > c.hi) c.hi = key;
            c.any = true;
        }
    }

    // The accumulated statistics into file `row` of `m` (whose columns are the
    // accumulator's, in order).
    void write(NativeManifest& m, size_t row) const {
        if (m.column_count() != columns_.size()) {
            throw std::invalid_argument("file statistics for a different column count");
        }
        for (size_t k = 0; k < columns_.size(); ++k) {
            const Column& c = columns_[k];
            ManifestCell& cell = m.cell(row, k);
            cell.null_count = c.nulls;
            if (c.ordered && c.any) {
                set_ordinal_bound(cell.bounds, physical_[k], true, c.lo);
                set_ordinal_bound(cell.bounds, physical_[k], false, c.hi);
            }
        }
    }

private:
    struct Column {
        int64_t lo = 0, hi = 0;
        bool any = false;
        bool ordered = true;
        int64_t nulls = 0;
    };
    std::vector<DrakenType> physical_;
    std::vector<Column> columns_;
    std::vector<int64_t> scratch_;
};

}  // namespace opteryx::planner
