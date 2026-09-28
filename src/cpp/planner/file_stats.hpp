// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// See the License at http://www.apache.org/licenses/LICENSE-2.0
// Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

// src/cpp/planner/file_stats.hpp — a data file's statistics, accumulated row
// group by row group as a writer writes it (native plan graph Q8; writers
// native, architect rulings 2026-09-27 (4) and (6c)).
//
// The FULL statistic set the catalog's manifest carries, computed natively: the
// port of opteryx_catalog's ParquetManifestEntryAccumulator in its streaming
// (bounded-histogram) mode, running the SAME kernels (draken's hash_shaped via
// draken_hash_rows, ordinalize, ops/column_profile.h), so a file described here
// and a file described by the catalog carry the same numbers. Per column:
//   - a KMV sketch of the 32 smallest distinct hash_shaped() values (nulls hash
//     too); none for an ARRAY;
//   - the null count;
//   - the in-memory byte footprint (the column's view, an ARRAY's child
//     subtree, a carried key-hash seed - Morsel.nbytes' accounting);
//   - for the catalog's compressible categories: the ORDINAL min / max and an
//     equi-width histogram (BOOL: exact [true, false] counts; otherwise each
//     row group bucketed at 256 bins over its own range, redistributed into 32
//     over the file's range at the end - bounded memory, bin placement within
//     one fine bin of the exact kernel's);
//   - strings: byte-class counts, total bytes, the length range; ARRAY: the
//     list length range and statistics over the flat elements (KMV + ordinal
//     bounds).
// A statistic with no value stays unknown in the cell (never 0, never the
// sentinel); the manifest encoder writes the format's own spelling of unknown.

#pragma once

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <stdexcept>
#include <string>
#include <vector>

#include "core/buffers.h"
#include "core/draken_bridge.h"
#include "core/kmv_sketch.h"
#include "core/vector_owner.h"
#include "ops/column_profile.h"
#include "ops/exact_sum.h"
#include "ops/hash.h"
#include "ops/ordinalize.h"
#include "planner/native_manifest.hpp"
#include "planner/py_numeric.hpp"

namespace opteryx::planner {

namespace file_stats_detail {

inline constexpr size_t kMinK = 32;
inline constexpr int64_t kHistogramBins = 32;
inline constexpr int64_t kFineHistogramBins = kHistogramBins * 8;

using Kmv = draken::KmvSketch<kMinK, draken::KmvHashFamily::kDrakenVectorHash>;

// The catalog's compressible categories: the types it records ordinal bounds
// and a histogram for. UINT64 is not among them (its ordinal is sign-biased).
inline bool compressible(DrakenType t) {
    switch (t) {
        case DRAKEN_INT8: case DRAKEN_INT16: case DRAKEN_INT32: case DRAKEN_INT64:
        case DRAKEN_UINT8: case DRAKEN_UINT16: case DRAKEN_UINT32:
        case DRAKEN_DECIMAL: case DRAKEN_DECIMAL128:
        case DRAKEN_FLOAT32: case DRAKEN_FLOAT64:
        case DRAKEN_DATE32: case DRAKEN_TIMESTAMP64: case DRAKEN_TIME32: case DRAKEN_TIME64:
        case DRAKEN_INTERVAL: case DRAKEN_BOOL:
        case DRAKEN_VARCHAR: case DRAKEN_NVARCHAR: case DRAKEN_VARBINARY:
            return true;
        default:
            return false;
    }
}

inline bool has_ordinal_kernel(DrakenType t) {
    const unsigned idx = static_cast<unsigned>(t);
    return idx < OpsTable::kSize && g_ops_table().entries[idx].ordinalize != nullptr;
}

// opteryx_catalog's _redistribute_histograms: a kHistogramBins histogram over
// [vmin, vmax] assembled from per-row-group fine histograms, each over its own
// [gmin, gmax] (a single-valued group has no fine histogram). Counts are
// conserved exactly - the result sums to the non-null total, rounding settled
// by largest remainder. Mirrors the Python expression by expression.
struct Group {
    int64_t gmin, gmax;
    std::vector<int64_t> fine;   // empty: single-valued
    int64_t non_null;
};

inline std::vector<int64_t> redistribute(const std::vector<Group>& groups, int64_t vmin, int64_t vmax) {
    const int64_t bins = kHistogramBins;
    const __int128 span = static_cast<__int128>(vmax) - vmin;
    const double span_d = static_cast<double>(span);
    const double vmin_d = static_cast<double>(vmin);
    std::vector<double> acc(static_cast<size_t>(bins), 0.0);
    int64_t total = 0;

    auto target_of_int = [&](int64_t value) {
        return py::true_divide(static_cast<__int128>(value) - vmin, span) * static_cast<double>(bins - 1);
    };
    auto target_of_float = [&](double value) { return (value - vmin_d) / span_d * static_cast<double>(bins - 1); };
    auto clamp = [&](double index) -> size_t {
        const int64_t b = py::int_of(index);
        return static_cast<size_t>(b < 0 ? 0 : (b >= bins ? bins - 1 : b));
    };

    for (const Group& g : groups) {
        total += g.non_null;
        if (g.fine.empty()) {
            acc[clamp(target_of_int(g.gmin))] += static_cast<double>(g.non_null);
            continue;
        }
        const int64_t fine_bins = static_cast<int64_t>(g.fine.size());
        const __int128 gspan = static_cast<__int128>(g.gmax) - g.gmin;
        const double gmin_d = static_cast<double>(g.gmin);
        for (int64_t j = 0; j < fine_bins; ++j) {
            const int64_t count = g.fine[static_cast<size_t>(j)];
            if (count == 0) continue;
            if (j == fine_bins - 1) {
                // the last fine bin holds only values exactly at gmax
                acc[clamp(target_of_int(g.gmax))] += static_cast<double>(count);
                continue;
            }
            const double v_lo = gmin_d + py::true_divide(gspan * j, fine_bins - 1);
            const double v_hi = gmin_d + py::true_divide(gspan * (j + 1), fine_bins - 1);
            const double t_lo = target_of_float(v_lo);
            const double t_hi = target_of_float(v_hi);
            if (t_hi <= t_lo) {
                acc[clamp(t_lo)] += static_cast<double>(count);
                continue;
            }
            const double width = t_hi - t_lo;
            for (int64_t b = py::int_of(t_lo); b < bins && static_cast<double>(b) <= t_hi; ++b) {
                const double seg_lo = std::max(t_lo, static_cast<double>(b));
                const double seg_hi = std::min(t_hi, static_cast<double>(b + 1));
                if (seg_hi > seg_lo) acc[static_cast<size_t>(b)] += static_cast<double>(count) * (seg_hi - seg_lo) / width;
            }
        }
    }

    std::vector<int64_t> floors(static_cast<size_t>(bins));
    int64_t floor_sum = 0;
    for (size_t i = 0; i < floors.size(); ++i) {
        floors[i] = py::int_of(acc[i]);
        floor_sum += floors[i];
    }
    int64_t remainder = total - floor_sum;
    if (remainder > 0) {
        std::vector<size_t> by_fraction(floors.size());
        for (size_t i = 0; i < by_fraction.size(); ++i) by_fraction[i] = i;
        // Python's sorted(..., reverse=True) is stable: equal fractions keep index order
        std::stable_sort(by_fraction.begin(), by_fraction.end(), [&](size_t a, size_t b) {
            return (acc[a] - static_cast<double>(floors[a])) > (acc[b] - static_cast<double>(floors[b]));
        });
        for (size_t k = 0; k < by_fraction.size() && remainder > 0; ++k, --remainder) floors[by_fraction[k]] += 1;
    }
    return floors;
}

}  // namespace file_stats_detail

class FileStatsAccumulator {
public:
    explicit FileStatsAccumulator(std::vector<DrakenType> physical)
        : physical_(std::move(physical)), columns_(physical_.size()) {}

    size_t column_count() const { return columns_.size(); }
    int64_t uncompressed_size() const { return uncompressed_; }

    // One row group's column at `position`.
    void add(size_t position, const VectorOwner& owner) {
        using namespace file_stats_detail;
        Column& c = columns_.at(position);
        const DrakenVector& v = owner.vec;
        const DrakenType category = physical_[position];
        const uint32_t n = v.length;

        if (v.type != DRAKEN_ARRAY) hash_into(owner, c.kmv);
        if (v.validity != nullptr) c.nulls += count_nulls(v);
        if (is_integer(category) && c.sum_ok) fold_sum(c, v);

        const int64_t bytes = column_nbytes(owner);
        c.nbytes += bytes;
        uncompressed_ += bytes;

        if (compressible(category) && has_ordinal_kernel(v.type) && n > 0) fold_ordinal(c, v, category);

        if (draken::ops::is_string_type(category)) {
            draken::ops::char_class_stats(v, c.chars);
        } else if (category == DRAKEN_ARRAY) {
            fold_list_lengths(c, v);
        }

        if (category == DRAKEN_ARRAY && owner.child_owner) {
            const VectorOwner& child = *owner.child_owner;
            if (child.vec.type != DRAKEN_ARRAY) hash_into(child, c.element_kmv);
            if (has_ordinal_kernel(child.vec.type) && child.vec.length > 0) {
                ordinal_.resize(child.vec.length);
                draken_ordinalize(child.vec, ordinal_.data(), child.vec.length);
                int64_t lo = 0, hi = 0;
                if (draken::ops::ordinal_min_max(ordinal_.data(), identity(child.vec.length), child.vec.length, lo, hi)) {
                    c.element_min = c.element_any ? std::min(c.element_min, lo) : lo;
                    c.element_max = c.element_any ? std::max(c.element_max, hi) : hi;
                    c.element_any = true;
                }
            }
        }
    }

    // The per-column statistics into file `row` of `m` (whose columns are the
    // accumulator's, in order); the sketches go to `min_k`, `histogram` and
    // `char_class` (one slice per column, EMPTY when the column has none).
    void write(NativeManifest& m, size_t row, std::vector<std::vector<uint64_t>>& min_k,
               std::vector<std::vector<int64_t>>& histogram,
               std::vector<std::vector<int64_t>>& char_class) const {
        using namespace file_stats_detail;
        if (m.column_count() != columns_.size()) {
            throw std::invalid_argument("file statistics for a different column count");
        }
        min_k.assign(columns_.size(), {});
        histogram.assign(columns_.size(), {});
        char_class.assign(columns_.size(), {});
        for (size_t k = 0; k < columns_.size(); ++k) {
            const Column& c = columns_[k];
            const DrakenType category = physical_[k];
            ManifestCell& cell = m.cell(row, k);
            cell.null_count = c.nulls;
            cell.uncompressed_size = c.nbytes;
            if (is_integer(category) && c.sum_ok) {
                cell.has_sum = true;
                cell.sum = c.sum;
            }
            min_k[k] = c.kmv.min_k(kMinK);
            if (c.any) {
                set_ordinal_bound(cell.bounds, category, true, c.lo);
                set_ordinal_bound(cell.bounds, category, false, c.hi);
                if (category == DRAKEN_BOOL) {
                    histogram[k] = {c.true_count, c.false_count};
                } else if (c.hi > c.lo) {
                    histogram[k] = redistribute(c.groups, c.lo, c.hi);
                }
            }
            if (draken::ops::is_string_type(category)) {
                char_class[k].assign(std::begin(c.chars.counts), std::end(c.chars.counts));
                cell.char_total_bytes = static_cast<int64_t>(c.chars.total_bytes);
                if (c.chars.any) {
                    cell.min_length = c.chars.min_len;
                    cell.max_length = c.chars.max_len;
                }
            } else if (category == DRAKEN_ARRAY && c.lengths_any) {
                cell.min_length = c.min_len;
                cell.max_length = c.max_len;
            }
            if (category == DRAKEN_ARRAY) {
                if (c.element_any) {
                    cell.element_min = c.element_min;
                    cell.element_max = c.element_max;
                }
                cell.element_min_k = c.element_kmv.min_k(kMinK);
            }
        }
    }

private:
    struct Column {
        file_stats_detail::Kmv kmv;
        int64_t nulls = 0;
        int64_t nbytes = 0;
        bool any = false;
        int64_t lo = 0, hi = 0;
        int64_t true_count = 0, false_count = 0;
        std::vector<file_stats_detail::Group> groups;
        draken::ops::CharClassStats chars;
        bool lengths_any = false;
        int64_t min_len = 0, max_len = 0;
        file_stats_detail::Kmv element_kmv;
        bool element_any = false;
        int64_t element_min = 0, element_max = 0;
        // Integer columns: the exact sum of the non-null values
        // (draken/ops/exact_sum.h); sum_ok falls for good on a row group whose
        // vector is not an integer one, or an overflowing fold.
        bool sum_ok = true;
        __int128 sum = 0;
    };

    static void fold_sum(Column& c, const DrakenVector& v) {
        __int128 part = 0;
        uint64_t valid = 0;
        if ((!is_integer(v.type) && v.type != DRAKEN_NULL)
                || !draken::ops::exact_sum(v, &part, &valid)
                || __builtin_add_overflow(c.sum, part, &c.sum)) {
            c.sum_ok = false;
        }
    }

    // Vector.null_count(): rows minus valid rows; no validity means no nulls.
    static int64_t count_nulls(const DrakenVector& v) {
        const uint32_t n = v.length;
        uint32_t valid = 0;
        const uint32_t full = n >> 3;
        for (uint32_t k = 0; k < full; ++k) valid += static_cast<uint32_t>(__builtin_popcount(v.validity[k]));
        for (uint32_t i = full << 3; i < n; ++i) valid += (v.validity[i >> 3] >> (i & 7u)) & 1u;
        return static_cast<int64_t>(n) - valid;
    }

    // Morsel.nbytes' accounting for one column (cxx_morsel_nbytes).
    static int64_t column_nbytes(const VectorOwner& owner) {
        size_t bytes = draken_vector_nbytes(&owner.vec);
        if (owner.vec.type == DRAKEN_ARRAY && owner.child_owner) bytes += draken_vector_owner_nbytes(owner.child_owner.get());
        if (owner.keyhash_buf) bytes += static_cast<size_t>(owner.vec.data_length) * sizeof(uint64_t);
        return static_cast<int64_t>(bytes);
    }

    void hash_into(const VectorOwner& owner, file_stats_detail::Kmv& kmv) {
        const uint32_t n = owner.vec.length;
        if (n == 0) return;
        hashes_.resize(n);
        char error[256] = {0};
        if (draken_hash_rows(&owner, hashes_.data(), error, sizeof(error)) != 0) {
            throw std::invalid_argument(std::string("file statistics: ") + error);
        }
        for (uint32_t i = 0; i < n; ++i) kmv.add(hashes_[i]);
    }

    const uint32_t* identity(uint32_t n) {
        if (identity_.size() < n) {
            const size_t from = identity_.size();
            identity_.resize(n);
            for (size_t i = from; i < n; ++i) identity_[i] = static_cast<uint32_t>(i);
        }
        return identity_.data();
    }

    void fold_ordinal(Column& c, const DrakenVector& v, DrakenType category) {
        using namespace file_stats_detail;
        const uint32_t n = v.length;
        ordinal_.resize(n);
        draken_ordinalize(v, ordinal_.data(), n);
        const uint32_t* sel = identity(n);
        int64_t gmin = 0, gmax = 0;
        if (!draken::ops::ordinal_min_max(ordinal_.data(), sel, n, gmin, gmax)) return;   // every row null
        c.lo = c.any ? std::min(c.lo, gmin) : gmin;
        c.hi = c.any ? std::max(c.hi, gmax) : gmax;
        c.any = true;
        if (category == DRAKEN_BOOL) {
            int64_t counts[2] = {0, 0};
            draken::ops::histogram_bucket(ordinal_.data(), sel, n, 0, 1, 2, counts);
            c.false_count += counts[0];
            c.true_count += counts[1];
            return;
        }
        if (gmax > gmin) {
            Group g{gmin, gmax, std::vector<int64_t>(static_cast<size_t>(kFineHistogramBins), 0), 0};
            draken::ops::histogram_bucket(ordinal_.data(), sel, n, gmin, gmax, kFineHistogramBins, g.fine.data());
            for (int64_t count : g.fine) g.non_null += count;
            c.groups.push_back(std::move(g));
        } else {
            int64_t non_null = 0;
            draken::ops::histogram_bucket(ordinal_.data(), sel, n, gmin, gmax, 1, &non_null);
            c.groups.push_back(Group{gmin, gmax, {}, non_null});
        }
    }

    // An ARRAY column's list lengths (int32 offsets): the range over valid rows.
    static void fold_list_lengths(Column& c, const DrakenVector& v) {
        const int32_t* offsets = static_cast<const int32_t*>(v.data);
        for (uint32_t i = 0; i < v.length; ++i) {
            if (v.validity != nullptr && !((v.validity[i >> 3] >> (i & 7u)) & 1u)) continue;
            const uint32_t at = v.selection[i];
            const int64_t len = static_cast<int64_t>(offsets[at + 1]) - offsets[at];
            c.min_len = c.lengths_any ? std::min(c.min_len, len) : len;
            c.max_len = c.lengths_any ? std::max(c.max_len, len) : len;
            c.lengths_any = true;
        }
    }

    std::vector<DrakenType> physical_;
    std::vector<Column> columns_;
    int64_t uncompressed_ = 0;
    std::vector<int64_t> ordinal_;
    std::vector<uint64_t> hashes_;
    std::vector<uint32_t> identity_;
};

}  // namespace opteryx::planner
