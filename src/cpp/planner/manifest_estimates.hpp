// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// See the License at http://www.apache.org/licenses/LICENSE-2.0
// Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

// src/cpp/planner/manifest_estimates.hpp — a relation's statistics, estimated
// from its NativeManifest (native plan graph Q8, M-d1).
//
// Each estimate reads the sources, in the order, its Python predecessor on
// opteryx/models/manifest.py read them - that order is behaviour the planner's
// numbers depend on, so it is kept, not tidied:
//   - a file's OWN footer (FooterStats, `has_footer`) outranks the manifest for
//     bounds, null counts, distinct counts and sizes, file by file, and a file
//     whose footer does not know a fact does NOT fall back to the manifest;
//   - ordinal-dialect bounds are read as the int64 keys they are; the decoded
//     dialect's by what each end decoded as.
// Columns are load-time positions; unknown is kUnknown, never zero.

#pragma once

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <memory>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include "opteryx/third_party/maki_nage/_distogram.hpp"
#include "planner/manifest_sketch.hpp"
#include "planner/native_manifest.hpp"

namespace opteryx::planner {

namespace estimate_detail {

// One end of a bound as the estimates compare it. The tag stands for the Python
// type the planner held it as - int, float, str, bytes, Decimal, bool - and two
// ends are "the same type" exactly when their tags are equal.
struct End {
    DecodedTag tag = DECODED_NONE;
    int64_t i = 0;
    double d = 0.0;
    int32_t scale = 0;                  // DECIMAL: value = i * 10^-scale
    const std::string* text = nullptr;

    bool present() const { return tag != DECODED_NONE; }
    bool integer() const { return tag == DECODED_INT64 || tag == DECODED_UINT64; }
    bool numeric() const { return integer() || tag == DECODED_DOUBLE; }
    uint64_t u() const { return static_cast<uint64_t>(i); }
    double as_double() const {
        if (tag == DECODED_INT64) return static_cast<double>(i);
        if (tag == DECODED_UINT64) return static_cast<double>(u());
        return d;
    }
    // The integer exactly, whichever of the two integer tags holds it.
    __int128 wide() const { return tag == DECODED_UINT64 ? static_cast<__int128>(u()) : static_cast<__int128>(i); }
};

// Two ends of the one Python type: int is int however wide.
inline bool same_type(const End& a, const End& b) {
    return a.tag == b.tag || (a.integer() && b.integer());
}

inline End decoded_end(const Bounds& b, bool is_min) {
    End e;
    e.tag = is_min ? b.min_tag : b.max_tag;
    e.i = is_min ? b.min_int : b.max_int;
    e.d = is_min ? b.min_double : b.max_double;
    e.scale = is_min ? b.min_scale : b.max_scale;
    e.text = is_min ? &b.min_text : &b.max_text;
    return e;
}

inline End ordinal_end(int64_t ordinal) {
    End e;
    if (ordinal == kNoBound) return e;
    e.tag = DECODED_INT64;
    e.i = ordinal;
    return e;
}

// file `row`'s (min, max) for the column at `position`: its footer's decoded
// ends when it has a footer, else the manifest's - ordinal keys as integers in
// the ordinal dialect, decoded ends otherwise.
inline void file_ends(const NativeManifest& m, size_t row, size_t position, End& lo, End& hi) {
    const ManifestCell& cell = m.cell(row, position);
    if (m.file(row).has_footer) {
        lo = decoded_end(cell.footer.bounds, true);
        hi = decoded_end(cell.footer.bounds, false);
    } else if (m.bounds_are_ordinal()) {
        lo = ordinal_end(cell.bounds.min_ordinal);
        hi = ordinal_end(cell.bounds.max_ordinal);
    } else {
        lo = decoded_end(cell.bounds, true);
        hi = decoded_end(cell.bounds, false);
    }
}

// a < b for two present ends of comparable type (the caller has checked).
inline bool less(const End& a, const End& b) {
    if (a.numeric()) {
        if (a.integer() && b.integer()) return a.wide() < b.wide();
        return a.as_double() < b.as_double();
    }
    switch (a.tag) {
        case DECODED_DECIMAL:
        case DECODED_BOOL:
            return a.i < b.i;
        default:
            return *a.text < *b.text;   // str (UTF-8) and bytes both order bytewise
    }
}

// Whether two present ends compare at all, as the Python values did: numbers
// with numbers, otherwise only like with like.
inline bool comparable(const End& a, const End& b) {
    if (a.numeric() && b.numeric()) return true;
    return same_type(a, b);
}

}  // namespace estimate_detail

// The files' rows in the sketch vectors, in file order.
inline std::vector<uint32_t> vector_rows(const NativeManifest& m) {
    std::vector<uint32_t> rows(m.file_count());
    for (size_t f = 0; f < rows.size(); ++f) rows[f] = m.file(f).vector_row;
    return rows;
}

inline int64_t row_group_count(const NativeManifest& m) {
    int64_t total = 0;
    for (size_t f = 0; f < m.file_count(); ++f) {
        if (m.file(f).row_group_count == kUnknown) return kUnknown;
        total += m.file(f).row_group_count;
    }
    return total;
}

inline bool has_deletes(const NativeManifest& m) {
    for (size_t f = 0; f < m.file_count(); ++f) {
        if (m.file(f).deleted_record_count != 0) return true;
    }
    return false;
}

// Relation-wide ordinal-key span: the ordinal dialect only, and only
// non-negative keys (a negative key is a producer's "no bound" sentinel - a real
// string-family key is always >= 0).
inline bool ordinal_bounds(const NativeManifest& m, size_t position, int64_t& lo, int64_t& hi) {
    if (!m.bounds_are_ordinal()) return false;
    bool any = false;
    for (size_t f = 0; f < m.file_count(); ++f) {
        const Bounds& b = m.cell(f, position).bounds;
        if (b.min_ordinal == kNoBound || b.max_ordinal == kNoBound) continue;
        if (b.min_ordinal < 0 || b.max_ordinal < 0) continue;
        lo = any ? std::min(lo, b.min_ordinal) : b.min_ordinal;
        hi = any ? std::max(hi, b.max_ordinal) : b.max_ordinal;
        any = true;
    }
    return any;
}

// Relation-wide (min, max) byte length; non-positive lengths are "not computed".
inline bool length_bounds(const NativeManifest& m, size_t position, int64_t& lo, int64_t& hi) {
    bool any = false;
    for (size_t f = 0; f < m.file_count(); ++f) {
        const ManifestCell& c = m.cell(f, position);
        if (c.min_length <= 0 || c.max_length <= 0) continue;
        lo = any ? std::min(lo, c.min_length) : c.min_length;
        hi = any ? std::max(hi, c.max_length) : c.max_length;
        any = true;
    }
    return any;
}

// The column's 8-class byte totals and its non-null row count (files' rows less
// their manifest null counts, an unknown taken as 0). False when there are no
// char-class statistics for it.
inline bool char_class_stats(const NativeManifest& m, size_t position, int64_t (&totals)[8], int64_t& non_null_rows) {
    if (!m.char_class.present()) return false;
    if (!char_class_totals(m.char_class, static_cast<int64_t>(position), vector_rows(m), totals)) return false;
    non_null_rows = 0;
    for (size_t f = 0; f < m.file_count(); ++f) {
        const int64_t rows = m.file(f).record_count == kUnknown ? 0 : m.file(f).record_count;
        const int64_t nulls = m.cell(f, position).null_count == kUnknown ? 0 : m.cell(f, position).null_count;
        non_null_rows += std::max<int64_t>(0, rows - nulls);
    }
    return true;
}

// One file's histogram for the column: its bins are counts[begin, end), spread
// over (lo, hi).
struct HistogramPart {
    size_t begin;
    size_t end;
    double lo;
    double hi;
};

// Every file's histogram for the column, with the bounds that place its bins:
// the manifest's (ordinal keys in the ordinal dialect, the decoded numbers
// otherwise). A file without bins, or without both bounds, has no part.
inline void histogram_parts(const NativeManifest& m, size_t position, std::vector<int64_t>& counts,
                            std::vector<HistogramPart>& parts) {
    if (!m.histogram.present()) return;
    for (size_t f = 0; f < m.file_count(); ++f) {
        const size_t begin = counts.size();
        histogram_slice(m.histogram, static_cast<int64_t>(position), m.file(f).vector_row, counts);
        if (counts.size() == begin) continue;
        const Bounds& b = m.cell(f, position).bounds;
        double lo, hi;
        if (m.bounds_are_ordinal()) {
            if (b.min_ordinal == kNoBound || b.max_ordinal == kNoBound) {
                counts.resize(begin);
                continue;
            }
            lo = static_cast<double>(b.min_ordinal);
            hi = static_cast<double>(b.max_ordinal);
        } else {
            const estimate_detail::End l = estimate_detail::decoded_end(b, true);
            const estimate_detail::End h = estimate_detail::decoded_end(b, false);
            if (!l.present() || !h.present()) {
                counts.resize(begin);
                continue;
            }
            auto number = [](const estimate_detail::End& e) {
                switch (e.tag) {
                    case DECODED_INT64: case DECODED_BOOL: return static_cast<double>(e.i);
                    case DECODED_UINT64: return static_cast<double>(e.u());
                    case DECODED_DOUBLE: case DECODED_DECIMAL: return e.d;
                    default: throw std::invalid_argument("a histogram's bounds are not numbers");
                }
            };
            lo = number(l);
            hi = number(h);
        }
        parts.push_back(HistogramPart{begin, counts.size(), lo, hi});
    }
}

// The column's per-file histograms folded into one Distogram, in file order
// (the first file's, then every other merged into it), or nullptr when no file
// has one.
inline std::shared_ptr<maki_nage::Distogram> column_distogram(const NativeManifest& m, size_t position) {
    std::vector<int64_t> counts;
    std::vector<HistogramPart> parts;
    histogram_parts(m, position, counts, parts);
    if (parts.empty()) return nullptr;
    auto combined = std::make_shared<maki_nage::Distogram>(maki_nage::Distogram::from_counts(
        counts.data() + parts[0].begin, static_cast<int64_t>(parts[0].end - parts[0].begin), parts[0].lo,
        parts[0].hi));
    for (size_t k = 1; k < parts.size(); ++k) {
        combined->merge(maki_nage::Distogram::from_counts(counts.data() + parts[k].begin,
                                                          static_cast<int64_t>(parts[k].end - parts[k].begin),
                                                          parts[k].lo, parts[k].hi));
    }
    return combined;
}

// The relation's EXACT distinct count from the files' exact footer counts, or
// kUnknown: one file, or several whose ordinal ranges are pairwise STRICTLY
// disjoint (ordinalize is monotonic but not injective, so touching ranges prove
// nothing).
inline int64_t exact_cardinality_from_footers(const NativeManifest& m, size_t position) {
    if (m.file_count() == 0) return kUnknown;
    int64_t total = 0;
    std::vector<std::pair<int64_t, int64_t>> intervals;
    intervals.reserve(m.file_count());
    for (size_t f = 0; f < m.file_count(); ++f) {
        const ManifestCell& c = m.cell(f, position);
        if (c.distinct_count == kUnknown || !c.distinct_exact) return kUnknown;
        total += c.distinct_count;
        intervals.emplace_back(c.bounds.min_ordinal, c.bounds.max_ordinal);
    }
    if (m.file_count() == 1) return total;
    if (!m.bounds_are_ordinal()) return kUnknown;
    for (const auto& interval : intervals) {
        if (interval.first == kNoBound || interval.second == kNoBound) return kUnknown;
    }
    std::sort(intervals.begin(), intervals.end());
    for (size_t k = 1; k < intervals.size(); ++k) {
        if (!(intervals[k - 1].second < intervals[k].first)) return kUnknown;
    }
    return total;
}

namespace estimate_detail {

template <typename Sketch>
inline double union_estimate(const NativeManifest& m, size_t position, bool& exact) {
    Sketch merged;
    for (size_t f = 0; f < m.file_count(); ++f) {
        for (uint64_t hash : m.cell(f, position).distinct_sketch) merged.add(hash);
    }
    return rounded_estimate(merged, exact);
}

}  // namespace estimate_detail

// The relation's distinct NON-NULL count from the files' own KMV sketches (every
// file must carry one, all of one hash family), or kUnknown.
inline int64_t cardinality_from_sketches(const NativeManifest& m, size_t position) {
    const size_t n = m.file_count();
    if (n == 0) return kUnknown;
    int32_t family = 0;
    for (size_t f = 0; f < n; ++f) {
        if (!m.cell(f, position).has_distinct_sketch) return kUnknown;
        const int32_t this_family = m.file(f).distinct_sketch_family;
        if (f > 0 && this_family != family) return kUnknown;
        family = this_family;
    }
    bool exact = false;
    double estimate;
    if (family == static_cast<int32_t>(draken::KmvHashFamily::kDrakenVectorHash)) {
        estimate = estimate_detail::union_estimate<ManifestSketch>(m, position, exact);
    } else if (family == static_cast<int32_t>(draken::KmvHashFamily::kXxh3ValueBytes)) {
        estimate = estimate_detail::union_estimate<SkeneSketch>(m, position, exact);
    } else {
        throw std::invalid_argument("a distinct-value sketch of an unknown hash family");
    }
    // truncated, as the Python int of the rounded estimate was
    double count = std::trunc(estimate);

    // A family-2 sketch holds the null row's hash once when the column has a
    // null; this count is of NON-NULL values, so that hash comes out - which
    // needs every file's null count.
    if (family == static_cast<int32_t>(draken::KmvHashFamily::kDrakenVectorHash)) {
        bool has_null = false;
        for (size_t f = 0; f < n; ++f) {
            const int64_t nulls = m.cell(f, position).null_count;
            if (nulls == kUnknown) return kUnknown;
            if (nulls > 0) has_null = true;
        }
        if (has_null && count > 0) count -= 1;
    }

    if (!exact) {
        // Floor with what the footers PROVE: the largest exact per-file count.
        double floor = 0;
        for (size_t f = 0; f < n; ++f) {
            const ManifestCell& c = m.cell(f, position);
            if (c.distinct_floor > 0) floor = std::max(floor, static_cast<double>(c.distinct_floor));
            if (c.distinct_count != kUnknown && c.distinct_exact) {
                floor = std::max(floor, static_cast<double>(c.distinct_count));
            }
        }
        count = std::max(count, floor);
    }

    const int64_t total_rows = m.record_count();
    if (total_rows != kUnknown && total_rows > 0) count = std::min(count, static_cast<double>(total_rows));
    if (count == 0) return 0;
    count = std::max(1.0, count);
    return count >= 9.2e18 ? INT64_MAX : static_cast<int64_t>(count);
}

// The relation's distinct count (see Manifest.estimate_cardinality): exact
// footer counts, else ANALYZE's min-k sketches, else the files' own sketches.
// kUnknown when none answers. `estimate` is set for a saturated min-k union,
// whose count can outgrow int64 - the caller reads it as a double then.
inline int64_t estimate_cardinality(const NativeManifest& m, size_t position, double& estimate) {
    estimate = -1.0;
    const int64_t exact = exact_cardinality_from_footers(m, position);
    if (exact != kUnknown) return exact;
    if (!m.min_k.present()) return cardinality_from_sketches(m, position);
    ManifestSketch merged;
    kmv_union(m.min_k, static_cast<int64_t>(position), vector_rows(m), merged);
    if (merged.size() == 0) return kUnknown;
    if (merged.is_exact()) return static_cast<int64_t>(merged.size());
    estimate = std::trunc(merged.estimate());
    return kUnknown;
}

// NDV from per-file footer statistics - the costing-only fallback (see
// Manifest.estimate_range_cardinality for the rules and their reasons).
// `identity_category`: the column's ordinal keys ARE its values (INTEGER, DATE);
// in the ordinal dialect only such a column's bounds are usable.
inline int64_t estimate_range_cardinality(const NativeManifest& m, size_t position, bool identity_category) {
    using estimate_detail::End;
    const int64_t total_rows = m.record_count();
    if (total_rows == kUnknown || total_rows <= 0) return kUnknown;
    const bool bounds_usable = !m.bounds_are_ordinal() || identity_category;

    struct PerFile {
        double ndv;
        End lo, hi;
        bool numeric;
    };
    std::vector<PerFile> per_file;
    per_file.reserve(m.file_count());
    for (size_t f = 0; f < m.file_count(); ++f) {
        const int64_t rows = m.file(f).record_count;
        if (rows == kUnknown || rows <= 0) return kUnknown;
        const ManifestCell& cell = m.cell(f, position);
        const int64_t footer_ndv = m.file(f).has_footer ? cell.footer.distinct_count : cell.distinct_count;
        PerFile p;
        if (bounds_usable) estimate_detail::file_ends(m, f, position, p.lo, p.hi);
        p.numeric = p.lo.numeric() && p.hi.numeric() && !estimate_detail::less(p.hi, p.lo);
        int64_t ndv;
        if (footer_ndv != kUnknown) {
            ndv = std::min(rows, footer_ndv);
        } else if (p.numeric && p.lo.integer() && p.hi.integer()) {
            const __int128 span = p.hi.wide() - p.lo.wide() + 1;
            ndv = span < static_cast<__int128>(rows) ? static_cast<int64_t>(span) : rows;
        } else if (p.numeric) {
            ndv = std::max<int64_t>(1, rows / 2);
        } else {
            return kUnknown;
        }
        p.ndv = static_cast<double>(ndv);
        per_file.push_back(p);
    }
    if (per_file.empty()) return kUnknown;

    double running_ndv = per_file[0].ndv;
    End running_lo, running_hi;
    if (per_file[0].lo.present()) {
        running_lo = per_file[0].lo;
        running_hi = per_file[0].hi;
    }
    const bool running_numeric = per_file[0].numeric;
    for (size_t k = 1; k < per_file.size(); ++k) {
        const PerFile& p = per_file[k];
        if (p.numeric && running_numeric && running_lo.present()) {
            // numeric overlap: accrue the fraction of this file's range outside
            // the running range
            const double width = p.hi.as_double() - p.lo.as_double();
            double fraction;
            if (width <= 0.0) {
                const bool inside = !estimate_detail::less(p.lo, running_lo) && !estimate_detail::less(running_hi, p.lo);
                fraction = inside ? 0.0 : 1.0;
            } else {
                const double outside = std::max(0.0, running_lo.as_double() - p.lo.as_double()) +
                                       std::max(0.0, p.hi.as_double() - running_hi.as_double());
                fraction = std::min(1.0, outside / width);
            }
            running_ndv += fraction * p.ndv;
            if (estimate_detail::less(p.lo, running_lo)) running_lo = p.lo;
            if (estimate_detail::less(running_hi, p.hi)) running_hi = p.hi;
        } else if (p.lo.present() && running_lo.present() && estimate_detail::same_type(p.lo, running_lo) &&
                   estimate_detail::same_type(p.hi, running_hi)) {
            // comparable non-numeric bounds: disjoint ranges hold disjoint values
            // (sum), overlapping ones take the max. An absent end here is a
            // comparison with nothing, which never had an answer.
            if (!p.hi.present() || !running_hi.present()) return kUnknown;
            if (estimate_detail::less(p.hi, running_lo) || estimate_detail::less(running_hi, p.lo)) {
                running_ndv += p.ndv;
            } else {
                running_ndv = std::max(running_ndv, p.ndv);
            }
            if (estimate_detail::less(p.lo, running_lo)) running_lo = p.lo;
            if (estimate_detail::less(running_hi, p.hi)) running_hi = p.hi;
        } else {
            running_ndv = std::max(running_ndv, p.ndv);
        }
    }
    const double capped = std::min(std::trunc(running_ndv), static_cast<double>(total_rows));
    return std::max<int64_t>(1, static_cast<int64_t>(capped));
}

// Total nulls: every file's (footer's when it has one), else kUnknown.
inline int64_t total_null_count(const NativeManifest& m, size_t position) {
    int64_t total = 0;
    for (size_t f = 0; f < m.file_count(); ++f) {
        const ManifestCell& c = m.cell(f, position);
        const int64_t nulls = m.file(f).has_footer ? c.footer.null_count : c.null_count;
        if (nulls == kUnknown) return kUnknown;
        total += nulls;
    }
    return total;
}

// The column's EXACT sum over every file (draken/ops/exact_sum.h) - an ANSWER,
// not an estimate: SUM/AVG are returned from it with the scan removed. Per file
// its parquet footer's sum when the footer carries one, else the manifest's own
// (both are exact). False - no answer - when any file has neither, when any file
// carries merge-on-read deletes (the sums describe the physical superset), or
// when the fold leaves __int128.
inline bool total_sum(const NativeManifest& m, size_t position, __int128& out) {
    if (has_deletes(m)) return false;
    __int128 total = 0;
    for (size_t f = 0; f < m.file_count(); ++f) {
        const ManifestCell& c = m.cell(f, position);
        __int128 part = 0;
        if (m.file(f).has_footer && c.footer.has_sum) part = c.footer.sum;
        else if (c.has_sum) part = c.sum;
        else return false;
        if (__builtin_add_overflow(total, part, &total)) return false;
    }
    out = total;
    return true;
}

// Fraction of nulls over the live rows, the known counts summed; false when the
// row count is unknown or zero.
inline bool null_fraction(const NativeManifest& m, size_t position, double& fraction) {
    const int64_t total_rows = m.record_count();
    if (total_rows == kUnknown || total_rows == 0) return false;
    int64_t nulls = 0;
    for (size_t f = 0; f < m.file_count(); ++f) {
        const ManifestCell& c = m.cell(f, position);
        const int64_t n = m.file(f).has_footer ? c.footer.null_count : c.null_count;
        if (n != kUnknown) nulls += n;
    }
    fraction = total_rows > 0 ? static_cast<double>(nulls) / static_cast<double>(total_rows) : 0.0;
    return true;
}

// Total uncompressed bytes: every file's (footer's when it has one), else kUnknown.
inline int64_t total_uncompressed_size(const NativeManifest& m, size_t position) {
    int64_t total = 0;
    for (size_t f = 0; f < m.file_count(); ++f) {
        const ManifestCell& c = m.cell(f, position);
        const int64_t size = m.file(f).has_footer ? c.footer.uncompressed_size : c.uncompressed_size;
        if (size == kUnknown) return kUnknown;
        total += size;
    }
    return total;
}

// The relation's NUMERIC (min, max): every file's ends (footer's first) folded,
// kept only when both ends are numbers - the only bounds the planner's value
// ranges hold. In the ordinal dialect only an identity-category column has one.
// False when there is no numeric range, or the ends never compared.
inline bool value_range(const NativeManifest& m, size_t position, bool identity_category,
                        estimate_detail::End& min_value, estimate_detail::End& max_value) {
    using estimate_detail::End;
    if (m.bounds_are_ordinal() && !identity_category) return false;
    min_value = End();
    max_value = End();
    for (size_t f = 0; f < m.file_count(); ++f) {
        End lo, hi;
        estimate_detail::file_ends(m, f, position, lo, hi);
        if (lo.present()) {
            if (min_value.present() && !estimate_detail::comparable(lo, min_value)) return false;
            if (!min_value.present() || estimate_detail::less(lo, min_value)) min_value = lo;
        }
        if (hi.present()) {
            if (max_value.present() && !estimate_detail::comparable(hi, max_value)) return false;
            if (!max_value.present() || estimate_detail::less(max_value, hi)) max_value = hi;
        }
    }
    return min_value.numeric() && max_value.numeric();
}

// The relation's (min, max) for the column as the statistics-only MIN/MAX
// answer reads it: every file's ends (footer's first, else the manifest's)
// folded, whatever their type. False when two ends never compared (the
// Python compare raised). An end nothing bounded is absent (tag NONE).
inline bool extreme_ends(const NativeManifest& m, size_t position,
                         estimate_detail::End& min_value, estimate_detail::End& max_value) {
    using estimate_detail::End;
    min_value = End();
    max_value = End();
    for (size_t f = 0; f < m.file_count(); ++f) {
        End lo, hi;
        estimate_detail::file_ends(m, f, position, lo, hi);
        if (lo.present()) {
            if (min_value.present() && !estimate_detail::comparable(lo, min_value)) return false;
            if (!min_value.present() || estimate_detail::less(lo, min_value)) min_value = lo;
        }
        if (hi.present()) {
            if (max_value.present() && !estimate_detail::comparable(hi, max_value)) return false;
            if (!max_value.present() || estimate_detail::less(max_value, hi)) max_value = hi;
        }
    }
    return true;
}

// Whether any file records a null count for any column - in its footer or in
// the manifest, either one - the gate the statistics refresh puts on null
// fractions.
inline bool has_null_counts(const NativeManifest& m) {
    for (size_t f = 0; f < m.file_count(); ++f) {
        for (size_t c = 0; c < m.column_count(); ++c) {
            const ManifestCell& cell = m.cell(f, c);
            if (cell.footer.null_count != kUnknown || cell.null_count != kUnknown) return true;
        }
    }
    return false;
}

}  // namespace opteryx::planner
