// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// See the License at http://www.apache.org/licenses/LICENSE-2.0
// Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

// src/cpp/planner/native_manifest.hpp — a relation's manifest as native rows.
//
// One row per data file and, per file, one cell per column of the schema the
// manifest was LOADED against (native plan graph Q8, architect rulings
// 2026-09-27):
//   - columns are keyed by their load-time position; a manifest's field_id and
//     write-order lists are re-keyed to it once, when the manifest is decoded;
//   - every bound is an int64 ORDINAL key (draken/ops/ordinalize.h), and a
//     numeric or temporal column also keeps its DECODED min/max;
//   - a parquet file's own footer statistics sit beside the manifest's, never
//     over them (FooterStats);
//   - UNKNOWN is never zero: a count nobody recorded is kUnknown, a bound
//     nobody recorded is kNoBound.
//
// The whole-column sketch vectors (min-k hashes, histograms, char classes)
// stay draken vectors; the Python NativeManifest holds them and the native side
// reads borrowed views (manifest_sketch.hpp). `vector_row` maps a file row back to its row in them,
// so a pruned manifest reads the right file's sketch.

#pragma once

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "core/buffers.h"
#include "ops/ordinalize.h"
#include "planner/manifest_sketch.hpp"

namespace opteryx::planner {

inline constexpr int64_t kUnknown = -1;
inline constexpr int64_t kNoBound = INT64_MIN;

// What a decoded bound IS, as the statistics that read it distinguish values:
// an integer (every int32/int64 footer statistic, temporal ones included), a
// float, text (a string column's UTF-8), raw bytes (a binary column), a DECIMAL,
// a boolean, or another value only its ordinal can order.
enum DecodedTag : uint8_t {
    DECODED_NONE = 0,
    DECODED_INT64 = 1,
    DECODED_DOUBLE = 2,
    DECODED_TEXT = 3,
    DECODED_BYTES = 4,
    DECODED_DECIMAL = 5,
    DECODED_BOOL = 6,
    DECODED_OTHER = 7,
    DECODED_UINT64 = 8,   // an unsigned integer above INT64_MAX; its bits in the int field
};

// One column's (min, max) as ONE source recorded it: the ordinal keys, and the
// decoded values where that source has them. Min and max are independent - a
// source can know one without the other.
struct Bounds {
    int64_t min_ordinal = kNoBound;
    int64_t max_ordinal = kNoBound;
    // What each decoded end IS; NONE when the source holds no decoded value for
    // it. Per end, because the ends can differ: a JSON column's min can be
    // printable text while its max is not, an unsigned 64-bit max can outgrow
    // int64 while its min does not.
    DecodedTag min_tag = DECODED_NONE;
    DecodedTag max_tag = DECODED_NONE;
    int64_t min_int = 0;          // INT64, BOOL; DECIMAL's unscaled value
    int64_t max_int = 0;
    double min_double = 0.0;      // DOUBLE; DECIMAL's value
    double max_double = 0.0;
    int32_t min_scale = 0;        // DECIMAL: value = unscaled * 10^-scale
    int32_t max_scale = 0;
    std::string min_text;         // TEXT, BYTES, OTHER
    std::string max_text;
};

// What a parquet file's OWN footer says about a column (rugo's AggColumnStat,
// see manifest_footer.hpp). Held apart from the manifest's statistics because
// both describe the same file at once - a parquet dataset ANALYZE has run over
// carries the footer's decoded bounds AND the manifest's ordinal ones - and each
// estimate reads the source it always has. Present only when the file row's
// `has_footer` is set; a column the footer does not describe stays all-unknown.
struct FooterStats {
    Bounds bounds;
    int64_t null_count = kUnknown;
    int64_t distinct_count = kUnknown;
    int64_t uncompressed_size = kUnknown;
};

struct ManifestCell {
    Bounds bounds;                // the manifest's (ordinal or decoded dialect)
    int64_t null_count = kUnknown;
    int64_t min_length = kUnknown;
    int64_t max_length = kUnknown;
    int64_t char_total_bytes = kUnknown;
    int64_t uncompressed_size = kUnknown;
    int64_t distinct_count = kUnknown;
    bool distinct_exact = false;
    int64_t distinct_floor = kUnknown;
    bool has_distinct_sketch = false;
    std::vector<uint64_t> distinct_sketch;   // a skene file's own KMV sketch (may be empty)
    FooterStats footer;
    // ARRAY columns: statistics over the flat child - the elements of every
    // list - which the catalog's manifest carries (element_min_values /
    // element_max_values / element_min_k_hashes). Ordinal keys.
    int64_t element_min = kNoBound;
    int64_t element_max = kNoBound;
    std::vector<uint64_t> element_min_k;
};

struct ManifestFile {
    std::string path;
    std::string format;
    int64_t record_count = kUnknown;          // PHYSICAL rows
    int64_t file_size = 0;
    int64_t uncompressed_size = kUnknown;
    int64_t row_group_count = kUnknown;
    int64_t histogram_bins = kUnknown;
    int64_t deleted_record_count = 0;
    std::string delete_file_path;
    bool delete_positions_resolved = false;
    std::vector<int64_t> delete_positions;
    int32_t distinct_sketch_family = 0;       // 0 none, 1 skene XXH3, 2 draken Vector.hash
    bool has_footer = false;                  // the cells' `footer` statistics are the file's
    uint32_t vector_row = 0;                  // this file's row in the sketch vectors
};

inline bool is_integer(DrakenType t) {
    switch (t) {
        case DRAKEN_INT8: case DRAKEN_INT16: case DRAKEN_INT32: case DRAKEN_INT64:
        case DRAKEN_UINT8: case DRAKEN_UINT16: case DRAKEN_UINT32: case DRAKEN_UINT64:
            return true;
        default:
            return false;
    }
}

inline bool is_float(DrakenType t) { return t == DRAKEN_FLOAT32 || t == DRAKEN_FLOAT64; }

// A string's heap allocation: 0 while its characters sit inline (short-string).
inline size_t heap_bytes(const std::string& s) {
    const char* data = s.data();
    const char* self = reinterpret_cast<const char*>(&s);
    return (data >= self && data < self + sizeof(s)) ? 0 : s.capacity() + 1;
}

// An integer or temporal column whose ordinal key IS its value (UINT64's key is
// sign-biased, so it is not one of them).
inline bool ordinal_is_value(DrakenType t) {
    return (is_integer(t) && t != DRAKEN_UINT64) || t == DRAKEN_DATE32 || t == DRAKEN_TIME32 ||
           t == DRAKEN_TIME64 || t == DRAKEN_TIMESTAMP64;
}

// The ways a manifest bound lands in a cell - the manifest decoder
// (manifest_decode.hpp) and the builder (native_manifest.pyx) both write through
// these, so a bound means the same thing whoever recorded it.

// An ORDINAL-dialect bound: the key, and - where the key IS the value - the value.
inline void set_ordinal_bound(Bounds& b, DrakenType physical, bool is_min, int64_t ordinal) {
    (is_min ? b.min_ordinal : b.max_ordinal) = ordinal;
    if (ordinal_is_value(physical)) {
        (is_min ? b.min_tag : b.max_tag) = DECODED_INT64;
        (is_min ? b.min_int : b.max_int) = ordinal;
    }
}

// A decoded integer bound. It is an integer VALUE whatever the column's type
// says - a manifest that recorded an integer is read as one - and its ordinal is
// the value, sign-biased for an unsigned 64-bit column.
inline void set_int_bound(Bounds& b, DrakenType physical, bool is_min, int64_t value) {
    (is_min ? b.min_ordinal : b.max_ordinal) =
        physical == DRAKEN_UINT64 ? draken::ops::ordinalize_scalar_u64(static_cast<uint64_t>(value)) : value;
    (is_min ? b.min_tag : b.max_tag) = DECODED_INT64;
    (is_min ? b.min_int : b.max_int) = value;
}

// A decoded unsigned 64-bit integer bound: INT64 when it fits, UINT64 above.
inline void set_uint_bound(Bounds& b, bool is_min, uint64_t value) {
    (is_min ? b.min_ordinal : b.max_ordinal) = draken::ops::ordinalize_scalar_u64(value);
    (is_min ? b.min_tag : b.max_tag) = value > static_cast<uint64_t>(INT64_MAX) ? DECODED_UINT64 : DECODED_INT64;
    (is_min ? b.min_int : b.max_int) = static_cast<int64_t>(value);
}

inline void set_double_bound(Bounds& b, bool is_min, double value) {
    (is_min ? b.min_ordinal : b.max_ordinal) = draken::ops::ordinalize_scalar_f64(value);
    (is_min ? b.min_tag : b.max_tag) = DECODED_DOUBLE;
    (is_min ? b.min_double : b.max_double) = value;
}

// Text (UTF-8) or opaque bytes, ordered by their 8-byte prefix.
inline void set_bytes_bound(Bounds& b, bool is_min, bool is_text, std::string value) {
    (is_min ? b.min_ordinal : b.max_ordinal) = draken::ops::ordinalize_scalar_bytes8(
        reinterpret_cast<const uint8_t*>(value.data()), static_cast<uint32_t>(value.size()));
    (is_min ? b.min_tag : b.max_tag) = is_text ? DECODED_TEXT : DECODED_BYTES;
    (is_min ? b.min_text : b.max_text) = std::move(value);
}

inline void set_decimal_bound(Bounds& b, bool is_min, int64_t unscaled, int32_t scale, double value) {
    (is_min ? b.min_ordinal : b.max_ordinal) = unscaled;
    (is_min ? b.min_tag : b.max_tag) = DECODED_DECIMAL;
    (is_min ? b.min_int : b.max_int) = unscaled;
    (is_min ? b.min_scale : b.max_scale) = scale;
    (is_min ? b.min_double : b.max_double) = value;
}

inline void set_bool_bound(Bounds& b, bool is_min, bool value) {
    (is_min ? b.min_ordinal : b.max_ordinal) = value ? 1 : 0;
    (is_min ? b.min_tag : b.max_tag) = DECODED_BOOL;
    (is_min ? b.min_int : b.max_int) = value ? 1 : 0;
}

// A whole-column sketch the manifest OWNS - built by a producer (ANALYZE) rather
// than decoded - laid out exactly as a decoded manifest's draken
// `array<array<T>>` vector is (outer = files, middle = columns, leaf = values),
// so the one NestedArrayView reads both.
struct OwnedNested {
    std::vector<int32_t> outer_offsets;    // files + 1
    std::vector<int32_t> mid_offsets;      // middle rows + 1
    std::vector<uint8_t> mid_validity;     // 1 bit per middle row (a null column slice)
    std::vector<uint64_t> leaf;            // the values' bits (uint64 or int64)
    std::vector<uint32_t> outer_selection, mid_selection, leaf_selection;   // identity
    DrakenVector outer{}, mid{}, leaf_vector{};

    // `per_file[f]` is file f's per-column slices (empty: the file has none);
    // a slice that is nullopt is a null middle row.
    OwnedNested(const std::vector<std::vector<std::optional<std::vector<uint64_t>>>>& per_file, DrakenType leaf_type) {
        outer_offsets.push_back(0);
        mid_offsets.push_back(0);
        size_t mid_rows = 0;
        for (const auto& file : per_file) mid_rows += file.size();
        mid_validity.assign((mid_rows + 7) / 8, 0);
        size_t m = 0;
        for (const auto& file : per_file) {
            for (const auto& slice : file) {
                if (slice) {
                    mid_validity[m >> 3] |= static_cast<uint8_t>(1u << (m & 7));
                    leaf.insert(leaf.end(), slice->begin(), slice->end());
                }
                mid_offsets.push_back(static_cast<int32_t>(leaf.size()));
                ++m;
            }
            outer_offsets.push_back(static_cast<int32_t>(m));
        }
        auto identity = [](std::vector<uint32_t>& v, size_t n) {
            v.resize(n == 0 ? 1 : n);
            for (size_t k = 0; k < v.size(); ++k) v[k] = static_cast<uint32_t>(k);
        };
        identity(outer_selection, per_file.size());
        identity(mid_selection, mid_rows);
        identity(leaf_selection, leaf.size());
        outer.data = outer_offsets.data();
        outer.selection = outer_selection.data();
        outer.length = outer.data_length = static_cast<uint32_t>(per_file.size());
        outer.type = DRAKEN_ARRAY;
        mid.data = mid_offsets.data();
        mid.selection = mid_selection.data();
        mid.length = mid.data_length = static_cast<uint32_t>(mid_rows);
        mid.validity = mid_validity.empty() ? nullptr : mid_validity.data();
        mid.type = DRAKEN_ARRAY;
        leaf_vector.data = leaf.data();
        leaf_vector.selection = leaf_selection.data();
        leaf_vector.length = leaf_vector.data_length = static_cast<uint32_t>(leaf.size());
        leaf_vector.type = leaf_type;
    }

    OwnedNested(const OwnedNested&) = delete;
    OwnedNested& operator=(const OwnedNested&) = delete;

    NestedArrayView view() const { return NestedArrayView(&outer, &mid, &leaf_vector); }
};

// A builder's sketch rows for one sketch kind, file by file: a file with no row
// is nullopt; a row holds one slice per column (a slice nullopt is a null
// middle row, an empty slice is "nothing recorded").
struct SketchStaging {
    std::vector<std::optional<std::vector<std::optional<std::vector<uint64_t>>>>> files;

    void grow(size_t rows) {
        if (files.size() < rows) files.resize(rows);
    }

    // File `row`'s row, created with an empty slice per column when absent.
    std::vector<std::optional<std::vector<uint64_t>>>& row_of(size_t row, size_t columns) {
        grow(row + 1);
        if (!files[row]) files[row].emplace(columns, std::vector<uint64_t>());
        return *files[row];
    }

    // Carry `view`'s row `vector_row` into file `row` - only a row exactly
    // `columns` wide (a narrower or wider one describes another schema).
    bool carry(const NestedArrayView& view, uint32_t vector_row, size_t row, size_t columns) {
        if (!view.present() || vector_row >= view.n_files() || !sketch_bit_valid(view.outer->validity, vector_row)) {
            return false;
        }
        const int32_t* poff = static_cast<const int32_t*>(view.outer->data);
        const uint32_t pi = view.outer->selection[vector_row];
        if (static_cast<size_t>(poff[pi + 1] - poff[pi]) != columns) return false;
        grow(row + 1);
        std::vector<std::optional<std::vector<uint64_t>>> slices(columns);
        for (size_t c = 0; c < columns; ++c) {
            view.with_field_slice(vector_row, static_cast<int64_t>(c), [&](int32_t g0, int32_t g1) {
                std::vector<uint64_t> values;
                values.reserve(static_cast<size_t>(g1 - g0));
                for (int32_t g = g0; g < g1; ++g) values.push_back(view.u64(g));
                slices[c] = std::move(values);
            });
        }
        files[row] = std::move(slices);
        return true;
    }

    // Carry `view`'s row `vector_row` into file `row`, re-keyed: column k takes
    // the source column positions[k]'s slice (-1: an empty slice). Nothing is
    // staged when the source row holds no sketches.
    void carry_keyed(const NestedArrayView& view, uint32_t vector_row, size_t row,
                     const std::vector<int64_t>& positions) {
        if (!view.present() || vector_row >= view.n_files() || !sketch_bit_valid(view.outer->validity, vector_row)) {
            return;
        }
        grow(row + 1);
        std::vector<std::optional<std::vector<uint64_t>>> slices(positions.size(), std::vector<uint64_t>());
        for (size_t k = 0; k < positions.size(); ++k) {
            if (positions[k] < 0) continue;
            view.with_field_slice(vector_row, positions[k], [&](int32_t g0, int32_t g1) {
                std::vector<uint64_t> values;
                values.reserve(static_cast<size_t>(g1 - g0));
                for (int32_t g = g0; g < g1; ++g) values.push_back(view.u64(g));
                slices[k] = std::move(values);
            });
        }
        files[row] = std::move(slices);
    }

    // Empty column `column`'s slice of file `row`; whether it held anything.
    bool clear(size_t row, size_t column) {
        if (row >= files.size() || !files[row] || column >= files[row]->size()) return false;
        auto& slice = (*files[row])[column];
        const bool had = slice && !slice->empty();
        slice = std::vector<uint64_t>();
        return had;
    }

    bool any() const {
        for (const auto& f : files) {
            if (f) return true;
        }
        return false;
    }

    bool any_values() const {
        for (const auto& f : files) {
            if (!f) continue;
            for (const auto& slice : *f) {
                if (slice && !slice->empty()) return true;
            }
        }
        return false;
    }

    std::shared_ptr<const OwnedNested> build(size_t rows, DrakenType leaf_type) const {
        if (!any()) return nullptr;
        std::vector<std::vector<std::optional<std::vector<uint64_t>>>> per_file(rows);
        for (size_t f = 0; f < rows && f < files.size(); ++f) {
            if (files[f]) per_file[f] = *files[f];
        }
        return std::make_shared<const OwnedNested>(per_file, leaf_type);
    }
};

// A manifest's identity for memo keys: an allocation of its own, which a memo
// keeps alive by holding it - so no later manifest can come to share it, as a
// freed manifest's ADDRESS can. A copy is a new manifest and takes a new one.
class ManifestIdentity {
public:
    ManifestIdentity() : token_(std::make_shared<char>(0)) {}
    ManifestIdentity(const ManifestIdentity&) : token_(std::make_shared<char>(0)) {}
    ManifestIdentity& operator=(const ManifestIdentity&) {
        token_ = std::make_shared<char>(0);
        return *this;
    }
    const std::shared_ptr<const void>& token() const { return token_; }

private:
    std::shared_ptr<const void> token_;
};

class NativeManifest {
public:
    NativeManifest(std::vector<std::string> columns, bool bounds_are_ordinal, bool stats_are_authoritative)
        : columns_(std::move(columns)),
          bounds_are_ordinal_(bounds_are_ordinal),
          stats_are_authoritative_(stats_are_authoritative) {
        for (size_t k = 0; k < columns_.size(); ++k) positions_.emplace(columns_[k], k);
    }

    // File `row` of `src` as a new row here - the file and its cells - each
    // column k taking `src`'s column positions[k] (-1: nothing recorded). The
    // file's SKETCHES are not cells: the builder carries them alongside
    // (SketchStaging::carry_keyed), which is the only caller.
    size_t add_file_cells_from(const NativeManifest& src, size_t row, const std::vector<int64_t>& positions) {
        if (positions.size() != columns_.size()) throw std::invalid_argument("one source position per column");
        ManifestFile file = src.files_.at(row);
        file.vector_row = 0;
        const size_t added = add_file(std::move(file));
        for (size_t k = 0; k < columns_.size(); ++k) {
            if (positions[k] < 0) continue;
            cell(added, k) = src.cell(row, static_cast<size_t>(positions[k]));
        }
        return added;
    }

    // The row of the file at `path`, or -1.
    int64_t find_file(const std::string& path) const {
        for (size_t f = 0; f < files_.size(); ++f) {
            if (files_[f].path == path) return static_cast<int64_t>(f);
        }
        return -1;
    }

    // This manifest's identity token (see ManifestIdentity).
    const std::shared_ptr<const void>& identity() const { return identity_.token(); }

    // A column name's load-time position.
    const std::unordered_map<std::string, size_t>& positions() const { return positions_; }

    // A new file row with a cell per column; its index.
    size_t add_file(ManifestFile file) {
        files_.push_back(std::move(file));
        cells_.resize(files_.size() * columns_.size());
        return files_.size() - 1;
    }

    size_t file_count() const { return files_.size(); }
    size_t column_count() const { return columns_.size(); }
    const std::vector<std::string>& columns() const { return columns_; }
    // What the manifest's bounds hold: ordinal keys (ANALYZE manifests, skene
    // footers) or decoded values. One dialect per manifest, never mixed.
    bool bounds_are_ordinal() const { return bounds_are_ordinal_; }
    // Set by a producer that learns its dialect only while building (a skene
    // dataset is ordinal when any file bounds anything).
    void set_bounds_are_ordinal(bool ordinal) { bounds_are_ordinal_ = ordinal; }
    bool stats_are_authoritative() const { return stats_are_authoritative_; }

    // The whole-column sketch vectors, borrowed: the Python NativeManifest holds
    // the draken Vectors they view. Absent views are not present().
    NestedArrayView min_k;
    NestedArrayView histogram;
    NestedArrayView char_class;

    // Sketches this manifest owns (a builder's); the views above then point in.
    void own_sketches(std::shared_ptr<const OwnedNested> k, std::shared_ptr<const OwnedNested> h,
                      std::shared_ptr<const OwnedNested> c) {
        owned_min_k_ = std::move(k);
        owned_histogram_ = std::move(h);
        owned_char_class_ = std::move(c);
        if (owned_min_k_) min_k = owned_min_k_->view();
        if (owned_histogram_) histogram = owned_histogram_->view();
        if (owned_char_class_) char_class = owned_char_class_->view();
    }

    ManifestFile& file(size_t row) { return files_.at(row); }
    const ManifestFile& file(size_t row) const { return files_.at(row); }
    ManifestCell& cell(size_t row, size_t column) {
        return cells_.at(row * columns_.size() + column);
    }
    const ManifestCell& cell(size_t row, size_t column) const {
        return cells_.at(row * columns_.size() + column);
    }

    // LIVE rows (physical minus deleted), or kUnknown when any file's count is.
    int64_t record_count() const {
        int64_t total = 0;
        for (const ManifestFile& f : files_) {
            if (f.record_count == kUnknown) return kUnknown;
            total += f.record_count - f.deleted_record_count;
        }
        return total;
    }

    int64_t total_size() const {
        int64_t total = 0;
        for (const ManifestFile& f : files_) total += f.file_size;
        return total;
    }

    // The memory this manifest holds resident: its file rows and cells with
    // their heap strings and vectors, plus the sketch vectors it views (owned
    // or borrowed - either way this manifest keeps them alive). What a cache of
    // decoded manifests budgets by; raw manifest bytes are no proxy (a
    // compressible manifest decodes to 60-85x its size, an incompressible one
    // to 3.5x - measured 2026-09-27).
    size_t resident_bytes() const {
        size_t total = sizeof(*this) + columns_.size() * sizeof(std::string);
        for (const std::string& c : columns_) total += heap_bytes(c);
        total += files_.size() * sizeof(ManifestFile) + cells_.size() * sizeof(ManifestCell);
        for (const ManifestFile& f : files_) {
            total += heap_bytes(f.path) + heap_bytes(f.format) + heap_bytes(f.delete_file_path) +
                     f.delete_positions.capacity() * sizeof(int64_t);
        }
        for (const ManifestCell& c : cells_) {
            total += heap_bytes(c.bounds.min_text) + heap_bytes(c.bounds.max_text) +
                     heap_bytes(c.footer.bounds.min_text) + heap_bytes(c.footer.bounds.max_text) +
                     (c.distinct_sketch.capacity() + c.element_min_k.capacity()) * sizeof(uint64_t);
        }
        for (const NestedArrayView* v : {&min_k, &histogram, &char_class}) {
            if (!v->present()) continue;
            total += draken_vector_nbytes(v->outer) + draken_vector_nbytes(v->mid) + draken_vector_nbytes(v->leaf);
        }
        return total;
    }

    // A manifest over the files at `rows` (in that order), sharing nothing
    // mutable with this one: pruning is copy-on-write.
    NativeManifest subset(const std::vector<size_t>& rows) const {
        NativeManifest out(columns_, bounds_are_ordinal_, stats_are_authoritative_);
        out.min_k = min_k;
        out.histogram = histogram;
        out.char_class = char_class;
        out.owned_min_k_ = owned_min_k_;
        out.owned_histogram_ = owned_histogram_;
        out.owned_char_class_ = owned_char_class_;
        for (size_t row : rows) {
            size_t added = out.add_file(files_.at(row));
            for (size_t c = 0; c < columns_.size(); ++c) {
                out.cell(added, c) = cell(row, c);
            }
        }
        return out;
    }

private:
    std::shared_ptr<const OwnedNested> owned_min_k_, owned_histogram_, owned_char_class_;
    std::vector<std::string> columns_;
    std::unordered_map<std::string, size_t> positions_;
    bool bounds_are_ordinal_;
    bool stats_are_authoritative_;
    std::vector<ManifestFile> files_;
    std::vector<ManifestCell> cells_;   // files x columns, row-major
    ManifestIdentity identity_;
};

}  // namespace opteryx::planner
