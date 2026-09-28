// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// See the License at http://www.apache.org/licenses/LICENSE-2.0
// Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

// src/cpp/planner/manifest_encode.hpp — a NativeManifest as the manifest
// parquet's columns (native plan graph Q8, writers native; architect rulings
// 2026-09-27 (4)).
//
// The inverse of manifest_decode.hpp: every column of the manifest format
// (opteryx/models/manifest_io.py `_MANIFEST_COLUMNS`, shared with the catalog),
// filled from the native rows into draken_malloc'd buffers that the draken
// bridge adopts as Vectors (see native_manifest.pyx), and rugo writes. Bounds
// are written in the ORDINAL dialect, for every column; `field_ids` is the
// catalog's ids when given, else the load-time position. A per-column list a
// file tracks nothing for is written
// EMPTY (the format's "not tracked"), otherwise positionally with a null for
// each unknown column - never a zero.

#pragma once

#include <cstdint>
#include <cstring>
#include <new>
#include <stdexcept>
#include <string>
#include <vector>

#include "core/alloc.h"
#include "core/buffers.h"
#include "core/string_slot.h"
#include "planner/native_manifest.hpp"

namespace opteryx::planner {

// A column of scalars: `data` holds `rows` values, `validity` 1 bit per row
// (nullptr: all valid).
struct EncodedScalar {
    void* data = nullptr;
    uint8_t* validity = nullptr;
};

struct EncodedStrings {
    DrakenStringSlot* slots = nullptr;
    uint8_t* arena = nullptr;
    size_t arena_len = 0;
};

// A list column: depth 1 (array<T>) or 2 (array<array<T>>). Rows are never
// null; a null middle row (depth 2) or leaf is marked in its validity.
struct EncodedList {
    int depth = 1;
    int32_t* offsets = nullptr;         // rows + 1
    int32_t* mid_offsets = nullptr;     // middle rows + 1 (depth 2)
    uint8_t* mid_validity = nullptr;
    uint32_t mid_length = 0;
    void* leaf = nullptr;
    uint8_t* leaf_validity = nullptr;
    uint32_t leaf_length = 0;
    DrakenType leaf_type = DRAKEN_INT64;
};

struct EncodedManifest {
    uint32_t rows = 0;
    EncodedStrings file_path, file_format;
    EncodedStrings delete_file_path;          // "" on a row without deletes (null there)
    uint8_t* delete_file_path_validity = nullptr;
    EncodedScalar deleted_record_count;
    EncodedScalar record_count, file_size, uncompressed_size, histogram_bins;
    EncodedList column_sizes, null_counts, min_k, histogram_counts, min_values, max_values, field_ids;
    EncodedList min_lengths, max_lengths, char_class_counts, char_total_bytes, distinct_counts;
    EncodedList element_min_values, element_max_values, element_min_k_hashes;
    // The cells' exact sums (ManifestCell::sum, int128) as their two int64
    // halves: sums_hi = the high 64 bits, sums_lo = the low 64 bits' pattern.
    EncodedList sums_hi, sums_lo;
};

namespace encode_detail {

template <typename T>
inline T* alloc(size_t n) {
    void* p = draken_malloc(n == 0 ? sizeof(T) : n * sizeof(T));
    if (p == nullptr) throw std::bad_alloc();
    return static_cast<T*>(p);
}

inline uint8_t* bits(size_t n) {
    uint8_t* v = alloc<uint8_t>((n + 7) / 8);
    std::memset(v, 0, (n + 7) / 8 == 0 ? 1 : (n + 7) / 8);
    return v;
}

inline void set_bit(uint8_t* v, size_t k) { v[k >> 3] |= static_cast<uint8_t>(1u << (k & 7)); }

inline EncodedStrings strings(const std::vector<const std::string*>& values) {
    EncodedStrings out;
    out.slots = alloc<DrakenStringSlot>(values.size());
    for (const std::string* v : values) {
        if (v->size() > STR_INLINE_MAX) out.arena_len += v->size();
    }
    out.arena = alloc<uint8_t>(out.arena_len);
    size_t at = 0;
    for (size_t k = 0; k < values.size(); ++k) {
        const std::string& v = *values[k];
        const auto* src = reinterpret_cast<const uint8_t*>(v.data());
        if (v.size() <= STR_INLINE_MAX) {
            str_init_inline(&out.slots[k], src, static_cast<uint32_t>(v.size()));
        } else {
            std::memcpy(out.arena + at, v.data(), v.size());
            str_init_extern(&out.slots[k], out.arena + at, static_cast<uint32_t>(v.size()), static_cast<uint32_t>(at));
            at += v.size();
        }
    }
    return out;
}

// int64 scalars, `kUnknown` as null.
inline EncodedScalar int64s(const std::vector<int64_t>& values, bool unknown_is_null) {
    EncodedScalar out;
    auto* data = alloc<int64_t>(values.size());
    for (size_t k = 0; k < values.size(); ++k) {
        data[k] = values[k];
        if (unknown_is_null && values[k] == kUnknown) {
            if (out.validity == nullptr) {
                out.validity = bits(values.size());
                for (size_t j = 0; j < values.size(); ++j) set_bit(out.validity, j);
            }
            out.validity[k >> 3] &= static_cast<uint8_t>(~(1u << (k & 7)));
            data[k] = 0;
        }
    }
    out.data = data;
    return out;
}

// A depth-1 int64 list per row: each row either EMPTY (nothing tracked) or one
// value per column, `absent` marking a null element.
inline EncodedList positional(const NativeManifest& m, int64_t absent,
                              int64_t (*read)(const NativeManifest&, size_t, size_t)) {
    const size_t rows = m.file_count(), columns = m.column_count();
    std::vector<uint8_t> tracked(rows, 0);
    size_t leaf_length = 0;
    for (size_t f = 0; f < rows; ++f) {
        for (size_t c = 0; c < columns; ++c) {
            if (read(m, f, c) != absent) { tracked[f] = 1; break; }
        }
        if (tracked[f]) leaf_length += columns;
    }
    EncodedList out;
    out.depth = 1;
    out.offsets = alloc<int32_t>(rows + 1);
    auto* leaf = alloc<int64_t>(leaf_length);
    out.leaf_validity = bits(leaf_length);
    size_t at = 0;
    out.offsets[0] = 0;
    for (size_t f = 0; f < rows; ++f) {
        if (tracked[f]) {
            for (size_t c = 0; c < columns; ++c, ++at) {
                const int64_t v = read(m, f, c);
                leaf[at] = v == absent ? 0 : v;
                if (v != absent) set_bit(out.leaf_validity, at);
            }
        }
        out.offsets[f + 1] = static_cast<int32_t>(at);
    }
    out.leaf = leaf;
    out.leaf_length = static_cast<uint32_t>(leaf_length);
    out.leaf_type = DRAKEN_INT64;
    return out;
}

// positional() for a statistic whose every int64 value is legal, so presence
// cannot be a sentinel: a file is EMPTY unless some column `has` it, else one
// element per column, null where the column does not.
inline EncodedList positional_where(const NativeManifest& m, bool (*has)(const ManifestCell&),
                                    int64_t (*read)(const ManifestCell&)) {
    const size_t rows = m.file_count(), columns = m.column_count();
    std::vector<uint8_t> tracked(rows, 0);
    size_t leaf_length = 0;
    for (size_t f = 0; f < rows; ++f) {
        for (size_t c = 0; c < columns; ++c) {
            if (has(m.cell(f, c))) { tracked[f] = 1; break; }
        }
        if (tracked[f]) leaf_length += columns;
    }
    EncodedList out;
    out.depth = 1;
    out.offsets = alloc<int32_t>(rows + 1);
    auto* leaf = alloc<int64_t>(leaf_length);
    out.leaf_validity = bits(leaf_length);
    size_t at = 0;
    out.offsets[0] = 0;
    for (size_t f = 0; f < rows; ++f) {
        if (tracked[f]) {
            for (size_t c = 0; c < columns; ++c, ++at) {
                const ManifestCell& cell = m.cell(f, c);
                const bool present = has(cell);
                leaf[at] = present ? read(cell) : 0;
                if (present) set_bit(out.leaf_validity, at);
            }
        }
        out.offsets[f + 1] = static_cast<int32_t>(at);
    }
    out.leaf = leaf;
    out.leaf_length = static_cast<uint32_t>(leaf_length);
    out.leaf_type = DRAKEN_INT64;
    return out;
}

// A depth-2 sketch column copied out of the manifest's view of it: each file's
// per-column slices, or an empty list for a file without them.
inline EncodedList nested(const NativeManifest& m, const NestedArrayView& v, DrakenType leaf_type) {
    const size_t rows = m.file_count(), columns = m.column_count();
    std::vector<int32_t> offsets{0}, mid_offsets{0};
    std::vector<uint8_t> mid_valid;
    std::vector<uint64_t> leaf;
    std::vector<uint8_t> leaf_valid;
    for (size_t f = 0; f < rows; ++f) {
        const uint32_t vector_row = m.file(f).vector_row;
        const bool has = v.present() && vector_row < v.n_files() &&
                         sketch_bit_valid(v.outer->validity, vector_row);
        if (has) {
            const int32_t* poff = static_cast<const int32_t*>(v.outer->data);
            const uint32_t pi = v.outer->selection[vector_row];
            const int64_t width = static_cast<int64_t>(poff[pi + 1]) - poff[pi];
            for (int64_t c = 0; c < width; ++c) {
                bool valid_slice = false;
                v.with_field_slice(vector_row, c, [&](int32_t g0, int32_t g1) {
                    valid_slice = true;
                    for (int32_t g = g0; g < g1; ++g) {
                        leaf.push_back(v.u64(g));
                        leaf_valid.push_back(v.leaf_valid(g) ? 1 : 0);
                    }
                });
                mid_valid.push_back(valid_slice ? 1 : 0);
                mid_offsets.push_back(static_cast<int32_t>(leaf.size()));
            }
        }
        offsets.push_back(static_cast<int32_t>(mid_offsets.size() - 1));
    }
    (void)columns;
    EncodedList out;
    out.depth = 2;
    out.offsets = alloc<int32_t>(offsets.size());
    std::memcpy(out.offsets, offsets.data(), offsets.size() * sizeof(int32_t));
    out.mid_offsets = alloc<int32_t>(mid_offsets.size());
    std::memcpy(out.mid_offsets, mid_offsets.data(), mid_offsets.size() * sizeof(int32_t));
    out.mid_length = static_cast<uint32_t>(mid_valid.size());
    out.mid_validity = bits(mid_valid.size());
    for (size_t k = 0; k < mid_valid.size(); ++k) if (mid_valid[k]) set_bit(out.mid_validity, k);
    auto* leaf_data = alloc<uint64_t>(leaf.size());
    if (!leaf.empty()) std::memcpy(leaf_data, leaf.data(), leaf.size() * sizeof(uint64_t));
    out.leaf = leaf_data;
    out.leaf_length = static_cast<uint32_t>(leaf.size());
    out.leaf_validity = bits(leaf.size());
    for (size_t k = 0; k < leaf_valid.size(); ++k) if (leaf_valid[k]) set_bit(out.leaf_validity, k);
    out.leaf_type = leaf_type;
    return out;
}

// A depth-2 uint64 list per file from the cells' per-column hash lists: EMPTY
// for a file none of whose columns holds one, else one list per column.
inline EncodedList cell_hash_lists(const NativeManifest& m,
                                   const std::vector<uint64_t>& (*read)(const ManifestCell&)) {
    const size_t rows = m.file_count(), columns = m.column_count();
    std::vector<int32_t> offsets{0}, mid_offsets{0};
    std::vector<uint64_t> leaf;
    for (size_t f = 0; f < rows; ++f) {
        bool tracked = false;
        for (size_t c = 0; c < columns && !tracked; ++c) tracked = !read(m.cell(f, c)).empty();
        if (tracked) {
            for (size_t c = 0; c < columns; ++c) {
                const std::vector<uint64_t>& hashes = read(m.cell(f, c));
                leaf.insert(leaf.end(), hashes.begin(), hashes.end());
                mid_offsets.push_back(static_cast<int32_t>(leaf.size()));
            }
        }
        offsets.push_back(static_cast<int32_t>(mid_offsets.size() - 1));
    }
    EncodedList out;
    out.depth = 2;
    out.offsets = alloc<int32_t>(offsets.size());
    std::memcpy(out.offsets, offsets.data(), offsets.size() * sizeof(int32_t));
    out.mid_offsets = alloc<int32_t>(mid_offsets.size());
    std::memcpy(out.mid_offsets, mid_offsets.data(), mid_offsets.size() * sizeof(int32_t));
    out.mid_length = static_cast<uint32_t>(mid_offsets.size() - 1);
    out.mid_validity = bits(out.mid_length);
    for (uint32_t k = 0; k < out.mid_length; ++k) set_bit(out.mid_validity, k);
    auto* leaf_data = alloc<uint64_t>(leaf.size());
    if (!leaf.empty()) std::memcpy(leaf_data, leaf.data(), leaf.size() * sizeof(uint64_t));
    out.leaf = leaf_data;
    out.leaf_length = static_cast<uint32_t>(leaf.size());
    out.leaf_validity = bits(leaf.size());
    for (size_t k = 0; k < leaf.size(); ++k) set_bit(out.leaf_validity, k);
    out.leaf_type = DRAKEN_UINT64;
    return out;
}

// `_histogram_bins_of`: the one width every non-empty histogram slice of the
// file shares, else 0.
inline int64_t histogram_bins(const NativeManifest& m, size_t f) {
    const NestedArrayView& v = m.histogram;
    const uint32_t vector_row = m.file(f).vector_row;
    if (!v.present() || vector_row >= v.n_files() || !sketch_bit_valid(v.outer->validity, vector_row)) return 0;
    const int32_t* poff = static_cast<const int32_t*>(v.outer->data);
    const uint32_t pi = v.outer->selection[vector_row];
    int64_t width = 0;
    for (int64_t c = 0; c < static_cast<int64_t>(poff[pi + 1]) - poff[pi]; ++c) {
        int64_t n = 0;
        v.with_field_slice(vector_row, c, [&](int32_t g0, int32_t g1) { n = g1 - g0; });
        if (n == 0) continue;
        if (width == 0) width = n;
        else if (width != n) return 0;
    }
    return width;
}

}  // namespace encode_detail

// Every manifest column of `m`, in draken_malloc'd buffers the caller hands to
// the draken bridge (which takes ownership). `field_ids`, one per column, keys
// each row's lists (-1: a column with no id, written null); empty keys them by
// load-time position - core's own manifests. A catalog's manifest carries the
// catalog's ids.
inline EncodedManifest encode_manifest(const NativeManifest& m, const std::vector<int64_t>& field_ids) {
    using namespace encode_detail;
    EncodedManifest out;
    const size_t rows = m.file_count();
    out.rows = static_cast<uint32_t>(rows);

    std::vector<const std::string*> paths, formats;
    std::vector<int64_t> records, sizes, uncompressed, bins;
    for (size_t f = 0; f < rows; ++f) {
        const ManifestFile& file = m.file(f);
        paths.push_back(&file.path);
        formats.push_back(&file.format);
        records.push_back(file.record_count);
        sizes.push_back(file.file_size);
        uncompressed.push_back(file.uncompressed_size);
        bins.push_back(histogram_bins(m, f));
    }
    // merge-on-read deletes (the catalog's two trailing columns): a null path
    // IS "no deletes", and the count is 0 there
    std::vector<const std::string*> delete_paths;
    std::vector<int64_t> deleted;
    out.delete_file_path_validity = bits(rows);
    for (size_t f = 0; f < rows; ++f) {
        const ManifestFile& file = m.file(f);
        delete_paths.push_back(&file.delete_file_path);
        deleted.push_back(file.deleted_record_count);
        if (!file.delete_file_path.empty()) set_bit(out.delete_file_path_validity, f);
    }
    out.delete_file_path = strings(delete_paths);
    out.deleted_record_count = int64s(deleted, false);
    out.file_path = strings(paths);
    out.file_format = strings(formats);
    out.record_count = int64s(records, true);
    out.file_size = int64s(sizes, false);
    out.uncompressed_size = int64s(uncompressed, true);
    out.histogram_bins = int64s(bins, false);

    out.column_sizes = positional(m, kUnknown, [](const NativeManifest& n, size_t f, size_t c) {
        return n.cell(f, c).uncompressed_size;
    });
    out.null_counts = positional(m, kUnknown, [](const NativeManifest& n, size_t f, size_t c) {
        return n.cell(f, c).null_count;
    });
    out.min_values = positional(m, kNoBound, [](const NativeManifest& n, size_t f, size_t c) {
        return n.cell(f, c).bounds.min_ordinal;
    });
    out.max_values = positional(m, kNoBound, [](const NativeManifest& n, size_t f, size_t c) {
        return n.cell(f, c).bounds.max_ordinal;
    });
    out.min_lengths = positional(m, kUnknown, [](const NativeManifest& n, size_t f, size_t c) {
        return n.cell(f, c).min_length;
    });
    out.max_lengths = positional(m, kUnknown, [](const NativeManifest& n, size_t f, size_t c) {
        return n.cell(f, c).max_length;
    });
    out.char_total_bytes = positional(m, kUnknown, [](const NativeManifest& n, size_t f, size_t c) {
        return n.cell(f, c).char_total_bytes;
    });
    out.distinct_counts = positional(m, kUnknown, [](const NativeManifest& n, size_t f, size_t c) {
        return n.cell(f, c).distinct_count;
    });

    out.element_min_values = positional(m, kNoBound, [](const NativeManifest& n, size_t f, size_t c) {
        return n.cell(f, c).element_min;
    });
    out.element_max_values = positional(m, kNoBound, [](const NativeManifest& n, size_t f, size_t c) {
        return n.cell(f, c).element_max;
    });
    out.element_min_k_hashes = cell_hash_lists(m, [](const ManifestCell& c) -> const std::vector<uint64_t>& {
        return c.element_min_k;
    });
    out.sums_hi = positional_where(
        m, [](const ManifestCell& c) { return c.has_sum; },
        [](const ManifestCell& c) { return static_cast<int64_t>(c.sum >> 64); });
    out.sums_lo = positional_where(
        m, [](const ManifestCell& c) { return c.has_sum; },
        [](const ManifestCell& c) {
            return static_cast<int64_t>(static_cast<uint64_t>(static_cast<unsigned __int128>(c.sum)));
        });

    // field_ids, on every row: the given ids, or the load-time positions
    const size_t columns = m.column_count();
    if (!field_ids.empty() && field_ids.size() != columns) {
        throw std::invalid_argument("encode_manifest: one field id per column");
    }
    out.field_ids.depth = 1;
    out.field_ids.offsets = alloc<int32_t>(rows + 1);
    auto* ids = alloc<int64_t>(rows * columns);
    out.field_ids.leaf_validity = bits(rows * columns);
    for (size_t f = 0; f <= rows; ++f) out.field_ids.offsets[f] = static_cast<int32_t>(f * columns);
    for (size_t k = 0; k < rows * columns; ++k) {
        const int64_t id = field_ids.empty() ? static_cast<int64_t>(k % columns) : field_ids[k % columns];
        ids[k] = id < 0 ? 0 : id;
        if (id >= 0) set_bit(out.field_ids.leaf_validity, k);
    }
    out.field_ids.leaf = ids;
    out.field_ids.leaf_length = static_cast<uint32_t>(rows * columns);

    out.min_k = nested(m, m.min_k, DRAKEN_UINT64);
    out.histogram_counts = nested(m, m.histogram, DRAKEN_INT64);
    out.char_class_counts = nested(m, m.char_class, DRAKEN_INT64);
    return out;
}

}  // namespace opteryx::planner
