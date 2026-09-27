// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// See the License at http://www.apache.org/licenses/LICENSE-2.0
// Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

// src/cpp/planner/manifest_decode.hpp — a manifest parquet's columns, as rugo
// decoded them into draken vectors, into a NativeManifest (native plan graph Q8).
//
// The manifest format is the one shared by the catalog's writer
// (opteryx_catalog.write_parquet_manifest) and core's (manifest_io): one row
// per data file, per-column statistics as arrays in the file's WRITE ORDER,
// which `field_ids` names. Each array element lands in the cell of the column
// at its LOAD-TIME position:
//   - when the schema the manifest is loaded against has field ids and the
//     row carries `field_ids`, through the field id;
//   - otherwise by position (core's own manifests write field id == position).
// An element whose column the schema does not have is dropped, as before.
//
// Bounds: `bounds_are_ordinal` says whether min_values/max_values hold int64
// ordinal keys (the catalog, ANALYZE, the stats refresh) or decoded values
// (external catalogs, a local store round trip). Either way the cell gets the
// ordinal key, and an integer / temporal / float column also its decoded
// value. INT64_MIN in an ordinal manifest is "no bound".

#pragma once

#include <cctype>
#include <cstdint>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <vector>

#include "core/buffers.h"
#include "core/string_slot.h"
#include "ops/ordinalize.h"
#include "planner/native_manifest.hpp"

namespace opteryx::planner {

// An ARRAY column: the outer vector (one row per file) and its child values
// (for an array<array<T>> column, the middle lists and their `grandchild` values).
struct ManifestArrayColumn {
    const DrakenVector* outer = nullptr;
    const DrakenVector* child = nullptr;
    const DrakenVector* grandchild = nullptr;
};

struct ManifestColumnsIn {
    const DrakenVector* file_path = nullptr;
    const DrakenVector* file_format = nullptr;
    const DrakenVector* record_count = nullptr;
    const DrakenVector* file_size = nullptr;
    const DrakenVector* uncompressed_size = nullptr;
    const DrakenVector* histogram_bins = nullptr;
    const DrakenVector* deleted_record_count = nullptr;   // optional column
    const DrakenVector* delete_file_path = nullptr;       // optional column
    ManifestArrayColumn column_uncompressed_sizes;
    ManifestArrayColumn null_counts;
    ManifestArrayColumn min_values;
    ManifestArrayColumn max_values;
    ManifestArrayColumn min_lengths;
    ManifestArrayColumn max_lengths;
    ManifestArrayColumn field_ids;
    ManifestArrayColumn char_total_bytes;
    ManifestArrayColumn distinct_counts;                  // optional column
    // ARRAY columns' element statistics - optional columns (the catalog's)
    ManifestArrayColumn element_min_values;
    ManifestArrayColumn element_max_values;
    ManifestArrayColumn element_min_k_hashes;             // array<array<uint64>>
};

struct ManifestSchemaIn {
    std::vector<std::string> columns;                     // load-time order
    std::vector<DrakenType> physical;                     // per column
    std::unordered_map<int64_t, size_t> position_of_field_id;   // empty: no field ids
    bool bounds_are_ordinal = true;
    bool stats_are_authoritative = false;
};

namespace manifest_detail {

inline bool valid_at(const DrakenVector* v, uint32_t index) {
    return v->validity == nullptr || ((v->validity[index >> 3] >> (index & 7u)) & 1u);
}

// The value at physical slot `k` of an integer vector.
inline int64_t int_at(const DrakenVector* v, uint32_t k) {
    switch (v->type) {
        case DRAKEN_INT8: return static_cast<const int8_t*>(v->data)[k];
        case DRAKEN_INT16: return static_cast<const int16_t*>(v->data)[k];
        case DRAKEN_INT32: return static_cast<const int32_t*>(v->data)[k];
        case DRAKEN_INT64: return static_cast<const int64_t*>(v->data)[k];
        case DRAKEN_UINT8: return static_cast<const uint8_t*>(v->data)[k];
        case DRAKEN_UINT16: return static_cast<const uint16_t*>(v->data)[k];
        case DRAKEN_UINT32: return static_cast<const uint32_t*>(v->data)[k];
        case DRAKEN_UINT64: return static_cast<int64_t>(static_cast<const uint64_t*>(v->data)[k]);
        default: throw std::invalid_argument("a manifest integer column holds a non-integer type");
    }
}

// Row `row` of a scalar integer column; false when NULL.
inline bool read_int(const DrakenVector* v, uint32_t row, int64_t& out) {
    if (v == nullptr || !valid_at(v, row)) return false;
    out = int_at(v, v->selection[row]);
    return true;
}

inline bool read_string(const DrakenVector* v, uint32_t row, std::string& out) {
    if (v == nullptr || !valid_at(v, row)) return false;
    const DrakenStringArena* arena = static_cast<const DrakenStringArena*>(v->data);
    const DrakenStringSlot* slot = &arena->slots[v->selection[row]];
    out.assign(reinterpret_cast<const char*>(str_data(slot, arena->arena)), str_length(slot));
    return true;
}

// Row `row` of an ARRAY column: the child index range; false when NULL.
inline bool array_range(const ManifestArrayColumn& col, uint32_t row, int32_t& begin, int32_t& end) {
    if (col.outer == nullptr || !valid_at(col.outer, row)) return false;
    const int32_t* offsets = static_cast<const int32_t*>(col.outer->data);
    uint32_t k = col.outer->selection[row];
    begin = offsets[k];
    end = offsets[k + 1];
    return true;
}

// Child element `g` of an integer ARRAY column; false when NULL.
inline bool child_int(const ManifestArrayColumn& col, int32_t g, int64_t& out) {
    uint32_t index = static_cast<uint32_t>(g);
    if (!valid_at(col.child, index)) return false;
    out = int_at(col.child, col.child->selection[index]);
    return true;
}

inline bool child_double(const ManifestArrayColumn& col, int32_t g, double& out) {
    uint32_t index = static_cast<uint32_t>(g);
    if (!valid_at(col.child, index)) return false;
    uint32_t k = col.child->selection[index];
    if (col.child->type == DRAKEN_FLOAT64) {
        out = static_cast<const double*>(col.child->data)[k];
    } else if (col.child->type == DRAKEN_FLOAT32) {
        out = static_cast<const float*>(col.child->data)[k];
    } else {
        out = static_cast<double>(int_at(col.child, k));
    }
    return true;
}

// The load-time position of each element of row `row`'s write-order lists
// (-1: the element is dropped). With a schema that has field ids:
//   - a row carrying `field_ids` keys each element by its id, and a list whose
//     length is not the id list's cannot be lined up, so ALL of it is dropped;
//   - a row with none was written in schema order: positional, and when every
//     schema column is keyed, only when the list covers exactly the schema's
//     columns - a partial list could belong to any of them.
// With no field ids (or only some), a row with no ids is positional.
// No stats is correct but slower; stats keyed by the wrong column is a wrong answer.
inline std::vector<int64_t> positions_of(const ManifestColumnsIn& in, const ManifestSchemaIn& schema,
                                         uint32_t row, size_t elements) {
    std::vector<int64_t> positions(elements, -1);
    const bool schema_keyed = !schema.position_of_field_id.empty();
    int32_t begin = 0, end = 0;
    const bool row_keyed = schema_keyed && array_range(in.field_ids, row, begin, end) && end > begin;
    if (row_keyed) {
        if (static_cast<size_t>(end - begin) != elements) return positions;
        for (size_t j = 0; j < elements; ++j) {
            int64_t field_id = 0;
            if (!child_int(in.field_ids, begin + static_cast<int32_t>(j), field_id)) continue;
            auto found = schema.position_of_field_id.find(field_id);
            if (found != schema.position_of_field_id.end()) positions[j] = static_cast<int64_t>(found->second);
        }
        return positions;
    }
    if (schema.position_of_field_id.size() == schema.columns.size() && schema_keyed &&
        elements != schema.columns.size()) {
        return positions;
    }
    for (size_t j = 0; j < elements && j < schema.columns.size(); ++j) positions[j] = static_cast<int64_t>(j);
    return positions;
}

// Each element of row `row` of an integer ARRAY column, into cell field `field`.
template <typename Store>
inline void scatter_ints(NativeManifest& manifest, size_t file, const ManifestColumnsIn& in,
                         const ManifestSchemaIn& schema, const ManifestArrayColumn& col,
                         uint32_t row, Store store) {
    int32_t begin = 0, end = 0;
    if (col.outer == nullptr || !array_range(col, row, begin, end)) return;
    std::vector<int64_t> positions = positions_of(in, schema, row, static_cast<size_t>(end - begin));
    for (int32_t g = begin; g < end; ++g) {
        int64_t position = positions[static_cast<size_t>(g - begin)];
        int64_t value = 0;
        if (position < 0 || !child_int(col, g, value)) continue;
        store(manifest.cell(file, static_cast<size_t>(position)), value);
    }
}

// Each element of row `row` of an array<array<uint64>> column - one hash list
// per column - into cell field `field`. A null list stays unset.
template <typename Store>
inline void scatter_hash_lists(NativeManifest& manifest, size_t file, const ManifestColumnsIn& in,
                               const ManifestSchemaIn& schema, const ManifestArrayColumn& col,
                               uint32_t row, Store store) {
    int32_t begin = 0, end = 0;
    if (col.outer == nullptr || col.grandchild == nullptr || !array_range(col, row, begin, end)) return;
    std::vector<int64_t> positions = positions_of(in, schema, row, static_cast<size_t>(end - begin));
    const int32_t* middle = static_cast<const int32_t*>(col.child->data);
    for (int32_t g = begin; g < end; ++g) {
        const int64_t position = positions[static_cast<size_t>(g - begin)];
        if (position < 0 || !valid_at(col.child, static_cast<uint32_t>(g))) continue;
        const uint32_t k = col.child->selection[static_cast<uint32_t>(g)];
        std::vector<uint64_t> hashes;
        for (int32_t h = middle[k]; h < middle[k + 1]; ++h) {
            if (!valid_at(col.grandchild, static_cast<uint32_t>(h))) continue;
            hashes.push_back(static_cast<uint64_t>(int_at(col.grandchild, col.grandchild->selection[static_cast<uint32_t>(h)])));
        }
        store(manifest.cell(file, static_cast<size_t>(position)), std::move(hashes));
    }
}

// min_values or max_values of row `row`: the ordinal key and, where the column
// has one, the decoded value.
inline void scatter_bounds(NativeManifest& manifest, size_t file, const ManifestColumnsIn& in,
                           const ManifestSchemaIn& schema, const ManifestArrayColumn& col,
                           uint32_t row, bool is_min) {
    int32_t begin = 0, end = 0;
    if (col.outer == nullptr || !array_range(col, row, begin, end)) return;
    std::vector<int64_t> positions = positions_of(in, schema, row, static_cast<size_t>(end - begin));
    bool child_is_float = is_float(col.child->type);
    for (int32_t g = begin; g < end; ++g) {
        int64_t position = positions[static_cast<size_t>(g - begin)];
        if (position < 0) continue;
        ManifestCell& cell = manifest.cell(file, static_cast<size_t>(position));
        DrakenType physical = schema.physical[static_cast<size_t>(position)];
        if (schema.bounds_are_ordinal) {
            int64_t ordinal = 0;
            if (!child_int(col, g, ordinal) || ordinal == kNoBound) continue;
            set_ordinal_bound(cell.bounds, physical, is_min, ordinal);
        } else if (child_is_float || is_float(physical)) {
            double value = 0.0;
            if (!child_double(col, g, value)) continue;
            set_double_bound(cell.bounds, is_min, value);
        } else {
            int64_t value = 0;
            if (!child_int(col, g, value)) continue;
            set_int_bound(cell.bounds, physical, is_min, value);
        }
    }
}

}  // namespace manifest_detail

inline NativeManifest decode_manifest(const ManifestColumnsIn& in, const ManifestSchemaIn& schema,
                                      uint32_t rows) {
    using namespace manifest_detail;
    if (schema.physical.size() != schema.columns.size()) {
        throw std::invalid_argument("manifest schema: one physical type per column");
    }
    NativeManifest manifest(schema.columns, schema.bounds_are_ordinal, schema.stats_are_authoritative);
    for (uint32_t row = 0; row < rows; ++row) {
        ManifestFile file;
        read_string(in.file_path, row, file.path);
        read_string(in.file_format, row, file.format);
        // A format is a case-insensitive name: the catalog writes "parquet",
        // core writes "PARQUET"; the engine's vocabulary is upper case.
        for (char& c : file.format) c = static_cast<char>(std::toupper(static_cast<unsigned char>(c)));
        read_int(in.record_count, row, file.record_count);
        read_int(in.file_size, row, file.file_size);
        read_int(in.uncompressed_size, row, file.uncompressed_size);
        int64_t bins = 0;
        if (read_int(in.histogram_bins, row, bins) && bins != 0) {
            file.histogram_bins = bins;   // 0 is the writer's "no histogram"
        }
        read_int(in.deleted_record_count, row, file.deleted_record_count);
        read_string(in.delete_file_path, row, file.delete_file_path);
        file.vector_row = row;
        size_t f = manifest.add_file(std::move(file));

        scatter_bounds(manifest, f, in, schema, in.min_values, row, true);
        scatter_bounds(manifest, f, in, schema, in.max_values, row, false);
        scatter_ints(manifest, f, in, schema, in.null_counts, row,
                     [](ManifestCell& c, int64_t v) { c.null_count = v; });
        scatter_ints(manifest, f, in, schema, in.min_lengths, row,
                     [](ManifestCell& c, int64_t v) { c.min_length = v; });
        scatter_ints(manifest, f, in, schema, in.max_lengths, row,
                     [](ManifestCell& c, int64_t v) { c.max_length = v; });
        scatter_ints(manifest, f, in, schema, in.char_total_bytes, row,
                     [](ManifestCell& c, int64_t v) { c.char_total_bytes = v; });
        scatter_ints(manifest, f, in, schema, in.column_uncompressed_sizes, row,
                     [](ManifestCell& c, int64_t v) { c.uncompressed_size = v; });
        // INT64_MIN is the catalog's "no bound" here as in min_values
        scatter_ints(manifest, f, in, schema, in.element_min_values, row, [](ManifestCell& c, int64_t v) {
            if (v != kNoBound) c.element_min = v;
        });
        scatter_ints(manifest, f, in, schema, in.element_max_values, row, [](ManifestCell& c, int64_t v) {
            if (v != kNoBound) c.element_max = v;
        });
        scatter_hash_lists(manifest, f, in, schema, in.element_min_k_hashes, row,
                           [](ManifestCell& c, std::vector<uint64_t> v) { c.element_min_k = std::move(v); });
        scatter_ints(manifest, f, in, schema, in.distinct_counts, row, [](ManifestCell& c, int64_t v) {
            c.distinct_count = v;
            c.distinct_exact = false;   // the manifest does not persist exactness
        });
    }
    return manifest;
}

// decode_manifest onto the heap, for an owner that holds it by pointer.
inline NativeManifest* new_decoded_manifest(const ManifestColumnsIn& in, const ManifestSchemaIn& schema,
                                            uint32_t rows) {
    return new NativeManifest(decode_manifest(in, schema, rows));
}

}  // namespace opteryx::planner
