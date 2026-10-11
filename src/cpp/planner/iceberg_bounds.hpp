// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// See the License at http://www.apache.org/licenses/LICENSE-2.0
// Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

// src/cpp/planner/iceberg_bounds.hpp — an Iceberg data file's column bounds and
// null counts, as rugo's Avro reader renders them, into the opteryx manifest's
// per-file lists: field_ids, ordinal min/max keys, null counts.
//
// Input, per manifest entry: `data_file.lower_bounds` / `upper_bounds` /
// `null_value_counts`, Iceberg map<int, *> stored as array<record{key, value}>,
// which rugo renders as JSON text `[{"key":1,"value":...},...]` (binary values
// base64). Parsed here with yyjson and decoded with mabel base64, both resolved
// from draken_native at load.
//
// A bound is Iceberg's single-value binary serialisation (Iceberg spec, Appendix
// D) of the column's type, turned into the same int64 key ColumnType.ordinalize
// gives the bound's decoded value (draken/ops/ordinalize.h) — the result the
// Python path (pyiceberg `from_bytes` + `ColumnType.ordinalize`) produced:
//   BOOL                1 byte, nonzero = true
//   INT32 / DATE32      4 bytes little-endian (days for a date)
//   INT64 / TIMESTAMP64 8 bytes little-endian (microseconds for a timestamp)
//   FLOAT32 / FLOAT64   4 / 8 bytes IEEE 754 little-endian
//   VARCHAR / VARBINARY the bytes; 8-byte prefix key. A string bound must be
//                       valid UTF-8 (Python's rule, as `from_bytes` decoded it)
//   DECIMAL             big-endian two's-complement unscaled value, at the
//                       column's own scale, so the unscaled value IS the key
//   DECIMAL128          refused — draken has no ordinal key for it (as
//                       ColumnType.ordinalize refuses)
// A wrong-length value or a string bound that is not UTF-8 is refused, as
// `from_bytes` refuses them. Field ids are the
// sorted union of both bound maps' keys; a field the type table does not carry
// (no ordinal type, or not a top-level column) has no bound. Within one map the
// last entry for a key wins, as building a dict from it does.

#pragma once

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "base64/_base64.h"
#include "core/buffers.h"
#include "core/string_slot.h"
#include "ops/ordinalize.h"
#include "planner/py_numeric.hpp"
#include "yyjson.h"

namespace opteryx::planner::iceberg {

using TypeTable = std::unordered_map<int32_t, DrakenType>;

struct FileBounds {
    std::vector<int32_t> field_ids;
    std::vector<int64_t> min_values;
    std::vector<int64_t> max_values;
    std::vector<int64_t> null_counts;
    std::vector<uint8_t> has_min;
    std::vector<uint8_t> has_max;
    std::vector<uint8_t> has_null_count;
};

namespace detail {

[[noreturn]] inline void corrupt(const std::string& what) {
    throw std::invalid_argument("Iceberg manifest: " + what);
}

// Row `row` of a string vector (the JSON text); false when NULL.
inline bool text_at(const DrakenVector* v, uint32_t row, const char*& p, size_t& n) {
    if (v->validity != nullptr && !((v->validity[row >> 3] >> (row & 7u)) & 1u)) return false;
    const DrakenStringArena* arena = static_cast<const DrakenStringArena*>(v->data);
    const DrakenStringSlot* slot = &arena->slots[v->selection[row]];
    p = reinterpret_cast<const char*>(str_data(slot, arena->arena));
    n = str_length(slot);
    return true;
}

struct Doc {
    yyjson_doc* doc = nullptr;
    ~Doc() { if (doc != nullptr) yyjson_doc_free(doc); }
};

// The map's {"key","value"} entries; null text = empty.
inline yyjson_val* parse_map(Doc& d, const char* p, size_t n) {
    d.doc = yyjson_read_opts(const_cast<char*>(p), n, 0, nullptr, nullptr);
    if (d.doc == nullptr) corrupt("a bounds map is not valid JSON");
    yyjson_val* root = yyjson_doc_get_root(d.doc);
    if (!yyjson_is_arr(root)) corrupt("a bounds map is not a JSON array");
    return root;
}

inline int32_t entry_key(yyjson_val* entry) {
    yyjson_val* key = yyjson_obj_get(entry, "key");
    if (key == nullptr || !yyjson_is_int(key)) corrupt("a bounds map entry has no integer key");
    const int64_t k = yyjson_get_sint(key);
    if (k < INT32_MIN || k > INT32_MAX) corrupt("a bounds map key is out of the field-id range");
    return static_cast<int32_t>(k);
}

// {key: decoded bytes}, sorted by key, last entry per key kept.
inline void binary_map(const DrakenVector* v, uint32_t row,
                       std::vector<std::pair<int32_t, std::string>>& out) {
    out.clear();
    const char* p;
    size_t n;
    if (v == nullptr || !text_at(v, row, p, n)) return;
    Doc d;
    yyjson_val* root = parse_map(d, p, n);
    size_t i, max;
    yyjson_val* entry;
    yyjson_arr_foreach(root, i, max, entry) {
        const int32_t key = entry_key(entry);
        yyjson_val* value = yyjson_obj_get(entry, "value");
        if (value == nullptr || !yyjson_is_str(value)) corrupt("a bound is not base64 text");
        const char* b64 = yyjson_get_str(value);
        const size_t b64_len = yyjson_get_len(value);
        std::string bytes(b64_decoded_size(b64_len), '\0');
        void* end = b64tobin_len(bytes.data(), b64, b64_len);
        if (end == nullptr) corrupt("a bound is not valid base64");
        bytes.resize(static_cast<size_t>(static_cast<char*>(end) - bytes.data()));
        out.emplace_back(key, std::move(bytes));
    }
    std::stable_sort(out.begin(), out.end(),
                     [](const auto& a, const auto& b) { return a.first < b.first; });
    size_t w = 0;
    for (size_t r = 0; r < out.size(); ++r) {
        if (r + 1 < out.size() && out[r + 1].first == out[r].first) continue;
        if (w != r) out[w] = std::move(out[r]);
        ++w;
    }
    out.resize(w);
}

// {key: count}, last entry per key kept.
inline void count_map(const DrakenVector* v, uint32_t row, std::unordered_map<int32_t, int64_t>& out) {
    out.clear();
    const char* p;
    size_t n;
    if (v == nullptr || !text_at(v, row, p, n)) return;
    Doc d;
    yyjson_val* root = parse_map(d, p, n);
    size_t i, max;
    yyjson_val* entry;
    yyjson_arr_foreach(root, i, max, entry) {
        const int32_t key = entry_key(entry);
        yyjson_val* value = yyjson_obj_get(entry, "value");
        if (value == nullptr || !yyjson_is_int(value)) corrupt("a null count is not an integer");
        out[key] = yyjson_get_sint(value);
    }
}

template <typename T>
inline T little_endian(const std::string& b, int32_t field_id) {
    if (b.size() != sizeof(T))
        corrupt("the bound for field " + std::to_string(field_id) + " is " +
                std::to_string(b.size()) + " bytes; its type takes " + std::to_string(sizeof(T)));
    T v;
    std::memcpy(&v, b.data(), sizeof(T));  // little-endian on every supported target
    return v;
}

}  // namespace detail

// The ordinal key of Iceberg bound `b` for a column of physical type `type`.
inline int64_t ordinal(DrakenType type, const std::string& b, int32_t field_id) {
    using namespace draken::ops;
    switch (type) {
        case DRAKEN_BOOL:
            return ordinalize_scalar_widen<int64_t>(detail::little_endian<uint8_t>(b, field_id) != 0);
        case DRAKEN_INT32:
        case DRAKEN_DATE32:
            return ordinalize_scalar_widen<int32_t>(detail::little_endian<int32_t>(b, field_id));
        case DRAKEN_INT64:
        case DRAKEN_TIMESTAMP64:
            return ordinalize_scalar_widen<int64_t>(detail::little_endian<int64_t>(b, field_id));
        case DRAKEN_FLOAT32:
            return ordinalize_scalar_f32(detail::little_endian<float>(b, field_id));
        case DRAKEN_FLOAT64:
            return ordinalize_scalar_f64(detail::little_endian<double>(b, field_id));
        case DRAKEN_VARCHAR: {
            std::vector<uint32_t> code_points;
            if (!py::utf8_code_points(b, code_points))
                detail::corrupt("the string bound for field " + std::to_string(field_id) +
                                " is not valid UTF-8");
            return ordinalize_scalar_bytes8(reinterpret_cast<const uint8_t*>(b.data()),
                                            static_cast<uint32_t>(b.size()));
        }
        case DRAKEN_VARBINARY:
            return ordinalize_scalar_bytes8(reinterpret_cast<const uint8_t*>(b.data()),
                                            static_cast<uint32_t>(b.size()));
        case DRAKEN_DECIMAL: {
            // Big-endian two's complement, minimal length; at most 8 bytes fit int64.
            if (b.size() > 8)
                detail::corrupt("the decimal bound for field " + std::to_string(field_id) +
                                " does not fit in int64 range");
            if (b.empty()) return 0;
            uint64_t v = (static_cast<uint8_t>(b[0]) & 0x80u) ? ~uint64_t{0} : 0;
            for (char c : b) v = (v << 8) | static_cast<uint8_t>(c);
            return static_cast<int64_t>(v);
        }
        case DRAKEN_DECIMAL128:
            throw std::invalid_argument(
                "ordinalize: DrakenType.DECIMAL128 is not supported — draken produces no "
                "ordinal key for it, so a stored bound in this space cannot exist");
        default:
            throw std::invalid_argument("Iceberg bounds: field " + std::to_string(field_id) +
                                        " has a type with no Iceberg bound decoding");
    }
}

struct Scratch {
    std::vector<std::pair<int32_t, std::string>> lower;
    std::vector<std::pair<int32_t, std::string>> upper;
    std::unordered_map<int32_t, int64_t> nulls;
};

// One manifest entry's bounds (row `row` of the three JSON columns; any may be
// nullptr when the manifest lacks the column) into `out`.
inline void decode_row(const DrakenVector* lower, const DrakenVector* upper,
                       const DrakenVector* nulls, uint32_t row, const TypeTable& types,
                       Scratch& s, FileBounds& out) {
    out.field_ids.clear();
    out.min_values.clear();
    out.max_values.clear();
    out.null_counts.clear();
    out.has_min.clear();
    out.has_max.clear();
    out.has_null_count.clear();
    detail::binary_map(lower, row, s.lower);
    detail::binary_map(upper, row, s.upper);
    detail::count_map(nulls, row, s.nulls);

    size_t li = 0, ui = 0;
    while (li < s.lower.size() || ui < s.upper.size()) {
        int32_t fid;
        const std::string* lo = nullptr;
        const std::string* hi = nullptr;
        if (ui == s.upper.size() || (li < s.lower.size() && s.lower[li].first < s.upper[ui].first)) {
            fid = s.lower[li].first;
            lo = &s.lower[li++].second;
        } else if (li == s.lower.size() || s.upper[ui].first < s.lower[li].first) {
            fid = s.upper[ui].first;
            hi = &s.upper[ui++].second;
        } else {
            fid = s.lower[li].first;
            lo = &s.lower[li++].second;
            hi = &s.upper[ui++].second;
        }
        out.field_ids.push_back(fid);
        const auto type = types.find(fid);
        const bool typed = type != types.end();
        out.has_min.push_back(typed && lo != nullptr);
        out.min_values.push_back(typed && lo != nullptr ? ordinal(type->second, *lo, fid) : 0);
        out.has_max.push_back(typed && hi != nullptr);
        out.max_values.push_back(typed && hi != nullptr ? ordinal(type->second, *hi, fid) : 0);
        const auto count = s.nulls.find(fid);
        out.has_null_count.push_back(count != s.nulls.end());
        out.null_counts.push_back(count != s.nulls.end() ? count->second : 0);
    }
}

}  // namespace opteryx::planner::iceberg
