// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// See the License at http://www.apache.org/licenses/LICENSE-2.0
// Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

// src/cpp/planner/manifest_prune.hpp — which files of a NativeManifest a query
// must read (native plan graph Q8, M-d2).
//
// The native port of Manifest.prune_files / prune_files_for_topn /
// ordinal_zone_map_terms (opteryx/models/manifest.py). Terms come from
// predicate_bounds.hpp; this decides them against each file's MANIFEST bounds
// (never the footer's - file pruning has only ever read the manifest), null
// counts, char-class counts and min-k sketches. Every rule is the Python's, and
// each is sound in one direction only: a term that cannot be decided keeps the
// file. Where the Python raised out of the optimizer on a literal it could not
// handle, this keeps the file instead.

#pragma once

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

#include "ops/hash.h"
#include "ops/ordinalize.h"
#include "planner/bound_interval.hpp"
#include "planner/manifest_estimates.hpp"
#include "planner/manifest_sketch.hpp"
#include "planner/native_manifest.hpp"
#include "planner/predicate_bounds.hpp"
#include "ryu.h"

namespace opteryx::planner {

// What pruning knows of the manifest's columns: the load-time position of each
// name, and the LIVE schema's type of each name still in it.
struct PruneColumns {
    const std::unordered_map<std::string, size_t>* position = nullptr;
    const std::unordered_map<std::string, ColumnTypeId>* live_types = nullptr;
    const ColumnTypeTable* types = nullptr;

    const ColumnTypeEntry* live_type(const std::string& name) const {
        auto found = live_types->find(name);
        return found == live_types->end() ? nullptr : &types->entry(found->second);
    }

    bool position_of(const std::string& name, size_t& out) const {
        auto found = position->find(name);
        if (found == position->end()) return false;
        out = found->second;
        return true;
    }
};

namespace prune_detail {

inline bool is_float(const ColumnTypeEntry* t) {
    return t != nullptr && (t->physical == DRAKEN_FLOAT32 || t->physical == DRAKEN_FLOAT64);
}

inline bool is_temporal_physical(DrakenType p) {
    return p == DRAKEN_DATE32 || p == DRAKEN_TIMESTAMP64 || p == DRAKEN_TIME32 || p == DRAKEN_TIME64;
}

// `_temporal_domain_mismatch`: both temporal but different raw integer domains.
inline bool temporal_domain_mismatch(const ColumnTypeEntry* column, const ColumnTypeEntry* literal) {
    if (column == nullptr || literal == nullptr) return false;
    if (!is_temporal_physical(column->physical) || !is_temporal_physical(literal->physical)) return false;
    if (column->physical != literal->physical) return true;
    const int column_unit = column->has_logical ? static_cast<int>(column->logical.unit) : -1;
    const int literal_unit = literal->has_logical ? static_cast<int>(literal->logical.unit) : -1;
    return column_unit != literal_unit;
}

// The Python type a term's value had: what ordinalize and the bound comparison
// dispatch on.
enum class PyType : uint8_t { NONE, INT, FLOAT, BYTES, BOOL, DECIMAL, TUPLE };

inline PyType py_type(const BVal& v) {
    switch (v.kind) {
        case BKind::INT: return PyType::INT;
        case BKind::FLOAT: return PyType::FLOAT;
        case BKind::BYTES: return PyType::BYTES;
        case BKind::BOOL: return PyType::BOOL;
        default: break;
    }
    if (v.literal == nullptr) return PyType::NONE;
    switch (v.literal->tag) {
        case LITERAL_DECIMAL: return PyType::DECIMAL;
        case LITERAL_INTERVAL:
        case LITERAL_ITEMS: return PyType::TUPLE;
        default: return PyType::NONE;
    }
}

inline __int128 decimal_unscaled(const LiteralValue& lit) {
    return (static_cast<__int128>(lit.j) << 64) | static_cast<__int128>(static_cast<uint64_t>(lit.i));
}

inline bool pow10(int32_t e, __int128& out) {
    if (e < 0 || e > 38) return false;
    out = 1;
    for (int32_t k = 0; k < e; ++k) out *= 10;
    return true;
}

// A decimal number (unscaled * 10^exponent) on a column's gridline 10^-scale,
// as the unscaled integer there - only when it lands EXACTLY (and within the
// 28-digit context Python's quantize works in).
inline std::optional<__int128> on_gridline(__int128 unscaled, int32_t exponent, int32_t scale) {
    const int32_t shift = exponent + scale;
    __int128 factor;
    if (shift >= 0) {
        if (!pow10(shift, factor)) return std::nullopt;
        __int128 out;
        if (__builtin_mul_overflow(unscaled, factor, &out)) return std::nullopt;
        const __int128 limit = static_cast<__int128>(1000000000000000000LL) * 10000000000LL;   // 10^28
        if (out >= limit || out <= -limit) return std::nullopt;
        return out;
    }
    if (!pow10(-shift, factor)) return std::nullopt;
    if (unscaled % factor != 0) return std::nullopt;
    return unscaled / factor;
}

// A float as Python's `Decimal(str(value))`: its shortest round-trip digits.
inline bool float_decimal(double v, __int128& unscaled, int32_t& exponent) {
    if (!std::isfinite(v)) return false;
    char buffer[32];
    const int n = d2s_buffered_n(v, buffer);
    buffer[n] = '\0';
    // ryu: [-]D[.DDD]E[-]X
    std::string digits;
    bool negative = false;
    int32_t point = 0;
    int k = 0;
    if (buffer[k] == '-') { negative = true; ++k; }
    bool after_point = false;
    for (; k < n && buffer[k] != 'E'; ++k) {
        if (buffer[k] == '.') { after_point = true; continue; }
        digits.push_back(buffer[k]);
        if (after_point) --point;
    }
    if (k >= n) return false;
    const int32_t e = static_cast<int32_t>(std::strtol(buffer + k + 1, nullptr, 10));
    if (digits.size() > 30) return false;
    __int128 u = 0;
    for (char c : digits) u = u * 10 + (c - '0');
    unscaled = negative ? -u : u;
    exponent = e + point;
    return true;
}

// `ColumnType.ordinalize(value)`: the literal's key in the column's ordinal
// space; nullopt where the Python returned None or raised.
inline std::optional<__int128> ordinalize(const ColumnTypeEntry* t, const BVal& v) {
    if (t == nullptr) return std::nullopt;
    const PyType type = py_type(v);
    const DrakenType p = t->physical;
    if (is_temporal_physical(p)) {
        if (type != PyType::INT) return std::nullopt;
        return v.i;
    }
    if (p == DRAKEN_DECIMAL) {
        if (!t->has_logical) return std::nullopt;
        const int32_t scale = t->logical.scale;
        if (type == PyType::INT) return on_gridline(v.i, 0, scale);
        if (type == PyType::DECIMAL) return on_gridline(decimal_unscaled(*v.literal), v.literal->k, scale);
        if (type == PyType::FLOAT) {
            __int128 unscaled;
            int32_t exponent;
            if (!float_decimal(v.d, unscaled, exponent)) return std::nullopt;
            return on_gridline(unscaled, exponent, scale);
        }
        return std::nullopt;
    }
    // Python ints (bools among them) for the integer kinds
    const bool integer = type == PyType::INT || type == PyType::BOOL;
    const __int128 as_int = v.i;
    switch (p) {
        case DRAKEN_INT64:
            if (!integer || as_int < INT64_MIN || as_int > INT64_MAX) return std::nullopt;
            return as_int;
        case DRAKEN_INT8: case DRAKEN_INT16: case DRAKEN_INT32:
        case DRAKEN_UINT8: case DRAKEN_UINT16: case DRAKEN_UINT32: case DRAKEN_BOOL: {
            if (!integer) return std::nullopt;
            __int128 lo, hi;
            switch (p) {
                case DRAKEN_INT8: lo = INT8_MIN; hi = INT8_MAX; break;
                case DRAKEN_INT16: lo = INT16_MIN; hi = INT16_MAX; break;
                case DRAKEN_INT32: lo = INT32_MIN; hi = INT32_MAX; break;
                case DRAKEN_UINT8: lo = 0; hi = UINT8_MAX; break;
                case DRAKEN_UINT16: lo = 0; hi = UINT16_MAX; break;
                case DRAKEN_UINT32: lo = 0; hi = UINT32_MAX; break;
                default: lo = 0; hi = 1; break;
            }
            if (as_int < lo || as_int > hi) return std::nullopt;
            return as_int;
        }
        case DRAKEN_UINT64:
            if (!integer || as_int < 0 || as_int > static_cast<__int128>(UINT64_MAX)) return std::nullopt;
            return draken::ops::ordinalize_scalar_u64(static_cast<uint64_t>(as_int));
        case DRAKEN_FLOAT32:
        case DRAKEN_FLOAT64: {
            double d;
            if (type == PyType::FLOAT) d = v.d;
            else if (integer) d = static_cast<double>(as_int);
            else if (type == PyType::DECIMAL) {
                d = static_cast<double>(decimal_unscaled(*v.literal)) * std::pow(10.0, v.literal->k);
            } else return std::nullopt;
            return p == DRAKEN_FLOAT32 ? draken::ops::ordinalize_scalar_f32(static_cast<float>(d))
                                       : draken::ops::ordinalize_scalar_f64(d);
        }
        case DRAKEN_INTERVAL:
            if (v.literal == nullptr || v.literal->tag != LITERAL_INTERVAL) return std::nullopt;
            return draken::ops::ordinalize_scalar_interval(v.literal->i, v.literal->j);
        case DRAKEN_VARCHAR: case DRAKEN_NVARCHAR: case DRAKEN_VARBINARY: case DRAKEN_VARIANT:
            if (type != PyType::BYTES) return std::nullopt;
            return draken::ops::ordinalize_scalar_bytes8(reinterpret_cast<const uint8_t*>(v.bytes.data()),
                                                         static_cast<uint32_t>(v.bytes.size()));
        default:
            return std::nullopt;
    }
}

// A value the handlers compare: a literal's (INT/FLOAT/BYTES/BOOL/DECIMAL) or a
// bound's. Comparisons are Python's between the Python types they stand for.
struct Cmp {
    PyType type = PyType::NONE;
    __int128 i = 0;         // INT, BOOL, DECIMAL unscaled
    double d = 0.0;
    int32_t exponent = 0;   // DECIMAL: value = i * 10^exponent
    const std::string* text = nullptr;
    bool text_is_str = false;   // a TEXT/OTHER bound is a Python str, never equal-typed to bytes
};

inline Cmp cmp_of_literal(const BVal& v) {
    Cmp c;
    c.type = py_type(v);
    if (c.type == PyType::INT || c.type == PyType::BOOL) c.i = v.i;
    else if (c.type == PyType::FLOAT) c.d = v.d;
    else if (c.type == PyType::BYTES) c.text = &v.bytes;
    else if (c.type == PyType::DECIMAL) { c.i = decimal_unscaled(*v.literal); c.exponent = v.literal->k; }
    return c;
}

inline Cmp cmp_of_int(__int128 v) {
    Cmp c;
    c.type = PyType::INT;
    c.i = v;
    return c;
}

// One decoded end of a manifest bound, as its Python value.
inline Cmp cmp_of_bound(const Bounds& b, bool is_min) {
    Cmp c;
    const DecodedTag tag = is_min ? b.min_tag : b.max_tag;
    const int64_t as_int = is_min ? b.min_int : b.max_int;
    switch (tag) {
        case DECODED_INT64: c.type = PyType::INT; c.i = as_int; break;
        case DECODED_UINT64: c.type = PyType::INT; c.i = static_cast<__int128>(static_cast<uint64_t>(as_int)); break;
        case DECODED_DOUBLE: c.type = PyType::FLOAT; c.d = is_min ? b.min_double : b.max_double; break;
        case DECODED_BYTES: c.type = PyType::BYTES; c.text = is_min ? &b.min_text : &b.max_text; break;
        case DECODED_TEXT:
        case DECODED_OTHER:
            c.type = PyType::BYTES;
            c.text = is_min ? &b.min_text : &b.max_text;
            c.text_is_str = true;
            break;
        case DECODED_BOOL: c.type = PyType::BOOL; c.i = as_int; break;
        case DECODED_DECIMAL:
            c.type = PyType::DECIMAL;
            c.i = as_int;
            c.exponent = -(is_min ? b.min_scale : b.max_scale);
            break;
        default: c.type = PyType::NONE; break;
    }
    return c;
}

inline bool numeric(const Cmp& c) { return c.type == PyType::INT || c.type == PyType::FLOAT || c.type == PyType::BOOL; }

// Exact order of two decimals (a * 10^ea vs b * 10^eb).
inline int cmp_decimal(__int128 a, int32_t ea, __int128 b, int32_t eb) {
    const int32_t shift = ea - eb;
    __int128 factor;
    if (shift >= 0) {
        if (pow10(shift, factor)) {
            __int128 scaled;
            if (!__builtin_mul_overflow(a, factor, &scaled)) return scaled < b ? -1 : (scaled > b ? 1 : 0);
        }
    } else if (pow10(-shift, factor)) {
        __int128 scaled;
        if (!__builtin_mul_overflow(b, factor, &scaled)) return a < scaled ? -1 : (a > scaled ? 1 : 0);
    }
    const long double x = static_cast<long double>(a) * std::pow(10.0L, ea);
    const long double y = static_cast<long double>(b) * std::pow(10.0L, eb);
    return x < y ? -1 : (x > y ? 1 : 0);
}

// Python's order of two values; 2 when Python would raise or the pair is
// unordered (a NaN).
inline int order(const Cmp& a, const Cmp& b) {
    if (numeric(a) && numeric(b)) {
        BVal x, y;
        x = a.type == PyType::FLOAT ? BVal::of_float(a.d) : BVal::of_int(a.i);
        y = b.type == PyType::FLOAT ? BVal::of_float(b.d) : BVal::of_int(b.i);
        return cmp(x, y);
    }
    if (a.type == PyType::DECIMAL && b.type == PyType::DECIMAL) return cmp_decimal(a.i, a.exponent, b.i, b.exponent);
    if (a.type == PyType::DECIMAL && (b.type == PyType::INT || b.type == PyType::BOOL)) return cmp_decimal(a.i, a.exponent, b.i, 0);
    if (b.type == PyType::DECIMAL && (a.type == PyType::INT || a.type == PyType::BOOL)) return cmp_decimal(a.i, 0, b.i, b.exponent);
    if (a.type == PyType::BYTES && b.type == PyType::BYTES && a.text_is_str == b.text_is_str) {
        const int r = a.text->compare(*b.text);
        return r < 0 ? -1 : (r > 0 ? 1 : 0);
    }
    return 2;
}

// `_comparable_literal(literal, bound_sample)`: whether the literal is compared
// with the bound at all.
inline bool comparable_literal(const Cmp& literal, const Cmp& sample) {
    if (sample.type == PyType::BYTES && !sample.text_is_str) {
        return literal.type == PyType::BYTES;
    }
    if (sample.type == PyType::BOOL) return literal.type == PyType::BOOL;
    if (sample.type == PyType::INT || sample.type == PyType::FLOAT) {
        return literal.type == PyType::INT || literal.type == PyType::FLOAT;
    }
    // str / Decimal samples: same Python type only (bool is an int, never a Decimal)
    if (sample.type == PyType::BYTES) return false;   // a str bound; literals are never str
    if (sample.type == PyType::DECIMAL) return literal.type == PyType::DECIMAL;
    return false;
}

// `_is_real_bound`: present, and not the -(1<<63) "no bound" sentinel.
inline bool real_bound(const Cmp& c) {
    if (c.type == PyType::NONE) return false;
    static const __int128 sentinel = static_cast<__int128>(INT64_MIN);
    if (c.type == PyType::INT) return c.i != sentinel;
    if (c.type == PyType::FLOAT) return c.d != -9223372036854775808.0;
    if (c.type == PyType::DECIMAL) return cmp_decimal(c.i, c.exponent, sentinel, 0) != 0;
    return true;
}

// Whether `op` prunes the file for `v` against (min, max). Every comparison must
// be one Python makes without raising; one that would raise keeps the file.
inline bool handler_prunes(TermOp op, const Cmp& v, const Cmp& lo, const Cmp& hi) {
    auto o = [](const Cmp& a, const Cmp& b) { return order(a, b); };
    switch (op) {
        case OP_EQ: {
            const int a = o(v, lo);
            if (a == 2) return false;
            if (a == -1) return true;
            const int b = o(v, hi);
            return b == 1;
        }
        case OP_NOTEQ: {
            const int a = o(lo, hi);
            if (a != 0) return false;
            return o(hi, v) == 0;
        }
        case OP_GT: { const int r = o(hi, v); return r == -1 || r == 0; }
        case OP_GTEQ: return o(hi, v) == -1;
        case OP_LT: { const int r = o(lo, v); return r == 1 || r == 0; }
        case OP_LTEQ: return o(lo, v) == 1;
        default: return false;
    }
}

// Whether a term's value is a NULL literal (compares with nothing).
inline bool null_value(const BVal& v) {
    return v.kind == BKind::OPAQUE && v.literal != nullptr &&
           (v.literal->tag == LITERAL_NULL || v.literal->tag == LITERAL_NONE);
}

}  // namespace prune_detail

// `_bounds_may_omit_nan`: a NaN row can sit outside the bounds - only for decoded
// (rugo min/max) bounds of a float (or unresolvable) column.
inline bool bounds_may_omit_nan(const NativeManifest& m, const PruneColumns& cols, const std::string& column) {
    if (m.bounds_are_ordinal()) return false;
    const ColumnTypeEntry* t = cols.live_type(column);
    return t == nullptr || prune_detail::is_float(t);
}

// `_nan_invisible_to_bounds`: Gt / GtEq / NotEq on such a column.
inline bool nan_invisible(const NativeManifest& m, const PruneColumns& cols, const std::string& column, TermOp op) {
    return (op == OP_GT || op == OP_GTEQ || op == OP_NOTEQ) && bounds_may_omit_nan(m, cols, column);
}

// `_predicate_domain_mismatch` for a canonical term.
inline bool domain_mismatch(const BoundTerm& t, const PruneColumns& cols, const DeriveInputs& in) {
    if (t.derived) return false;
    const ColumnTypeEntry* column = cols.live_type(t.column);
    if (column == nullptr) {
        const ExprRow& identifier = in.exprs->row(t.identifier);
        if (identifier.column_slot != kNoColumnSlot) {
            const ColumnTypeId type_id = in.columns->row(identifier.column_slot).type_id;
            if (type_id != kNoColumnType) column = &in.types->entry(type_id);
        }
    }
    auto literal = [&](ColumnTypeId id) -> const ColumnTypeEntry* {
        return id == kNoColumnType ? nullptr : &in.types->entry(id);
    };
    if (prune_detail::temporal_domain_mismatch(column, literal(t.literal_type))) return true;
    return t.op == OP_BETWEEN && prune_detail::temporal_domain_mismatch(column, literal(t.upper_type));
}

namespace prune_detail {

// A term's value, in the space the file's bounds are in: ordinalized through
// the LIVE column type in the ordinal dialect, as-is otherwise.
inline std::optional<Cmp> compare_value(const NativeManifest& m, const PruneColumns& cols, const std::string& column,
                                        const BVal& value) {
    if (null_value(value)) return std::nullopt;
    if (m.bounds_are_ordinal()) {
        auto ordinal = ordinalize(cols.live_type(column), value);
        if (!ordinal) return std::nullopt;
        return cmp_of_int(*ordinal);
    }
    return cmp_of_literal(value);
}

// The file's (min, max) for the column as Python values, or false when the
// bound is not real.
inline bool file_bounds(const NativeManifest& m, size_t row, size_t position, Cmp& lo, Cmp& hi) {
    const Bounds& b = m.cell(row, position).bounds;
    if (m.bounds_are_ordinal()) {
        if (b.min_ordinal == kNoBound || b.max_ordinal == kNoBound) return false;
        lo = cmp_of_int(b.min_ordinal);
        hi = cmp_of_int(b.max_ordinal);
        return true;
    }
    lo = cmp_of_bound(b, true);
    hi = cmp_of_bound(b, false);
    return real_bound(lo) && real_bound(hi);
}

}  // namespace prune_detail

// Whether one term proves file `row` holds no matching row.
inline bool term_prunes(const NativeManifest& m, const PruneColumns& cols, const BoundTerm& t, size_t row) {
    using namespace prune_detail;
    size_t position;
    if (!cols.position_of(t.column, position)) return false;
    if (t.op == OP_BETWEEN) {
        Cmp lo, hi;
        if (!file_bounds(m, row, position, lo, hi)) return false;
        auto lower = compare_value(m, cols, t.column, t.value);
        auto upper = compare_value(m, cols, t.column, t.upper);
        if (!lower || !upper) return false;
        if (!comparable_literal(*lower, lo) || !comparable_literal(*upper, lo)) return false;
        if (!bounds_may_omit_nan(m, cols, t.column) && order(hi, *lower) == -1) return true;
        return order(lo, *upper) == 1;
    }
    if (nan_invisible(m, cols, t.column, t.op)) return false;
    Cmp lo, hi;
    if (!file_bounds(m, row, position, lo, hi)) return false;
    if (m.bounds_are_ordinal() && t.op == OP_NOTEQ) {
        // String ordinals are monotonic but not injective: `min == max == v`
        // is not value uniformity for them.
        const ColumnTypeEntry* type = cols.live_type(t.column);
        if (type == nullptr || type->physical == DRAKEN_VARCHAR || type->physical == DRAKEN_VARBINARY) return false;
    }
    auto value = compare_value(m, cols, t.column, t.value);
    if (!value) return false;
    if (!comparable_literal(*value, lo)) return false;
    return handler_prunes(t.op, *value, lo, hi);
}

// `_membership_keep_masks`: for `int_col = <int>`, the files whose unsaturated
// min-k sketch proves the value absent. One keep mask per eligible term,
// indexed by vector row.
inline std::vector<std::vector<uint8_t>> membership_masks(const NativeManifest& m, const PruneColumns& cols,
                                                          const std::vector<BoundTerm>& terms) {
    std::vector<std::vector<uint8_t>> masks;
    if (!m.min_k.present()) return masks;
    for (const BoundTerm& t : terms) {
        if (t.op != OP_EQ || t.value.kind != BKind::INT) continue;
        size_t position;
        if (!cols.position_of(t.column, position)) continue;
        const ColumnTypeEntry* type = cols.live_type(t.column);
        if (type == nullptr) continue;
        const DrakenType p = type->physical;
        if (p != DRAKEN_INT8 && p != DRAKEN_INT16 && p != DRAKEN_INT32 && p != DRAKEN_INT64) continue;
        // hashed as a one-row vector of the column's own type, exactly as the
        // sketch's writer hashed it
        const __int128 v = t.value.i;
        int64_t i64 = 0; int32_t i32 = 0; int16_t i16 = 0; int8_t i8 = 0;
        DrakenVector one{};
        static const uint32_t zero_selection = 0;
        one.selection = &zero_selection;
        one.length = 1;
        one.data_length = 1;
        one.type = p;
        switch (p) {
            case DRAKEN_INT8: if (v < INT8_MIN || v > INT8_MAX) continue; i8 = static_cast<int8_t>(v); one.data = &i8; break;
            case DRAKEN_INT16: if (v < INT16_MIN || v > INT16_MAX) continue; i16 = static_cast<int16_t>(v); one.data = &i16; break;
            case DRAKEN_INT32: if (v < INT32_MIN || v > INT32_MAX) continue; i32 = static_cast<int32_t>(v); one.data = &i32; break;
            default: if (v < INT64_MIN || v > INT64_MAX) continue; i64 = static_cast<int64_t>(v); one.data = &i64; break;
        }
        uint64_t probe = 0;
        draken_hash(one, &probe, 1);

        std::vector<uint8_t> mask(m.min_k.n_files(), 1);
        for (uint32_t i = 0, n = m.min_k.n_files(); i < n; ++i) {
            m.min_k.with_field_slice(i, static_cast<int64_t>(position), [&](int32_t g0, int32_t g1) {
                const uint32_t count = static_cast<uint32_t>(g1 - g0);
                if (count == 0 || count >= ManifestSketch::kK) return;
                for (int32_t g = g0; g < g1; ++g) {
                    if (!m.min_k.leaf_valid(g)) continue;
                    if (m.min_k.u64(g) == probe) return;
                }
                mask[i] = 0;
            });
        }
        masks.push_back(std::move(mask));
    }
    return masks;
}

// `_fold_is_identity`: the file's char-class counts prove the fold changes
// nothing in the column.
inline bool fold_is_identity(const NativeManifest& m, const ColumnTypeEntry* type, size_t position, uint32_t vector_row,
                             bool lower) {
    if (type == nullptr) return false;
    const DrakenType p = type->physical;
    if (p != DRAKEN_VARCHAR && p != DRAKEN_VARBINARY && p != DRAKEN_NVARCHAR) return false;
    int64_t totals[8];
    if (!char_class_totals(m.char_class, static_cast<int64_t>(position), std::vector<uint32_t>{vector_row}, totals)) {
        return false;
    }
    if (totals[lower ? 0 : 1] != 0) return false;   // UPPER / LOWER class bytes
    if (p == DRAKEN_NVARCHAR && totals[6] != 0) return false;   // EXTENDED
    return true;
}

// `prune_files`: the rows of the files the terms cannot rule out, in order.
// `terms` are the domain-checked canonical + derived terms; returns every row
// when there are none.
inline std::vector<size_t> prune_files(const NativeManifest& m, const PruneColumns& cols,
                                       const std::vector<BoundTerm>& terms, const std::vector<NullTerm>& null_terms,
                                       const std::vector<FoldTerm>& fold_terms) {
    std::vector<size_t> kept;
    kept.reserve(m.file_count());
    const auto masks = membership_masks(m, cols, terms);

    struct NullCheck { size_t position; bool requires_null; };
    std::vector<NullCheck> nulls;
    for (const NullTerm& n : null_terms) {
        size_t position;
        if (cols.position_of(n.column, position)) nulls.push_back({position, n.requires_null});
    }
    struct Fold { size_t position; bool lower; const ColumnTypeEntry* type; const std::vector<BoundTerm>* terms; };
    std::vector<Fold> folds;
    if (m.char_class.present()) {
        for (const FoldTerm& f : fold_terms) {
            size_t position;
            if (cols.position_of(f.column, position)) folds.push_back({position, f.lower, cols.live_type(f.column), &f.terms});
        }
    }

    for (size_t row = 0; row < m.file_count(); ++row) {
        const ManifestFile& file = m.file(row);
        bool skip = false;
        for (const auto& mask : masks) {
            if (file.vector_row < mask.size() && mask[file.vector_row] == 0) { skip = true; break; }
        }
        if (skip) continue;
        for (const NullCheck& n : nulls) {
            const ManifestCell& c = m.cell(row, n.position);
            const int64_t null_count = file.has_footer ? c.footer.null_count : c.null_count;
            if (null_count == kUnknown) continue;
            if (n.requires_null) {
                if (null_count == 0) { skip = true; break; }
            } else if (file.record_count != kUnknown && null_count >= file.record_count) {
                skip = true;
                break;
            }
        }
        if (skip) continue;
        for (const BoundTerm& t : terms) {
            if (term_prunes(m, cols, t, row)) { skip = true; break; }
        }
        if (!skip) {
            for (const Fold& f : folds) {
                if (!fold_is_identity(m, f.type, f.position, file.vector_row, f.lower)) continue;
                for (const BoundTerm& t : *f.terms) {
                    if (term_prunes(m, cols, t, row)) { skip = true; break; }
                }
                if (skip) break;
            }
        }
        if (!skip) kept.push_back(row);
    }
    return kept;
}

// `prune_files_for_topn`: for `ORDER BY column LIMIT limit` over a column with
// no NULLs (the caller's precondition), the rows of the files that can hold a
// top-`limit` row.
inline std::vector<size_t> prune_files_for_topn(const NativeManifest& m, const PruneColumns& cols,
                                                const std::string& column, bool descending, int64_t limit) {
    using namespace prune_detail;
    std::vector<size_t> all(m.file_count());
    for (size_t k = 0; k < all.size(); ++k) all[k] = k;
    size_t position;
    if (!cols.position_of(column, position) || limit <= 0) return all;
    if (bounds_may_omit_nan(m, cols, column)) return all;

    struct Ranked { size_t row; Cmp lo, hi; };
    std::vector<Ranked> bounded;
    std::vector<int8_t> has(m.file_count(), 0);
    std::vector<Cmp> lows(m.file_count()), highs(m.file_count());
    for (size_t row = 0; row < m.file_count(); ++row) {
        Cmp lo, hi;
        if (!file_bounds(m, row, position, lo, hi)) continue;
        bounded.push_back({row, lo, hi});
        has[row] = 1;
        lows[row] = lo;
        highs[row] = hi;
    }
    if (bounded.empty()) return all;
    std::stable_sort(bounded.begin(), bounded.end(), [&](const Ranked& a, const Ranked& b) {
        return descending ? order(a.hi, b.hi) == 1 : order(a.lo, b.lo) == -1;
    });
    int64_t accumulated = 0;
    Cmp threshold;
    bool have_threshold = false;
    for (const Ranked& r : bounded) {
        const int64_t rows = m.file(r.row).record_count;
        accumulated += rows == kUnknown ? 0 : rows;
        const Cmp& candidate = descending ? r.lo : r.hi;
        if (!have_threshold) {
            threshold = candidate;
            have_threshold = true;
        } else if (descending ? order(candidate, threshold) == -1 : order(threshold, candidate) == -1) {
            threshold = candidate;
        }
        if (accumulated >= limit) break;
    }
    std::vector<size_t> kept;
    for (size_t row = 0; row < m.file_count(); ++row) {
        if (has[row]) {
            if (descending ? order(highs[row], threshold) == -1 : order(lows[row], threshold) == 1) continue;
        }
        kept.push_back(row);
    }
    return kept;
}

// One `(column, op, ordinal)` a row-group zone map is tested against
// (`ordinal_zone_map_terms`); op codes are Manifest.ZONE_OP_*.
struct ZoneTerm {
    std::string column;
    uint8_t op;          // 0 Eq, 1 Gt, 2 GtEq, 3 Lt, 4 LtEq
    __int128 ordinal;
};

inline std::vector<ZoneTerm> zone_map_terms(const NativeManifest& m, const PruneColumns& cols,
                                            const std::vector<BoundTerm>& terms) {
    std::vector<ZoneTerm> out;
    if (!m.bounds_are_ordinal()) return out;
    auto emit = [&](const std::string& column, uint8_t op, const BVal& value) {
        auto ordinal = prune_detail::ordinalize(cols.live_type(column), value);
        if (ordinal) out.push_back({column, op, *ordinal});
    };
    for (const BoundTerm& t : terms) {
        uint8_t code;
        switch (t.op) {
            case OP_EQ: code = 0; break;
            case OP_GT: code = 1; break;
            case OP_GTEQ: code = 2; break;
            case OP_LT: code = 3; break;
            case OP_LTEQ: code = 4; break;
            case OP_BETWEEN:
                if (!nan_invisible(m, cols, t.column, OP_GTEQ)) emit(t.column, 2, t.value);
                if (!nan_invisible(m, cols, t.column, OP_LTEQ)) emit(t.column, 4, t.upper);
                continue;
            default: continue;
        }
        if (nan_invisible(m, cols, t.column, t.op)) continue;
        emit(t.column, code, t.value);
    }
    return out;
}

// The terms of `predicates` a file prune decides on: the canonical and derived
// terms that pass the temporal-domain guard, and whether any conjunct survived
// it at all (`prune_files` prunes nothing when none did).
inline std::vector<BoundTerm> checked_terms(const DeriveInputs& in, const PruneColumns& cols,
                                            const std::vector<ExprId>& conjuncts, bool& any_left) {
    std::vector<BoundTerm> terms = derive_bound_terms(in, conjuncts);
    size_t mismatched = 0;
    std::vector<BoundTerm> kept;
    kept.reserve(terms.size());
    for (BoundTerm& t : terms) {
        if (domain_mismatch(t, cols, in)) {
            ++mismatched;
            continue;
        }
        kept.push_back(std::move(t));
    }
    any_left = conjuncts.size() > mismatched;
    return kept;
}

// `Manifest.prune_files`: the rows of the files `predicates` cannot rule out.
inline std::vector<size_t> prune(const NativeManifest& m, const DeriveInputs& in, const PruneColumns& cols,
                                 const std::vector<ExprId>& predicates) {
    const std::vector<ExprId> conjuncts = split_conjuncts(in, predicates);
    bool any_left = false;
    const std::vector<BoundTerm> terms = checked_terms(in, cols, conjuncts, any_left);
    if (!any_left) {
        std::vector<size_t> all(m.file_count());
        for (size_t k = 0; k < all.size(); ++k) all[k] = k;
        return all;
    }
    const std::vector<NullTerm> nulls = derive_null_terms(in, conjuncts);
    std::vector<FoldTerm> folds;
    if (m.char_class.present()) folds = derive_fold_terms(in, conjuncts);
    return prune_files(m, cols, terms, nulls, folds);
}

// `Manifest.ordinal_zone_map_terms`.
inline std::vector<ZoneTerm> zone_terms(const NativeManifest& m, const DeriveInputs& in, const PruneColumns& cols,
                                        const std::vector<ExprId>& predicates) {
    if (!m.bounds_are_ordinal()) return {};
    const std::vector<ExprId> conjuncts = split_conjuncts(in, predicates);
    bool any_left = false;
    const std::vector<BoundTerm> terms = checked_terms(in, cols, conjuncts, any_left);
    return zone_map_terms(m, cols, terms);
}

// File `row`'s key range on the column at `position`, as compaction planning
// reads it (`file_key_ranges`): the MANIFEST's bounds only, both ends real (no
// "no bound" sentinel) - ordinal keys in the ordinal dialect, the decoded ends
// otherwise. False when the file has none.
inline bool file_key_range(const NativeManifest& m, size_t row, size_t position,
                           estimate_detail::End& lo, estimate_detail::End& hi) {
    using namespace prune_detail;
    Cmp low, high;
    if (!file_bounds(m, row, position, low, high)) return false;
    const Bounds& b = m.cell(row, position).bounds;
    if (m.bounds_are_ordinal()) {
        lo = estimate_detail::ordinal_end(b.min_ordinal);
        hi = estimate_detail::ordinal_end(b.max_ordinal);
    } else {
        lo = estimate_detail::decoded_end(b, true);
        hi = estimate_detail::decoded_end(b, false);
    }
    return lo.present() && hi.present();
}

}  // namespace opteryx::planner
