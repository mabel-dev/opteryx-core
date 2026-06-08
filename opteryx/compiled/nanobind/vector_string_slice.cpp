// opteryx/compiled/nanobind/vector_string_slice.cpp — Milestone E.26+.
//
// C′ pattern: pure nanobind C++, zero Cython.  One NB_MODULE, three functions:
//
//   vector_string_slice_left(vec, length)   — keep first N bytes/codepoints.
//   vector_string_slice_right(vec, length)  — keep last N bytes/codepoints.
//   vector_string_substring(vec, from, count) — SQL SUBSTRING(s, from, count).
//
// `length` / `from` / `count` may be a Python int (applied uniformly) or a
// DRAKEN_INT64 Vector (per-row).  NullVector (Python None) propagates NULL.
//
// Null TVL: null in string row OR null in any integer arg → null output row.
//
// Operations are byte-level for VARCHAR and VARBINARY, codepoint-level for NVARCHAR.
//
// Replaces: opteryx/compiled/vector_ops/vector_string_slice.pyx

#include <Python.h>
#include <nanobind/nanobind.h>
#include <cstdint>
#include <cstring>
#include <stdexcept>
#include <string>

#include "core/buffers.h"
#include "core/alloc.h"
#include "core/string_slot.h"
#include "core/vector_alloc.h"
#include "core/draken_bridge.h"

namespace nb = nanobind;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

static inline bool is_valid_at(const DrakenVector* dv, uint32_t i) noexcept {
    if (!dv->validity) return true;
    return ((dv->validity[i >> 3] >> (i & 7u)) & 1u) != 0u;
}

static uint8_t* alloc_validity_all_valid(uint32_t n) {
    const uint32_t nb_ = (n + 7u) >> 3;
    uint8_t* v = static_cast<uint8_t*>(draken_malloc(nb_ > 0u ? nb_ : 1u));
    if (!v) throw std::bad_alloc();
    std::memset(v, 0xFF, nb_ > 0u ? nb_ : 1u);
    return v;
}

// Unwrap a string-family Vector. Raises TypeError on non-Vector or non-string type.
static const DrakenVector* unwrap_str(nb::object obj, const char* fn) {
    const DrakenVector* dv = draken_vector_unwrap(obj.ptr());
    if (!dv) throw nb::python_error();
    const bool is_str = dv->type == DRAKEN_VARCHAR || dv->type == DRAKEN_NVARCHAR ||
                        dv->type == DRAKEN_VARBINARY;
    if (!is_str)
        throw nb::type_error(
            (std::string(fn) + ": expected a string-family Vector").c_str());
    return dv;
}

// Integer argument carrier: scalar or per-row Vector.
struct IntArg {
    bool     is_null;    // NullVector — all rows null
    bool     is_scalar;  // Python int
    int64_t  scalar;
    const DrakenVector* vec;  // DRAKEN_INT64 Vector (when !is_null && !is_scalar)
};

static IntArg parse_int_arg(nb::object obj, const char* fn) {
    IntArg a;
    a.is_null   = false;
    a.is_scalar = false;
    a.scalar    = 0;
    a.vec       = nullptr;

    if (obj.is_none()) {
        a.is_null = true;
        return a;
    }
    if (PyLong_Check(obj.ptr())) {
        a.is_scalar = true;
        a.scalar    = nb::cast<int64_t>(obj);
        return a;
    }
    const DrakenVector* dv = draken_vector_unwrap(obj.ptr());
    if (!dv) throw nb::python_error();
    if (dv->type != DRAKEN_INT64)
        throw nb::type_error(
            (std::string(fn) + ": integer argument must be a Python int, None, or INT64 Vector").c_str());
    a.vec = dv;
    return a;
}

static inline bool int_arg_null_at(const IntArg& a, uint32_t i) noexcept {
    if (a.is_null) return true;
    if (a.is_scalar) return false;
    return !((a.vec->validity == nullptr) ||
             ((a.vec->validity[i >> 3] >> (i & 7u)) & 1u));
}

static inline int64_t int_arg_value_at(const IntArg& a, uint32_t i) noexcept {
    if (a.is_scalar) return a.scalar;
    return static_cast<const int64_t*>(a.vec->data)[a.vec->selection[i]];
}

// Output builder: accumulate slots + arena, wrap via draken_vector_own_string.
struct SliceOut {
    DrakenStringSlot* slots;
    uint8_t*          arena;
    size_t            arena_cap;
    size_t            arena_used;
    uint8_t*          validity;
    bool              any_null;
    uint32_t          n;
    DrakenType        type;

    SliceOut(uint32_t n_, DrakenType t, size_t arena_hint) : n(n_), type(t) {
        const size_t slots_sz = (n > 0u ? n : 1u) * sizeof(DrakenStringSlot);
        slots = static_cast<DrakenStringSlot*>(draken_malloc(slots_sz));
        if (!slots) throw std::bad_alloc();
        std::memset(slots, 0, slots_sz);
        arena_cap  = arena_hint > 0u ? arena_hint : 64u;
        arena      = static_cast<uint8_t*>(draken_malloc(arena_cap));
        if (!arena) { draken_free(slots); throw std::bad_alloc(); }
        arena_used = 0u;
        validity   = nullptr;
        any_null   = false;
    }

    // Must call release() before destruction if successful.
    ~SliceOut() {
        if (slots)    draken_free(slots);
        if (arena)    draken_free(arena);
        if (validity) draken_free(validity);
    }

    void emit_null(uint32_t i) {
        if (!validity) { validity = alloc_validity_all_valid(n); }
        validity[i >> 3] &= ~static_cast<uint8_t>(1u << (i & 7u));
        str_init_null(&slots[i]);
        any_null = true;
    }

    void emit_bytes(uint32_t i, const uint8_t* data, uint32_t len) {
        if (len <= STR_INLINE_MAX) {
            draken_build_string_slot(&slots[i], data, len, 0u);
            return;
        }
        if (arena_used + len > arena_cap) {
            size_t nc = arena_cap * 2u;
            if (nc < arena_used + len) nc = arena_used + len;
            uint8_t* na = static_cast<uint8_t*>(draken_malloc(nc));
            if (!na) throw std::bad_alloc();
            if (arena_used > 0u) std::memcpy(na, arena, arena_used);
            draken_free(arena); arena = na; arena_cap = nc;
        }
        const uint32_t off = static_cast<uint32_t>(arena_used);
        std::memcpy(arena + off, data, len);
        draken_build_string_slot(&slots[i], data, len, off);
        arena_used += len;
    }

    nb::object finish() {
        if (!any_null && validity) { draken_free(validity); validity = nullptr; }
        DrakenStringSlot* s = slots;  slots    = nullptr;
        uint8_t*          a = arena;  arena    = nullptr;
        uint8_t*          v = validity; validity = nullptr;
        PyObject* out = draken_vector_own_string(s, a, arena_used, v, n, type);
        if (!out) { draken_free(s); draken_free(a); if (v) draken_free(v); throw nb::python_error(); }
        return nb::steal<nb::object>(out);
    }
};

// ---------------------------------------------------------------------------
// vector_string_slice_left
// ---------------------------------------------------------------------------

static nb::object impl_slice_left(nb::object str_obj, nb::object len_obj) {
    const DrakenVector* dv   = unwrap_str(str_obj, "vector_string_slice_left");
    const IntArg        len  = parse_int_arg(len_obj, "vector_string_slice_left");
    const DrakenStringArena* sa = static_cast<const DrakenStringArena*>(dv->data);
    const uint32_t n = dv->length;

    SliceOut out(n, dv->type, sa->arena_used);

    for (uint32_t i = 0u; i < n; ++i) {
        if (!is_valid_at(dv, i) || int_arg_null_at(len, i)) {
            out.emit_null(i); continue;
        }
        const DrakenStringSlot* slot = &sa->slots[dv->selection[i]];
        const uint32_t slen  = str_length(slot);
        const uint8_t* sdata = str_data(slot, sa->arena);
        int64_t take_val = int_arg_value_at(len, i);
        uint32_t take;
        if (take_val < 0) {
            int64_t t = (int64_t)slen + take_val;
            take = (t < 0) ? 0u : (uint32_t)t;
        } else {
            take = (uint32_t)take_val;
        }
        if (take > slen) take = slen;
        out.emit_bytes(i, sdata, take);
    }
    return out.finish();
}

// ---------------------------------------------------------------------------
// vector_string_slice_right
// ---------------------------------------------------------------------------

static nb::object impl_slice_right(nb::object str_obj, nb::object len_obj) {
    const DrakenVector* dv   = unwrap_str(str_obj, "vector_string_slice_right");
    const IntArg        len  = parse_int_arg(len_obj, "vector_string_slice_right");
    const DrakenStringArena* sa = static_cast<const DrakenStringArena*>(dv->data);
    const uint32_t n = dv->length;

    SliceOut out(n, dv->type, sa->arena_used);

    for (uint32_t i = 0u; i < n; ++i) {
        if (!is_valid_at(dv, i) || int_arg_null_at(len, i)) {
            out.emit_null(i); continue;
        }
        const DrakenStringSlot* slot = &sa->slots[dv->selection[i]];
        const uint32_t slen  = str_length(slot);
        const uint8_t* sdata = str_data(slot, sa->arena);
        int64_t take_val = int_arg_value_at(len, i);
        uint32_t take;
        if (take_val <= 0) {
            take = 0u;
        } else {
            take = (uint32_t)take_val;
        }
        if (take > slen) take = slen;
        const uint32_t start = slen - take;
        out.emit_bytes(i, sdata + start, take);
    }
    return out.finish();
}

// ---------------------------------------------------------------------------
// vector_string_substring — SQL SUBSTRING(s, from_pos, count).
// Position is 1-based; 0 treated as 1; negative counts from end.
// count=None means "to end of string".
// ---------------------------------------------------------------------------

static nb::object impl_substring(nb::object str_obj, nb::object from_obj, nb::object cnt_obj) {
    const DrakenVector* dv   = unwrap_str(str_obj, "vector_string_substring");
    const IntArg        from = parse_int_arg(from_obj, "vector_string_substring");
    const IntArg        cnt  = parse_int_arg(cnt_obj, "vector_string_substring");
    const DrakenStringArena* sa = static_cast<const DrakenStringArena*>(dv->data);
    const uint32_t n = dv->length;

    SliceOut out(n, dv->type, sa->arena_used);

    for (uint32_t i = 0u; i < n; ++i) {
        if (!is_valid_at(dv, i) || int_arg_null_at(from, i)) {
            out.emit_null(i); continue;
        }
        const DrakenStringSlot* slot = &sa->slots[dv->selection[i]];
        const uint32_t slen  = str_length(slot);
        const uint8_t* sdata = str_data(slot, sa->arena);

        int64_t start_val = int_arg_value_at(from, i);
        uint32_t s_idx;
        if (start_val > 0)       s_idx = (uint32_t)(start_val - 1);
        else if (start_val < 0) {
            int64_t t = (int64_t)slen + start_val;
            s_idx = (t < 0) ? 0u : (uint32_t)t;
        } else                   s_idx = 0u;
        if (s_idx > slen) s_idx = slen;

        uint32_t take;
        if (cnt.is_null) {
            take = slen - s_idx;
        } else {
            if (int_arg_null_at(cnt, i)) { out.emit_null(i); continue; }
            int64_t cnt_val = int_arg_value_at(cnt, i);
            if (cnt_val <= 0) { take = 0u; }
            else {
                take = (uint32_t)cnt_val;
                if (s_idx + take > slen) take = slen - s_idx;
            }
        }

        out.emit_bytes(i, sdata + s_idx, take);
    }
    return out.finish();
}

// ---------------------------------------------------------------------------
// NB_MODULE
// ---------------------------------------------------------------------------

NB_MODULE(vector_string_slice, m) {
    m.def("vector_string_slice_left",
        [](nb::object vec, nb::object length) -> nb::object {
            return impl_slice_left(vec, length);
        },
        nb::arg("vec"), nb::arg("length"),
        "Keep first N bytes of each string.  Negative N trims N bytes from the right.\n"
        "length may be a Python int, INT64 Vector, or None (→ all-null output).\n"
        "Null TVL: null vec row or null length → null output row.");

    m.def("vector_string_slice_right",
        [](nb::object vec, nb::object length) -> nb::object {
            return impl_slice_right(vec, length);
        },
        nb::arg("vec"), nb::arg("length"),
        "Keep last N bytes of each string.\n"
        "length may be a Python int, INT64 Vector, or None (→ all-null output).\n"
        "Null TVL: null vec row or null length → null output row.");

    m.def("vector_string_substring",
        [](nb::object vec, nb::object from_pos, nb::object count) -> nb::object {
            return impl_substring(vec, from_pos, count);
        },
        nb::arg("vec"), nb::arg("from_pos"), nb::arg("count") = nb::none(),
        "SQL SUBSTRING(s, from_pos, count).  Position is 1-based; 0 treated as 1;\n"
        "negative counts from end.  count=None means 'to end of string'.\n"
        "from_pos / count may be Python int, INT64 Vector, or None.\n"
        "Null TVL: null vec row, null from_pos, or null count → null output row.");
}
