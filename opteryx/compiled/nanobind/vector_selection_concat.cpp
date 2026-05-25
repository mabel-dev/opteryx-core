// opteryx/compiled/nanobind/vector_selection_concat.cpp — Milestone E.11, Phase 10, C′.
//
// C′ pattern: pure nanobind C++, zero Cython.  One NB_MODULE, 3 functions:
//
//   vector_coalesce(*args)          — N-ary row selection: first non-null across N vectors.
//   vector_iif(mask, true_v, false_v) — ternary: bool mask selects between two value vectors.
//   vector_concat(*args)            — bytewise string concatenation across N string vectors.
//
// vector_coalesce:
//   Inputs: ≥2 Vectors of the same Draken type family (bool/string/fixed-width).
//   Output: same type. Per row: first non-null input value. All-null rows → null.
//   Cross-family type mismatch → TypeError.
//   String output type: VARCHAR unless any input is NVARCHAR (→ NVARCHAR) or VARBINARY (→ VARBINARY).
//
// vector_iif:
//   mask must be DRAKEN_BOOL. true_v and false_v must be same type family.
//   Per row: null/false mask → take false_v row; true mask → take true_v row.
//   Null in mask → treated as FALSE (SQL CASE WHEN NULL THEN x ELSE y = y).
//   Null in selected branch → null in output.
//
// vector_concat:
//   Inputs: ≥2 string-family Vectors (VARCHAR/NVARCHAR/VARBINARY/DICTIONARY/CONSTANT).
//   Output: bytewise concatenation per row.
//   Type promotion: VARCHAR + NVARCHAR → NVARCHAR; any VARBINARY → VARBINARY.
//   Null TVL: any null input at a row → null output row.
//   Length mismatch across inputs → ValueError.
//
// Fails loud on:
//   - Non-Vector arg → TypeError.
//   - Type mismatch within coalesce/iif branches → TypeError.
//   - coalesce/iif: bool-family mixed with other → TypeError.
//   - concat: non-string-family input → TypeError.
//   - Length mismatch → ValueError.
//   - Fewer than 2 inputs to coalesce/concat → ValueError.
//
// Replaces:
//   opteryx/compiled/vector_ops/vector_coalesce.pyx
//   opteryx/compiled/vector_ops/vector_iif.pyx
//   opteryx/compiled/vector_ops/vector_concat.pyx  (vector_string_concat_binary only;
//     vector_concat_array / vector_concat_ws_array had no callers and are deleted)

#include <Python.h>
#include <nanobind/nanobind.h>
#include <cstdint>
#include <cstring>
#include <stdexcept>
#include <vector>

#include "core/buffers.h"
#include "core/alloc.h"
#include "core/string_slot.h"
#include "core/vector_alloc.h"
#include "core/draken_bridge.h"

namespace nb = nanobind;

// ---------------------------------------------------------------------------
// Shared type helpers
// ---------------------------------------------------------------------------

static inline bool is_bool_type(DrakenType t)   { return t == DRAKEN_BOOL; }

static inline bool is_string_type(DrakenType t) {
    return t == DRAKEN_VARCHAR   || t == DRAKEN_NVARCHAR  ||
           t == DRAKEN_VARBINARY || t == DRAKEN_DICTIONARY ||
           t == DRAKEN_CONSTANT;
}

static inline bool is_fixed_type(DrakenType t) {
    return !is_bool_type(t) && !is_string_type(t) &&
           t != DRAKEN_ARRAY && t != DRAKEN_NON_NATIVE &&
           t != DRAKEN_NULL  && t != DRAKEN_VECTOR_FP16;
}

static size_t fixed_itemsize(DrakenType t) {
    switch (t) {
    case DRAKEN_INT8:        return 1u;
    case DRAKEN_INT16:       return 2u;
    case DRAKEN_INT32:       return 4u;
    case DRAKEN_INT64:       return 8u;
    case DRAKEN_DECIMAL:     return 8u;
    case DRAKEN_FLOAT32:     return 4u;
    case DRAKEN_FLOAT64:     return 8u;
    case DRAKEN_DATE32:      return 4u;
    case DRAKEN_TIMESTAMP64: return 8u;
    case DRAKEN_TIME32:      return 4u;
    case DRAKEN_TIME64:      return 8u;
    case DRAKEN_INTERVAL:    return 16u;
    default:                 return 0u;
    }
}

// Canonical string type for promotion: DICTIONARY/CONSTANT → VARCHAR.
static inline DrakenType canon_string_type(DrakenType t) {
    if (t == DRAKEN_NVARCHAR)              return DRAKEN_NVARCHAR;
    if (t == DRAKEN_VARBINARY)             return DRAKEN_VARBINARY;
    return DRAKEN_VARCHAR;  // VARCHAR / DICTIONARY / CONSTANT
}

// Promote two canonical string types. VARBINARY beats NVARCHAR beats VARCHAR.
static inline DrakenType promote_string(DrakenType a, DrakenType b) {
    DrakenType ca = canon_string_type(a), cb = canon_string_type(b);
    if (ca == DRAKEN_VARBINARY || cb == DRAKEN_VARBINARY) return DRAKEN_VARBINARY;
    if (ca == DRAKEN_NVARCHAR  || cb == DRAKEN_NVARCHAR)  return DRAKEN_NVARCHAR;
    return DRAKEN_VARCHAR;
}

// ---------------------------------------------------------------------------
// Row-level access helpers (uniform access pattern: data[selection[i]])
// ---------------------------------------------------------------------------

static inline bool row_is_valid(const DrakenVector* dv, uint32_t row) {
    if (!dv->validity) return true;
    return ((dv->validity[row >> 3] >> (row & 7u)) & 1u) != 0u;
}

static inline void set_bit(uint8_t* bits, uint32_t i) {
    bits[i >> 3u] |= static_cast<uint8_t>(1u << (i & 7u));
}

static inline bool read_bool_row(const DrakenVector* dv, uint32_t row) {
    uint32_t code = dv->selection[row];
    return ((static_cast<const uint8_t*>(dv->data)[code >> 3u] >> (code & 7u)) & 1u) != 0u;
}

struct StrView { const uint8_t* data; uint32_t len; };

static inline StrView read_string_row(const DrakenVector* dv, uint32_t row) {
    const DrakenStringArena* sa   = static_cast<const DrakenStringArena*>(dv->data);
    const DrakenStringSlot*  slot = &sa->slots[dv->selection[row]];
    return { str_data(slot, sa->arena), str_length(slot) };
}

// Allocate a zero-filled validity bitmap (bit=1 valid). Returns nullptr when n==0.
static uint8_t* alloc_validity(uint32_t n) {
    if (n == 0u) return nullptr;
    const uint32_t bm     = (n + 7u) >> 3u;
    const uint32_t padded = (bm + 7u) & ~7u;
    const size_t   sz     = padded ? static_cast<size_t>(padded) : 8u;
    uint8_t* v = static_cast<uint8_t*>(draken_malloc(sz));
    if (!v) throw std::bad_alloc();
    std::memset(v, 0, sz);  // 0 = null; set bits as valid rows are found
    return v;
}

// Unwrap a Python object to DrakenVector*. Raises TypeError on failure.
static const DrakenVector* unwrap(nb::object obj, const char* fn) {
    const DrakenVector* dv = draken_vector_unwrap(obj.ptr());
    if (!dv) throw nb::python_error();  // TypeError already set by unwrap
    return dv;
}

// Wrap hand-allocated buffers in Python Vector.
static nb::object own_raw(void* data, uint8_t* validity, uint32_t length, DrakenType type) {
    PyObject* out = draken_vector_own_raw(data, validity, length, type);
    if (!out) throw nb::python_error();
    return nb::steal<nb::object>(out);
}

static nb::object own_string(DrakenStringSlot* slots, uint8_t* arena, size_t arena_len,
                              uint8_t* validity, uint32_t length, DrakenType type) {
    PyObject* out = draken_vector_own_string(slots, arena, arena_len, validity, length, type);
    if (!out) throw nb::python_error();
    return nb::steal<nb::object>(out);
}

// ---------------------------------------------------------------------------
// RAII guard for in-flight string output buffers
// ---------------------------------------------------------------------------

struct StrOutGuard {
    DrakenStringSlot* slots    = nullptr;
    uint8_t*          arena    = nullptr;
    uint8_t*          validity = nullptr;
    ~StrOutGuard() {
        if (slots)    draken_free(slots);
        if (arena)    draken_free(arena);
        if (validity) draken_free(validity);
    }
    void release() { slots = nullptr; arena = nullptr; validity = nullptr; }
};

// ---------------------------------------------------------------------------
// Bool coalesce / iif kernels
// ---------------------------------------------------------------------------

static nb::object coalesce_bool(const std::vector<const DrakenVector*>& vecs, uint32_t n) {
    const uint32_t padded = ((((n + 7u) >> 3u) + 7u) & ~7u);
    const size_t   sz     = padded ? static_cast<size_t>(padded) : 8u;

    uint8_t* out_bits = static_cast<uint8_t*>(draken_malloc(sz));
    if (!out_bits) throw std::bad_alloc();
    std::memset(out_bits, 0, sz);

    uint8_t* out_valid = alloc_validity(n);

    struct BG { uint8_t* d; uint8_t* v;
                ~BG() { if (d) draken_free(d); if (v) draken_free(v); } } g{out_bits, out_valid};

    bool any_null = false;
    for (uint32_t row = 0u; row < n; ++row) {
        bool found = false;
        for (const DrakenVector* dv : vecs) {
            if (!row_is_valid(dv, row)) continue;
            if (read_bool_row(dv, row)) set_bit(out_bits, row);
            if (out_valid)              set_bit(out_valid, row);
            found = true;
            break;
        }
        if (!found) any_null = true;
    }
    if (!any_null && out_valid) { draken_free(out_valid); out_valid = nullptr; g.v = nullptr; }

    g.d = nullptr; g.v = nullptr;
    return own_raw(out_bits, out_valid, n, DRAKEN_BOOL);
}

static nb::object iif_bool(const DrakenVector* cond, const DrakenVector* true_v,
                            const DrakenVector* false_v, uint32_t n) {
    const uint32_t padded = ((((n + 7u) >> 3u) + 7u) & ~7u);
    const size_t   sz     = padded ? static_cast<size_t>(padded) : 8u;

    uint8_t* out_bits = static_cast<uint8_t*>(draken_malloc(sz));
    if (!out_bits) throw std::bad_alloc();
    std::memset(out_bits, 0, sz);

    uint8_t* out_valid = alloc_validity(n);

    struct BG { uint8_t* d; uint8_t* v;
                ~BG() { if (d) draken_free(d); if (v) draken_free(v); } } g{out_bits, out_valid};

    bool any_null = false;
    for (uint32_t row = 0u; row < n; ++row) {
        // Null or false mask → choose false_v (SQL: CASE WHEN NULL = ELSE branch).
        bool choose_true = row_is_valid(cond, row) && read_bool_row(cond, row);
        const DrakenVector* src = choose_true ? true_v : false_v;
        if (!row_is_valid(src, row)) { any_null = true; continue; }
        if (read_bool_row(src, row)) set_bit(out_bits, row);
        if (out_valid)               set_bit(out_valid, row);
    }
    if (!any_null && out_valid) { draken_free(out_valid); out_valid = nullptr; g.v = nullptr; }

    g.d = nullptr; g.v = nullptr;
    return own_raw(out_bits, out_valid, n, DRAKEN_BOOL);
}

// ---------------------------------------------------------------------------
// Fixed-width coalesce / iif kernels
// ---------------------------------------------------------------------------

static nb::object coalesce_fixed(const std::vector<const DrakenVector*>& vecs,
                                  uint32_t n, DrakenType type) {
    const size_t isz    = fixed_itemsize(type);
    const size_t alloc  = n > 0u ? static_cast<size_t>(n) * isz : isz;
    uint8_t* out_data   = static_cast<uint8_t*>(draken_malloc(alloc));
    if (!out_data) throw std::bad_alloc();
    std::memset(out_data, 0, alloc);

    uint8_t* out_valid = alloc_validity(n);

    struct FG { uint8_t* d; uint8_t* v;
                ~FG() { if (d) draken_free(d); if (v) draken_free(v); } } g{out_data, out_valid};

    bool any_null = false;
    for (uint32_t row = 0u; row < n; ++row) {
        bool found = false;
        for (const DrakenVector* dv : vecs) {
            if (!row_is_valid(dv, row)) continue;
            std::memcpy(out_data + static_cast<size_t>(row) * isz,
                        static_cast<const uint8_t*>(dv->data) + static_cast<size_t>(dv->selection[row]) * isz,
                        isz);
            if (out_valid) set_bit(out_valid, row);
            found = true;
            break;
        }
        if (!found) any_null = true;
    }
    if (!any_null && out_valid) { draken_free(out_valid); out_valid = nullptr; g.v = nullptr; }

    g.d = nullptr; g.v = nullptr;
    return own_raw(out_data, out_valid, n, type);
}

static nb::object iif_fixed(const DrakenVector* cond, const DrakenVector* true_v,
                              const DrakenVector* false_v, uint32_t n, DrakenType type) {
    const size_t isz   = fixed_itemsize(type);
    const size_t alloc = n > 0u ? static_cast<size_t>(n) * isz : isz;
    uint8_t* out_data  = static_cast<uint8_t*>(draken_malloc(alloc));
    if (!out_data) throw std::bad_alloc();
    std::memset(out_data, 0, alloc);

    uint8_t* out_valid = alloc_validity(n);

    struct FG { uint8_t* d; uint8_t* v;
                ~FG() { if (d) draken_free(d); if (v) draken_free(v); } } g{out_data, out_valid};

    bool any_null = false;
    for (uint32_t row = 0u; row < n; ++row) {
        bool choose_true = row_is_valid(cond, row) && read_bool_row(cond, row);
        const DrakenVector* src = choose_true ? true_v : false_v;
        if (!row_is_valid(src, row)) { any_null = true; continue; }
        std::memcpy(out_data + static_cast<size_t>(row) * isz,
                    static_cast<const uint8_t*>(src->data) + static_cast<size_t>(src->selection[row]) * isz,
                    isz);
        if (out_valid) set_bit(out_valid, row);
    }
    if (!any_null && out_valid) { draken_free(out_valid); out_valid = nullptr; g.v = nullptr; }

    g.d = nullptr; g.v = nullptr;
    return own_raw(out_data, out_valid, n, type);
}

// ---------------------------------------------------------------------------
// String coalesce / iif kernels (2-pass: budget → fill)
// ---------------------------------------------------------------------------

static nb::object coalesce_string(const std::vector<const DrakenVector*>& vecs,
                                   uint32_t n, DrakenType out_type) {
    // Pass 1: total extern arena budget.
    size_t total_bytes = 0u;
    for (uint32_t row = 0u; row < n; ++row) {
        for (const DrakenVector* dv : vecs) {
            if (!row_is_valid(dv, row)) continue;
            StrView sv = read_string_row(dv, row);
            if (sv.len > STR_INLINE_MAX) total_bytes += sv.len;
            break;  // first non-null only
        }
    }

    const size_t slots_sz  = (n > 0u ? n : 1u) * sizeof(DrakenStringSlot);
    const size_t arena_sz  = total_bytes > 0u ? total_bytes : 1u;

    StrOutGuard g;
    g.slots    = static_cast<DrakenStringSlot*>(draken_malloc(slots_sz));
    if (!g.slots) throw std::bad_alloc();
    std::memset(g.slots, 0, slots_sz);

    g.arena = static_cast<uint8_t*>(draken_malloc(arena_sz));
    if (!g.arena) throw std::bad_alloc();

    g.validity = alloc_validity(n);

    bool     any_null  = false;
    size_t   arena_pos = 0u;

    // Pass 2: fill slots.
    for (uint32_t row = 0u; row < n; ++row) {
        bool found = false;
        for (const DrakenVector* dv : vecs) {
            if (!row_is_valid(dv, row)) continue;
            StrView sv = read_string_row(dv, row);
            if (sv.len > STR_INLINE_MAX) {
                const uint32_t off = static_cast<uint32_t>(arena_pos);
                std::memcpy(g.arena + off, sv.data, sv.len);
                draken_build_string_slot(&g.slots[row], sv.data, sv.len, off);
                arena_pos += sv.len;
            } else {
                draken_build_string_slot(&g.slots[row], sv.data, sv.len, 0u);
            }
            if (g.validity) set_bit(g.validity, row);
            found = true;
            break;
        }
        if (!found) {
            str_init_null(&g.slots[row]);
            any_null = true;
        }
    }
    if (!any_null && g.validity) { draken_free(g.validity); g.validity = nullptr; }

    DrakenStringSlot* slots = g.slots; uint8_t* arena = g.arena; uint8_t* validity = g.validity;
    g.release();
    return own_string(slots, arena, arena_pos, validity, n, out_type);
}

static nb::object iif_string(const DrakenVector* cond, const DrakenVector* true_v,
                               const DrakenVector* false_v, uint32_t n, DrakenType out_type) {
    // Pass 1: total extern arena budget.
    size_t total_bytes = 0u;
    for (uint32_t row = 0u; row < n; ++row) {
        bool choose_true = row_is_valid(cond, row) && read_bool_row(cond, row);
        const DrakenVector* src = choose_true ? true_v : false_v;
        if (!row_is_valid(src, row)) continue;
        StrView sv = read_string_row(src, row);
        if (sv.len > STR_INLINE_MAX) total_bytes += sv.len;
    }

    const size_t slots_sz  = (n > 0u ? n : 1u) * sizeof(DrakenStringSlot);
    const size_t arena_sz  = total_bytes > 0u ? total_bytes : 1u;

    StrOutGuard g;
    g.slots = static_cast<DrakenStringSlot*>(draken_malloc(slots_sz));
    if (!g.slots) throw std::bad_alloc();
    std::memset(g.slots, 0, slots_sz);

    g.arena = static_cast<uint8_t*>(draken_malloc(arena_sz));
    if (!g.arena) throw std::bad_alloc();

    g.validity = alloc_validity(n);

    bool   any_null  = false;
    size_t arena_pos = 0u;

    // Pass 2: fill slots.
    for (uint32_t row = 0u; row < n; ++row) {
        bool choose_true = row_is_valid(cond, row) && read_bool_row(cond, row);
        const DrakenVector* src = choose_true ? true_v : false_v;
        if (!row_is_valid(src, row)) {
            str_init_null(&g.slots[row]);
            any_null = true;
            continue;
        }
        StrView sv = read_string_row(src, row);
        if (sv.len > STR_INLINE_MAX) {
            const uint32_t off = static_cast<uint32_t>(arena_pos);
            std::memcpy(g.arena + off, sv.data, sv.len);
            draken_build_string_slot(&g.slots[row], sv.data, sv.len, off);
            arena_pos += sv.len;
        } else {
            draken_build_string_slot(&g.slots[row], sv.data, sv.len, 0u);
        }
        if (g.validity) set_bit(g.validity, row);
    }
    if (!any_null && g.validity) { draken_free(g.validity); g.validity = nullptr; }

    DrakenStringSlot* slots = g.slots; uint8_t* arena = g.arena; uint8_t* validity = g.validity;
    g.release();
    return own_string(slots, arena, arena_pos, validity, n, out_type);
}

// ---------------------------------------------------------------------------
// String concat kernel (N-ary bytewise, 2-pass)
// ---------------------------------------------------------------------------

static nb::object impl_concat(const std::vector<const DrakenVector*>& vecs,
                               uint32_t n, DrakenType out_type) {
    // Pass 1: total extern arena budget per row (sum of non-null row widths).
    size_t total_bytes = 0u;
    for (uint32_t row = 0u; row < n; ++row) {
        // If any input is null this row → null output; skip arena budget.
        bool row_null = false;
        size_t row_extern = 0u;
        for (const DrakenVector* dv : vecs) {
            if (!row_is_valid(dv, row)) { row_null = true; break; }
            StrView sv = read_string_row(dv, row);
            if (sv.len > STR_INLINE_MAX) row_extern += sv.len;
        }
        if (!row_null) total_bytes += row_extern;
    }

    const size_t slots_sz  = (n > 0u ? n : 1u) * sizeof(DrakenStringSlot);
    const size_t arena_sz  = total_bytes > 0u ? total_bytes : 1u;

    StrOutGuard g;
    g.slots = static_cast<DrakenStringSlot*>(draken_malloc(slots_sz));
    if (!g.slots) throw std::bad_alloc();
    std::memset(g.slots, 0, slots_sz);

    g.arena = static_cast<uint8_t*>(draken_malloc(arena_sz));
    if (!g.arena) throw std::bad_alloc();

    g.validity = alloc_validity(n);

    // Temp scratch buffer for assembling inline concats (or fitting small concats).
    // We grow it on demand; max inline is STR_INLINE_MAX so start at 64.
    size_t   scratch_cap = 64u;
    uint8_t* scratch     = static_cast<uint8_t*>(draken_malloc(scratch_cap));
    if (!scratch) throw std::bad_alloc();

    bool   any_null  = false;
    size_t arena_pos = 0u;

    // Pass 2: fill slots.
    for (uint32_t row = 0u; row < n; ++row) {
        // Check for null inputs first.
        bool row_null = false;
        for (const DrakenVector* dv : vecs) {
            if (!row_is_valid(dv, row)) { row_null = true; break; }
        }
        if (row_null) {
            str_init_null(&g.slots[row]);
            any_null = true;
            continue;
        }

        // Compute total byte length for this row.
        uint32_t row_len = 0u;
        for (const DrakenVector* dv : vecs) {
            StrView sv = read_string_row(dv, row);
            row_len += sv.len;
        }

        if (row_len == 0u) {
            draken_build_string_slot(&g.slots[row], nullptr, 0u, 0u);
            if (g.validity) set_bit(g.validity, row);
            continue;
        }

        // Ensure scratch is large enough.
        if (static_cast<size_t>(row_len) > scratch_cap) {
            draken_free(scratch);
            scratch_cap = static_cast<size_t>(row_len) + 64u;
            scratch = static_cast<uint8_t*>(draken_malloc(scratch_cap));
            if (!scratch) {
                draken_free(scratch); scratch = nullptr;
                throw std::bad_alloc();
            }
        }

        // Assemble concatenated bytes in scratch.
        uint32_t pos = 0u;
        for (const DrakenVector* dv : vecs) {
            StrView sv = read_string_row(dv, row);
            if (sv.len > 0u) {
                std::memcpy(scratch + pos, sv.data, sv.len);
                pos += sv.len;
            }
        }

        if (row_len > STR_INLINE_MAX) {
            const uint32_t off = static_cast<uint32_t>(arena_pos);
            std::memcpy(g.arena + off, scratch, row_len);
            draken_build_string_slot(&g.slots[row], scratch, row_len, off);
            arena_pos += row_len;
        } else {
            draken_build_string_slot(&g.slots[row], scratch, row_len, 0u);
        }
        if (g.validity) set_bit(g.validity, row);
    }
    draken_free(scratch);

    if (!any_null && g.validity) { draken_free(g.validity); g.validity = nullptr; }

    DrakenStringSlot* slots = g.slots; uint8_t* arena = g.arena; uint8_t* validity = g.validity;
    g.release();
    return own_string(slots, arena, arena_pos, validity, n, out_type);
}

// ---------------------------------------------------------------------------
// Public entry points (validation + dispatch)
// ---------------------------------------------------------------------------

static nb::object impl_coalesce(nb::args args) {
    const Py_ssize_t argc = static_cast<Py_ssize_t>(args.size());
    if (argc < 2)
        throw nb::value_error("vector_coalesce: requires at least 2 arguments");

    std::vector<const DrakenVector*> vecs;
    vecs.reserve(static_cast<size_t>(argc));
    for (Py_ssize_t i = 0; i < argc; ++i)
        vecs.push_back(unwrap(args[i], "vector_coalesce"));

    const uint32_t n = vecs[0]->length;
    for (Py_ssize_t i = 1; i < argc; ++i) {
        if (vecs[i]->length != n) {
            PyErr_Format(PyExc_ValueError,
                "vector_coalesce: argument %zd length %u does not match argument 0 length %u",
                i, vecs[i]->length, n);
            throw nb::python_error();
        }
    }

    const DrakenType t0 = vecs[0]->type;

    if (is_bool_type(t0)) {
        for (Py_ssize_t i = 1; i < argc; ++i) {
            if (!is_bool_type(vecs[i]->type)) {
                PyErr_Format(PyExc_TypeError,
                    "vector_coalesce: argument %zd type %d is not BOOL (expected BOOL)",
                    i, static_cast<int>(vecs[i]->type));
                throw nb::python_error();
            }
        }
        return coalesce_bool(vecs, n);
    }

    if (is_string_type(t0)) {
        DrakenType out_type = canon_string_type(t0);
        for (Py_ssize_t i = 1; i < argc; ++i) {
            if (!is_string_type(vecs[i]->type)) {
                PyErr_Format(PyExc_TypeError,
                    "vector_coalesce: argument %zd type %d is not string-family (expected string-family)",
                    i, static_cast<int>(vecs[i]->type));
                throw nb::python_error();
            }
            out_type = promote_string(out_type, vecs[i]->type);
        }
        return coalesce_string(vecs, n, out_type);
    }

    if (is_fixed_type(t0)) {
        const size_t isz = fixed_itemsize(t0);
        if (isz == 0u) {
            PyErr_Format(PyExc_TypeError,
                "vector_coalesce: unsupported fixed-width type %d",
                static_cast<int>(t0));
            throw nb::python_error();
        }
        for (Py_ssize_t i = 1; i < argc; ++i) {
            if (vecs[i]->type != t0) {
                PyErr_Format(PyExc_TypeError,
                    "vector_coalesce: argument %zd type %d does not match argument 0 type %d",
                    i, static_cast<int>(vecs[i]->type), static_cast<int>(t0));
                throw nb::python_error();
            }
        }
        return coalesce_fixed(vecs, n, t0);
    }

    PyErr_Format(PyExc_TypeError,
        "vector_coalesce: unsupported type %d on argument 0",
        static_cast<int>(t0));
    throw nb::python_error();
}

static nb::object impl_iif(nb::object mask_obj, nb::object true_obj, nb::object false_obj) {
    const DrakenVector* cond    = unwrap(mask_obj,  "vector_iif");
    const DrakenVector* true_v  = unwrap(true_obj,  "vector_iif");
    const DrakenVector* false_v = unwrap(false_obj, "vector_iif");

    if (!is_bool_type(cond->type))
        throw nb::type_error("vector_iif: mask must be a BOOL Vector");

    const uint32_t n = cond->length;
    if (true_v->length != n || false_v->length != n) {
        PyErr_Format(PyExc_ValueError,
            "vector_iif: length mismatch: mask %u, true_v %u, false_v %u",
            n, true_v->length, false_v->length);
        throw nb::python_error();
    }

    const DrakenType tt = true_v->type, ft = false_v->type;

    if (is_bool_type(tt) && is_bool_type(ft))
        return iif_bool(cond, true_v, false_v, n);

    if (is_bool_type(tt) || is_bool_type(ft)) {
        PyErr_Format(PyExc_TypeError,
            "vector_iif: branch type mismatch: BOOL vs non-BOOL (true_v %d, false_v %d)",
            static_cast<int>(tt), static_cast<int>(ft));
        throw nb::python_error();
    }

    if (is_string_type(tt) && is_string_type(ft)) {
        DrakenType out_type = promote_string(tt, ft);
        return iif_string(cond, true_v, false_v, n, out_type);
    }

    if (is_string_type(tt) || is_string_type(ft)) {
        PyErr_Format(PyExc_TypeError,
            "vector_iif: branch type mismatch: string-family vs non-string (true_v %d, false_v %d)",
            static_cast<int>(tt), static_cast<int>(ft));
        throw nb::python_error();
    }

    if (is_fixed_type(tt) && is_fixed_type(ft)) {
        if (tt != ft) {
            PyErr_Format(PyExc_TypeError,
                "vector_iif: fixed-width branch type mismatch (true_v %d, false_v %d)",
                static_cast<int>(tt), static_cast<int>(ft));
            throw nb::python_error();
        }
        const size_t isz = fixed_itemsize(tt);
        if (isz == 0u) {
            PyErr_Format(PyExc_TypeError,
                "vector_iif: unsupported fixed-width type %d", static_cast<int>(tt));
            throw nb::python_error();
        }
        return iif_fixed(cond, true_v, false_v, n, tt);
    }

    PyErr_Format(PyExc_TypeError,
        "vector_iif: unsupported branch types (true_v %d, false_v %d)",
        static_cast<int>(tt), static_cast<int>(ft));
    throw nb::python_error();
}

static nb::object impl_vector_concat(nb::args args) {
    const Py_ssize_t argc = static_cast<Py_ssize_t>(args.size());
    if (argc < 2)
        throw nb::value_error("vector_concat: requires at least 2 arguments");

    std::vector<const DrakenVector*> vecs;
    vecs.reserve(static_cast<size_t>(argc));
    for (Py_ssize_t i = 0; i < argc; ++i) {
        const DrakenVector* dv = unwrap(args[i], "vector_concat");
        if (!is_string_type(dv->type)) {
            PyErr_Format(PyExc_TypeError,
                "vector_concat: argument %zd type %d is not string-family "
                "(expected VARCHAR/NVARCHAR/VARBINARY/DICTIONARY/CONSTANT)",
                i, static_cast<int>(dv->type));
            throw nb::python_error();
        }
        vecs.push_back(dv);
    }

    const uint32_t n = vecs[0]->length;
    for (Py_ssize_t i = 1; i < argc; ++i) {
        if (vecs[i]->length != n) {
            PyErr_Format(PyExc_ValueError,
                "vector_concat: argument %zd length %u does not match argument 0 length %u",
                i, vecs[i]->length, n);
            throw nb::python_error();
        }
    }

    // Compute output type via promotion.
    DrakenType out_type = canon_string_type(vecs[0]->type);
    for (Py_ssize_t i = 1; i < argc; ++i)
        out_type = promote_string(out_type, vecs[i]->type);

    return impl_concat(vecs, n, out_type);
}

// ---------------------------------------------------------------------------
// NB_MODULE
// ---------------------------------------------------------------------------

NB_MODULE(vector_selection_concat, m) {

    m.def("vector_coalesce",
        [](nb::args args) -> nb::object { return impl_coalesce(args); },
        "SQL COALESCE across ≥2 Vectors: first non-null per row. "
        "All inputs must be same type family (bool / string / fixed-width). "
        "Fixed-width: exact type match required. "
        "String: promotes VARCHAR+NVARCHAR→NVARCHAR, VARBINARY beats all. "
        "Fails loud on non-Vector, type mismatch, or length mismatch.");

    m.def("vector_iif",
        [](nb::object mask, nb::object tv, nb::object fv) -> nb::object {
            return impl_iif(mask, tv, fv);
        },
        nb::arg("mask"), nb::arg("when_true"), nb::arg("when_false"),
        "SQL IIF(mask, when_true, when_false). "
        "mask must be BOOL; branches must be same type family. "
        "Null or false mask → when_false (SQL CASE WHEN NULL = ELSE branch). "
        "Null in selected branch → null in output. "
        "Fails loud on non-Vector, type mismatch, or length mismatch.");

    m.def("vector_concat",
        [](nb::args args) -> nb::object { return impl_vector_concat(args); },
        "Bytewise string concatenation across ≥2 string-family Vectors. "
        "Type promotion: VARCHAR+NVARCHAR→NVARCHAR, any VARBINARY→VARBINARY. "
        "Null TVL: any null input at a row → null output row. "
        "Fails loud on non-Vector, non-string-family input, or length mismatch.");
}
