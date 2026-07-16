// opteryx/compiled/nanobind/vector_selection_concat.cpp — Milestone E.11, Phase 10, C′.
//
// C′ pattern: pure nanobind C++, zero Cython.  One NB_MODULE, 1 function:
//
//   vector_concat(*args)            — bytewise string concatenation across N string vectors.
//
// vector_coalesce / vector_iif / vector_ifnull / vector_ifnotnull USED to live here.
// They are deleted: COALESCE/IIF/IFNULL/IFNOTNULL are now C-ABI kernels
// (draken/ops/kernels/function_null_conditional.cpp), which is their sole
// implementation. These bindings were never reachable from the engine anyway — the
// plan compiler refuses any expression without a c-native kernel, so the registrar's
// callable_ref pointed at code the executor could not call. Keeping them would have
// meant two copies of the null/conditional semantics (CLAUDE.md §2).
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
//   - concat: non-string-family input → TypeError.
//   - Length mismatch → ValueError.
//   - Fewer than 2 inputs to concat → ValueError.
//
// Replaces:
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

static inline bool is_string_type(DrakenType t) {
    return t == DRAKEN_VARCHAR  || t == DRAKEN_NVARCHAR ||
           t == DRAKEN_VARBINARY;
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


static nb::object impl_concat(const std::vector<const DrakenVector*>& vecs,
                               uint32_t n, DrakenType out_type) {
    // Pass 1: total extern arena budget per row (sum of non-null row widths).
    //
    // The arena holds a row's bytes iff the *concatenated* length exceeds the
    // inline limit — that is the exact decision Pass 2 makes below. The budget
    // must use the same predicate on the same quantity (the per-row sum), not
    // the per-operand length: two individually-inline operands can concatenate
    // past STR_INLINE_MAX (e.g. 7 + 7 = 14), which still needs arena bytes. A
    // per-operand test under-budgets those rows and Pass 2 overruns the arena.
    size_t total_bytes = 0u;
    for (uint32_t row = 0u; row < n; ++row) {
        // If any input is null this row → null output; skip arena budget.
        bool row_null = false;
        size_t row_len = 0u;
        for (const DrakenVector* dv : vecs) {
            if (!row_is_valid(dv, row)) { row_null = true; break; }
            StrView sv = read_string_row(dv, row);
            row_len += sv.len;
        }
        if (!row_null && row_len > STR_INLINE_MAX) total_bytes += row_len;
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

void register_vector_selection_concat(nb::module_ &m) {

    m.def("vector_concat",
        [](nb::args args) -> nb::object { return impl_vector_concat(args); },
        "Bytewise string concatenation across ≥2 string-family Vectors. "
        "Type promotion: VARCHAR+NVARCHAR→NVARCHAR, any VARBINARY→VARBINARY. "
        "Null TVL: any null input at a row → null output row. "
        "Fails loud on non-Vector, non-string-family input, or length mismatch.");
}
