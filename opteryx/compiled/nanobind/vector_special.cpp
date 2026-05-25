// opteryx/compiled/nanobind/vector_special.cpp — Milestone E.19.
//
// C′ pattern: pure nanobind C++, zero Cython.  One NB_MODULE, two functions.
//
//   vector_map_access_string(vec, key) — character subscript on StringVector.
//   vector_map_access_array(vec, key)  — element subscript on ArrayVector.
//
// Both inputs:
//   vec — source DrakenVector (string-family or DRAKEN_ARRAY).
//   key — Integer64Vector; constant-encoded; caller enforces that all values
//         are identical and passes a single-row vector (selection[0] is the index).
//
// Null TVL:
//   Null input row   → null output row.
//   Out-of-range     → null output row (SQL convention).
//   Negative index   → Python convention (counts from end).
//
// Replaces:
//   opteryx/compiled/vector_ops/vector_subscript.pyx (deleted as part of E.19).
//   vector_get_element from that file is dead (zero callers) and not reproduced.

#include <Python.h>
#include <nanobind/nanobind.h>
#include <cstdint>
#include <cstring>
#include <stdexcept>

#include "core/buffers.h"
#include "core/alloc.h"
#include "core/string_slot.h"
#include "core/vector_alloc.h"
#include "core/draken_bridge.h"

namespace nb = nanobind;

// ---------------------------------------------------------------------------
// Shared validity helpers
// ---------------------------------------------------------------------------

static inline bool sv_row_valid(const DrakenVector* dv, uint32_t i) noexcept {
    return (dv->validity == nullptr) || ((dv->validity[i >> 3] >> (i & 7u)) & 1u);
}

// Lazily allocate an all-valid validity bitmap for n logical rows, then
// mark row i as null.
static inline void sv_mark_null(uint8_t*& validity, uint32_t i, uint32_t n) {
    if (!validity) {
        const uint32_t nb_bytes = (n + 7u) >> 3;
        validity = static_cast<uint8_t*>(draken_malloc(nb_bytes));
        if (!validity) throw std::bad_alloc();
        std::memset(validity, 0xFFu, nb_bytes);
    }
    validity[i >> 3] &= ~static_cast<uint8_t>(1u << (i & 7u));
}

// ---------------------------------------------------------------------------
// vector_map_access_string
//
// Character subscript: vec[key] → StringVector of 1-byte slices.
// Single-char output fits inline (STR_INLINE_MAX = 12); no arena needed.
// ---------------------------------------------------------------------------

static nb::object impl_map_access_string(nb::object vec_obj, nb::object key_obj) {
    const DrakenVector* dv = draken_vector_unwrap(vec_obj.ptr());
    if (!dv) throw nb::python_error();
    const bool is_str =
        dv->type == DRAKEN_VARCHAR   ||
        dv->type == DRAKEN_NVARCHAR  ||
        dv->type == DRAKEN_VARBINARY ||
        dv->type == DRAKEN_DICTIONARY||
        dv->type == DRAKEN_CONSTANT;
    if (!is_str)
        throw nb::type_error(
            "vector_map_access_string: expected a string-family DrakenVector");

    const DrakenVector* kv = draken_vector_unwrap(key_obj.ptr());
    if (!kv) throw nb::python_error();
    if (kv->type != DRAKEN_INT64)
        throw nb::type_error(
            "vector_map_access_string: key must be an Integer64Vector");

    const int64_t index = static_cast<const int64_t*>(kv->data)[kv->selection[0]];
    const uint32_t n    = dv->length;
    const DrakenStringArena* arena =
        static_cast<const DrakenStringArena*>(dv->data);

    const size_t slots_sz = (n > 0u ? n : 1u) * sizeof(DrakenStringSlot);
    auto* slots = static_cast<DrakenStringSlot*>(draken_malloc(slots_sz));
    if (!slots) throw std::bad_alloc();
    std::memset(slots, 0, slots_sz);

    uint8_t* validity = nullptr;
    bool any_null = false;

    for (uint32_t i = 0u; i < n; ++i) {
        if (!sv_row_valid(dv, i)) {
            str_init_null(&slots[i]);
            sv_mark_null(validity, i, n);
            any_null = true;
            continue;
        }
        const DrakenStringSlot* src_slot = &arena->slots[dv->selection[i]];
        const uint8_t* sdata = str_data(src_slot, arena->arena);
        const uint32_t slen  = str_length(src_slot);
        int64_t pos = (index >= 0) ? index : (int64_t)slen + index;
        if (pos < 0 || pos >= (int64_t)slen) {
            str_init_null(&slots[i]);
            sv_mark_null(validity, i, n);
            any_null = true;
        } else {
            str_init_inline(&slots[i], sdata + (uint32_t)pos, 1u);
        }
    }

    if (!any_null && validity) { draken_free(validity); validity = nullptr; }

    PyObject* out = draken_vector_own_string(
        slots, nullptr, 0u, validity, n, DRAKEN_VARCHAR);
    if (!out) {
        draken_free(slots);
        if (validity) draken_free(validity);
        throw nb::python_error();
    }
    return nb::steal<nb::object>(out);
}

// ---------------------------------------------------------------------------
// vector_map_access_array
//
// Element subscript on ArrayVector: vec[key] → Python list of extracted items.
// ArrayVector rows are Python objects (lists); access requires the Python C API.
// The caller wraps the result with vector_from_sequence.
// ---------------------------------------------------------------------------

static nb::object impl_map_access_array(nb::object vec_obj, nb::object key_obj) {
    const DrakenVector* kv = draken_vector_unwrap(key_obj.ptr());
    if (!kv) throw nb::python_error();
    if (kv->type != DRAKEN_INT64)
        throw nb::type_error(
            "vector_map_access_array: key must be an Integer64Vector");

    const int64_t index  = static_cast<const int64_t*>(kv->data)[kv->selection[0]];

    const DrakenVector* av = draken_vector_unwrap(vec_obj.ptr());
    if (!av) throw nb::python_error();
    const uint32_t n = av->length;

    PyObject* result = PyList_New((Py_ssize_t)n);
    if (!result) throw nb::python_error();

    for (uint32_t i = 0u; i < n; ++i) {
        PyObject* py_i = PyLong_FromUnsignedLong((unsigned long)i);
        if (!py_i) { Py_DECREF(result); throw nb::python_error(); }
        PyObject* row = PyObject_GetItem(vec_obj.ptr(), py_i);
        Py_DECREF(py_i);
        if (!row) { Py_DECREF(result); throw nb::python_error(); }

        if (row == Py_None) {
            PyList_SET_ITEM(result, (Py_ssize_t)i, row);  // steals ref to None
            continue;
        }

        Py_ssize_t row_len = PySequence_Size(row);
        if (row_len < 0) { Py_DECREF(row); Py_DECREF(result); throw nb::python_error(); }
        int64_t pos = (index >= 0) ? index : (int64_t)row_len + index;

        if (pos < 0 || pos >= (int64_t)row_len) {
            Py_DECREF(row);
            Py_INCREF(Py_None);
            PyList_SET_ITEM(result, (Py_ssize_t)i, Py_None);
        } else {
            PyObject* elem = PySequence_GetItem(row, (Py_ssize_t)pos);
            Py_DECREF(row);
            if (!elem) { Py_DECREF(result); throw nb::python_error(); }
            PyList_SET_ITEM(result, (Py_ssize_t)i, elem);
        }
    }

    return nb::steal<nb::object>(result);
}

// ---------------------------------------------------------------------------
// NB_MODULE
// ---------------------------------------------------------------------------

NB_MODULE(vector_special, m) {

    m.def("vector_map_access_string",
        [](nb::object vec, nb::object key) -> nb::object {
            return impl_map_access_string(vec, key);
        },
        nb::arg("vec"), nb::arg("key"),
        "Character subscript on a string DrakenVector.\n"
        "key: Integer64Vector — constant index (positive or negative).\n"
        "Returns a VARCHAR DrakenVector of 1-byte slices.\n"
        "Null TVL: null input row or out-of-range index → null output row.");

    m.def("vector_map_access_array",
        [](nb::object vec, nb::object key) -> nb::object {
            return impl_map_access_array(vec, key);
        },
        nb::arg("vec"), nb::arg("key"),
        "Element subscript on an ArrayVector.\n"
        "key: Integer64Vector — constant index (positive or negative).\n"
        "Returns a Python list; caller wraps with vector_from_sequence.\n"
        "Null TVL: null row or out-of-range index → None in output list.");
}
