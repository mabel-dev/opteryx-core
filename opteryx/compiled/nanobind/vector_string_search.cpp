// opteryx/compiled/nanobind/vector_string_search.cpp — Milestone E.10, C′.
//
// C′ pattern: pure nanobind C++, zero Cython.  One NB_MODULE, 7 functions:
//
// String-family search (haystack=Vector, needle=Vector → DRAKEN_BOOL):
//   vector_starts_with         — CS byte-level prefix match.
//   vector_ci_starts_with      — ASCII-CI prefix match (needle pre-lowercased here).
//   vector_ends_with           — CS byte-level suffix match.
//   vector_ci_ends_with        — ASCII-CI suffix match.
//   vector_contains(v, ndl, ignore_case=False) — Volnitsky substring search.
//     ignore_case=True → CI variant (ASCII fold, needle lowercased here at the edge).
//     Null needle → all-null result (SQL: x CONTAINS NULL = NULL).
//     Null/empty needle for starts/ends → treated as empty needle (always True).
//
// Array membership (arr=Vector, items=Python set/iterable → DRAKEN_BOOL):
//   vector_contains_any(arr, items) — True where any array row element is in items.
//   vector_contains_all(arr, items) — True where all items appear in the array row.
//     Null rows → False (no output validity). Empty items → True for non-null rows.
//
// Null TVL for string ops:
//   null haystack row → null output row; validity bitmap is a copy of haystack's.
//
// Fails loud:
//   vector_starts/ends/contains: non-Vector haystack/needle → TypeError.
//   vector_contains_any/all: accepts any object with to_pylist() (duck-typed,
//   matching the old ArrayVector contract).
//
// Replaces:
//   opteryx/compiled/vector_ops/vector_starts_ends.pyx
//   opteryx/compiled/vector_ops/vector_contains.pyx
//   opteryx/compiled/vector_ops/vector_contains_all.pyx
//   opteryx/compiled/vector_ops/vector_contains_any.pyx

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
#include "ops/string_search.h"   // includes int64_compare.h + volnitsky.h

namespace nb = nanobind;

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

// Unwrap a haystack Vector. Raises TypeError on non-Vector or wrong string type.
static const DrakenVector* unwrap_string(nb::object obj, const char* fn) {
    const DrakenVector* dv = draken_vector_unwrap(obj.ptr());
    if (!dv) throw nb::python_error();
    const bool is_str =
        dv->type == DRAKEN_VARCHAR  ||
        dv->type == DRAKEN_NVARCHAR ||
        dv->type == DRAKEN_VARBINARY ||
        dv->type == DRAKEN_DICTIONARY ||
        dv->type == DRAKEN_CONSTANT;
    if (!is_str)
        throw nb::type_error(
            (std::string(fn) + ": expected a string Vector "
             "(VARCHAR, NVARCHAR, VARBINARY, DICTIONARY, or CONSTANT)").c_str());
    return dv;
}

// Wrap a VecResult in a Python Vector. Moves res into the new owner.
static nb::object own(VecResult res) {
    PyObject* out = draken_vector_own(std::move(res));
    if (!out) throw nb::python_error();
    return nb::steal<nb::object>(out);
}

// Build an all-null DRAKEN_BOOL Vector of length n.
static nb::object all_null_bool(uint32_t n) {
    const uint32_t nbytes  = (n + 7u) >> 3;
    const uint32_t padded  = ((nbytes + 7u) & ~7u);
    const size_t   sz      = padded > 0u ? padded : 8u;

    uint8_t* bits = static_cast<uint8_t*>(draken_malloc(sz));
    if (!bits) throw std::bad_alloc();
    std::memset(bits, 0, sz);

    uint8_t* validity = static_cast<uint8_t*>(draken_malloc(sz));
    if (!validity) { draken_free(bits); throw std::bad_alloc(); }
    std::memset(validity, 0, sz);  // all bits 0 → all null

    PyObject* out = draken_vector_own_raw(bits, validity, n, DRAKEN_BOOL);
    if (!out) throw nb::python_error();
    return nb::steal<nb::object>(out);
}

// Extract needle bytes + length from a constant/scalar string Vector.
// Returns {nullptr, 0, true} when the Vector is null or empty.
struct NeedleView { const uint8_t* bytes; uint32_t len; bool is_null; };

static NeedleView extract_needle(const DrakenVector* dv) {
    if (dv->length == 0 ||
        (dv->validity && !((dv->validity[0]) & 1u)))
        return {nullptr, 0u, true};
    const DrakenStringArena* sa   = static_cast<const DrakenStringArena*>(dv->data);
    const DrakenStringSlot*  slot = &sa->slots[dv->selection[0]];
    return {str_data(slot, sa->arena), str_length(slot), false};
}

// Pre-lowercase ndl.len bytes into a heap buffer. Returns the buffer;
// caller owns it. Wraps in std::vector for RAII.
static std::vector<uint8_t> lowercase_needle(const NeedleView& ndl) {
    std::vector<uint8_t> buf(ndl.len > 0u ? ndl.len : 1u);
    for (uint32_t k = 0; k < ndl.len; ++k) {
        uint8_t b = ndl.bytes[k];
        buf[k] = (b >= 'A' && b <= 'Z') ? static_cast<uint8_t>(b | 0x20u) : b;
    }
    return buf;
}

// ---------------------------------------------------------------------------
// String search ops
// ---------------------------------------------------------------------------

static nb::object impl_starts_with(nb::object hay, nb::object ndl) {
    const DrakenVector* h = unwrap_string(hay, "vector_starts_with");
    const DrakenVector* n = unwrap_string(ndl, "vector_starts_with");
    auto nv = extract_needle(n);
    // Null/empty needle → empty-needle convention (always True for non-null rows).
    return own(draken::ops::str_starts_with(*h, nv.bytes, nv.len));
}

static nb::object impl_ci_starts_with(nb::object hay, nb::object ndl) {
    const DrakenVector* h = unwrap_string(hay, "vector_ci_starts_with");
    const DrakenVector* n = unwrap_string(ndl, "vector_ci_starts_with");
    auto nv = extract_needle(n);
    if (nv.len == 0 || nv.is_null)
        return own(draken::ops::str_starts_with_ci(*h, nullptr, 0u));
    auto lo = lowercase_needle(nv);
    return own(draken::ops::str_starts_with_ci(*h, lo.data(), nv.len));
}

static nb::object impl_ends_with(nb::object hay, nb::object ndl) {
    const DrakenVector* h = unwrap_string(hay, "vector_ends_with");
    const DrakenVector* n = unwrap_string(ndl, "vector_ends_with");
    auto nv = extract_needle(n);
    return own(draken::ops::str_ends_with(*h, nv.bytes, nv.len));
}

static nb::object impl_ci_ends_with(nb::object hay, nb::object ndl) {
    const DrakenVector* h = unwrap_string(hay, "vector_ci_ends_with");
    const DrakenVector* n = unwrap_string(ndl, "vector_ci_ends_with");
    auto nv = extract_needle(n);
    if (nv.len == 0 || nv.is_null)
        return own(draken::ops::str_ends_with_ci(*h, nullptr, 0u));
    auto lo = lowercase_needle(nv);
    return own(draken::ops::str_ends_with_ci(*h, lo.data(), nv.len));
}

static nb::object impl_contains(nb::object hay, nb::object ndl, bool ignore_case) {
    const DrakenVector* h = unwrap_string(hay, "vector_contains");
    const DrakenVector* n = unwrap_string(ndl, "vector_contains");
    auto nv = extract_needle(n);

    // SQL: x CONTAINS NULL → NULL for all rows (matches old vector_contains.pyx).
    if (nv.is_null) return all_null_bool(h->length);

    if (ignore_case) {
        auto lo = lowercase_needle(nv);
        return own(draken::ops::str_contains_ci(*h, lo.data(), nv.len));
    }
    return own(draken::ops::str_contains(*h, nv.bytes, nv.len));
}

// ---------------------------------------------------------------------------
// Array membership ops — duck-typed via to_pylist() for both old ArrayVector
// and new draken_native.Vector (preserves old vector_contains_any/all .pyx
// behaviour exactly: null rows → False, no output validity).
// ---------------------------------------------------------------------------

// Allocate a zeroed bit buffer for n rows (8-byte SIMD-padded).
static uint8_t* _alloc_bits(uint32_t n) {
    return draken::ops::cmp_alloc_bool_buf(n);
}

static nb::object impl_contains_any(nb::object arr, nb::object items) {
    // Get rows as Python list via to_pylist(). Raises AttributeError on bad input.
    nb::object rows_obj = arr.attr("to_pylist")();
    PyObject*  rows     = rows_obj.ptr();

    const Py_ssize_t nrows = PyList_Size(rows);
    if (nrows < 0) throw nb::python_error();

    const uint32_t n    = static_cast<uint32_t>(nrows);
    uint8_t*       bits = _alloc_bits(n);

    PyObject* items_raw = items.ptr();

    for (uint32_t i = 0; i < n; ++i) {
        PyObject* row = PyList_GET_ITEM(rows, static_cast<Py_ssize_t>(i));
        if (row == Py_None) continue;

        PyObject* it = PyObject_GetIter(row);
        if (!it) { draken_free(bits); throw nb::python_error(); }

        bool found = false;
        PyObject* elem;
        while ((elem = PyIter_Next(it)) != nullptr) {
            const int rc = PySequence_Contains(items_raw, elem);
            Py_DECREF(elem);
            if (rc < 0) { Py_DECREF(it); draken_free(bits); throw nb::python_error(); }
            if (rc) { found = true; break; }
        }
        Py_DECREF(it);
        if (PyErr_Occurred()) { draken_free(bits); throw nb::python_error(); }

        if (found) bits[i >> 3u] |= static_cast<uint8_t>(1u << (i & 7u));
    }

    PyObject* out = draken_vector_own_raw(bits, nullptr, n, DRAKEN_BOOL);
    if (!out) throw nb::python_error();
    return nb::steal<nb::object>(out);
}

static nb::object impl_contains_all(nb::object arr, nb::object items) {
    nb::object rows_obj = arr.attr("to_pylist")();
    PyObject*  rows     = rows_obj.ptr();

    const Py_ssize_t nrows = PyList_Size(rows);
    if (nrows < 0) throw nb::python_error();

    const uint32_t n    = static_cast<uint32_t>(nrows);
    uint8_t*       bits = _alloc_bits(n);

    // Materialise items into a list once so we can iterate per-row cheaply.
    PyObject* items_list = PySequence_List(items.ptr());
    if (!items_list) { draken_free(bits); throw nb::python_error(); }
    const Py_ssize_t nitems = PyList_GET_SIZE(items_list);

    for (uint32_t i = 0; i < n; ++i) {
        PyObject* row = PyList_GET_ITEM(rows, static_cast<Py_ssize_t>(i));
        if (row == Py_None) continue;

        bool all_found = true;
        for (Py_ssize_t j = 0; j < nitems; ++j) {
            PyObject* item = PyList_GET_ITEM(items_list, j);
            const int rc   = PySequence_Contains(row, item);
            if (rc < 0) {
                Py_DECREF(items_list);
                draken_free(bits);
                throw nb::python_error();
            }
            if (!rc) { all_found = false; break; }
        }
        if (all_found) bits[i >> 3u] |= static_cast<uint8_t>(1u << (i & 7u));
    }
    Py_DECREF(items_list);

    PyObject* out = draken_vector_own_raw(bits, nullptr, n, DRAKEN_BOOL);
    if (!out) throw nb::python_error();
    return nb::steal<nb::object>(out);
}

// ---------------------------------------------------------------------------
// NB_MODULE
// ---------------------------------------------------------------------------

NB_MODULE(vector_string_search, m) {

    m.def("vector_starts_with",
        [](nb::object h, nb::object n) -> nb::object { return impl_starts_with(h, n); },
        nb::arg("haystack"), nb::arg("needle"),
        "CS prefix match: True where haystack starts with needle. "
        "Null haystack row → null. Empty/null needle → True for all non-null rows.");

    m.def("vector_ci_starts_with",
        [](nb::object h, nb::object n) -> nb::object { return impl_ci_starts_with(h, n); },
        nb::arg("haystack"), nb::arg("needle"),
        "ASCII-CI prefix match. Needle lowercased at the edge; haystack folded inline.");

    m.def("vector_ends_with",
        [](nb::object h, nb::object n) -> nb::object { return impl_ends_with(h, n); },
        nb::arg("haystack"), nb::arg("needle"),
        "CS suffix match: True where haystack ends with needle. "
        "Null haystack row → null. Empty/null needle → True for all non-null rows.");

    m.def("vector_ci_ends_with",
        [](nb::object h, nb::object n) -> nb::object { return impl_ci_ends_with(h, n); },
        nb::arg("haystack"), nb::arg("needle"),
        "ASCII-CI suffix match. Needle lowercased at the edge; haystack folded inline.");

    m.def("vector_contains",
        [](nb::object h, nb::object n, bool ic) -> nb::object {
            return impl_contains(h, n, ic);
        },
        nb::arg("haystack"), nb::arg("needle"), nb::arg("ignore_case") = false,
        "Volnitsky substring search. Null needle → all-null result (SQL TVL). "
        "Empty needle → True for all non-null rows. "
        "ignore_case=True: ASCII fold only (A-Z), needle lowercased at the edge.");

    m.def("vector_contains_any",
        [](nb::object a, nb::object i) -> nb::object { return impl_contains_any(a, i); },
        nb::arg("arr"), nb::arg("items"),
        "Array membership: True where any element of the array row is in items. "
        "Null rows → False (no output validity). Empty items → all False.");

    m.def("vector_contains_all",
        [](nb::object a, nb::object i) -> nb::object { return impl_contains_all(a, i); },
        nb::arg("arr"), nb::arg("items"),
        "Array membership: True where all items appear in the array row. "
        "Null rows → False (no output validity). Empty items → True for all non-null rows.");
}
