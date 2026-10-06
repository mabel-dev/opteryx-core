// opteryx/compiled/nanobind/vector_array_membership.cpp — Milestone E.10, C′.
//
// C′ pattern: pure nanobind C++, zero Cython. Two functions:
//
// (Was vector_string_search.cpp. Its string-search functions — vector_starts_with,
// vector_ci_starts_with, vector_ends_with, vector_ci_ends_with, vector_contains —
// and their header draken/ops/string_search.h were deleted 2026-10-06: never
// dispatched; LIKE / STARTS_WITH / ENDS_WITH / InStr run on the c-native
// draken_like / draken_starts_with / draken_ends_with / draken_contains kernels.)
//
// Array membership (arr=DRAKEN_ARRAY Vector, items=Python set/iterable → DRAKEN_BOOL):
//   vector_contains_any(arr, items) — True where any array row element is in items.
//   vector_contains_all(arr, items) — True where all items appear in the array row.
//     Null rows → False (no output validity). Empty items → True for non-null rows.
//     Native: the array column and its child are iterated as DrakenVectors via the
//     uniform data[selection[i]] path; the small Python item set is converted once
//     here (under the GIL) into a typed lookup, then the row scan runs nogil. No
//     per-element Python objects are created. See ops/array_membership.h.
//     Child element types: INT64, FLOAT64, and the string family (VARCHAR/NVARCHAR/
//     VARBINARY). Any other child type fails loud. A NULL item never matches
//     (SQL TVL): skipped for _any, makes _all all-False.
//
// Fails loud:
//   vector_contains_any/all: non-Vector or non-ARRAY arr → TypeError;
//   unsupported child element type → ValueError (std::invalid_argument).
//
// Replaces:
//   opteryx/compiled/vector_ops/vector_contains_all.pyx
//   opteryx/compiled/vector_ops/vector_contains_any.pyx

#include <Python.h>
#include <nanobind/nanobind.h>
#include <cstdint>
#include <cmath>
#include <cstring>
#include <stdexcept>
#include <vector>

#include "core/buffers.h"
#include "core/alloc.h"
#include "core/string_slot.h"
#include "core/vector_alloc.h"
#include "vectors/_vector_bridge.h"
#include "ops/array_membership.h"   // native arr_contains_any / arr_contains_all

namespace nb = nanobind;

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

// Wrap a VecResult in a Python Vector. Moves res into the new owner.
static nb::object own(VecResult res) {
    PyObject* out = draken_vector_own(std::move(res));
    if (!out) throw nb::python_error();
    return nb::steal<nb::object>(out);
}

// ---------------------------------------------------------------------------
// Array membership ops — native over DrakenVectors (ops/array_membership.h).
//
// The array column and its child are iterated via the uniform data[selection[i]]
// path; the small Python item set is converted once here (under the GIL) into a
// typed lookup matching the child's element type, then the row scan runs nogil.
// Behaviour matches the old vector_contains_any/all .pyx: null rows → False, no
// output validity; _any empty items → all False; _all empty items → True for
// non-null rows. A NULL item never matches (SQL TVL).
// ---------------------------------------------------------------------------

// Unwrap a DRAKEN_ARRAY parent Vector. Raises TypeError on non-Vector / non-array.
static const DrakenVector* unwrap_array(nb::object obj, const char* fn) {
    const DrakenVector* dv = draken_vector_unwrap(obj.ptr());
    if (!dv) throw nb::python_error();
    if (dv->type != DRAKEN_ARRAY)
        throw nb::type_error((std::string(fn) + ": expected an ARRAY Vector").c_str());
    return dv;
}

// Convert a Python set/iterable of items into a typed native lookup for child_type.
// int/float cross-coercion mirrors Python's 5 == 5.0; anything not representable in
// the child's element family (including None) sets has_unrepresentable.
static draken::ops::MembershipItems
build_items(nb::object items, DrakenType child_type) {
    const bool is_int = child_type == DRAKEN_INT64;
    const bool is_flt = child_type == DRAKEN_FLOAT64;
    const bool is_str = child_type == DRAKEN_VARCHAR  ||
                        child_type == DRAKEN_NVARCHAR ||
                        child_type == DRAKEN_VARBINARY;
    if (!is_int && !is_flt && !is_str)
        throw std::invalid_argument(
            "vector_contains_*: unsupported array child element type "
            "(only INT64, FLOAT64, and the string family are supported)");

    draken::ops::MembershipItems out;

    PyObject* it = PyObject_GetIter(items.ptr());
    if (!it) throw nb::python_error();

    PyObject* elem;
    while ((elem = PyIter_Next(it)) != nullptr) {
        out.requested_count++;

        if (elem == Py_None) {
            out.has_unrepresentable = true;          // NULL never equals anything
        } else if (is_int) {
            if (PyLong_Check(elem)) {
                int overflow = 0;
                const long long v = PyLong_AsLongLongAndOverflow(elem, &overflow);
                if (overflow != 0) {
                    out.has_unrepresentable = true;  // outside int64 → cannot appear
                } else if (v == -1 && PyErr_Occurred()) {
                    Py_DECREF(elem); Py_DECREF(it); throw nb::python_error();
                } else {
                    out.i64.push_back(static_cast<int64_t>(v));
                }
            } else if (PyFloat_Check(elem)) {
                const double d = PyFloat_AS_DOUBLE(elem);
                if (d == std::floor(d) &&
                    d >= -9223372036854775808.0 && d < 9223372036854775808.0)
                    out.i64.push_back(static_cast<int64_t>(d));   // 5.0 matches int 5
                else
                    out.has_unrepresentable = true;
            } else {
                out.has_unrepresentable = true;
            }
        } else if (is_flt) {
            if (PyFloat_Check(elem)) {
                out.f64.push_back(PyFloat_AS_DOUBLE(elem));
            } else if (PyLong_Check(elem)) {
                const double d = PyLong_AsDouble(elem);
                if (d == -1.0 && PyErr_Occurred()) {
                    Py_DECREF(elem); Py_DECREF(it); throw nb::python_error();
                }
                out.f64.push_back(d);
            } else {
                out.has_unrepresentable = true;
            }
        } else {  // string family
            const uint8_t* data = nullptr;
            uint32_t       len  = 0;
            if (PyBytes_Check(elem)) {
                data = reinterpret_cast<const uint8_t*>(PyBytes_AS_STRING(elem));
                len  = static_cast<uint32_t>(PyBytes_GET_SIZE(elem));
            } else if (PyUnicode_Check(elem)) {
                Py_ssize_t  n8 = 0;
                const char* u8 = PyUnicode_AsUTF8AndSize(elem, &n8);
                if (!u8) { Py_DECREF(elem); Py_DECREF(it); throw nb::python_error(); }
                data = reinterpret_cast<const uint8_t*>(u8);
                len  = static_cast<uint32_t>(n8);
            }
            if (data == nullptr) {
                out.has_unrepresentable = true;
            } else {
                draken::ops::MembershipStrItem si;
                si.bytes.assign(data, data + len);
                draken_build_string_slot(&si.slot, si.bytes.data(), len, 0u);
                out.str.push_back(std::move(si));
            }
        }

        Py_DECREF(elem);
    }
    Py_DECREF(it);
    if (PyErr_Occurred()) throw nb::python_error();
    return out;
}

static nb::object impl_contains_any(nb::object arr, nb::object items) {
    const DrakenVector* a     = unwrap_array(arr, "vector_contains_any");
    const DrakenVector* child = draken_array_child_unwrap(arr.ptr());
    if (!child) throw nb::python_error();

    draken::ops::MembershipItems mi = build_items(items, child->type);

    // Item conversion is done; the row scan touches no Python and mi owns its
    // bytes — release the GIL so concurrent morsels scan in parallel (§2).
    VecResult res;
    {
        nb::gil_scoped_release rel;
        res = draken::ops::arr_contains_any(*a, *child, mi);
    }
    return own(std::move(res));
}

static nb::object impl_contains_all(nb::object arr, nb::object items) {
    const DrakenVector* a     = unwrap_array(arr, "vector_contains_all");
    const DrakenVector* child = draken_array_child_unwrap(arr.ptr());
    if (!child) throw nb::python_error();

    draken::ops::MembershipItems mi = build_items(items, child->type);

    VecResult res;
    {
        nb::gil_scoped_release rel;
        res = draken::ops::arr_contains_all(*a, *child, mi);
    }
    return own(std::move(res));
}

// ---------------------------------------------------------------------------
// NB_MODULE
// ---------------------------------------------------------------------------

void register_vector_array_membership(nb::module_ &m) {

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
