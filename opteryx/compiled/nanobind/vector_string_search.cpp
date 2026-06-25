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
// Null TVL for string ops:
//   null haystack row → null output row; validity bitmap is a copy of haystack's.
//
// Fails loud:
//   vector_starts/ends/contains: non-Vector haystack/needle → TypeError.
//   vector_contains_any/all: non-Vector or non-ARRAY arr → TypeError;
//   unsupported child element type → ValueError (std::invalid_argument).
//
// Replaces:
//   opteryx/compiled/vector_ops/vector_starts_ends.pyx
//   opteryx/compiled/vector_ops/vector_contains.pyx
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
#include "core/draken_bridge.h"
#include "ops/string_search.h"      // includes int64_compare.h + volnitsky.h
#include "ops/array_membership.h"   // native arr_contains_any / arr_contains_all

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
        dv->type == DRAKEN_VARBINARY;
    if (!is_str)
        throw nb::type_error(
            (std::string(fn) + ": expected a string Vector "
             "(VARCHAR, NVARCHAR, or VARBINARY)").c_str());
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

// Prefix/suffix scans are pure C++ over the unwrapped DrakenVectors and their
// arenas, exactly like impl_contains: hay/ndl stay live for the whole call so the
// buffers cannot be freed, and the CI needle is lowered (allocated) under the GIL
// before the scan. Drop the GIL around the scan so concurrent morsels run in
// parallel (§2). gil_scoped_release re-acquires in its destructor, including
// during exception unwind.
static nb::object impl_starts_with(nb::object hay, nb::object ndl) {
    const DrakenVector* h = unwrap_string(hay, "vector_starts_with");
    const DrakenVector* n = unwrap_string(ndl, "vector_starts_with");
    auto nv = extract_needle(n);
    // Null/empty needle → empty-needle convention (always True for non-null rows).
    VecResult res;
    {
        nb::gil_scoped_release rel;
        res = draken::ops::str_starts_with(*h, nv.bytes, nv.len);
    }
    return own(std::move(res));
}

static nb::object impl_ci_starts_with(nb::object hay, nb::object ndl) {
    const DrakenVector* h = unwrap_string(hay, "vector_ci_starts_with");
    const DrakenVector* n = unwrap_string(ndl, "vector_ci_starts_with");
    auto nv = extract_needle(n);
    VecResult res;
    if (nv.len == 0 || nv.is_null) {
        nb::gil_scoped_release rel;
        res = draken::ops::str_starts_with_ci(*h, nullptr, 0u);
    } else {
        auto lo = lowercase_needle(nv);          // alloc under GIL; outlives the scan
        nb::gil_scoped_release rel;
        res = draken::ops::str_starts_with_ci(*h, lo.data(), nv.len);
    }
    return own(std::move(res));
}

static nb::object impl_ends_with(nb::object hay, nb::object ndl) {
    const DrakenVector* h = unwrap_string(hay, "vector_ends_with");
    const DrakenVector* n = unwrap_string(ndl, "vector_ends_with");
    auto nv = extract_needle(n);
    VecResult res;
    {
        nb::gil_scoped_release rel;
        res = draken::ops::str_ends_with(*h, nv.bytes, nv.len);
    }
    return own(std::move(res));
}

static nb::object impl_ci_ends_with(nb::object hay, nb::object ndl) {
    const DrakenVector* h = unwrap_string(hay, "vector_ci_ends_with");
    const DrakenVector* n = unwrap_string(ndl, "vector_ci_ends_with");
    auto nv = extract_needle(n);
    VecResult res;
    if (nv.len == 0 || nv.is_null) {
        nb::gil_scoped_release rel;
        res = draken::ops::str_ends_with_ci(*h, nullptr, 0u);
    } else {
        auto lo = lowercase_needle(nv);          // alloc under GIL; outlives the scan
        nb::gil_scoped_release rel;
        res = draken::ops::str_ends_with_ci(*h, lo.data(), nv.len);
    }
    return own(std::move(res));
}

static nb::object impl_contains(nb::object hay, nb::object ndl, bool ignore_case) {
    const DrakenVector* h = unwrap_string(hay, "vector_contains");
    const DrakenVector* n = unwrap_string(ndl, "vector_contains");
    auto nv = extract_needle(n);

    // SQL: x CONTAINS NULL → NULL for all rows (matches old vector_contains.pyx).
    if (nv.is_null) return all_null_bool(h->length);

    // The Volnitsky scan is pure C++ over the unwrapped DrakenVectors and their
    // arenas — no Python object is touched between release and re-acquire. Drop
    // the GIL so concurrent morsels run the scan in parallel rather than
    // serialising on it (§2: release the GIL as early as possible). Safety:
    //   - hay/ndl are live nb::object references for the whole call, so the
    //     underlying buffers cannot be freed while the GIL is released;
    //   - str_contains builds its own per-call Volnitsky table — no shared
    //     mutable state between threads;
    //   - gil_scoped_release re-acquires the GIL in its destructor, including
    //     during exception unwinding (str_contains may throw bad_alloc).
    // unwrap/extract/lowercase/own all stay under the GIL.
    VecResult res;
    if (ignore_case) {
        auto lo = lowercase_needle(nv);          // alloc under GIL; outlives the scan scope
        nb::gil_scoped_release rel;
        res = draken::ops::str_contains_ci(*h, lo.data(), nv.len);
    } else {
        nb::gil_scoped_release rel;
        res = draken::ops::str_contains(*h, nv.bytes, nv.len);
    }
    return own(std::move(res));
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

void register_vector_string_search(nb::module_ &m) {

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
