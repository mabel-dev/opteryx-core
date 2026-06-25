// opteryx/compiled/nanobind/vector_bitwise.cpp — Milestone E.2, Part B.
//
// C′ pattern: pure nanobind C++, zero Cython.  One NB_MODULE, six functions.
//
// Each function:
//   1. Receives nb::object operand(s).
//   2. Calls draken_vector_unwrap (declared in draken_bridge.h,
//      implemented in draken_native.so — resolved at import time via
//      RTLD_GLOBAL set in draken/__init__.py).
//   3. Calls draken::ops::bitwise_* from draken/ops/int_bitwise.h.
//   4. Wraps the VecResult via draken_vector_own_raw → returns new Vector.
//
// Type-check contract: draken_vector_unwrap raises TypeError and returns
// nullptr on non-Vector input — never segfaults.
//
// Replaces: opteryx/compiled/vector_ops/vector_bitwise_{and,or,xor,not,
//           shift_left,shift_right}.pyx (deleted as part of E.2).

#include <Python.h>
#include <nanobind/nanobind.h>

#include "core/buffers.h"
#include "core/draken_bridge.h"  // draken_vector_unwrap, draken_vector_own_raw
#include "ops/int_bitwise.h"     // draken::ops::bitwise_*

namespace nb = nanobind;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// Unwrap one operand; raises TypeError on non-Vector (via draken_vector_unwrap).
static const DrakenVector* unwrap(nb::object obj) {
    const DrakenVector* dv = draken_vector_unwrap(obj.ptr());
    if (!dv) throw nb::python_error();  // TypeError already set by unwrap
    return dv;
}

// Wrap a VecResult into a new Python Vector.
// Transfers ownership of res.data and res.validity to the new Vector.
// res.selection is NOT freed here (always a shared identity pointer for dense
// results from int_bitwise.h; owns_selection == false).
static nb::object wrap(VecResult res) {
    PyObject* out = draken_vector_own_raw(res.data, res.validity, res.length, res.type);
    if (!out) throw nb::python_error();
    return nb::steal<nb::object>(out);
}

// ---------------------------------------------------------------------------
// NB_MODULE — six functions, one module.
// ---------------------------------------------------------------------------

void register_vector_bitwise(nb::module_ &m) {

    m.def("vector_bitwise_and",
        [](nb::object left, nb::object right) -> nb::object {
            const DrakenVector* lv = unwrap(left);
            const DrakenVector* rv = unwrap(right);
            return wrap(draken::ops::bitwise_and(*lv, *rv));
        },
        nb::arg("left"), nb::arg("right"),
        "Bitwise AND two integer Vectors element-wise. NULL propagates from either operand.");

    m.def("vector_bitwise_or",
        [](nb::object left, nb::object right) -> nb::object {
            const DrakenVector* lv = unwrap(left);
            const DrakenVector* rv = unwrap(right);
            return wrap(draken::ops::bitwise_or(*lv, *rv));
        },
        nb::arg("left"), nb::arg("right"),
        "Bitwise OR two integer Vectors element-wise. NULL propagates from either operand.");

    m.def("vector_bitwise_xor",
        [](nb::object left, nb::object right) -> nb::object {
            const DrakenVector* lv = unwrap(left);
            const DrakenVector* rv = unwrap(right);
            return wrap(draken::ops::bitwise_xor(*lv, *rv));
        },
        nb::arg("left"), nb::arg("right"),
        "Bitwise XOR two integer Vectors element-wise. NULL propagates from either operand.");

    m.def("vector_bitwise_not",
        [](nb::object operand) -> nb::object {
            const DrakenVector* dv = unwrap(operand);
            return wrap(draken::ops::bitwise_not(*dv));
        },
        nb::arg("operand"),
        "Bitwise NOT (one's complement) of an integer Vector. NULL propagates.");

    m.def("vector_bitwise_shift_left",
        [](nb::object left, nb::object right) -> nb::object {
            const DrakenVector* lv = unwrap(left);
            const DrakenVector* rv = unwrap(right);
            return wrap(draken::ops::bitwise_shl(*lv, *rv));
        },
        nb::arg("left"), nb::arg("right"),
        "Shift left[i] left by right[i] bits. Raises on out-of-range shift count. "
        "NULL propagates from either operand.");

    m.def("vector_bitwise_shift_right",
        [](nb::object left, nb::object right) -> nb::object {
            const DrakenVector* lv = unwrap(left);
            const DrakenVector* rv = unwrap(right);
            return wrap(draken::ops::bitwise_shr(*lv, *rv));
        },
        nb::arg("left"), nb::arg("right"),
        "Arithmetic right-shift left[i] by right[i] bits. Raises on out-of-range "
        "shift count. NULL propagates from either operand.");
}
