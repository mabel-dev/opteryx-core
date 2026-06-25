// opteryx/compiled/nanobind/vector_array_reduce.cpp — Milestone E.5, Part B.
//
// C′ pattern: pure nanobind C++, zero Cython.  One NB_MODULE, 8 functions.
//
// Replaces: opteryx/compiled/vector_ops/vector_anyop_{eq,neq,gt,gte,lt,lte}.pyx
//           opteryx/compiled/vector_ops/vector_allop_{eq,neq}.pyx
//           (9 files deleted — anyop_like excluded per ticket, separate ticket)
//
// Each function:
//   1. Receives (nb::object arr_vec, nb::object literal).
//   2. Calls draken_vector_unwrap → fails loud (TypeError) on non-Vector arr_vec.
//   3. Calls draken_array_child_unwrap → fails loud (TypeError/RuntimeError) if
//      arr_vec is not DRAKEN_ARRAY or has no child.
//   4. Builds ArrScalar from literal (int64 or str→bytes; None→DRAKEN_NULL).
//      Raises TypeError on unsupported literal type.
//   5. Calls draken::ops::arr_* → may raise std::invalid_argument on unsupported
//      child type.
//   6. Wraps VecResult via draken_vector_own → returns new DRAKEN_BOOL Vector.
//
// Semantic note: empty-row and null-row semantics match correct SQL (ticket spec),
// which differs from the old .pyx for ALL ops (old: null/empty row → False; new:
// null row → NULL/TVL, empty row → True for ALL, False for ANY).

#define XXH_INLINE_ALL
#include "xxhash.h"

#include <Python.h>
#include <nanobind/nanobind.h>
#include <stdexcept>
#include <cstring>

#include "core/buffers.h"
#include "core/string_slot.h"
#include "core/draken_bridge.h"
#include "ops/array_reductions.h"

namespace nb = nanobind;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

static const DrakenVector* unwrap_arr(nb::object obj) {
    const DrakenVector* dv = draken_vector_unwrap(obj.ptr());
    if (!dv) throw nb::python_error();
    return dv;
}

static const DrakenVector* unwrap_child(nb::object obj) {
    const DrakenVector* dv = draken_array_child_unwrap(obj.ptr());
    if (!dv) throw nb::python_error();
    return dv;
}

// Build ArrScalar from a Python literal object.
// Returns a struct owning the slot + keeping bytes pointer valid via slot_storage.
// IMPORTANT: slot_storage and bytes_storage must remain alive for the duration of
// the call to the reduction op (they back the slot's inline or arena bytes).
static draken::ops::ArrScalar build_scalar(nb::object lit,
                                           DrakenStringSlot& slot_storage,
                                           nb::bytes& bytes_storage)
{
    draken::ops::ArrScalar s;

    if (lit.is_none()) {
        s.type = DRAKEN_NULL;
        s.i64  = 0;
        s.str.slot  = nullptr;
        s.str.bytes = nullptr;
        return s;
    }

    if (PyLong_Check(lit.ptr())) {
        s.type = DRAKEN_INT64;
        s.i64  = nb::cast<int64_t>(lit);
        s.str.slot  = nullptr;
        s.str.bytes = nullptr;
        return s;
    }

    if (PyUnicode_Check(lit.ptr()) || PyBytes_Check(lit.ptr())) {
        // Normalise to bytes (UTF-8).
        if (PyUnicode_Check(lit.ptr()))
            bytes_storage = nb::cast<nb::bytes>(lit.attr("encode")("utf-8"));
        else
            bytes_storage = nb::cast<nb::bytes>(lit);

        const uint8_t* data = reinterpret_cast<const uint8_t*>(
            PyBytes_AS_STRING(bytes_storage.ptr()));
        const uint32_t len = static_cast<uint32_t>(PyBytes_GET_SIZE(bytes_storage.ptr()));

        if (len <= STR_INLINE_MAX) {
            str_init_inline(&slot_storage, data, len);
        } else {
            const uint32_t hash32 = static_cast<uint32_t>(XXH3_64bits(data, len));
            // arena_offset == 0: str_data(&slot, data) returns data directly.
            str_init_extern(&slot_storage, data, len, hash32, 0u);
        }

        s.type      = DRAKEN_VARCHAR;
        s.i64       = 0;
        s.str.slot  = &slot_storage;
        s.str.bytes = data;
        return s;
    }

    throw nb::type_error(
        "array_reduce: literal must be int, str, bytes, or None");
}

// Wrap a VecResult into a Python Vector (transfers ownership).
static nb::object wrap(VecResult res) {
    PyObject* out = draken_vector_own(res);
    if (!out) throw nb::python_error();
    return nb::steal<nb::object>(out);
}

// ---------------------------------------------------------------------------
// Per-op dispatch helper macro (avoids repetitive try/catch boilerplate)
// ---------------------------------------------------------------------------

#define ARR_REDUCE_FN(fn_name, ops_call)                                    \
    [](nb::object arr_obj, nb::object lit) -> nb::object {                  \
        const DrakenVector* arr   = unwrap_arr(arr_obj);                    \
        const DrakenVector* child = unwrap_child(arr_obj);                  \
        DrakenStringSlot slot_storage;                                      \
        nb::bytes bytes_storage;                                             \
        draken::ops::ArrScalar s = build_scalar(lit, slot_storage, bytes_storage); \
        return wrap(ops_call(*arr, *child, s));                             \
    }

// ---------------------------------------------------------------------------
// NB_MODULE — 8 functions, one module.
// ---------------------------------------------------------------------------

void register_vector_array_reduce(nb::module_ &m) {

    m.def("vector_anyop_eq",  ARR_REDUCE_FN(vector_anyop_eq,  draken::ops::arr_any_eq),
        nb::arg("column"), nb::arg("literal").none(),
        "literal = ANY(row): True iff any non-null element equals literal.");

    m.def("vector_allop_eq",  ARR_REDUCE_FN(vector_allop_eq,  draken::ops::arr_all_eq),
        nb::arg("column"), nb::arg("literal").none(),
        "literal = ALL(row): True iff all non-null elements equal literal. "
        "Empty row → True (vacuous). Null row → NULL.");

    m.def("vector_anyop_neq", ARR_REDUCE_FN(vector_anyop_neq, draken::ops::arr_any_ne),
        nb::arg("column"), nb::arg("literal").none(),
        "literal != ANY(row): True iff any non-null element differs from literal.");

    m.def("vector_allop_neq", ARR_REDUCE_FN(vector_allop_neq, draken::ops::arr_all_ne),
        nb::arg("column"), nb::arg("literal").none(),
        "literal != ALL(row): True iff all non-null elements differ from literal. "
        "Empty row → True (vacuous). Null row → NULL.");

    m.def("vector_anyop_gt",  ARR_REDUCE_FN(vector_anyop_gt,  draken::ops::arr_any_gt),
        nb::arg("column"), nb::arg("literal").none(),
        "literal > ANY(row): True iff any non-null element is less than literal.");

    m.def("vector_anyop_gte", ARR_REDUCE_FN(vector_anyop_gte, draken::ops::arr_any_ge),
        nb::arg("column"), nb::arg("literal").none(),
        "literal >= ANY(row): True iff any non-null element is <= literal.");

    m.def("vector_anyop_lt",  ARR_REDUCE_FN(vector_anyop_lt,  draken::ops::arr_any_lt),
        nb::arg("column"), nb::arg("literal").none(),
        "literal < ANY(row): True iff any non-null element is greater than literal.");

    m.def("vector_anyop_lte", ARR_REDUCE_FN(vector_anyop_lte, draken::ops::arr_any_le),
        nb::arg("column"), nb::arg("literal").none(),
        "literal <= ANY(row): True iff any non-null element is >= literal.");
}
