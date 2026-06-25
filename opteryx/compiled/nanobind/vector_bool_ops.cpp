// opteryx/compiled/nanobind/vector_bool_ops.cpp — Milestone E.4, Phase 3, C′.
//
// C′ pattern: pure nanobind C++, zero Cython.  One NB_MODULE, four functions.
//
// This file exposes the same surface as the deleted bool_vector_ops.pyx:
//
//   bool_vector_from_int8_mask(mask, n)
//     Build a DRAKEN_BOOL Vector from a byte-per-element null mask.
//     mask[i] != 0 → bit i = 1 (IS NULL = true).
//     Accepts any object that supports the buffer protocol (e.g. memoryview,
//     bytes, bytearray). BufferError is raised on non-buffer inputs.
//
//   bool_vector_from_inverted_null_bitmap(bitmap, n)
//     Build a DRAKEN_BOOL Vector from an inverted null bitmap.
//     The input bitmap uses bit=1 for VALID, bit=0 for NULL (Arrow / Draken
//     convention).  The output Vector has bit=1 where the input bit=0 (IS NULL).
//
//   bool_vector_all_true(n)
//     Build a DRAKEN_BOOL Vector of length n with every bit = 1 (all IS NULL).
//
//   bool_vector_and_chain(masks)
//     AND a list of DRAKEN_BOOL Vectors with early-exit short-circuit.
//     Stops as soon as the running result contains no True rows (bool_any == 0).
//     Returns the first element unchanged if the list has one entry.
//     Returns None for empty list (callers must guard).
//
// Note on bool_and / bool_or / bool_not:
//   Those ops are already exposed as methods on the Python Vector object in
//   draken_native.  This file only exposes the utility construction functions
//   that bool_vector_ops.pyx previously provided via opteryx.compiled.vector_ops.
//
// Replaces: opteryx/compiled/vector_ops/bool_vector_ops.pyx (deleted in E.4).

#include <Python.h>
#include <nanobind/nanobind.h>
#include <cstring>
#include <stdexcept>

#include "core/buffers.h"
#include "core/alloc.h"
#include "core/vector_alloc.h"
#include "core/draken_bridge.h"
#include "ops/bool_logical.h"    // draken::ops::bool_and
#include "ops/bool_reductions.h" // draken::ops::bool_any

namespace nb = nanobind;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

static const DrakenVector* unwrap_bool(nb::object obj) {
    const DrakenVector* dv = draken_vector_unwrap(obj.ptr());
    if (!dv) throw nb::python_error();  // TypeError already set
    return dv;
}

// Allocate a zero-initialised, SIMD-padded uint8_t bitmap for n bits.
// Returns owned pointer (caller must draken_free on error; own_raw consumes it).
static uint8_t* alloc_bitmap(uint32_t n) {
    const uint32_t bm     = (n + 7u) >> 3;
    const uint32_t padded = ((bm + 7u) & ~7u);
    const size_t   alloc  = padded > 0u ? padded : 8u;
    uint8_t* p = static_cast<uint8_t*>(draken_malloc(alloc));
    if (!p) throw std::bad_alloc();
    std::memset(p, 0, alloc);
    return p;
}

// Mask trailing bits beyond n in the last bitmap byte to 0.
static void mask_tail(uint8_t* buf, uint32_t n) noexcept {
    const uint32_t tail = n & 7u;
    if (tail != 0u && n > 0u)
        buf[(n - 1u) >> 3] &= static_cast<uint8_t>((1u << tail) - 1u);
}

// Wrap a bool bitmap into a DRAKEN_BOOL Vector (dense, all-valid).
static nb::object wrap_bool(uint8_t* data, uint32_t n) {
    PyObject* out = draken_vector_own_raw(data, nullptr, n, DRAKEN_BOOL);
    if (!out) throw nb::python_error();
    return nb::steal<nb::object>(out);
}

// ---------------------------------------------------------------------------
// NB_MODULE — four functions, one module.
// ---------------------------------------------------------------------------

void register_vector_bool_ops(nb::module_ &m) {

    m.def("bool_vector_from_int8_mask",
        [](nb::object mask_obj, int64_t n_signed) -> nb::object {
            if (n_signed < 0)
                throw std::invalid_argument("bool_vector_from_int8_mask: n must be >= 0");
            const uint32_t n = static_cast<uint32_t>(n_signed);

            Py_buffer view;
            if (PyObject_GetBuffer(mask_obj.ptr(), &view, PyBUF_SIMPLE) < 0)
                throw nb::python_error();

            uint8_t* data = alloc_bitmap(n);
            const int8_t* mask = static_cast<const int8_t*>(view.buf);
            for (uint32_t i = 0u; i < n; ++i)
                if (mask[i])
                    data[i >> 3] |= static_cast<uint8_t>(1u << (i & 7u));

            PyBuffer_Release(&view);
            return wrap_bool(data, n);
        },
        nb::arg("mask"), nb::arg("n"),
        "Build a DRAKEN_BOOL IS-NULL Vector from a byte-per-element mask. "
        "mask[i] != 0 → bit i = 1 (IS NULL). "
        "Accepts any buffer-protocol object (memoryview, bytes, bytearray).");

    m.def("bool_vector_from_inverted_null_bitmap",
        [](nb::object bitmap_obj, int64_t n_signed) -> nb::object {
            if (n_signed < 0)
                throw std::invalid_argument(
                    "bool_vector_from_inverted_null_bitmap: n must be >= 0");
            const uint32_t n = static_cast<uint32_t>(n_signed);

            Py_buffer view;
            if (PyObject_GetBuffer(bitmap_obj.ptr(), &view, PyBUF_SIMPLE) < 0)
                throw nb::python_error();

            const uint32_t bm = (n + 7u) >> 3;
            uint8_t* data = alloc_bitmap(n);
            const uint8_t* bitmap = static_cast<const uint8_t*>(view.buf);

            // Invert: bit=1 (valid) → bit=0 (IS NULL = false);
            //         bit=0 (null)  → bit=1 (IS NULL = true).
            for (uint32_t k = 0u; k < bm; ++k)
                data[k] = ~bitmap[k];
            mask_tail(data, n);

            PyBuffer_Release(&view);
            return wrap_bool(data, n);
        },
        nb::arg("bitmap"), nb::arg("n"),
        "Build a DRAKEN_BOOL IS-NULL Vector by inverting a null-bitmap. "
        "Input convention: bit=1 = valid, bit=0 = null (Arrow/Draken). "
        "Output: bit=1 = IS NULL = true.");

    m.def("bool_vector_all_true",
        [](int64_t n_signed) -> nb::object {
            if (n_signed < 0)
                throw std::invalid_argument("bool_vector_all_true: n must be >= 0");
            const uint32_t n  = static_cast<uint32_t>(n_signed);
            const uint32_t bm = (n + 7u) >> 3;
            uint8_t* data = alloc_bitmap(n);
            if (bm > 0u) std::memset(data, 0xFF, bm);
            mask_tail(data, n);
            return wrap_bool(data, n);
        },
        nb::arg("n"),
        "Build a DRAKEN_BOOL Vector of length n with every bit = 1 (all IS NULL = true). "
        "Used when a column is entirely SQL NULL (constant-null encoding).");

    m.def("vector_uint64_eq_scalar",
        [](nb::object buffer_obj, int64_t length_signed, uint64_t target) -> nb::object {
            // Element-wise scalar equality on a contiguous uint64 buffer.
            // For each row i in [0, length): out[i] = (buffer[i] == target).
            // Returns a DRAKEN_BOOL Vector (dense, all-valid).
            //
            // Use case: morsel.hash(col_names) returns a uint64 buffer (one hash per
            // row); the join/group-by fast path needs a BoolVector of "rows whose hash
            // matches target_hash."  Replaces old-draken `_bool_vector_from_uint64_eq`.
            //
            // No Python loops; buffer accessed zero-copy via the buffer protocol.
            if (length_signed < 0)
                throw std::invalid_argument("vector_uint64_eq_scalar: length must be >= 0");
            const uint32_t n = static_cast<uint32_t>(length_signed);

            Py_buffer view;
            if (PyObject_GetBuffer(buffer_obj.ptr(), &view, PyBUF_SIMPLE) < 0)
                throw nb::python_error();
            // Caller is responsible for ensuring buffer holds >= length uint64s.
            // PyBUF_SIMPLE doesn't validate element type; size check is a sanity guard.
            if (static_cast<uint32_t>(view.len) < n * sizeof(uint64_t)) {
                PyBuffer_Release(&view);
                throw std::invalid_argument(
                    "vector_uint64_eq_scalar: buffer smaller than length * 8 bytes");
            }

            uint8_t* data = alloc_bitmap(n);
            const uint64_t* buf = static_cast<const uint64_t*>(view.buf);
            for (uint32_t i = 0u; i < n; ++i)
                if (buf[i] == target)
                    data[i >> 3] |= static_cast<uint8_t>(1u << (i & 7u));

            PyBuffer_Release(&view);
            return wrap_bool(data, n);
        },
        nb::arg("buffer"), nb::arg("length"), nb::arg("target"),
        "Element-wise equality of a uint64 buffer against a scalar target. "
        "Returns a DRAKEN_BOOL Vector of length `length`, bit i = (buffer[i] == target). "
        "Accepts any buffer-protocol object (array.array('Q'), bytes, memoryview).");

    m.def("bool_vector_and_chain",
        [](nb::object masks_obj) -> nb::object {
            if (!PyList_Check(masks_obj.ptr()))
                throw std::invalid_argument("bool_vector_and_chain: argument must be a list");
            const Py_ssize_t size = PyList_GET_SIZE(masks_obj.ptr());
            if (size == 0)
                return nb::none();

            // Start with the first element (borrowed reference).
            nb::object result = nb::borrow<nb::object>(
                PyList_GET_ITEM(masks_obj.ptr(), 0));

            for (Py_ssize_t i = 1; i < size; ++i) {
                const DrakenVector* rv = unwrap_bool(result);
                // Early exit: if all valid rows are False, AND with anything
                // still gives all-False (Kleene: bool_any == 0 means no True).
                if (draken::ops::bool_any(*rv) == 0)
                    break;

                nb::object other = nb::borrow<nb::object>(
                    PyList_GET_ITEM(masks_obj.ptr(), i));
                const DrakenVector* mv = unwrap_bool(other);

                VecResult res = draken::ops::bool_and(*rv, *mv);
                PyObject* out = draken_vector_own_raw(
                    res.data, res.validity, res.length, res.type);
                if (!out) throw nb::python_error();
                result = nb::steal<nb::object>(out);
            }

            return result;
        },
        nb::arg("masks"),
        "AND a list of DRAKEN_BOOL Vectors with early-exit short-circuit. "
        "Stops when running result has no True rows (bool_any == 0). "
        "Returns first element for single-item list; None for empty list.");
}
