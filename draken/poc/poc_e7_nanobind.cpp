// draken/poc/poc_e7_nanobind.cpp — Milestone E.7 POC
//
// Proves draken_vector_own_string: a C++ consumer manually populates DrakenStringSlot[]
// + arena using draken_malloc + draken_build_string_slot, hands all three buffers to
// draken_vector_own_string, and gets back a Python Vector whose to_pylist() and
// _slot_fields() agree with vector_from_string_sequence on the same values.
//
// draken_vector_own_string / draken_vector_own_raw / draken_vector_unwrap are compiled
// into draken_native.so and resolved at runtime via RTLD_GLOBAL. run_poc_e7.py loads
// draken_native first.

#include <Python.h>
#include <nanobind/nanobind.h>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <stdexcept>
#include <vector>

#include "core/buffers.h"
#include "core/alloc.h"
#include "core/string_slot.h"
#include "core/draken_bridge.h"

namespace nb = nanobind;

// ---------------------------------------------------------------------------
// Helper: build a DRAKEN_STRING Vector via draken_vector_own_string from a
// C++ std::vector of (bytes, len) pairs. Null rows are indicated by len == UINT32_MAX.
// ---------------------------------------------------------------------------

static nb::object build_via_own(
    const std::vector<const char*>& strs,
    const std::vector<uint32_t>& lens,
    const std::vector<bool>& is_null)
{
    const uint32_t n = static_cast<uint32_t>(strs.size());

    // Pass 1: count arena bytes, detect nulls.
    size_t total_extern = 0u;
    bool   has_nulls    = false;
    for (uint32_t i = 0u; i < n; ++i) {
        if (is_null[i]) { has_nulls = true; continue; }
        if (lens[i] > STR_INLINE_MAX) total_extern += lens[i];
    }
    if (total_extern > static_cast<size_t>(UINT32_MAX))
        throw std::overflow_error("poc_e7: arena exceeds 4 GB");

    // Allocate slots.
    DrakenStringSlot* slots = static_cast<DrakenStringSlot*>(
        draken_malloc((n > 0u ? n : 1u) * sizeof(DrakenStringSlot)));
    if (!slots) throw std::bad_alloc();
    std::memset(slots, 0, (n > 0u ? n : 1u) * sizeof(DrakenStringSlot));

    // Allocate arena (may be nullptr when all strings are inline).
    uint8_t* arena = nullptr;
    if (total_extern > 0u) {
        arena = static_cast<uint8_t*>(draken_malloc(total_extern));
        if (!arena) { draken_free(slots); throw std::bad_alloc(); }
    }

    // Allocate validity bitmap (nullptr when all valid).
    uint8_t* validity = nullptr;
    if (has_nulls) {
        const uint32_t vbytes = (n + 7u) / 8u;
        const uint32_t padded = ((vbytes + 7u) & ~7u);
        const size_t   vsize  = padded > 0u ? padded : 8u;
        validity = static_cast<uint8_t*>(draken_malloc(vsize));
        if (!validity) { draken_free(slots); draken_free(arena); throw std::bad_alloc(); }
        std::memset(validity, 0xFF, vsize);  // all valid; nulls cleared below
    }

    // Pass 2: fill slots, arena, validity.
    size_t arena_offset = 0u;
    for (uint32_t i = 0u; i < n; ++i) {
        if (is_null[i]) {
            str_init_null(&slots[i]);
            if (validity) validity[i / 8u] &= static_cast<uint8_t>(~(1u << (i % 8u)));
        } else {
            const uint8_t* ubytes = reinterpret_cast<const uint8_t*>(strs[i]);
            const uint32_t ulen   = lens[i];
            if (ulen > STR_INLINE_MAX) {
                // Write bytes to arena first, then build slot.
                std::memcpy(arena + arena_offset, ubytes, ulen);
                draken_build_string_slot(&slots[i], ubytes, ulen,
                                         static_cast<uint32_t>(arena_offset));
                arena_offset += ulen;
            } else {
                draken_build_string_slot(&slots[i], ubytes, ulen, 0u);
            }
        }
    }

    // Hand off all three buffers to draken_vector_own_string (ownership transfers).
    PyObject* out = draken_vector_own_string(slots, arena, total_extern, validity, n);
    if (!out) throw nb::python_error();
    return nb::steal<nb::object>(out);
}

// ---------------------------------------------------------------------------
// NB_MODULE
// ---------------------------------------------------------------------------

NB_MODULE(poc_e7, m) {
    m.attr("__doc__") = (
        "Milestone E.7 POC — draken_vector_own_string bridge.\n"
        "\n"
        "Proves: C++ consumer allocates DrakenStringSlot[] + arena via draken_malloc,\n"
        "populates slots with draken_build_string_slot, transfers all three buffers to\n"
        "draken_vector_own_string, and gets back a Vector whose to_pylist() and\n"
        "_slot_fields() match vector_from_string_sequence on the same input.\n"
    );

    // make_string_vec: build a string Vector via draken_vector_own_string.
    // values: list[str | None]
    m.def("make_string_vec",
        [](nb::list seq) -> nb::object {
            const uint32_t n = static_cast<uint32_t>(seq.size());

            std::vector<const char*> strs(n, nullptr);
            std::vector<uint32_t>    lens(n, 0u);
            std::vector<bool>        is_null(n, false);

            for (uint32_t i = 0u; i < n; ++i) {
                nb::object obj = seq[static_cast<Py_ssize_t>(i)];
                if (obj.is_none()) {
                    is_null[i] = true;
                } else {
                    PyObject* s = obj.ptr();
                    if (!PyUnicode_Check(s))
                        throw std::invalid_argument(
                            "make_string_vec: element is not str or None");
                    Py_ssize_t slen = 0;
                    const char* utf8 = PyUnicode_AsUTF8AndSize(s, &slen);
                    if (!utf8) throw nb::python_error();
                    strs[i]    = utf8;
                    lens[i]    = static_cast<uint32_t>(slen);
                    is_null[i] = false;
                }
            }
            return build_via_own(strs, lens, is_null);
        },
        nb::arg("values"),
        "Build a DRAKEN_STRING Vector via draken_vector_own_string.\n"
        "values: list[str | None]. Proves the bridge ownership path.\n"
        "to_pylist() and _slot_fields() must agree with vector_from_string_sequence."
    );

    // stress_construct_destroy: build and immediately destroy n Vectors.
    // Each iteration allocates slots + arena + validity, calls draken_vector_own_string,
    // then lets Python GC free the Vector (no reference kept). Tests RAII correctness.
    m.def("stress_construct_destroy",
        [](nb::list values, uint32_t iterations) -> uint32_t {
            const uint32_t n = static_cast<uint32_t>(values.size());

            std::vector<const char*> strs(n, nullptr);
            std::vector<uint32_t>    lens(n, 0u);
            std::vector<bool>        is_null(n, false);

            for (uint32_t i = 0u; i < n; ++i) {
                nb::object obj = values[static_cast<Py_ssize_t>(i)];
                if (obj.is_none()) {
                    is_null[i] = true;
                } else {
                    PyObject* s = obj.ptr();
                    if (!PyUnicode_Check(s))
                        throw std::invalid_argument(
                            "stress_construct_destroy: element is not str or None");
                    Py_ssize_t slen = 0;
                    const char* utf8 = PyUnicode_AsUTF8AndSize(s, &slen);
                    if (!utf8) throw nb::python_error();
                    strs[i]    = utf8;
                    lens[i]    = static_cast<uint32_t>(slen);
                }
            }

            for (uint32_t k = 0u; k < iterations; ++k) {
                // build_via_own returns a nb::object; when it goes out of scope,
                // the Python refcount drops to 0 and VectorOwner's RAII destructor
                // calls draken_free on all three buffers.
                nb::object vec = build_via_own(strs, lens, is_null);
                (void)vec;  // destructor frees on scope exit
            }
            return iterations;
        },
        nb::arg("values"), nb::arg("iterations"),
        "Construct and destroy `iterations` string Vectors via draken_vector_own_string.\n"
        "Each Vector is immediately released (refcount → 0) to exercise RAII teardown.\n"
        "Returns the iteration count on success; raises on any alloc failure."
    );
}
