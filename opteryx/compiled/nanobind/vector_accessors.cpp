// opteryx/compiled/nanobind/vector_accessors.cpp — Milestone E.6, Phase 5 (pure C′ batch).
// Updated E.7: null-TVL fix + string type family (VARCHAR/NVARCHAR/VARBINARY) + per-type LENGTH.
//
// C′ pattern: pure nanobind C++, zero Cython.  One NB_MODULE, 4 functions.
//
// Functions:
//   vector_string_length     — per-row length → INT64:
//                              VARCHAR/VARBINARY: byte length
//                              NVARCHAR: UTF-8 codepoint count
//   vector_octet_length      — per-row byte length → INT64 (always bytes,
//                              regardless of string type)
//   vector_string_is_empty   — per-row empty check (string family → BOOL; null propagates)
//   vector_string_is_not_empty — inverse (string family → BOOL; null propagates)
//   vector_length            — per-row array element count (ARRAY → INT64)
//
// Replaces:
//   opteryx/compiled/vector_ops/vector_string_length.pyx
//   opteryx/compiled/vector_ops/vector_string_emptiness.pyx
//   opteryx/compiled/vector_ops/vector_length.pyx
//
// Null semantics:
//   vector_string_length: null input row → null output (SQL 3VL: LENGTH(NULL)=NULL).
//     Output validity bitmap is a copy of input validity when nulls are present.
//   vector_length: null input row → 0, no output validity (preserves old .pyx parity).
//   vector_string_is_empty / vector_string_is_not_empty: null row → null.
//     Output validity is a copy of input validity (SQL 3VL).
//
// Not ported here (require DrakenStringArena single-block output, no bridge):
//   vector_map_access_string — stays Cython in vector_map_access.pyx.

#include <Python.h>
#include <nanobind/nanobind.h>
#include <cstdint>
#include <cstring>

#include "core/buffers.h"
#include "core/alloc.h"
#include "core/string_slot.h"
#include "core/draken_bridge.h"

namespace nb = nanobind;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

static const DrakenVector* unwrap(nb::object obj) {
    const DrakenVector* dv = draken_vector_unwrap(obj.ptr());
    if (!dv) throw nb::python_error();
    return dv;
}

static nb::object wrap_int64(int64_t* data, uint8_t* validity, uint32_t n) {
    PyObject* out = draken_vector_own_raw(data, validity, n, DRAKEN_INT64);
    if (!out) throw nb::python_error();
    return nb::steal<nb::object>(out);
}

static nb::object wrap_bool(uint8_t* bits, uint8_t* validity, uint32_t n) {
    PyObject* out = draken_vector_own_raw(bits, validity, n, DRAKEN_BOOL);
    if (!out) throw nb::python_error();
    return nb::steal<nb::object>(out);
}

static inline bool row_valid(const uint8_t* validity, uint32_t i) {
    return (validity[i >> 3u] >> (i & 7u)) & 1u;
}

// ---------------------------------------------------------------------------
// vector_string_length
// ---------------------------------------------------------------------------

// Count UTF-8 codepoints: lead bytes are those where (b & 0xC0) != 0x80.
// Continuation bytes (10xxxxxx) are not counted. Invalid lone continuations
// count loosely (not an error — documented in E.7 ticket).
static inline uint32_t count_utf8_codepoints(const uint8_t* bytes, uint32_t nbytes) {
    uint32_t n = 0u;
    for (uint32_t k = 0u; k < nbytes; ++k)
        if ((bytes[k] & 0xC0u) != 0x80u) ++n;
    return n;
}

static nb::object impl_vector_string_length(nb::object v, bool force_bytes) {
    const DrakenVector* dv = unwrap(v);
    const bool is_varchar_family =
        dv->type == DRAKEN_VARCHAR  ||
        dv->type == DRAKEN_NVARCHAR ||
        dv->type == DRAKEN_VARBINARY;
    if (!is_varchar_family) {
        throw nb::type_error(
            "vector_string_length: expected a string Vector "
            "(VARCHAR, NVARCHAR, or VARBINARY)");
    }

    const DrakenStringArena* sa  = static_cast<const DrakenStringArena*>(dv->data);
    const uint32_t* sel      = dv->selection;
    const uint8_t*  validity = dv->validity;
    const uint32_t  n        = dv->length;
    // OCTET_LENGTH forces byte counting; LENGTH/CHAR_LENGTH counts UTF-8
    // codepoints only for the Unicode type (NVARCHAR), bytes otherwise.
    const bool      nvarchar = (!force_bytes) && (dv->type == DRAKEN_NVARCHAR);

    int64_t* result = static_cast<int64_t*>(
        draken_malloc((n > 0u ? n : 1u) * sizeof(int64_t)));
    if (!result) throw std::bad_alloc();

    // Propagate input validity to output (SQL 3VL: LENGTH(NULL) = NULL).
    uint8_t* out_validity = nullptr;
    if (validity) {
        const uint32_t vbytes  = (n + 7u) / 8u;
        const uint32_t padded  = ((vbytes + 7u) & ~7u);
        const size_t   vsize   = padded > 0u ? padded : 8u;
        out_validity = static_cast<uint8_t*>(draken_malloc(vsize));
        if (!out_validity) { draken_free(result); throw std::bad_alloc(); }
        std::memcpy(out_validity, validity, vsize);
        // Mask tail bits beyond n so they don't appear valid.
        if ((n & 7u) != 0u && vbytes > 0u)
            out_validity[vbytes - 1u] &= static_cast<uint8_t>((1u << (n & 7u)) - 1u);
    }

    for (uint32_t i = 0u; i < n; ++i) {
        if (validity && !row_valid(validity, i)) {
            result[i] = 0;  // null row: data is don't-care; validity bit marks it null
        } else {
            const DrakenStringSlot* slot = &sa->slots[sel[i]];
            const uint32_t byte_len = str_length(slot);
            if (nvarchar) {
                // Codepoint count: scan bytes counting non-continuation bytes.
                const uint8_t* bytes = str_data(slot, sa->arena);
                result[i] = static_cast<int64_t>(
                    count_utf8_codepoints(bytes, byte_len));
            } else {
                result[i] = static_cast<int64_t>(byte_len);
            }
        }
    }
    return wrap_int64(result, out_validity, n);
}

// ---------------------------------------------------------------------------
// vector_string_is_empty / vector_string_is_not_empty
// ---------------------------------------------------------------------------

static nb::object impl_string_emptiness(nb::object v, bool emit_when_empty) {
    const DrakenVector* dv = unwrap(v);
    const bool is_varchar_family =
        dv->type == DRAKEN_VARCHAR  ||
        dv->type == DRAKEN_NVARCHAR ||
        dv->type == DRAKEN_VARBINARY;
    if (!is_varchar_family) {
        throw nb::type_error(
            "vector_string_is_empty: expected a string Vector "
            "(VARCHAR, NVARCHAR, or VARBINARY)");
    }

    const DrakenStringArena* arena = static_cast<const DrakenStringArena*>(dv->data);
    const uint32_t* sel      = dv->selection;
    const uint8_t*  validity = dv->validity;
    const uint32_t  n        = dv->length;
    const uint32_t  nbytes   = (n + 7u) / 8u;

    uint8_t* bits = static_cast<uint8_t*>(
        draken_malloc(nbytes > 0u ? nbytes : 1u));
    if (!bits) throw std::bad_alloc();
    std::memset(bits, 0, nbytes > 0u ? nbytes : 1u);

    uint8_t* out_validity = nullptr;
    if (validity) {
        out_validity = static_cast<uint8_t*>(
            draken_malloc(nbytes > 0u ? nbytes : 1u));
        if (!out_validity) { draken_free(bits); throw std::bad_alloc(); }
        std::memcpy(out_validity, validity, nbytes);
        if ((n & 7u) != 0u)
            out_validity[nbytes - 1u] &= static_cast<uint8_t>((1u << (n & 7u)) - 1u);
    }

    for (uint32_t i = 0u; i < n; ++i) {
        if (validity && !row_valid(validity, i)) continue;
        bool is_empty = (str_length(&arena->slots[sel[i]]) == 0u);
        if (is_empty == emit_when_empty)
            bits[i >> 3u] |= static_cast<uint8_t>(1u << (i & 7u));
    }
    return wrap_bool(bits, out_validity, n);
}

// ---------------------------------------------------------------------------
// vector_length — per-row array element count
// ---------------------------------------------------------------------------

static nb::object impl_vector_length(nb::object v) {
    const DrakenVector* dv = unwrap(v);
    if (dv->type != DRAKEN_ARRAY) {
        throw nb::type_error("vector_length: expected an ARRAY Vector");
    }

    // For DRAKEN_ARRAY: vec.data = int32_t* offsets (length+1 entries).
    // element count at logical row i = offsets[sel[i]+1] - offsets[sel[i]].
    const int32_t*  offsets  = static_cast<const int32_t*>(dv->data);
    const uint32_t* sel      = dv->selection;
    const uint8_t*  validity = dv->validity;
    const uint32_t  n        = dv->length;

    int64_t* result = static_cast<int64_t*>(
        draken_malloc((n > 0u ? n : 1u) * sizeof(int64_t)));
    if (!result) throw std::bad_alloc();

    for (uint32_t i = 0u; i < n; ++i) {
        if (validity && !row_valid(validity, i)) {
            result[i] = 0;
        } else {
            const uint32_t s = sel[i];
            result[i] = static_cast<int64_t>(offsets[s + 1u] - offsets[s]);
        }
    }
    return wrap_int64(result, nullptr, n);
}

// ---------------------------------------------------------------------------
// NB_MODULE
// ---------------------------------------------------------------------------

NB_MODULE(vector_accessors, m) {

    m.def("vector_string_length",
        [](nb::object v) -> nb::object { return impl_vector_string_length(v, false); },
        nb::arg("v"),
        "LENGTH(v): per-row length → INT64. null input → null output (SQL 3VL).\n"
        "VARCHAR/VARBINARY: byte length. NVARCHAR: UTF-8 codepoint count.");

    m.def("vector_octet_length",
        [](nb::object v) -> nb::object { return impl_vector_string_length(v, true); },
        nb::arg("v"),
        "OCTET_LENGTH(v): per-row BYTE length → INT64, regardless of string\n"
        "type. null input → null output (SQL 3VL).");

    m.def("vector_string_is_empty",
        [](nb::object v) -> nb::object { return impl_string_emptiness(v, true); },
        nb::arg("v"),
        "IS EMPTY: True where string length == 0. null propagates (SQL 3VL).");

    m.def("vector_string_is_not_empty",
        [](nb::object v) -> nb::object { return impl_string_emptiness(v, false); },
        nb::arg("v"),
        "IS NOT EMPTY: True where string length > 0. null propagates (SQL 3VL).");

    m.def("vector_length",
        [](nb::object v) -> nb::object { return impl_vector_length(v); },
        nb::arg("v"),
        "LENGTH(v): per-row ARRAY element count → INT64. null → 0 (no output validity).");
}
