// opteryx/compiled/nanobind/vector_split_native.cpp — Milestone E.16b, C′.
//
// C′ pattern: pure nanobind C++, zero Cython.  One NB_MODULE, one function:
//
//   vector_split(vec, delimiter) — bytewise single-byte split → DRAKEN_ARRAY[VARCHAR].
//
// Accepts VARCHAR, NVARCHAR, VARBINARY.  Delimiter is a Python int (0–255).
//
// Null TVL: null input row → null output row (zero-length child slice, validity cleared).
// All child elements are always valid — individual segments are never null.
//
// Empty string input: produces a single-element child (the empty string).
// Consecutive delimiters: produce empty-string segments (no collapsing).
//
// Two-pass algorithm:
//   Pass 1: count total child segments + accumulate upper bound on child arena bytes.
//   Allocate child_slots, child_arena, parent_offsets, parent_validity.
//   Pass 2: scan each row and emit slots + arena bytes via draken_build_string_slot.
//   Call draken_vector_own_array to wrap everything (transfers all buffer ownership).
//
// Replaces: opteryx/compiled/vector_ops/vector_split.pyx

#include <Python.h>
#include <nanobind/nanobind.h>
#include <cstdint>
#include <cstring>
#include <stdexcept>
#include <string>

#include "core/buffers.h"
#include "core/alloc.h"
#include "core/string_slot.h"
#include "core/vector_alloc.h"
#include "core/draken_bridge.h"

namespace nb = nanobind;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

static inline bool is_valid_at(const DrakenVector* dv, uint32_t i) noexcept {
    if (!dv->validity) return true;
    return ((dv->validity[i >> 3] >> (i & 7u)) & 1u) != 0u;
}

// ---------------------------------------------------------------------------
// impl_split — core implementation
// ---------------------------------------------------------------------------

static nb::object impl_split(nb::object vec_obj, nb::object delim_obj)
{
    // Validate and extract delimiter byte.
    if (!PyLong_Check(delim_obj.ptr()))
        throw std::invalid_argument("vector_split: delimiter must be an int (0–255)");
    const long dval = PyLong_AsLong(delim_obj.ptr());
    if (dval == -1L && PyErr_Occurred()) throw nb::python_error();
    if (dval < 0L || dval > 255L)
        throw std::invalid_argument("vector_split: delimiter must be in range 0–255");
    const uint8_t delim = static_cast<uint8_t>(dval);

    // Unwrap input vector.
    const DrakenVector* vec = draken_vector_unwrap(vec_obj.ptr());
    if (!vec) throw nb::python_error();
    if (vec->type != DRAKEN_VARCHAR  &&
        vec->type != DRAKEN_NVARCHAR &&
        vec->type != DRAKEN_VARBINARY)
        throw nb::type_error(
            "vector_split: input must be a string-family Vector "
            "(VARCHAR, NVARCHAR, or VARBINARY)");

    const uint32_t n = vec->length;
    const DrakenStringArena* sa = static_cast<const DrakenStringArena*>(vec->data);

    // Allocate parent_offsets[n+1].
    const size_t off_bytes = ((size_t)n + 1u) * sizeof(int32_t);
    int32_t* parent_offsets = static_cast<int32_t*>(
        draken_malloc(off_bytes > 0u ? off_bytes : sizeof(int32_t)));
    if (!parent_offsets) throw std::bad_alloc();

    // ---------------------------------------------------------------------------
    // Pass 1: count total child segments; accumulate upper bound on arena bytes.
    // ---------------------------------------------------------------------------
    parent_offsets[0] = 0;
    uint32_t total_segments  = 0u;
    size_t   total_arena_ub  = 0u;
    bool     has_nulls       = false;

    for (uint32_t i = 0u; i < n; ++i) {
        if (!is_valid_at(vec, i)) {
            parent_offsets[i + 1u] = static_cast<int32_t>(total_segments);
            has_nulls = true;
            continue;
        }
        const uint32_t sel_i = vec->selection[i];
        const uint32_t slen  = str_length(&sa->slots[sel_i]);
        const uint8_t* bytes = str_data(&sa->slots[sel_i], sa->arena);

        uint32_t k = 0u;
        for (uint32_t j = 0u; j < slen; ++j)
            if (bytes[j] == delim) ++k;

        total_segments        += k + 1u;
        total_arena_ub        += slen;
        parent_offsets[i + 1u] = static_cast<int32_t>(total_segments);
    }

    // ---------------------------------------------------------------------------
    // Allocate child buffers.
    // ---------------------------------------------------------------------------
    const size_t slots_sz = (total_segments > 0u ? total_segments : 1u)
                            * sizeof(DrakenStringSlot);
    DrakenStringSlot* child_slots =
        static_cast<DrakenStringSlot*>(draken_malloc(slots_sz));
    if (!child_slots) {
        draken_free(parent_offsets);
        throw std::bad_alloc();
    }
    std::memset(child_slots, 0, slots_sz);

    uint8_t* child_arena = nullptr;
    if (total_arena_ub > 0u) {
        child_arena = static_cast<uint8_t*>(draken_malloc(total_arena_ub));
        if (!child_arena) {
            draken_free(child_slots);
            draken_free(parent_offsets);
            throw std::bad_alloc();
        }
    }

    uint8_t* parent_validity = nullptr;
    if (has_nulls) {
        const uint32_t bm     = (n + 7u) >> 3;
        const uint32_t padded = ((bm + 7u) & ~7u);
        const size_t   vbytes = padded > 0u ? padded : 8u;
        parent_validity = static_cast<uint8_t*>(draken_malloc(vbytes));
        if (!parent_validity) {
            draken_free(child_arena);
            draken_free(child_slots);
            draken_free(parent_offsets);
            throw std::bad_alloc();
        }
        std::memset(parent_validity, 0xFF, vbytes);
        for (uint32_t i = 0u; i < n; ++i) {
            if (!is_valid_at(vec, i))
                parent_validity[i >> 3] &= ~static_cast<uint8_t>(1u << (i & 7u));
        }
        // Zero-pad tail bits beyond row n to prevent stale data.
        if (n & 7u) {
            const uint32_t last = (n + 7u) >> 3;
            parent_validity[last - 1u] &= static_cast<uint8_t>((1u << (n & 7u)) - 1u);
        }
    }

    // ---------------------------------------------------------------------------
    // Pass 2: emit child slots + arena bytes.
    // ---------------------------------------------------------------------------
    uint32_t seg_cursor   = 0u;
    size_t   arena_cursor = 0u;

    for (uint32_t i = 0u; i < n; ++i) {
        if (!is_valid_at(vec, i)) continue;

        const uint32_t sel_i = vec->selection[i];
        const uint32_t slen  = str_length(&sa->slots[sel_i]);
        const uint8_t* bytes = str_data(&sa->slots[sel_i], sa->arena);

        uint32_t seg_start = 0u;
        for (uint32_t j = 0u; j <= slen; ++j) {
            if (j == slen || bytes[j] == delim) {
                const uint32_t seg_len   = j - seg_start;
                const uint8_t* seg_bytes = bytes + seg_start;

                if (seg_len <= static_cast<uint32_t>(STR_INLINE_MAX)) {
                    draken_build_string_slot(
                        &child_slots[seg_cursor], seg_bytes, seg_len, 0u);
                } else {
                    // Write bytes to arena first; build extern slot pointing there.
                    std::memcpy(child_arena + arena_cursor, seg_bytes, seg_len);
                    draken_build_string_slot(
                        &child_slots[seg_cursor], seg_bytes, seg_len,
                        static_cast<uint32_t>(arena_cursor));
                    arena_cursor += seg_len;
                }
                ++seg_cursor;
                seg_start = j + 1u;
            }
        }
    }

    // arena_cursor is actual arena bytes used (≤ total_arena_ub).  The bridge
    // copies only arena_cursor bytes and frees the over-allocated tail.

    // ---------------------------------------------------------------------------
    // Transfer ownership to bridge.  All buffers owned by bridge from this point.
    // ---------------------------------------------------------------------------
    PyObject* out = draken_vector_own_array(
        parent_offsets,
        child_slots,
        child_arena,
        arena_cursor,
        total_segments,
        DRAKEN_VARCHAR,
        parent_validity,
        n);
    if (!out) throw nb::python_error();
    return nb::steal<nb::object>(out);
}

// ---------------------------------------------------------------------------
// Module definition
// ---------------------------------------------------------------------------

NB_MODULE(vector_split_native, m) {
    m.doc() =
        "vector_split_native — bytewise single-byte string split → DRAKEN_ARRAY[VARCHAR].\n"
        "Milestone E.16b.  Replaces opteryx/compiled/vector_ops/vector_split.pyx.";

    m.def("vector_split", &impl_split,
          nb::arg("vec"), nb::arg("delimiter"),
          "Split each VARCHAR/NVARCHAR/VARBINARY string by a single-byte delimiter "
          "(Python int 0–255).  Returns a DRAKEN_ARRAY[VARCHAR] Vector.  "
          "Null input rows propagate as null output rows.");
}
