// opteryx/compiled/nanobind/vector_string_case.cpp — Milestone E.26.
//
// C′ pattern: pure nanobind C++, zero Cython.  One NB_MODULE, one function:
//
//   vector_lowercase(input)  — lower-case a string column.
//
// Per-type dispatch (per draken-string-type-family design):
//   DRAKEN_VARCHAR:    ASCII-only fold via simd_to_lower.  Non-ASCII bytes (>127)
//                      are left unchanged.  Cost: one memcpy + SIMD pass per row.
//   DRAKEN_NVARCHAR:   Unicode codepoint fold via utf8.h utf8lwr.  Length-preserving
//                      only — all case mappings handled by utf8lwrcodepoint stay within
//                      the same UTF-8 byte width, so output length == input length.
//                      Codepoints with longer lowercase forms (e.g. ß → ss) are not
//                      expanded; utf8.h performs only in-range codepoint substitution.
//   DRAKEN_VARBINARY:  throws std::invalid_argument → Python ValueError.
//                      Case operations on opaque bytes are unsupported by design.
//   DRAKEN_DICTIONARY,
//   DRAKEN_CONSTANT:   treated as VARCHAR (ASCII fold).  Dict/constant are encoding
//                      shapes, not a distinct base type; lowercase output is dense.
//   Other types:       throws nb::type_error.
//
// Null TVL: null input row → null output row.  Validity bitmap allocated lazily
// when the first null row is encountered.
//
// Replaces (partially): opteryx/compiled/vector_ops/vector_lowercase.pyx.
// The old .pyx is intentionally NOT deleted here — the four remaining UTF-8
// cluster files (uppercase, initcap, reverse, string_slice) still build from it.
// Deletion is a single cleanup ticket after all five cluster ports land.
//
// utf8.h: sheredom/utf8.h MIT, commit 1194293f5b56 (2026-05-23).
//   Vendored at third_party/utf8h/utf8.h — no modifications.

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

#include "simd_string_ops.h"
#include "utf8.h"

namespace nb = nanobind;

// ---------------------------------------------------------------------------
// Shared helpers (same pattern as vector_string_misc.cpp)
// ---------------------------------------------------------------------------

static inline bool is_valid_at(const DrakenVector* dv, uint32_t i) {
    if (!dv->validity) return true;
    return ((dv->validity[i >> 3] >> (i & 7u)) & 1u) != 0u;
}

static uint8_t* alloc_validity_all_valid(uint32_t n) {
    if (n == 0u) return nullptr;
    const uint32_t nbytes = (n + 7u) >> 3;
    uint8_t* v = static_cast<uint8_t*>(draken_malloc(nbytes));
    if (!v) throw std::bad_alloc();
    std::memset(v, 0xFF, nbytes);
    return v;
}

// ---------------------------------------------------------------------------
// impl_lowercase
// ---------------------------------------------------------------------------

static nb::object impl_lowercase(nb::object in_obj) {
    const DrakenVector* dv = draken_vector_unwrap(in_obj.ptr());
    if (!dv) throw nb::python_error();

    // Type check + VARBINARY guard (per string-family design).
    if (dv->type == DRAKEN_VARBINARY)
        throw std::invalid_argument(
            "vector_lowercase: VARBINARY does not support case operations");
    const bool is_nvarchar = (dv->type == DRAKEN_NVARCHAR);
    if (dv->type != DRAKEN_VARCHAR &&
        dv->type != DRAKEN_NVARCHAR &&
        dv->type != DRAKEN_DICTIONARY &&
        dv->type != DRAKEN_CONSTANT)
        throw nb::type_error(
            "vector_lowercase: expected VARCHAR, NVARCHAR, DICTIONARY, or CONSTANT Vector");

    const uint32_t n  = dv->length;
    const DrakenStringArena* sa = static_cast<const DrakenStringArena*>(dv->data);

    // Output type tag: NVARCHAR input → NVARCHAR output; everything else → VARCHAR.
    const DrakenType out_type = is_nvarchar ? DRAKEN_NVARCHAR : DRAKEN_VARCHAR;

    // Allocate output slots.
    const size_t slots_sz = (n > 0u ? n : 1u) * sizeof(DrakenStringSlot);
    DrakenStringSlot* out_slots = static_cast<DrakenStringSlot*>(draken_malloc(slots_sz));
    if (!out_slots) throw std::bad_alloc();
    std::memset(out_slots, 0, slots_sz);

    // Arena: pre-size to match input (lowercase is length-preserving for utf8lwr,
    // and strictly same-length for ASCII fold).
    size_t arena_cap  = (sa->arena_used > 0u ? sa->arena_used : 64u);
    uint8_t* out_arena = static_cast<uint8_t*>(draken_malloc(arena_cap));
    if (!out_arena) { draken_free(out_slots); throw std::bad_alloc(); }
    size_t arena_used  = 0u;

    uint8_t* validity = nullptr;
    uint8_t* tmp_buf  = nullptr;
    size_t   tmp_cap  = 0u;

    // Guard: frees all four buffers on exception.  Uses pointer-to-pointer so
    // reallocation of out_arena or tmp_buf is automatically visible to the guard.
    struct Guard {
        DrakenStringSlot** sp;
        uint8_t**          ap;
        uint8_t**          vp;
        uint8_t**          tp;
        void release() { sp = nullptr; ap = nullptr; vp = nullptr; tp = nullptr; }
        ~Guard() {
            if (sp) draken_free(*sp);
            if (ap) draken_free(*ap);
            if (vp && *vp) draken_free(*vp);
            if (tp && *tp) draken_free(*tp);
        }
    } g{&out_slots, &out_arena, &validity, &tmp_buf};

    bool any_null = false;

    for (uint32_t i = 0u; i < n; ++i) {
        if (!is_valid_at(dv, i)) {
            if (!validity) { validity = alloc_validity_all_valid(n); }
            validity[i >> 3] &= ~static_cast<uint8_t>(1u << (i & 7u));
            str_init_null(&out_slots[i]);
            any_null = true;
            continue;
        }

        const DrakenStringSlot* slot = &sa->slots[dv->selection[i]];
        const uint32_t slen  = str_length(slot);
        const uint8_t* sdata = str_data(slot, sa->arena);

        if (slen == 0u) {
            str_init_inline(&out_slots[i], nullptr, 0u);
            continue;
        }

        // Grow temp buffer as needed.
        // NVARCHAR needs +1 byte for null terminator; VARCHAR does not.
        const size_t need = static_cast<size_t>(slen) + (is_nvarchar ? 1u : 0u);
        if (need > tmp_cap) {
            auto* new_tmp = static_cast<uint8_t*>(draken_malloc(need));
            if (!new_tmp) throw std::bad_alloc();
            draken_free(tmp_buf);
            tmp_buf = new_tmp;
            tmp_cap = need;
        }

        std::memcpy(tmp_buf, sdata, slen);

        if (!is_nvarchar) {
            // VARCHAR: ASCII-only fold, no null terminator.
            simd_to_lower(reinterpret_cast<char*>(tmp_buf), slen);
        } else {
            // NVARCHAR: null-terminate, fold codepoint-by-codepoint via utf8lwr.
            // utf8lwr is length-preserving (all utf8lwrcodepoint ranges map to same
            // UTF-8 byte width), so out_len == slen.
            // In C++20 utf8_int8_t is char8_t (not char), so we cast accordingly.
            tmp_buf[slen] = '\0';
            utf8lwr(reinterpret_cast<utf8_int8_t*>(tmp_buf));
        }
        const uint32_t out_len = slen;  // length-preserving for both paths

        // Build output slot.
        if (out_len <= STR_INLINE_MAX) {
            draken_build_string_slot(&out_slots[i], tmp_buf, out_len, 0u);
        } else {
            // Grow arena if needed.
            if (arena_used + out_len > arena_cap) {
                size_t new_cap = arena_cap * 2u;
                if (new_cap < arena_used + out_len) new_cap = arena_used + out_len;
                auto* new_arena = static_cast<uint8_t*>(draken_malloc(new_cap));
                if (!new_arena) throw std::bad_alloc();
                if (arena_used > 0u) std::memcpy(new_arena, out_arena, arena_used);
                draken_free(out_arena);
                out_arena = new_arena;
                arena_cap = new_cap;
            }
            const uint32_t off = static_cast<uint32_t>(arena_used);
            std::memcpy(out_arena + off, tmp_buf, out_len);
            draken_build_string_slot(&out_slots[i], tmp_buf, out_len, off);
            arena_used += out_len;
        }
    }

    draken_free(tmp_buf);
    tmp_buf = nullptr;

    if (!any_null && validity) { draken_free(validity); validity = nullptr; }

    g.release();
    PyObject* out = draken_vector_own_string(
        out_slots, out_arena, arena_used, validity, n, out_type);
    if (!out) throw nb::python_error();
    return nb::steal<nb::object>(out);
}

// ---------------------------------------------------------------------------
// NB_MODULE
// ---------------------------------------------------------------------------

NB_MODULE(vector_string_case, m) {
    m.def("vector_lowercase",
        [](nb::object input) -> nb::object {
            return impl_lowercase(input);
        },
        nb::arg("input"),
        "Lower-case a string column.  Per-type dispatch:\n"
        "  VARCHAR:    ASCII-only fold (simd_to_lower; non-ASCII bytes unchanged).\n"
        "  NVARCHAR:   Unicode codepoint fold via utf8.h utf8lwr.  Length-preserving\n"
        "              only — codepoints with longer lowercase forms are not expanded.\n"
        "  VARBINARY:  raises ValueError (case ops on opaque bytes are unsupported).\n"
        "Null TVL: null input row → null output row.");
}
