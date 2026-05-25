// opteryx/compiled/nanobind/vector_string_misc3.cpp — Milestone E.17, C′.
//
// C′ pattern: pure nanobind C++, zero Cython.  One NB_MODULE, one function:
//
//   vector_soundex(vec)  — VARCHAR → VARCHAR (4-char Soundex codes, e.g. "R163").
//
// Soundex algorithm:
//   American Soundex, matching the vendored opteryx/third_party/fuzzy/soundex.pyx
//   exactly.  Map: "01230120022455012623010202" for A-Z.
//   First char: uppercased letter.
//   Subsequent chars: map to digit code; skip '0' entries except:
//     - vowels (A/E/I/O/U/Y) reset prev_code to '0'
//     - H/W do not reset prev_code (treated as separators only)
//   Adjacent duplicates (same non-zero code) are collapsed.
//   Pad to 4 chars with '0'.
//   Empty input → null output row.  All outputs are exactly 4 bytes (inline slot).
//
// Null TVL: null input row → null output row.
//
// Replaces:
//   opteryx/compiled/vector_ops/vector_soundex.pyx

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
// Soundex algorithm
// ---------------------------------------------------------------------------
//
// Map matches opteryx/third_party/fuzzy/soundex.pyx:
//   cdef char* soundex_map = "01230120022455012623010202"
//
// Indices 0-25 correspond to A-Z.  Digit chars '0'..'6'.

static const char SOUNDEX_MAP[26] = {
    '0','1','2','3','0','1','2','0','0','2','2','4','5',
    '5','0','1','2','6','2','3','0','1','0','2','0','2'
};

static const uint32_t SOUNDEX_LEN = 4u;

// Compute Soundex code for raw bytes s[0..len).
// Writes exactly 4 ASCII bytes into out[0..4) and returns true.
// Returns false if the input contains no alphabetic characters (null output).
static bool soundex_compute(const uint8_t* s, uint32_t len, char out[4]) {
    uint32_t written = 0u;
    char prev_code = '\0';

    for (uint32_t i = 0u; i < len && written < SOUNDEX_LEN; ++i) {
        char c = static_cast<char>(s[i]);
        if (c >= 'a' && c <= 'z') c = static_cast<char>(c - 32);
        if (c < 'A' || c > 'Z')   continue;

        if (written == 0u) {
            out[written++] = c;
            prev_code = SOUNDEX_MAP[c - 'A'];
        } else {
            const char code = SOUNDEX_MAP[c - 'A'];
            if (code != '0') {
                if (code != prev_code) {
                    out[written++] = code;
                }
                prev_code = code;
            } else {
                // H and W: separator only — do not reset prev_code.
                // All true vowels (A/E/I/O/U) and Y map to '0'; reset prev_code.
                if (c != 'H' && c != 'W') {
                    prev_code = '0';
                }
            }
        }
    }

    if (written == 0u) return false;

    // Pad to exactly SOUNDEX_LEN with '0'.
    while (written < SOUNDEX_LEN) {
        out[written++] = '0';
    }
    return true;
}

// ---------------------------------------------------------------------------
// Shared null helper
// ---------------------------------------------------------------------------

static inline bool row_is_valid(const DrakenVector* dv, uint32_t i) noexcept {
    if (!dv->validity) return true;
    return ((dv->validity[i >> 3] >> (i & 7u)) & 1u) != 0u;
}

// Lazily allocate a validity bitmap initialised all-valid; clear bit i.
static void mark_null(uint8_t*& out_null, bool& any_null,
                      uint32_t i, uint32_t n, void* guard_slots) {
    if (!any_null) {
        const uint32_t bm     = (n + 7u) >> 3;
        const uint32_t padded = (bm + 7u) & ~7u;
        out_null = static_cast<uint8_t*>(
            draken_malloc(padded > 0u ? padded : 8u));
        if (!out_null) {
            draken_free(guard_slots);
            throw std::bad_alloc();
        }
        std::memset(out_null, 0xFF, padded > 0u ? padded : 8u);
        any_null = true;
    }
    out_null[i >> 3] &= ~static_cast<uint8_t>(1u << (i & 7u));
}

// ---------------------------------------------------------------------------
// vector_soundex — VARCHAR → VARCHAR (4-char Soundex codes)
// ---------------------------------------------------------------------------

static nb::object impl_soundex(nb::object vec_obj) {
    const DrakenVector* dv = draken_vector_unwrap(vec_obj.ptr());
    if (!dv) throw nb::python_error();

    const bool is_str =
        dv->type == DRAKEN_VARCHAR  ||
        dv->type == DRAKEN_NVARCHAR ||
        dv->type == DRAKEN_VARBINARY;
    if (!is_str)
        throw nb::type_error(
            "vector_soundex: expected a string-family Vector "
            "(VARCHAR, NVARCHAR, or VARBINARY)");

    const uint32_t n = dv->length;

    // Allocate output slot array.  All outputs are 4-byte inline — no arena needed.
    const size_t slots_sz = (n > 0u ? n : 1u) * sizeof(DrakenStringSlot);
    DrakenStringSlot* slots = static_cast<DrakenStringSlot*>(draken_malloc(slots_sz));
    if (!slots) throw std::bad_alloc();
    std::memset(slots, 0, slots_sz);

    uint8_t* out_null = nullptr;
    bool any_null = false;

    const DrakenStringArena* sa = static_cast<const DrakenStringArena*>(dv->data);
    char code[4];

    for (uint32_t i = 0u; i < n; ++i) {
        if (!row_is_valid(dv, i)) {
            mark_null(out_null, any_null, i, n, slots);
            // slots[i] already zeroed (null canonical form).
            continue;
        }

        const DrakenStringSlot* src = &sa->slots[dv->selection[i]];
        const uint8_t* bytes = str_data(src, sa->arena);
        const uint32_t len   = str_length(src);

        if (!soundex_compute(bytes, len, code)) {
            // Empty / no-alphabetic input → null output.
            mark_null(out_null, any_null, i, n, slots);
            continue;
        }

        // All soundex outputs are exactly 4 bytes ≤ STR_INLINE_MAX (12).
        draken_build_string_slot(
            &slots[i],
            reinterpret_cast<const uint8_t*>(code),
            4u,
            0u  // arena_offset ignored for inline slots
        );
    }

    // Clear validity tail bits beyond last complete byte.
    if (any_null && (n & 7u)) {
        const uint32_t bm = (n + 7u) >> 3;
        out_null[bm - 1u] &= static_cast<uint8_t>((1u << (n & 7u)) - 1u);
    }

    // arena is NULL — all slots are inline.
    PyObject* out = draken_vector_own_string(
        slots, nullptr, 0u, out_null, n, DRAKEN_VARCHAR);
    if (!out) throw nb::python_error();
    return nb::steal<nb::object>(out);
}

// ---------------------------------------------------------------------------
// NB_MODULE
// ---------------------------------------------------------------------------

NB_MODULE(vector_string_misc3, m) {

    m.def("vector_soundex",
        [](nb::object vec) -> nb::object {
            return impl_soundex(vec);
        },
        nb::arg("vec"),
        "Compute American Soundex codes for each string row.\n"
        "Output: DRAKEN_VARCHAR.  All codes are exactly 4 bytes (e.g. 'R163').\n"
        "Null TVL: null or empty/non-alpha input row → null output.\n"
        "Algorithm matches opteryx/third_party/fuzzy/soundex.pyx exactly.");
}
