// opteryx/compiled/nanobind/vector_string_case.cpp — Milestone E.26/E.26+.
//
// C′ pattern: pure nanobind C++, zero Cython.  One NB_MODULE, four functions:
//
//   vector_lowercase(input)  — lower-case a string column.
//   vector_uppercase(input)  — upper-case a string column.
//   vector_initcap(input)    — title-case (first letter of each word upper, rest lower).
//   vector_reverse(input)    — reverse each string (UTF-8 codepoint-aware for NVARCHAR).
//
// Per-type dispatch (per draken-string-type-family design):
//   DRAKEN_VARCHAR:    ASCII-only fold / byte-level operation.
//   DRAKEN_NVARCHAR:   Unicode codepoint fold via utf8.h (case ops) or codepoint
//                      reversal (reverse op).
//   DRAKEN_VARBINARY:  same ASCII-only byte fold as VARCHAR (case ops, trim) or
//                      byte-reversal (reverse op) — same byte layout as VARCHAR,
//                      no UTF-8 assumption made or needed; tag is preserved.
//   Other types:       throws nb::type_error.
//
// Null TVL: null input row → null output row.  Validity bitmap allocated lazily.
//
// Replaces: opteryx/compiled/vector_ops/vector_lowercase.pyx
//           opteryx/compiled/vector_ops/vector_uppercase.pyx
//           opteryx/compiled/vector_ops/vector_initcap.pyx
//           opteryx/compiled/vector_ops/vector_reverse.pyx
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

// Below this length the per-string indirect call into simd_to_upper/lower costs
// more than it saves: its vectorised body only engages at >= 32 bytes, so for
// shorter strings the call is pure dispatch + indirect-branch overhead on top of
// a scalar tail. Fold those inline instead (one copy+transform pass, no call).
// Longer strings keep the vectorised path, which amortises the call.
static constexpr uint32_t SIMD_FOLD_MIN = 32u;

// ---------------------------------------------------------------------------
// impl_lowercase
// ---------------------------------------------------------------------------

static nb::object impl_lowercase(nb::object in_obj) {
    const DrakenVector* dv = draken_vector_unwrap(in_obj.ptr());
    if (!dv) throw nb::python_error();

    // Type check. VARBINARY takes the same ASCII-only byte fold as VARCHAR —
    // same DrakenStringSlot/arena layout, non-ASCII bytes pass through
    // unchanged either way, so folding opaque bytes is safe; it just isn't
    // NVARCHAR's UTF-8-aware path.
    const bool is_nvarchar = (dv->type == DRAKEN_NVARCHAR);
    if (dv->type != DRAKEN_VARCHAR &&
        dv->type != DRAKEN_NVARCHAR &&
        dv->type != DRAKEN_VARBINARY)
        throw nb::type_error(
            "vector_lowercase: expected VARCHAR, NVARCHAR, or VARBINARY Vector");

    // GIL-free compute: in_obj keeps the source arena alive for the whole call
    // and everything below — allocation, the fold loop, native cleanup — touches
    // no Python (draken_malloc + string slots). Drop the GIL until we publish the
    // result (§2). The matching gil_scoped_acquire at the tail re-takes it for
    // draken_vector_own_string; gil_scoped_release re-acquires in its destructor
    // during exception unwind too (bad_alloc from the loop).
    nb::gil_scoped_release _rel;

    const uint32_t n  = dv->length;
    const DrakenStringArena* sa = static_cast<const DrakenStringArena*>(dv->data);

    // Output type tag matches the input's (VARCHAR->VARCHAR, VARBINARY->VARBINARY,
    // NVARCHAR->NVARCHAR) — the tag is preserved, never widened.
    const DrakenType out_type = is_nvarchar ? DRAKEN_NVARCHAR : dv->type;

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

        if (!is_nvarchar) {
            // VARCHAR: ASCII-only fold (non-ASCII bytes pass through unchanged,
            // matching simd_to_lower). Short strings fold inline in a single
            // copy+transform pass to skip the per-string simd_to_lower call.
            if (slen < SIMD_FOLD_MIN) {
                for (uint32_t j = 0u; j < slen; ++j) {
                    const uint8_t c = sdata[j];
                    tmp_buf[j] = (c >= 'A' && c <= 'Z')
                        ? static_cast<uint8_t>(c + 32u) : c;
                }
            } else {
                std::memcpy(tmp_buf, sdata, slen);
                simd_to_lower(reinterpret_cast<char*>(tmp_buf), slen);
            }
        } else {
            // NVARCHAR: null-terminate, fold codepoint-by-codepoint via utf8lwr.
            // utf8lwr is length-preserving (all utf8lwrcodepoint ranges map to same
            // UTF-8 byte width), so out_len == slen.
            // In C++20 utf8_int8_t is char8_t (not char), so we cast accordingly.
            std::memcpy(tmp_buf, sdata, slen);
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

    nb::gil_scoped_acquire _acq;  // re-take the GIL to publish the result to Python
    g.release();
    PyObject* out = draken_vector_own_string(
        out_slots, out_arena, arena_used, validity, n, out_type);
    if (!out) throw nb::python_error();
    return nb::steal<nb::object>(out);
}

// ---------------------------------------------------------------------------
// impl_uppercase
// ---------------------------------------------------------------------------

static nb::object impl_uppercase(nb::object in_obj) {
    const DrakenVector* dv = draken_vector_unwrap(in_obj.ptr());
    if (!dv) throw nb::python_error();

    // VARBINARY takes the same ASCII-only byte fold as VARCHAR — see
    // impl_lowercase for the full rationale.
    const bool is_nvarchar = (dv->type == DRAKEN_NVARCHAR);
    if (dv->type != DRAKEN_VARCHAR && dv->type != DRAKEN_NVARCHAR && dv->type != DRAKEN_VARBINARY)
        throw nb::type_error(
            "vector_uppercase: expected VARCHAR, NVARCHAR, or VARBINARY Vector");

    // GIL-free compute — see impl_lowercase for the full rationale.
    nb::gil_scoped_release _rel;

    const uint32_t n  = dv->length;
    const DrakenStringArena* sa = static_cast<const DrakenStringArena*>(dv->data);
    const DrakenType out_type = is_nvarchar ? DRAKEN_NVARCHAR : dv->type;

    const size_t slots_sz = (n > 0u ? n : 1u) * sizeof(DrakenStringSlot);
    DrakenStringSlot* out_slots = static_cast<DrakenStringSlot*>(draken_malloc(slots_sz));
    if (!out_slots) throw std::bad_alloc();
    std::memset(out_slots, 0, slots_sz);

    size_t arena_cap  = (sa->arena_used > 0u ? sa->arena_used : 64u);
    uint8_t* out_arena = static_cast<uint8_t*>(draken_malloc(arena_cap));
    if (!out_arena) { draken_free(out_slots); throw std::bad_alloc(); }
    size_t arena_used  = 0u;

    uint8_t* validity = nullptr;
    uint8_t* tmp_buf  = nullptr;
    size_t   tmp_cap  = 0u;

    struct Guard {
        DrakenStringSlot** sp; uint8_t** ap; uint8_t** vp; uint8_t** tp;
        void release() { sp = nullptr; ap = nullptr; vp = nullptr; tp = nullptr; }
        ~Guard() {
            if (sp) draken_free(*sp); if (ap) draken_free(*ap);
            if (vp && *vp) draken_free(*vp); if (tp && *tp) draken_free(*tp);
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

        if (slen == 0u) { str_init_inline(&out_slots[i], nullptr, 0u); continue; }

        const size_t need = static_cast<size_t>(slen) + (is_nvarchar ? 1u : 0u);
        if (need > tmp_cap) {
            auto* new_tmp = static_cast<uint8_t*>(draken_malloc(need));
            if (!new_tmp) throw std::bad_alloc();
            draken_free(tmp_buf); tmp_buf = new_tmp; tmp_cap = need;
        }

        if (!is_nvarchar) {
            // VARCHAR: ASCII-only fold (non-ASCII bytes pass through unchanged,
            // matching simd_to_upper). Short strings fold inline in a single
            // copy+transform pass to skip the per-string simd_to_upper call.
            if (slen < SIMD_FOLD_MIN) {
                for (uint32_t j = 0u; j < slen; ++j) {
                    const uint8_t c = sdata[j];
                    tmp_buf[j] = (c >= 'a' && c <= 'z')
                        ? static_cast<uint8_t>(c - 32u) : c;
                }
            } else {
                std::memcpy(tmp_buf, sdata, slen);
                simd_to_upper(reinterpret_cast<char*>(tmp_buf), slen);
            }
        } else {
            std::memcpy(tmp_buf, sdata, slen);
            tmp_buf[slen] = '\0';
            utf8upr(reinterpret_cast<utf8_int8_t*>(tmp_buf));
        }
        const uint32_t out_len = slen;

        if (out_len <= STR_INLINE_MAX) {
            draken_build_string_slot(&out_slots[i], tmp_buf, out_len, 0u);
        } else {
            if (arena_used + out_len > arena_cap) {
                size_t new_cap = arena_cap * 2u;
                if (new_cap < arena_used + out_len) new_cap = arena_used + out_len;
                auto* new_arena = static_cast<uint8_t*>(draken_malloc(new_cap));
                if (!new_arena) throw std::bad_alloc();
                if (arena_used > 0u) std::memcpy(new_arena, out_arena, arena_used);
                draken_free(out_arena); out_arena = new_arena; arena_cap = new_cap;
            }
            const uint32_t off = static_cast<uint32_t>(arena_used);
            std::memcpy(out_arena + off, tmp_buf, out_len);
            draken_build_string_slot(&out_slots[i], tmp_buf, out_len, off);
            arena_used += out_len;
        }
    }

    draken_free(tmp_buf); tmp_buf = nullptr;
    if (!any_null && validity) { draken_free(validity); validity = nullptr; }

    nb::gil_scoped_acquire _acq;  // re-take the GIL to publish the result to Python
    g.release();
    PyObject* out = draken_vector_own_string(
        out_slots, out_arena, arena_used, validity, n, out_type);
    if (!out) throw nb::python_error();
    return nb::steal<nb::object>(out);
}

// ---------------------------------------------------------------------------
// impl_initcap — title-case: uppercase first letter of each word, lowercase rest.
// Word boundary: any non-alphanumeric ASCII character (space, punct).
// VARCHAR: ASCII-only.  NVARCHAR: codepoint-aware via utf8.h.
// ---------------------------------------------------------------------------

static nb::object impl_initcap(nb::object in_obj) {
    const DrakenVector* dv = draken_vector_unwrap(in_obj.ptr());
    if (!dv) throw nb::python_error();

    // VARBINARY takes the same ASCII-only byte fold as VARCHAR — see
    // impl_lowercase for the full rationale.
    const bool is_nvarchar = (dv->type == DRAKEN_NVARCHAR);
    if (dv->type != DRAKEN_VARCHAR && dv->type != DRAKEN_NVARCHAR && dv->type != DRAKEN_VARBINARY)
        throw nb::type_error(
            "vector_initcap: expected VARCHAR, NVARCHAR, or VARBINARY Vector");

    // GIL-free compute — see impl_lowercase for the full rationale.
    nb::gil_scoped_release _rel;

    const uint32_t n  = dv->length;
    const DrakenStringArena* sa = static_cast<const DrakenStringArena*>(dv->data);
    const DrakenType out_type = is_nvarchar ? DRAKEN_NVARCHAR : dv->type;

    const size_t slots_sz = (n > 0u ? n : 1u) * sizeof(DrakenStringSlot);
    DrakenStringSlot* out_slots = static_cast<DrakenStringSlot*>(draken_malloc(slots_sz));
    if (!out_slots) throw std::bad_alloc();
    std::memset(out_slots, 0, slots_sz);

    size_t arena_cap  = (sa->arena_used > 0u ? sa->arena_used : 64u);
    uint8_t* out_arena = static_cast<uint8_t*>(draken_malloc(arena_cap));
    if (!out_arena) { draken_free(out_slots); throw std::bad_alloc(); }
    size_t arena_used  = 0u;

    uint8_t* validity = nullptr;
    uint8_t* tmp_buf  = nullptr;
    size_t   tmp_cap  = 0u;

    struct Guard {
        DrakenStringSlot** sp; uint8_t** ap; uint8_t** vp; uint8_t** tp;
        void release() { sp = nullptr; ap = nullptr; vp = nullptr; tp = nullptr; }
        ~Guard() {
            if (sp) draken_free(*sp); if (ap) draken_free(*ap);
            if (vp && *vp) draken_free(*vp); if (tp && *tp) draken_free(*tp);
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

        if (slen == 0u) { str_init_inline(&out_slots[i], nullptr, 0u); continue; }

        const size_t need = static_cast<size_t>(slen) + (is_nvarchar ? 1u : 0u);
        if (need > tmp_cap) {
            auto* new_tmp = static_cast<uint8_t*>(draken_malloc(need));
            if (!new_tmp) throw std::bad_alloc();
            draken_free(tmp_buf); tmp_buf = new_tmp; tmp_cap = need;
        }

        if (!is_nvarchar) {
            // VARCHAR: ASCII-only initcap.
            bool new_word = true;
            for (uint32_t j = 0u; j < slen; ++j) {
                const uint8_t c = sdata[j];
                if ((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
                    (c >= '0' && c <= '9')) {
                    if (new_word && c >= 'a' && c <= 'z') {
                        tmp_buf[j] = static_cast<uint8_t>(c - 32u);
                    } else if (!new_word && c >= 'A' && c <= 'Z') {
                        tmp_buf[j] = static_cast<uint8_t>(c + 32u);
                    } else {
                        tmp_buf[j] = c;
                    }
                    new_word = false;
                } else {
                    tmp_buf[j] = c;
                    new_word = true;
                }
            }
        } else {
            // NVARCHAR: null-terminate and use utf8.h codepoint iteration.
            std::memcpy(tmp_buf, sdata, slen);
            tmp_buf[slen] = '\0';
            utf8_int8_t* p = reinterpret_cast<utf8_int8_t*>(tmp_buf);
            bool new_word = true;
            while (*p) {
                utf8_int32_t cp;
                utf8_int8_t* next = utf8codepoint(p, &cp);
                const utf8_int32_t lo = utf8lwrcodepoint(cp);
                const utf8_int32_t up = utf8uprcodepoint(cp);
                // Only apply case change if lo and up have the same byte width.
                if (new_word && cp == lo && up != cp) {
                    // Start of word and char is lowercase — uppercase it.
                    // Only safe when codepoint size is unchanged.
                    if (utf8codepointsize(up) == utf8codepointsize(cp)) {
                        utf8catcodepoint(p, up, utf8codepointsize(up));
                    }
                    new_word = false;
                } else if (!new_word && cp == up && lo != cp) {
                    // Mid-word and char is uppercase — lowercase it.
                    if (utf8codepointsize(lo) == utf8codepointsize(cp)) {
                        utf8catcodepoint(p, lo, utf8codepointsize(lo));
                    }
                    new_word = false;
                } else {
                    // Separator: anything that has no case distinction.
                    if (lo == up) new_word = true;
                }
                p = next;
            }
        }

        const uint32_t out_len = slen;

        if (out_len <= STR_INLINE_MAX) {
            draken_build_string_slot(&out_slots[i], tmp_buf, out_len, 0u);
        } else {
            if (arena_used + out_len > arena_cap) {
                size_t new_cap = arena_cap * 2u;
                if (new_cap < arena_used + out_len) new_cap = arena_used + out_len;
                auto* new_arena = static_cast<uint8_t*>(draken_malloc(new_cap));
                if (!new_arena) throw std::bad_alloc();
                if (arena_used > 0u) std::memcpy(new_arena, out_arena, arena_used);
                draken_free(out_arena); out_arena = new_arena; arena_cap = new_cap;
            }
            const uint32_t off = static_cast<uint32_t>(arena_used);
            std::memcpy(out_arena + off, tmp_buf, out_len);
            draken_build_string_slot(&out_slots[i], tmp_buf, out_len, off);
            arena_used += out_len;
        }
    }

    draken_free(tmp_buf); tmp_buf = nullptr;
    if (!any_null && validity) { draken_free(validity); validity = nullptr; }

    nb::gil_scoped_acquire _acq;  // re-take the GIL to publish the result to Python
    g.release();
    PyObject* out = draken_vector_own_string(
        out_slots, out_arena, arena_used, validity, n, out_type);
    if (!out) throw nb::python_error();
    return nb::steal<nb::object>(out);
}

// ---------------------------------------------------------------------------
// impl_reverse — reverse each string.
// VARCHAR: byte-reversal (correct since VARCHAR is ASCII).
// NVARCHAR: UTF-8 codepoint-aware reversal.
// VARBINARY: byte-reversal.
// ---------------------------------------------------------------------------

// Reverse UTF-8 codepoints in [src, src+src_len) into dst.
// dst must have capacity >= src_len.  Returns number of bytes written (== src_len).
static uint32_t reverse_utf8(const uint8_t* src, uint32_t src_len, uint8_t* dst) {
    // Collect codepoint boundaries (start offsets) in forward order.
    uint32_t offsets[4096];  // stack; strings > 4096 codepoints fall back to heap path
    uint32_t n_cp = 0u;
    uint32_t pos  = 0u;
    while (pos < src_len) {
        offsets[n_cp++] = pos;
        const uint8_t b = src[pos];
        if      (b < 0x80u)               pos += 1u;
        else if ((b & 0xE0u) == 0xC0u)   pos += 2u;
        else if ((b & 0xF0u) == 0xE0u)   pos += 3u;
        else if ((b & 0xF8u) == 0xF0u)   pos += 4u;
        else                              pos += 1u;  // invalid byte: treat as 1
        if (pos > src_len) pos = src_len;
        if (n_cp >= 4096u) break;
    }
    // Write codepoints in reverse order.
    uint32_t out_pos = 0u;
    for (uint32_t k = n_cp; k > 0u; --k) {
        const uint32_t cp_start = offsets[k - 1u];
        const uint32_t cp_end   = (k < n_cp) ? offsets[k] : src_len;
        const uint32_t cp_len   = cp_end - cp_start;
        std::memcpy(dst + out_pos, src + cp_start, cp_len);
        out_pos += cp_len;
    }
    return out_pos;
}

static nb::object impl_reverse(nb::object in_obj) {
    const DrakenVector* dv = draken_vector_unwrap(in_obj.ptr());
    if (!dv) throw nb::python_error();

    const bool is_string =
        dv->type == DRAKEN_VARCHAR || dv->type == DRAKEN_NVARCHAR ||
        dv->type == DRAKEN_VARBINARY;
    if (!is_string)
        throw nb::type_error(
            "vector_reverse: expected VARCHAR, NVARCHAR, or VARBINARY Vector");

    // GIL-free compute — see impl_lowercase for the full rationale.
    nb::gil_scoped_release _rel;

    const bool nvarchar = (dv->type == DRAKEN_NVARCHAR);
    const uint32_t n    = dv->length;
    const DrakenStringArena* sa = static_cast<const DrakenStringArena*>(dv->data);

    const size_t slots_sz = (n > 0u ? n : 1u) * sizeof(DrakenStringSlot);
    DrakenStringSlot* out_slots = static_cast<DrakenStringSlot*>(draken_malloc(slots_sz));
    if (!out_slots) throw std::bad_alloc();
    std::memset(out_slots, 0, slots_sz);

    size_t arena_cap  = (sa->arena_used > 0u ? sa->arena_used : 64u);
    uint8_t* out_arena = static_cast<uint8_t*>(draken_malloc(arena_cap));
    if (!out_arena) { draken_free(out_slots); throw std::bad_alloc(); }
    size_t arena_used  = 0u;

    uint8_t* validity = nullptr;
    uint8_t* tmp_buf  = nullptr;
    size_t   tmp_cap  = 0u;

    struct Guard {
        DrakenStringSlot** sp; uint8_t** ap; uint8_t** vp; uint8_t** tp;
        void release() { sp = nullptr; ap = nullptr; vp = nullptr; tp = nullptr; }
        ~Guard() {
            if (sp) draken_free(*sp); if (ap) draken_free(*ap);
            if (vp && *vp) draken_free(*vp); if (tp && *tp) draken_free(*tp);
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

        if (slen == 0u) { str_init_inline(&out_slots[i], nullptr, 0u); continue; }

        if (slen > tmp_cap) {
            auto* new_tmp = static_cast<uint8_t*>(draken_malloc(slen));
            if (!new_tmp) throw std::bad_alloc();
            draken_free(tmp_buf); tmp_buf = new_tmp; tmp_cap = slen;
        }

        uint32_t out_len;
        if (!nvarchar) {
            // VARCHAR/VARBINARY: byte reversal.
            for (uint32_t j = 0u; j < slen; ++j)
                tmp_buf[j] = sdata[slen - 1u - j];
            out_len = slen;
        } else {
            out_len = reverse_utf8(sdata, slen, tmp_buf);
        }

        if (out_len <= STR_INLINE_MAX) {
            draken_build_string_slot(&out_slots[i], tmp_buf, out_len, 0u);
        } else {
            if (arena_used + out_len > arena_cap) {
                size_t new_cap = arena_cap * 2u;
                if (new_cap < arena_used + out_len) new_cap = arena_used + out_len;
                auto* new_arena = static_cast<uint8_t*>(draken_malloc(new_cap));
                if (!new_arena) throw std::bad_alloc();
                if (arena_used > 0u) std::memcpy(new_arena, out_arena, arena_used);
                draken_free(out_arena); out_arena = new_arena; arena_cap = new_cap;
            }
            const uint32_t off = static_cast<uint32_t>(arena_used);
            std::memcpy(out_arena + off, tmp_buf, out_len);
            draken_build_string_slot(&out_slots[i], tmp_buf, out_len, off);
            arena_used += out_len;
        }
    }

    draken_free(tmp_buf); tmp_buf = nullptr;
    if (!any_null && validity) { draken_free(validity); validity = nullptr; }

    nb::gil_scoped_acquire _acq;  // re-take the GIL to publish the result to Python
    g.release();
    PyObject* out = draken_vector_own_string(
        out_slots, out_arena, arena_used, validity, n, dv->type);
    if (!out) throw nb::python_error();
    return nb::steal<nb::object>(out);
}

// ---------------------------------------------------------------------------
// impl_trim_common — TRIM / LTRIM / RTRIM (ASCII whitespace, bytes 0x09-0x0D + 0x20)
// ---------------------------------------------------------------------------

static inline bool is_ascii_whitespace(uint8_t c) {
    return c == 0x20u || (c >= 0x09u && c <= 0x0Du);
}

static nb::object impl_trim_common(nb::object in_obj, bool trim_left, bool trim_right) {
    const DrakenVector* dv = draken_vector_unwrap(in_obj.ptr());
    if (!dv) throw nb::python_error();

    // ASCII-whitespace trim is byte-safe for VARBINARY too — no NVARCHAR-only
    // logic here (the byte scan below doesn't distinguish string subtype).
    if (dv->type != DRAKEN_VARCHAR && dv->type != DRAKEN_NVARCHAR && dv->type != DRAKEN_VARBINARY)
        throw nb::type_error("vector_trim: expected VARCHAR, NVARCHAR, or VARBINARY Vector");

    // GIL-free compute — see impl_lowercase for the full rationale.
    nb::gil_scoped_release _rel;

    const uint32_t n = dv->length;
    const DrakenStringArena* sa = static_cast<const DrakenStringArena*>(dv->data);
    const DrakenType out_type = dv->type;

    const size_t slots_sz = (n > 0u ? n : 1u) * sizeof(DrakenStringSlot);
    DrakenStringSlot* out_slots = static_cast<DrakenStringSlot*>(draken_malloc(slots_sz));
    if (!out_slots) throw std::bad_alloc();
    std::memset(out_slots, 0, slots_sz);

    size_t arena_cap = (sa->arena_used > 0u ? sa->arena_used : 64u);
    uint8_t* out_arena = static_cast<uint8_t*>(draken_malloc(arena_cap));
    if (!out_arena) { draken_free(out_slots); throw std::bad_alloc(); }
    size_t arena_used = 0u;

    uint8_t* validity = nullptr;
    bool any_null = false;

    struct Guard {
        DrakenStringSlot** sp;
        uint8_t**          ap;
        uint8_t**          vp;
        void release() { sp = nullptr; ap = nullptr; vp = nullptr; }
        ~Guard() {
            if (sp) draken_free(*sp);
            if (ap) draken_free(*ap);
            if (vp && *vp) draken_free(*vp);
        }
    } g{&out_slots, &out_arena, &validity};

    for (uint32_t i = 0u; i < n; ++i) {
        if (!is_valid_at(dv, i)) {
            if (!validity) { validity = alloc_validity_all_valid(n); }
            validity[i >> 3] &= ~static_cast<uint8_t>(1u << (i & 7u));
            str_init_null(&out_slots[i]);
            any_null = true;
            continue;
        }

        const DrakenStringSlot* slot = &sa->slots[dv->selection[i]];
        const uint32_t slen = str_length(slot);
        const uint8_t* sdata = str_data(slot, sa->arena);

        if (slen == 0u) {
            str_init_inline(&out_slots[i], nullptr, 0u);
            continue;
        }

        uint32_t start = 0u;
        uint32_t end   = slen;

        if (trim_left) {
            while (start < end && is_ascii_whitespace(sdata[start])) ++start;
        }
        if (trim_right) {
            while (end > start && is_ascii_whitespace(sdata[end - 1u])) --end;
        }

        const uint8_t* tdata = sdata + start;
        const uint32_t tlen  = end - start;

        if (tlen <= STR_INLINE_MAX) {
            str_init_inline(&out_slots[i], tdata, tlen);
        } else {
            if (arena_used + tlen > arena_cap) {
                size_t new_cap = arena_cap * 2u;
                if (new_cap < arena_used + tlen) new_cap = arena_used + tlen;
                auto* new_arena = static_cast<uint8_t*>(draken_malloc(new_cap));
                if (!new_arena) throw std::bad_alloc();
                if (arena_used > 0u) std::memcpy(new_arena, out_arena, arena_used);
                draken_free(out_arena);
                out_arena = new_arena;
                arena_cap = new_cap;
            }
            const uint32_t off = static_cast<uint32_t>(arena_used);
            std::memcpy(out_arena + off, tdata, tlen);
            draken_build_string_slot(&out_slots[i], tdata, tlen, off);
            arena_used += tlen;
        }
    }

    if (!any_null && validity) { draken_free(validity); validity = nullptr; }

    nb::gil_scoped_acquire _acq;  // re-take the GIL to publish the result to Python
    g.release();
    PyObject* out = draken_vector_own_string(
        out_slots, out_arena, arena_used, validity, n, out_type);
    if (!out) throw nb::python_error();
    return nb::steal<nb::object>(out);
}

// ---------------------------------------------------------------------------
// NB_MODULE
// ---------------------------------------------------------------------------

void register_vector_string_case(nb::module_ &m) {
    m.def("vector_lowercase",
        [](nb::object input) -> nb::object {
            return impl_lowercase(input);
        },
        nb::arg("input"),
        "Lower-case a string column.  VARCHAR: ASCII fold.  NVARCHAR: utf8lwr.\n"
        "Null TVL: null input row → null output row.");

    m.def("vector_uppercase",
        [](nb::object input) -> nb::object {
            return impl_uppercase(input);
        },
        nb::arg("input"),
        "Upper-case a string column.  VARCHAR: ASCII fold.  NVARCHAR: utf8upr.\n"
        "Null TVL: null input row → null output row.");

    m.def("vector_initcap",
        [](nb::object input) -> nb::object {
            return impl_initcap(input);
        },
        nb::arg("input"),
        "Title-case a string column (first letter of each word upper, rest lower).\n"
        "VARCHAR: ASCII-only.  NVARCHAR: codepoint-aware.\n"
        "Null TVL: null input row → null output row.");

    m.def("vector_reverse",
        [](nb::object input) -> nb::object {
            return impl_reverse(input);
        },
        nb::arg("input"),
        "Reverse each string.  VARCHAR/VARBINARY: byte-reversal.\n"
        "NVARCHAR: UTF-8 codepoint-aware reversal.\n"
        "Null TVL: null input row → null output row.");

    m.def("vector_trim",
        [](nb::object input) -> nb::object {
            return impl_trim_common(input, true, true);
        },
        nb::arg("input"),
        "Remove leading and trailing ASCII whitespace from each string.\n"
        "Null TVL: null input row → null output row.");

    m.def("vector_ltrim",
        [](nb::object input) -> nb::object {
            return impl_trim_common(input, true, false);
        },
        nb::arg("input"),
        "Remove leading ASCII whitespace from each string.\n"
        "Null TVL: null input row → null output row.");

    m.def("vector_rtrim",
        [](nb::object input) -> nb::object {
            return impl_trim_common(input, false, true);
        },
        nb::arg("input"),
        "Remove trailing ASCII whitespace from each string.\n"
        "Null TVL: null input row → null output row.");
}
