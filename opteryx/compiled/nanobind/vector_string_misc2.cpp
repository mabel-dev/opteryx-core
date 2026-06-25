// opteryx/compiled/nanobind/vector_string_misc2.cpp — Milestone E.16 / E.26+, C′.
//
// C′ pattern: pure nanobind C++, zero Cython.  One NB_MODULE, four functions:
//
//   vector_replace(haystack, needle, replacement) — bytewise string replace (VARCHAR).
//   vector_regex_replace(data, pattern, replacement) — RE2 regex replace.
//   vector_cosine_similarity(a, b)               — fp16 cosine similarity (FLOAT64).
//   vector_cosine_distance(a, b)                 — 1 - clip(similarity, -1, 1) (FLOAT64).
//
// Null TVL:
//   vector_replace:            any null in (haystack, needle, replacement) → null output.
//   vector_regex_replace:      null input row → null output row.
//   vector_cosine_similarity:  null in either input row → null output row.
//   vector_cosine_distance:    inherits similarity null TVL.
//
// vector_replace notes:
//   Bytewise replace — works correctly for VARCHAR and VARBINARY, and for valid
//   UTF-8 NVARCHAR when needle/replacement are themselves valid UTF-8.
//   Empty needle: no-op (returns haystack unchanged; PostgreSQL convention).
//
// vector_regex_replace notes:
//   RE2-based regex replace (all non-overlapping occurrences).
//   pattern and replacement are Python bytes.
//   RE2 patterns are compiled once and cached at module level (process lifetime).
//
// vector_cosine notes:
//   Both inputs must be DRAKEN_VECTOR_FP16 with a mandatory logical_type_dimension.
//   fp16 values are widened to float64 for accumulation.
//   Zero-norm row → NaN (IEEE).  Dimension mismatch → ValueError.
//
// Replaces:
//   opteryx/compiled/vector_ops/vector_replace.pyx
//   opteryx/compiled/vector_ops/vector_regex_replace.pyx
//   opteryx/compiled/vector_ops/vector_cosine.pyx

#include <Python.h>
#include <nanobind/nanobind.h>
#include <cstdint>
#include <cstring>
#include <cmath>
#include <limits>
#include <stdexcept>
#include <string>
#include <vector>
#include <cstdlib>

#include "core/buffers.h"
#include "core/alloc.h"
#include "core/string_slot.h"
#include "core/vector_alloc.h"
#include "core/draken_bridge.h"
#include "ops/vector_cosine.h"

#include "re2/re2.h"
#include "re2/stringpiece.h"

#include <unordered_map>
#include <mutex>

namespace nb = nanobind;

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

static inline bool is_valid_at(const DrakenVector* dv, uint32_t i) noexcept {
    if (!dv->validity) return true;
    return ((dv->validity[i >> 3] >> (i & 7u)) & 1u) != 0u;
}

// Unwrap a string-family Vector.  Accepts VARCHAR, NVARCHAR, VARBINARY,
// DICTIONARY, CONSTANT.  Raises TypeError on non-Vector or non-string type.
static const DrakenVector* unwrap_str(nb::object obj, const char* fn) {
    const DrakenVector* dv = draken_vector_unwrap(obj.ptr());
    if (!dv) throw nb::python_error();
    const bool is_str =
        dv->type == DRAKEN_VARCHAR  ||
        dv->type == DRAKEN_NVARCHAR ||
        dv->type == DRAKEN_VARBINARY;
    if (!is_str)
        throw nb::type_error(
            (std::string(fn) + ": expected a string-family Vector "
             "(VARCHAR, NVARCHAR, or VARBINARY)").c_str());
    return dv;
}

// Unwrap an fp16 Vector.  Raises TypeError on non-Vector or non-fp16 type.
static const DrakenVector* unwrap_fp16(nb::object obj, const char* fn) {
    const DrakenVector* dv = draken_vector_unwrap(obj.ptr());
    if (!dv) throw nb::python_error();
    if (dv->type != DRAKEN_VECTOR_FP16)
        throw nb::type_error(
            (std::string(fn) + ": expected a VECTOR_FP16 Vector").c_str());
    return dv;
}

// Read logical_type_dimension from the Python attribute.  Raises on missing or invalid.
static uint32_t get_fp16_dimension(nb::object obj, const char* fn) {
    PyObject* raw = PyObject_GetAttrString(obj.ptr(), "logical_type_dimension");
    if (!raw) throw nb::python_error();
    nb::object dim_obj = nb::steal<nb::object>(raw);
    if (dim_obj.is_none())
        throw nb::type_error(
            (std::string(fn) +
             ": VECTOR_FP16 is missing mandatory logical_type_dimension descriptor").c_str());
    const long dim = PyLong_AsLong(dim_obj.ptr());
    if (dim == -1L && PyErr_Occurred()) throw nb::python_error();
    if (dim <= 0L)
        throw std::invalid_argument(
            std::string(fn) + ": logical_type_dimension must be >= 1");
    return static_cast<uint32_t>(dim);
}

// Lazily allocate a validity bitmap initialised all-valid; clear bit i.
// On first call, allocates the bitmap.  Guards against OOM.
// Note: caller must hold onto guard_slots in case of exception (so that
// draken_free(guard_slots) is reachable — handled in each impl function).
static void mark_null(uint8_t*& out_null, bool& any_null,
                      uint32_t i, uint32_t n, void* guard) {
    if (!any_null) {
        const uint32_t bm     = (n + 7u) >> 3;
        const uint32_t padded = (bm + 7u) & ~7u;
        out_null = static_cast<uint8_t*>(draken_malloc(padded > 0u ? padded : 8u));
        if (!out_null) {
            draken_free(guard);
            throw std::bad_alloc();
        }
        std::memset(out_null, 0xFF, padded > 0u ? padded : 8u);
        any_null = true;
    }
    out_null[i >> 3] &= ~static_cast<uint8_t>(1u << (i & 7u));
}

// ---------------------------------------------------------------------------
// vector_replace — bytewise string replace, null TVL
// ---------------------------------------------------------------------------
//
// SQL REPLACE(haystack, needle, replacement): replaces all non-overlapping
// occurrences of needle in haystack with replacement.  Bytewise scan (KMP/BMH
// not warranted — most strings are short).
//
// Empty needle: no-op (returns haystack unchanged).  PostgreSQL convention.

static nb::object impl_replace(
    nb::object hay_obj, nb::object ndl_obj, nb::object rep_obj)
{
    const DrakenVector* hay = unwrap_str(hay_obj, "vector_replace");
    const DrakenVector* ndl = unwrap_str(ndl_obj, "vector_replace");
    const DrakenVector* rep = unwrap_str(rep_obj, "vector_replace");

    const uint32_t n = hay->length;
    if (ndl->length != n || rep->length != n)
        throw std::invalid_argument(
            "vector_replace: haystack, needle, and replacement must have the same length");

    const DrakenStringArena* sh = static_cast<const DrakenStringArena*>(hay->data);
    const DrakenStringArena* sn = static_cast<const DrakenStringArena*>(ndl->data);
    const DrakenStringArena* sr = static_cast<const DrakenStringArena*>(rep->data);

    // Allocate slot array (at least 1 to keep pointer non-null).
    const size_t slots_sz = (n > 0u ? n : 1u) * sizeof(DrakenStringSlot);
    DrakenStringSlot* slots = static_cast<DrakenStringSlot*>(draken_malloc(slots_sz));
    if (!slots) throw std::bad_alloc();
    std::memset(slots, 0, slots_sz);

    uint8_t* out_null = nullptr;
    bool any_null = false;

    // Dynamic arena accumulator: std::vector for growth, then copied to draken buffer.
    std::vector<uint8_t> arena_acc;
    arena_acc.reserve(n * 8u);

    // Per-row scratch output buffer.
    std::string scratch;

    for (uint32_t i = 0u; i < n; ++i) {
        // Null TVL: any null among the three inputs → null output row.
        if (!is_valid_at(hay, i) || !is_valid_at(ndl, i) || !is_valid_at(rep, i)) {
            mark_null(out_null, any_null, i, n, slots);
            // slots[i] already zeroed (null canonical form).
            continue;
        }

        const DrakenStringSlot* hs = &sh->slots[hay->selection[i]];
        const DrakenStringSlot* ns = &sn->slots[ndl->selection[i]];
        const DrakenStringSlot* rs = &sr->slots[rep->selection[i]];

        const uint8_t* hay_data = str_data(hs, sh->arena);
        const uint8_t* ndl_data = str_data(ns, sn->arena);
        const uint8_t* rep_data = str_data(rs, sr->arena);
        const uint32_t hay_len  = str_length(hs);
        const uint32_t ndl_len  = str_length(ns);
        const uint32_t rep_len  = str_length(rs);

        scratch.clear();

        if (ndl_len == 0u) {
            // Empty needle: return haystack unchanged (no-op, PostgreSQL convention).
            scratch.assign(reinterpret_cast<const char*>(hay_data), hay_len);
        } else {
            // Scan left-to-right replacing all non-overlapping occurrences.
            uint32_t pos = 0u;
            while (pos + ndl_len <= hay_len) {
                if (std::memcmp(hay_data + pos, ndl_data, ndl_len) == 0) {
                    scratch.append(reinterpret_cast<const char*>(rep_data), rep_len);
                    pos += ndl_len;
                } else {
                    scratch += static_cast<char>(hay_data[pos]);
                    ++pos;
                }
            }
            // Tail bytes after the last scan position.
            scratch.append(
                reinterpret_cast<const char*>(hay_data + pos),
                static_cast<size_t>(hay_len - pos));
        }

        const uint32_t out_len = static_cast<uint32_t>(scratch.size());
        const uint8_t* out_bytes = reinterpret_cast<const uint8_t*>(scratch.data());

        if (out_len <= STR_INLINE_MAX) {
            // Inline slot — no arena byte written.
            draken_build_string_slot(&slots[i], out_bytes, out_len, 0u);
        } else {
            // Long-form: record arena offset, append bytes.
            const uint32_t off = static_cast<uint32_t>(arena_acc.size());
            arena_acc.insert(arena_acc.end(), out_bytes, out_bytes + out_len);
            draken_build_string_slot(&slots[i], out_bytes, out_len, off);
        }
    }

    // Clear validity tail bits beyond last complete byte.
    if (any_null && (n & 7u)) {
        const uint32_t bm = (n + 7u) >> 3;
        out_null[bm - 1u] &= static_cast<uint8_t>((1u << (n & 7u)) - 1u);
    }

    // Copy arena accumulator to a draken_malloc'd buffer.
    const size_t arena_len = arena_acc.size();
    uint8_t* arena = nullptr;
    if (arena_len > 0u) {
        arena = static_cast<uint8_t*>(draken_malloc(arena_len));
        if (!arena) {
            draken_free(slots);
            if (out_null) draken_free(out_null);
            throw std::bad_alloc();
        }
        std::memcpy(arena, arena_acc.data(), arena_len);
    }

    PyObject* out = draken_vector_own_string(
        slots, arena, arena_len, out_null, n, DRAKEN_VARCHAR);
    if (!out) throw nb::python_error();
    return nb::steal<nb::object>(out);
}

// ---------------------------------------------------------------------------
// vector_cosine_similarity — fp16 row-wise cosine similarity → FLOAT64
// ---------------------------------------------------------------------------
//
// Both inputs must be DRAKEN_VECTOR_FP16 with matching logical_type_dimension.
// Dimension is read from the Python attribute on `a`; `b` must have the same.
// Raises ValueError if dimensions differ.

static nb::object impl_cosine_similarity(nb::object a_obj, nb::object b_obj) {
    const DrakenVector* a = unwrap_fp16(a_obj, "vector_cosine_similarity");
    const DrakenVector* b = unwrap_fp16(b_obj, "vector_cosine_similarity");

    const uint32_t dim_a = get_fp16_dimension(a_obj, "vector_cosine_similarity");
    const uint32_t dim_b = get_fp16_dimension(b_obj, "vector_cosine_similarity");
    if (dim_a != dim_b)
        throw std::invalid_argument(
            "vector_cosine_similarity: both inputs must have the same logical_type_dimension");

    VecResult r = draken::ops::cosine_sim_fp16(*a, *b, dim_a);

    PyObject* out = draken_vector_own_raw(r.data, r.validity, r.length, DRAKEN_FLOAT64);
    if (!out) throw nb::python_error();
    return nb::steal<nb::object>(out);
}

// ---------------------------------------------------------------------------
// vector_cosine_distance — 1 - clip(similarity, -1, 1) → FLOAT64
// ---------------------------------------------------------------------------

static nb::object impl_cosine_distance(nb::object a_obj, nb::object b_obj) {
    const DrakenVector* a = unwrap_fp16(a_obj, "vector_cosine_distance");
    const DrakenVector* b = unwrap_fp16(b_obj, "vector_cosine_distance");

    const uint32_t dim_a = get_fp16_dimension(a_obj, "vector_cosine_distance");
    const uint32_t dim_b = get_fp16_dimension(b_obj, "vector_cosine_distance");
    if (dim_a != dim_b)
        throw std::invalid_argument(
            "vector_cosine_distance: both inputs must have the same logical_type_dimension");

    VecResult r = draken::ops::cosine_sim_fp16(*a, *b, dim_a);

    // Apply distance transform: d = 1 - clip(sim, -1, 1).
    // NaN rows remain NaN (null-like; validity bit controls whether row is null).
    double* dst = static_cast<double*>(r.data);
    const uint32_t n = r.length;
    for (uint32_t i = 0u; i < n; ++i) {
        if (r.validity && !((r.validity[i >> 3] >> (i & 7u)) & 1u))
            continue;  // null row — leave as-is
        const double s = dst[i];
        if (s != s) continue;  // NaN — leave as-is (zero-norm row)
        double clipped = s < -1.0 ? -1.0 : (s > 1.0 ? 1.0 : s);
        dst[i] = 1.0 - clipped;
    }

    PyObject* out = draken_vector_own_raw(r.data, r.validity, r.length, DRAKEN_FLOAT64);
    if (!out) throw nb::python_error();
    return nb::steal<nb::object>(out);
}

// ---------------------------------------------------------------------------
// vector_regex_replace — RE2 regex replace, all non-overlapping occurrences.
// Pattern and replacement are Python bytes. Pattern compiled once, cached.
// ---------------------------------------------------------------------------

static std::unordered_map<std::string, RE2*> s_re2_cache;
static std::mutex s_re2_mutex;

static RE2* get_or_compile_re2(const char* pat_data, size_t pat_len) {
    std::string key(pat_data, pat_len);
    std::lock_guard<std::mutex> lk(s_re2_mutex);
    auto it = s_re2_cache.find(key);
    if (it != s_re2_cache.end()) return it->second;
    RE2* re = new RE2(key);
    if (!re->ok()) {
        std::string err = re->error();
        delete re;
        throw std::invalid_argument("vector_regex_replace: invalid RE2 pattern: " + err);
    }
    s_re2_cache.emplace(std::move(key), re);
    return re;
}

static nb::object impl_regex_replace(
    nb::object data_obj, nb::object pattern_obj, nb::object repl_obj)
{
    const DrakenVector* dv = draken_vector_unwrap(data_obj.ptr());
    if (!dv) throw nb::python_error();
    const bool is_str = dv->type == DRAKEN_VARCHAR || dv->type == DRAKEN_NVARCHAR ||
                        dv->type == DRAKEN_VARBINARY;
    if (!is_str)
        throw nb::type_error(
            "vector_regex_replace: expected a string-family Vector");

    if (!PyBytes_Check(pattern_obj.ptr()))
        throw nb::type_error("vector_regex_replace: pattern must be bytes");
    if (!PyBytes_Check(repl_obj.ptr()))
        throw nb::type_error("vector_regex_replace: replacement must be bytes");

    const char* pat_data = PyBytes_AS_STRING(pattern_obj.ptr());
    const size_t pat_len = static_cast<size_t>(PyBytes_GET_SIZE(pattern_obj.ptr()));
    const char* rep_data = PyBytes_AS_STRING(repl_obj.ptr());
    const size_t rep_len = static_cast<size_t>(PyBytes_GET_SIZE(repl_obj.ptr()));

    RE2* regex = get_or_compile_re2(pat_data, pat_len);
    const re2::StringPiece repl_piece(rep_data, rep_len);

    const DrakenStringArena* sa = static_cast<const DrakenStringArena*>(dv->data);
    const uint32_t n = dv->length;

    const size_t slots_sz = (n > 0u ? n : 1u) * sizeof(DrakenStringSlot);
    DrakenStringSlot* out_slots = static_cast<DrakenStringSlot*>(draken_malloc(slots_sz));
    if (!out_slots) throw std::bad_alloc();
    std::memset(out_slots, 0, slots_sz);

    size_t arena_cap  = (sa->arena_used > 0u ? sa->arena_used : 64u);
    uint8_t* out_arena = static_cast<uint8_t*>(draken_malloc(arena_cap));
    if (!out_arena) { draken_free(out_slots); throw std::bad_alloc(); }
    size_t arena_used  = 0u;

    uint8_t* validity = nullptr;
    bool any_null = false;

    // Per-row scratch string.
    std::string scratch;
    scratch.reserve(256u);

    for (uint32_t i = 0u; i < n; ++i) {
        if (!(dv->validity == nullptr ||
              ((dv->validity[i >> 3] >> (i & 7u)) & 1u))) {
            // Null row.
            if (!validity) {
                const uint32_t nb_ = (n + 7u) >> 3;
                validity = static_cast<uint8_t*>(draken_malloc(nb_ > 0u ? nb_ : 1u));
                if (!validity) { draken_free(out_slots); draken_free(out_arena); throw std::bad_alloc(); }
                std::memset(validity, 0xFF, nb_ > 0u ? nb_ : 1u);
            }
            validity[i >> 3] &= ~static_cast<uint8_t>(1u << (i & 7u));
            str_init_null(&out_slots[i]);
            any_null = true;
            continue;
        }

        const DrakenStringSlot* slot = &sa->slots[dv->selection[i]];
        const uint32_t slen  = str_length(slot);
        const uint8_t* sdata = str_data(slot, sa->arena);

        scratch.assign(reinterpret_cast<const char*>(sdata), slen);
        RE2::GlobalReplace(&scratch, *regex, repl_piece);

        const uint32_t out_len = static_cast<uint32_t>(scratch.size());
        const uint8_t* out_bytes = reinterpret_cast<const uint8_t*>(scratch.data());

        if (out_len <= STR_INLINE_MAX) {
            draken_build_string_slot(&out_slots[i], out_bytes, out_len, 0u);
        } else {
            if (arena_used + out_len > arena_cap) {
                size_t nc = arena_cap * 2u;
                if (nc < arena_used + out_len) nc = arena_used + out_len;
                uint8_t* na = static_cast<uint8_t*>(draken_malloc(nc));
                if (!na) {
                    draken_free(out_slots); draken_free(out_arena);
                    if (validity) draken_free(validity);
                    throw std::bad_alloc();
                }
                if (arena_used > 0u) std::memcpy(na, out_arena, arena_used);
                draken_free(out_arena); out_arena = na; arena_cap = nc;
            }
            const uint32_t off = static_cast<uint32_t>(arena_used);
            std::memcpy(out_arena + off, out_bytes, out_len);
            draken_build_string_slot(&out_slots[i], out_bytes, out_len, off);
            arena_used += out_len;
        }
    }

    if (!any_null && validity) { draken_free(validity); validity = nullptr; }

    // Debug validation: when OPTERYX_DEBUG_VALIDATE_SLOTS is set, verify that
    // every extern slot's arena_offset + length fits within the produced arena.
    // This is strictly diagnostic and only enabled by environment to avoid
    // impacting normal runtime performance.
    const char* dbg_env = std::getenv("OPTERYX_DEBUG_VALIDATE_SLOTS");
    if (dbg_env) {
        for (uint32_t idx = 0u; idx < n; ++idx) {
            const DrakenStringSlot* s = &out_slots[idx];
            const uint32_t len = str_length(s);
            if (!str_is_inline(s)) {
                const uint32_t off = s->ext.arena_offset;
                if ((size_t)off + (size_t)len > arena_used) {
                    // Clean up allocated buffers before throwing so Python traceback
                    // is visible and ownership isn't leaked.
                    if (validity) draken_free(validity);
                    draken_free(out_arena);
                    draken_free(out_slots);
                    throw std::runtime_error("vector_regex_replace: slot references beyond arena bounds");
                }
            }
        }
    }

    PyObject* out = draken_vector_own_string(
        out_slots, out_arena, arena_used, validity, n, dv->type);
    if (!out) throw nb::python_error();
    return nb::steal<nb::object>(out);
}

// ---------------------------------------------------------------------------
// NB_MODULE
// ---------------------------------------------------------------------------

void register_vector_string_misc2(nb::module_ &m) {

    m.def("vector_replace",
        [](nb::object hay, nb::object ndl, nb::object rep) -> nb::object {
            return impl_replace(hay, ndl, rep);
        },
        nb::arg("haystack"), nb::arg("needle"), nb::arg("replacement"),
        "Bytewise string replace: replace all occurrences of needle in haystack.\n"
        "Output: DRAKEN_VARCHAR. Null TVL: any null input row → null output.\n"
        "Empty needle: no-op (returns haystack unchanged; PostgreSQL convention).");

    m.def("vector_regex_replace",
        [](nb::object data, nb::object pattern, nb::object replacement) -> nb::object {
            return impl_regex_replace(data, pattern, replacement);
        },
        nb::arg("data"), nb::arg("pattern"), nb::arg("replacement"),
        "RE2 regex replace: replace all non-overlapping occurrences of pattern.\n"
        "pattern and replacement must be Python bytes.  Pattern cached at module level.\n"
        "Null TVL: null input row → null output row.");

    m.def("vector_cosine_similarity",
        [](nb::object a, nb::object b) -> nb::object {
            return impl_cosine_similarity(a, b);
        },
        nb::arg("a"), nb::arg("b"),
        "Row-wise cosine similarity for two DRAKEN_VECTOR_FP16 columns.\n"
        "Both inputs must share the same logical_type_dimension.\n"
        "Output: DRAKEN_FLOAT64. Null TVL: null in either row → null output.\n"
        "Zero-norm row → NaN. Dimension mismatch → ValueError.");

    m.def("vector_cosine_distance",
        [](nb::object a, nb::object b) -> nb::object {
            return impl_cosine_distance(a, b);
        },
        nb::arg("a"), nb::arg("b"),
        "Row-wise cosine distance = 1 - clip(cosine_similarity, -1, 1).\n"
        "Both inputs must be DRAKEN_VECTOR_FP16 with matching dimension.\n"
        "Output: DRAKEN_FLOAT64. Null TVL: null in either row → null output.\n"
        "Zero-norm row → NaN. Dimension mismatch → ValueError.");
}
