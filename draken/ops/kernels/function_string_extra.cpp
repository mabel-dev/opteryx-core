// draken/ops/kernels/function_string_extra.cpp — Phase 9a-fn: the fixed-result
// (INT64-producing) string kernels on the C ABI. Signature is the design's
// func_fn_t:
//
//     VecResult fn(void* ctx, const DrakenVector* const* args, uint32_t nargs)
//
// Dispatched DIRECTLY from the nogil DV* VM (compiled_expression.pyx probes the
// registry for draken_{function_name.lower()} at bind time) — no Python, no
// nanobind, no GIL.
//
//   draken_octet_length(str)          — byte count, ALWAYS (never codepoints).
//   draken_position(sub, string)      — SQL POSITION(sub IN string), 1-based.
//   draken_levenshtein(str1, str2)    — bytewise edit distance.
//   draken_to_ascii(str)              — codepoint of the FIRST character.
//
// Two STRING-PRODUCING kernels also live here (dense, not INT64):
//   draken_to_char(codepoint)         — inverse of draken_to_ascii: an integer
//                                       Unicode codepoint → its UTF-8 encoding
//                                       (one character), VARCHAR. Empty for 0
//                                       (mirroring to_ascii's empty→0); a loud
//                                       error for any non-scalar value.
//   draken_random_string(n)           — VOLATILE: n random BYTES per row as
//                                       VARBINARY (architect ruling 2026-07-17),
//                                       where n is the operand's per-row value.
//
// DENSE OUTPUT, NOT SHAPE-PRESERVING. The INT64-producing four match the sibling
// draken_length's fixed-result fold contract (a dense INT64 block). The two
// string producers are dense per-LOGICAL-ROW like draken_date_format
// (function_temporal.cpp): they do NOT reduce over data_length physical uniques,
// because validating/generating per unique would touch physical slots that only
// NULL rows reference (an out-of-range codepoint hidden behind a null would raise
// wrongly; a random draw would be shared across the wrong rows). Every valid
// logical row is read on its own via the uniform data[selection[i]] access, so
// dense / constant / dict inputs all yield identical answers (CLAUDE.md §11).
//
// draken_octet_length stages per-UNIQUE work for compressed inputs the way
// draken_length does; the other three read a per-row operand pair (position,
// levenshtein) or are already O(1)-per-slot, so they stay on the plain uniform
// loop.
//
// Ported algorithms (CLAUDE.md §3 — the precedent set by string_replace_soundex.cpp,
// which ports Soundex byte-for-byte out of opteryx/compiled/nanobind):
//   draken_position    reproduces substring_position (memchr-anchored scan +
//                      Boyer-Moore-Horspool skip table) from
//                      opteryx/compiled/nanobind/vector_string_misc.cpp.
//   draken_levenshtein reproduces lev_myers (Myers 1999 / Hyyrö bit-parallel) and
//                      the two-row rolling DP fallback from the same file.
// NOTE: those nanobind copies are NOT deleted here — they are still reachable via
// the Python `vectors` module. This is duplication, and it is called out in the
// hand-back rather than resolved unilaterally.
//
// No C++ exception escapes into the nogil VM: allocations go through
// draken_malloc (returns nullptr, never throws) and every failure path returns an
// error sentinel.

#include <vector>
#include <cstdint>
#include <cstring>
#include <new>                     // std::bad_alloc (kernel_copy_validity)
#include <random>                  // std::random_device — per-thread RNG seeding

#include "pcg_random.hpp"          // vendored third_party/pcg — RANDOM_STRING
#include "core/buffers.h"
#include "core/string_slot.h"
#include "core/alloc.h"
#include "core/vector_alloc.h"     // draken_identity_sel
#include "ops/vec_result.h"
#include "ops/kernels/result_helpers.h"
#include "ops/kernels/error_handling.h"

namespace {

inline bool fse_is_string(DrakenType t) {
    return t == DRAKEN_VARCHAR || t == DRAKEN_NVARCHAR || t == DRAKEN_VARBINARY;
}

inline bool fse_row_valid(const DrakenVector* v, uint32_t row) {
    return v->validity == nullptr || ((v->validity[row >> 3] >> (row & 7u)) & 1u);
}

// Allocate an all-valid (0xFF) logical-row validity bitmap, padded to the 8-byte
// multiple every other draken producer uses. Returns nullptr on OOM.
uint8_t* fse_alloc_validity_all_valid(uint32_t n) {
    const uint32_t bm     = (n + 7u) >> 3;
    const uint32_t padded = (bm + 7u) & ~7u;
    const size_t   vbytes = padded > 0u ? padded : 8u;
    uint8_t* v = static_cast<uint8_t*>(draken_malloc(vbytes));
    if (v) std::memset(v, 0xFF, vbytes);
    return v;
}

// Finalize a dense INT64 result block.
VecResult fse_dense_int64(int64_t* out, uint8_t* validity, uint32_t n) {
    VecResult r{};
    r.data           = out;
    r.validity       = validity;
    r.selection      = draken_identity_sel(n);
    r.owns_selection = false;
    r.data_length    = n;
    r.length         = n;
    r.type           = DRAKEN_INT64;
    r.flags          = DRAKEN_SEL_IDENTITY | DRAKEN_SEL_PERMUTATION;
    return r;
}

// ---------------------------------------------------------------------------
// POSITION — ported from vector_string_misc.cpp::substring_position
// ---------------------------------------------------------------------------
//
// SQL-92 (E021-11): POSITION('foo' IN 'foobar') → 1; not found → 0; empty needle
// → 1. Bytewise: for NVARCHAR the result is a BYTE position, not a codepoint
// position (matching the nanobind kernel this is ported from — a codepoint-aware
// variant is deferred there too).
int64_t fse_substring_position(const uint8_t* hay, uint32_t hay_len,
                               const uint8_t* ndl, uint32_t ndl_len) {
    if (ndl_len == 0u) return 1;             // empty needle → position 1
    if (hay_len < ndl_len) return 0;

    // Single-byte fast path.
    if (ndl_len == 1u) {
        const void* p = std::memchr(hay, static_cast<int>(ndl[0]), hay_len);
        return p ? static_cast<int64_t>(static_cast<const uint8_t*>(p) - hay) + 1 : 0;
    }

    // memchr-anchored scan: short needle, or few candidate start positions. The
    // 256-entry skip table below is pure overhead in those cases.
    if (ndl_len < 8u || (hay_len - ndl_len) < 16u) {
        const uint8_t  first = ndl[0];
        const uint8_t* p     = hay;
        uint32_t       span  = hay_len - ndl_len + 1u;  // # of valid start offsets
        while (span > 0u) {
            const uint8_t* hit = static_cast<const uint8_t*>(
                std::memchr(p, static_cast<int>(first), span));
            if (!hit) return 0;
            if (std::memcmp(hit, ndl, ndl_len) == 0)
                return static_cast<int64_t>(hit - hay) + 1;
            const uint32_t adv = static_cast<uint32_t>(hit - p) + 1u;
            p    = hit + 1u;
            span -= adv;
        }
        return 0;
    }

    // Boyer-Moore-Horspool skip table (longer needle in a much longer haystack).
    uint32_t skip[256];
    for (int k = 0; k < 256; ++k) skip[k] = ndl_len;
    for (uint32_t k = 0u; k < ndl_len - 1u; ++k)
        skip[ndl[k]] = ndl_len - k - 1u;

    const uint8_t last = ndl[ndl_len - 1u];
    uint32_t i = 0u;
    while (i <= hay_len - ndl_len) {
        if (hay[i + ndl_len - 1u] == last && std::memcmp(hay + i, ndl, ndl_len) == 0)
            return static_cast<int64_t>(i) + 1;
        i += skip[hay[i + ndl_len - 1u]];
    }
    return 0;
}

// ---------------------------------------------------------------------------
// LEVENSHTEIN — ported from vector_string_misc.cpp::lev_myers
// ---------------------------------------------------------------------------
//
// Myers' bit-parallel edit distance (Myers 1999 / Hyyrö). Exact distance between
// a pattern P (length m ≤ 64, one machine word) and text T (length n) in O(n)
// word ops. PEq is a caller-owned 256-entry table kept zeroed across calls: set
// the pattern's bits, run, then reset only the touched entries so it stays clean
// (O(m), not O(256), per row).
inline int64_t fse_lev_myers(const uint8_t* P, uint32_t m,
                             const uint8_t* T, uint32_t n,
                             uint64_t* PEq) {
    for (uint32_t k = 0u; k < m; ++k) PEq[P[k]] |= static_cast<uint64_t>(1) << k;
    uint64_t VP = (m < 64u) ? ((static_cast<uint64_t>(1) << m) - 1u) : ~static_cast<uint64_t>(0);
    uint64_t VN = 0u;
    const uint64_t mask = static_cast<uint64_t>(1) << (m - 1u);
    int64_t score = static_cast<int64_t>(m);
    for (uint32_t j = 0u; j < n; ++j) {
        const uint64_t Eq = PEq[T[j]];
        const uint64_t Xv = Eq | VN;
        const uint64_t Xh = (((Eq & VP) + VP) ^ VP) | Eq;
        uint64_t Ph = VN | ~(Xh | VP);
        uint64_t Mh = VP & Xh;
        if (Ph & mask) ++score;
        if (Mh & mask) --score;
        Ph = (Ph << 1) | 1u;
        Mh = Mh << 1;
        VP = Mh | ~(Xv | Ph);
        VN = Ph & Xv;
    }
    for (uint32_t k = 0u; k < m; ++k) PEq[P[k]] = 0u;
    return score;
}

// ---------------------------------------------------------------------------
// TO_ASCII
// ---------------------------------------------------------------------------
//
// Decode the first character of a byte run.
//   NVARCHAR  → the first UTF-8 codepoint.
//   VARCHAR / VARBINARY → the first byte (both are byte-semantics families).
// Empty input → 0 (the PostgreSQL / DuckDB ascii('') convention).
// Returns false only for a malformed leading UTF-8 sequence in an NVARCHAR — the
// caller turns that into a loud error rather than a silently wrong codepoint.
bool fse_first_codepoint(const uint8_t* p, uint32_t len, bool is_utf8, int64_t* out) {
    if (len == 0u) { *out = 0; return true; }
    if (!is_utf8) { *out = static_cast<int64_t>(p[0]); return true; }

    const uint8_t b0 = p[0];
    uint32_t need;      // continuation bytes required
    uint32_t cp;
    if      (b0 < 0x80u)          { *out = static_cast<int64_t>(b0); return true; }
    else if ((b0 & 0xE0u) == 0xC0u) { need = 1u; cp = b0 & 0x1Fu; }
    else if ((b0 & 0xF0u) == 0xE0u) { need = 2u; cp = b0 & 0x0Fu; }
    else if ((b0 & 0xF8u) == 0xF0u) { need = 3u; cp = b0 & 0x07u; }
    else return false;                       // continuation byte or 5/6-byte form

    if (len < need + 1u) return false;       // truncated sequence
    for (uint32_t i = 1u; i <= need; ++i) {
        if ((p[i] & 0xC0u) != 0x80u) return false;
        cp = (cp << 6) | (p[i] & 0x3Fu);
    }
    *out = static_cast<int64_t>(cp);
    return true;
}

// ---------------------------------------------------------------------------
// TO_CHAR / RANDOM_STRING helpers
// ---------------------------------------------------------------------------

inline bool fse_is_integer(DrakenType t) {
    switch (t) {
        case DRAKEN_INT8:  case DRAKEN_INT16:  case DRAKEN_INT32:  case DRAKEN_INT64:
        case DRAKEN_UINT8: case DRAKEN_UINT16: case DRAKEN_UINT32: case DRAKEN_UINT64:
            return true;
        default:
            return false;
    }
}

// Read the integer operand at a logical row as int64, for EVERY integer width and
// signedness (codepoint for TO_CHAR, byte-count for RANDOM_STRING). Both are
// semantically non-negative; reading through int64 keeps a negative signed value
// intact so the caller rejects it loudly rather than silently wrapping it. A
// UINT64 above INT64_MAX becomes negative here and is likewise rejected — no valid
// codepoint or byte-count reaches that magnitude. Caller gates on fse_is_integer.
inline int64_t fse_read_int(const DrakenVector* v, uint32_t row) {
    const uint32_t j = v->selection[row];
    switch (v->type) {
        case DRAKEN_INT8:   return static_cast<const int8_t*>(v->data)[j];
        case DRAKEN_INT16:  return static_cast<const int16_t*>(v->data)[j];
        case DRAKEN_INT32:  return static_cast<const int32_t*>(v->data)[j];
        case DRAKEN_INT64:  return static_cast<const int64_t*>(v->data)[j];
        case DRAKEN_UINT8:  return static_cast<const uint8_t*>(v->data)[j];
        case DRAKEN_UINT16: return static_cast<const uint16_t*>(v->data)[j];
        case DRAKEN_UINT32: return static_cast<const uint32_t*>(v->data)[j];
        case DRAKEN_UINT64: return static_cast<int64_t>(static_cast<const uint64_t*>(v->data)[j]);
        default:            return -1;   // unreachable: fse_is_integer gates every caller
    }
}

// Encode a Unicode scalar value as UTF-8 into buf (≤4 bytes); returns the byte
// count, or -1 for a value that is NOT a scalar (negative, > U+10FFFF, or a
// UTF-16 surrogate U+D800..U+DFFF, which has no UTF-8 form). Codepoint 0 encodes
// to an EMPTY string — TO_CHAR is the documented inverse of TO_ASCII, whose
// empty→0 rule this mirrors (0→empty), rather than emitting an embedded NUL.
inline int fse_utf8_encode(int64_t cp, uint8_t* buf) {
    if (cp == 0)          return 0;
    if (cp < 0 || cp > 0x10FFFF)      return -1;
    if (cp >= 0xD800 && cp <= 0xDFFF) return -1;
    if (cp < 0x80) {
        buf[0] = static_cast<uint8_t>(cp);
        return 1;
    }
    if (cp < 0x800) {
        buf[0] = static_cast<uint8_t>(0xC0u | (cp >> 6));
        buf[1] = static_cast<uint8_t>(0x80u | (cp & 0x3F));
        return 2;
    }
    if (cp < 0x10000) {
        buf[0] = static_cast<uint8_t>(0xE0u | (cp >> 12));
        buf[1] = static_cast<uint8_t>(0x80u | ((cp >> 6) & 0x3F));
        buf[2] = static_cast<uint8_t>(0x80u | (cp & 0x3F));
        return 3;
    }
    buf[0] = static_cast<uint8_t>(0xF0u | (cp >> 18));
    buf[1] = static_cast<uint8_t>(0x80u | ((cp >> 12) & 0x3F));
    buf[2] = static_cast<uint8_t>(0x80u | ((cp >> 6) & 0x3F));
    buf[3] = static_cast<uint8_t>(0x80u | (cp & 0x3F));
    return 4;
}

// RANDOM_STRING RNG. Mirrors function_numeric.cpp's fn_thread_rng: one pcg64 per
// worker thread, seeded independently from std::random_device on first use —
// lock-free under the morsel scheduler, and the correct design for a VOLATILE
// function (no reproducibility contract to honour). This is a SEPARATE
// thread_local from function_numeric's: its accessor lives in that file's
// anonymous namespace and is unreachable across the TU, and that file is not this
// chip's to edit — the same duplication precedent its header sets for the fk_*
// readers. Only the accessor is re-stated; the RNG algorithm (vendored PCG) is not
// re-implemented.
inline pcg64& fse_thread_rng() {
    static thread_local pcg64 rng{pcg_extras::seed_seq_from<std::random_device>()};
    return rng;
}

// Fill `width` bytes of `dst` with random bytes drawn from `rng` (64 bits/draw).
inline void fse_fill_random_bytes(pcg64& rng, uint8_t* dst, uint32_t width) {
    uint32_t i = 0u;
    while (i + 8u <= width) {
        const uint64_t r = rng();
        std::memcpy(dst + i, &r, 8u);
        i += 8u;
    }
    if (i < width) {
        const uint64_t r = rng();
        std::memcpy(dst + i, &r, static_cast<size_t>(width - i));
    }
}

// RAII free-on-throw guard for the hand-allocated string component buffers (the
// draken_date_format pattern). On the success path the kernel nulls the members
// before handing ownership to vecresult_from_string_buffers.
struct FseStringBufGuard {
    DrakenStringSlot* slots;
    uint8_t*          arena;
    uint8_t*          validity;
    ~FseStringBufGuard() {
        if (slots)    draken_free(slots);
        if (arena)    draken_free(arena);
        if (validity) draken_free(validity);
    }
};

}  // namespace

extern "C" {

// OCTET_LENGTH(string) -> INT64. Number of BYTES, for every string family —
// this is exactly the VARCHAR/NVARCHAR/VARBINARY distinction that separates it
// from LENGTH: LENGTH counts CODEPOINTS for NVARCHAR, OCTET_LENGTH never does.
// A null row → null output. Accepts VARCHAR / NVARCHAR / VARBINARY.
VecResult draken_octet_length(void* /*ctx*/, const DrakenVector* const* args, uint32_t nargs) {
    if (nargs != 1) return draken_error_sentinel("draken_octet_length: expected 1 argument");
    const DrakenVector* v = args[0];
    if (!fse_is_string(v->type))
        return draken_error_sentinel("draken_octet_length: string input required");

    const auto* sa = static_cast<const DrakenStringArena*>(v->data);
    const uint32_t n = v->length;

    auto* out = static_cast<int64_t*>(draken_malloc((n > 0u ? n : 1u) * sizeof(int64_t)));
    if (out == nullptr) return draken_error_sentinel("draken_octet_length: allocation failed");

    uint8_t* validity = nullptr;
    if (v->validity != nullptr) {
        const uint32_t bm     = (n + 7u) >> 3;
        const uint32_t padded = (bm + 7u) & ~7u;
        const size_t   vbytes = padded > 0u ? padded : 8u;
        validity = static_cast<uint8_t*>(draken_malloc(vbytes));
        if (validity == nullptr) {
            draken_free(out);
            return draken_error_sentinel("draken_octet_length: allocation failed");
        }
        std::memcpy(validity, v->validity, vbytes);
    }

    // str_length is O(1), so the per-unique staging draken_length needs for its
    // NVARCHAR codepoint scan buys nothing here — the uniform loop IS the fast
    // path for every shape.
    for (uint32_t i = 0u; i < n; ++i) {
        if (!fse_row_valid(v, i)) { out[i] = 0; continue; }
        out[i] = static_cast<int64_t>(str_length(&sa->slots[v->selection[i]]));
    }
    return fse_dense_int64(out, validity, n);
}

// POSITION(sub IN string) -> INT64. 1-based byte position of `sub` within
// `string`, 0 when not found, 1 for an empty needle, null when either input row
// is null.
//
// ARGUMENT ORDER: the registrar declares POSITION_2 as (sub, string) — the SQL
// grammar's order — so args[0] is the NEEDLE and args[1] the HAYSTACK. That is
// the reverse of the underlying search helper's (hay, ndl); the Python shim did
// this same swap explicitly. Getting it backwards is a silent wrong answer, not
// a crash, so it is spelled out here.
VecResult draken_position(void* /*ctx*/, const DrakenVector* const* args, uint32_t nargs) {
    if (nargs != 2) return draken_error_sentinel("draken_position: expected 2 arguments");
    const DrakenVector* ndl = args[0];   // sub     — the needle
    const DrakenVector* hay = args[1];   // string  — the haystack
    if (!fse_is_string(ndl->type) || !fse_is_string(hay->type))
        return draken_error_sentinel("draken_position: string operands required");
    if (ndl->length != hay->length)
        return draken_error_sentinel(
            "draken_position: needle and haystack must have the same length");

    const uint32_t n = hay->length;
    const auto* sh = static_cast<const DrakenStringArena*>(hay->data);
    const auto* sn = static_cast<const DrakenStringArena*>(ndl->data);

    auto* out = static_cast<int64_t*>(draken_malloc((n > 0u ? n : 1u) * sizeof(int64_t)));
    if (out == nullptr) return draken_error_sentinel("draken_position: allocation failed");

    uint8_t* validity = nullptr;
    bool any_null = false;
    for (uint32_t i = 0u; i < n; ++i) {
        if (!fse_row_valid(hay, i) || !fse_row_valid(ndl, i)) {
            if (!validity) {
                validity = fse_alloc_validity_all_valid(n);
                if (!validity) {
                    draken_free(out);
                    return draken_error_sentinel("draken_position: allocation failed");
                }
            }
            validity[i >> 3] &= static_cast<uint8_t>(~(1u << (i & 7u)));
            out[i] = 0;
            any_null = true;
            continue;
        }
        const DrakenStringSlot* hs = &sh->slots[hay->selection[i]];
        const DrakenStringSlot* ns = &sn->slots[ndl->selection[i]];
        out[i] = fse_substring_position(str_data(hs, sh->arena), str_length(hs),
                                        str_data(ns, sn->arena), str_length(ns));
    }
    if (!any_null && validity) { draken_free(validity); validity = nullptr; }
    return fse_dense_int64(out, validity, n);
}

// LEVENSHTEIN(str1, str2) -> INT64. Bytewise edit distance; for NVARCHAR this is
// a BYTE distance, not a codepoint distance (as in the nanobind kernel this is
// ported from). Any null input row → null output row.
VecResult draken_levenshtein(void* /*ctx*/, const DrakenVector* const* args, uint32_t nargs) {
    if (nargs != 2) return draken_error_sentinel("draken_levenshtein: expected 2 arguments");
    const DrakenVector* a = args[0];
    const DrakenVector* b = args[1];
    if (!fse_is_string(a->type) || !fse_is_string(b->type))
        return draken_error_sentinel("draken_levenshtein: string operands required");
    if (a->length != b->length)
        return draken_error_sentinel(
            "draken_levenshtein: input vectors must have the same length");

    const uint32_t n = a->length;
    const auto* sa = static_cast<const DrakenStringArena*>(a->data);
    const auto* sb = static_cast<const DrakenStringArena*>(b->data);

    auto* out = static_cast<int64_t*>(draken_malloc((n > 0u ? n : 1u) * sizeof(int64_t)));
    if (out == nullptr) return draken_error_sentinel("draken_levenshtein: allocation failed");

    // Rolling DP workspace: two rows, grown on demand (DP fallback only).
    size_t   ws_cap = 0u;
    int64_t* prev   = nullptr;
    int64_t* curr   = nullptr;

    // Myers PEq table: 256 entries, invariant-zero between rows.
    uint64_t PEq[256] = {0};

    uint8_t* validity = nullptr;
    bool any_null = false;

    for (uint32_t i = 0u; i < n; ++i) {
        if (!fse_row_valid(a, i) || !fse_row_valid(b, i)) {
            if (!validity) {
                validity = fse_alloc_validity_all_valid(n);
                if (!validity) {
                    draken_free(prev); draken_free(curr); draken_free(out);
                    return draken_error_sentinel("draken_levenshtein: allocation failed");
                }
            }
            validity[i >> 3] &= static_cast<uint8_t>(~(1u << (i & 7u)));
            out[i] = 0;
            any_null = true;
            continue;
        }

        const DrakenStringSlot* slot_a = &sa->slots[a->selection[i]];
        const DrakenStringSlot* slot_b = &sb->slots[b->selection[i]];
        const uint8_t* pa = str_data(slot_a, sa->arena);
        const uint8_t* pb = str_data(slot_b, sb->arena);
        uint32_t la = str_length(slot_a);
        uint32_t lb = str_length(slot_b);

        // Keep the shorter string in lb/pb (Myers pattern = shorter side; the DP
        // fallback workspace is then O(min)).
        if (la < lb) {
            const uint8_t* tmp_p = pa; pa = pb; pb = tmp_p;
            const uint32_t tmp_l = la; la = lb; lb = tmp_l;
        }

        // Cheap exact early-outs.
        if (lb == 0u) { out[i] = static_cast<int64_t>(la); continue; }
        if (la == lb && std::memcmp(pa, pb, la) == 0) { out[i] = 0; continue; }

        // Bit-parallel path: shorter side (pb, length lb) is the pattern.
        if (lb <= 64u) {
            out[i] = fse_lev_myers(pb, lb, pa, la, PEq);
            continue;
        }

        // DP fallback (shorter side > 64 bytes). Grow the workspace if needed.
        const size_t s_len = static_cast<size_t>(lb) + 1u;
        if (s_len > ws_cap) {
            draken_free(prev); prev = nullptr;
            draken_free(curr); curr = nullptr;
            prev = static_cast<int64_t*>(draken_malloc(s_len * sizeof(int64_t)));
            curr = static_cast<int64_t*>(draken_malloc(s_len * sizeof(int64_t)));
            if (!prev || !curr) {
                draken_free(prev); draken_free(curr); draken_free(out);
                if (validity) draken_free(validity);
                return draken_error_sentinel("draken_levenshtein: allocation failed");
            }
            ws_cap = s_len;
        }

        // Row 0: dist(empty, pb[0..j-1]) = j.
        for (uint32_t j = 0u; j <= lb; ++j) prev[j] = j;

        for (uint32_t r = 1u; r <= la; ++r) {
            curr[0] = static_cast<int64_t>(r);
            for (uint32_t c = 1u; c <= lb; ++c) {
                if (pa[r - 1u] == pb[c - 1u]) {
                    curr[c] = prev[c - 1u];
                } else {
                    int64_t best = prev[c];                        // delete
                    if (curr[c - 1u] < best) best = curr[c - 1u];  // insert
                    if (prev[c - 1u] < best) best = prev[c - 1u];  // substitute
                    curr[c] = 1 + best;
                }
            }
            int64_t* tmp = prev; prev = curr; curr = tmp;   // swap rows (pointers)
        }
        out[i] = prev[lb];
    }

    draken_free(prev);
    draken_free(curr);
    if (!any_null && validity) { draken_free(validity); validity = nullptr; }
    return fse_dense_int64(out, validity, n);
}

// TO_ASCII(string) -> INT64 (aliases ASCII, ORD). Codepoint of the FIRST
// character: the first UTF-8 codepoint for NVARCHAR, the first byte for VARCHAR
// and VARBINARY. Empty string → 0. Null row → null output.
//
// This implements the DOCUMENTED contract ("Converts the first character of a
// string to its integer codepoint"). The Python callable it replaces
// (`[ord(a) for a in arr]`) does NOT: ord() raises on any string that is not
// exactly one character, so TO_ASCII('Mercury') and TO_ASCII('') both blew up
// there. This kernel is therefore a deliberate behaviour CHANGE toward the
// documented contract (and toward PostgreSQL / DuckDB), not a faithful port.
VecResult draken_to_ascii(void* /*ctx*/, const DrakenVector* const* args, uint32_t nargs) {
    if (nargs != 1) return draken_error_sentinel("draken_to_ascii: expected 1 argument");
    const DrakenVector* v = args[0];
    if (!fse_is_string(v->type))
        return draken_error_sentinel("draken_to_ascii: string input required");

    const auto* sa = static_cast<const DrakenStringArena*>(v->data);
    const uint32_t n = v->length;
    const bool is_utf8 = (v->type == DRAKEN_NVARCHAR);

    auto* out = static_cast<int64_t*>(draken_malloc((n > 0u ? n : 1u) * sizeof(int64_t)));
    if (out == nullptr) return draken_error_sentinel("draken_to_ascii: allocation failed");

    uint8_t* validity = nullptr;
    if (v->validity != nullptr) {
        const uint32_t bm     = (n + 7u) >> 3;
        const uint32_t padded = (bm + 7u) & ~7u;
        const size_t   vbytes = padded > 0u ? padded : 8u;
        validity = static_cast<uint8_t*>(draken_malloc(vbytes));
        if (validity == nullptr) {
            draken_free(out);
            return draken_error_sentinel("draken_to_ascii: allocation failed");
        }
        std::memcpy(validity, v->validity, vbytes);
    }

    for (uint32_t i = 0u; i < n; ++i) {
        if (!fse_row_valid(v, i)) { out[i] = 0; continue; }
        const DrakenStringSlot* slot = &sa->slots[v->selection[i]];
        if (!fse_first_codepoint(str_data(slot, sa->arena), str_length(slot),
                                 is_utf8, &out[i])) {
            draken_free(out);
            if (validity) draken_free(validity);
            return draken_error_sentinel(
                "draken_to_ascii: malformed UTF-8 leading sequence in NVARCHAR input");
        }
    }
    return fse_dense_int64(out, validity, n);
}

// TO_CHAR(codepoint) -> VARCHAR (alias CHR). The inverse of draken_to_ascii:
// converts an integer Unicode codepoint to its UTF-8 encoding — one character.
// Empty string for codepoint 0 (mirroring TO_ASCII's empty→0); a LOUD error for
// any value that is not a Unicode scalar (negative, > U+10FFFF, or a surrogate),
// never a silently wrong or replacement character. Null row → null output.
//
// Output type is the registrar's declared VARCHAR; its bytes are the codepoint's
// UTF-8 form (≤4 bytes, always inline — no arena). Dense per-logical-row (see the
// file header for why this is not shape-preserving). Single overload, so a bare
// draken_to_char registry entry captures TO_CHAR/CHR with no _fn_skip_lookup gate.
VecResult draken_to_char(void* /*ctx*/, const DrakenVector* const* args, uint32_t nargs) {
    if (nargs != 1) return draken_error_sentinel("draken_to_char: expected 1 argument");
    const DrakenVector* v = args[0];
    if (!fse_is_integer(v->type))
        return draken_error_sentinel("draken_to_char: integer codepoint input required");

    // Explicit try/catch (NOT DRAKEN_KERNEL_TRY): the body's brace-initializer
    // comma lists would be split by the function-like macro's argument parser.
    try {
        const uint32_t n = v->length;
        const size_t slots_sz = (n > 0u ? n : 1u) * sizeof(DrakenStringSlot);
        auto* slots = static_cast<DrakenStringSlot*>(draken_malloc(slots_sz));
        if (!slots) return draken_error_sentinel("draken_to_char: allocation failed");
        std::memset(slots, 0, slots_sz);
        FseStringBufGuard g{slots, nullptr, nullptr};

        g.validity = kernel_copy_validity(v);   // nullptr if all-valid; throws on OOM (caught)

        for (uint32_t i = 0u; i < n; ++i) {
            if (!fse_row_valid(v, i)) { str_init_null(&slots[i]); continue; }
            const int64_t cp = fse_read_int(v, i);
            uint8_t buf[4];
            const int len = fse_utf8_encode(cp, buf);
            if (len < 0)
                return draken_error_sentinel_fmt(
                    "draken_to_char: codepoint %lld is not a Unicode scalar value",
                    static_cast<long long>(cp));
            str_init_inline(&slots[i], buf, static_cast<uint32_t>(len));
        }

        DrakenStringSlot* out_slots = g.slots;
        uint8_t*          validity  = g.validity;
        g.slots = nullptr; g.validity = nullptr;
        return vecresult_from_string_buffers(out_slots, nullptr, 0u, validity, n, DRAKEN_VARCHAR);
    } catch (const std::exception& e) {
        return draken_error_sentinel(e.what());
    } catch (...) {
        return draken_error_sentinel("draken_to_char: unknown error");
    }
}

// RANDOM_STRING(n) -> VARBINARY. Each output row is `n` random BYTES, where `n` is
// that row's value of the integer operand — a constant literal broadcast to the
// morsel, or a real column. Architect ruling (2026-07-17): honour the declared
// VARBINARY return, n random bytes (not characters), one value per row.
//
// VOLATILE: one independent draw per row (the per-thread PCG stream). It is never
// constant-folded — constant_folding.py excludes RANDOM_STRING — and never
// shape-preserving (every row needs its own value). n = 0 → empty VARBINARY; a
// NULL operand row → NULL output; a negative n → loud error. Single overload, so a
// bare draken_random_string entry captures it with no _fn_skip_lookup gate.
VecResult draken_random_string(void* /*ctx*/, const DrakenVector* const* args, uint32_t nargs) {
    if (nargs != 1) return draken_error_sentinel("draken_random_string: expected 1 argument");
    const DrakenVector* v = args[0];
    if (!fse_is_integer(v->type))
        return draken_error_sentinel("draken_random_string: integer length input required");

    // Explicit try/catch (NOT DRAKEN_KERNEL_TRY): the body's brace-initializer
    // comma lists would be split by the function-like macro's argument parser.
    try {
        const uint32_t n = v->length;
        const size_t slots_sz = (n > 0u ? n : 1u) * sizeof(DrakenStringSlot);
        auto* slots = static_cast<DrakenStringSlot*>(draken_malloc(slots_sz));
        if (!slots) return draken_error_sentinel("draken_random_string: allocation failed");
        std::memset(slots, 0, slots_sz);
        FseStringBufGuard g{slots, nullptr, nullptr};

        // Pass 1: sum arena bytes for the widths that exceed the inline slot, and
        // validate every width loudly before committing further allocations.
        size_t arena_cap = 0u;
        for (uint32_t i = 0u; i < n; ++i) {
            if (!fse_row_valid(v, i)) continue;
            const int64_t w = fse_read_int(v, i);
            if (w < 0)
                return draken_error_sentinel_fmt(
                    "draken_random_string: negative length %lld", static_cast<long long>(w));
            if (w > STR_INLINE_MAX) arena_cap += static_cast<size_t>(w);
        }

        if (arena_cap > 0u) {
            g.arena = static_cast<uint8_t*>(draken_malloc(arena_cap));
            if (!g.arena) return draken_error_sentinel("draken_random_string: allocation failed");
        }
        g.validity = kernel_copy_validity(v);   // nullptr if all-valid; throws on OOM (caught)

        pcg64& rng = fse_thread_rng();
        size_t arena_used = 0u;
        for (uint32_t i = 0u; i < n; ++i) {
            if (!fse_row_valid(v, i)) { str_init_null(&slots[i]); continue; }
            const uint32_t width = static_cast<uint32_t>(fse_read_int(v, i));  // ≥0 (pass 1)
            if (width <= STR_INLINE_MAX) {
                uint8_t inl[STR_INLINE_MAX];
                fse_fill_random_bytes(rng, inl, width);
                str_init_inline(&slots[i], inl, width);
            } else {
                uint8_t* dst = g.arena + arena_used;
                fse_fill_random_bytes(rng, dst, width);
                draken_build_string_slot(&slots[i], dst, width, static_cast<uint32_t>(arena_used));
                arena_used += width;
            }
        }

        DrakenStringSlot* out_slots = g.slots;
        uint8_t*          out_arena = g.arena;
        uint8_t*          validity  = g.validity;
        g.slots = nullptr; g.arena = nullptr; g.validity = nullptr;
        return vecresult_from_string_buffers(out_slots, out_arena, arena_used, validity, n,
                                             DRAKEN_VARBINARY);
    } catch (const std::exception& e) {
        return draken_error_sentinel(e.what());
    } catch (...) {
        return draken_error_sentinel("draken_random_string: unknown error");
    }
}

}  // extern "C"
