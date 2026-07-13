// draken/ops/kernels/string_replace_soundex.cpp — Phase 9a-fn: the two
// "less-templatey" string transforms on the C ABI (func_fn_t shape):
//
//     VecResult fn(void* ctx, const DrakenVector* const* args, uint32_t nargs)
//
//   draken_replace(haystack, search, replace) — replace ALL non-overlapping
//       occurrences of the LITERAL substring `search` with `replace`. NOT regex
//       (that is REGEXP_REPLACE). Variable-length output → two-pass arena sizing.
//   draken_soundex(str)                        — American Soundex (uppercase first
//       letter + 3 digits). Fixed 4-byte output, always inline. NULL-introducing:
//       empty / no-alphabetic input → null output row.
//
// Both are SHAPE-PRESERVING (the string→string pattern established by
// ascii_case_transform / draken_substring in function_kernels.cpp): the transform
// is a pure function of a physical value's bytes, so it is computed ONCE per
// data_length PHYSICAL unique value into a K-slot string block, then
// kernel_preserve_shape carries the input's selection + per-logical-row validity
// onto the result. Dense stays dense, constant stays constant, dict stays dict.
//
// Both fail LOUD with an error sentinel for anything outside their contract —
// never a silent wrong answer. Neither lets a C++ exception escape into the nogil
// VM: allocations use draken_malloc (returns nullptr, never throws) and the only
// throwing call (kernel_preserve_shape) is wrapped in a local try/catch that frees
// the block and returns a sentinel.
//
// Ported algorithms (CLAUDE.md §3 — no duplication):
//   draken_soundex reproduces the vendored Soundex byte-for-byte from
//     opteryx/compiled/nanobind/vector_string_misc3.cpp (soundex_compute).
//   draken_replace reproduces the bytewise scan from
//     opteryx/compiled/nanobind/vector_string_misc2.cpp (impl_replace), lifted to
//     the per-physical-value shape-preserving form.
//
// NVARCHAR note (REPLACE): byte-level replacement is codepoint-safe for valid
// UTF-8 because UTF-8 is self-synchronizing — a whole valid-UTF-8 `search` can
// only match on codepoint boundaries within a valid-UTF-8 haystack, never
// straddling one. So for NVARCHAR we VALIDATE that `search` and `replace` are
// themselves whole valid UTF-8 and fail loud otherwise (a partial-codepoint
// needle/replacement could corrupt encoding — never silently wrong).

#include <vector>
#include <cstdint>
#include <cstring>

#include "core/buffers.h"
#include "core/string_slot.h"
#include "core/alloc.h"
#include "ops/vec_result.h"
#include "ops/kernels/result_helpers.h"
#include "ops/kernels/error_handling.h"
#include "xxhash.h"                // XXH3_64bits — long-slot hash32, same as every builder
#include "utf8.h"                  // utf8nvalid — NVARCHAR needle/replacement validation

namespace {

inline bool rs_is_string(DrakenType t) {
    return t == DRAKEN_VARCHAR || t == DRAKEN_NVARCHAR || t == DRAKEN_VARBINARY;
}

// ---------------------------------------------------------------------------
// Soundex — ported verbatim from vector_string_misc3.cpp
// ---------------------------------------------------------------------------
//
// Indices 0-25 correspond to A-Z. Digit chars '0'..'6'.
const char SOUNDEX_MAP[26] = {
    '0','1','2','3','0','1','2','0','0','2','2','4','5',
    '5','0','1','2','6','2','3','0','1','0','2','0','2'
};
const uint32_t SOUNDEX_LEN = 4u;

// Compute Soundex code for raw bytes s[0..len). Writes exactly 4 ASCII bytes into
// out[0..4) and returns true. Returns false if the input contains no alphabetic
// characters (→ null output row).
bool soundex_compute(const uint8_t* s, uint32_t len, char out[4]) {
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
// REPLACE — bytewise scan ported from vector_string_misc2.cpp::impl_replace.
// ---------------------------------------------------------------------------

// Count non-overlapping occurrences of needle[0..nlen) in hay[0..hlen).
// nlen == 0 → 0 (empty needle is a no-op; handled by the caller).
uint32_t rs_count_occurrences(const uint8_t* hay, uint32_t hlen,
                              const uint8_t* ndl, uint32_t nlen) {
    if (nlen == 0u || nlen > hlen) return 0u;
    uint32_t count = 0u;
    uint32_t pos = 0u;
    while (pos + nlen <= hlen) {
        if (std::memcmp(hay + pos, ndl, nlen) == 0) {
            ++count;
            pos += nlen;   // non-overlapping
        } else {
            ++pos;
        }
    }
    return count;
}

// Build the replaced bytes for one haystack value into dst[0..out_len). Caller
// sized dst via the same count. nlen == 0 → copy haystack verbatim.
void rs_build_replaced(const uint8_t* hay, uint32_t hlen,
                       const uint8_t* ndl, uint32_t nlen,
                       const uint8_t* rep, uint32_t rlen,
                       uint8_t* dst) {
    if (nlen == 0u) {
        std::memcpy(dst, hay, hlen);
        return;
    }
    uint32_t pos = 0u;
    uint32_t w = 0u;
    while (pos + nlen <= hlen) {
        if (std::memcmp(hay + pos, ndl, nlen) == 0) {
            std::memcpy(dst + w, rep, rlen);
            w += rlen;
            pos += nlen;
        } else {
            dst[w++] = hay[pos++];
        }
    }
    // Tail bytes after the last scan position.
    if (pos < hlen) {
        std::memcpy(dst + w, hay + pos, hlen - pos);
    }
}

}  // namespace

extern "C" {

// SOUNDEX(string) -> VARCHAR. American Soundex; ASCII-only, fixed 4-byte codes,
// always inline. NULL-introducing: empty / no-alphabetic input → null output row.
// Accepts VARCHAR / NVARCHAR / VARBINARY (byte scan; non-A-Z bytes skipped, so
// UTF-8 multibyte bytes are simply ignored — matches the vendored impl).
VecResult draken_soundex(void* /*ctx*/, const DrakenVector* const* args, uint32_t nargs) {
    if (nargs != 1) return draken_error_sentinel("draken_soundex: expected 1 argument");
    const DrakenVector* v = args[0];
    if (!rs_is_string(v->type))
        return draken_error_sentinel("draken_soundex: string operand required");

    const auto* sa = static_cast<const DrakenStringArena*>(v->data);
    const uint32_t k = v->data_length;   // physical unique count
    const uint32_t n = v->length;

    // K-slot physical block. All Soundex codes are exactly 4 bytes ≤ STR_INLINE_MAX
    // (12) → every slot inline, no arena. NO embedded validity (per-logical-row
    // nulls come from kernel_preserve_shape below, then the bad-fold).
    DrakenStringSlot* slots;
    uint8_t* arena_unused;
    uint8_t* validity_unused;
    uint8_t* block = vecresult_string_block_alloc(k, /*arena_len=*/0, /*want_validity=*/0,
                                                  &slots, &arena_unused, &validity_unused);
    if (block == nullptr) return draken_error_sentinel("draken_soundex: allocation failed");

    // Per-physical-value null flag: soundex_compute() == false (empty / non-alpha).
    std::vector<uint8_t> bad(k > 0u ? k : 1u, 0u);
    bool any_bad = false;
    char code[4];
    for (uint32_t j = 0u; j < k; ++j) {
        const DrakenStringSlot* slot = &sa->slots[j];
        const uint8_t* bytes = str_data(slot, sa->arena);
        const uint32_t len   = str_length(slot);
        if (soundex_compute(bytes, len, code)) {
            str_init_inline(&slots[j], reinterpret_cast<const uint8_t*>(code), SOUNDEX_LEN);
        } else {
            // slots[j] stays zeroed (null canonical); the logical rows referencing
            // it are marked null via the bad-fold below.
            bad[j] = 1u;
            any_bad = true;
        }
    }

    VecResult r = vecresult_from_string_block(block, k, /*arena_len=*/0,
                                              /*has_validity=*/0, DRAKEN_VARCHAR);
    try {
        kernel_preserve_shape(r, v);   // r.validity = input copy (or null), non-embedded
    } catch (const std::exception&) {
        draken_free(block);
        return draken_error_sentinel("draken_soundex: shape-carry allocation failed");
    }

    // Fold per-physical null-ness into the preserved per-logical-row validity
    // (the string→float cast's null-introduction pattern): a physical value whose
    // Soundex is null maps EVERY logical row referencing it (via selection) to null.
    if (any_bad) {
        if (!r.validity) {   // input was all-valid — materialise an all-valid bitmap
            const uint32_t bmn    = (n + 7u) >> 3;
            const uint32_t padded = (bmn + 7u) & ~7u;
            const size_t   vbytes = padded > 0u ? padded : 8u;
            uint8_t* nv = static_cast<uint8_t*>(draken_malloc(vbytes));
            if (!nv) {
                if (r.owns_selection) draken_free(const_cast<uint32_t*>(r.selection));
                draken_free(block);
                return draken_error_sentinel("draken_soundex: allocation failed");
            }
            std::memset(nv, 0xFF, vbytes);
            r.validity = nv;
        }
        for (uint32_t i = 0u; i < n; ++i)
            if (bad[v->selection[i]])
                r.validity[i >> 3] &= static_cast<uint8_t>(~(1u << (i & 7u)));
    }
    return r;
}

// REPLACE(string, search, replace) -> same string family as `string`. Replaces
// ALL non-overlapping occurrences of the LITERAL substring `search` with
// `replace`. Empty `search` → input unchanged (PostgreSQL / SQL convention).
//
// `search` and `replace` MUST be scalar (data_length == 1 — the shape a literal
// materialises to, BC_LOAD_LIT_CONST). A non-scalar (per-row) needle/replacement
// is not supported natively: fail loud rather than silently mis-shape.
//
// NULL policy (strict, matching the signature): a null haystack row → null output
// (carried by kernel_preserve_shape). A null scalar `search` or `replace` → the
// whole column is null.
VecResult draken_replace(void* /*ctx*/, const DrakenVector* const* args, uint32_t nargs) {
    if (nargs != 3) return draken_error_sentinel("draken_replace: expected 3 arguments");
    const DrakenVector* hay = args[0];
    const DrakenVector* ndl = args[1];
    const DrakenVector* rep = args[2];
    if (!rs_is_string(hay->type) || !rs_is_string(ndl->type) || !rs_is_string(rep->type))
        return draken_error_sentinel("draken_replace: string operands required");
    if (ndl->data_length != 1u || rep->data_length != 1u)
        return draken_error_sentinel(
            "draken_replace: search and replace must be scalar literals "
            "(per-row search/replace not supported natively)");

    const uint32_t n = hay->length;
    const auto* sh = static_cast<const DrakenStringArena*>(hay->data);
    const auto* sn = static_cast<const DrakenStringArena*>(ndl->data);
    const auto* sr = static_cast<const DrakenStringArena*>(rep->data);

    // Scalar needle/replacement live at physical slot 0 (data_length == 1). Their
    // null-ness is per-logical-row; row 0 represents the shared constant value.
    const bool scalar_null =
        (n > 0u && (kernel_row_is_null(ndl, 0u) || kernel_row_is_null(rep, 0u)));

    const DrakenStringSlot* nslot = &sn->slots[0];
    const DrakenStringSlot* rslot = &sr->slots[0];
    const uint8_t* ndl_data = str_data(nslot, sn->arena);
    const uint8_t* rep_data = str_data(rslot, sr->arena);
    const uint32_t ndl_len  = str_length(nslot);
    const uint32_t rep_len  = str_length(rslot);

    // NVARCHAR: byte-level replace is only codepoint-safe when needle/replacement
    // are whole valid UTF-8 (see file header). Fail loud on partial codepoints.
    if (hay->type == DRAKEN_NVARCHAR) {
        if ((ndl_len > 0u &&
             utf8nvalid(reinterpret_cast<const utf8_int8_t*>(ndl_data), ndl_len) != nullptr) ||
            (rep_len > 0u &&
             utf8nvalid(reinterpret_cast<const utf8_int8_t*>(rep_data), rep_len) != nullptr))
            return draken_error_sentinel(
                "draken_replace: NVARCHAR search/replace must be whole valid UTF-8");
    }

    const uint32_t k = hay->data_length;   // physical unique count

    // Pass 1: output length per physical haystack value, summing long-form bytes to
    // size the arena exactly. out_len = hlen + count*(rep_len - ndl_len), computed
    // in the non-negative form hlen - count*ndl_len + count*rep_len.
    std::vector<uint32_t> out_lens(k > 0u ? k : 1u, 0u);
    size_t arena_len = 0;
    for (uint32_t j = 0u; j < k; ++j) {
        const DrakenStringSlot* slot = &sh->slots[j];
        const uint8_t* hd = str_data(slot, sh->arena);
        const uint32_t hl = str_length(slot);
        uint32_t out_len;
        if (ndl_len == 0u) {
            out_len = hl;
        } else {
            const uint32_t c = rs_count_occurrences(hd, hl, ndl_data, ndl_len);
            out_len = hl - c * ndl_len + c * rep_len;
        }
        out_lens[j] = out_len;
        if (out_len > STR_INLINE_MAX) arena_len += out_len;
    }

    DrakenStringSlot* slots;
    uint8_t* arena;
    uint8_t* validity_unused;
    uint8_t* block = vecresult_string_block_alloc(k, arena_len, /*want_validity=*/0,
                                                  &slots, &arena, &validity_unused);
    if (block == nullptr) return draken_error_sentinel("draken_replace: allocation failed");

    // Pass 2: build each physical output, inline or into the sized arena.
    uint8_t buf_inline[STR_INLINE_MAX];
    size_t arena_pos = 0;
    for (uint32_t j = 0u; j < k; ++j) {
        const DrakenStringSlot* slot = &sh->slots[j];
        const uint8_t* hd = str_data(slot, sh->arena);
        const uint32_t hl = str_length(slot);
        const uint32_t out_len = out_lens[j];
        if (out_len <= STR_INLINE_MAX) {
            rs_build_replaced(hd, hl, ndl_data, ndl_len, rep_data, rep_len, buf_inline);
            str_init_inline(&slots[j], buf_inline, out_len);
        } else {
            uint8_t* dst = arena + arena_pos;
            rs_build_replaced(hd, hl, ndl_data, ndl_len, rep_data, rep_len, dst);
            str_init_extern(&slots[j], dst, out_len,
                            static_cast<uint32_t>(XXH3_64bits(dst, out_len)),
                            static_cast<uint32_t>(arena_pos));
            arena_pos += out_len;
        }
    }

    VecResult r = vecresult_from_string_block(block, k, arena_len,
                                              /*has_validity=*/0, hay->type);
    try {
        kernel_preserve_shape(r, hay);   // preserves haystack's per-row nulls
    } catch (const std::exception&) {
        draken_free(block);
        return draken_error_sentinel("draken_replace: shape-carry allocation failed");
    }

    // Strict null: a null scalar search/replace makes the entire column null.
    if (scalar_null && n > 0u) {
        if (!r.validity) {
            const uint32_t bmn    = (n + 7u) >> 3;
            const uint32_t padded = (bmn + 7u) & ~7u;
            const size_t   vbytes = padded > 0u ? padded : 8u;
            uint8_t* nv = static_cast<uint8_t*>(draken_malloc(vbytes));
            if (!nv) {
                if (r.owns_selection) draken_free(const_cast<uint32_t*>(r.selection));
                draken_free(block);
                return draken_error_sentinel("draken_replace: allocation failed");
            }
            r.validity = nv;
        }
        const uint32_t bmn = (n + 7u) >> 3;
        std::memset(r.validity, 0x00, bmn);   // all rows null
    }
    return r;
}

}  // extern "C"
