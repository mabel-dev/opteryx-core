// draken/ops/kernels/string_trim.cpp — Phase 9a-fn: TRIM / LTRIM / RTRIM string
// kernels on the C ABI. Signature is the design's func_fn_t:
//
//     VecResult fn(void* ctx, const DrakenVector* const* args, uint32_t nargs)
//
// Dispatched DIRECTLY from the nogil DV* VM (evaluation.pyx's BC_FUNCTION C-native
// arm) — no Python, no nanobind, no GIL. TRIM strips both ends, LTRIM leading,
// RTRIM trailing.
//
// TWO ARITIES, one per SQL spelling:
//   1 argument   TRIM(s)                        strip ASCII whitespace (0x09-0x0D, 0x20)
//   2 arguments  TRIM(BOTH 'ab' FROM s)         strip any character in the SET 'ab'
//                TRIM(s, 'ab')                  the call spelling of the same thing
// The trim argument is a SET OF CHARACTERS, not a substring: TRIM(BOTH 'ab' FROM
// 'baXab') is 'X'. (Architect ruling, 2026-08-10 — the standard's single-character
// reading is stricter than every engine a caller has met, and the set reading is
// what Postgres and DuckDB do.) An EMPTY set strips nothing.
//
// String subtype: ASCII-whitespace trim is BYTE-SAFE for every string family.
// The whitespace bytes we scan for (0x09-0x0D, 0x20) are all < 0x80, so they can
// never be a UTF-8 continuation byte or part of a multibyte sequence — stripping
// them from either end of an NVARCHAR value can never split a codepoint. VARBINARY
// is bytes by definition. So all three of VARCHAR / NVARCHAR / VARBINARY take the
// same byte scan and the input's tag is preserved. (This is why TRIM accepts
// NVARCHAR where the case transforms fail loud: casing needs full Unicode mapping;
// whitespace trimming does not.)
//
// AN ARBITRARY CHARACTER SET BREAKS THAT ARGUMENT, so the two-argument form picks
// its scan from the operand's family (architect ruling, 2026-08-10 — codepoint
// scan for NVARCHAR rather than an ASCII-only restriction):
//
//   VARCHAR    BYTE scan. VARCHAR is ASCII bytes and non-ASCII content in one is
//              undefined (RATIFIED/varchar-is-ascii-bytes-and-non-ascii-content-
//              is-undefined), so a byte membership test IS the character test.
//   VARBINARY  BYTE scan. Bytes by definition; there are no codepoints to split.
//   NVARCHAR   CODEPOINT scan — see CodepointMatcher below. A byte scan here would
//              happily strip the 0xA9 tail off `é` (C3 A9) when the set contained
//              some unrelated character whose encoding shares that byte, leaving a
//              truncated sequence behind. The codepoint scan cannot: it only ever
//              consumes whole encoded characters.
//
//              When the set is entirely ASCII the byte scan is used for NVARCHAR
//              too, because there the two are provably the same test — an ASCII
//              byte (< 0x80) is never part of a multibyte sequence, so matching it
//              as a byte and matching it as a codepoint accept exactly the same
//              positions. That keeps TRIM(BOTH ' ' FROM nvarchar_col) on the fast
//              path without weakening anything.
//
// CONSTANT CHARACTER SET (architect ruling, 2026-08-10). A per-ROW set would
// destroy the shape preservation below, because the trimmed range would stop
// being a function of the value alone. Opteryx declares the parameter
// `constant_only` and enforces that at BIND (compiled_expression.pyx), which is
// where a caller gets a message naming the argument; this kernel refuses it again
// because it is a C ABI kernel and draken has callers that never see a binder.
//
// SHAPE-PRESERVING (the string-CAST / substring pattern, function_kernels.cpp):
// the trimmed range is a pure function of a physical value's bytes, so it is
// computed ONCE per data_length PHYSICAL unique value, then kernel_preserve_shape
// carries the input's selection + per-logical-row validity onto the result. Dense
// stays dense, constant stays constant, dict stays dict. Trim shortens lengths per
// value, so the arena is sized over the K outputs (two-pass, like draken_substring).

#include <cstdint>
#include <cstring>
#include <vector>

#include "core/buffers.h"
#include "core/string_slot.h"
#include "core/alloc.h"
#include "ops/vec_result.h"
#include "ops/kernels/result_helpers.h"
#include "ops/kernels/error_handling.h"
#include "xxhash.h"                // XXH3_64bits — long-slot hash32, same as every builder

namespace {

inline bool trim_is_string(DrakenType t) {
    return t == DRAKEN_VARCHAR || t == DRAKEN_NVARCHAR || t == DRAKEN_VARBINARY;
}

// ─────────────────────────────────────────────────────────────────────────────
// Matchers. Each answers ONE question at ONE edge: "how many bytes of the trim
// set match here?", 0 meaning "stop scanning". Returning a byte COUNT rather than
// a bool is what lets the codepoint matcher consume a whole multibyte character
// in a single step, on the same loop the byte matchers use.
// ─────────────────────────────────────────────────────────────────────────────

// The one-argument form, unchanged: ASCII whitespace, tested directly rather than
// through the membership bitmap so the existing hot path keeps its codegen.
struct WhitespaceMatcher {
    static inline bool is_ws(uint8_t c) noexcept {
        return c == 0x20u || (c >= 0x09u && c <= 0x0Du);
    }
    inline uint32_t left(const uint8_t* p, uint32_t /*avail*/) const noexcept {
        return is_ws(p[0]) ? 1u : 0u;
    }
    // `p` points ONE PAST the last byte of the window.
    inline uint32_t right(const uint8_t* p, uint32_t /*avail*/) const noexcept {
        return is_ws(p[-1]) ? 1u : 0u;
    }
};

// Byte membership over a 256-bit bitmap: VARCHAR, VARBINARY, and any all-ASCII set.
struct ByteMatcher {
    const uint64_t* bits;
    inline bool has(uint8_t c) const noexcept {
        return ((bits[c >> 6] >> (c & 63u)) & 1ull) != 0ull;
    }
    inline uint32_t left(const uint8_t* p, uint32_t /*avail*/) const noexcept {
        return has(p[0]) ? 1u : 0u;
    }
    inline uint32_t right(const uint8_t* p, uint32_t /*avail*/) const noexcept {
        return has(p[-1]) ? 1u : 0u;
    }
};

// Codepoint membership for NVARCHAR: the set is split into its encoded characters
// once, and each edge is tested by comparing whole encoded spans.
//
// WHY SPAN COMPARISON IS EXACTLY A CODEPOINT TEST, with no decoding of the VALUE.
// Every span in the set is a validated UTF-8 sequence, so it starts with a lead
// byte (< 0x80, or 0xC0-0xF7) and contains only continuation bytes (0x80-0xBF)
// after it.
//   * At the LEFT edge the scan starts at offset 0 — a character boundary — and
//     only ever advances by a whole span, so it is always at a boundary. UTF-8 is
//     prefix-free at boundaries, so a span that matches there IS that character.
//   * At the RIGHT edge, no valid UTF-8 sequence is a proper SUFFIX of another:
//     any proper suffix begins with a continuation byte, and no valid sequence
//     does. So a span matching the last bytes of the window can only be the final
//     character, never the tail of a longer one.
// Either way the scan consumes whole characters and can never split one. An
// undecodable tail in the VALUE simply matches nothing and stops the scan, which
// is the right answer — a byte sequence that is not a character is not in a set
// of characters. The value is never rejected for its content; only the SET is
// validated, and it is a constant, so that failure is not data-dependent.
struct CodepointMatcher {
    const uint8_t*  set_bytes;
    const uint32_t* spans;   // ncp + 1 offsets into set_bytes
    uint32_t        ncp;
    inline uint32_t left(const uint8_t* p, uint32_t avail) const noexcept {
        for (uint32_t i = 0u; i < ncp; ++i) {
            const uint32_t n = spans[i + 1u] - spans[i];
            if (n <= avail && std::memcmp(p, set_bytes + spans[i], n) == 0) return n;
        }
        return 0u;
    }
    inline uint32_t right(const uint8_t* p, uint32_t avail) const noexcept {
        for (uint32_t i = 0u; i < ncp; ++i) {
            const uint32_t n = spans[i + 1u] - spans[i];
            if (n <= avail && std::memcmp(p - n, set_bytes + spans[i], n) == 0) return n;
        }
        return 0u;
    }
};

// An empty character set strips nothing. Used for TRIM(BOTH '' FROM s) and, with
// both edges off, to produce the identity copy the NULL-set path nulls out.
struct EmptyMatcher {
    inline uint32_t left(const uint8_t*, uint32_t) const noexcept { return 0u; }
    inline uint32_t right(const uint8_t*, uint32_t) const noexcept { return 0u; }
};

// Compute the trimmed byte window [off, off+len) of one slot's bytes.
template <class Matcher>
inline void trim_range(const uint8_t* data, uint32_t blen,
                       bool trim_left, bool trim_right, const Matcher& m,
                       uint32_t* out_off, uint32_t* out_len) {
    uint32_t start = 0u;
    uint32_t end   = blen;
    if (trim_left) {
        for (;;) {
            if (start >= end) break;
            const uint32_t n = m.left(data + start, end - start);
            if (n == 0u) break;
            start += n;
        }
    }
    if (trim_right) {
        for (;;) {
            if (end <= start) break;
            const uint32_t n = m.right(data + end, end - start);
            if (n == 0u) break;
            end -= n;
        }
    }
    *out_off = start;
    *out_len = end - start;
}

template <class Matcher>
VecResult trim_kernel(const DrakenVector* v, bool trim_left, bool trim_right,
                      const Matcher& m, const char* who) {
    const auto* sa = static_cast<const DrakenStringArena*>(v->data);
    const uint32_t k = v->data_length;   // physical unique count

    // Pass 1: total long-form bytes over the K physical trimmed outputs.
    size_t arena_len = 0;
    for (uint32_t j = 0; j < k; ++j) {
        const DrakenStringSlot* slot = &sa->slots[j];
        uint32_t off, len;
        trim_range(str_data(slot, sa->arena), str_length(slot),
                   trim_left, trim_right, m, &off, &len);
        if (len > STR_INLINE_MAX) arena_len += len;
    }

    // K-slot physical block, NO embedded validity (per-logical-row nulls come from
    // kernel_preserve_shape).
    DrakenStringSlot* slots;
    uint8_t* arena;
    uint8_t* validity_unused;
    uint8_t* block = vecresult_string_block_alloc(k, arena_len, /*want_validity=*/0,
                                                  &slots, &arena, &validity_unused);
    if (block == nullptr) return draken_error_sentinel_fmt("%s: allocation failed", who);

    size_t arena_pos = 0;
    for (uint32_t j = 0; j < k; ++j) {
        const DrakenStringSlot* slot = &sa->slots[j];
        const uint8_t* src = str_data(slot, sa->arena);
        uint32_t off, len;
        trim_range(src, str_length(slot), trim_left, trim_right, m, &off, &len);
        const uint8_t* sub = src + off;
        if (len <= STR_INLINE_MAX) {
            str_init_inline(&slots[j], sub, len);
        } else {
            uint8_t* dst = arena + arena_pos;
            std::memcpy(dst, sub, len);
            str_init_extern(&slots[j], dst, len,
                            static_cast<uint32_t>(arena_pos));
            arena_pos += len;
        }
    }

    // Carry the input's shape onto the dense K-block (see draken_substring for the
    // identity-replacement and bad_alloc-containment rationale).
    VecResult r = vecresult_from_string_block(block, k, arena_len, /*has_validity=*/0, v->type);
    try {
        kernel_preserve_shape(r, v);
    } catch (const std::exception&) {
        draken_free(block);
        return draken_error_sentinel_fmt("%s: shape-carry allocation failed", who);
    }
    return r;
}

// ─────────────────────────────────────────────────────────────────────────────
// Trim-set extraction from the second argument.
// ─────────────────────────────────────────────────────────────────────────────

// Length of the UTF-8 sequence a lead byte opens; 0 for a continuation byte or an
// invalid lead.
inline uint32_t utf8_seq_len(uint8_t b) noexcept {
    if (b < 0x80u)            return 1u;
    if ((b & 0xE0u) == 0xC0u) return 2u;
    if ((b & 0xF0u) == 0xE0u) return 3u;
    if ((b & 0xF8u) == 0xF0u) return 4u;
    return 0u;
}

struct TrimSetArg {
    const uint8_t* bytes;
    uint32_t       nbytes;
    bool           is_null;      // the constant is NULL — the whole result is NULL
    bool           all_ascii;
};

// Read the single physical value out of a CONSTANT string vector. Returns false
// and formats the error on anything that is not one.
bool trim_set_from(const DrakenVector* sv, const char* who, TrimSetArg* out, VecResult* err) {
    if (!trim_is_string(sv->type)) {
        *err = draken_error_sentinel_fmt("%s: the trim character set must be a string", who);
        return false;
    }
    // A constant is ONE physical value shared by every row. data_length == 1 is
    // exactly that, whatever encoding shape carries it (CLAUDE.md §11 — this is
    // reading the layout, not dispatching on it).
    if (sv->data_length != 1u) {
        *err = draken_error_sentinel_fmt(
            "%s: the trim character set must be a constant, not a column", who);
        return false;
    }
    // ...and its NULL-ness must be uniform too. One physical value with a mixed
    // validity is a per-ROW null pattern, which is not a constant however the
    // data buffer happens to look.
    bool is_null = false;
    if (sv->validity != nullptr) {
        uint32_t nulls = 0u;
        for (uint32_t i = 0u; i < sv->length; ++i)
            if (kernel_row_is_null(sv, i)) ++nulls;
        if (nulls != 0u && nulls != sv->length) {
            *err = draken_error_sentinel_fmt(
                "%s: the trim character set must be a constant, not a column", who);
            return false;
        }
        is_null = (nulls != 0u);
    }

    const auto* sa = static_cast<const DrakenStringArena*>(sv->data);
    const DrakenStringSlot* slot = &sa->slots[0];
    out->bytes   = str_data(slot, sa->arena);
    out->nbytes  = str_length(slot);
    out->is_null = is_null;
    out->all_ascii = true;
    for (uint32_t i = 0u; i < out->nbytes; ++i)
        if (out->bytes[i] >= 0x80u) { out->all_ascii = false; break; }
    return true;
}

// Split a validated-UTF-8 trim set into per-character span offsets (ncp + 1 of
// them). Returns false and formats the error when the set is not valid UTF-8 —
// which is a CONSTANT, so this failure is a property of the query, not the data.
bool trim_set_spans(const TrimSetArg& set, const char* who,
                    std::vector<uint32_t>* spans, VecResult* err) {
    uint32_t i = 0u;
    while (i < set.nbytes) {
        const uint32_t n = utf8_seq_len(set.bytes[i]);
        if (n == 0u || i + n > set.nbytes) {
            *err = draken_error_sentinel_fmt(
                "%s: the trim character set is not valid UTF-8", who);
            return false;
        }
        for (uint32_t c = 1u; c < n; ++c) {
            if ((set.bytes[i + c] & 0xC0u) != 0x80u) {
                *err = draken_error_sentinel_fmt(
                    "%s: the trim character set is not valid UTF-8", who);
                return false;
            }
        }
        spans->push_back(i);
        i += n;
    }
    spans->push_back(set.nbytes);
    return true;
}

// Free a FULLY-FORMED result (post-kernel_preserve_shape), for the error paths
// that have to abandon one. Mirrors vecresult_to_owner's ownership contract:
// selection only when owned, validity only when it is a separate allocation.
inline void trim_dispose(VecResult& r) {
    if (r.owns_selection && r.selection != nullptr) draken_free((void*)r.selection);
    if (r.validity != nullptr && r.validity_embedded == 0u) draken_free(r.validity);
    if (r.data != nullptr) draken_free(r.data);
}

// TRIM(BOTH <null> FROM s) is NULL for every row. Built as the identity copy —
// which already carries the input's shape — with every logical row nulled through
// it. The copy is wasted work for an all-NULL answer, and deliberately so: this
// needs an explicitly typed NULL constant to reach at all (an untyped NULL scores
// _INF in overload resolution and is refused at plan time, catalog.pyx), so the
// path is correct-and-plain rather than fast.
VecResult trim_all_null(const DrakenVector* v, const char* who) {
    VecResult r = trim_kernel(v, /*left=*/false, /*right=*/false, EmptyMatcher{}, who);
    if (r.data == nullptr) return r;
    // `bad` is indexed by PHYSICAL value for a shape-preserving result; every
    // physical value is bad, so every logical row referencing one is nulled.
    const size_t nbad = v->data_length > 0u ? v->data_length : 1u;
    uint8_t* bad = static_cast<uint8_t*>(draken_malloc(nbad));
    if (bad == nullptr) {
        trim_dispose(r);
        return draken_error_sentinel_fmt("%s: allocation failed", who);
    }
    std::memset(bad, 1, nbad);
    try {
        kernel_null_bad_rows(r, v, bad);
    } catch (const std::exception&) {
        draken_free(bad);
        trim_dispose(r);
        return draken_error_sentinel_fmt("%s: allocation failed", who);
    }
    draken_free(bad);
    return r;
}

// The shared body of all three entry points: arity split, family-directed scan
// choice, dispatch.
VecResult trim_entry(const DrakenVector* const* args, uint32_t nargs,
                     bool trim_left, bool trim_right, const char* who) {
    if (nargs != 1u && nargs != 2u)
        return draken_error_sentinel_fmt("%s: expected 1 or 2 arguments", who);

    const DrakenVector* v = args[0];
    if (!trim_is_string(v->type))
        return draken_error_sentinel_fmt("%s: string operand required", who);

    if (nargs == 1u)
        return trim_kernel(v, trim_left, trim_right, WhitespaceMatcher{}, who);

    TrimSetArg set;
    VecResult  err;
    if (!trim_set_from(args[1], who, &set, &err)) return err;

    if (set.is_null) return trim_all_null(v, who);
    if (set.nbytes == 0u)
        return trim_kernel(v, trim_left, trim_right, EmptyMatcher{}, who);

    // NVARCHAR with a non-ASCII set is the only case that needs the codepoint
    // scan; see the header comment for why an all-ASCII set is the same test.
    if (v->type == DRAKEN_NVARCHAR && !set.all_ascii) {
        std::vector<uint32_t> spans;
        try {
            spans.reserve(set.nbytes + 1u);
        } catch (const std::exception&) {
            return draken_error_sentinel_fmt("%s: allocation failed", who);
        }
        if (!trim_set_spans(set, who, &spans, &err)) return err;
        CodepointMatcher m{set.bytes, spans.data(),
                           static_cast<uint32_t>(spans.size() - 1u)};
        return trim_kernel(v, trim_left, trim_right, m, who);
    }

    uint64_t bits[4] = {0ull, 0ull, 0ull, 0ull};
    for (uint32_t i = 0u; i < set.nbytes; ++i) {
        const uint8_t c = set.bytes[i];
        bits[c >> 6] |= (1ull << (c & 63u));
    }
    ByteMatcher m{bits};
    return trim_kernel(v, trim_left, trim_right, m, who);
}

}  // namespace

extern "C" {

VecResult draken_trim(void* /*ctx*/, const DrakenVector* const* args, uint32_t nargs) {
    return trim_entry(args, nargs, /*left=*/true, /*right=*/true, "draken_trim");
}

VecResult draken_ltrim(void* /*ctx*/, const DrakenVector* const* args, uint32_t nargs) {
    return trim_entry(args, nargs, /*left=*/true, /*right=*/false, "draken_ltrim");
}

VecResult draken_rtrim(void* /*ctx*/, const DrakenVector* const* args, uint32_t nargs) {
    return trim_entry(args, nargs, /*left=*/false, /*right=*/true, "draken_rtrim");
}

}  // extern "C"
