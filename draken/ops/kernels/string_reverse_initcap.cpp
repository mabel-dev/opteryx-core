// draken/ops/kernels/string_reverse_initcap.cpp — Phase 9a-fn: REVERSE / INITCAP
// string kernels on the C ABI. Signature is the design's func_fn_t:
//
//     VecResult fn(void* ctx, const DrakenVector* const* args, uint32_t nargs)
//
// Dispatched DIRECTLY from the nogil DV* VM (evaluation.pyx's BC_FUNCTION C-native
// arm) — no Python, no nanobind, no GIL. Both are UNARY, LENGTH-PRESERVING string
// transforms, so they share the exact shape-preserving template the case/trim
// kernels use (function_kernels.cpp's ascii_case_transform, string_trim.cpp).
//
// REVERSE(str): reverse a string.
//   VARCHAR / VARBINARY — reverse BYTES (VARCHAR is ASCII; VARBINARY is bytes).
//   NVARCHAR           — reverse the sequence of UTF-8 CODEPOINTS, keeping each
//                        codepoint's bytes intact (never split a multibyte run).
//   All three accepted; the input's tag is preserved; length is unchanged.
//
// INITCAP(str): title-case — the first alphabetic byte of each word uppercased,
//   the rest lowercased. A "word" is a maximal run of ASCII alphanumerics; any
//   non-alphanumeric byte is a boundary.
//   VARCHAR / VARBINARY — ASCII byte fold (exactly like draken_upper/lower).
//   NVARCHAR           — full Unicode case mapping is not implemented natively;
//                        fail loud with an error sentinel, matching draken_upper /
//                        draken_lower's NVARCHAR contract (never a wrong answer).
//
// SHAPE-PRESERVING (the string-CAST / trim pattern): the transform is a pure
// function of a physical value's bytes, so it is computed ONCE per data_length
// PHYSICAL unique value, then kernel_preserve_shape carries the input's selection
// + per-logical-row validity onto the result. Dense stays dense, constant stays
// constant, dict stays dict. Both transforms preserve length, so the arena is
// sized exactly as the input's long-form byte total over the K uniques (like
// ascii_case_transform). Validity is per-logical-row, so the K-slot physical block
// carries none; the preserved validity is the sole null authority.

#include <cstdint>
#include <cstring>

#include "core/buffers.h"
#include "core/string_slot.h"
#include "core/alloc.h"
#include "ops/vec_result.h"
#include "ops/kernels/result_helpers.h"
#include "ops/kernels/error_handling.h"
#include "xxhash.h"                // XXH3_64bits — long-slot hash32, same as every builder

namespace {

inline bool sri_is_string(DrakenType t) {
    return t == DRAKEN_VARCHAR || t == DRAKEN_NVARCHAR || t == DRAKEN_VARBINARY;
}

// UTF-8 codepoint byte-width from a lead byte (matches reverse_utf8 in the
// nanobind vector_string_case.cpp this ports). An invalid lead byte is treated
// as a lone 1-byte unit — never split further.
inline uint32_t sri_cp_len(uint8_t b) {
    if (b < 0x80u)              return 1u;
    if ((b & 0xE0u) == 0xC0u)   return 2u;
    if ((b & 0xF0u) == 0xE0u)   return 3u;
    if ((b & 0xF8u) == 0xF0u)   return 4u;
    return 1u;
}

// Byte reversal of [src, src+len) into dst (VARCHAR / VARBINARY).
inline void sri_reverse_bytes(const uint8_t* src, uint32_t len, uint8_t* dst) {
    for (uint32_t j = 0u; j < len; ++j) dst[j] = src[len - 1u - j];
}

// Codepoint reversal of [src, src+len) into dst (NVARCHAR): the SEQUENCE of
// codepoints is reversed, each codepoint's bytes copied intact. No cap and no
// scratch buffer — each codepoint is written straight to its mirrored offset
// (len - pos - cplen), which is the reverse position of a length-preserving map.
inline void sri_reverse_codepoints(const uint8_t* src, uint32_t len, uint8_t* dst) {
    uint32_t pos = 0u;
    while (pos < len) {
        uint32_t cplen = sri_cp_len(src[pos]);
        if (pos + cplen > len) cplen = len - pos;   // truncated tail: keep as-is
        std::memcpy(dst + (len - pos - cplen), src + pos, cplen);
        pos += cplen;
    }
}

// ASCII initcap fold of [src, src+len) into dst (same length; VARCHAR/VARBINARY).
// Word boundary = any non-alphanumeric byte; digits are word-interior (so the
// first LETTER after a digit is not re-capitalised) — ported verbatim from the
// nanobind impl_initcap this replaces.
inline void sri_initcap_ascii(const uint8_t* src, uint32_t len, uint8_t* dst) {
    bool new_word = true;
    for (uint32_t j = 0u; j < len; ++j) {
        const uint8_t c = src[j];
        const bool alnum = (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
                           (c >= '0' && c <= '9');
        if (alnum) {
            if (new_word && c >= 'a' && c <= 'z')       dst[j] = static_cast<uint8_t>(c - 32u);
            else if (!new_word && c >= 'A' && c <= 'Z') dst[j] = static_cast<uint8_t>(c + 32u);
            else                                        dst[j] = c;
            new_word = false;
        } else {
            dst[j] = c;
            new_word = true;
        }
    }
}

// Shared length-preserving, shape-preserving driver. `fn(src, len, dst)` writes
// exactly `len` transformed bytes into dst (see ascii_case_transform / trim_kernel
// for the identity-replacement and bad_alloc-containment rationale).
template <typename Fn>
VecResult sri_transform(const DrakenVector* v, DrakenType out_type, const char* who, Fn fn) {
    const auto* sa = static_cast<const DrakenStringArena*>(v->data);
    const uint32_t k = v->data_length;   // physical unique count

    // Pass 1: exact long-form byte total over the K physical values (lengths are
    // unchanged by either transform).
    size_t arena_len = 0;
    for (uint32_t j = 0; j < k; ++j) {
        uint32_t len = str_length(&sa->slots[j]);
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

    uint8_t buf_inline[STR_INLINE_MAX];
    size_t arena_pos = 0;
    for (uint32_t j = 0; j < k; ++j) {
        const DrakenStringSlot* slot = &sa->slots[j];
        uint32_t len = str_length(slot);
        const uint8_t* src = str_data(slot, sa->arena);
        if (len <= STR_INLINE_MAX) {
            fn(src, len, buf_inline);
            str_init_inline(&slots[j], buf_inline, len);
        } else {
            uint8_t* dst = arena + arena_pos;
            fn(src, len, dst);
            str_init_extern(&slots[j], dst, len,
                            static_cast<uint32_t>(XXH3_64bits(dst, len)),
                            static_cast<uint32_t>(arena_pos));
            arena_pos += len;
        }
    }

    VecResult r = vecresult_from_string_block(block, k, arena_len, /*has_validity=*/0, out_type);
    try {
        kernel_preserve_shape(r, v);
    } catch (const std::exception&) {
        draken_free(block);
        return draken_error_sentinel_fmt("%s: shape-carry allocation failed", who);
    }
    return r;
}

}  // namespace

extern "C" {

VecResult draken_reverse(void* /*ctx*/, const DrakenVector* const* args, uint32_t nargs) {
    if (nargs != 1) return draken_error_sentinel("draken_reverse: expected 1 argument");
    const DrakenVector* v = args[0];
    if (!sri_is_string(v->type))
        return draken_error_sentinel("draken_reverse: string operand required");
    if (v->type == DRAKEN_NVARCHAR) {
        return sri_transform(v, v->type, "draken_reverse",
            [](const uint8_t* s, uint32_t l, uint8_t* d) { sri_reverse_codepoints(s, l, d); });
    }
    return sri_transform(v, v->type, "draken_reverse",
        [](const uint8_t* s, uint32_t l, uint8_t* d) { sri_reverse_bytes(s, l, d); });
}

VecResult draken_initcap(void* /*ctx*/, const DrakenVector* const* args, uint32_t nargs) {
    if (nargs != 1) return draken_error_sentinel("draken_initcap: expected 1 argument");
    const DrakenVector* v = args[0];
    if (v->type != DRAKEN_VARCHAR && v->type != DRAKEN_VARBINARY) {
        return draken_error_sentinel(
            "draken_initcap: VARCHAR or VARBINARY (ASCII fold) input required — "
            "NVARCHAR case mapping is not implemented natively yet (fail loud, "
            "never wrong)");
    }
    return sri_transform(v, v->type, "draken_initcap",
        [](const uint8_t* s, uint32_t l, uint8_t* d) { sri_initcap_ascii(s, l, d); });
}

}  // extern "C"
