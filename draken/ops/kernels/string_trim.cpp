// draken/ops/kernels/string_trim.cpp — Phase 9a-fn: TRIM / LTRIM / RTRIM string
// kernels on the C ABI. Signature is the design's func_fn_t:
//
//     VecResult fn(void* ctx, const DrakenVector* const* args, uint32_t nargs)
//
// Dispatched DIRECTLY from the nogil DV* VM (evaluation.pyx's BC_FUNCTION C-native
// arm) — no Python, no nanobind, no GIL. TRIM strips both ends, LTRIM leading,
// RTRIM trailing. The characters stripped are ASCII whitespace (0x09-0x0D + 0x20).
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
// SHAPE-PRESERVING (the string-CAST / substring pattern, function_kernels.cpp):
// the trimmed range is a pure function of a physical value's bytes, so it is
// computed ONCE per data_length PHYSICAL unique value, then kernel_preserve_shape
// carries the input's selection + per-logical-row validity onto the result. Dense
// stays dense, constant stays constant, dict stays dict. Trim shortens lengths per
// value, so the arena is sized over the K outputs (two-pass, like draken_substring).

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

inline bool trim_is_ascii_whitespace(uint8_t c) {
    return c == 0x20u || (c >= 0x09u && c <= 0x0Du);
}

inline bool trim_is_string(DrakenType t) {
    return t == DRAKEN_VARCHAR || t == DRAKEN_NVARCHAR || t == DRAKEN_VARBINARY;
}

// Compute the trimmed byte window [off, off+len) of one slot's bytes.
inline void trim_range(const uint8_t* data, uint32_t blen,
                       bool trim_left, bool trim_right,
                       uint32_t* out_off, uint32_t* out_len) {
    uint32_t start = 0u;
    uint32_t end   = blen;
    if (trim_left) {
        while (start < end && trim_is_ascii_whitespace(data[start])) ++start;
    }
    if (trim_right) {
        while (end > start && trim_is_ascii_whitespace(data[end - 1u])) --end;
    }
    *out_off = start;
    *out_len = end - start;
}

VecResult trim_kernel(const DrakenVector* v, bool trim_left, bool trim_right, const char* who) {
    if (!trim_is_string(v->type))
        return draken_error_sentinel_fmt("%s: string operand required", who);

    const auto* sa = static_cast<const DrakenStringArena*>(v->data);
    const uint32_t k = v->data_length;   // physical unique count

    // Pass 1: total long-form bytes over the K physical trimmed outputs.
    size_t arena_len = 0;
    for (uint32_t j = 0; j < k; ++j) {
        const DrakenStringSlot* slot = &sa->slots[j];
        uint32_t off, len;
        trim_range(str_data(slot, sa->arena), str_length(slot),
                   trim_left, trim_right, &off, &len);
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
        trim_range(src, str_length(slot), trim_left, trim_right, &off, &len);
        const uint8_t* sub = src + off;
        if (len <= STR_INLINE_MAX) {
            str_init_inline(&slots[j], sub, len);
        } else {
            uint8_t* dst = arena + arena_pos;
            std::memcpy(dst, sub, len);
            str_init_extern(&slots[j], dst, len,
                            static_cast<uint32_t>(XXH3_64bits(dst, len)),
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

}  // namespace

extern "C" {

VecResult draken_trim(void* /*ctx*/, const DrakenVector* const* args, uint32_t nargs) {
    if (nargs != 1) return draken_error_sentinel("draken_trim: expected 1 argument");
    return trim_kernel(args[0], /*left=*/true, /*right=*/true, "draken_trim");
}

VecResult draken_ltrim(void* /*ctx*/, const DrakenVector* const* args, uint32_t nargs) {
    if (nargs != 1) return draken_error_sentinel("draken_ltrim: expected 1 argument");
    return trim_kernel(args[0], /*left=*/true, /*right=*/false, "draken_ltrim");
}

VecResult draken_rtrim(void* /*ctx*/, const DrakenVector* const* args, uint32_t nargs) {
    if (nargs != 1) return draken_error_sentinel("draken_rtrim: expected 1 argument");
    return trim_kernel(args[0], /*left=*/false, /*right=*/true, "draken_rtrim");
}

}  // extern "C"
