// draken/ops/kernels/string_pad.cpp — Phase 9a-fn: LPAD / RPAD string kernels on
// the C ABI. Signature is the design's func_fn_t:
//
//     VecResult fn(void* ctx, const DrakenVector* const* args, uint32_t nargs)
//
// Dispatched DIRECTLY from the nogil DV* VM (evaluation.pyx's BC_FUNCTION C-native
// arm) — no Python, no nanobind, no GIL. Three operands are pushed:
//
//     args[0] = string   (dense / constant / dict — shape preserved)
//     args[1] = width    (integer scalar — read from logical row 0)
//     args[2] = fill     (string scalar  — read from logical row 0)
//
// SEMANTICS (Postgres / DuckDB LPAD/RPAD):
//   LPAD(str, width, fill): if str is longer than `width` units it is truncated to
//   its leftmost `width` units; otherwise `fill` is tiled on the LEFT to bring the
//   result up to `width` units. RPAD is identical but tiles on the RIGHT. Truncation
//   always keeps the leftmost `width` units for both. width <= 0 → empty string.
//   Empty fill with padding needed → the (possibly truncated) input unchanged (SQL:
//   nothing to pad with). A NULL string ROW stays NULL (carried by preserve_shape).
//
//   Width/truncation/tiling are measured in BYTES for VARCHAR/VARBINARY and in
//   CODEPOINTS for NVARCHAR — a multibyte sequence is never split, neither when
//   truncating nor when a partial fill instance lands at the tail.
//
// SHAPE-PRESERVING (the string-CAST / substring / trim pattern): with width and fill
// fixed scalars, the padded value is a pure function of a physical value's bytes, so
// it is computed ONCE per data_length PHYSICAL unique value, then
// kernel_preserve_shape carries the input's selection + per-logical-row validity onto
// the result. Dense stays dense, constant stays constant, dict stays dict. Output
// length varies per value, so the arena is sized over the K outputs (two-pass, like
// draken_substring).

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

inline bool pad_is_string(DrakenType t) {
    return t == DRAKEN_VARCHAR || t == DRAKEN_NVARCHAR || t == DRAKEN_VARBINARY;
}

inline bool pad_row_valid(const DrakenVector* v, uint32_t row) {
    return v->validity == nullptr || ((v->validity[row >> 3] >> (row & 7u)) & 1u);
}

// UTF-8 codepoint count of a byte run (continuation bytes 10xxxxxx excluded).
inline uint32_t pad_cp_count(const uint8_t* d, uint32_t blen) {
    uint32_t c = 0;
    for (uint32_t b = 0; b < blen; ++b)
        if ((d[b] & 0xC0u) != 0x80u) ++c;
    return c;
}

// Byte offset of the `cp`-th codepoint (0-indexed count of codepoints from the
// start). Returns blen when cp >= total codepoints.
inline uint32_t pad_cp_to_byte(const uint8_t* d, uint32_t blen, uint32_t cp) {
    uint32_t bpos = 0, seen = 0;
    while (bpos < blen && seen < cp) {
        ++bpos;
        while (bpos < blen && (d[bpos] & 0xC0u) == 0x80u) ++bpos;
        ++seen;
    }
    return bpos;
}

// Per-value plan: how many leading src bytes to copy, and how many fill units (and
// bytes) to tile. Size and emit are derived from the SAME plan so they never drift.
struct PadPlan {
    uint32_t src_bytes;   // leading bytes of src to keep (after any truncation)
    uint32_t pad_units;   // units of fill to tile (0 when none)
    size_t   pad_bytes;   // total bytes those pad units occupy
};

// fill_units / flen describe the (fixed) scalar fill string.
inline PadPlan pad_plan_for(const uint8_t* src, uint32_t blen, bool is_utf8,
                            const uint8_t* fill, uint32_t flen, uint32_t fill_units,
                            int64_t width) {
    PadPlan p{0u, 0u, 0u};
    const int64_t target = width < 0 ? 0 : width;
    const uint32_t unit_len = is_utf8 ? pad_cp_count(src, blen) : blen;

    if (static_cast<int64_t>(unit_len) >= target) {
        // Longer-or-equal → truncate to the leftmost `target` units.
        const uint32_t t = static_cast<uint32_t>(target);
        p.src_bytes = is_utf8 ? pad_cp_to_byte(src, blen, t)
                              : (t < blen ? t : blen);
        return p;
    }

    // Shorter → pad. Empty fill cannot contribute, so the value is left as-is.
    p.src_bytes = blen;
    if (fill_units == 0u) return p;

    const uint64_t need = static_cast<uint64_t>(target) - unit_len;
    p.pad_units = static_cast<uint32_t>(need);
    const uint64_t full = need / fill_units;
    const uint32_t rem  = static_cast<uint32_t>(need % fill_units);
    size_t pb = static_cast<size_t>(full) * flen;
    pb += is_utf8 ? pad_cp_to_byte(fill, flen, rem) : rem;
    p.pad_bytes = pb;
    return p;
}

// Tile `pad_units` units of fill into out (whole fill copies plus a codepoint-aligned
// partial tail). Returns bytes written — matches PadPlan.pad_bytes by construction.
inline size_t pad_emit_fill(uint8_t* out, bool is_utf8,
                            const uint8_t* fill, uint32_t flen, uint32_t fill_units,
                            uint32_t pad_units) {
    // Nothing to tile — and a zero-unit fill would make `left >= fill_units` an
    // infinite `0 >= 0` loop, so bail before the loop (the plan already forces
    // pad_units == 0 whenever fill_units == 0, this is the belt-and-braces guard).
    if (pad_units == 0u || fill_units == 0u) return 0;
    size_t pos = 0;
    uint32_t left = pad_units;
    while (left >= fill_units) {
        std::memcpy(out + pos, fill, flen);
        pos += flen;
        left -= fill_units;
    }
    if (left > 0u) {
        const uint32_t pb = is_utf8 ? pad_cp_to_byte(fill, flen, left) : left;
        std::memcpy(out + pos, fill, pb);
        pos += pb;
    }
    return pos;
}

// Emit the whole padded value (src + tiled fill, ordered by is_lpad) into out.
inline void pad_emit(uint8_t* out, const PadPlan& p, bool is_lpad, bool is_utf8,
                     const uint8_t* src, const uint8_t* fill, uint32_t flen,
                     uint32_t fill_units) {
    size_t pos = 0;
    if (is_lpad) {
        pos += pad_emit_fill(out + pos, is_utf8, fill, flen, fill_units, p.pad_units);
        std::memcpy(out + pos, src, p.src_bytes);
    } else {
        std::memcpy(out + pos, src, p.src_bytes);
        pos += p.src_bytes;
        pad_emit_fill(out + pos, is_utf8, fill, flen, fill_units, p.pad_units);
    }
}

VecResult pad_kernel(const DrakenVector* const* args, uint32_t nargs, bool is_lpad,
                     const char* who) {
    if (nargs != 3)
        return draken_error_sentinel_fmt("%s: expected 3 arguments (string, width, fill)", who);

    const DrakenVector* v  = args[0];
    const DrakenVector* wv = args[1];
    const DrakenVector* fv = args[2];

    if (!pad_is_string(v->type))
        return draken_error_sentinel_fmt("%s: string operand required", who);
    if (!pad_is_string(fv->type))
        return draken_error_sentinel_fmt("%s: string fill required", who);

    const bool is_utf8 = (v->type == DRAKEN_NVARCHAR);
    const auto* sa = static_cast<const DrakenStringArena*>(v->data);
    const uint32_t k = v->data_length;   // physical unique count

    // Empty morsel: no scalars to read (their selection[0] would be out of range),
    // just carry the empty shape through.
    if (v->length == 0u) {
        DrakenStringSlot* s0; uint8_t* a0; uint8_t* vu0;
        uint8_t* blk = vecresult_string_block_alloc(0u, 0u, /*want_validity=*/0, &s0, &a0, &vu0);
        if (blk == nullptr) return draken_error_sentinel_fmt("%s: allocation failed", who);
        VecResult r0 = vecresult_from_string_block(blk, 0u, 0u, /*has_validity=*/0, v->type);
        try { kernel_preserve_shape(r0, v); }
        catch (const std::exception&) { draken_free(blk); return draken_error_sentinel_fmt("%s: shape-carry allocation failed", who); }
        return r0;
    }

    // Scalar width — logical row 0 of a constant-shaped integer operand. A NULL
    // scalar width/fill is unsupported (fail loud, never a wrong answer): literal
    // NULLs fold to NULL before reaching a kernel, so this is effectively unreachable.
    if (!pad_row_valid(wv, 0u))
        return draken_error_sentinel_fmt("%s: NULL width is not supported natively", who);
    const uint32_t wphys = wv->selection[0];
    int64_t width;
    switch (wv->type) {
        case DRAKEN_INT8:   width = static_cast<const int8_t*>(wv->data)[wphys]; break;
        case DRAKEN_INT16:  width = static_cast<const int16_t*>(wv->data)[wphys]; break;
        case DRAKEN_INT32:  width = static_cast<const int32_t*>(wv->data)[wphys]; break;
        case DRAKEN_INT64:  width = static_cast<const int64_t*>(wv->data)[wphys]; break;
        case DRAKEN_UINT8:  width = static_cast<const uint8_t*>(wv->data)[wphys]; break;
        case DRAKEN_UINT16: width = static_cast<const uint16_t*>(wv->data)[wphys]; break;
        case DRAKEN_UINT32: width = static_cast<const uint32_t*>(wv->data)[wphys]; break;
        case DRAKEN_UINT64: width = static_cast<int64_t>(static_cast<const uint64_t*>(wv->data)[wphys]); break;
        default:
            return draken_error_sentinel_fmt("%s: integer width operand required", who);
    }

    // Scalar fill — logical row 0 of a constant-shaped string operand.
    if (!pad_row_valid(fv, 0u))
        return draken_error_sentinel_fmt("%s: NULL fill is not supported natively", who);
    const auto* fsa = static_cast<const DrakenStringArena*>(fv->data);
    const DrakenStringSlot* fslot = &fsa->slots[fv->selection[0]];
    const uint8_t* fill = str_data(fslot, fsa->arena);
    const uint32_t flen = str_length(fslot);
    const uint32_t fill_units = is_utf8 ? pad_cp_count(fill, flen) : flen;

    // Pass 1: total long-form bytes over the K physical padded outputs. Reject any
    // single value that would exceed the 4 GB per-vector string slot limit.
    size_t arena_len = 0;
    for (uint32_t j = 0; j < k; ++j) {
        const DrakenStringSlot* slot = &sa->slots[j];
        PadPlan p = pad_plan_for(str_data(slot, sa->arena), str_length(slot), is_utf8,
                                 fill, flen, fill_units, width);
        const size_t total = static_cast<size_t>(p.src_bytes) + p.pad_bytes;
        if (total > 0xFFFFFFFFu)
            return draken_error_sentinel_fmt("%s: result exceeds 4 GB slot limit", who);
        if (total > STR_INLINE_MAX) arena_len += total;
    }

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
        const uint8_t* src = str_data(slot, sa->arena);
        PadPlan p = pad_plan_for(src, str_length(slot), is_utf8,
                                 fill, flen, fill_units, width);
        const uint32_t total = static_cast<uint32_t>(p.src_bytes + p.pad_bytes);
        if (total <= STR_INLINE_MAX) {
            pad_emit(buf_inline, p, is_lpad, is_utf8, src, fill, flen, fill_units);
            str_init_inline(&slots[j], buf_inline, total);
        } else {
            uint8_t* dst = arena + arena_pos;
            pad_emit(dst, p, is_lpad, is_utf8, src, fill, flen, fill_units);
            str_init_extern(&slots[j], dst, total,
                            static_cast<uint32_t>(arena_pos));
            arena_pos += total;
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

VecResult draken_lpad(void* /*ctx*/, const DrakenVector* const* args, uint32_t nargs) {
    return pad_kernel(args, nargs, /*is_lpad=*/true, "draken_lpad");
}

VecResult draken_rpad(void* /*ctx*/, const DrakenVector* const* args, uint32_t nargs) {
    return pad_kernel(args, nargs, /*is_lpad=*/false, "draken_rpad");
}

}  // extern "C"
