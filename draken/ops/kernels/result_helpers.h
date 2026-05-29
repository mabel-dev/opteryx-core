#pragma once
// draken/ops/kernels/result_helpers.h — shared VecResult constructors for kernels.
//
// Phase 9c: string-producing kernels (casts-to-VARCHAR, string functions) cannot
// hand back two independent buffers the way numeric kernels do — a string column
// stores its arena struct, slot array, long-form bytes and null bitmap in ONE
// draken_malloc block. vecresult_from_string_buffers consolidates the caller's
// separately-allocated slots / arena / validity into that single block and returns
// a VecResult flagged validity_embedded = 1, so vecresult_to_owner frees the block
// once and never double-frees the embedded bitmap.

#include "ops/vec_result.h"
#include "core/buffers.h"
#include "core/string_slot.h"

#ifdef __cplusplus
extern "C" {
#endif

// Build a string-family VecResult from hand-allocated component buffers.
//
// CONSUMES ownership of all three input buffers (draken_free'd internally after
// copying into the consolidated block) — the caller MUST NOT free them after the
// call, success or failure. All non-null buffers MUST be draken_malloc'd.
//
//   slots     — DrakenStringSlot[length], populated via draken_build_string_slot
//               (null rows zeroed via str_init_null). May be null only when length == 0.
//   arena     — long-form byte arena backing slots with len > STR_INLINE_MAX. May be
//               null when arena_len == 0.
//   arena_len — valid bytes in arena (may be 0).
//   validity  — 1-bit-per-row null bitmap (Arrow convention: bit set = valid), or null
//               when all rows are valid.
//   length    — logical row count.
//   type      — DRAKEN_VARCHAR | DRAKEN_NVARCHAR | DRAKEN_VARBINARY.
//
// On success: VecResult with data = consolidated block, validity pointing INSIDE that
// block (validity_embedded = 1) or null, selection = global identity, type as given.
// On failure: a draken_error_sentinel (data == nullptr); inputs are still freed.
VecResult vecresult_from_string_buffers(
    DrakenStringSlot* slots,
    uint8_t*          arena,
    size_t            arena_len,
    uint8_t*          validity,
    uint32_t          length,
    DrakenType        type);

#ifdef __cplusplus
}  // extern "C"

// ---------------------------------------------------------------------------
// C++-only inline helpers shared by the cast/function kernel .cpp files.
// Kept out of the extern "C" block (they may throw; callers run inside
// DRAKEN_KERNEL_TRY, which converts std::exception to an error sentinel).
// ---------------------------------------------------------------------------
#include "core/alloc.h"
#include <cstring>
#include <new>

// Logical-row null test under the unified access model. validity is indexed by
// logical row i (bit set = valid); NULL validity means all-valid.
static inline bool kernel_row_is_null(const DrakenVector* dv, uint32_t i) noexcept {
    if (!dv->validity) return false;
    return ((dv->validity[i >> 3] >> (i & 7u)) & 1u) == 0u;
}

// Copy a logical-row-indexed validity bitmap into a fresh draken_malloc buffer.
// Returns nullptr when dv has no validity (all-valid). Throws std::bad_alloc on
// allocation failure (caught by DRAKEN_KERNEL_TRY → error sentinel). Dense output
// preserves logical-row order, so the input bitmap maps 1:1 to the output.
static inline uint8_t* kernel_copy_validity(const DrakenVector* dv) {
    if (!dv->validity) return nullptr;
    const uint32_t bm     = (dv->length + 7u) >> 3;
    const uint32_t padded = (bm + 7u) & ~7u;
    const size_t   vbytes = padded > 0u ? padded : 8u;
    uint8_t* out = static_cast<uint8_t*>(draken_malloc(vbytes));
    if (!out) throw std::bad_alloc();
    std::memcpy(out, dv->validity, vbytes);
    return out;
}
#endif
