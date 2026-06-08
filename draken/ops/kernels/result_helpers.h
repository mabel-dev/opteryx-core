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

// Direct-write variant for kernels whose output size is known BEFORE formatting
// (fixed-width casts: date/timestamp/bool, or any kernel that sized its arena in a
// prior pass). Allocates the single consolidated block up front and hands back
// pointers to write slots / arena / validity IN PLACE — skipping the
// separate-buffers-then-consolidate copy (the n*16 slot memcpy) that
// vecresult_from_string_buffers performs.
//
// Usage:
//   DrakenStringSlot* slots; uint8_t* arena; uint8_t* validity;
//   uint8_t* block = vecresult_string_block_alloc(n, arena_len, want_validity,
//                                                 &slots, &arena, &validity);
//   if (!block) return draken_error_sentinel("Allocation failed");
//   // ... write slots[i], arena bytes, and (if want_validity) the bitmap ...
//   return vecresult_from_string_block(block, n, arena_len, want_validity, type);
//
//   arena_len      — EXACT long-form byte count (0 when every slot is inline).
//   want_validity  — non-zero reserves an embedded null bitmap for the caller to fill.
//   out_arena      — null when arena_len == 0; out_validity — null when !want_validity.
// Block is draken_malloc'd and zeroed. Returns nullptr on OOM.
uint8_t* vecresult_string_block_alloc(
    uint32_t           length,
    size_t             arena_len,
    int                want_validity,
    DrakenStringSlot** out_slots,
    uint8_t**          out_arena,
    uint8_t**          out_validity);

// Finalize a vecresult_string_block_alloc block into a string VecResult. arena_len
// and has_validity MUST match the values passed to the alloc call (same layout).
VecResult vecresult_from_string_block(
    uint8_t*    block,
    uint32_t    length,
    size_t      arena_len,
    int         has_validity,
    DrakenType  type);

#ifdef __cplusplus
}  // extern "C"

// ---------------------------------------------------------------------------
// C++-only inline helpers shared by the cast/function kernel .cpp files.
// Kept out of the extern "C" block (they may throw; callers run inside
// DRAKEN_KERNEL_TRY, which converts std::exception to an error sentinel).
// ---------------------------------------------------------------------------
#include "core/alloc.h"
#include "core/vector_alloc.h"   // draken_identity_sel / draken_zero_sel
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

// Compression-preserving finalizer. A compression-aware cast produces
// in->data_length PHYSICAL cast values in r.data (one per dictionary entry, 1 for
// a constant, or `length` for dense); this carries the INPUT's selection and
// per-logical-row validity onto the result so the output keeps the input's
// encoding (dense stays dense, constant stays constant, dict stays dict). The
// input's selection points at the immortal global identity/zero arrays for
// dense/constant (shared, not owned) — only dict codes are copied.
//
// The caller sets r.data, r.type, r.ts_unit, r.validity_embedded before calling;
// this sets length / data_length / flags / selection / owns_selection / validity.
// ONLY valid for non-null-introducing casts (validity preserved 1:1). Throws
// std::bad_alloc on failure (caught by DRAKEN_KERNEL_TRY).
static inline void kernel_preserve_shape(VecResult& r, const DrakenVector* in) {
    r.length      = in->length;
    r.data_length = in->data_length;
    r.flags       = in->flags;
    r.validity    = kernel_copy_validity(in);
    if (in->flags & DRAKEN_SEL_IDENTITY) {
        r.selection      = draken_identity_sel(in->length);   // dense: global identity
        r.owns_selection = false;
    } else if (in->data_length == 1u) {
        r.selection      = draken_zero_sel(in->length);       // constant: global zero
        r.owns_selection = false;
    } else {
        const size_t cn = (size_t)(in->length > 0u ? in->length : 1u);
        uint32_t* codes = static_cast<uint32_t*>(draken_malloc(cn * sizeof(uint32_t)));
        if (!codes) throw std::bad_alloc();
        std::memcpy(codes, in->selection, (size_t)in->length * sizeof(uint32_t));
        r.selection      = codes;                              // dict: copy owned codes
        r.owns_selection = true;
    }
}
#endif
