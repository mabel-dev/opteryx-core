#include "ops/kernels/result_helpers.h"
#include "ops/kernels/error_handling.h"
#include "core/alloc.h"
#include "core/vector_alloc.h"
#include "core/string_slot.h"
#include <cstring>

/**
 * Phase 9c: consolidate hand-allocated string component buffers into the single
 * draken_malloc block a string DrakenVector requires, and return it as a
 * VecResult with the null bitmap embedded in that block.
 *
 * Block layout (matches make_string_from_sequence):
 *   [ DrakenStringArena | DrakenStringSlot[length] | arena_bytes | validity ]
 *
 * Ownership: CONSUMES the three input buffers (freed after copying). The block
 * becomes VecResult.data; validity (if any) points inside the block and is
 * flagged validity_embedded so vecresult_to_owner does not free it twice.
 */

extern "C" VecResult vecresult_from_string_buffers(
    DrakenStringSlot* slots,
    uint8_t*          arena,
    size_t            arena_len,
    uint8_t*          validity,
    uint32_t          length,
    DrakenType        type)
{
    // Validate before we commit to the consolidated allocation. On any early
    // return we must still free the caller's buffers (ownership is transferred
    // on entry, per the header contract).
    if (type != DRAKEN_VARCHAR && type != DRAKEN_NVARCHAR && type != DRAKEN_VARBINARY) {
        draken_free(slots);
        draken_free(arena);
        draken_free(validity);
        return draken_error_sentinel(
            "vecresult_from_string_buffers: type must be VARCHAR/NVARCHAR/VARBINARY");
    }

    // --- Compute single-block layout -----------------------------------------
    constexpr size_t kSlotAlign = alignof(DrakenStringSlot);
    const size_t struct_end =
        (sizeof(DrakenStringArena) + kSlotAlign - 1u) & ~(kSlotAlign - 1u);
    const size_t slots_bytes  = (length > 0u ? (size_t)length : 1u) * sizeof(DrakenStringSlot);
    const size_t arena_start  = struct_end + slots_bytes;
    const size_t validity_start = arena_start + arena_len;

    size_t validity_bytes = 0u;
    if (validity) {
        const uint32_t bm     = (length + 7u) / 8u;
        const uint32_t padded = (bm + 7u) & ~7u;
        validity_bytes = padded > 0u ? padded : 8u;
    }
    const size_t total      = validity_start + validity_bytes;
    const size_t alloc_size = total > 0u ? total : sizeof(DrakenStringArena);

    uint8_t* block = static_cast<uint8_t*>(draken_malloc(alloc_size));
    if (!block) {
        draken_free(slots);
        draken_free(arena);
        draken_free(validity);
        return draken_error_sentinel("vecresult_from_string_buffers: allocation failed");
    }
    std::memset(block, 0, alloc_size);

    DrakenStringArena* sa     = reinterpret_cast<DrakenStringArena*>(block);
    DrakenStringSlot*  dslots = reinterpret_cast<DrakenStringSlot*>(block + struct_end);
    uint8_t*           darena = (arena_len > 0u) ? (block + arena_start) : nullptr;
    uint8_t*           dvalid = validity        ? (block + validity_start) : nullptr;

    // --- Copy components into the consolidated block -------------------------
    if (length > 0u && slots)
        std::memcpy(dslots, slots, (size_t)length * sizeof(DrakenStringSlot));
    if (arena_len > 0u && arena)
        std::memcpy(darena, arena, arena_len);
    if (dvalid)
        std::memcpy(dvalid, validity, validity_bytes);

    // Caller buffers consumed — free the originals now that they are copied.
    draken_free(slots);
    draken_free(arena);
    draken_free(validity);

    // --- Initialise the embedded arena struct --------------------------------
    sa->slots        = dslots;
    sa->arena        = darena;
    sa->length       = (size_t)length;
    sa->arena_used   = arena_len;
    sa->arena_cap    = arena_len;
    sa->null_bitmap  = dvalid;   // embedded; freed with the block
    sa->owns_buffers = 0;
    sa->type         = type;

    VecResult r;
    r.data              = block;
    r.validity          = dvalid;          // points inside `block`
    r.selection         = draken_identity_sel(length);
    r.owns_selection    = false;
    r.data_length       = length;
    r.length            = length;
    r.type              = type;
    r.flags             = 0;
    r.validity_embedded = dvalid ? 1u : 0u;
    r.ts_unit           = 0xFFu;
    return r;
}
