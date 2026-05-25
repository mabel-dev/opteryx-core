#pragma once
// draken/ops/string_hash.h — string hash kernel (Milestone D.2).
//
// Row hash: one uint64_t per logical row written into out[0..n).
//
// SHORT strings (len ≤ 12 bytes):
//   The slot's raw.lo (length || first-4-bytes) and raw.hi (bytes 4-11, zero-
//   padded beyond length) contain the full string content deterministically.
//   Seed = raw.lo + raw.hi * MIX_HASH_CONSTANT; then simd_hash_i64.
//   No hash collisions beyond the final mixing step: distinct short strings
//   always produce distinct (lo, hi) pairs because zero-padding is guaranteed.
//
// LONG strings (len > 12 bytes):
//   Seed = XXH3_64bits(full arena payload, length); then simd_hash_i64.
//   This reads the arena, but hash-table consumers that treat row hashes as key
//   identity get a full-content 64-bit string hash rather than the slot hash32.
//
// NULL rows: row hash == NULL_HASH sentinel (matches draken_old convention).
//
// ACCESS PATTERN: slots[v.selection[i]] for i in [0, v.length).
// No shape discrimination — uniform access; works for dict shape automatically.

#include <cstdint>
#include <cstring>
#include <stdexcept>

#include "core/buffers.h"
#include "core/string_slot.h"
#include "simd_hash.h"   // simd_hash_i64, NULL_HASH, MIX_HASH_CONSTANT

namespace draken { namespace ops {

// ---------------------------------------------------------------------------
// str_hash_seed — fold one slot into a single uint64 seed for simd_hash_i64.
//
// Short: combines both raw 64-bit slot words; all 16 bytes are deterministic
//   (zero-padding guaranteed) so equal short strings → identical seeds.
// Long: hashes the full arena payload with XXH3_64bits. Long slots require a
//   valid arena base; inline slots ignore the arena pointer.
// ---------------------------------------------------------------------------
static inline uint64_t str_hash_seed(const DrakenStringSlot* s,
                                     const uint8_t* arena) noexcept {
    if (str_is_inline(s)) {
        // Combine both raw words. Multiplication spreads raw.hi bits across the
        // full 64-bit width; addition mixes with raw.lo.
        return s->raw.lo + s->raw.hi * MIX_HASH_CONSTANT;
    }
    return XXH3_64bits(arena + s->ext.arena_offset, s->ext.length);
}

// ---------------------------------------------------------------------------
// hash_string — dispatch-table hash kernel for DRAKEN_VARCHAR.
//
// Fills out[0..n) with one uint64_t per logical row.
// Builds seeds in ≤1024-row chunks, then calls simd_hash_i64 for mixing —
// same chunked pattern as hash_int64 so parity tests can reuse infrastructure.
// Null rows: seed = NULL_HASH before mixing (matches draken_old null sentinel).
// ---------------------------------------------------------------------------
static inline void hash_string(const DrakenVector& v, uint64_t* out, uint32_t n) {
    if (n == 0) return;

    const DrakenStringArena* sa    = static_cast<const DrakenStringArena*>(v.data);
    const DrakenStringSlot*  slots = sa->slots;
    const uint8_t*           validity = v.validity;

    uint64_t scratch[1024];
    uint32_t i = 0;

    while (i < n) {
        const uint32_t block = (n - i < 1024u) ? (n - i) : 1024u;

        if (validity != nullptr) {
            for (uint32_t j = 0; j < block; ++j) {
                const uint32_t logical = i + j;
                const uint64_t is_valid =
                    (validity[logical >> 3] >> (logical & 7)) & 1u;
                // Null path: substitute NULL_HASH directly (no slot access).
                // Valid path: exact slot seed for short; full-content hash for long.
                scratch[j] = is_valid
                    ? str_hash_seed(&slots[v.selection[logical]], sa->arena)
                    : (uint64_t)NULL_HASH;
            }
        } else {
            for (uint32_t j = 0; j < block; ++j) {
                scratch[j] = str_hash_seed(&slots[v.selection[i + j]], sa->arena);
            }
        }

        simd_hash_i64(scratch, out + i, block);
        i += block;
    }
}

}} // namespace draken::ops
