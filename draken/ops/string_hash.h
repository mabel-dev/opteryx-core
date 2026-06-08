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
// NULL rows: row hash == NULL_HASH sentinel.
//
// ACCESS PATTERN: slots[v.selection[i]] for i in [0, v.length).
// No shape discrimination — uniform access; works for dict shape automatically.

#include <cstdint>
#include <cstdlib>
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
// Null rows: seed = NULL_HASH before mixing.
// ---------------------------------------------------------------------------
static inline void hash_string(const DrakenVector& v, uint64_t* out, uint32_t n) {
    if (n == 0) return;

    const DrakenStringArena* sa       = static_cast<const DrakenStringArena*>(v.data);
    const DrakenStringSlot*  slots    = sa->slots;
    const uint8_t*           validity = v.validity;
    const uint32_t           nd       = v.data_length;

    // Compressed-shape specialization (dict or constant): pre-compute one hash
    // per distinct slot then scatter via selection. Avoids calling XXH3_64bits
    // once per logical row for repeated codes — the dominant cost for string
    // GROUP BY on dict-encoded Parquet columns. A constant column has one value
    // to hash, so it takes this path too.
    // Morsels are bounded at 64K rows so nd <= 64K and the allocation is at
    // most 512KB. Falls through to the dense path on allocation failure.
    if (draken_is_compressed(&v)) {
        uint64_t* slot_hashes = static_cast<uint64_t*>(std::malloc(nd * sizeof(uint64_t)));
        if (slot_hashes != nullptr) {
            // Phase 1: one hash per distinct slot (nulls are per-logical-row,
            // not per-slot; handled in the scatter loop below).
            for (uint32_t k = 0; k < nd; ++k) {
                slot_hashes[k] = str_hash_seed(&slots[k], sa->arena);
            }

            // Phase 2: scatter into 1024-wide chunks, substitute NULL_HASH for
            // null rows, then mix. Output is byte-identical to the dense path.
            uint64_t scratch[1024];
            uint32_t i = 0;
            while (i < n) {
                const uint32_t block = (n - i < 1024u) ? (n - i) : 1024u;
                if (validity != nullptr) {
                    for (uint32_t j = 0; j < block; ++j) {
                        const uint32_t logical = i + j;
                        const uint64_t is_valid =
                            (validity[logical >> 3] >> (logical & 7)) & 1u;
                        scratch[j] = is_valid
                            ? slot_hashes[v.selection[logical]]
                            : (uint64_t)NULL_HASH;
                    }
                } else {
                    for (uint32_t j = 0; j < block; ++j) {
                        scratch[j] = slot_hashes[v.selection[i + j]];
                    }
                }
                simd_hash_i64(scratch, out + i, block);
                i += block;
            }
            std::free(slot_hashes);
            return;
        }
        // malloc failed: fall through to dense path
    }

    // Dense path (data_length == length) or malloc fallback: hash each logical
    // row independently via the arena.
    uint64_t scratch[1024];
    uint32_t i = 0;
    while (i < n) {
        const uint32_t block = (n - i < 1024u) ? (n - i) : 1024u;
        if (validity != nullptr) {
            for (uint32_t j = 0; j < block; ++j) {
                const uint32_t logical = i + j;
                const uint64_t is_valid =
                    (validity[logical >> 3] >> (logical & 7)) & 1u;
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
