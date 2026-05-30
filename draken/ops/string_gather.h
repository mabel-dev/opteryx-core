#pragma once
// draken/ops/string_gather.h — take / materialize / compress for DRAKEN_VARCHAR.
//
// All three ops produce owned, self-contained string vectors.
//
// MEMORY LAYOUT PER RESULT:
//   data block  = [DrakenStringArena struct | DrakenStringSlot[data_length] | arena bytes]
//                 Owned by VecResult.data / data_buf; freed as one unit.
//   validity    = SEPARATE allocation (never embedded in the data block).
//                 nullptr means all-valid (normalization invariant).
//                 Owned by VecResult.validity / validity_buf; freed independently.
//   codes       = SEPARATE allocation for dict results (VecResult.owns_selection = true).
//
// This layout is intentionally different from the D.1 single-block ingestion
// (which embeds validity).  Keeping validity separate here lets vecresult_to_owner
// free both buffers independently with no double-free risk.
//
// MATERIALIZE(v):
//   Expand any shape (dense / constant / dict) → dense owned string vector.
//   Uniform access: data[selection[i]] for i in [0, length).
//   Arena compact-copy: one copy per unique referenced long string's bytes;
//   multiple output slots with the same source code share the new arena_offset.
//   Result flags: DRAKEN_SEL_IDENTITY | DRAKEN_SEL_PERMUTATION.
//
// TAKE(v, indices, n):
//   Gather v[indices[i]] for i in [0, n).  Dense output (identity flags).
//   Null source row → null output row.  Normalization: validity == NULL when no
//   output rows are null.
//   Arena compact-copy: same per-code dedup as materialize.
//
// COMPRESS(v):
//   Dict-encode a string vector.  Unique non-null values found via sg_eq_slots
//   (exact equality; length/prefix/hash32 fast-reject before arena compare).
//   Unique slots stored in
//   first-appearance order; owned codes[length] map logical rows to unique slots.
//   All-null / empty: constant-shape result (data_length=1).
//   The stored hash32 in every unique slot is the XXH3 content hash (lower 32 bits
//   of XXH3_64bits) — identical to D.1 ingestion invariant.
//
// Round-trip: materialize(compress(dense)) produces the same logical values.
//
// ACCESS PATTERN: all loops use data[selection[i]] — no shape discrimination.

#include <cstdint>
#include <cstring>
#include <stdexcept>
#include <unordered_map>
#include <vector>

#include "core/alloc.h"
#include "core/buffers.h"
#include "core/string_slot.h"
#include "core/vector_alloc.h"
#include "ops/string_hash.h"  // str_hash_seed
#include "ops/vec_result.h"

// DrakenFree is defined in draken_native.cpp; redeclare here for the guards.
struct DrakenFreeLocal {
    void operator()(void* p) const noexcept { draken_free(p); }
};
template <typename T>
using SgOwned = std::unique_ptr<T, DrakenFreeLocal>;

namespace draken {
namespace ops {

// ---------------------------------------------------------------------------
// Validity helpers
// ---------------------------------------------------------------------------

static inline bool sg_val_row(const uint8_t* validity, uint32_t i) noexcept {
    return (validity == nullptr) || ((validity[i >> 3] >> (i & 7)) & 1u);
}

// Allocate a zeroed, SIMD-padded validity bitmap for n logical rows.
static inline uint8_t* sg_alloc_validity(uint32_t n) {
    const uint32_t nb     = (n + 7u) >> 3;
    const uint32_t padded = ((nb + 7u) & ~7u);
    const size_t   bytes  = padded > 0u ? padded : 8u;
    uint8_t* p = static_cast<uint8_t*>(draken_malloc(bytes));
    if (!p) throw std::bad_alloc();
    std::memset(p, 0, bytes);
    return p;
}

// Copy src validity for n logical rows into a new SIMD-padded buffer.
// Returns nullptr if src is nullptr (all-valid pass-through).
static inline uint8_t* sg_copy_validity(const uint8_t* src, uint32_t n) {
    if (src == nullptr) return nullptr;
    const uint32_t nb     = (n + 7u) >> 3;
    const uint32_t padded = ((nb + 7u) & ~7u);
    const size_t   bytes  = padded > 0u ? padded : 8u;
    uint8_t* p = static_cast<uint8_t*>(draken_malloc(bytes));
    if (!p) throw std::bad_alloc();
    if (nb > 0) std::memcpy(p, src, nb);
    // Zero tail padding (bits beyond n in the last byte, plus pad bytes).
    if ((n & 7u) != 0u && nb > 0u)
        p[nb - 1] &= static_cast<uint8_t>((1u << (n & 7u)) - 1u);
    if (padded > nb) std::memset(p + nb, 0, padded - nb);
    return p;
}

// If all n bits in `validity` are set, free the buffer and return nullptr.
// Otherwise return it unchanged.  noexcept — safe to call after releasing guards.
static inline uint8_t* sg_normalize_validity(uint8_t* validity,
                                             uint32_t n) noexcept {
    if (validity == nullptr || n == 0) {
        if (validity) draken_free(validity);
        return nullptr;
    }
    const uint32_t nb = (n + 7u) >> 3;
    for (uint32_t k = 0; k < nb; ++k) {
        uint8_t expected = 0xFFu;
        if (k == nb - 1u && (n & 7u) != 0u)
            expected = static_cast<uint8_t>((1u << (n & 7u)) - 1u);
        if (validity[k] != expected) return validity;
    }
    draken_free(validity);
    return nullptr;
}

// ---------------------------------------------------------------------------
// String block: [DrakenStringArena | DrakenStringSlot[n_slots] | arena_bytes].
// Validity is NOT embedded here — it is a separate allocation.
// ---------------------------------------------------------------------------

struct StrBlock {
    uint8_t*          block;        // raw allocation
    DrakenStringArena* sa;          // alias at offset 0
    DrakenStringSlot*  slots;       // alias after sa (aligned)
    uint8_t*           arena_bytes; // alias after slots (nullptr if arena_bytes==0)
};

static inline StrBlock sg_alloc_str_block(uint32_t n_slots,
                                          size_t   arena_bytes) {
    constexpr size_t kAlign = alignof(DrakenStringSlot);
    const size_t struct_end =
        (sizeof(DrakenStringArena) + kAlign - 1u) & ~(kAlign - 1u);
    const size_t slots_sz  = static_cast<size_t>(n_slots > 0u ? n_slots : 1u)
                             * sizeof(DrakenStringSlot);
    const size_t arena_off = struct_end + slots_sz;
    const size_t total     = arena_off + arena_bytes;
    const size_t alloc     = total > 0u ? total : sizeof(DrakenStringArena);

    uint8_t* block = static_cast<uint8_t*>(draken_malloc(alloc));
    if (!block) throw std::bad_alloc();
    std::memset(block, 0, alloc);

    StrBlock sb;
    sb.block       = block;
    sb.sa          = reinterpret_cast<DrakenStringArena*>(block);
    sb.slots       = reinterpret_cast<DrakenStringSlot*>(block + struct_end);
    sb.arena_bytes = (arena_bytes > 0u) ? (block + arena_off) : nullptr;
    return sb;
}

// Build a VecResult from a completed StrBlock.
// validity must be a SEPARATE allocation (or nullptr).
static inline VecResult sg_finalize(const StrBlock& sb,
                                    uint8_t*         validity,
                                    const uint32_t*  selection,
                                    bool             owns_sel,
                                    uint32_t         data_length,
                                    uint32_t         length,
                                    size_t           arena_used,
                                    uint8_t          flags) {
    sb.sa->slots        = sb.slots;
    sb.sa->arena        = sb.arena_bytes;
    sb.sa->length       = data_length;
    sb.sa->arena_used   = arena_used;
    sb.sa->arena_cap    = arena_used;
    sb.sa->null_bitmap  = validity;  // for consistency; C++ ops use v.validity
    sb.sa->owns_buffers = 0;
    sb.sa->type         = DRAKEN_VARCHAR;

    VecResult r;
    r.data           = sb.block;
    r.validity       = validity;
    r.selection      = selection;
    r.owns_selection = owns_sel;
    r.data_length    = data_length;
    r.length         = length;
    r.type           = DRAKEN_VARCHAR;
    r.flags          = flags;
    return r;
}

// ---------------------------------------------------------------------------
// sg_eq_slots — exact equality for two slots.
//
// Duplicated inline from string_compare.h to avoid the large include.
// Short (≤12): exact — raw.lo and raw.hi cover all content.
// Long  (>12): length + prefix + hash32 fast-reject, then arena byte compare.
// Must match str_eq_slots semantics exactly (runtime equality uses the same rule).
// ---------------------------------------------------------------------------
static inline int sg_eq_slots(const DrakenStringSlot* a,
                              const uint8_t* arena_a,
                              const DrakenStringSlot* b,
                              const uint8_t* arena_b) noexcept {
    if (a->raw.lo != b->raw.lo) return 0;
    if (str_is_inline(a)) return a->raw.hi == b->raw.hi;
    if (a->ext.hash32 != b->ext.hash32) return 0;
    return std::memcmp(arena_a + a->ext.arena_offset,
                       arena_b + b->ext.arena_offset,
                       a->ext.length) == 0;
}

// ---------------------------------------------------------------------------
// MATERIALIZE — expand any shape → owned dense string vector.
//
// Compact arena: each unique source slot's long bytes are copied once.
// Multiple output slots with the same code share the resulting arena_offset.
// ---------------------------------------------------------------------------
static inline VecResult str_materialize(const DrakenVector& v) {
    const uint32_t          n        = v.length;
    const DrakenStringArena* sa      = static_cast<const DrakenStringArena*>(v.data);
    const DrakenStringSlot*  src_s   = sa->slots;
    const uint8_t*           src_a   = sa->arena;
    const uint8_t*           src_v   = v.validity;

    // Phase 1: assign new arena offsets per unique source slot (indexed by code).
    // data_length == number of unique slots in the source.
    std::vector<uint32_t> new_off(v.data_length, 0u);
    size_t total_arena = 0u;
    for (uint32_t k = 0; k < v.data_length; ++k) {
        if (!str_is_inline(&src_s[k])) {
            new_off[k] = static_cast<uint32_t>(total_arena);
            total_arena += src_s[k].ext.length;
        }
    }
    if (total_arena > static_cast<size_t>(UINT32_MAX))
        throw std::overflow_error("str_materialize: arena exceeds 4 GB");

    // Phase 2: allocate output block.
    StrBlock sb = sg_alloc_str_block(n, total_arena);
    SgOwned<void> bg(sb.block);  // frees block if validity alloc throws

    // Phase 3: copy arena bytes for each unique long slot.
    for (uint32_t k = 0; k < v.data_length; ++k) {
        if (!str_is_inline(&src_s[k]) && sb.arena_bytes != nullptr) {
            std::memcpy(sb.arena_bytes + new_off[k],
                        src_a + src_s[k].ext.arena_offset,
                        src_s[k].ext.length);
        }
    }

    // Phase 4: fill output slots using uniform access data[selection[i]].
    for (uint32_t i = 0; i < n; ++i) {
        const uint32_t         code = v.selection[i];
        const DrakenStringSlot* src = &src_s[code];
        if (str_is_inline(src)) {
            sb.slots[i] = *src;
        } else {
            sb.slots[i].ext.length       = src->ext.length;
            sb.slots[i].ext.prefix       = src->ext.prefix;
            sb.slots[i].ext.hash32       = src->ext.hash32;
            sb.slots[i].ext.arena_offset = new_off[code];
        }
    }

    // Phase 5: copy validity (separate allocation; may throw — bg fires on fail).
    uint8_t* out_v = sg_copy_validity(src_v, n);

    bg.release();
    return sg_finalize(sb, out_v, draken_identity_sel(n), false, n, n, total_arena,
                       static_cast<uint8_t>(DRAKEN_SEL_IDENTITY |
                                            DRAKEN_SEL_PERMUTATION));
}

// ---------------------------------------------------------------------------
// SLICE — contiguous range [start, start+length). Same logic as take but
// source indices are start, start+1, ..., start+length-1 — no index array.
// ---------------------------------------------------------------------------
static inline VecResult str_slice(const DrakenVector& v, uint32_t start, uint32_t n) {
    const DrakenStringArena* sa    = static_cast<const DrakenStringArena*>(v.data);
    const DrakenStringSlot*  src_s = sa->slots;
    const uint8_t*           src_a = sa->arena;
    const uint8_t*           src_v = v.validity;

    // Phase 1: scan [start, start+n) to compute output arena size.
    std::vector<uint32_t> new_off(v.data_length, UINT32_MAX);
    size_t total_arena = 0u;

    for (uint32_t i = 0; i < n; ++i) {
        const uint32_t src_log = start + i;
        if (!sg_val_row(src_v, src_log)) continue;
        const uint32_t code = v.selection[src_log];
        if (!str_is_inline(&src_s[code]) && new_off[code] == UINT32_MAX) {
            new_off[code] = static_cast<uint32_t>(total_arena);
            total_arena  += src_s[code].ext.length;
        }
    }
    if (total_arena > static_cast<size_t>(UINT32_MAX))
        throw std::overflow_error("str_slice: arena exceeds 4 GB");

    // Phase 2: allocate output block.
    StrBlock sb = sg_alloc_str_block(n, total_arena);
    SgOwned<void> bg(sb.block);

    // Phase 3: copy arena bytes for each referenced unique long slot.
    for (uint32_t k = 0; k < v.data_length; ++k) {
        if (!str_is_inline(&src_s[k]) && new_off[k] != UINT32_MAX &&
            sb.arena_bytes != nullptr) {
            std::memcpy(sb.arena_bytes + new_off[k],
                        src_a + src_s[k].ext.arena_offset,
                        src_s[k].ext.length);
        }
    }

    // Phase 4: allocate validity.
    uint8_t* out_v = nullptr;
    SgOwned<uint8_t> vg;
    if (src_v != nullptr && n > 0u) {
        out_v = sg_alloc_validity(n);
        vg.reset(out_v);
    }

    // Phase 5: fill output slots and validity.
    bool has_nulls = false;
    for (uint32_t i = 0; i < n; ++i) {
        const uint32_t src_log = start + i;
        if (!sg_val_row(src_v, src_log)) {
            has_nulls = true;
        } else {
            const uint32_t         code = v.selection[src_log];
            const DrakenStringSlot* src = &src_s[code];
            if (str_is_inline(src)) {
                sb.slots[i] = *src;
            } else {
                sb.slots[i].ext.length       = src->ext.length;
                sb.slots[i].ext.prefix       = src->ext.prefix;
                sb.slots[i].ext.hash32       = src->ext.hash32;
                sb.slots[i].ext.arena_offset = new_off[code];
            }
            if (out_v != nullptr)
                out_v[i >> 3] |= static_cast<uint8_t>(1u << (i & 7u));
        }
    }

    vg.release();
    if (!has_nulls && out_v != nullptr) {
        draken_free(out_v);
        out_v = nullptr;
    } else {
        out_v = sg_normalize_validity(out_v, n);
    }

    bg.release();
    return sg_finalize(sb, out_v, draken_identity_sel(n), false, n, n, total_arena,
                       static_cast<uint8_t>(DRAKEN_SEL_IDENTITY | DRAKEN_SEL_PERMUTATION));
}

// ---------------------------------------------------------------------------
// TAKE — gather logical rows by index.
//
// indices[i] is a logical row position in v.  Output row i = source row
// indices[i].  Null source row → null output row.
// Compact arena: one copy per unique referenced long source slot (by code).
// ---------------------------------------------------------------------------
static inline VecResult str_take(const DrakenVector& v,
                                  const int32_t*      indices,
                                  uint32_t            n) {
    const DrakenStringArena* sa    = static_cast<const DrakenStringArena*>(v.data);
    const DrakenStringSlot*  src_s = sa->slots;
    const uint8_t*           src_a = sa->arena;
    const uint8_t*           src_v = v.validity;

    // Phase 1: scan indices to compute arena layout.
    // new_off[k] = output arena offset for source slot k (UINT32_MAX = unassigned).
    std::vector<uint32_t> new_off(v.data_length, UINT32_MAX);
    size_t total_arena = 0u;

    for (uint32_t i = 0; i < n; ++i) {
        const uint32_t src_log = static_cast<uint32_t>(indices[i]);
        if (!sg_val_row(src_v, src_log)) continue;
        const uint32_t code = v.selection[src_log];
        if (!str_is_inline(&src_s[code]) && new_off[code] == UINT32_MAX) {
            new_off[code] = static_cast<uint32_t>(total_arena);
            total_arena  += src_s[code].ext.length;
        }
    }
    if (total_arena > static_cast<size_t>(UINT32_MAX))
        throw std::overflow_error("str_take: arena exceeds 4 GB");

    // Phase 2: allocate output block.
    StrBlock sb = sg_alloc_str_block(n, total_arena);
    SgOwned<void> bg(sb.block);

    // Phase 3: copy arena bytes for each referenced unique long slot.
    for (uint32_t k = 0; k < v.data_length; ++k) {
        if (!str_is_inline(&src_s[k]) && new_off[k] != UINT32_MAX &&
            sb.arena_bytes != nullptr) {
            std::memcpy(sb.arena_bytes + new_off[k],
                        src_a + src_s[k].ext.arena_offset,
                        src_s[k].ext.length);
        }
    }

    // Phase 4: allocate validity if source has nulls (may throw — bg fires).
    uint8_t* out_v = nullptr;
    SgOwned<uint8_t> vg;
    if (src_v != nullptr && n > 0) {
        out_v = sg_alloc_validity(n);  // zeroed = all null initially
        vg.reset(out_v);
    }

    // Phase 5: fill output slots and validity.
    bool has_nulls = false;
    for (uint32_t i = 0; i < n; ++i) {
        const uint32_t src_log = static_cast<uint32_t>(indices[i]);
        if (!sg_val_row(src_v, src_log)) {
            has_nulls = true;  // slot stays zero (null)
        } else {
            const uint32_t         code = v.selection[src_log];
            const DrakenStringSlot* src = &src_s[code];
            if (str_is_inline(src)) {
                sb.slots[i] = *src;
            } else {
                sb.slots[i].ext.length       = src->ext.length;
                sb.slots[i].ext.prefix       = src->ext.prefix;
                sb.slots[i].ext.hash32       = src->ext.hash32;
                sb.slots[i].ext.arena_offset = new_off[code];
            }
            if (out_v != nullptr)
                out_v[i >> 3] |= static_cast<uint8_t>(1u << (i & 7u));
        }
    }

    // Normalize: if no nulls appeared, release and nullify.
    vg.release();
    if (!has_nulls && out_v != nullptr) {
        draken_free(out_v);
        out_v = nullptr;
    } else {
        out_v = sg_normalize_validity(out_v, n);
    }

    bg.release();
    return sg_finalize(sb, out_v, draken_identity_sel(n), false, n, n, total_arena,
                       static_cast<uint8_t>(DRAKEN_SEL_IDENTITY |
                                            DRAKEN_SEL_PERMUTATION));
}

// ---------------------------------------------------------------------------
// COMPRESS — dict-encode a string vector.
//
// Dedup: sg_eq_slots exact equality — matches runtime ops.
// Null rows: code=0, validity marks them null; data slot[0] is the first unique
// non-null value.  All-null / empty: constant-shape (data_length=1).
//
// XXH3 content hash reuse: each unique slot's hash32 was set by str_init_extern
// (or str_init_inline leaves hash32 unused) during ingestion.  New compress
// preserves whatever hash32 is in the source slots — deterministic because D.1
// and the dict ingestion factory both use XXH3_64bits.
// ---------------------------------------------------------------------------
static inline VecResult str_compress(const DrakenVector& v) {
    const uint32_t          n    = v.length;
    const DrakenStringArena* sa  = static_cast<const DrakenStringArena*>(v.data);
    const DrakenStringSlot*  src_s = sa->slots;
    const uint8_t*           src_a = sa->arena;
    const uint8_t*           src_v = v.validity;

    // Edge: empty vector.
    if (n == 0) {
        StrBlock sb = sg_alloc_str_block(1u, 0u);
        SgOwned<void> bg(sb.block);
        bg.release();
        return sg_finalize(sb, nullptr, draken_identity_sel(0u), false, 0u, 0u, 0u, 0u);
    }

    // Phase 1: scan all non-null rows; collect unique slots in first-appearance order.
    // Key = str_hash_seed. sg_eq_slots resolves same-hash candidates exactly.
    // Value = vector of unique-slot indices sharing this seed (for collision chains).
    std::unordered_map<uint64_t, std::vector<uint32_t>> dedup;
    std::vector<uint32_t> unique_src_codes;  // source data[] index for each unique entry
    std::vector<uint32_t> codes(n, 0u);     // output codes per logical row
    bool has_nonnull = false;

    for (uint32_t i = 0; i < n; ++i) {
        if (!sg_val_row(src_v, i)) continue;
        has_nonnull = true;
        const uint32_t         src_code = v.selection[i];
        const DrakenStringSlot* slot    = &src_s[src_code];
        const uint64_t          hseed   = str_hash_seed(slot, src_a);

        bool found = false;
        auto it = dedup.find(hseed);
        if (it != dedup.end()) {
            for (uint32_t uidx : it->second) {
                if (sg_eq_slots(&src_s[unique_src_codes[uidx]], src_a, slot, src_a)) {
                    codes[i] = uidx;
                    found = true;
                    break;
                }
            }
        }
        if (!found) {
            const uint32_t new_idx = static_cast<uint32_t>(unique_src_codes.size());
            unique_src_codes.push_back(src_code);
            codes[i] = new_idx;
            dedup[hseed].push_back(new_idx);
        }
    }

    // All-null: constant-shape with one dummy slot; all rows null.
    if (!has_nonnull) {
        StrBlock sb = sg_alloc_str_block(1u, 0u);
        SgOwned<void> bg(sb.block);
        uint8_t* out_v = sg_copy_validity(src_v, n);
        bg.release();
        return sg_finalize(sb, out_v, draken_zero_sel(n), false, 1u, n, 0u, 0u);
    }

    const uint32_t dict_size = static_cast<uint32_t>(unique_src_codes.size());

    // Phase 2: assign output arena offsets for unique long slots.
    std::vector<uint32_t> new_off(dict_size, 0u);
    size_t total_arena = 0u;
    for (uint32_t k = 0; k < dict_size; ++k) {
        const DrakenStringSlot* slot = &src_s[unique_src_codes[k]];
        if (!str_is_inline(slot)) {
            new_off[k]   = static_cast<uint32_t>(total_arena);
            total_arena += slot->ext.length;
        }
    }
    if (total_arena > static_cast<size_t>(UINT32_MAX))
        throw std::overflow_error("str_compress: arena exceeds 4 GB");

    // Phase 3: allocate data block + codes buffer.
    StrBlock sb = sg_alloc_str_block(dict_size, total_arena);
    SgOwned<void> bg(sb.block);

    SgOwned<void> cg(draken_malloc((n > 0u ? n : 1u) * sizeof(uint32_t)));
    if (!cg) { throw std::bad_alloc(); }
    uint32_t* out_codes = static_cast<uint32_t*>(cg.get());

    // Phase 4: copy unique slots + arena bytes.
    for (uint32_t k = 0; k < dict_size; ++k) {
        const DrakenStringSlot* src = &src_s[unique_src_codes[k]];
        if (str_is_inline(src)) {
            sb.slots[k] = *src;
        } else {
            sb.slots[k].ext.length       = src->ext.length;
            sb.slots[k].ext.prefix       = src->ext.prefix;
            sb.slots[k].ext.hash32       = src->ext.hash32;
            sb.slots[k].ext.arena_offset = new_off[k];
            if (sb.arena_bytes != nullptr)
                std::memcpy(sb.arena_bytes + new_off[k],
                            src_a + src->ext.arena_offset,
                            src->ext.length);
        }
    }

    // Phase 5: fill codes array.
    std::memcpy(out_codes, codes.data(), n * sizeof(uint32_t));

    // Phase 6: copy validity (may throw — bg and cg fire).
    uint8_t* out_v = sg_copy_validity(src_v, n);

    bg.release();
    cg.release();
    return sg_finalize(sb, out_v, out_codes, true, dict_size, n, total_arena, 0u);
}

}  // namespace ops
}  // namespace draken
