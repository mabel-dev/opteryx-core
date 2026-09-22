#pragma once
// draken/ops/string_gather.h — take / materialize / dictionary_encode for DRAKEN_VARCHAR.
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
//   (exact equality; length + first-4-bytes fast-reject before arena compare).
//   Unique slots stored in
//   first-appearance order; owned codes[length] map logical rows to unique slots.
//   All-null / empty: constant-shape result (data_length=1).
//   E37: hash32 is a DEAD FIELD — always 0, no XXH3 is computed for it, and no
//   reader consults it.  It is copied verbatim from the source slot only to keep
//   the 16-byte slot copy whole.  See core/string_slot.h for the current note.
//
// Round-trip: materialize(dictionary_encode(dense)) produces the same logical values.
//
// ACCESS PATTERN: all loops use data[selection[i]] — no shape discrimination.

#include <cstdint>
#include <cstring>
#include <memory>
#include <stdexcept>
#include <unordered_map>
#include <vector>

#include <new>        // std::bad_alloc / placement new — not reliably pulled in by <stdexcept> on stricter libc++
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
    // Zero only struct + slots; the arena [arena_off, total) is written by the
    // caller for valid long strings and its unused tail is never read, so skip it.
    std::memset(block, 0, arena_off);

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
                                    uint8_t          flags,
                                    uint8_t          payloads_elided = 0) {
    sb.sa->slots        = sb.slots;
    sb.sa->arena        = sb.arena_bytes;
    sb.sa->length       = data_length;
    sb.sa->arena_used   = arena_used;
    sb.sa->arena_cap    = arena_used;
    sb.sa->null_bitmap  = validity;  // for consistency; C++ ops use v.validity
    sb.sa->owns_buffers = 0;
    sb.sa->payloads_elided = payloads_elided;
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
// Long  (>12): length + first-4-bytes fast-reject (raw.lo), then arena byte
//              compare.  E37 removed the hash32 reject; hash32 is dead.
// Must match str_eq_slots semantics exactly (runtime equality uses the same rule).
// ---------------------------------------------------------------------------
static inline int sg_eq_slots(const DrakenStringSlot* a,
                              const uint8_t* arena_a,
                              const DrakenStringSlot* b,
                              const uint8_t* arena_b) noexcept {
    if (a->raw.lo != b->raw.lo) return 0;
    if (str_is_inline(a)) return a->raw.hi == b->raw.hi;
    // E37: hash32 fast-reject removed — this dedup only calls sg_eq_slots on
    // candidates already sharing the same 64-bit str_hash_seed, so hash32 (its low
    // 32 bits) always matches. length+first4 match then authoritative byte compare.
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
    // data_length == number of unique slots in the source. A slot's OWN offset
    // says whether its payload exists (STR_ELIDED_PAYLOAD_OFFSET = it does not),
    // checked per-slot — never inferred from a vector-level flag, so a producer
    // that forgets to set one can never misread a real payload as absent, or an
    // elided one as present (see DrakenStringArena.payloads_elided in buffers.h
    // for why the struct flag alone is not trustworthy).
    std::vector<uint32_t> new_off(v.data_length, 0u);
    size_t total_arena = 0u;
    bool any_elided = false;
    for (uint32_t k = 0; k < v.data_length; ++k) {
        if (!str_is_inline(&src_s[k])) {
            if (src_s[k].ext.arena_offset == STR_ELIDED_PAYLOAD_OFFSET) {
                new_off[k] = STR_ELIDED_PAYLOAD_OFFSET;
                any_elided = true;
            } else {
                new_off[k] = static_cast<uint32_t>(total_arena);
                total_arena += src_s[k].ext.length;
            }
        }
    }
    if (total_arena > static_cast<size_t>(UINT32_MAX))
        throw std::overflow_error("str_materialize: arena exceeds 4 GB");

    // Phase 2: allocate output block.
    StrBlock sb = sg_alloc_str_block(n, total_arena);
    SgOwned<void> bg(sb.block);  // frees block if validity alloc throws

    // Phase 3: copy arena bytes for each unique long slot with a real payload.
    for (uint32_t k = 0; k < v.data_length; ++k) {
        if (!str_is_inline(&src_s[k]) && new_off[k] != STR_ELIDED_PAYLOAD_OFFSET &&
            sb.arena_bytes != nullptr) {
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
                                            DRAKEN_SEL_PERMUTATION),
                       any_elided ? 1u : 0u);
}

// ---------------------------------------------------------------------------
// code -> output-arena-offset lookup for str_slice's compact path.
//
// The keys are DICTIONARY INDICES, i.e. already dense in [0, k) — so hashing
// them is pure overhead. `SgDirectOffsets` is a flat array indexed by the code
// itself: one load, no hash, no probe, no rehash. `SgHashOffsets` keeps the
// original map for the case a flat array would be the wrong shape (a huge
// dictionary sampled by a small slice), chosen by the k/n test at the call site.
//
// MEASURED: slicing a 100k-entry dictionary into 65,536-row chunks (a join
// emitting dict-encoded string columns through the cursor's output-boundary
// split) spent ~1.75ms per column-chunk in `unordered_map` — ~27ns per row, and
// 263ms across one 2.5M-row query, which was more than the entire rest of that
// query. The keys were dense the whole time.
//
// A separate `seen` byte is required: STR_ELIDED_PAYLOAD_OFFSET is 0xFFFFFFFF
// and is stored as a legitimate VALUE here, so no offset sentinel can also mean
// "absent".
struct SgDirectOffsets {
    std::vector<uint32_t> off;
    std::vector<uint8_t>  seen;
    explicit SgDirectOffsets(uint32_t k)
        : off(k > 0u ? k : 1u), seen(k > 0u ? k : 1u, 0u) {}
    inline bool has(uint32_t code) const { return seen[code] != 0u; }
    inline void set(uint32_t code, uint32_t value) { off[code] = value; seen[code] = 1u; }
    inline uint32_t get(uint32_t code) const { return off[code]; }
};

struct SgHashOffsets {
    std::unordered_map<uint32_t, uint32_t> m;
    explicit SgHashOffsets(uint32_t n) { m.reserve(n); }
    inline bool has(uint32_t code) const { return m.find(code) != m.end(); }
    inline void set(uint32_t code, uint32_t value) { m[code] = value; }
    inline uint32_t get(uint32_t code) const { return m.find(code)->second; }
};

// Flat-array scratch is O(k) to allocate and clear but O(1) unhashed per row;
// the map is O(n) hashed operations and independent of k. Clearing 4k+k bytes
// costs on the order of k cycles, while n map operations cost on the order of
// 75n — so the array wins for any k up to roughly 75n. 32 leaves margin and
// keeps the scratch bounded at ~160 bytes per sliced row in the worst case.
static constexpr uint32_t kSgDirectOffsetsMaxKPerRow = 32u;

template <typename Offsets>
static inline VecResult str_slice_compact(const DrakenVector& v, uint32_t start,
                                          uint32_t n, Offsets& new_off);

template <typename Offsets>
static inline VecResult str_take_compact(const DrakenVector& v, const int32_t* indices,
                                         uint32_t n, Offsets& new_off);

// ---------------------------------------------------------------------------
// SLICE — contiguous range [start, start+length). Same logic as take but
// source indices are start, start+1, ..., start+length-1 — no index array.
// ---------------------------------------------------------------------------
static inline VecResult str_slice(const DrakenVector& v, uint32_t start, uint32_t n) {
    const DrakenStringArena* sa    = static_cast<const DrakenStringArena*>(v.data);
    const DrakenStringSlot*  src_s = sa->slots;
    const uint8_t*           src_a = sa->arena;
    const uint8_t*           src_v = v.validity;
    const uint32_t           k     = v.data_length;

    // ── Dense fast-path ───────────────────────────────────────────────────────
    // Reads physical slots src_s[start + i] directly, so it is valid ONLY when the
    // selection is the identity permutation (selection[i] == i).  draken_is_dense
    // (data_length == length) is NOT sufficient: a PERMUTATION (e.g. the result of
    // str_take after a sort) also has data_length == length but selection[i] != i —
    // taking this path would read rows in physical order and silently drop the sort.
    // Require the IDENTITY flag; permutations fall through to the selection-honouring
    // k<=n / k>n paths below.  (Per buffers.h: IDENTITY ⟹ PERMUTATION ⟹ dense.)
    if (draken_is_dense(&v) && (v.flags & DRAKEN_SEL_IDENTITY)) {
        size_t total_arena = 0u;
        for (uint32_t i = 0; i < n; ++i) {
            const DrakenStringSlot* s = &src_s[start + i];
            if (!str_is_inline(s) && sg_val_row(src_v, start + i) &&
                s->ext.arena_offset != STR_ELIDED_PAYLOAD_OFFSET)
                total_arena += s->ext.length;
        }
        if (total_arena > static_cast<size_t>(UINT32_MAX))
            throw std::overflow_error("str_slice(dense): arena exceeds 4 GB");

        StrBlock sb = sg_alloc_str_block(n, total_arena);
        SgOwned<void> bg(sb.block);

        uint8_t* out_v = nullptr;
        SgOwned<uint8_t> vg;
        if (src_v != nullptr && n > 0u) { out_v = sg_alloc_validity(n); vg.reset(out_v); }

        bool has_nulls = false;
        size_t arena_pos = 0u;
        for (uint32_t i = 0; i < n; ++i) {
            const uint32_t src_log = start + i;
            if (!sg_val_row(src_v, src_log)) {
                has_nulls = true;
                str_init_null(&sb.slots[i]);
            } else {
                const DrakenStringSlot* src = &src_s[src_log];
                if (str_is_inline(src)) {
                    sb.slots[i] = *src;
                } else if (src->ext.arena_offset == STR_ELIDED_PAYLOAD_OFFSET) {
                    // No payload to move; keep the slot's length and the trap offset.
                    str_clone_with_offset(&sb.slots[i], src, STR_ELIDED_PAYLOAD_OFFSET);
                } else {
                    std::memcpy(sb.arena_bytes + arena_pos,
                                src_a + src->ext.arena_offset,
                                src->ext.length);
                    str_clone_with_offset(&sb.slots[i], src,
                                          static_cast<uint32_t>(arena_pos));
                    arena_pos += src->ext.length;
                }
                if (out_v != nullptr)
                    out_v[i >> 3] |= static_cast<uint8_t>(1u << (i & 7u));
            }
        }

        vg.release();
        if (!has_nulls && out_v != nullptr) { draken_free(out_v); out_v = nullptr; }
        else out_v = sg_normalize_validity(out_v, n);

        bg.release();
        return sg_finalize(sb, out_v, draken_identity_sel(n), false, n, n,
                           static_cast<uint32_t>(total_arena),
                           static_cast<uint8_t>(DRAKEN_SEL_IDENTITY | DRAKEN_SEL_PERMUTATION));
    }

    // When k <= n the source is dict-encoded with fewer distinct values than
    // rows in the slice.  Copy the full k-slot data block as-is and walk the
    // selection array into an owned codes buffer.  data_length stays k,
    // preserving the encoding through the pipeline.
    //
    // When k > n the data block is larger than the slice (e.g. a PLAIN vector
    // being chunked, or a dict whose unique count exceeds the slice size).
    // Copying the full block would waste memory and hurt cache — use the
    // compact path instead: build exactly n output slots from the referenced
    // source slots and emit with data_length == n (dense).
    if (k <= n) {
        // ── Dict-preserving path ──────────────────────────────────────────
        StrBlock sb = sg_alloc_str_block(k > 0u ? k : 1u, sa->arena_used);
        SgOwned<void> bg(sb.block);
        std::memcpy(sb.slots, src_s, (k > 0u ? k : 1u) * sizeof(DrakenStringSlot));
        if (sa->arena_used > 0u && sb.arena_bytes != nullptr)
            std::memcpy(sb.arena_bytes, sa->arena, sa->arena_used);

        SgOwned<void> cg(draken_malloc((n > 0u ? n : 1u) * sizeof(uint32_t)));
        if (!cg) throw std::bad_alloc();
        uint32_t* out_codes = static_cast<uint32_t*>(cg.get());
        for (uint32_t i = 0; i < n; ++i)
            out_codes[i] = v.selection[start + i];

        uint8_t* out_v = nullptr;
        SgOwned<uint8_t> vg;
        bool has_nulls = false;
        if (src_v != nullptr && n > 0u) {
            out_v = sg_alloc_validity(n);
            vg.reset(out_v);
            for (uint32_t i = 0; i < n; ++i) {
                const uint32_t src_log = start + i;
                if (sg_val_row(src_v, src_log))
                    out_v[i >> 3] |= static_cast<uint8_t>(1u << (i & 7u));
                else
                    has_nulls = true;
            }
            vg.release();
            if (!has_nulls) { draken_free(out_v); out_v = nullptr; }
            else out_v = sg_normalize_validity(out_v, n);
        }

        bg.release();
        cg.release();
        return sg_finalize(sb, out_v, out_codes, true, k, n, sa->arena_used, 0u,
                           0u);
    }

    // ── Compact path (k > n): build exactly n output slots ────────────────
    // Dispatched on the shape of the lookup, not on the data: dictionary codes
    // are dense in [0, k), so unless the dictionary dwarfs the slice a flat array
    // beats hashing them. Both arms run the SAME body below and produce the same
    // bytes — this chooses a data structure, never an answer.
    if (n > 0u && k <= kSgDirectOffsetsMaxKPerRow * n) {
        SgDirectOffsets new_off(k);
        return str_slice_compact(v, start, n, new_off);
    }
    SgHashOffsets new_off_hash(n);
    return str_slice_compact(v, start, n, new_off_hash);
}

template <typename Offsets>
static inline VecResult str_slice_compact(const DrakenVector& v, uint32_t start,
                                          uint32_t n, Offsets& new_off) {
    const DrakenStringArena* sa    = static_cast<const DrakenStringArena*>(v.data);
    const DrakenStringSlot*  src_s = sa->slots;
    const uint8_t*           src_a = sa->arena;
    const uint8_t*           src_v = v.validity;

    // Scan [start, start+n) to find referenced unique long slots and their
    // output arena offsets.
    std::vector<uint32_t> seen_codes;
    seen_codes.reserve(n);
    size_t total_arena = 0u;

    for (uint32_t i = 0; i < n; ++i) {
        const uint32_t src_log = start + i;
        if (!sg_val_row(src_v, src_log)) continue;
        const uint32_t code = v.selection[src_log];
        if (!str_is_inline(&src_s[code]) && !new_off.has(code)) {
            if (src_s[code].ext.arena_offset == STR_ELIDED_PAYLOAD_OFFSET) {
                new_off.set(code, STR_ELIDED_PAYLOAD_OFFSET);
            } else {
                new_off.set(code, static_cast<uint32_t>(total_arena));
                total_arena  += src_s[code].ext.length;
            }
            seen_codes.push_back(code);
        }
    }
    if (total_arena > static_cast<size_t>(UINT32_MAX))
        throw std::overflow_error("str_slice: arena exceeds 4 GB");

    StrBlock sb = sg_alloc_str_block(n, total_arena);
    SgOwned<void> bg(sb.block);

    if (sb.arena_bytes != nullptr) {
        for (uint32_t code : seen_codes)
            if (new_off.get(code) != STR_ELIDED_PAYLOAD_OFFSET)
                std::memcpy(sb.arena_bytes + new_off.get(code),
                            src_a + src_s[code].ext.arena_offset,
                            src_s[code].ext.length);
    }

    uint8_t* out_v = nullptr;
    SgOwned<uint8_t> vg;
    if (src_v != nullptr && n > 0u) { out_v = sg_alloc_validity(n); vg.reset(out_v); }

    bool has_nulls = false;
    for (uint32_t i = 0; i < n; ++i) {
        const uint32_t src_log = start + i;
        if (!sg_val_row(src_v, src_log)) {
            has_nulls = true;
        } else {
            const uint32_t          code = v.selection[src_log];
            const DrakenStringSlot* src  = &src_s[code];
            if (str_is_inline(src)) {
                sb.slots[i] = *src;
            } else {
                sb.slots[i].ext.length       = src->ext.length;
                sb.slots[i].ext.prefix       = src->ext.prefix;
                sb.slots[i].ext.hash32       = src->ext.hash32;
                sb.slots[i].ext.arena_offset = new_off.get(code);
            }
            if (out_v != nullptr)
                out_v[i >> 3] |= static_cast<uint8_t>(1u << (i & 7u));
        }
    }

    vg.release();
    if (!has_nulls && out_v != nullptr) { draken_free(out_v); out_v = nullptr; }
    else out_v = sg_normalize_validity(out_v, n);

    bg.release();
    return sg_finalize(sb, out_v, draken_identity_sel(n), false, n, n, total_arena,
                       static_cast<uint8_t>(DRAKEN_SEL_IDENTITY | DRAKEN_SEL_PERMUTATION));
}

// ---------------------------------------------------------------------------
// TAKE — gather logical rows by index.
//
// indices[i] is a logical row position in v.  Output row i = source row
// indices[i].  Null source row → null output row.
//
// Dict-preserving path (k <= n): copies the full k-slot data block and builds
// an owned codes array (out_codes[i] = v.selection[indices[i]]).  Keeps
// data_length = k so compressed-fast-path consumers (group-by, join) see the
// repeated-value structure even after a filter/gather.
//
// Compact path (k > n): builds exactly n output slots from the referenced
// source slots.  The code -> output-arena-offset lookup is dispatched on the
// SHAPE of the lookup exactly as str_slice's is (SgDirectOffsets vs
// SgHashOffsets, chosen by kSgDirectOffsetsMaxKPerRow) — see the comment on
// those types.  This is a data-structure choice, never an answer: both arms run
// the same body and emit the same bytes — verified per call by comparing the
// emitted slots, arena and validity across 8 JOB queries: zero mismatches.
//
// MEASURED on JOB skene, 1a/6a/8a/10a/13a/20a/26a/33a, MAX_EXECUTION_WORKER_CAP=4
// (2026-09-22).  Both policies were instantiated in one binary and run on the
// SAME call with the SAME inputs — per call, one untimed warm-up of each arm,
// then paired reps alternating which arm leads.  The warm-up is load-bearing:
// without it the trailing arm inherits the warm cache and the ratio inflates
// (an uncorrected run showed 99% order bias).  Residual bias after correction
// was within +/-2%.
//
//   kernel, thread-time: 28.6-36.6 ns/row with the bare std::unordered_map,
//   12.4-14.3 ns/row with the flat array; row-weighted 30.3 -> 13.3, i.e.
//   2.27x weighted, 2.10-2.64x per query (2 of the 8 reach 2.4x).
//
//   end to end: +5.6% +/- 1.1% of query wall time, 47 of 48 paired rounds,
//   6 of 6 rounds.
//
// ⛔ Those two numbers are not the same number and must not be quoted as one.
// ns/row is PER THREAD; the wall-clock share divides by DOP, so a 2.27x kernel
// win is a ~5.6% query win at 4 workers (20a: ~76ms of thread time, ~19ms of
// wall).  Quote the end-to-end figure when deciding whether this is worth
// anything; quote ns/row only when comparing the two lookups to each other.
//
// The keys were dense dictionary codes the whole time.  Every call on that
// suite fell inside the flat-array arm — the hash arm exists for the
// dictionary-dwarfs-the-gather shape, not for the common one, and is therefore
// NOT exercised by this measurement: kSgDirectOffsetsMaxKPerRow is reasoned,
// not measured.
// ---------------------------------------------------------------------------
static inline VecResult str_take(const DrakenVector& v,
                                  const int32_t*      indices,
                                  uint32_t            n) {
    const DrakenStringArena* sa    = static_cast<const DrakenStringArena*>(v.data);
    const DrakenStringSlot*  src_s = sa->slots;
    const uint8_t*           src_a = sa->arena;
    const uint8_t*           src_v = v.validity;
    const uint32_t           k     = v.data_length;

    if (k <= n) {
        // ── Dict-preserving path ─────────────────────────────────────────
        StrBlock sb = sg_alloc_str_block(k > 0u ? k : 1u, sa->arena_used);
        SgOwned<void> bg(sb.block);
        std::memcpy(sb.slots, src_s, (k > 0u ? k : 1u) * sizeof(DrakenStringSlot));
        if (sa->arena_used > 0u && sb.arena_bytes != nullptr)
            std::memcpy(sb.arena_bytes, sa->arena, sa->arena_used);

        SgOwned<void> cg(draken_malloc((n > 0u ? n : 1u) * sizeof(uint32_t)));
        if (!cg) throw std::bad_alloc();
        uint32_t* out_codes = static_cast<uint32_t*>(cg.get());

        uint8_t* out_v = nullptr;
        SgOwned<uint8_t> vg;
        bool has_nulls = false;
        if (src_v != nullptr && n > 0u) { out_v = sg_alloc_validity(n); vg.reset(out_v); }

        for (uint32_t i = 0; i < n; ++i) {
            const uint32_t src_log = static_cast<uint32_t>(indices[i]);
            if (!sg_val_row(src_v, src_log)) {
                out_codes[i] = 0u;
                has_nulls = true;
            } else {
                out_codes[i] = v.selection[src_log];
                if (out_v != nullptr)
                    out_v[i >> 3] |= static_cast<uint8_t>(1u << (i & 7u));
            }
        }

        vg.release();
        if (!has_nulls && out_v != nullptr) { draken_free(out_v); out_v = nullptr; }
        else out_v = sg_normalize_validity(out_v, n);

        bg.release();
        cg.release();
        return sg_finalize(sb, out_v, out_codes, true, k, n, sa->arena_used, 0u,
                           0u);
    }

    // ── Compact path (k > n): build exactly n output slots ────────────────
    // Dispatched on the shape of the lookup, not on the data — identical to
    // str_slice's choice above, for the same reason: dictionary codes are dense
    // in [0, k), so unless the dictionary dwarfs the gather a flat array beats
    // hashing them. Both arms run the SAME body and produce the same bytes.
    if (n > 0u && k <= kSgDirectOffsetsMaxKPerRow * n) {
        SgDirectOffsets new_off(k);
        return str_take_compact(v, indices, n, new_off);
    }
    SgHashOffsets new_off_hash(n);
    return str_take_compact(v, indices, n, new_off_hash);
}

template <typename Offsets>
static inline VecResult str_take_compact(const DrakenVector& v, const int32_t* indices,
                                         uint32_t n, Offsets& new_off) {
    const DrakenStringArena* sa    = static_cast<const DrakenStringArena*>(v.data);
    const DrakenStringSlot*  src_s = sa->slots;
    const uint8_t*           src_a = sa->arena;
    const uint8_t*           src_v = v.validity;

    // Phase 1: scan indices to compute arena layout.
    std::vector<uint32_t> seen_codes;
    seen_codes.reserve(n);
    size_t total_arena = 0u;

    for (uint32_t i = 0; i < n; ++i) {
        const uint32_t src_log = static_cast<uint32_t>(indices[i]);
        if (!sg_val_row(src_v, src_log)) continue;
        const uint32_t code = v.selection[src_log];
        if (!str_is_inline(&src_s[code]) && !new_off.has(code)) {
            if (src_s[code].ext.arena_offset == STR_ELIDED_PAYLOAD_OFFSET) {
                new_off.set(code, STR_ELIDED_PAYLOAD_OFFSET);
            } else {
                new_off.set(code, static_cast<uint32_t>(total_arena));
                total_arena  += src_s[code].ext.length;
            }
            seen_codes.push_back(code);
        }
    }
    if (total_arena > static_cast<size_t>(UINT32_MAX))
        throw std::overflow_error("str_take: arena exceeds 4 GB");

    StrBlock sb = sg_alloc_str_block(n, total_arena);
    SgOwned<void> bg(sb.block);

    if (sb.arena_bytes != nullptr) {
        for (uint32_t code : seen_codes)
            if (new_off.get(code) != STR_ELIDED_PAYLOAD_OFFSET)
                std::memcpy(sb.arena_bytes + new_off.get(code),
                            src_a + src_s[code].ext.arena_offset,
                            src_s[code].ext.length);
    }

    uint8_t* out_v = nullptr;
    SgOwned<uint8_t> vg;
    if (src_v != nullptr && n > 0) { out_v = sg_alloc_validity(n); vg.reset(out_v); }

    bool has_nulls = false;
    for (uint32_t i = 0; i < n; ++i) {
        const uint32_t src_log = static_cast<uint32_t>(indices[i]);
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
                sb.slots[i].ext.arena_offset = new_off.get(code);
            }
            if (out_v != nullptr)
                out_v[i >> 3] |= static_cast<uint8_t>(1u << (i & 7u));
        }
    }

    vg.release();
    if (!has_nulls && out_v != nullptr) { draken_free(out_v); out_v = nullptr; }
    else out_v = sg_normalize_validity(out_v, n);

    bg.release();
    return sg_finalize(sb, out_v, draken_identity_sel(n), false, n, n, total_arena,
                       static_cast<uint8_t>(DRAKEN_SEL_IDENTITY |
                                            DRAKEN_SEL_PERMUTATION));
}

// ---------------------------------------------------------------------------
// SgDedupTable — open-addressed flat hash table for str_dictionary_encode.
//
// Replaces `std::unordered_map<uint64_t, std::vector<uint32_t>>`, which cost
// ONE node malloc PLUS one vector malloc per distinct value and chased a
// pointer on every probe. Same class of defect as the offset maps above; the
// keys here are 64-bit content hashes rather than dense dictionary codes, so a
// flat direct-indexed array is not available — but an open-addressed table is,
// and it removes the per-distinct-value allocations and the indirection.
//
// Storage: one draken_malloc'd array of 16-byte entries. Linear probing,
// power-of-two capacity, load factor kept <= 0.5, doubling rehash. Collision
// chains resolve IN PLACE — a genuine 64-bit hash collision simply occupies the
// next probe position, and the probe walk verifies every key-equal candidate
// with sg_eq_slots, which remains the authoritative equality test. Empty is
// encoded as `val_plus_one == 0`, so no key value is reserved as a sentinel.
//
// This is a data-structure choice, never an answer: the dedup body, the
// equality rule and the emitted codes are unchanged.
//
// FIRST-APPEARANCE ORDER is not held here — it lives, exactly as before, in
// `unique_src_codes`, whose push_back order defines the emitted codes. This
// table only maps hash -> unique index; it never reorders anything.
//
// MEASURED (2026-09-22, interleaved A/B, both arms in one binary, arm order
// alternated within each round, 65,536-row vectors): 2.5x / 2.2x / 2.4x on
// short (inline) columns at 100 / 10,000 / all-unique distinct values, and
// 1.45x / 1.9x / 2.1x on long (extern) columns at the same three. Equivalence
// verified against the previous implementation over 400 randomised cases
// (varying row count, distinct count, short/long, with and without nulls):
// identical dict size, per-row codes and dictionary slot bytes.
// ---------------------------------------------------------------------------
struct SgDedupEntry {
    uint64_t key;
    uint32_t val_plus_one;  // 0 == empty
    uint32_t pad_;
};

class SgDedupTable {
  public:
    // `hint` is an upper bound on distinct values (the caller passes the row
    // count). Capacity starts at the smaller of hint-derived and a fixed floor
    // so a high-dedup column does not allocate for rows it will never store.
    explicit SgDedupTable(uint32_t hint) {
        uint32_t want = (hint < 512u) ? hint : 512u;
        cap_ = 64u;
        while (cap_ < want * 2u) cap_ <<= 1;
        e_ = alloc_(cap_);
    }
    ~SgDedupTable() { if (e_ != nullptr) draken_free(e_); }
    SgDedupTable(const SgDedupTable&)            = delete;
    SgDedupTable& operator=(const SgDedupTable&) = delete;

    // Must be called before each probe/insert pair: guarantees room for one
    // more entry, so the position returned by a probe stays valid.
    inline void reserve_one() {
        if (count_ + 1u > (cap_ >> 1)) grow_();
    }

    // MEASURED, and the reason this finalizer exists: str_hash_seed is a SEED,
    // not a finished hash. For INLINE slots it is `raw.lo + raw.hi * K` — the
    // low 32 bits carry almost no entropy (raw.lo's low word is the string
    // LENGTH, identical across a column, and a constant multiply pushes
    // entropy upward, never down). Masking those bits for a probe index
    // clustered catastrophically: the flat table ran 2.5-3x SLOWER than the
    // unordered_map it replaced on short-string columns, while long-string
    // columns — whose seed is a fully-mixed XXH3 — were already 1.5-3x faster.
    // std::unordered_map hid the defect by taking a PRIME modulus of the whole
    // 64 bits. An open-addressed table masks low bits, so it must finalize the
    // seed first. splitmix64's finalizer, ~3 cycles.
    static inline uint64_t finalize_(uint64_t k) noexcept {
        k ^= k >> 33;
        k *= 0xff51afd7ed558ccdULL;
        k ^= k >> 29;
        return k;
    }
    inline uint32_t probe_start(uint64_t key) const noexcept {
        return static_cast<uint32_t>(finalize_(key)) & (cap_ - 1u);
    }
    inline uint32_t next(uint32_t pos) const noexcept {
        return (pos + 1u) & (cap_ - 1u);
    }
    inline const SgDedupEntry& at(uint32_t pos) const noexcept { return e_[pos]; }

    inline void insert_at(uint32_t pos, uint64_t key, uint32_t value) noexcept {
        e_[pos].key          = key;
        e_[pos].val_plus_one = value + 1u;
        ++count_;
    }

  private:
    // Allocate a zeroed table of `cap` entries. Never partially commits.
    static SgDedupEntry* alloc_(uint32_t cap) {
        const size_t bytes = static_cast<size_t>(cap) * sizeof(SgDedupEntry);
        SgDedupEntry* p = static_cast<SgDedupEntry*>(draken_malloc(bytes));
        if (p == nullptr) throw std::bad_alloc();
        std::memset(p, 0, bytes);
        return p;
    }

    // Strong exception guarantee: the new table is fully built before the old
    // one is released, so a failed allocation leaves the table untouched and
    // leaks nothing.
    void grow_() {
        // Hard ceiling: capacity is uint32 and must stay a power of two.
        // 2^31 entries holds 2^30 distinct values — far past any real vector.
        // Fail loudly rather than wrapping to zero.
        if (cap_ > (1u << 30)) throw std::overflow_error(
            "str_dictionary_encode: dedup table exceeds 2^31 entries");
        const uint32_t new_cap = cap_ << 1;
        SgDedupEntry*  ne      = alloc_(new_cap);  // throws: e_/cap_ unchanged
        for (uint32_t i = 0; i < cap_; ++i) {
            if (e_[i].val_plus_one == 0u) continue;
            uint32_t pos =
                static_cast<uint32_t>(finalize_(e_[i].key)) & (new_cap - 1u);
            while (ne[pos].val_plus_one != 0u) pos = (pos + 1u) & (new_cap - 1u);
            ne[pos] = e_[i];
        }
        draken_free(e_);
        e_   = ne;
        cap_ = new_cap;
    }

    SgDedupEntry* e_     = nullptr;
    uint32_t      cap_   = 0u;
    uint32_t      count_ = 0u;
};

// ---------------------------------------------------------------------------
// COMPRESS — dict-encode a string vector.
//
// Dedup: sg_eq_slots exact equality — matches runtime ops.
// Null rows: code=0, validity marks them null; data slot[0] is the first unique
// non-null value.  All-null / empty: constant-shape (data_length=1).
//
// XXH3 content hash reuse: each unique slot's hash32 was set by str_init_extern
// (or str_init_inline leaves hash32 unused) during ingestion.  New dictionary_encode
// preserves whatever hash32 is in the source slots — deterministic because D.1
// and the dict ingestion factory both use XXH3_64bits.
// ---------------------------------------------------------------------------
static inline VecResult str_dictionary_encode(const DrakenVector& v) {
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
    // Key = str_hash_seed. sg_eq_slots resolves same-hash candidates exactly —
    // the table only narrows the candidate set, it never decides equality.
    // The probe walk stops at the first EMPTY entry, which is also the insert
    // position; every key-equal entry passed on the way is a 64-bit hash
    // collision and is verified (and rejected) by sg_eq_slots.
    SgDedupTable dedup(n);
    std::vector<uint32_t> unique_src_codes;  // source data[] index for each unique entry
    std::vector<uint32_t> codes(n, 0u);     // output codes per logical row
    bool has_nonnull = false;

    for (uint32_t i = 0; i < n; ++i) {
        if (!sg_val_row(src_v, i)) continue;
        has_nonnull = true;
        const uint32_t         src_code = v.selection[i];
        const DrakenStringSlot* slot    = &src_s[src_code];
        const uint64_t          hseed   = str_hash_seed(slot, src_a);

        dedup.reserve_one();  // keeps `pos` below valid across a possible rehash

        bool     found = false;
        uint32_t pos   = dedup.probe_start(hseed);
        for (;;) {
            const SgDedupEntry& e = dedup.at(pos);
            if (e.val_plus_one == 0u) break;  // empty — not present; insert here
            if (e.key == hseed) {
                const uint32_t uidx = e.val_plus_one - 1u;
                if (sg_eq_slots(&src_s[unique_src_codes[uidx]], src_a, slot, src_a)) {
                    codes[i] = uidx;
                    found = true;
                    break;
                }
            }
            pos = dedup.next(pos);
        }
        if (!found) {
            const uint32_t new_idx = static_cast<uint32_t>(unique_src_codes.size());
            unique_src_codes.push_back(src_code);
            codes[i] = new_idx;
            dedup.insert_at(pos, hseed, new_idx);
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
            if (slot->ext.arena_offset == STR_ELIDED_PAYLOAD_OFFSET) {
                new_off[k] = STR_ELIDED_PAYLOAD_OFFSET;
            } else {
                new_off[k]   = static_cast<uint32_t>(total_arena);
                total_arena += slot->ext.length;
            }
        }
    }
    if (total_arena > static_cast<size_t>(UINT32_MAX))
        throw std::overflow_error("str_dictionary_encode: arena exceeds 4 GB");

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
            if (new_off[k] != STR_ELIDED_PAYLOAD_OFFSET && sb.arena_bytes != nullptr)
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
