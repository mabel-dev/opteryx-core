#pragma once
// draken/ops/string_predicates.h — string predicates: between and in_list.
//
// str_between(v, lo_slot, lo_bytes, hi_slot, hi_bytes, lo_incl, hi_incl)
//   → bit-packed DRAKEN_BOOL VecResult.
//   Lexicographic range check using str_compare (string_slot.h).
//   lo_incl / hi_incl control whether bounds are closed (SQL BETWEEN = both true).
//   4-way compile-time dispatch on inclusivity pair eliminates per-row branches.
//
// str_in_list(v, set) → bit-packed DRAKEN_BOOL VecResult.
//   Set membership via hash-only CarcharSet probe.
//
// HASH SEMANTICS (in_list):
//   SHORT strings (len ≤ 12): full content is in the slot-derived hash seed.
//   LONG strings (len > 12): hash seed is XXH3_64bits(full arena payload).
//   Membership remains hash-only because CarcharSet stores uint64 hashes, not
//   keys; this is a 64-bit hash identity check, not exact value verification.
//
// HASH PATH: str_hash_seed → simd_hash_i64 — identical to the hash_string
// kernel and to the nanobind binding's set-building loop. Any path divergence
// between set-build and probe causes present values to miss silently; do NOT
// introduce an alternative hash path.
//
// NULL SEMANTICS (TVL, both ops): null row → null output row (validity bit 0,
// result bit 0). Output validity is a copy of input validity; nullptr when no
// nulls.
//
// ACCESS PATTERN: uniform slots[v.selection[i]] for i in [0, v.length).
// No shape discrimination by default. Constant and dict fast paths are
// architect-approved exceptions (see buffers.h approved exceptions comment).
//
// ALLOCATOR DISCIPLINE: output buffer owned by mimalloc (draken_malloc).
// CarcharSet is constructed and owned by the caller (std::vector storage);
// passed by const reference — no allocator-boundary crossing.

#include <cstdint>
#include <cstring>

#include <new>        // std::bad_alloc / placement new — not reliably pulled in by <stdexcept> on stricter libc++
#include "core/buffers.h"       // DrakenVector, DrakenStringArena, DRAKEN_BOOL …
#include "core/alloc.h"
#include "core/vector_alloc.h"
#include "core/string_slot.h"   // DrakenStringSlot, str_compare
#include "ops/vec_result.h"
#include "ops/int64_compare.h"  // cmp_alloc_bool_buf, cmp_copy_validity
#include "ops/string_compare.h" // str_constant_bool_result, str_dict_bool_result
#include "ops/string_hash.h"    // str_hash_seed — must match set-build path exactly
#include "simd_hash.h"          // simd_hash_i64, NULL_HASH
#include "carchar_set.hpp"      // opteryx::carchar::CarcharSet

namespace draken { namespace ops {

// ---------------------------------------------------------------------------
// str_between — lexicographic range membership.
//
// str_between_row<lo_incl,hi_incl>: per-row test using str_compare.
//   str_compare(a, a_arena, b, b_arena) returns <0 / 0 / >0 (a vs b).
//   lo_incl=true  → lo ≤ v (lo_cmp ≥ 0)    lo_incl=false → lo < v (lo_cmp > 0)
//   hi_incl=true  → v ≤ hi (hi_cmp ≤ 0)    hi_incl=false → v < hi (hi_cmp < 0)
// ---------------------------------------------------------------------------

template<bool lo_incl, bool hi_incl>
static inline bool str_between_row(
    const DrakenStringSlot* slot, const uint8_t* arena,
    const DrakenStringSlot& lo_slot, const uint8_t* lo_bytes,
    const DrakenStringSlot& hi_slot, const uint8_t* hi_bytes) noexcept
{
    const int lo_cmp = str_compare(slot, arena, &lo_slot, lo_bytes);
    const int hi_cmp = str_compare(slot, arena, &hi_slot, hi_bytes);
    const bool lo_ok = lo_incl ? (lo_cmp >= 0) : (lo_cmp > 0);
    const bool hi_ok = hi_incl ? (hi_cmp <= 0) : (hi_cmp < 0);
    return lo_ok & hi_ok;
}

// 8-way byte-pack kernel (same packing structure as int64_predicates.h).
// dst must be pre-zeroed.  Non-null path packs 8 rows per byte; null path
// ANDs each packed byte with the validity byte so null rows → 0.
template<bool lo_incl, bool hi_incl>
static inline void str_between_kernel(
    const DrakenStringSlot* slots, const uint8_t* arena,
    const uint32_t* selection,
    const DrakenStringSlot& lo_slot, const uint8_t* lo_bytes,
    const DrakenStringSlot& hi_slot, const uint8_t* hi_bytes,
    const uint8_t* src_null,
    uint8_t* dst, uint32_t n)
{
    const uint32_t whole_bytes = n >> 3;

    if (src_null == nullptr) {
        for (uint32_t b = 0; b < whole_bytes; ++b) {
            const uint32_t base = b << 3;
            dst[b] = static_cast<uint8_t>(
                (static_cast<unsigned>(str_between_row<lo_incl,hi_incl>(&slots[selection[base+0]], arena, lo_slot, lo_bytes, hi_slot, hi_bytes)) << 0) |
                (static_cast<unsigned>(str_between_row<lo_incl,hi_incl>(&slots[selection[base+1]], arena, lo_slot, lo_bytes, hi_slot, hi_bytes)) << 1) |
                (static_cast<unsigned>(str_between_row<lo_incl,hi_incl>(&slots[selection[base+2]], arena, lo_slot, lo_bytes, hi_slot, hi_bytes)) << 2) |
                (static_cast<unsigned>(str_between_row<lo_incl,hi_incl>(&slots[selection[base+3]], arena, lo_slot, lo_bytes, hi_slot, hi_bytes)) << 3) |
                (static_cast<unsigned>(str_between_row<lo_incl,hi_incl>(&slots[selection[base+4]], arena, lo_slot, lo_bytes, hi_slot, hi_bytes)) << 4) |
                (static_cast<unsigned>(str_between_row<lo_incl,hi_incl>(&slots[selection[base+5]], arena, lo_slot, lo_bytes, hi_slot, hi_bytes)) << 5) |
                (static_cast<unsigned>(str_between_row<lo_incl,hi_incl>(&slots[selection[base+6]], arena, lo_slot, lo_bytes, hi_slot, hi_bytes)) << 6) |
                (static_cast<unsigned>(str_between_row<lo_incl,hi_incl>(&slots[selection[base+7]], arena, lo_slot, lo_bytes, hi_slot, hi_bytes)) << 7));
        }
        for (uint32_t i = whole_bytes << 3; i < n; ++i) {
            if (str_between_row<lo_incl,hi_incl>(&slots[selection[i]], arena, lo_slot, lo_bytes, hi_slot, hi_bytes))
                dst[i >> 3] |= static_cast<uint8_t>(1u << (i & 7));
        }
    } else {
        for (uint32_t b = 0; b < whole_bytes; ++b) {
            const uint32_t base = b << 3;
            const uint8_t m = static_cast<uint8_t>(
                (static_cast<unsigned>(str_between_row<lo_incl,hi_incl>(&slots[selection[base+0]], arena, lo_slot, lo_bytes, hi_slot, hi_bytes)) << 0) |
                (static_cast<unsigned>(str_between_row<lo_incl,hi_incl>(&slots[selection[base+1]], arena, lo_slot, lo_bytes, hi_slot, hi_bytes)) << 1) |
                (static_cast<unsigned>(str_between_row<lo_incl,hi_incl>(&slots[selection[base+2]], arena, lo_slot, lo_bytes, hi_slot, hi_bytes)) << 2) |
                (static_cast<unsigned>(str_between_row<lo_incl,hi_incl>(&slots[selection[base+3]], arena, lo_slot, lo_bytes, hi_slot, hi_bytes)) << 3) |
                (static_cast<unsigned>(str_between_row<lo_incl,hi_incl>(&slots[selection[base+4]], arena, lo_slot, lo_bytes, hi_slot, hi_bytes)) << 4) |
                (static_cast<unsigned>(str_between_row<lo_incl,hi_incl>(&slots[selection[base+5]], arena, lo_slot, lo_bytes, hi_slot, hi_bytes)) << 5) |
                (static_cast<unsigned>(str_between_row<lo_incl,hi_incl>(&slots[selection[base+6]], arena, lo_slot, lo_bytes, hi_slot, hi_bytes)) << 6) |
                (static_cast<unsigned>(str_between_row<lo_incl,hi_incl>(&slots[selection[base+7]], arena, lo_slot, lo_bytes, hi_slot, hi_bytes)) << 7));
            dst[b] = static_cast<uint8_t>(m & src_null[b]);
        }
        for (uint32_t i = whole_bytes << 3; i < n; ++i) {
            if ((src_null[i >> 3] >> (i & 7)) & 1u) {
                if (str_between_row<lo_incl,hi_incl>(&slots[selection[i]], arena, lo_slot, lo_bytes, hi_slot, hi_bytes))
                    dst[i >> 3] |= static_cast<uint8_t>(1u << (i & 7));
            }
        }
    }
}

template<bool lo_incl, bool hi_incl>
static inline VecResult str_between_impl(
    const DrakenVector&     v,
    const DrakenStringSlot& lo_slot, const uint8_t* lo_bytes,
    const DrakenStringSlot& hi_slot, const uint8_t* hi_bytes)
{
    const uint32_t           n     = v.length;
    const DrakenStringArena* sa    = static_cast<const DrakenStringArena*>(v.data);
    const DrakenStringSlot*  slots = sa->slots;
    const uint8_t*           arena = sa->arena;
    const uint8_t*        src_null = v.validity;

    if (draken_is_constant(&v)) {
        return str_constant_bool_result(
            str_between_row<lo_incl,hi_incl>(&slots[0], arena, lo_slot, lo_bytes, hi_slot, hi_bytes),
            src_null, n);
    }

    if (draken_is_dict(&v)) {
        const uint32_t dl = v.data_length;
        uint8_t* db = static_cast<uint8_t*>(draken_malloc(dl));
        if (!db) throw std::bad_alloc();
        for (uint32_t k = 0; k < dl; ++k)
            db[k] = str_between_row<lo_incl,hi_incl>(&slots[k], arena, lo_slot, lo_bytes, hi_slot, hi_bytes) ? 1u : 0u;
        VecResult r;
        try { r = str_dict_bool_result(db, v); }
        catch (...) { draken_free(db); throw; }
        draken_free(db);
        return r;
    }

    uint8_t* dst = cmp_alloc_bool_buf(n);

    uint8_t* out_null = nullptr;
    if (src_null != nullptr) {
        try {
            out_null = cmp_copy_validity(src_null, n);
        } catch (...) {
            draken_free(dst);
            throw;
        }
    }

    str_between_kernel<lo_incl,hi_incl>(slots, arena, v.selection,
                                        lo_slot, lo_bytes, hi_slot, hi_bytes,
                                        src_null, dst, n);

    VecResult r;
    r.data           = dst;
    r.validity       = out_null;
    r.selection      = draken_identity_sel(n);
    r.owns_selection = false;
    r.data_length    = n;
    r.length         = n;
    r.type           = DRAKEN_BOOL;
    r.flags          = static_cast<uint8_t>(DRAKEN_SEL_IDENTITY | DRAKEN_SEL_PERMUTATION);
    return r;
}

// Public function — dispatches to one of 4 compile-time specialisations.
static inline VecResult str_between(
    const DrakenVector&     v,
    const DrakenStringSlot& lo_slot, const uint8_t* lo_bytes,
    const DrakenStringSlot& hi_slot, const uint8_t* hi_bytes,
    bool lo_incl, bool hi_incl)
{
    if (lo_incl) {
        if (hi_incl) return str_between_impl<true,  true >(v, lo_slot, lo_bytes, hi_slot, hi_bytes);
        else         return str_between_impl<true,  false>(v, lo_slot, lo_bytes, hi_slot, hi_bytes);
    } else {
        if (hi_incl) return str_between_impl<false, true >(v, lo_slot, lo_bytes, hi_slot, hi_bytes);
        else         return str_between_impl<false, false>(v, lo_slot, lo_bytes, hi_slot, hi_bytes);
    }
}

// ---------------------------------------------------------------------------
// str_in_list — hash-only membership via CarcharSet bulk probe.
//
// Hash-only; no key verification.
// The hash path (str_hash_seed → simd_hash_i64) MUST be the same path used
// by hash_string and by the binding's set-building loop. Any deviation causes
// present long values to silently miss.
// ---------------------------------------------------------------------------
static inline VecResult str_in_list(
    const DrakenVector& v,
    const opteryx::carchar::CarcharSet& set)
{
    const uint32_t           n        = v.length;
    const DrakenStringArena* sa       = static_cast<const DrakenStringArena*>(v.data);
    const DrakenStringSlot*  slots    = sa->slots;
    const uint8_t*           src_null = v.validity;

    if (draken_is_constant(&v)) {
        uint64_t seed = str_hash_seed(&slots[0], sa->arena);
        uint64_t h;
        simd_hash_i64(&seed, &h, 1);
        return cmp_constant_bool_result(set.contains(h), src_null, n);
    }

    if (draken_is_dict(&v)) {
        const uint32_t dl = v.data_length;
        uint8_t* db = static_cast<uint8_t*>(draken_malloc(dl));
        if (!db) throw std::bad_alloc();
        uint64_t seeds[1024], hashes[1024];
        uint32_t done = 0;
        while (done < dl) {
            const uint32_t block = (dl - done < 1024u) ? (dl - done) : 1024u;
            for (uint32_t j = 0; j < block; ++j)
                seeds[j] = str_hash_seed(&slots[done + j], sa->arena);
            simd_hash_i64(seeds, hashes, block);
            for (uint32_t j = 0; j < block; ++j)
                db[done + j] = set.contains(hashes[j]) ? 1u : 0u;
            done += block;
        }
        VecResult r;
        try { r = cmp_dict_bool_result(db, v); }
        catch (...) { draken_free(db); throw; }
        draken_free(db);
        return r;
    }

    uint8_t* dst = cmp_alloc_bool_buf(n);

    uint8_t* out_null = nullptr;
    if (src_null != nullptr) {
        try {
            out_null = cmp_copy_validity(src_null, n);
        } catch (...) {
            draken_free(dst);
            throw;
        }
    }

    // Hash rows in ≤1024-element chunks (matches hash_string block size).
    // Null rows are hashed from their slot position — whatever is there — but
    // never probed (validity gate below prevents it). Result bit stays 0 from
    // zeroed allocation; validity bit stays 0 from copied validity.
    uint64_t seeds[1024];
    uint64_t hashes[1024];

    uint32_t i = 0;
    while (i < n) {
        const uint32_t block = (n - i < 1024u) ? (n - i) : 1024u;

        for (uint32_t j = 0; j < block; ++j)
            seeds[j] = str_hash_seed(&slots[v.selection[i + j]], sa->arena);

        simd_hash_i64(seeds, hashes, block);

        if (src_null == nullptr) {
            for (uint32_t j = 0; j < block; ++j) {
                if (set.contains(hashes[j]))
                    dst[(i + j) >> 3] |= static_cast<uint8_t>(1u << ((i + j) & 7));
            }
        } else {
            for (uint32_t j = 0; j < block; ++j) {
                const uint32_t row = i + j;
                if ((src_null[row >> 3] >> (row & 7)) & 1u) {
                    if (set.contains(hashes[j]))
                        dst[row >> 3] |= static_cast<uint8_t>(1u << (row & 7));
                }
            }
        }

        i += block;
    }

    VecResult r;
    r.data           = dst;
    r.validity       = out_null;
    r.selection      = draken_identity_sel(n);
    r.owns_selection = false;
    r.data_length    = n;
    r.length         = n;
    r.type           = DRAKEN_BOOL;
    r.flags          = static_cast<uint8_t>(DRAKEN_SEL_IDENTITY | DRAKEN_SEL_PERMUTATION);
    return r;
}

}} // namespace draken::ops
