#pragma once
// src/cpp/engine/native_key_hash.hpp — the shared equi-key hash.
//
// One function: compute_row_hashes. Draken owns the key hash (cxx_hash_c); every
// keyed operator — the joins (native_join2.hpp) and GROUP BY / DISTINCT
// (native_group_sinks.hpp) — derives its per-row 64-bit key from here so they
// cannot disagree about what "the same key" means. Equality is 64-bit hash
// identity, so no key bytes are stored or compared.
//
// This file was `native_hash_join.hpp` and held the v1 hash join plus the
// row-store that materialized a join's build-side payload. The v1 join was dead
// (superseded by native_join2.hpp) and is deleted; the row-store moved to
// native_group_sinks.hpp, its only remaining consumer, as the GROUP BY key store.

#include <cstdint>
#include <cstring>
#include <memory>
#include <vector>

#include "executor.hpp"
#include "native_sort.hpp"     // sort_row_valid, string_arena_of, canonical string layout
#include "core/string_slot.h"  // DrakenStringSlot — string payload columns
#include "core/alloc.h"        // draken_malloc / draken_free
#include "core/vector_owner.h" // VectorOwner, OwnedBuffer
#include "morsels/cxx_hash.h"  // cxx_hash_c — draken owns the join-key hash

namespace opteryx::engine {

// Draken owns the join-key hash (shared by the single- and multi-column joins).
// cxx_hash_c hashes the given key columns of morsel `in` into a dense per-row
// uint64 hash. Equality is 64-bit hash identity (the sanctioned join contract) —
// no key bytes are stored or compared, and width-stability across integer tiers
// comes from draken's hash. NULL rows hash to the NULL_HASH sentinel; the caller
// still excludes them from build/probe by reading the KEY column validity.
// Shape-preserving variant: hands back draken's hash vector AS IT IS, without
// the densifying gather that compute_row_hashes performs.
//
// For a SINGLE key, cxx_hash_c is shape-preserving — a dict/compressed key
// yields `data_length` distinct hashes addressed by `selection` codes ("hash
// each distinct value once", see cxx_hash.h). compute_row_hashes then does
// `out[i] = khashes[codes[i]]`, which throws that structure away one line after
// draken produced it: the caller ends up probing once per ROW to rediscover
// groups that were already addressed once per DISTINCT VALUE. On a 65,536-row
// morsel of RegionID that is ~2-5k distinct hashes expanded to 65,536 probes.
//
// The caller owns the returned morsel and MUST free it with cxx_morsel_delete
// AFTER it has finished with `hashes`/`codes` — both point into it.
struct ShapedKeyHash {
    CxxMorsel*      owner = nullptr;   // free with cxx_morsel_delete
    const uint64_t* hashes = nullptr;  // data_length distinct hashes
    const uint32_t* codes = nullptr;   // one code per row (never NULL)
    uint32_t        data_length = 0;   // distinct hashes
    uint32_t        length = 0;        // rows
    bool compressed() const { return data_length < length; }
};

inline bool compute_row_hashes_shaped(const MorselPtr& in,
                                      const std::vector<size_t>& key_idx,
                                      ShapedKeyHash& out, ErrCtx& err) {
    std::vector<int32_t> col_idxs(key_idx.size());
    for (size_t k = 0; k < key_idx.size(); ++k)
        col_idxs[k] = static_cast<int32_t>(key_idx[k]);
    CxxMorsel* hm = cxx_hash_c(in.get(), col_idxs.data(),
                               static_cast<uint32_t>(key_idx.size()));
    if (hm == nullptr) {
        err.code = 1;
        err.msg = "native GROUP BY: cxx_hash_c allocation failed";
        return false;
    }
    const DrakenVector& hv = hm->columns[0].view;
    out.owner = hm;
    out.hashes = static_cast<const uint64_t*>(hv.data);
    out.codes = hv.selection;          // draken invariant: never NULL
    out.data_length = hv.data_length;
    out.length = hv.length;
    return true;
}

inline bool compute_row_hashes(const MorselPtr& in, const std::vector<size_t>& key_idx,
                               std::vector<uint64_t>& out, ErrCtx& err) {
    uint32_t n = in->num_rows();
    std::vector<int32_t> col_idxs(key_idx.size());
    for (size_t k = 0; k < key_idx.size(); ++k)
        col_idxs[k] = static_cast<int32_t>(key_idx[k]);
    CxxMorsel* hm = cxx_hash_c(in.get(), col_idxs.data(),
                               static_cast<uint32_t>(key_idx.size()));
    if (hm == nullptr) {
        err.code = 1;
        err.msg = "native JOIN: cxx_hash_c allocation failed";
        return false;
    }
    const DrakenVector& hv = hm->columns[0].view;
    const uint64_t* khashes = static_cast<const uint64_t*>(hv.data);
    const uint32_t* codes = hv.selection;   // never NULL (draken invariant)
    out.resize(n);
    for (uint32_t i = 0; i < n; ++i) out[i] = khashes[codes[i]];
    cxx_morsel_delete(hm);
    return true;
}

// Byte width of one value of `t` in this engine's supported payload type set.
// DrakenStringSlot is a 16-byte POD (draken/core/string_slot.h) — treating it
// as "just another fixed-width element" lets the row-store be one generic
// byte-vector implementation instead of one per type.
// NOTE for DRAKEN_BOOL: this is the ROW-STORE stride, not the vector width. A
// BOOL vector's `data` is bit-packed (one bit per element, see buffers.h), but
// the row-store holds one unpacked 0/1 byte per row so the generic byte-vector
// append and local->global merge work unchanged. `append_row` unpacks on the way
// in and the emit paths re-pack on the way out; both go through
// `join_type_is_bool`. Never memcpy a BOOL payload by `elem_size`.

}  // namespace opteryx::engine
