#pragma once
// draken/ops/string_predicates.h — string in_list (Milestone D.4).
//
// str_in_list(v, set) → bit-packed DRAKEN_BOOL VecResult.
//
// Set membership via hash-only CarcharSet probe.
//
// §1 EXCEPTION (design doc 02, architect sign-off; SAME exception as string
// eq and string hash — not a new decision):
//   SHORT strings (len ≤ 12): full content is in the hash seed → effectively
//     collision-free within any realistic data set.
//   LONG strings (len > 12): hash seed derived from (length, prefix, hash32)
//     only; no arena fetch. Two distinct long strings sharing all three fields
//     produce the same row hash → false match. Probability ≈ 2⁻³² per pair
//     sharing length + prefix. Accepted trade-off; documented here, not silent.
//
// HASH PATH: str_hash_seed → simd_hash_i64 — identical to the hash_string
// kernel and to the nanobind binding's set-building loop. Any path divergence
// between set-build and probe causes present values to miss silently; do NOT
// introduce an alternative hash path.
//
// NULL SEMANTICS (TVL): null row → null output row (validity bit 0, result
// bit 0). Output validity is a copy of input validity; nullptr when no nulls.
//
// ACCESS PATTERN: uniform slots[v.selection[i]] for i in [0, v.length).
// No shape discrimination — works for dense and dict automatically.
//
// ALLOCATOR DISCIPLINE: output buffer owned by mimalloc (draken_malloc).
// CarcharSet is constructed and owned by the caller (std::vector storage);
// passed by const reference — no allocator-boundary crossing.

#include <cstdint>
#include <cstring>

#include "core/buffers.h"       // DrakenVector, DrakenStringArena, DRAKEN_BOOL …
#include "core/alloc.h"
#include "core/vector_alloc.h"
#include "core/string_slot.h"   // DrakenStringSlot, str_is_inline
#include "ops/vec_result.h"
#include "ops/int64_compare.h"  // cmp_alloc_bool_buf, cmp_copy_validity
#include "ops/string_hash.h"    // str_hash_seed — must match set-build path exactly
#include "simd_hash.h"          // simd_hash_i64, NULL_HASH
#include "carchar_set.hpp"      // opteryx::carchar::CarcharSet

namespace draken { namespace ops {

// ---------------------------------------------------------------------------
// str_in_list — hash-only membership via CarcharSet bulk probe.
//
// §1 EXCEPTION (see file header): hash-only; no key verification.
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
            seeds[j] = str_hash_seed(&slots[v.selection[i + j]]);

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
