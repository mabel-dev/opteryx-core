#pragma once
// draken/ops/int64_predicates.h — between and in_list for int64 (Milestone C.4).
//
// BETWEEN
//   i64_between(v, lo, hi, lo_incl, hi_incl) → DRAKEN_BOOL VecResult.
//   Single fused pass: (lo ≤/< v ≤/< hi). The inclusivity pair is hoisted
//   outside the hot loop via a 4-way compile-time dispatch; no per-row branch
//   on inclusivity. Same 8-way byte-pack technique as int64_compare.h so the
//   compiler sees a clean dependency graph for NEON/AVX2 auto-vectorisation.
//
// IN_LIST
//   i64_in_list(v, set) → DRAKEN_BOOL VecResult.
//   Hash-only membership via CarcharSet bulk probe.
//
//   §1 EXCEPTION (design docs 02 / 07): CarcharSet stores 64-bit hashes only —
//   no key verification.  A hash collision admits a wrong row.  This is accepted
//   at our data volumes.  The exception is documented HERE (call site) and is
//   NOT silent.
//
//   The hash path is the SINGLE SHARED PATH used by the hash op and joins:
//   raw = static_cast<uint64_t>(value), then simd_hash_i64(&raw, &h, 1).
//   No alternative raw-key branch is permitted; any deviation would cause
//   set-building and join-side hashes to diverge silently.
//
// NULL SEMANTICS (TVL, both ops)
//   Null input row → null output row (validity 0, result bit 0).
//   Output validity == copy of input validity; nullptr when input has no nulls.
//
// BIT-BOUNDARY CORRECTNESS
//   cmp_alloc_bool_buf() zero-initialises the full padded allocation.
//   Partial tail bytes accumulate only via OR — they start at 0.
//   No read past ceil(n/8).  Tested at sizes 1..9 (test_int64_predicates.py).
//
// ALLOCATOR DISCIPLINE
//   between: output buffer owned via mimalloc (draken_malloc/draken_free).
//   in_list: the CarcharSet is constructed entirely from std::vector (system heap)
//   and passed by const reference — no buffer crosses allocator boundaries.

#include <cstdint>
#include <cstring>
#include <stdexcept>

#include <new>        // std::bad_alloc / placement new — not reliably pulled in by <stdexcept> on stricter libc++
#include "core/buffers.h"
#include "core/alloc.h"
#include "core/vector_alloc.h"
#include "ops/vec_result.h"
#include "ops/int64_compare.h"  // cmp_alloc_bool_buf, cmp_copy_validity
#include "simd_hash.h"          // simd_hash_i64 — shared hash path
#include "carchar_set.hpp"      // opteryx::carchar::CarcharSet

namespace draken { namespace ops {

// ---------------------------------------------------------------------------
// between — compile-time specialisation on inclusivity (4 combinations)
//
// BetweenOp<lo_incl, hi_incl>::apply(v, lo, hi)
//   lo_incl=true:  lo <= v       lo_incl=false: lo < v
//   hi_incl=true:  v  <= hi      hi_incl=false: v  < hi
// The compile-time booleans eliminate the branches at the instruction level.
// ---------------------------------------------------------------------------

template<bool lo_incl, bool hi_incl>
struct BetweenOp {
    static inline bool apply(int64_t v, int64_t lo, int64_t hi) noexcept {
        const bool lo_ok = lo_incl ? (lo <= v) : (lo < v);
        const bool hi_ok = hi_incl ? (v <= hi) : (v < hi);
        return lo_ok & hi_ok;
    }
};

// Inner kernel: 8-way byte-pack, two paths (non-null / null).
// dst must be pre-zeroed (cmp_alloc_bool_buf guarantees this).
// Identity == true indexes data[pos] directly (vectorisable); false gathers
// data[selection[pos]]. Same answer (hint-based dispatch, §11).
template<bool lo_incl, bool hi_incl, bool Identity>
static inline void between_kernel(
    const int64_t*  data,
    const uint32_t* selection,
    int64_t         lo,
    int64_t         hi,
    const uint8_t*  src_null,
    uint8_t*        dst,
    uint32_t        n)
{
    using Op = BetweenOp<lo_incl, hi_incl>;
    const uint32_t whole_bytes = n >> 3;
    auto at = [&](uint32_t pos) -> int64_t {
        if constexpr (Identity) return data[pos];
        else                    return data[selection[pos]];
    };

    if (src_null == nullptr) {
        for (uint32_t b = 0; b < whole_bytes; ++b) {
            const uint32_t base = b << 3;
            dst[b] = static_cast<uint8_t>(
                (static_cast<unsigned>(Op::apply(at(base+0), lo, hi)) << 0) |
                (static_cast<unsigned>(Op::apply(at(base+1), lo, hi)) << 1) |
                (static_cast<unsigned>(Op::apply(at(base+2), lo, hi)) << 2) |
                (static_cast<unsigned>(Op::apply(at(base+3), lo, hi)) << 3) |
                (static_cast<unsigned>(Op::apply(at(base+4), lo, hi)) << 4) |
                (static_cast<unsigned>(Op::apply(at(base+5), lo, hi)) << 5) |
                (static_cast<unsigned>(Op::apply(at(base+6), lo, hi)) << 6) |
                (static_cast<unsigned>(Op::apply(at(base+7), lo, hi)) << 7));
        }
        for (uint32_t i = whole_bytes << 3; i < n; ++i) {
            if (Op::apply(at(i), lo, hi))
                dst[i >> 3] |= static_cast<uint8_t>(1u << (i & 7));
        }
    } else {
        // Branchless: AND packed result with validity byte so null rows → 0.
        for (uint32_t b = 0; b < whole_bytes; ++b) {
            const uint32_t base = b << 3;
            const uint8_t m = static_cast<uint8_t>(
                (static_cast<unsigned>(Op::apply(at(base+0), lo, hi)) << 0) |
                (static_cast<unsigned>(Op::apply(at(base+1), lo, hi)) << 1) |
                (static_cast<unsigned>(Op::apply(at(base+2), lo, hi)) << 2) |
                (static_cast<unsigned>(Op::apply(at(base+3), lo, hi)) << 3) |
                (static_cast<unsigned>(Op::apply(at(base+4), lo, hi)) << 4) |
                (static_cast<unsigned>(Op::apply(at(base+5), lo, hi)) << 5) |
                (static_cast<unsigned>(Op::apply(at(base+6), lo, hi)) << 6) |
                (static_cast<unsigned>(Op::apply(at(base+7), lo, hi)) << 7));
            dst[b] = static_cast<uint8_t>(m & src_null[b]);
        }
        for (uint32_t i = whole_bytes << 3; i < n; ++i) {
            if ((src_null[i >> 3] >> (i & 7)) & 1u) {
                if (Op::apply(at(i), lo, hi))
                    dst[i >> 3] |= static_cast<uint8_t>(1u << (i & 7));
            }
        }
    }
}

template<bool lo_incl, bool hi_incl>
static inline VecResult between_impl(
    const DrakenVector& v, int64_t lo, int64_t hi)
{
    const uint32_t  n        = v.length;
    const int64_t*  data     = static_cast<const int64_t*>(v.data);
    const uint8_t*  src_null = v.validity;

    if (draken_is_constant(&v))
        return cmp_constant_bool_result(
            BetweenOp<lo_incl, hi_incl>::apply(data[0], lo, hi), src_null, n);

    if (draken_is_dict(&v)) {
        const uint32_t dl = v.data_length;
        uint8_t* db = static_cast<uint8_t*>(draken_malloc(dl));
        if (!db) throw std::bad_alloc();
        for (uint32_t k = 0; k < dl; ++k)
            db[k] = BetweenOp<lo_incl, hi_incl>::apply(data[k], lo, hi) ? 1u : 0u;
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

    // Identity-gated: contiguous direct-index when selection is identity.
    if (v.flags & DRAKEN_SEL_IDENTITY)
        between_kernel<lo_incl, hi_incl, true >(data, v.selection, lo, hi, src_null, dst, n);
    else
        between_kernel<lo_incl, hi_incl, false>(data, v.selection, lo, hi, src_null, dst, n);

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
static inline VecResult i64_between(
    const DrakenVector& v,
    int64_t lo, int64_t hi,
    bool lo_incl, bool hi_incl)
{
    if (lo_incl) {
        if (hi_incl) return between_impl<true,  true >(v, lo, hi);
        else         return between_impl<true,  false>(v, lo, hi);
    } else {
        if (hi_incl) return between_impl<false, true >(v, lo, hi);
        else         return between_impl<false, false>(v, lo, hi);
    }
}

// ---------------------------------------------------------------------------
// in_list — hash-only membership via CarcharSet bulk probe.
//
// §1 EXCEPTION (see file header): hash-only; no key verification.
// The hash used here MUST be the same simd_hash_i64 path used by the hash op
// and joins so that set-building and probe hashes are computed identically.
// ---------------------------------------------------------------------------

static inline VecResult i64_in_list(
    const DrakenVector& v,
    const opteryx::carchar::CarcharSet& set)
{
    const uint32_t  n        = v.length;
    const int64_t*  data     = static_cast<const int64_t*>(v.data);
    const uint8_t*  src_null = v.validity;

    if (draken_is_constant(&v)) {
        uint64_t raw = static_cast<uint64_t>(data[0]);
        uint64_t h;
        simd_hash_i64(&raw, &h, 1);
        return cmp_constant_bool_result(set.contains(h), src_null, n);
    }

    if (draken_is_dict(&v)) {
        const uint32_t dl = v.data_length;
        uint8_t* db = static_cast<uint8_t*>(draken_malloc(dl));
        if (!db) throw std::bad_alloc();
        uint64_t scratch[1024], hashes[1024];
        uint32_t done = 0;
        while (done < dl) {
            const uint32_t block = (dl - done < 1024u) ? (dl - done) : 1024u;
            for (uint32_t j = 0; j < block; ++j)
                scratch[j] = static_cast<uint64_t>(data[done + j]);
            simd_hash_i64(scratch, hashes, block);
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

    // Hash rows in 1024-element chunks (matches hash_int64 block size),
    // then probe the set per row. Null rows are skipped — their result bit
    // stays 0 from the zeroed allocation; their validity bit stays 0 from
    // the copied validity.
    uint64_t scratch[1024];
    uint64_t hashes[1024];

    uint32_t i = 0;
    while (i < n) {
        const uint32_t block = (n - i < 1024u) ? (n - i) : 1024u;

        // Gather raw values as uint64 (same cast as hash_int64).
        for (uint32_t j = 0; j < block; ++j)
            scratch[j] = static_cast<uint64_t>(data[v.selection[i + j]]);

        // Hash the block via the shared path.
        simd_hash_i64(scratch, hashes, block);

        // Probe: emit membership bit only for valid rows.
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
