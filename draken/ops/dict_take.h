#pragma once
// draken/ops/dict_take.h — compression-preserving + compacting take for
// fixed-width vectors.
//
// When a compressed (dict/constant) vector is gathered and its value array (k
// entries) is no larger than the output (k <= n), we keep the compressed shape
// instead of materialising n values. While gathering the per-row CODES we mark a
// seen-bitmap over the k slots (valid rows only), popcount it to d, and:
//
//   d == k  -> no dead entries: copy all k values, keep the gathered codes.
//   1<=d<k  -> COMPACT: copy the d live values (in ascending old-code order, so a
//              sorted dictionary stays sorted) and remap codes via a prefix-rank.
//   d == 0  -> every taken row null: a constant-shape all-null vector.
//
// The output's every code in [0,data_length) is referenced by a valid row, so we
// set DRAKEN_DICT_CODES_DENSE; the value array is a (sub)sequence of the input's,
// so DRAKEN_DICT_KEYS_SORTED is carried through. With both set, data[0] /
// data[data_length-1] are the column min / max (the "ends" shortcut).
//
// Cost: one fused gather pass (free — take scans the codes anyway) + an O(k/32)
// popcount (the cheap "should we compact" decision) + an O(k) value copy + one
// remap pass over the codes when compacting.

#include <cstdint>
#include <cstring>
#include <stdexcept>
#include "core/buffers.h"
#include "core/alloc.h"
#include "ops/vec_result.h"

namespace draken { namespace ops {

// Free out_null if every logical-row bit is set (all valid) — matches the
// per-kernel normalize_validity convention so an all-valid result has NULL validity.
static inline uint8_t* dt_normalize_validity(uint8_t* validity, uint32_t n) noexcept {
    if (validity == nullptr) return nullptr;
    const uint32_t nb = (n + 7u) >> 3;
    for (uint32_t b = 0; b < nb; ++b) {
        uint8_t expected = 0xFFu;
        if (b == nb - 1u && (n & 7u) != 0u)
            expected = static_cast<uint8_t>((1u << (n & 7u)) - 1u);
        if (validity[b] != expected) return validity;
    }
    draken_free(validity);
    return nullptr;
}

// Precondition: caller verified k == v.data_length <= n AND draken_is_compressed(&v).
template<typename T, DrakenType TAG>
static inline VecResult fixed_dict_compact_take(
    const DrakenVector& v, const int32_t* indices, uint32_t n)
{
    const uint32_t k        = v.data_length;
    const T*       data     = static_cast<const T*>(v.data);
    const uint8_t* src_null = v.validity;

    uint32_t* codes = static_cast<uint32_t*>(draken_malloc((n > 0u ? n : 1u) * sizeof(uint32_t)));
    if (!codes) throw std::bad_alloc();

    const uint32_t words = (k + 31u) >> 5;
    uint32_t* seen = static_cast<uint32_t*>(draken_malloc((words > 0u ? words : 1u) * sizeof(uint32_t)));
    if (!seen) { draken_free(codes); throw std::bad_alloc(); }
    std::memset(seen, 0, (words > 0u ? words : 1u) * sizeof(uint32_t));

    uint8_t* out_null = nullptr;
    const uint32_t nbytes = (n + 7u) >> 3;
    if (src_null != nullptr && n > 0u) {
        out_null = static_cast<uint8_t*>(draken_malloc(nbytes > 0u ? nbytes : 1u));
        if (!out_null) { draken_free(seen); draken_free(codes); throw std::bad_alloc(); }
        std::memset(out_null, 0, nbytes > 0u ? nbytes : 1u);
    }

    // Pass 1 (fused gather + seen-mark, valid rows only).
    for (uint32_t i = 0; i < n; ++i) {
        const uint32_t si = static_cast<uint32_t>(indices[i]);
        const bool valid = (src_null == nullptr) || ((src_null[si >> 3] >> (si & 7)) & 1u);
        if (!valid) {
            codes[i] = 0u;
        } else {
            const uint32_t c = v.selection[si];
            codes[i] = c;
            seen[c >> 5] |= (1u << (c & 31u));
            if (out_null != nullptr) out_null[i >> 3] |= static_cast<uint8_t>(1u << (i & 7u));
        }
    }
    out_null = dt_normalize_validity(out_null, n);

    uint32_t d = 0u;
    for (uint32_t w = 0; w < words; ++w) d += static_cast<uint32_t>(__builtin_popcount(seen[w]));

    const uint8_t dense_sorted = static_cast<uint8_t>(
        (v.flags & DRAKEN_DICT_KEYS_SORTED) | DRAKEN_DICT_CODES_DENSE);

    VecResult r;
    r.validity = out_null; r.selection = codes; r.owns_selection = true;
    r.length = n; r.type = TAG;

    if (d == k) {
        // No dead entries — keep the full value array + gathered codes.
        draken_free(seen);
        T* vals = static_cast<T*>(draken_malloc((k > 0u ? k : 1u) * sizeof(T)));
        if (!vals) { draken_free(out_null); draken_free(codes); throw std::bad_alloc(); }
        std::memcpy(vals, data, static_cast<size_t>(k) * sizeof(T));
        r.data = vals; r.data_length = k; r.flags = dense_sorted;
        return r;
    }

    if (d == 0u) {
        // Every taken row is null → constant-shape all-null vector.
        draken_free(seen);
        T* vals = static_cast<T*>(draken_malloc(sizeof(T)));
        if (!vals) { draken_free(out_null); draken_free(codes); throw std::bad_alloc(); }
        vals[0] = T(0);
        r.data = vals; r.data_length = 1u;
        r.flags = static_cast<uint8_t>(v.flags & DRAKEN_DICT_KEYS_SORTED);
        return r;
    }

    // Compact: live values copied in ascending old-code order (preserves sort).
    T* vals = static_cast<T*>(draken_malloc(static_cast<size_t>(d) * sizeof(T)));
    if (!vals) { draken_free(seen); draken_free(out_null); draken_free(codes); throw std::bad_alloc(); }
    uint32_t* rank = static_cast<uint32_t*>(draken_malloc(static_cast<size_t>(k) * sizeof(uint32_t)));
    if (!rank) { draken_free(vals); draken_free(seen); draken_free(out_null); draken_free(codes); throw std::bad_alloc(); }
    uint32_t next = 0u;
    for (uint32_t c = 0; c < k; ++c) {
        if ((seen[c >> 5] >> (c & 31u)) & 1u) {
            rank[c] = next;
            vals[next] = data[c];
            ++next;
        }
    }
    // Pass 2: remap codes. Valid rows → rank[code]; null rows → 0 (valid since d>=1).
    for (uint32_t i = 0; i < n; ++i) {
        const bool valid = (out_null == nullptr) || ((out_null[i >> 3] >> (i & 7)) & 1u);
        codes[i] = valid ? rank[codes[i]] : 0u;
    }
    draken_free(rank);
    draken_free(seen);

    r.data = vals; r.data_length = d; r.flags = dense_sorted;
    return r;
}

}} // namespace draken::ops
