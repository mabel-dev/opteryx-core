#pragma once
// draken/ops/int64_gather.h — take / materialize / compress for int64 vectors.
//
// take(v, indices, n):
//   Gather v[indices[i]] for i in [0,n). Indices are logical-row positions.
//   Result is dense-identity. Validity is propagated: null source row → null output row.
//   Normalization: output validity is nullptr when no output rows are null.
//
// materialize(v):
//   Expand any shape (dense/constant/dict) to an owned dense vector by running
//   data[selection[i]] for all i. Validity is copied. Result always has IDENTITY flag.
//
// compress(v):
//   Dict-encode an int64 vector. Finds unique non-null values (in order of first
//   appearance), assigns codes 0..k-1. Null rows get code 0 but are marked null in
//   validity. Returns a dict-encoded VecResult.
//   Special case — all-null or empty: returns a constant-shape vector (data_length=1,
//   data[0]=0) with the original validity to avoid a zero-size dict.
//
// Round-trip guarantee: materialize(compress(v)) produces the same logical values as v.

#include <stdint.h>
#include <stddef.h>
#include <string.h>
#include <stdexcept>
#include <unordered_map>
#include <vector>
#include <new>        // std::bad_alloc / placement new — not reliably pulled in by <stdexcept> on stricter libc++
#include "core/buffers.h"
#include "core/alloc.h"
#include "core/vector_alloc.h"
#include "ops/vec_result.h"
#include "ops/dict_take.h"

namespace draken { namespace ops {

// ---------------------------------------------------------------------------
// Internal: validity helpers
// ---------------------------------------------------------------------------

static inline bool val_row(const uint8_t* validity, uint32_t i) noexcept {
    return (validity == nullptr) || ((validity[i >> 3] >> (i & 7)) & 1u);
}

static inline void set_valid_bit(uint8_t* bitmap, uint32_t i) noexcept {
    bitmap[i >> 3] |= static_cast<uint8_t>(1u << (i & 7));
}

// Normalize: if every bit in the bitmap is set (all valid), free it and return nullptr.
static inline uint8_t* normalize_validity(uint8_t* validity, uint32_t n) noexcept {
    if (validity == nullptr) return nullptr;
    const uint32_t nb = (n + 7u) >> 3;
    for (uint32_t k = 0; k < nb; ++k) {
        uint8_t expected = 0xFFu;
        if (k == nb - 1 && (n & 7u) != 0)
            expected = static_cast<uint8_t>((1u << (n & 7u)) - 1u);
        if (validity[k] != expected) return validity;
    }
    draken_free(validity);
    return nullptr;
}

// ---------------------------------------------------------------------------
// VALIDITY RANGE COPY — extract bits [start, start+n) from src into dst[0..n).
// ---------------------------------------------------------------------------
static inline void copy_validity_range(uint8_t* dst, const uint8_t* src,
                                        uint32_t start, uint32_t n) noexcept {
    if (n == 0) return;
    const uint32_t nb = (n + 7u) >> 3;
    if ((start & 7u) == 0) {
        std::memcpy(dst, src + (start >> 3), nb);
    } else {
        const uint32_t shift = start & 7u;
        const uint32_t byte0 = start >> 3;
        const uint32_t last_src_byte = (start + n - 1u) >> 3;
        for (uint32_t i = 0; i < nb; ++i) {
            const uint8_t lo = src[byte0 + i] >> shift;
            const uint8_t hi = (byte0 + i < last_src_byte)
                               ? src[byte0 + i + 1] << (8u - shift) : 0u;
            dst[i] = lo | hi;
        }
    }
    if (n & 7u) dst[nb - 1] &= static_cast<uint8_t>((1u << (n & 7u)) - 1u);
}

// ---------------------------------------------------------------------------
// TAKE
// ---------------------------------------------------------------------------
static inline VecResult i64_take(
    const DrakenVector& v, const int32_t* indices, uint32_t n_indices)
{
    const uint32_t n   = n_indices;
    const int64_t* data = static_cast<const int64_t*>(v.data);
    const uint8_t* src_null = v.validity;

    // Compression-preserving + compacting gather (see ops/dict_take.h): when the
    // value array is no larger than the output and the input is compressed, keep
    // (and compact away any dead entries from) the dict shape instead of
    // materialising n values.
    if (v.data_length <= n && draken_is_compressed(&v))
        return fixed_dict_compact_take<int64_t, DRAKEN_INT64>(v, indices, n);

    // Allocate output data (at least 1 element to keep data non-null).
    size_t data_bytes = (n > 0 ? n : 1u) * sizeof(int64_t);
    int64_t* dst = static_cast<int64_t*>(draken_malloc(data_bytes));
    if (!dst) throw std::bad_alloc();

    uint8_t* out_null = nullptr;
    if (src_null != nullptr && n > 0) {
        uint32_t nb = (n + 7u) >> 3;
        out_null = static_cast<uint8_t*>(draken_malloc(nb));
        if (!out_null) { draken_free(dst); throw std::bad_alloc(); }
        memset(out_null, 0, nb);
    }

    for (uint32_t i = 0; i < n; ++i) {
        uint32_t src_idx = static_cast<uint32_t>(indices[i]);
        if (!val_row(src_null, src_idx)) {
            dst[i] = 0;
        } else {
            dst[i] = data[v.selection[src_idx]];
            if (out_null != nullptr) set_valid_bit(out_null, i);
        }
    }

    out_null = normalize_validity(out_null, n);

    VecResult r;
    r.data           = dst;
    r.validity       = out_null;
    r.selection      = draken_identity_sel(n);
    r.owns_selection = false;
    r.data_length    = n;
    r.length         = n;
    r.type           = DRAKEN_INT64;
    r.flags          = static_cast<uint8_t>(DRAKEN_SEL_IDENTITY | DRAKEN_SEL_PERMUTATION);
    return r;
}

// ---------------------------------------------------------------------------
// SLICE — contiguous range [start, start+length). Fast memcpy for dense.
// ---------------------------------------------------------------------------
static inline VecResult i64_slice(const DrakenVector& v, uint32_t start, uint32_t length) {
    const uint32_t n        = length;
    const int64_t* data     = static_cast<const int64_t*>(v.data);
    const uint8_t* src_null = v.validity;

    int64_t* dst = static_cast<int64_t*>(draken_malloc((n > 0u ? n : 1u) * sizeof(int64_t)));
    if (!dst) throw std::bad_alloc();

    // Physical memcpy is valid ONLY when selection is identity (selection[i]==i).
    // data_length==length alone admits a PERMUTATION (e.g. take after sort), which
    // would memcpy rows in physical order and silently reorder/drop. Require the
    // IDENTITY flag; permutations fall through to the selection-honouring path.
    if (draken_is_dense(&v) && (v.flags & DRAKEN_SEL_IDENTITY)) {
        std::memcpy(dst, data + start, n * sizeof(int64_t));
    } else {
        for (uint32_t i = 0; i < n; ++i)
            dst[i] = data[v.selection[start + i]];
    }

    uint8_t* out_null = nullptr;
    if (src_null != nullptr && n > 0u) {
        const uint32_t nb = (n + 7u) >> 3;
        out_null = static_cast<uint8_t*>(draken_malloc(nb));
        if (!out_null) { draken_free(dst); throw std::bad_alloc(); }
        copy_validity_range(out_null, src_null, start, n);
        out_null = normalize_validity(out_null, n);
    }

    VecResult r;
    r.data           = dst;
    r.validity       = out_null;
    r.selection      = draken_identity_sel(n);
    r.owns_selection = false;
    r.data_length    = n;
    r.length         = n;
    r.type           = DRAKEN_INT64;
    r.flags          = static_cast<uint8_t>(DRAKEN_SEL_IDENTITY | DRAKEN_SEL_PERMUTATION);
    return r;
}

// ---------------------------------------------------------------------------
// MATERIALIZE
// ---------------------------------------------------------------------------
static inline VecResult i64_materialize(const DrakenVector& v) {
    const uint32_t n    = v.length;
    const int64_t* data = static_cast<const int64_t*>(v.data);
    const uint8_t* src_null = v.validity;

    size_t data_bytes = (n > 0 ? n : 1u) * sizeof(int64_t);
    int64_t* dst = static_cast<int64_t*>(draken_malloc(data_bytes));
    if (!dst) throw std::bad_alloc();

    // Uniform access: data[selection[i]].
    for (uint32_t i = 0; i < n; ++i) {
        if (src_null != nullptr && !((src_null[i >> 3] >> (i & 7)) & 1u)) {
            dst[i] = 0;
        } else {
            dst[i] = data[v.selection[i]];
        }
    }

    uint8_t* out_null = nullptr;
    if (src_null != nullptr && n > 0) {
        uint32_t nb = (n + 7u) >> 3;
        out_null = static_cast<uint8_t*>(draken_malloc(nb));
        if (!out_null) { draken_free(dst); throw std::bad_alloc(); }
        memcpy(out_null, src_null, nb);
    }

    VecResult r;
    r.data           = dst;
    r.validity       = out_null;
    r.selection      = draken_identity_sel(n);
    r.owns_selection = false;
    r.data_length    = n;
    r.length         = n;
    r.type           = DRAKEN_INT64;
    r.flags          = static_cast<uint8_t>(DRAKEN_SEL_IDENTITY | DRAKEN_SEL_PERMUTATION);
    return r;
}

// ---------------------------------------------------------------------------
// COMPRESS — dict-encode an int64 vector.
//
// Algorithm: iterate all non-null logical values in order, collect unique values
// (first-appearance order), assign codes 0..k-1. Null rows get code 0 but are
// marked null in the validity bitmap, so their data value is never observed.
//
// All-null / empty vectors return a constant-shape result (data_length=1) with
// the original validity to avoid a zero-length dict (which breaks the access
// model since any selection code would be out-of-bounds).
// ---------------------------------------------------------------------------
static inline VecResult i64_compress(const DrakenVector& v) {
    const uint32_t n    = v.length;
    const int64_t* data = static_cast<const int64_t*>(v.data);
    const uint8_t* src_null = v.validity;

    // Edge case: empty vector — return empty dense result.
    if (n == 0) {
        int64_t* d = static_cast<int64_t*>(draken_malloc(sizeof(int64_t)));
        if (!d) throw std::bad_alloc();
        d[0] = 0;
        VecResult r;
        r.data           = d;
        r.validity       = nullptr;
        r.selection      = draken_identity_sel(0);
        r.owns_selection = false;
        r.data_length    = 0;
        r.length         = 0;
        r.type           = DRAKEN_INT64;
        r.flags          = 0;
        return r;
    }

    // Phase 1: collect unique non-null values in first-appearance order.
    std::unordered_map<int64_t, uint32_t> value_to_code;
    // Reserve a bit to reduce rehashing.
    value_to_code.reserve(n < 256u ? n : 256u);

    std::vector<int64_t> dict_values;
    for (uint32_t i = 0; i < n; ++i) {
        if (!val_row(src_null, i)) continue;
        int64_t val = data[v.selection[i]];
        if (value_to_code.find(val) == value_to_code.end()) {
            uint32_t code = static_cast<uint32_t>(dict_values.size());
            value_to_code[val] = code;
            dict_values.push_back(val);
        }
    }

    const uint32_t dict_size = static_cast<uint32_t>(dict_values.size());

    // All-null: return constant-shape result (one dummy value, all rows null).
    if (dict_size == 0) {
        int64_t* d = static_cast<int64_t*>(draken_malloc(sizeof(int64_t)));
        if (!d) throw std::bad_alloc();
        d[0] = 0;

        uint8_t* out_null = nullptr;
        if (src_null != nullptr) {
            uint32_t nb = (n + 7u) >> 3;
            out_null = static_cast<uint8_t*>(draken_malloc(nb));
            if (!out_null) { draken_free(d); throw std::bad_alloc(); }
            memcpy(out_null, src_null, nb);
        }

        VecResult r;
        r.data           = d;
        r.validity       = out_null;
        r.selection      = draken_zero_sel(n);
        r.owns_selection = false;
        r.data_length    = 1;
        r.length         = n;
        r.type           = DRAKEN_INT64;
        r.flags          = 0;
        return r;
    }

    // Phase 2: allocate dict buffer and copy unique values.
    int64_t* dict_buf = static_cast<int64_t*>(
        draken_malloc(dict_size * sizeof(int64_t)));
    if (!dict_buf) throw std::bad_alloc();
    for (uint32_t k = 0; k < dict_size; ++k) dict_buf[k] = dict_values[k];

    // Phase 3: build codes array.
    uint32_t* codes = static_cast<uint32_t*>(draken_malloc(n * sizeof(uint32_t)));
    if (!codes) { draken_free(dict_buf); throw std::bad_alloc(); }

    for (uint32_t i = 0; i < n; ++i) {
        if (!val_row(src_null, i)) {
            codes[i] = 0;  // null row: code unused (validity marks it null)
        } else {
            codes[i] = value_to_code.at(data[v.selection[i]]);
        }
    }

    // Phase 4: copy validity.
    uint8_t* out_null = nullptr;
    if (src_null != nullptr) {
        uint32_t nb = (n + 7u) >> 3;
        out_null = static_cast<uint8_t*>(draken_malloc(nb));
        if (!out_null) {
            draken_free(dict_buf);
            draken_free(codes);
            throw std::bad_alloc();
        }
        memcpy(out_null, src_null, nb);
    }

    VecResult r;
    r.data           = dict_buf;
    r.validity       = out_null;
    r.selection      = codes;
    r.owns_selection = true;  // codes is heap-allocated; caller must draken_free
    r.data_length    = dict_size;
    r.length         = n;
    r.type           = DRAKEN_INT64;
    r.flags          = 0;
    return r;
}

}} // namespace draken::ops
