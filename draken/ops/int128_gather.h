#pragma once
// draken/ops/int128_gather.h — take / slice / materialize / compress for
// int128-backed vectors (DRAKEN_DECIMAL128, the "correct-but-scalar" decimal tier).
//
// A byte-for-byte mirror of int64_gather.h with two differences: the element type
// is __int128 (16-byte storage) and the result tag is DRAKEN_DECIMAL128. The slice
// / take / materialize / compress nanobind wrappers restore the original type and
// logical descriptor (precision/scale) after dispatch, so these kernels only have
// to move the right number of bytes.
//
// The validity helpers (val_row, set_valid_bit, normalize_validity,
// copy_validity_range) are shared from int64_gather.h — included below.

#include <stdint.h>
#include <stddef.h>
#include <string.h>
#include <stdexcept>
#include <unordered_map>
#include <vector>
#include "core/buffers.h"
#include "core/alloc.h"
#include "core/vector_alloc.h"
#include "ops/vec_result.h"
#include "ops/int64_gather.h"   // shared validity helpers (val_row, normalize_validity, …)

namespace draken { namespace ops {

// std::hash<__int128> is not portable (absent under libc++). Hash the two 64-bit
// halves for the compress dictionary map.
struct i128_hash {
    size_t operator()(__int128 v) const noexcept {
        const uint64_t lo = static_cast<uint64_t>(static_cast<unsigned __int128>(v));
        const uint64_t hi = static_cast<uint64_t>(static_cast<unsigned __int128>(v) >> 64);
        return std::hash<uint64_t>()(lo) ^ (std::hash<uint64_t>()(hi) * 1099511628211ull);
    }
};

// ---------------------------------------------------------------------------
// TAKE
// ---------------------------------------------------------------------------
static inline VecResult i128_take(
    const DrakenVector& v, const int32_t* indices, uint32_t n_indices)
{
    const uint32_t n    = n_indices;
    const __int128* data = static_cast<const __int128*>(v.data);
    const uint8_t* src_null = v.validity;

    __int128* dst = static_cast<__int128*>(
        draken_malloc((n > 0 ? n : 1u) * sizeof(__int128)));
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
    r.type           = DRAKEN_DECIMAL128;
    r.flags          = static_cast<uint8_t>(DRAKEN_SEL_IDENTITY | DRAKEN_SEL_PERMUTATION);
    return r;
}

// ---------------------------------------------------------------------------
// SLICE — contiguous range [start, start+length). Fast memcpy for dense.
// ---------------------------------------------------------------------------
static inline VecResult i128_slice(const DrakenVector& v, uint32_t start, uint32_t length) {
    const uint32_t n        = length;
    const __int128* data    = static_cast<const __int128*>(v.data);
    const uint8_t* src_null = v.validity;

    __int128* dst = static_cast<__int128*>(
        draken_malloc((n > 0u ? n : 1u) * sizeof(__int128)));
    if (!dst) throw std::bad_alloc();

    if (v.data_length == v.length) {
        std::memcpy(dst, data + start, n * sizeof(__int128));
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
    r.type           = DRAKEN_DECIMAL128;
    r.flags          = static_cast<uint8_t>(DRAKEN_SEL_IDENTITY | DRAKEN_SEL_PERMUTATION);
    return r;
}

// ---------------------------------------------------------------------------
// MATERIALIZE
// ---------------------------------------------------------------------------
static inline VecResult i128_materialize(const DrakenVector& v) {
    const uint32_t n     = v.length;
    const __int128* data = static_cast<const __int128*>(v.data);
    const uint8_t* src_null = v.validity;

    __int128* dst = static_cast<__int128*>(
        draken_malloc((n > 0 ? n : 1u) * sizeof(__int128)));
    if (!dst) throw std::bad_alloc();

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
    r.type           = DRAKEN_DECIMAL128;
    r.flags          = static_cast<uint8_t>(DRAKEN_SEL_IDENTITY | DRAKEN_SEL_PERMUTATION);
    return r;
}

// ---------------------------------------------------------------------------
// COMPRESS — dict-encode an int128 vector (see int64_gather.h for the algorithm).
// ---------------------------------------------------------------------------
static inline VecResult i128_compress(const DrakenVector& v) {
    const uint32_t n     = v.length;
    const __int128* data = static_cast<const __int128*>(v.data);
    const uint8_t* src_null = v.validity;

    if (n == 0) {
        __int128* d = static_cast<__int128*>(draken_malloc(sizeof(__int128)));
        if (!d) throw std::bad_alloc();
        d[0] = 0;
        VecResult r;
        r.data           = d;
        r.validity       = nullptr;
        r.selection      = draken_identity_sel(0);
        r.owns_selection = false;
        r.data_length    = 0;
        r.length         = 0;
        r.type           = DRAKEN_DECIMAL128;
        r.flags          = 0;
        return r;
    }

    std::unordered_map<__int128, uint32_t, i128_hash> value_to_code;
    value_to_code.reserve(n < 256u ? n : 256u);

    std::vector<__int128> dict_values;
    for (uint32_t i = 0; i < n; ++i) {
        if (!val_row(src_null, i)) continue;
        __int128 val = data[v.selection[i]];
        if (value_to_code.find(val) == value_to_code.end()) {
            uint32_t code = static_cast<uint32_t>(dict_values.size());
            value_to_code[val] = code;
            dict_values.push_back(val);
        }
    }

    const uint32_t dict_size = static_cast<uint32_t>(dict_values.size());

    if (dict_size == 0) {
        __int128* d = static_cast<__int128*>(draken_malloc(sizeof(__int128)));
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
        r.type           = DRAKEN_DECIMAL128;
        r.flags          = 0;
        return r;
    }

    __int128* dict_buf = static_cast<__int128*>(
        draken_malloc(dict_size * sizeof(__int128)));
    if (!dict_buf) throw std::bad_alloc();
    for (uint32_t k = 0; k < dict_size; ++k) dict_buf[k] = dict_values[k];

    uint32_t* codes = static_cast<uint32_t*>(draken_malloc(n * sizeof(uint32_t)));
    if (!codes) { draken_free(dict_buf); throw std::bad_alloc(); }

    for (uint32_t i = 0; i < n; ++i) {
        if (!val_row(src_null, i)) {
            codes[i] = 0;
        } else {
            codes[i] = value_to_code.at(data[v.selection[i]]);
        }
    }

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
    r.owns_selection = true;
    r.data_length    = dict_size;
    r.length         = n;
    r.type           = DRAKEN_DECIMAL128;
    r.flags          = 0;
    return r;
}

}} // namespace draken::ops
