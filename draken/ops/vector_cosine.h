#pragma once
// draken/ops/vector_cosine.h — fp16 cosine similarity kernel (Milestone E.16, Part A).
//
// cosine_sim_fp16(a, b, dimension):
//   Row-wise cosine similarity for two DRAKEN_VECTOR_FP16 columns of matching dimension.
//   fp16 values are widened to float64 on load; dot product and norms accumulate in float64.
//
// NULL TVL: null in either input row → null output row.
// Zero-norm: dot / (||a|| * ||b||) with any zero norm → NaN (IEEE; 0.0/0.0).
// Length mismatch or wrong type: throws std::invalid_argument.
//
// Output: DRAKEN_FLOAT64 (dense, identity selection).

#pragma once

#include <cstdint>
#include <cstring>
#include <cmath>
#include <limits>
#include <stdexcept>

#include "core/buffers.h"
#include "core/alloc.h"
#include "core/vector_alloc.h"
#include "ops/vec_result.h"
#include "fp16/fp16.h"

namespace draken { namespace ops {

static inline bool vc_row_valid(const uint8_t* validity, uint32_t i) noexcept {
    return (validity == nullptr) || ((validity[i >> 3] >> (i & 7u)) & 1u);
}

static inline VecResult cosine_sim_fp16(
    const DrakenVector& a,
    const DrakenVector& b,
    uint32_t dimension)
{
    if (a.type != DRAKEN_VECTOR_FP16 || b.type != DRAKEN_VECTOR_FP16)
        throw std::invalid_argument(
            "cosine_sim_fp16: both inputs must be DRAKEN_VECTOR_FP16");
    if (dimension == 0u)
        throw std::invalid_argument(
            "cosine_sim_fp16: dimension must be >= 1");
    if (a.length != b.length)
        throw std::invalid_argument(
            "cosine_sim_fp16: input vector lengths must match");

    const uint32_t n = a.length;

    double* dst = static_cast<double*>(
        draken_malloc((n > 0u ? n : 1u) * sizeof(double)));
    if (!dst) throw std::bad_alloc();

    const uint16_t* da = static_cast<const uint16_t*>(a.data);
    const uint16_t* db = static_cast<const uint16_t*>(b.data);

    // Lazily-allocated validity bitmap (nullptr = all valid).
    uint8_t* out_null = nullptr;
    bool any_null = false;

    for (uint32_t i = 0u; i < n; ++i) {
        if (!vc_row_valid(a.validity, i) || !vc_row_valid(b.validity, i)) {
            if (!any_null) {
                const uint32_t bm     = (n + 7u) >> 3;
                const uint32_t padded = (bm + 7u) & ~7u;
                const size_t   vbytes = padded > 0u ? padded : 8u;
                out_null = static_cast<uint8_t*>(draken_malloc(vbytes));
                if (!out_null) { draken_free(dst); throw std::bad_alloc(); }
                std::memset(out_null, 0xFF, vbytes);
                any_null = true;
            }
            out_null[i >> 3] &= ~static_cast<uint8_t>(1u << (i & 7u));
            dst[i] = 0.0;
            continue;
        }

        const uint32_t idx_a = a.selection[i];
        const uint32_t idx_b = b.selection[i];
        const uint16_t* pa = da + static_cast<size_t>(idx_a) * dimension;
        const uint16_t* pb = db + static_cast<size_t>(idx_b) * dimension;

        double dot = 0.0, sq_a = 0.0, sq_b = 0.0;
        for (uint32_t k = 0u; k < dimension; ++k) {
            const double fa = static_cast<double>(fp16_ieee_to_fp32_value(pa[k]));
            const double fb = static_cast<double>(fp16_ieee_to_fp32_value(pb[k]));
            dot  += fa * fb;
            sq_a += fa * fa;
            sq_b += fb * fb;
        }

        const double denom = std::sqrt(sq_a) * std::sqrt(sq_b);
        dst[i] = (denom == 0.0)
            ? std::numeric_limits<double>::quiet_NaN()
            : dot / denom;
    }

    // Clear validity tail bits beyond the last complete byte.
    if (any_null && (n & 7u)) {
        const uint32_t bm = (n + 7u) >> 3;
        out_null[bm - 1u] &= static_cast<uint8_t>((1u << (n & 7u)) - 1u);
    }

    VecResult r;
    r.data           = dst;
    r.validity       = out_null;
    r.selection      = draken_identity_sel(n);
    r.owns_selection = false;
    r.data_length    = n;
    r.length         = n;
    r.type           = DRAKEN_FLOAT64;
    r.flags          = static_cast<uint8_t>(DRAKEN_SEL_IDENTITY | DRAKEN_SEL_PERMUTATION);
    return r;
}

}} // namespace draken::ops
