#pragma once
// draken/ops/float_log.h — logarithm kernels (Milestone E.14, Part A).
//
// SQL LOG(value, base) = ln(value) / ln(base) — arbitrary-base logarithm.
//
// Types accepted (both `v` and `base` operands):
//   INT8/16/32/64, DATE32, TIME32, TIME64, TIMESTAMP64 → promoted to FLOAT64
//   FLOAT32, FLOAT64 → FLOAT64 output
//
// IEEE semantics via std::log (IEEE 754 compliant):
//   log(1)    = 0.0
//   log(0)    = -inf
//   log(-1)   = NaN
//   log(NaN)  = NaN
//   log(+inf) = +inf
//
// NULL TVL: null row in either operand → null output row.
//   Output validity = AND of input validities.
//
// Dispatch entry points:
//   draken::ops::float_log(v, base_v) — both DrakenVectors, broadcast supported
//     (one may have length==1, constant shape).
//
// No OpsTable dependency — self-contained header.

#include <cstdint>
#include <cstring>
#include <cmath>
#include <stdexcept>

#include "core/buffers.h"
#include "core/alloc.h"
#include "core/vector_alloc.h"
#include "ops/vec_result.h"

namespace draken { namespace ops {

// ---------------------------------------------------------------------------
// Helpers (fl_ prefix to avoid ODR clashes with float_math.h helpers)
// ---------------------------------------------------------------------------

static inline double fl_to_double(const DrakenVector& v, uint32_t logical) {
    const uint32_t idx = v.selection[logical];
    switch (v.type) {
        case DRAKEN_INT8:        return static_cast<double>(static_cast<const int8_t* >(v.data)[idx]);
        case DRAKEN_INT16:       return static_cast<double>(static_cast<const int16_t*>(v.data)[idx]);
        case DRAKEN_INT32:       return static_cast<double>(static_cast<const int32_t*>(v.data)[idx]);
        case DRAKEN_INT64:       return static_cast<double>(static_cast<const int64_t*>(v.data)[idx]);
        case DRAKEN_DATE32:      return static_cast<double>(static_cast<const int32_t*>(v.data)[idx]);
        case DRAKEN_TIME32:      return static_cast<double>(static_cast<const int32_t*>(v.data)[idx]);
        case DRAKEN_TIME64:      return static_cast<double>(static_cast<const int64_t*>(v.data)[idx]);
        case DRAKEN_TIMESTAMP64: return static_cast<double>(static_cast<const int64_t*>(v.data)[idx]);
        case DRAKEN_FLOAT32:     return static_cast<double>(static_cast<const float*   >(v.data)[idx]);
        case DRAKEN_FLOAT64:     return static_cast<const double*>(v.data)[idx];
        default:                 throw std::invalid_argument("float_log: unsupported type");
    }
}

static inline bool fl_row_valid(const uint8_t* validity, uint32_t i) noexcept {
    return (validity == nullptr) || ((validity[i >> 3] >> (i & 7)) & 1u);
}

// Combine two validity bitmaps: output null if either input is null.
// Returns nullptr only when both inputs have no nulls.
static inline uint8_t* fl_combine_validity(
    const uint8_t* va, const uint8_t* vb, uint32_t n)
{
    if (va == nullptr && vb == nullptr) return nullptr;
    const uint32_t nb = (n + 7u) >> 3;
    uint8_t* out = static_cast<uint8_t*>(draken_malloc(nb > 0u ? nb : 1u));
    if (!out) throw std::bad_alloc();
    if (va != nullptr && vb != nullptr)
        for (uint32_t k = 0; k < nb; ++k) out[k] = va[k] & vb[k];
    else if (va != nullptr)
        std::memcpy(out, va, nb);
    else
        std::memcpy(out, vb, nb);
    return out;
}

// ---------------------------------------------------------------------------
// float_log — element-wise LOG(v, base) = ln(v) / ln(base).
//
// Broadcast semantics: if one operand has length==1, its row 0 is used for
// all output rows (constant shape — selection[0] points at the single slot).
// Both equal-length is the common case.
// ---------------------------------------------------------------------------

static inline VecResult float_log(const DrakenVector& v, const DrakenVector& base_v) {
    const uint32_t n_v    = v.length;
    const uint32_t n_base = base_v.length;

    uint32_t n;
    bool val_scalar  = false;
    bool base_scalar = false;

    if (n_v == n_base) {
        n = n_v;
    } else if (n_v == 1) {
        n = n_base;
        val_scalar = true;
    } else if (n_base == 1) {
        n = n_v;
        base_scalar = true;
    } else {
        throw std::invalid_argument(
            "float_log: operand lengths must match or one must be 1");
    }

    double* dst = static_cast<double*>(draken_malloc((n > 0u ? n : 1u) * sizeof(double)));
    if (!dst) throw std::bad_alloc();

    // Determine output validity: per-row AND of both input validities.
    // For scalar operands we must replicate their validity across all rows.
    const uint8_t* va = v.validity;
    const uint8_t* vb = base_v.validity;

    // Fast path: both all-valid
    uint8_t* out_null = nullptr;

    if (va != nullptr || vb != nullptr) {
        const uint32_t nb = (n + 7u) >> 3;
        out_null = static_cast<uint8_t*>(draken_malloc(nb > 0u ? nb : 1u));
        if (!out_null) { draken_free(dst); throw std::bad_alloc(); }

        for (uint32_t i = 0; i < nb; ++i) {
            uint8_t a_byte, b_byte;

            if (val_scalar) {
                // row 0 of v replicated
                a_byte = (va == nullptr)
                    ? 0xFFu
                    : (((va[0] >> 0) & 1u) ? 0xFFu : 0x00u);
            } else {
                a_byte = (va == nullptr) ? 0xFFu : va[i];
            }

            if (base_scalar) {
                b_byte = (vb == nullptr)
                    ? 0xFFu
                    : (((vb[0] >> 0) & 1u) ? 0xFFu : 0x00u);
            } else {
                b_byte = (vb == nullptr) ? 0xFFu : vb[i];
            }

            out_null[i] = a_byte & b_byte;
        }

        // Clear tail bits beyond n
        if (n & 7u) {
            out_null[nb - 1] &= static_cast<uint8_t>((1u << (n & 7u)) - 1u);
        }

        // Promote to nullptr if all valid (no nulls introduced)
        bool any_null = false;
        for (uint32_t i = 0; i < nb && !any_null; ++i)
            if (out_null[i] != (i < nb - 1u || !(n & 7u)
                    ? 0xFFu
                    : static_cast<uint8_t>((1u << (n & 7u)) - 1u)))
                any_null = true;
        if (!any_null) { draken_free(out_null); out_null = nullptr; }
    }

    for (uint32_t i = 0; i < n; ++i) {
        if (!fl_row_valid(out_null, i)) { dst[i] = 0.0; continue; }

        const uint32_t vi   = val_scalar  ? 0u : i;
        const uint32_t bi   = base_scalar ? 0u : i;
        const double   val  = fl_to_double(v,      vi);
        const double   base = fl_to_double(base_v, bi);
        dst[i] = std::log(val) / std::log(base);
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
