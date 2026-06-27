#pragma once
// draken/ops/float_math.h — scalar math kernels: ABS / SIGN / SQRT / ROUND.
//
// Types handled per op (dispatch entry raises on unsupported):
//   ABS   — INT8/16/32/64 → same type (wraps at INT*_MIN, C convention);
//            FLOAT32/FLOAT64 → FLOAT64
//   SIGN  — INT8/16/32/64 → INT8 ∈ {-1, 0, 1};
//            FLOAT32/FLOAT64 → INT8; NaN → null row (no INT8 can hold NaN)
//   SQRT  — INT8/16/32/64 → FLOAT64; negative int → std::invalid_argument;
//            FLOAT32/FLOAT64 → FLOAT64; negative float → NaN (IEEE 754)
//   ROUND — INT8/16/32/64 → same type (identity — already integers);
//            FLOAT32/FLOAT64 → FLOAT64; uses 2^52 trick (half-to-even,
//            relies on IEEE 754 FE_TONEAREST default rounding mode)
//
// Null TVL:
//   All ops: null in → null out; null bitmap copied from input.
//   SIGN only: also injects new nulls for NaN input (see above).
//   ROUND with digits != 0: scale-round-unscale (uses std::pow for scale).
//
// Dispatch entry points:
//   draken::ops::float_abs(v)
//   draken::ops::float_sign(v)
//   draken::ops::float_sqrt(v)
//   draken::ops::float_round(v, digits = 0)
//
// No OpsTable dependency — this header is self-contained.

#include <stdint.h>
#include <stddef.h>
#include <string.h>
#include <cmath>
#include <limits>
#include <stdexcept>
#include <type_traits>
#include <new>        // std::bad_alloc / placement new — not reliably pulled in by <stdexcept> on stricter libc++
#include "core/buffers.h"
#include "core/alloc.h"
#include "core/vector_alloc.h"
#include "ops/vec_result.h"

namespace draken { namespace ops {

// ---------------------------------------------------------------------------
// Internal helpers (fm_ prefix)
// ---------------------------------------------------------------------------

static inline bool fm_row_valid(const uint8_t* validity, uint32_t i) noexcept {
    return (validity == nullptr) || ((validity[i >> 3] >> (i & 7)) & 1u);
}

static inline uint8_t* fm_copy_validity(const uint8_t* src, uint32_t n) {
    if (src == nullptr) return nullptr;
    uint32_t nb = (n + 7u) >> 3;
    if (nb == 0) nb = 1;
    uint8_t* out = static_cast<uint8_t*>(draken_malloc(nb));
    if (!out) throw std::bad_alloc();
    memcpy(out, src, nb);
    return out;
}

template<typename T>
static inline T* fm_alloc(uint32_t n) {
    if (n == 0) n = 1;
    T* p = static_cast<T*>(draken_malloc(n * sizeof(T)));
    if (!p) throw std::bad_alloc();
    return p;
}

template<typename T>
static inline VecResult fm_make_result(
    T* data, uint8_t* validity, uint32_t n, DrakenType tag)
{
    VecResult r;
    r.data           = data;
    r.validity       = validity;
    r.selection      = draken_identity_sel(n);
    r.owns_selection = false;
    r.data_length    = n;
    r.length         = n;
    r.type           = tag;
    r.flags          = static_cast<uint8_t>(DRAKEN_SEL_IDENTITY | DRAKEN_SEL_PERMUTATION);
    return r;
}

// 2^52 trick: half-to-even via the hardware FP rounding mode (FE_TONEAREST).
// For |x| >= 2^52 the value is already integral; Inf and NaN pass through.
static inline double fm_round_hte(double x) noexcept {
    constexpr double TWO52 = 4503599627370496.0;
    if (!(std::fabs(x) < TWO52)) return x;
    double s = std::copysign(TWO52, x);
    return (x + s) - s;
}

// Round to `digits` decimal places with half-to-even.
// digits == 0: direct fm_round_hte.
// digits > 0:  scale up, round, scale down.
// digits < 0:  scale down, round, scale up.
static inline double fm_round_digits(double x, int digits) noexcept {
    if (digits == 0) return fm_round_hte(x);
    if (std::isnan(x) || std::isinf(x)) return x;
    double scale = std::pow(10.0, static_cast<double>(std::abs(digits)));
    if (digits > 0) {
        return fm_round_hte(x * scale) / scale;
    } else {
        return fm_round_hte(x / scale) * scale;
    }
}

// ---------------------------------------------------------------------------
// ABS kernels
// ---------------------------------------------------------------------------

// Integer ABS: same output type as input. INT*_MIN wraps (C convention).
template<typename T, DrakenType TAG>
static inline VecResult fm_abs_int_tmpl(const DrakenVector& a) {
    const uint32_t n = a.length;
    const T* src = static_cast<const T*>(a.data);
    T* dst = fm_alloc<T>(n);
    for (uint32_t i = 0; i < n; ++i) {
        T v = src[a.selection[i]];
        dst[i] = v >= 0 ? v : static_cast<T>(-v);
    }
    return fm_make_result(dst, fm_copy_validity(a.validity, n), n, TAG);
}

// Float ABS: always outputs FLOAT64.
template<typename T>
static inline VecResult fm_abs_float_tmpl(const DrakenVector& a) {
    const uint32_t n = a.length;
    const T* src = static_cast<const T*>(a.data);
    double* dst = fm_alloc<double>(n);
    for (uint32_t i = 0; i < n; ++i)
        dst[i] = std::fabs(static_cast<double>(src[a.selection[i]]));
    return fm_make_result(dst, fm_copy_validity(a.validity, n), n, DRAKEN_FLOAT64);
}

// ---------------------------------------------------------------------------
// SIGN kernels
// ---------------------------------------------------------------------------

// Integer SIGN: result INT8 ∈ {-1, 0, 1}.
template<typename T>
static inline VecResult fm_sign_int_tmpl(const DrakenVector& a) {
    const uint32_t n = a.length;
    const T* src = static_cast<const T*>(a.data);
    int8_t* dst = fm_alloc<int8_t>(n);
    for (uint32_t i = 0; i < n; ++i) {
        T v = src[a.selection[i]];
        dst[i] = (v > 0) ? int8_t(1) : (v < 0) ? int8_t(-1) : int8_t(0);
    }
    return fm_make_result(dst, fm_copy_validity(a.validity, n), n, DRAKEN_INT8);
}

// Float SIGN: NaN rows become null (INT8 cannot represent NaN).
// Allocates a validity bitmap unconditionally (NaN may inject new nulls).
template<typename T>
static inline VecResult fm_sign_float_tmpl(const DrakenVector& a) {
    const uint32_t n = a.length;
    const T* src = static_cast<const T*>(a.data);
    int8_t* dst = fm_alloc<int8_t>(n);

    uint32_t nb = (n + 7u) >> 3;
    if (nb == 0) nb = 1;
    uint8_t* out_val = static_cast<uint8_t*>(draken_malloc(nb));
    if (!out_val) { draken_free(dst); throw std::bad_alloc(); }
    if (a.validity) {
        memcpy(out_val, a.validity, nb);
    } else {
        memset(out_val, 0xFF, nb);
    }

    bool any_null = (a.validity != nullptr);
    for (uint32_t i = 0; i < n; ++i) {
        if (!fm_row_valid(a.validity, i)) { dst[i] = 0; continue; }
        double v = static_cast<double>(src[a.selection[i]]);
        if (std::isnan(v)) {
            dst[i] = 0;
            out_val[i >> 3] &= ~(uint8_t(1u) << (i & 7));
            any_null = true;
        } else {
            dst[i] = (v > 0.0) ? int8_t(1) : (v < 0.0) ? int8_t(-1) : int8_t(0);
        }
    }

    if (!any_null) { draken_free(out_val); out_val = nullptr; }
    return fm_make_result(dst, out_val, n, DRAKEN_INT8);
}

// ---------------------------------------------------------------------------
// SQRT kernels
// ---------------------------------------------------------------------------

// Integer SQRT: raises on negative values; outputs FLOAT64.
template<typename T>
static inline VecResult fm_sqrt_int_tmpl(const DrakenVector& a) {
    const uint32_t n = a.length;
    const T* src = static_cast<const T*>(a.data);
    double* dst = fm_alloc<double>(n);
    for (uint32_t i = 0; i < n; ++i) {
        if (!fm_row_valid(a.validity, i)) { dst[i] = 0.0; continue; }
        T v = src[a.selection[i]];
        if (v < 0)
            throw std::invalid_argument("float_sqrt: cannot take sqrt of negative integer");
        dst[i] = std::sqrt(static_cast<double>(v));
    }
    return fm_make_result(dst, fm_copy_validity(a.validity, n), n, DRAKEN_FLOAT64);
}

// Float SQRT: negative → NaN (IEEE 754); outputs FLOAT64.
template<typename T>
static inline VecResult fm_sqrt_float_tmpl(const DrakenVector& a) {
    const uint32_t n = a.length;
    const T* src = static_cast<const T*>(a.data);
    double* dst = fm_alloc<double>(n);
    for (uint32_t i = 0; i < n; ++i)
        dst[i] = std::sqrt(static_cast<double>(src[a.selection[i]]));
    return fm_make_result(dst, fm_copy_validity(a.validity, n), n, DRAKEN_FLOAT64);
}

// ---------------------------------------------------------------------------
// Scaling helpers for CEIL / FLOOR / TRUNC  (scale argument matches SQL ROUND)
// ---------------------------------------------------------------------------

static inline double fm_ceil_scaled(double x, int scale) noexcept {
    if (scale == 0) return std::ceil(x);
    double sf = std::pow(10.0, static_cast<double>(std::abs(scale)));
    return (scale > 0) ? std::ceil(x * sf) / sf : std::ceil(x / sf) * sf;
}

static inline double fm_floor_scaled(double x, int scale) noexcept {
    if (scale == 0) return std::floor(x);
    double sf = std::pow(10.0, static_cast<double>(std::abs(scale)));
    return (scale > 0) ? std::floor(x * sf) / sf : std::floor(x / sf) * sf;
}

static inline double fm_trunc_scaled(double x, int scale) noexcept {
    if (scale == 0) return std::trunc(x);
    double sf = std::pow(10.0, static_cast<double>(std::abs(scale)));
    return (scale > 0) ? std::trunc(x * sf) / sf : std::trunc(x / sf) * sf;
}

// ---------------------------------------------------------------------------
// CEIL / FLOOR / TRUNC kernels  (all numeric types → FLOAT64)
// ---------------------------------------------------------------------------

template<typename T>
static inline VecResult fm_ceil_tmpl(const DrakenVector& a, int scale) {
    const uint32_t n = a.length;
    const T* src = static_cast<const T*>(a.data);
    double* dst = fm_alloc<double>(n);
    for (uint32_t i = 0; i < n; ++i) {
        if (!fm_row_valid(a.validity, i)) { dst[i] = 0.0; continue; }
        dst[i] = fm_ceil_scaled(static_cast<double>(src[a.selection[i]]), scale);
    }
    return fm_make_result(dst, fm_copy_validity(a.validity, n), n, DRAKEN_FLOAT64);
}

template<typename T>
static inline VecResult fm_floor_tmpl(const DrakenVector& a, int scale) {
    const uint32_t n = a.length;
    const T* src = static_cast<const T*>(a.data);
    double* dst = fm_alloc<double>(n);
    for (uint32_t i = 0; i < n; ++i) {
        if (!fm_row_valid(a.validity, i)) { dst[i] = 0.0; continue; }
        dst[i] = fm_floor_scaled(static_cast<double>(src[a.selection[i]]), scale);
    }
    return fm_make_result(dst, fm_copy_validity(a.validity, n), n, DRAKEN_FLOAT64);
}

template<typename T>
static inline VecResult fm_trunc_tmpl(const DrakenVector& a, int scale) {
    const uint32_t n = a.length;
    const T* src = static_cast<const T*>(a.data);
    double* dst = fm_alloc<double>(n);
    for (uint32_t i = 0; i < n; ++i) {
        if (!fm_row_valid(a.validity, i)) { dst[i] = 0.0; continue; }
        dst[i] = fm_trunc_scaled(static_cast<double>(src[a.selection[i]]), scale);
    }
    return fm_make_result(dst, fm_copy_validity(a.validity, n), n, DRAKEN_FLOAT64);
}

// ---------------------------------------------------------------------------
// POWER kernel  (all numeric types → FLOAT64)
// ---------------------------------------------------------------------------

template<typename T>
static inline VecResult fm_power_tmpl(const DrakenVector& a, double exponent) {
    const uint32_t n = a.length;
    const T* src = static_cast<const T*>(a.data);
    double* dst = fm_alloc<double>(n);
    for (uint32_t i = 0; i < n; ++i) {
        if (!fm_row_valid(a.validity, i)) { dst[i] = 0.0; continue; }
        dst[i] = std::pow(static_cast<double>(src[a.selection[i]]), exponent);
    }
    return fm_make_result(dst, fm_copy_validity(a.validity, n), n, DRAKEN_FLOAT64);
}

// ---------------------------------------------------------------------------
// ROUND kernels
// ---------------------------------------------------------------------------

// Integer ROUND: identity (already integers). Same output type as input.
template<typename T, DrakenType TAG>
static inline VecResult fm_round_int_tmpl(const DrakenVector& a) {
    const uint32_t n = a.length;
    const T* src = static_cast<const T*>(a.data);
    T* dst = fm_alloc<T>(n);
    for (uint32_t i = 0; i < n; ++i)
        dst[i] = src[a.selection[i]];
    return fm_make_result(dst, fm_copy_validity(a.validity, n), n, TAG);
}

// Float ROUND: half-to-even via 2^52 trick; outputs FLOAT64.
template<typename T>
static inline VecResult fm_round_float_tmpl(const DrakenVector& a, int digits) {
    const uint32_t n = a.length;
    const T* src = static_cast<const T*>(a.data);
    double* dst = fm_alloc<double>(n);
    for (uint32_t i = 0; i < n; ++i)
        dst[i] = fm_round_digits(static_cast<double>(src[a.selection[i]]), digits);
    return fm_make_result(dst, fm_copy_validity(a.validity, n), n, DRAKEN_FLOAT64);
}

// ---------------------------------------------------------------------------
// Dispatch entry points
// ---------------------------------------------------------------------------

static inline VecResult float_abs(const DrakenVector& a) {
    switch (a.type) {
        case DRAKEN_INT8:    return fm_abs_int_tmpl<int8_t,  DRAKEN_INT8>(a);
        case DRAKEN_INT16:   return fm_abs_int_tmpl<int16_t, DRAKEN_INT16>(a);
        case DRAKEN_INT32:   return fm_abs_int_tmpl<int32_t, DRAKEN_INT32>(a);
        case DRAKEN_INT64:   return fm_abs_int_tmpl<int64_t, DRAKEN_INT64>(a);
        case DRAKEN_FLOAT32: return fm_abs_float_tmpl<float>(a);
        case DRAKEN_FLOAT64: return fm_abs_float_tmpl<double>(a);
        default: throw std::invalid_argument("float_abs: unsupported type");
    }
}

static inline VecResult float_sign(const DrakenVector& a) {
    switch (a.type) {
        case DRAKEN_INT8:    return fm_sign_int_tmpl<int8_t>(a);
        case DRAKEN_INT16:   return fm_sign_int_tmpl<int16_t>(a);
        case DRAKEN_INT32:   return fm_sign_int_tmpl<int32_t>(a);
        case DRAKEN_INT64:   return fm_sign_int_tmpl<int64_t>(a);
        case DRAKEN_FLOAT32: return fm_sign_float_tmpl<float>(a);
        case DRAKEN_FLOAT64: return fm_sign_float_tmpl<double>(a);
        default: throw std::invalid_argument("float_sign: unsupported type");
    }
}

static inline VecResult float_sqrt(const DrakenVector& a) {
    switch (a.type) {
        case DRAKEN_INT8:    return fm_sqrt_int_tmpl<int8_t>(a);
        case DRAKEN_INT16:   return fm_sqrt_int_tmpl<int16_t>(a);
        case DRAKEN_INT32:   return fm_sqrt_int_tmpl<int32_t>(a);
        case DRAKEN_INT64:   return fm_sqrt_int_tmpl<int64_t>(a);
        case DRAKEN_FLOAT32: return fm_sqrt_float_tmpl<float>(a);
        case DRAKEN_FLOAT64: return fm_sqrt_float_tmpl<double>(a);
        default: throw std::invalid_argument("float_sqrt: unsupported type");
    }
}

static inline VecResult float_round(const DrakenVector& a, int digits = 0) {
    switch (a.type) {
        case DRAKEN_INT8:    return fm_round_int_tmpl<int8_t,  DRAKEN_INT8>(a);
        case DRAKEN_INT16:   return fm_round_int_tmpl<int16_t, DRAKEN_INT16>(a);
        case DRAKEN_INT32:   return fm_round_int_tmpl<int32_t, DRAKEN_INT32>(a);
        case DRAKEN_INT64:   return fm_round_int_tmpl<int64_t, DRAKEN_INT64>(a);
        case DRAKEN_FLOAT32: return fm_round_float_tmpl<float>(a, digits);
        case DRAKEN_FLOAT64: return fm_round_float_tmpl<double>(a, digits);
        default: throw std::invalid_argument("float_round: unsupported type");
    }
}

static inline VecResult float_ceil(const DrakenVector& a, int scale = 0) {
    switch (a.type) {
        case DRAKEN_INT8:    return fm_ceil_tmpl<int8_t>(a, scale);
        case DRAKEN_INT16:   return fm_ceil_tmpl<int16_t>(a, scale);
        case DRAKEN_INT32:   return fm_ceil_tmpl<int32_t>(a, scale);
        case DRAKEN_INT64:   return fm_ceil_tmpl<int64_t>(a, scale);
        case DRAKEN_FLOAT32: return fm_ceil_tmpl<float>(a, scale);
        case DRAKEN_FLOAT64: return fm_ceil_tmpl<double>(a, scale);
        default: throw std::invalid_argument("float_ceil: unsupported type");
    }
}

static inline VecResult float_floor(const DrakenVector& a, int scale = 0) {
    switch (a.type) {
        case DRAKEN_INT8:    return fm_floor_tmpl<int8_t>(a, scale);
        case DRAKEN_INT16:   return fm_floor_tmpl<int16_t>(a, scale);
        case DRAKEN_INT32:   return fm_floor_tmpl<int32_t>(a, scale);
        case DRAKEN_INT64:   return fm_floor_tmpl<int64_t>(a, scale);
        case DRAKEN_FLOAT32: return fm_floor_tmpl<float>(a, scale);
        case DRAKEN_FLOAT64: return fm_floor_tmpl<double>(a, scale);
        default: throw std::invalid_argument("float_floor: unsupported type");
    }
}

static inline VecResult float_trunc(const DrakenVector& a, int scale = 0) {
    switch (a.type) {
        case DRAKEN_INT8:    return fm_trunc_tmpl<int8_t>(a, scale);
        case DRAKEN_INT16:   return fm_trunc_tmpl<int16_t>(a, scale);
        case DRAKEN_INT32:   return fm_trunc_tmpl<int32_t>(a, scale);
        case DRAKEN_INT64:   return fm_trunc_tmpl<int64_t>(a, scale);
        case DRAKEN_FLOAT32: return fm_trunc_tmpl<float>(a, scale);
        case DRAKEN_FLOAT64: return fm_trunc_tmpl<double>(a, scale);
        default: throw std::invalid_argument("float_trunc: unsupported type");
    }
}

static inline VecResult float_power(const DrakenVector& a, double exponent) {
    switch (a.type) {
        case DRAKEN_INT8:    return fm_power_tmpl<int8_t>(a, exponent);
        case DRAKEN_INT16:   return fm_power_tmpl<int16_t>(a, exponent);
        case DRAKEN_INT32:   return fm_power_tmpl<int32_t>(a, exponent);
        case DRAKEN_INT64:   return fm_power_tmpl<int64_t>(a, exponent);
        case DRAKEN_FLOAT32: return fm_power_tmpl<float>(a, exponent);
        case DRAKEN_FLOAT64: return fm_power_tmpl<double>(a, exponent);
        default: throw std::invalid_argument("float_power: unsupported type");
    }
}

}} // namespace draken::ops
