#pragma once
// draken/ops/int64_arithmetic.h — int64 × int64 arithmetic kernels.
//
// Ops:  add / sub / mul / div / mod / neg
// Result type: always int64 (VecResult with type == DRAKEN_INT64).
//
// div semantics (SURFACE TO ARCHITECT):
//   Integer division uses cdivision=True / C truncation-toward-zero:
//   (-7)/2 == -3, not -4 (Python floor).
//   "True division" returning float64 is deferred until float64 is built.
//   A separate slot or op-code covers true-div when that type lands.
//
// Overflow:
//   add/sub/mul overflow: silent wrap (C signed arithmetic).
//   neg(INT64_MIN): wraps to INT64_MIN (signed overflow, platform-deterministic
//   on NEON/AVX2 x86; documents the behaviour rather than masking it).
//   div/mod by zero: result = 0.
//
// Null propagation: any null input → null output for that row.
//   result_valid[i] = a_valid[i] AND b_valid[i].
//   For unary neg: result_valid[i] = a_valid[i].
//
// SIMD note: scalar loops structured for auto-vectorisation.
// Manual NEON/AVX2 intrinsics are a follow-up; time-boxed waiver per 09_delivery §3.

#include <stdint.h>
#include <stddef.h>
#include <string.h>
#include <stdexcept>
#include "core/buffers.h"
#include "core/alloc.h"
#include "core/vector_alloc.h"
#include "ops/vec_result.h"

namespace draken { namespace ops {

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// Combine two validity bitmaps into an owned result bitmap.
// result[i] = a_valid[i] AND b_valid[i] (bit AND → both must be valid).
// Returns nullptr (all-valid) when both inputs are all-valid.
// Throws std::bad_alloc on OOM.
static inline uint8_t* combine_validity(
    const uint8_t* a_val, const uint8_t* b_val, uint32_t n)
{
    if (a_val == nullptr && b_val == nullptr) return nullptr;

    uint32_t nb = (n + 7u) >> 3;
    uint8_t* out = static_cast<uint8_t*>(draken_malloc(nb));
    if (!out) throw std::bad_alloc();

    if (a_val != nullptr && b_val != nullptr) {
        for (uint32_t k = 0; k < nb; ++k) out[k] = a_val[k] & b_val[k];
    } else if (a_val != nullptr) {
        memcpy(out, a_val, nb);
    } else {
        memcpy(out, b_val, nb);
    }
    return out;
}

// Copy a validity bitmap (may be nullptr → returns nullptr).
static inline uint8_t* copy_validity(const uint8_t* src, uint32_t n) {
    if (src == nullptr) return nullptr;
    uint32_t nb = (n + 7u) >> 3;
    uint8_t* out = static_cast<uint8_t*>(draken_malloc(nb));
    if (!out) throw std::bad_alloc();
    memcpy(out, src, nb);
    return out;
}

// Allocate data buffer for n int64 elements.
static inline int64_t* alloc_i64(uint32_t n) {
    if (n == 0) n = 1;  // always non-null pointer
    int64_t* p = static_cast<int64_t*>(draken_malloc(n * sizeof(int64_t)));
    if (!p) throw std::bad_alloc();
    return p;
}

// Build a dense-identity VecResult from an already-allocated data + validity.
static inline VecResult make_dense_result(
    int64_t* data, uint8_t* validity, uint32_t n)
{
    VecResult r;
    r.data           = data;
    r.validity       = validity;
    r.selection      = draken_identity_sel(n);
    r.owns_selection = false;
    r.data_length    = n;
    r.length         = n;
    r.type           = DRAKEN_INT64;
    r.flags          = static_cast<uint8_t>(DRAKEN_SEL_IDENTITY | DRAKEN_SEL_PERMUTATION);
    return r;
}

// ---------------------------------------------------------------------------
// ADD
// ---------------------------------------------------------------------------
static inline VecResult i64_add(const DrakenVector& a, const DrakenVector& b) {
    if (a.length != b.length)
        throw std::invalid_argument("i64_add: length mismatch");
    const uint32_t n = a.length;
    const int64_t* ad = static_cast<const int64_t*>(a.data);
    const int64_t* bd = static_cast<const int64_t*>(b.data);
    int64_t* dst = alloc_i64(n);
    for (uint32_t i = 0; i < n; ++i)
        dst[i] = ad[a.selection[i]] + bd[b.selection[i]];
    return make_dense_result(dst, combine_validity(a.validity, b.validity, n), n);
}

// Scalar variant: b is a single int64 constant (no validity).
static inline VecResult i64_add_scalar(const DrakenVector& a, int64_t scalar) {
    const uint32_t n = a.length;
    const int64_t* ad = static_cast<const int64_t*>(a.data);
    int64_t* dst = alloc_i64(n);
    for (uint32_t i = 0; i < n; ++i) dst[i] = ad[a.selection[i]] + scalar;
    return make_dense_result(dst, copy_validity(a.validity, n), n);
}

// ---------------------------------------------------------------------------
// SUB
// ---------------------------------------------------------------------------
static inline VecResult i64_sub(const DrakenVector& a, const DrakenVector& b) {
    if (a.length != b.length)
        throw std::invalid_argument("i64_sub: length mismatch");
    const uint32_t n = a.length;
    const int64_t* ad = static_cast<const int64_t*>(a.data);
    const int64_t* bd = static_cast<const int64_t*>(b.data);
    int64_t* dst = alloc_i64(n);
    for (uint32_t i = 0; i < n; ++i)
        dst[i] = ad[a.selection[i]] - bd[b.selection[i]];
    return make_dense_result(dst, combine_validity(a.validity, b.validity, n), n);
}

static inline VecResult i64_sub_scalar(const DrakenVector& a, int64_t scalar) {
    const uint32_t n = a.length;
    const int64_t* ad = static_cast<const int64_t*>(a.data);
    int64_t* dst = alloc_i64(n);
    for (uint32_t i = 0; i < n; ++i) dst[i] = ad[a.selection[i]] - scalar;
    return make_dense_result(dst, copy_validity(a.validity, n), n);
}

// ---------------------------------------------------------------------------
// MUL
// ---------------------------------------------------------------------------
static inline VecResult i64_mul(const DrakenVector& a, const DrakenVector& b) {
    if (a.length != b.length)
        throw std::invalid_argument("i64_mul: length mismatch");
    const uint32_t n = a.length;
    const int64_t* ad = static_cast<const int64_t*>(a.data);
    const int64_t* bd = static_cast<const int64_t*>(b.data);
    int64_t* dst = alloc_i64(n);
    for (uint32_t i = 0; i < n; ++i)
        dst[i] = ad[a.selection[i]] * bd[b.selection[i]];
    return make_dense_result(dst, combine_validity(a.validity, b.validity, n), n);
}

static inline VecResult i64_mul_scalar(const DrakenVector& a, int64_t scalar) {
    const uint32_t n = a.length;
    const int64_t* ad = static_cast<const int64_t*>(a.data);
    int64_t* dst = alloc_i64(n);
    for (uint32_t i = 0; i < n; ++i) dst[i] = ad[a.selection[i]] * scalar;
    return make_dense_result(dst, copy_validity(a.validity, n), n);
}

// ---------------------------------------------------------------------------
// DIV — integer division, C truncation toward zero, div-by-zero → 0.
// (cdivision=True semantics.)
// Note: true-division returning float64 is out of scope until float64 is built.
// ---------------------------------------------------------------------------
static inline VecResult i64_div(const DrakenVector& a, const DrakenVector& b) {
    if (a.length != b.length)
        throw std::invalid_argument("i64_div: length mismatch");
    const uint32_t n = a.length;
    const int64_t* ad = static_cast<const int64_t*>(a.data);
    const int64_t* bd = static_cast<const int64_t*>(b.data);
    int64_t* dst = alloc_i64(n);
    for (uint32_t i = 0; i < n; ++i) {
        const int64_t bv = bd[b.selection[i]];
        dst[i] = (bv == 0) ? 0 : ad[a.selection[i]] / bv;
    }
    return make_dense_result(dst, combine_validity(a.validity, b.validity, n), n);
}

static inline VecResult i64_div_scalar(const DrakenVector& a, int64_t scalar) {
    const uint32_t n = a.length;
    const int64_t* ad = static_cast<const int64_t*>(a.data);
    int64_t* dst = alloc_i64(n);
    if (scalar == 0) {
        for (uint32_t i = 0; i < n; ++i) dst[i] = 0;
    } else {
        for (uint32_t i = 0; i < n; ++i) dst[i] = ad[a.selection[i]] / scalar;
    }
    return make_dense_result(dst, copy_validity(a.validity, n), n);
}

// ---------------------------------------------------------------------------
// MOD — C truncation-based modulo, mod-by-zero → 0.
// ---------------------------------------------------------------------------
static inline VecResult i64_mod(const DrakenVector& a, const DrakenVector& b) {
    if (a.length != b.length)
        throw std::invalid_argument("i64_mod: length mismatch");
    const uint32_t n = a.length;
    const int64_t* ad = static_cast<const int64_t*>(a.data);
    const int64_t* bd = static_cast<const int64_t*>(b.data);
    int64_t* dst = alloc_i64(n);
    for (uint32_t i = 0; i < n; ++i) {
        const int64_t bv = bd[b.selection[i]];
        dst[i] = (bv == 0) ? 0 : ad[a.selection[i]] % bv;
    }
    return make_dense_result(dst, combine_validity(a.validity, b.validity, n), n);
}

static inline VecResult i64_mod_scalar(const DrakenVector& a, int64_t scalar) {
    const uint32_t n = a.length;
    const int64_t* ad = static_cast<const int64_t*>(a.data);
    int64_t* dst = alloc_i64(n);
    if (scalar == 0) {
        for (uint32_t i = 0; i < n; ++i) dst[i] = 0;
    } else {
        for (uint32_t i = 0; i < n; ++i) dst[i] = ad[a.selection[i]] % scalar;
    }
    return make_dense_result(dst, copy_validity(a.validity, n), n);
}

// ---------------------------------------------------------------------------
// NEG — unary negation.
// neg(INT64_MIN) wraps to INT64_MIN (signed overflow, platform-deterministic
// on all our targets).
// ---------------------------------------------------------------------------
static inline VecResult i64_neg(const DrakenVector& a) {
    const uint32_t n = a.length;
    const int64_t* ad = static_cast<const int64_t*>(a.data);
    int64_t* dst = alloc_i64(n);
    for (uint32_t i = 0; i < n; ++i)
        dst[i] = -ad[a.selection[i]];
    return make_dense_result(dst, copy_validity(a.validity, n), n);
}

}} // namespace draken::ops
