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
#include <new>        // std::bad_alloc (not transitively provided by <stdexcept> on all libc++)
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

    // Normalize: if every logical bit is set (both inputs fully valid over their
    // overlap), drop the bitmap so downstream code takes the validity==nullptr
    // fast path instead of carrying a dead all-0xFF mask.
    bool all_valid = true;
    for (uint32_t k = 0; k < nb && all_valid; ++k) {
        uint8_t expected = 0xFFu;
        if (k == nb - 1u && (n & 7u) != 0u)
            expected = static_cast<uint8_t>((1u << (n & 7u)) - 1u);
        if (out[k] != expected) all_valid = false;
    }
    if (all_valid) { draken_free(out); return nullptr; }
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

// Shape-preserving result (CLAUDE.md §11, architect-approved for arithmetic).
// `values` holds src.data_length computed results (one per physical value);
// this grafts src's encoding shape — selection + per-logical-row validity — onto
// the result so dense→dense, constant→constant, dict→dict. Mirrors
// kernels/result_helpers.h::kernel_preserve_shape, kept self-contained for ops/.
// `values` is owned and freed on the error path.
static inline VecResult make_shaped_result(int64_t* values, const DrakenVector& src) {
    VecResult r;
    r.data        = values;
    r.type        = DRAKEN_INT64;
    r.length      = src.length;
    r.data_length = src.data_length;
    r.flags       = src.flags;
    r.validity    = copy_validity(src.validity, src.length);
    if (src.flags & DRAKEN_SEL_IDENTITY) {
        r.selection      = draken_identity_sel(src.length);   // dense: shared global
        r.owns_selection = false;
    } else if (src.data_length == 1u) {
        r.selection      = draken_zero_sel(src.length);       // constant: shared global
        r.owns_selection = false;
    } else {
        const size_t cn = src.length > 0u ? src.length : 1u;  // dict: copy owned codes
        uint32_t* codes = static_cast<uint32_t*>(draken_malloc(cn * sizeof(uint32_t)));
        if (!codes) { draken_free(values); throw std::bad_alloc(); }
        memcpy(codes, src.selection, static_cast<size_t>(src.length) * sizeof(uint32_t));
        r.selection      = codes;
        r.owns_selection = true;
    }
    return r;
}

// Constant-operand fast path for a binary op. When exactly one operand is a
// non-null constant (draken_is_constant gate), compute over the OTHER operand's
// data_length physical values and preserve its shape; when both are constant,
// the result is constant. Returns true + fills `out` when applied. A null
// constant (validity != nullptr) is NOT folded — it falls through to the general
// path, which propagates the all-null result correctly.
template <typename Op>
static inline bool i64_const_fold(const DrakenVector& a, const DrakenVector& b,
                                  Op op, VecResult& out) {
    const bool a_const = draken_is_constant(&a) && a.validity == nullptr;
    const bool b_const = draken_is_constant(&b) && b.validity == nullptr;
    if (!a_const && !b_const) return false;
    const int64_t* ad = static_cast<const int64_t*>(a.data);
    const int64_t* bd = static_cast<const int64_t*>(b.data);
    if (b_const) {                       // a varies (or is also constant) — shape from a
        const int64_t s = bd[0];
        const uint32_t k = a.data_length;
        int64_t* dst = alloc_i64(k);
        for (uint32_t j = 0; j < k; ++j) dst[j] = op(ad[j], s);
        out = make_shaped_result(dst, a);
    } else {                             // a constant, b varies — shape from b
        const int64_t s = ad[0];
        const uint32_t k = b.data_length;
        int64_t* dst = alloc_i64(k);
        for (uint32_t j = 0; j < k; ++j) dst[j] = op(s, bd[j]);
        out = make_shaped_result(dst, b);
    }
    return true;
}

// ---------------------------------------------------------------------------
// ADD
// ---------------------------------------------------------------------------
static inline VecResult i64_add(const DrakenVector& a, const DrakenVector& b) {
    if (a.length != b.length)
        throw std::invalid_argument("i64_add: length mismatch");
    VecResult out;
    if (i64_const_fold(a, b, [](int64_t x, int64_t y){ return x + y; }, out)) return out;
    const uint32_t n = a.length;
    const int64_t* ad = static_cast<const int64_t*>(a.data);
    const int64_t* bd = static_cast<const int64_t*>(b.data);
    int64_t* dst = alloc_i64(n);
    for (uint32_t i = 0; i < n; ++i)
        dst[i] = ad[a.selection[i]] + bd[b.selection[i]];
    return make_dense_result(dst, combine_validity(a.validity, b.validity, n), n);
}

// Scalar variant: b is a single int64 constant (no validity). Shape-preserving:
// computes over a.data_length physical values (dict→dict, constant→constant).
static inline VecResult i64_add_scalar(const DrakenVector& a, int64_t scalar) {
    const int64_t* ad = static_cast<const int64_t*>(a.data);
    const uint32_t k = a.data_length;
    int64_t* dst = alloc_i64(k);
    for (uint32_t j = 0; j < k; ++j) dst[j] = ad[j] + scalar;
    return make_shaped_result(dst, a);
}

// ---------------------------------------------------------------------------
// SUB
// ---------------------------------------------------------------------------
static inline VecResult i64_sub(const DrakenVector& a, const DrakenVector& b) {
    if (a.length != b.length)
        throw std::invalid_argument("i64_sub: length mismatch");
    VecResult out;
    if (i64_const_fold(a, b, [](int64_t x, int64_t y){ return x - y; }, out)) return out;
    const uint32_t n = a.length;
    const int64_t* ad = static_cast<const int64_t*>(a.data);
    const int64_t* bd = static_cast<const int64_t*>(b.data);
    int64_t* dst = alloc_i64(n);
    for (uint32_t i = 0; i < n; ++i)
        dst[i] = ad[a.selection[i]] - bd[b.selection[i]];
    return make_dense_result(dst, combine_validity(a.validity, b.validity, n), n);
}

static inline VecResult i64_sub_scalar(const DrakenVector& a, int64_t scalar) {
    const int64_t* ad = static_cast<const int64_t*>(a.data);
    const uint32_t k = a.data_length;
    int64_t* dst = alloc_i64(k);
    for (uint32_t j = 0; j < k; ++j) dst[j] = ad[j] - scalar;
    return make_shaped_result(dst, a);
}

// ---------------------------------------------------------------------------
// MUL
// ---------------------------------------------------------------------------
static inline VecResult i64_mul(const DrakenVector& a, const DrakenVector& b) {
    if (a.length != b.length)
        throw std::invalid_argument("i64_mul: length mismatch");
    VecResult out;
    if (i64_const_fold(a, b, [](int64_t x, int64_t y){ return x * y; }, out)) return out;
    const uint32_t n = a.length;
    const int64_t* ad = static_cast<const int64_t*>(a.data);
    const int64_t* bd = static_cast<const int64_t*>(b.data);
    int64_t* dst = alloc_i64(n);
    for (uint32_t i = 0; i < n; ++i)
        dst[i] = ad[a.selection[i]] * bd[b.selection[i]];
    return make_dense_result(dst, combine_validity(a.validity, b.validity, n), n);
}

static inline VecResult i64_mul_scalar(const DrakenVector& a, int64_t scalar) {
    const int64_t* ad = static_cast<const int64_t*>(a.data);
    const uint32_t k = a.data_length;
    int64_t* dst = alloc_i64(k);
    for (uint32_t j = 0; j < k; ++j) dst[j] = ad[j] * scalar;
    return make_shaped_result(dst, a);
}

// ---------------------------------------------------------------------------
// DIV — integer division, C truncation toward zero, div-by-zero → 0.
// (cdivision=True semantics.)
// Note: true-division returning float64 is out of scope until float64 is built.
// ---------------------------------------------------------------------------
static inline VecResult i64_div(const DrakenVector& a, const DrakenVector& b) {
    if (a.length != b.length)
        throw std::invalid_argument("i64_div: length mismatch");
    VecResult out;
    if (i64_const_fold(a, b, [](int64_t x, int64_t y){ return y == 0 ? (int64_t)0 : x / y; }, out)) return out;
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
    const int64_t* ad = static_cast<const int64_t*>(a.data);
    const uint32_t k = a.data_length;
    int64_t* dst = alloc_i64(k);
    if (scalar == 0) {
        for (uint32_t j = 0; j < k; ++j) dst[j] = 0;
    } else {
        for (uint32_t j = 0; j < k; ++j) dst[j] = ad[j] / scalar;
    }
    return make_shaped_result(dst, a);
}

// ---------------------------------------------------------------------------
// MOD — C truncation-based modulo, mod-by-zero → 0.
// ---------------------------------------------------------------------------
static inline VecResult i64_mod(const DrakenVector& a, const DrakenVector& b) {
    if (a.length != b.length)
        throw std::invalid_argument("i64_mod: length mismatch");
    VecResult out;
    if (i64_const_fold(a, b, [](int64_t x, int64_t y){ return y == 0 ? (int64_t)0 : x % y; }, out)) return out;
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
    const int64_t* ad = static_cast<const int64_t*>(a.data);
    const uint32_t k = a.data_length;
    int64_t* dst = alloc_i64(k);
    if (scalar == 0) {
        for (uint32_t j = 0; j < k; ++j) dst[j] = 0;
    } else {
        for (uint32_t j = 0; j < k; ++j) dst[j] = ad[j] % scalar;
    }
    return make_shaped_result(dst, a);
}

// ---------------------------------------------------------------------------
// NEG — unary negation.
// neg(INT64_MIN) wraps to INT64_MIN (signed overflow, platform-deterministic
// on all our targets).
// ---------------------------------------------------------------------------
static inline VecResult i64_neg(const DrakenVector& a) {
    // Unary → always shape-preserving: negate the data_length physical values and
    // reuse a's selection (dense→dense, constant→constant, dict→dict).
    const int64_t* ad = static_cast<const int64_t*>(a.data);
    const uint32_t k = a.data_length;
    int64_t* dst = alloc_i64(k);
    for (uint32_t j = 0; j < k; ++j) dst[j] = -ad[j];
    return make_shaped_result(dst, a);
}

}} // namespace draken::ops
