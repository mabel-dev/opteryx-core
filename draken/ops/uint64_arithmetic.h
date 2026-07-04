#pragma once
// draken/ops/uint64_arithmetic.h — uint64 × uint64 arithmetic kernels (E33).
//
// Mirrors int64_arithmetic.h exactly, except:
//   - genuinely unsigned semantics throughout (div/mod are unsigned, not the
//     C truncation-toward-zero the signed family uses — matters for values
//     that would be "negative" if misread as signed).
//   - add/sub/mul overflow: wraps (well-defined for unsigned, unlike the
//     signed family's implementation-defined wrap).
//   - no neg() — unsigned has no negation; the OpsTable slot stays
//     unregistered, so dispatching it fails loudly rather than silently
//     wrapping to a nonsensical "negative unsigned" value.
//
// Ops: add / sub / mul / div / mod
// Result type: always uint64 (VecResult with type == DRAKEN_UINT64).
// Null propagation: any null input -> null output for that row.

#include <stdint.h>
#include <stddef.h>
#include <string.h>
#include <new>
#include <stdexcept>
#include "core/buffers.h"
#include "core/alloc.h"
#include "core/vector_alloc.h"
#include "ops/vec_result.h"

namespace draken { namespace ops {

// ---------------------------------------------------------------------------
// Helpers (u64a_ prefix — no ODR conflicts with int64_arithmetic.h's helpers)
// ---------------------------------------------------------------------------

static inline uint8_t* u64a_combine_validity(
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

static inline uint8_t* u64a_copy_validity(const uint8_t* src, uint32_t n) {
    if (src == nullptr) return nullptr;
    uint32_t nb = (n + 7u) >> 3;
    uint8_t* out = static_cast<uint8_t*>(draken_malloc(nb));
    if (!out) throw std::bad_alloc();
    memcpy(out, src, nb);
    return out;
}

static inline uint64_t* u64a_alloc(uint32_t n) {
    if (n == 0) n = 1;
    uint64_t* p = static_cast<uint64_t*>(draken_malloc(n * sizeof(uint64_t)));
    if (!p) throw std::bad_alloc();
    return p;
}

static inline VecResult u64a_make_dense_result(
    uint64_t* data, uint8_t* validity, uint32_t n)
{
    VecResult r;
    r.data           = data;
    r.validity       = validity;
    r.selection      = draken_identity_sel(n);
    r.owns_selection = false;
    r.data_length    = n;
    r.length         = n;
    r.type           = DRAKEN_UINT64;
    r.flags          = static_cast<uint8_t>(DRAKEN_SEL_IDENTITY | DRAKEN_SEL_PERMUTATION);
    return r;
}

// Shape-preserving result (CLAUDE.md §11) — mirrors int64_arithmetic.h's
// make_shaped_result exactly, uint64 flavoured.
static inline VecResult u64a_make_shaped_result(uint64_t* values, const DrakenVector& src) {
    VecResult r;
    r.data        = values;
    r.type        = DRAKEN_UINT64;
    r.length      = src.length;
    r.data_length = src.data_length;
    r.flags       = src.flags;
    r.validity    = u64a_copy_validity(src.validity, src.length);
    if (src.flags & DRAKEN_SEL_IDENTITY) {
        r.selection      = draken_identity_sel(src.length);
        r.owns_selection = false;
    } else if (src.data_length == 1u) {
        r.selection      = draken_zero_sel(src.length);
        r.owns_selection = false;
    } else {
        const size_t cn = src.length > 0u ? src.length : 1u;
        uint32_t* codes = static_cast<uint32_t*>(draken_malloc(cn * sizeof(uint32_t)));
        if (!codes) { draken_free(values); throw std::bad_alloc(); }
        memcpy(codes, src.selection, static_cast<size_t>(src.length) * sizeof(uint32_t));
        r.selection      = codes;
        r.owns_selection = true;
    }
    return r;
}

template <typename Op>
static inline bool u64a_const_fold(const DrakenVector& a, const DrakenVector& b,
                                   Op op, VecResult& out) {
    const bool a_const = draken_is_constant(&a) && a.validity == nullptr;
    const bool b_const = draken_is_constant(&b) && b.validity == nullptr;
    if (!a_const && !b_const) return false;
    const uint64_t* ad = static_cast<const uint64_t*>(a.data);
    const uint64_t* bd = static_cast<const uint64_t*>(b.data);
    if (b_const) {
        const uint64_t s = bd[0];
        const uint32_t k = a.data_length;
        uint64_t* dst = u64a_alloc(k);
        for (uint32_t j = 0; j < k; ++j) dst[j] = op(ad[j], s);
        out = u64a_make_shaped_result(dst, a);
    } else {
        const uint64_t s = ad[0];
        const uint32_t k = b.data_length;
        uint64_t* dst = u64a_alloc(k);
        for (uint32_t j = 0; j < k; ++j) dst[j] = op(s, bd[j]);
        out = u64a_make_shaped_result(dst, b);
    }
    return true;
}

// ---------------------------------------------------------------------------
// ADD / SUB / MUL — wrap on overflow (well-defined for uint64_t).
// ---------------------------------------------------------------------------
static inline VecResult u64_add(const DrakenVector& a, const DrakenVector& b) {
    if (a.length != b.length)
        throw std::invalid_argument("u64_add: length mismatch");
    VecResult out;
    if (u64a_const_fold(a, b, [](uint64_t x, uint64_t y){ return x + y; }, out)) return out;
    const uint32_t n = a.length;
    const uint64_t* ad = static_cast<const uint64_t*>(a.data);
    const uint64_t* bd = static_cast<const uint64_t*>(b.data);
    uint64_t* dst = u64a_alloc(n);
    for (uint32_t i = 0; i < n; ++i)
        dst[i] = ad[a.selection[i]] + bd[b.selection[i]];
    return u64a_make_dense_result(dst, u64a_combine_validity(a.validity, b.validity, n), n);
}

static inline VecResult u64_add_scalar(const DrakenVector& a, int64_t scalar) {
    const uint64_t s = static_cast<uint64_t>(scalar);
    const uint64_t* ad = static_cast<const uint64_t*>(a.data);
    const uint32_t k = a.data_length;
    uint64_t* dst = u64a_alloc(k);
    for (uint32_t j = 0; j < k; ++j) dst[j] = ad[j] + s;
    return u64a_make_shaped_result(dst, a);
}

static inline VecResult u64_sub(const DrakenVector& a, const DrakenVector& b) {
    if (a.length != b.length)
        throw std::invalid_argument("u64_sub: length mismatch");
    VecResult out;
    if (u64a_const_fold(a, b, [](uint64_t x, uint64_t y){ return x - y; }, out)) return out;
    const uint32_t n = a.length;
    const uint64_t* ad = static_cast<const uint64_t*>(a.data);
    const uint64_t* bd = static_cast<const uint64_t*>(b.data);
    uint64_t* dst = u64a_alloc(n);
    for (uint32_t i = 0; i < n; ++i)
        dst[i] = ad[a.selection[i]] - bd[b.selection[i]];
    return u64a_make_dense_result(dst, u64a_combine_validity(a.validity, b.validity, n), n);
}

static inline VecResult u64_sub_scalar(const DrakenVector& a, int64_t scalar) {
    const uint64_t s = static_cast<uint64_t>(scalar);
    const uint64_t* ad = static_cast<const uint64_t*>(a.data);
    const uint32_t k = a.data_length;
    uint64_t* dst = u64a_alloc(k);
    for (uint32_t j = 0; j < k; ++j) dst[j] = ad[j] - s;
    return u64a_make_shaped_result(dst, a);
}

static inline VecResult u64_mul(const DrakenVector& a, const DrakenVector& b) {
    if (a.length != b.length)
        throw std::invalid_argument("u64_mul: length mismatch");
    VecResult out;
    if (u64a_const_fold(a, b, [](uint64_t x, uint64_t y){ return x * y; }, out)) return out;
    const uint32_t n = a.length;
    const uint64_t* ad = static_cast<const uint64_t*>(a.data);
    const uint64_t* bd = static_cast<const uint64_t*>(b.data);
    uint64_t* dst = u64a_alloc(n);
    for (uint32_t i = 0; i < n; ++i)
        dst[i] = ad[a.selection[i]] * bd[b.selection[i]];
    return u64a_make_dense_result(dst, u64a_combine_validity(a.validity, b.validity, n), n);
}

static inline VecResult u64_mul_scalar(const DrakenVector& a, int64_t scalar) {
    const uint64_t s = static_cast<uint64_t>(scalar);
    const uint64_t* ad = static_cast<const uint64_t*>(a.data);
    const uint32_t k = a.data_length;
    uint64_t* dst = u64a_alloc(k);
    for (uint32_t j = 0; j < k; ++j) dst[j] = ad[j] * s;
    return u64a_make_shaped_result(dst, a);
}

// ---------------------------------------------------------------------------
// DIV / MOD — genuinely unsigned division/modulo, div/mod-by-zero -> 0
// (matches the signed family's zero-by-zero convention, not its truncation
// semantics — unsigned division has no "toward zero" ambiguity to begin with).
// ---------------------------------------------------------------------------
static inline VecResult u64_div(const DrakenVector& a, const DrakenVector& b) {
    if (a.length != b.length)
        throw std::invalid_argument("u64_div: length mismatch");
    VecResult out;
    if (u64a_const_fold(a, b, [](uint64_t x, uint64_t y){ return y == 0u ? (uint64_t)0 : x / y; }, out)) return out;
    const uint32_t n = a.length;
    const uint64_t* ad = static_cast<const uint64_t*>(a.data);
    const uint64_t* bd = static_cast<const uint64_t*>(b.data);
    uint64_t* dst = u64a_alloc(n);
    for (uint32_t i = 0; i < n; ++i) {
        const uint64_t bv = bd[b.selection[i]];
        dst[i] = (bv == 0u) ? 0u : ad[a.selection[i]] / bv;
    }
    return u64a_make_dense_result(dst, u64a_combine_validity(a.validity, b.validity, n), n);
}

static inline VecResult u64_div_scalar(const DrakenVector& a, int64_t scalar) {
    const uint64_t s = static_cast<uint64_t>(scalar);
    const uint64_t* ad = static_cast<const uint64_t*>(a.data);
    const uint32_t k = a.data_length;
    uint64_t* dst = u64a_alloc(k);
    if (s == 0u) {
        for (uint32_t j = 0; j < k; ++j) dst[j] = 0u;
    } else {
        for (uint32_t j = 0; j < k; ++j) dst[j] = ad[j] / s;
    }
    return u64a_make_shaped_result(dst, a);
}

static inline VecResult u64_mod(const DrakenVector& a, const DrakenVector& b) {
    if (a.length != b.length)
        throw std::invalid_argument("u64_mod: length mismatch");
    VecResult out;
    if (u64a_const_fold(a, b, [](uint64_t x, uint64_t y){ return y == 0u ? (uint64_t)0 : x % y; }, out)) return out;
    const uint32_t n = a.length;
    const uint64_t* ad = static_cast<const uint64_t*>(a.data);
    const uint64_t* bd = static_cast<const uint64_t*>(b.data);
    uint64_t* dst = u64a_alloc(n);
    for (uint32_t i = 0; i < n; ++i) {
        const uint64_t bv = bd[b.selection[i]];
        dst[i] = (bv == 0u) ? 0u : ad[a.selection[i]] % bv;
    }
    return u64a_make_dense_result(dst, u64a_combine_validity(a.validity, b.validity, n), n);
}

static inline VecResult u64_mod_scalar(const DrakenVector& a, int64_t scalar) {
    const uint64_t s = static_cast<uint64_t>(scalar);
    const uint64_t* ad = static_cast<const uint64_t*>(a.data);
    const uint32_t k = a.data_length;
    uint64_t* dst = u64a_alloc(k);
    if (s == 0u) {
        for (uint32_t j = 0; j < k; ++j) dst[j] = 0u;
    } else {
        for (uint32_t j = 0; j < k; ++j) dst[j] = ad[j] % s;
    }
    return u64a_make_shaped_result(dst, a);
}

// ---------------------------------------------------------------------------
// SUM / MIN / MAX (E33) — genuinely unsigned reduction. *out_value carries the
// TRUE uint64_t result's bit pattern reinterpreted into the int64_t slot the
// ReduceFn signature requires; the Python-boxing site (draken_native.cpp's
// sum()/min()/max() bindings) must reinterpret it back via static_cast<uint64_t>
// before handing it to Python — mirrors the same bit-reinterpretation already
// used for compare_scalar's literal packing. Unlike int64_reductions.h, this is
// a straightforward O(n) scan — no sorted-dict shortcut yet (performance
// follow-up, not a correctness gap).
// ---------------------------------------------------------------------------
static inline bool u64r_row_valid(const uint8_t* validity, uint32_t i) noexcept {
    return (validity == nullptr) || ((validity[i >> 3] >> (i & 7u)) & 1u);
}

static inline uint32_t u64_sum(const DrakenVector& v, int64_t* out_value) noexcept {
    const uint32_t n = v.length;
    const uint64_t* data = static_cast<const uint64_t*>(v.data);
    const uint8_t* validity = v.validity;
    uint64_t total = 0;
    uint32_t count = 0;
    for (uint32_t i = 0; i < n; ++i) {
        if (!u64r_row_valid(validity, i)) continue;
        total += data[v.selection[i]];
        ++count;
    }
    *out_value = static_cast<int64_t>(total);
    return count;
}

static inline uint32_t u64_min(const DrakenVector& v, int64_t* out_min) noexcept {
    const uint32_t n = v.length;
    const uint64_t* data = static_cast<const uint64_t*>(v.data);
    const uint8_t* validity = v.validity;
    uint64_t m = UINT64_MAX;
    uint32_t count = 0;
    for (uint32_t i = 0; i < n; ++i) {
        if (!u64r_row_valid(validity, i)) continue;
        const uint64_t val = data[v.selection[i]];
        if (val < m) m = val;
        ++count;
    }
    *out_min = static_cast<int64_t>(m);
    return count;
}

static inline uint32_t u64_max(const DrakenVector& v, int64_t* out_max) noexcept {
    const uint32_t n = v.length;
    const uint64_t* data = static_cast<const uint64_t*>(v.data);
    const uint8_t* validity = v.validity;
    uint64_t m = 0;
    uint32_t count = 0;
    for (uint32_t i = 0; i < n; ++i) {
        if (!u64r_row_valid(validity, i)) continue;
        const uint64_t val = data[v.selection[i]];
        if (val > m) m = val;
        ++count;
    }
    *out_max = static_cast<int64_t>(m);
    return count;
}

}} // namespace draken::ops
