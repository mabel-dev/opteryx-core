#pragma once
// draken/ops/uint64_compare.h — compare_scalar and compare_vector for uint64 (E33).
//
// Mirrors int64_compare.h's contract exactly (bit-packed DRAKEN_BOOL result,
// same op codes, same 3VL null semantics) but compares as genuine uint64_t —
// NOT via the int64_t-cast trick fixed_int_ops.h's templates use for narrower
// unsigned widths, since a UINT64 value >= 2^63 would compare incorrectly
// (as negative) if cast to int64_t. This is a scalar (non-SIMD-unrolled)
// implementation — correctness first; the 8-way SIMD-friendly bit-pack
// int64_compare.h uses is a follow-up performance pass, not a correctness gap.
//
// OP CODES: 0=eq 1=ne 2=gt 3=ge 4=lt 5=le

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

struct U64CmpEq { static inline bool apply(uint64_t a, uint64_t b) noexcept { return a == b; } };
struct U64CmpNe { static inline bool apply(uint64_t a, uint64_t b) noexcept { return a != b; } };
struct U64CmpGt { static inline bool apply(uint64_t a, uint64_t b) noexcept { return a >  b; } };
struct U64CmpGe { static inline bool apply(uint64_t a, uint64_t b) noexcept { return a >= b; } };
struct U64CmpLt { static inline bool apply(uint64_t a, uint64_t b) noexcept { return a <  b; } };
struct U64CmpLe { static inline bool apply(uint64_t a, uint64_t b) noexcept { return a <= b; } };

static inline uint8_t* u64c_alloc_bool_buf(uint32_t n) {
    const uint32_t raw    = (n + 7u) >> 3;
    const uint32_t padded = (raw + 7u) & ~7u;
    const size_t   bytes  = padded > 0u ? padded : 8u;
    uint8_t* p = static_cast<uint8_t*>(draken_malloc(bytes));
    if (!p) throw std::bad_alloc();
    memset(p, 0, bytes);
    return p;
}

// Copy a validity bitmap for n rows. Masks the partial last byte so padding
// bits in the source are not propagated (same contract as cmp_copy_validity,
// int64_compare.h).
static inline uint8_t* u64c_copy_validity(const uint8_t* src, uint32_t n) {
    uint8_t* dst = u64c_alloc_bool_buf(n);
    const uint32_t nb = (n + 7u) >> 3;
    if (nb > 0) {
        memcpy(dst, src, nb);
        if ((n & 7u) != 0)
            dst[nb - 1] &= static_cast<uint8_t>((1u << (n & 7u)) - 1u);
    }
    return dst;
}

// AND of two validity bitmaps; nullptr when both inputs are nullptr. Mirrors
// cmp_and_validity's contract: an all-valid AND result is freed and returned
// as nullptr so downstream kernels take the no-nulls path.
static inline uint8_t* u64c_and_validity(const uint8_t* a, const uint8_t* b, uint32_t n) {
    if (a == nullptr && b == nullptr) return nullptr;
    if (a == nullptr) return u64c_copy_validity(b, n);
    if (b == nullptr) return u64c_copy_validity(a, n);
    uint8_t* out = u64c_alloc_bool_buf(n);
    const uint32_t nb = (n + 7u) >> 3;
    const uint8_t tail_mask = (n & 7u) ? static_cast<uint8_t>((1u << (n & 7u)) - 1u) : 0xFFu;
    bool all_valid = true;
    for (uint32_t k = 0; k < nb; ++k) {
        const uint8_t expected = (k == nb - 1u) ? tail_mask : 0xFFu;
        out[k] = static_cast<uint8_t>((a[k] & b[k]) & expected);
        all_valid = all_valid && (out[k] == expected);
    }
    if (all_valid) { draken_free(out); return nullptr; }
    return out;
}

template<typename Op>
static inline VecResult u64c_compare_scalar_impl(const DrakenVector& v, uint64_t scalar) {
    const uint32_t n = v.length;
    const uint64_t* data = static_cast<const uint64_t*>(v.data);
    const uint8_t* src_null = v.validity;

    uint8_t* dst = u64c_alloc_bool_buf(n);
    uint8_t* out_null = (src_null != nullptr) ? u64c_copy_validity(src_null, n) : nullptr;

    for (uint32_t i = 0; i < n; ++i) {
        if (src_null != nullptr && !((src_null[i >> 3] >> (i & 7u)) & 1u)) continue;
        if (Op::apply(data[v.selection[i]], scalar))
            dst[i >> 3] |= static_cast<uint8_t>(1u << (i & 7u));
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

// scalar is the caller's int64_t-packed slot, reinterpreted as uint64_t bits
// (matches the reinterpretation the nanobind boundary performs when packing a
// UINT64 Python literal — see draken_native.cpp's compare_scalar binding).
static inline VecResult u64_compare_scalar(const DrakenVector& v, int64_t scalar, int op) {
    const uint64_t s = static_cast<uint64_t>(scalar);
    switch (op) {
        case 0: return u64c_compare_scalar_impl<U64CmpEq>(v, s);
        case 1: return u64c_compare_scalar_impl<U64CmpNe>(v, s);
        case 2: return u64c_compare_scalar_impl<U64CmpGt>(v, s);
        case 3: return u64c_compare_scalar_impl<U64CmpGe>(v, s);
        case 4: return u64c_compare_scalar_impl<U64CmpLt>(v, s);
        default: return u64c_compare_scalar_impl<U64CmpLe>(v, s);
    }
}

template<typename Op>
static inline VecResult u64c_compare_vector_impl(const DrakenVector& a, const DrakenVector& b) {
    const uint32_t n = a.length;
    if (b.length != n)
        throw std::invalid_argument("u64_compare_vector: operand lengths must match");
    const uint64_t* ad = static_cast<const uint64_t*>(a.data);
    const uint64_t* bd = static_cast<const uint64_t*>(b.data);

    uint8_t* out_null = u64c_and_validity(a.validity, b.validity, n);
    uint8_t* dst = u64c_alloc_bool_buf(n);

    for (uint32_t i = 0; i < n; ++i) {
        if (out_null != nullptr && !((out_null[i >> 3] >> (i & 7u)) & 1u)) continue;
        if (Op::apply(ad[a.selection[i]], bd[b.selection[i]]))
            dst[i >> 3] |= static_cast<uint8_t>(1u << (i & 7u));
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

static inline VecResult u64_compare_vector(const DrakenVector& a, const DrakenVector& b, int op) {
    switch (op) {
        case 0: return u64c_compare_vector_impl<U64CmpEq>(a, b);
        case 1: return u64c_compare_vector_impl<U64CmpNe>(a, b);
        case 2: return u64c_compare_vector_impl<U64CmpGt>(a, b);
        case 3: return u64c_compare_vector_impl<U64CmpGe>(a, b);
        case 4: return u64c_compare_vector_impl<U64CmpLt>(a, b);
        default: return u64c_compare_vector_impl<U64CmpLe>(a, b);
    }
}

// BETWEEN — genuinely unsigned (fixed_int_between<T>'s static_cast<int64_t>(T)
// would misorder a value >= 2^63 as negative, the same corruption class as the
// already-fixed compare_scalar bug — cannot reuse that template for uint64_t).
// lo/hi arrive as the caller's int64_t-packed slot, reinterpreted as uint64_t
// bits (matches compare_scalar's convention).
static inline VecResult u64_between(const DrakenVector& v, int64_t lo, int64_t hi,
                                    bool lo_incl, bool hi_incl) {
    const uint64_t ulo = static_cast<uint64_t>(lo);
    const uint64_t uhi = static_cast<uint64_t>(hi);
    const uint32_t n = v.length;
    const uint64_t* data = static_cast<const uint64_t*>(v.data);
    const uint8_t* src_null = v.validity;

    uint8_t* dst = u64c_alloc_bool_buf(n);
    uint8_t* out_null = (src_null != nullptr) ? u64c_copy_validity(src_null, n) : nullptr;

    for (uint32_t i = 0; i < n; ++i) {
        if (src_null != nullptr && !((src_null[i >> 3] >> (i & 7u)) & 1u)) continue;
        const uint64_t x = data[v.selection[i]];
        const bool lo_ok = lo_incl ? (x >= ulo) : (x > ulo);
        const bool hi_ok = hi_incl ? (x <= uhi) : (x < uhi);
        if (lo_ok && hi_ok)
            dst[i >> 3] |= static_cast<uint8_t>(1u << (i & 7u));
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
