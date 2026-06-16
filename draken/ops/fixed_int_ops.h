#pragma once
// draken/ops/fixed_int_ops.h — templated kernels for int8_t / int16_t / int32_t.
//
// Template strategy:
//   T   = element type (int8_t, int16_t, int32_t)
//   TAG = corresponding DrakenType (DRAKEN_INT8, DRAKEN_INT16, DRAKEN_INT32)
//
// ARITHMETIC RESULT TYPE (Architect decision, D.6):
//   int8+int8   → int16 (DRAKEN_INT16)
//   int16+int16 → int32 (DRAKEN_INT32)
//   int32+int32 → int64 (DRAKEN_INT64)
//   Cross-width: promote narrower to the wider type first (lossless), then
//   the wider type's "next power" rule applies.
//   Rationale: there are no narrow-int arithmetic kernels to inherit;
//   the architect chose "widen to next power" for draken.
//
// COMPARE / BETWEEN / IN_LIST:
//   T values are widened to int64_t before comparison against int64_t scalar.
//
// SUM / MIN / MAX:
//   Accumulate into int64_t (avoids narrow-type overflow in reductions).
//
// GATHER (take / materialize / compress):
//   Result type stays T (same width). Compact T elements in output.
//
// HASH:
//   Sign-extend T → int64_t → uint64_t, then simd_hash_i64.
//   Ensures value 5 as int8 hashes identically to 5 as int64.
//
// All internal helper symbols use fi_ prefix to avoid ODR clashes.

#include <stdint.h>
#include <stddef.h>
#include <string.h>
#include <stdexcept>
#include <limits>
#include <unordered_map>
#include <vector>

#include "core/buffers.h"
#include "core/alloc.h"
#include "core/vector_alloc.h"
#include "ops/vec_result.h"
#include "ops/int64_compare.h"    // CmpEq/Ne/Gt/Ge/Lt/Le, cmp_alloc_bool_buf,
                                  // cmp_copy_validity, cmp_and_validity
#include "ops/int64_predicates.h" // BetweenOp<lo_incl,hi_incl>
#include "simd_hash.h"            // simd_hash_i64, NULL_HASH
#include "carchar_set.hpp"        // opteryx::carchar::CarcharSet

namespace draken { namespace ops {

// ===========================================================================
// NextWider: T → next wider signed type and its DRAKEN tag.
// ===========================================================================
template<typename T> struct NextWider;
template<> struct NextWider<int8_t>  {
    using type = int16_t;
    static constexpr DrakenType tag = DRAKEN_INT16;
};
template<> struct NextWider<int16_t> {
    using type = int32_t;
    static constexpr DrakenType tag = DRAKEN_INT32;
};
template<> struct NextWider<int32_t> {
    using type = int64_t;
    static constexpr DrakenType tag = DRAKEN_INT64;
};

// ===========================================================================
// Internal helpers (fi_ prefix)
// ===========================================================================

static inline bool fi_row_valid(const uint8_t* validity, uint32_t i) noexcept {
    return (validity == nullptr) || ((validity[i >> 3] >> (i & 7)) & 1u);
}

static inline void fi_set_valid_bit(uint8_t* bitmap, uint32_t i) noexcept {
    bitmap[i >> 3] |= static_cast<uint8_t>(1u << (i & 7));
}

static inline uint8_t* fi_copy_validity(const uint8_t* src, uint32_t n) {
    if (src == nullptr) return nullptr;
    const uint32_t nb = (n + 7u) >> 3;
    const size_t bytes = nb > 0u ? static_cast<size_t>(nb) : 1u;
    uint8_t* dst = static_cast<uint8_t*>(draken_malloc(bytes));
    if (!dst) throw std::bad_alloc();
    memcpy(dst, src, nb);
    return dst;
}

static inline uint8_t* fi_normalize_validity(uint8_t* validity, uint32_t n) noexcept {
    if (validity == nullptr) return nullptr;
    const uint32_t nb = (n + 7u) >> 3;
    for (uint32_t k = 0; k < nb; ++k) {
        uint8_t expected = 0xFFu;
        if (k == nb - 1u && (n & 7u) != 0u)
            expected = static_cast<uint8_t>((1u << (n & 7u)) - 1u);
        if (validity[k] != expected) return validity;
    }
    draken_free(validity);
    return nullptr;
}

static inline uint8_t* fi_combine_validity(
    const uint8_t* a, const uint8_t* b, uint32_t n)
{
    if (a == nullptr && b == nullptr) return nullptr;
    const uint32_t nb = (n + 7u) >> 3;
    const size_t bytes = nb > 0u ? static_cast<size_t>(nb) : 1u;
    uint8_t* out = static_cast<uint8_t*>(draken_malloc(bytes));
    if (!out) throw std::bad_alloc();
    if (a != nullptr && b != nullptr)
        for (uint32_t k = 0; k < nb; ++k) out[k] = a[k] & b[k];
    else if (a != nullptr)
        memcpy(out, a, nb);
    else
        memcpy(out, b, nb);
    // Drop a fully-valid result so downstream takes the validity==nullptr path.
    return fi_normalize_validity(out, n);
}

template<typename T>
static inline T* fi_alloc(uint32_t n) {
    const size_t bytes = (n > 0u ? static_cast<size_t>(n) : 1u) * sizeof(T);
    T* p = static_cast<T*>(draken_malloc(bytes));
    if (!p) throw std::bad_alloc();
    return p;
}

template<typename T, DrakenType TAG>
static inline VecResult fi_make_dense(T* data, uint8_t* validity, uint32_t n) {
    VecResult r;
    r.data           = data;
    r.validity       = validity;
    r.selection      = draken_identity_sel(n);
    r.owns_selection = false;
    r.data_length    = n;
    r.length         = n;
    r.type           = TAG;
    r.flags          = static_cast<uint8_t>(DRAKEN_SEL_IDENTITY | DRAKEN_SEL_PERMUTATION);
    return r;
}

// ===========================================================================
// ARITHMETIC (D.6 widen-to-next-power) — int8/16/32/64 element-wise.
//
// P9.1 (unify binops onto the registry kernel_fn ABI). Reads every operand as
// int64 (sign-extended through its declared width, via selection so all three
// vector shapes work uniformly), applies the op with the proven i64 semantics
// (div/mod by zero → 0, C truncation toward zero — matches int64_arithmetic.h),
// and writes the D.6 result width: int8→int16, int16→int32, int32→int64; for
// cross-width the wider operand's rank wins. Add/sub/mul cannot overflow the
// widened result, so the downcast to the result width is lossless. Output is
// dense; validity is the per-logical-row AND of the inputs.
//
// TRUE division (BOP_DIVIDE → float64) is NOT handled here — the dispatch routes
// it to the float path. Covers PLUS/MINUS/MULTIPLY/MODULO/INT_DIVIDE only.
// ===========================================================================

// Sign-extend the i-th logical value of any signed-int vector to int64.
static inline int64_t fi_read_i64(const DrakenVector& v, uint32_t i) {
    const uint32_t p = v.selection[i];
    switch (v.type) {
        case DRAKEN_INT8:  return static_cast<const int8_t*>(v.data)[p];
        case DRAKEN_INT16: return static_cast<const int16_t*>(v.data)[p];
        case DRAKEN_INT32: return static_cast<const int32_t*>(v.data)[p];
        case DRAKEN_INT64: return static_cast<const int64_t*>(v.data)[p];
        default: throw std::invalid_argument("fi_read_i64: non-integer type");
    }
}

// op codes match BCBinaryOpCode: 1=PLUS 2=MINUS 3=MULTIPLY 5=MODULO 6=INT_DIVIDE.
static inline int64_t fi_apply_i64(int op, int64_t x, int64_t y) {
    switch (op) {
        case 1: return x + y;
        case 2: return x - y;
        case 3: return x * y;
        case 5: return (y == 0) ? 0 : x % y;   // mod-by-zero → 0 (matches i64_mod)
        case 6: return (y == 0) ? 0 : x / y;   // div-by-zero → 0 (matches i64_div)
        default: throw std::invalid_argument("fi_apply_i64: unsupported op");
    }
}

template<typename W, DrakenType WTAG>
static inline VecResult fi_int_arith_store(int op, const DrakenVector& a, const DrakenVector& b) {
    const uint32_t n = a.length;
    W* dst = fi_alloc<W>(n);
    for (uint32_t i = 0; i < n; ++i)
        dst[i] = static_cast<W>(fi_apply_i64(op, fi_read_i64(a, i), fi_read_i64(b, i)));
    return fi_make_dense<W, WTAG>(dst, fi_combine_validity(a.validity, b.validity, n), n);
}

// D.6 result tag = next-power of the wider operand width.
static inline DrakenType fi_arith_result_tag(DrakenType ta, DrakenType tb) {
    auto rank = [](DrakenType t) -> int {
        switch (t) { case DRAKEN_INT8: return 0; case DRAKEN_INT16: return 1;
                     case DRAKEN_INT32: return 2; case DRAKEN_INT64: return 3;
                     default: return -1; }
    };
    const int r = rank(ta) > rank(tb) ? rank(ta) : rank(tb);
    switch (r) {
        case 0: return DRAKEN_INT16;  // int8  → int16
        case 1: return DRAKEN_INT32;  // int16 → int32
        case 2: return DRAKEN_INT64;  // int32 → int64
        case 3: return DRAKEN_INT64;  // int64 → int64
        default: throw std::invalid_argument("fi_arith_result_tag: non-integer operand");
    }
}

// Integer arithmetic entry (PLUS/MINUS/MULTIPLY/MODULO/INT_DIVIDE), D.6 result width.
static inline VecResult fi_int_arith(int op, const DrakenVector& a, const DrakenVector& b) {
    if (a.length != b.length)
        throw std::invalid_argument("fi_int_arith: length mismatch");
    switch (fi_arith_result_tag(a.type, b.type)) {
        case DRAKEN_INT16: return fi_int_arith_store<int16_t, DRAKEN_INT16>(op, a, b);
        case DRAKEN_INT32: return fi_int_arith_store<int32_t, DRAKEN_INT32>(op, a, b);
        case DRAKEN_INT64: return fi_int_arith_store<int64_t, DRAKEN_INT64>(op, a, b);
        default: throw std::invalid_argument("fi_int_arith: bad result tag");
    }
}

// ===========================================================================
// HASH — sign-extend T → int64_t → uint64_t, then simd_hash_i64
// ===========================================================================

template<typename T, DrakenType TAG>
static inline void fixed_int_hash(
    const DrakenVector& v, uint64_t* out, uint32_t n)
{
    if (n == 0) return;
    const T*       data     = static_cast<const T*>(v.data);
    const uint8_t* validity = v.validity;
    uint64_t scratch[1024];

    uint32_t i = 0;
    while (i < n) {
        const uint32_t block = (n - i < 1024u) ? (n - i) : 1024u;
        if (validity != nullptr) {
            for (uint32_t j = 0; j < block; ++j) {
                const uint64_t is_valid =
                    (validity[(i + j) >> 3] >> ((i + j) & 7)) & 1u;
                const uint64_t raw = static_cast<uint64_t>(
                    static_cast<int64_t>(data[v.selection[i + j]]));
                scratch[j] = (raw * is_valid) | (NULL_HASH * (1u - is_valid));
            }
        } else {
            for (uint32_t j = 0; j < block; ++j)
                scratch[j] = static_cast<uint64_t>(
                    static_cast<int64_t>(data[v.selection[i + j]]));
        }
        simd_hash_i64(scratch, out + i, block);
        i += block;
    }
}

// ===========================================================================
// HASH — DRAKEN_BOOL: bit-packed (1 bit/value), accessed via the uniform
// pattern data[selection[i]] at bit granularity:
//   bit = (data[code >> 3] >> (code & 7)) & 1   where code = selection[i]
// Seed is the bit value (0 or 1) widened to uint64_t; null rows bake NULL_HASH.
// Matches fixed_int_hash's branchless null-select so true/false/null collide
// consistently across the dense and compressed (data_length distinct) paths.
// ===========================================================================
static inline void hash_bool(const DrakenVector& v, uint64_t* out, uint32_t n) {
    if (n == 0) return;
    const uint8_t* data     = static_cast<const uint8_t*>(v.data);
    const uint8_t* validity = v.validity;
    uint64_t scratch[1024];

    uint32_t i = 0;
    while (i < n) {
        const uint32_t block = (n - i < 1024u) ? (n - i) : 1024u;
        if (validity != nullptr) {
            for (uint32_t j = 0; j < block; ++j) {
                const uint64_t is_valid =
                    (validity[(i + j) >> 3] >> ((i + j) & 7)) & 1u;
                const uint32_t code = v.selection[i + j];
                const uint64_t raw  = (data[code >> 3] >> (code & 7u)) & 1u;
                scratch[j] = (raw * is_valid) | (NULL_HASH * (1u - is_valid));
            }
        } else {
            for (uint32_t j = 0; j < block; ++j) {
                const uint32_t code = v.selection[i + j];
                scratch[j] = (data[code >> 3] >> (code & 7u)) & 1u;
            }
        }
        simd_hash_i64(scratch, out + i, block);
        i += block;
    }
}

// ===========================================================================
// COMPARE SCALAR — widen T to int64_t, compare against int64_t scalar
// ===========================================================================

template<typename T, typename Op>
static inline void fi_cmp_scalar_kernel(
    const T*        data,
    const uint32_t* selection,
    int64_t         scalar,
    const uint8_t*  src_null,
    uint8_t*        dst,
    uint32_t        n)
{
    const uint32_t whole_bytes = n >> 3;
    if (src_null == nullptr) {
        for (uint32_t b = 0; b < whole_bytes; ++b) {
            const uint32_t base = b << 3;
            dst[b] = static_cast<uint8_t>(
                (static_cast<unsigned>(Op::apply(static_cast<int64_t>(data[selection[base+0]]), scalar)) << 0) |
                (static_cast<unsigned>(Op::apply(static_cast<int64_t>(data[selection[base+1]]), scalar)) << 1) |
                (static_cast<unsigned>(Op::apply(static_cast<int64_t>(data[selection[base+2]]), scalar)) << 2) |
                (static_cast<unsigned>(Op::apply(static_cast<int64_t>(data[selection[base+3]]), scalar)) << 3) |
                (static_cast<unsigned>(Op::apply(static_cast<int64_t>(data[selection[base+4]]), scalar)) << 4) |
                (static_cast<unsigned>(Op::apply(static_cast<int64_t>(data[selection[base+5]]), scalar)) << 5) |
                (static_cast<unsigned>(Op::apply(static_cast<int64_t>(data[selection[base+6]]), scalar)) << 6) |
                (static_cast<unsigned>(Op::apply(static_cast<int64_t>(data[selection[base+7]]), scalar)) << 7));
        }
        for (uint32_t i = whole_bytes << 3; i < n; ++i)
            if (Op::apply(static_cast<int64_t>(data[selection[i]]), scalar))
                dst[i >> 3] |= static_cast<uint8_t>(1u << (i & 7));
    } else {
        for (uint32_t b = 0; b < whole_bytes; ++b) {
            const uint32_t base = b << 3;
            const uint8_t m = static_cast<uint8_t>(
                (static_cast<unsigned>(Op::apply(static_cast<int64_t>(data[selection[base+0]]), scalar)) << 0) |
                (static_cast<unsigned>(Op::apply(static_cast<int64_t>(data[selection[base+1]]), scalar)) << 1) |
                (static_cast<unsigned>(Op::apply(static_cast<int64_t>(data[selection[base+2]]), scalar)) << 2) |
                (static_cast<unsigned>(Op::apply(static_cast<int64_t>(data[selection[base+3]]), scalar)) << 3) |
                (static_cast<unsigned>(Op::apply(static_cast<int64_t>(data[selection[base+4]]), scalar)) << 4) |
                (static_cast<unsigned>(Op::apply(static_cast<int64_t>(data[selection[base+5]]), scalar)) << 5) |
                (static_cast<unsigned>(Op::apply(static_cast<int64_t>(data[selection[base+6]]), scalar)) << 6) |
                (static_cast<unsigned>(Op::apply(static_cast<int64_t>(data[selection[base+7]]), scalar)) << 7));
            dst[b] = static_cast<uint8_t>(m & src_null[b]);
        }
        for (uint32_t i = whole_bytes << 3; i < n; ++i)
            if ((src_null[i >> 3] >> (i & 7)) & 1u)
                if (Op::apply(static_cast<int64_t>(data[selection[i]]), scalar))
                    dst[i >> 3] |= static_cast<uint8_t>(1u << (i & 7));
    }
}

template<typename T, typename Op>
static inline VecResult fi_compare_scalar_impl(const DrakenVector& v, int64_t scalar) {
    const uint32_t n        = v.length;
    const T*       data     = static_cast<const T*>(v.data);
    const uint8_t* src_null = v.validity;

    if (draken_is_constant(&v))
        return cmp_constant_bool_result(
            Op::apply(static_cast<int64_t>(data[0]), scalar), src_null, n);

    if (draken_is_dict(&v)) {
        const uint32_t dl = v.data_length;
        uint8_t* db = static_cast<uint8_t*>(draken_malloc(dl));
        if (!db) throw std::bad_alloc();
        for (uint32_t k = 0; k < dl; ++k)
            db[k] = Op::apply(static_cast<int64_t>(data[k]), scalar) ? 1u : 0u;
        VecResult r;
        try { r = cmp_dict_bool_result(db, v); }
        catch (...) { draken_free(db); throw; }
        draken_free(db);
        return r;
    }

    uint8_t* dst      = cmp_alloc_bool_buf(n);
    uint8_t* out_null = nullptr;
    if (src_null != nullptr) {
        try { out_null = cmp_copy_validity(src_null, n); }
        catch (...) { draken_free(dst); throw; }
    }
    fi_cmp_scalar_kernel<T, Op>(data, v.selection, scalar, src_null, dst, n);

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

template<typename T>
static inline VecResult fixed_int_compare_scalar(
    const DrakenVector& v, int64_t scalar, int op)
{
    switch (op) {
        case 0: return fi_compare_scalar_impl<T, CmpEq>(v, scalar);
        case 1: return fi_compare_scalar_impl<T, CmpNe>(v, scalar);
        case 2: return fi_compare_scalar_impl<T, CmpGt>(v, scalar);
        case 3: return fi_compare_scalar_impl<T, CmpGe>(v, scalar);
        case 4: return fi_compare_scalar_impl<T, CmpLt>(v, scalar);
        default: return fi_compare_scalar_impl<T, CmpLe>(v, scalar);
    }
}

// ===========================================================================
// COMPARE VECTOR — both operands same type T, widened to int64_t
// ===========================================================================

template<typename T, typename Op>
static inline void fi_cmp_vector_kernel(
    const T*        a_data, const uint32_t* a_sel,
    const T*        b_data, const uint32_t* b_sel,
    const uint8_t*  comb_null,
    uint8_t*        dst,
    uint32_t        n)
{
    const uint32_t whole_bytes = n >> 3;
    if (comb_null == nullptr) {
        for (uint32_t b = 0; b < whole_bytes; ++b) {
            const uint32_t base = b << 3;
            dst[b] = static_cast<uint8_t>(
                (static_cast<unsigned>(Op::apply(static_cast<int64_t>(a_data[a_sel[base+0]]), static_cast<int64_t>(b_data[b_sel[base+0]]))) << 0) |
                (static_cast<unsigned>(Op::apply(static_cast<int64_t>(a_data[a_sel[base+1]]), static_cast<int64_t>(b_data[b_sel[base+1]]))) << 1) |
                (static_cast<unsigned>(Op::apply(static_cast<int64_t>(a_data[a_sel[base+2]]), static_cast<int64_t>(b_data[b_sel[base+2]]))) << 2) |
                (static_cast<unsigned>(Op::apply(static_cast<int64_t>(a_data[a_sel[base+3]]), static_cast<int64_t>(b_data[b_sel[base+3]]))) << 3) |
                (static_cast<unsigned>(Op::apply(static_cast<int64_t>(a_data[a_sel[base+4]]), static_cast<int64_t>(b_data[b_sel[base+4]]))) << 4) |
                (static_cast<unsigned>(Op::apply(static_cast<int64_t>(a_data[a_sel[base+5]]), static_cast<int64_t>(b_data[b_sel[base+5]]))) << 5) |
                (static_cast<unsigned>(Op::apply(static_cast<int64_t>(a_data[a_sel[base+6]]), static_cast<int64_t>(b_data[b_sel[base+6]]))) << 6) |
                (static_cast<unsigned>(Op::apply(static_cast<int64_t>(a_data[a_sel[base+7]]), static_cast<int64_t>(b_data[b_sel[base+7]]))) << 7));
        }
        for (uint32_t i = whole_bytes << 3; i < n; ++i)
            if (Op::apply(static_cast<int64_t>(a_data[a_sel[i]]),
                          static_cast<int64_t>(b_data[b_sel[i]])))
                dst[i >> 3] |= static_cast<uint8_t>(1u << (i & 7));
    } else {
        for (uint32_t b = 0; b < whole_bytes; ++b) {
            const uint32_t base = b << 3;
            const uint8_t m = static_cast<uint8_t>(
                (static_cast<unsigned>(Op::apply(static_cast<int64_t>(a_data[a_sel[base+0]]), static_cast<int64_t>(b_data[b_sel[base+0]]))) << 0) |
                (static_cast<unsigned>(Op::apply(static_cast<int64_t>(a_data[a_sel[base+1]]), static_cast<int64_t>(b_data[b_sel[base+1]]))) << 1) |
                (static_cast<unsigned>(Op::apply(static_cast<int64_t>(a_data[a_sel[base+2]]), static_cast<int64_t>(b_data[b_sel[base+2]]))) << 2) |
                (static_cast<unsigned>(Op::apply(static_cast<int64_t>(a_data[a_sel[base+3]]), static_cast<int64_t>(b_data[b_sel[base+3]]))) << 3) |
                (static_cast<unsigned>(Op::apply(static_cast<int64_t>(a_data[a_sel[base+4]]), static_cast<int64_t>(b_data[b_sel[base+4]]))) << 4) |
                (static_cast<unsigned>(Op::apply(static_cast<int64_t>(a_data[a_sel[base+5]]), static_cast<int64_t>(b_data[b_sel[base+5]]))) << 5) |
                (static_cast<unsigned>(Op::apply(static_cast<int64_t>(a_data[a_sel[base+6]]), static_cast<int64_t>(b_data[b_sel[base+6]]))) << 6) |
                (static_cast<unsigned>(Op::apply(static_cast<int64_t>(a_data[a_sel[base+7]]), static_cast<int64_t>(b_data[b_sel[base+7]]))) << 7));
            dst[b] = static_cast<uint8_t>(m & comb_null[b]);
        }
        for (uint32_t i = whole_bytes << 3; i < n; ++i)
            if ((comb_null[i >> 3] >> (i & 7)) & 1u)
                if (Op::apply(static_cast<int64_t>(a_data[a_sel[i]]),
                              static_cast<int64_t>(b_data[b_sel[i]])))
                    dst[i >> 3] |= static_cast<uint8_t>(1u << (i & 7));
    }
}

template<typename T, typename Op>
static inline VecResult fi_compare_vector_impl(
    const DrakenVector& a, const DrakenVector& b)
{
    const uint32_t n = a.length;
    if (b.length != n)
        throw std::invalid_argument("fi_compare_vector: operand lengths must match");
    const T* a_data = static_cast<const T*>(a.data);
    const T* b_data = static_cast<const T*>(b.data);

    if (draken_is_constant(&a) && draken_is_constant(&b)) {
        uint8_t* comb = cmp_and_validity(a.validity, b.validity, n);
        VecResult r;
        try {
            r = cmp_constant_bool_result(
                Op::apply(static_cast<int64_t>(a_data[0]),
                          static_cast<int64_t>(b_data[0])), comb, n);
        } catch (...) { if (comb) draken_free(comb); throw; }
        if (comb) draken_free(comb);
        return r;
    }

    uint8_t* out_null = cmp_and_validity(a.validity, b.validity, n);
    uint8_t* dst = nullptr;
    try { dst = cmp_alloc_bool_buf(n); }
    catch (...) { if (out_null) draken_free(out_null); throw; }

    fi_cmp_vector_kernel<T, Op>(
        a_data, a.selection, b_data, b.selection, out_null, dst, n);

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

template<typename T>
static inline VecResult fixed_int_compare_vector(
    const DrakenVector& a, const DrakenVector& b, int op)
{
    switch (op) {
        case 0: return fi_compare_vector_impl<T, CmpEq>(a, b);
        case 1: return fi_compare_vector_impl<T, CmpNe>(a, b);
        case 2: return fi_compare_vector_impl<T, CmpGt>(a, b);
        case 3: return fi_compare_vector_impl<T, CmpGe>(a, b);
        case 4: return fi_compare_vector_impl<T, CmpLt>(a, b);
        default: return fi_compare_vector_impl<T, CmpLe>(a, b);
    }
}

// ===========================================================================
// REDUCTIONS — accumulate into int64_t
// ===========================================================================

template<typename T>
static inline uint32_t fixed_int_sum(
    const DrakenVector& v, int64_t* out_value) noexcept
{
    const uint32_t n        = v.length;
    const T*       data     = static_cast<const T*>(v.data);
    const uint8_t* validity = v.validity;
    int64_t total = 0;
    uint32_t count = 0;

    if (validity == nullptr) {
        count = n;
        if (v.flags & DRAKEN_SEL_IDENTITY) {
            for (uint32_t i = 0; i < n; ++i) total += static_cast<int64_t>(data[i]);
        } else {
            for (uint32_t i = 0; i < n; ++i) total += static_cast<int64_t>(data[v.selection[i]]);
        }
    } else {
        if (v.flags & DRAKEN_SEL_IDENTITY) {
            for (uint32_t i = 0; i < n; ++i) {
                const uint32_t valid = (validity[i >> 3] >> (i & 7)) & 1u;
                total += static_cast<int64_t>(data[i]) * static_cast<int64_t>(valid);
                count += valid;
            }
        } else {
            for (uint32_t i = 0; i < n; ++i) {
                const uint32_t valid = (validity[i >> 3] >> (i & 7)) & 1u;
                total += static_cast<int64_t>(data[v.selection[i]]) * static_cast<int64_t>(valid);
                count += valid;
            }
        }
    }
    *out_value = total;
    return count;
}

template<typename T>
static inline uint32_t fixed_int_min(
    const DrakenVector& v, int64_t* out_min) noexcept
{
    const uint32_t n        = v.length;
    const T*       data     = static_cast<const T*>(v.data);
    const uint8_t* validity = v.validity;
    int64_t m = static_cast<int64_t>(std::numeric_limits<T>::max());
    uint32_t count = 0;

    if (validity == nullptr) {
        count = n;
        if (v.flags & DRAKEN_SEL_IDENTITY) {
            for (uint32_t i = 0; i < n; ++i) {
                const int64_t val = static_cast<int64_t>(data[i]);
                m = val < m ? val : m;
            }
        } else {
            for (uint32_t i = 0; i < n; ++i) {
                const int64_t val = static_cast<int64_t>(data[v.selection[i]]);
                m = val < m ? val : m;
            }
        }
    } else {
        if (v.flags & DRAKEN_SEL_IDENTITY) {
            for (uint32_t i = 0; i < n; ++i) {
                if ((validity[i >> 3] >> (i & 7)) & 1u) {
                    const int64_t val = static_cast<int64_t>(data[i]);
                    m = val < m ? val : m;
                    ++count;
                }
            }
        } else {
            for (uint32_t i = 0; i < n; ++i) {
                if ((validity[i >> 3] >> (i & 7)) & 1u) {
                    const int64_t val = static_cast<int64_t>(data[v.selection[i]]);
                    m = val < m ? val : m;
                    ++count;
                }
            }
        }
    }
    *out_min = m;
    return count;
}

template<typename T>
static inline uint32_t fixed_int_max(
    const DrakenVector& v, int64_t* out_max) noexcept
{
    const uint32_t n        = v.length;
    const T*       data     = static_cast<const T*>(v.data);
    const uint8_t* validity = v.validity;
    int64_t m = static_cast<int64_t>(std::numeric_limits<T>::min());
    uint32_t count = 0;

    if (validity == nullptr) {
        count = n;
        if (v.flags & DRAKEN_SEL_IDENTITY) {
            for (uint32_t i = 0; i < n; ++i) {
                const int64_t val = static_cast<int64_t>(data[i]);
                m = val > m ? val : m;
            }
        } else {
            for (uint32_t i = 0; i < n; ++i) {
                const int64_t val = static_cast<int64_t>(data[v.selection[i]]);
                m = val > m ? val : m;
            }
        }
    } else {
        if (v.flags & DRAKEN_SEL_IDENTITY) {
            for (uint32_t i = 0; i < n; ++i) {
                if ((validity[i >> 3] >> (i & 7)) & 1u) {
                    const int64_t val = static_cast<int64_t>(data[i]);
                    m = val > m ? val : m;
                    ++count;
                }
            }
        } else {
            for (uint32_t i = 0; i < n; ++i) {
                if ((validity[i >> 3] >> (i & 7)) & 1u) {
                    const int64_t val = static_cast<int64_t>(data[v.selection[i]]);
                    m = val > m ? val : m;
                    ++count;
                }
            }
        }
    }
    *out_max = m;
    return count;
}

// ===========================================================================
// ARITHMETIC — result type is NextWider<T>::type
// div/mod by zero → 0. Null propagation: binary AND; unary copies.
// ===========================================================================

template<typename T>
static inline VecResult fixed_int_add(const DrakenVector& a, const DrakenVector& b) {
    using W = typename NextWider<T>::type;
    constexpr DrakenType WTAG = NextWider<T>::tag;
    if (a.length != b.length) throw std::invalid_argument("fixed_int_add: length mismatch");
    const uint32_t n = a.length;
    const T* ad = static_cast<const T*>(a.data);
    const T* bd = static_cast<const T*>(b.data);
    W* dst = fi_alloc<W>(n);
    for (uint32_t i = 0; i < n; ++i)
        dst[i] = static_cast<W>(ad[a.selection[i]]) + static_cast<W>(bd[b.selection[i]]);
    return fi_make_dense<W, WTAG>(dst, fi_combine_validity(a.validity, b.validity, n), n);
}

template<typename T>
static inline VecResult fixed_int_add_scalar(const DrakenVector& a, int64_t scalar) {
    using W = typename NextWider<T>::type;
    constexpr DrakenType WTAG = NextWider<T>::tag;
    const uint32_t n = a.length;
    const T* ad = static_cast<const T*>(a.data);
    const W  sv = static_cast<W>(scalar);
    W* dst = fi_alloc<W>(n);
    for (uint32_t i = 0; i < n; ++i) dst[i] = static_cast<W>(ad[a.selection[i]]) + sv;
    return fi_make_dense<W, WTAG>(dst, fi_copy_validity(a.validity, n), n);
}

template<typename T>
static inline VecResult fixed_int_sub(const DrakenVector& a, const DrakenVector& b) {
    using W = typename NextWider<T>::type;
    constexpr DrakenType WTAG = NextWider<T>::tag;
    if (a.length != b.length) throw std::invalid_argument("fixed_int_sub: length mismatch");
    const uint32_t n = a.length;
    const T* ad = static_cast<const T*>(a.data);
    const T* bd = static_cast<const T*>(b.data);
    W* dst = fi_alloc<W>(n);
    for (uint32_t i = 0; i < n; ++i)
        dst[i] = static_cast<W>(ad[a.selection[i]]) - static_cast<W>(bd[b.selection[i]]);
    return fi_make_dense<W, WTAG>(dst, fi_combine_validity(a.validity, b.validity, n), n);
}

template<typename T>
static inline VecResult fixed_int_sub_scalar(const DrakenVector& a, int64_t scalar) {
    using W = typename NextWider<T>::type;
    constexpr DrakenType WTAG = NextWider<T>::tag;
    const uint32_t n = a.length;
    const T* ad = static_cast<const T*>(a.data);
    const W  sv = static_cast<W>(scalar);
    W* dst = fi_alloc<W>(n);
    for (uint32_t i = 0; i < n; ++i) dst[i] = static_cast<W>(ad[a.selection[i]]) - sv;
    return fi_make_dense<W, WTAG>(dst, fi_copy_validity(a.validity, n), n);
}

template<typename T>
static inline VecResult fixed_int_mul(const DrakenVector& a, const DrakenVector& b) {
    using W = typename NextWider<T>::type;
    constexpr DrakenType WTAG = NextWider<T>::tag;
    if (a.length != b.length) throw std::invalid_argument("fixed_int_mul: length mismatch");
    const uint32_t n = a.length;
    const T* ad = static_cast<const T*>(a.data);
    const T* bd = static_cast<const T*>(b.data);
    W* dst = fi_alloc<W>(n);
    for (uint32_t i = 0; i < n; ++i)
        dst[i] = static_cast<W>(ad[a.selection[i]]) * static_cast<W>(bd[b.selection[i]]);
    return fi_make_dense<W, WTAG>(dst, fi_combine_validity(a.validity, b.validity, n), n);
}

template<typename T>
static inline VecResult fixed_int_mul_scalar(const DrakenVector& a, int64_t scalar) {
    using W = typename NextWider<T>::type;
    constexpr DrakenType WTAG = NextWider<T>::tag;
    const uint32_t n = a.length;
    const T* ad = static_cast<const T*>(a.data);
    const W  sv = static_cast<W>(scalar);
    W* dst = fi_alloc<W>(n);
    for (uint32_t i = 0; i < n; ++i) dst[i] = static_cast<W>(ad[a.selection[i]]) * sv;
    return fi_make_dense<W, WTAG>(dst, fi_copy_validity(a.validity, n), n);
}

template<typename T>
static inline VecResult fixed_int_div(const DrakenVector& a, const DrakenVector& b) {
    using W = typename NextWider<T>::type;
    constexpr DrakenType WTAG = NextWider<T>::tag;
    if (a.length != b.length) throw std::invalid_argument("fixed_int_div: length mismatch");
    const uint32_t n = a.length;
    const T* ad = static_cast<const T*>(a.data);
    const T* bd = static_cast<const T*>(b.data);
    W* dst = fi_alloc<W>(n);
    for (uint32_t i = 0; i < n; ++i) {
        const W bv = static_cast<W>(bd[b.selection[i]]);
        dst[i] = (bv == W(0)) ? W(0) : static_cast<W>(ad[a.selection[i]]) / bv;
    }
    return fi_make_dense<W, WTAG>(dst, fi_combine_validity(a.validity, b.validity, n), n);
}

template<typename T>
static inline VecResult fixed_int_div_scalar(const DrakenVector& a, int64_t scalar) {
    using W = typename NextWider<T>::type;
    constexpr DrakenType WTAG = NextWider<T>::tag;
    const uint32_t n = a.length;
    const T* ad = static_cast<const T*>(a.data);
    const W  sv = static_cast<W>(scalar);
    W* dst = fi_alloc<W>(n);
    if (sv == W(0))
        for (uint32_t i = 0; i < n; ++i) dst[i] = W(0);
    else
        for (uint32_t i = 0; i < n; ++i) dst[i] = static_cast<W>(ad[a.selection[i]]) / sv;
    return fi_make_dense<W, WTAG>(dst, fi_copy_validity(a.validity, n), n);
}

template<typename T>
static inline VecResult fixed_int_mod(const DrakenVector& a, const DrakenVector& b) {
    using W = typename NextWider<T>::type;
    constexpr DrakenType WTAG = NextWider<T>::tag;
    if (a.length != b.length) throw std::invalid_argument("fixed_int_mod: length mismatch");
    const uint32_t n = a.length;
    const T* ad = static_cast<const T*>(a.data);
    const T* bd = static_cast<const T*>(b.data);
    W* dst = fi_alloc<W>(n);
    for (uint32_t i = 0; i < n; ++i) {
        const W bv = static_cast<W>(bd[b.selection[i]]);
        dst[i] = (bv == W(0)) ? W(0) : static_cast<W>(ad[a.selection[i]]) % bv;
    }
    return fi_make_dense<W, WTAG>(dst, fi_combine_validity(a.validity, b.validity, n), n);
}

template<typename T>
static inline VecResult fixed_int_mod_scalar(const DrakenVector& a, int64_t scalar) {
    using W = typename NextWider<T>::type;
    constexpr DrakenType WTAG = NextWider<T>::tag;
    const uint32_t n = a.length;
    const T* ad = static_cast<const T*>(a.data);
    const W  sv = static_cast<W>(scalar);
    W* dst = fi_alloc<W>(n);
    if (sv == W(0))
        for (uint32_t i = 0; i < n; ++i) dst[i] = W(0);
    else
        for (uint32_t i = 0; i < n; ++i) dst[i] = static_cast<W>(ad[a.selection[i]]) % sv;
    return fi_make_dense<W, WTAG>(dst, fi_copy_validity(a.validity, n), n);
}

// Unary negation: widened so -(INT8_MIN) = 128 fits in int16, etc.
template<typename T>
static inline VecResult fixed_int_neg(const DrakenVector& a) {
    using W = typename NextWider<T>::type;
    constexpr DrakenType WTAG = NextWider<T>::tag;
    const uint32_t n = a.length;
    const T* ad = static_cast<const T*>(a.data);
    W* dst = fi_alloc<W>(n);
    for (uint32_t i = 0; i < n; ++i)
        dst[i] = -static_cast<W>(ad[a.selection[i]]);
    return fi_make_dense<W, WTAG>(dst, fi_copy_validity(a.validity, n), n);
}

// ===========================================================================
// GATHER — take / materialize / compress (result stays type T)
// ===========================================================================

template<typename T, DrakenType TAG>
static inline VecResult fixed_int_take(
    const DrakenVector& v, const int32_t* indices, uint32_t n_indices)
{
    const uint32_t n        = n_indices;
    const T*       data     = static_cast<const T*>(v.data);
    const uint8_t* src_null = v.validity;

    T* dst = fi_alloc<T>(n);

    uint8_t* out_null = nullptr;
    if (src_null != nullptr && n > 0) {
        const uint32_t nb = (n + 7u) >> 3;
        out_null = static_cast<uint8_t*>(draken_malloc(nb));
        if (!out_null) { draken_free(dst); throw std::bad_alloc(); }
        memset(out_null, 0, nb);
    }

    for (uint32_t i = 0; i < n; ++i) {
        const uint32_t src_idx = static_cast<uint32_t>(indices[i]);
        if (!fi_row_valid(src_null, src_idx)) {
            dst[i] = T(0);
        } else {
            dst[i] = data[v.selection[src_idx]];
            if (out_null != nullptr) fi_set_valid_bit(out_null, i);
        }
    }

    out_null = fi_normalize_validity(out_null, n);

    VecResult r;
    r.data           = dst;
    r.validity       = out_null;
    r.selection      = draken_identity_sel(n);
    r.owns_selection = false;
    r.data_length    = n;
    r.length         = n;
    r.type           = TAG;
    r.flags          = static_cast<uint8_t>(DRAKEN_SEL_IDENTITY | DRAKEN_SEL_PERMUTATION);
    return r;
}

template<typename T, DrakenType TAG>
static inline VecResult fixed_int_materialize(const DrakenVector& v) {
    const uint32_t n        = v.length;
    const T*       data     = static_cast<const T*>(v.data);
    const uint8_t* src_null = v.validity;

    T* dst = fi_alloc<T>(n);

    for (uint32_t i = 0; i < n; ++i) {
        if (src_null != nullptr && !((src_null[i >> 3] >> (i & 7)) & 1u))
            dst[i] = T(0);
        else
            dst[i] = data[v.selection[i]];
    }

    VecResult r;
    r.data           = dst;
    r.validity       = fi_copy_validity(src_null, n);
    r.selection      = draken_identity_sel(n);
    r.owns_selection = false;
    r.data_length    = n;
    r.length         = n;
    r.type           = TAG;
    r.flags          = static_cast<uint8_t>(DRAKEN_SEL_IDENTITY | DRAKEN_SEL_PERMUTATION);
    return r;
}

template<typename T, DrakenType TAG>
static inline VecResult fixed_int_compress(const DrakenVector& v) {
    const uint32_t n        = v.length;
    const T*       data     = static_cast<const T*>(v.data);
    const uint8_t* src_null = v.validity;

    if (n == 0) {
        T* d = fi_alloc<T>(1); d[0] = T(0);
        VecResult r;
        r.data = d; r.validity = nullptr;
        r.selection = draken_identity_sel(0); r.owns_selection = false;
        r.data_length = 0; r.length = 0; r.type = TAG; r.flags = 0;
        return r;
    }

    std::unordered_map<T, uint32_t> value_to_code;
    value_to_code.reserve(n < 256u ? n : 256u);
    std::vector<T> dict_values;

    for (uint32_t i = 0; i < n; ++i) {
        if (!fi_row_valid(src_null, i)) continue;
        T val = data[v.selection[i]];
        if (value_to_code.find(val) == value_to_code.end()) {
            uint32_t code = static_cast<uint32_t>(dict_values.size());
            value_to_code[val] = code;
            dict_values.push_back(val);
        }
    }

    const uint32_t dict_size = static_cast<uint32_t>(dict_values.size());

    if (dict_size == 0) {
        T* d = fi_alloc<T>(1); d[0] = T(0);
        uint8_t* out_null = nullptr;
        if (src_null != nullptr) {
            const uint32_t nb = (n + 7u) >> 3;
            out_null = static_cast<uint8_t*>(draken_malloc(nb));
            if (!out_null) { draken_free(d); throw std::bad_alloc(); }
            memcpy(out_null, src_null, nb);
        }
        VecResult r;
        r.data = d; r.validity = out_null;
        r.selection = draken_zero_sel(n); r.owns_selection = false;
        r.data_length = 1; r.length = n; r.type = TAG; r.flags = 0;
        return r;
    }

    T* dict_buf = static_cast<T*>(draken_malloc(dict_size * sizeof(T)));
    if (!dict_buf) throw std::bad_alloc();
    for (uint32_t k = 0; k < dict_size; ++k) dict_buf[k] = dict_values[k];

    uint32_t* codes = static_cast<uint32_t*>(draken_malloc(n * sizeof(uint32_t)));
    if (!codes) { draken_free(dict_buf); throw std::bad_alloc(); }
    for (uint32_t i = 0; i < n; ++i)
        codes[i] = fi_row_valid(src_null, i) ? value_to_code.at(data[v.selection[i]]) : 0u;

    uint8_t* out_null = nullptr;
    if (src_null != nullptr) {
        const uint32_t nb = (n + 7u) >> 3;
        out_null = static_cast<uint8_t*>(draken_malloc(nb));
        if (!out_null) { draken_free(dict_buf); draken_free(codes); throw std::bad_alloc(); }
        memcpy(out_null, src_null, nb);
    }

    VecResult r;
    r.data = dict_buf; r.validity = out_null;
    r.selection = codes; r.owns_selection = true;
    r.data_length = dict_size; r.length = n; r.type = TAG; r.flags = 0;
    return r;
}

// ===========================================================================
// BETWEEN — widen T to int64_t, compare against int64_t lo/hi
// ===========================================================================

template<typename T, bool lo_incl, bool hi_incl>
static inline VecResult fi_between_impl(
    const DrakenVector& v, int64_t lo, int64_t hi)
{
    const uint32_t n        = v.length;
    const T*       data     = static_cast<const T*>(v.data);
    const uint8_t* src_null = v.validity;

    if (draken_is_constant(&v))
        return cmp_constant_bool_result(
            BetweenOp<lo_incl, hi_incl>::apply(static_cast<int64_t>(data[0]), lo, hi),
            src_null, n);

    if (draken_is_dict(&v)) {
        const uint32_t dl = v.data_length;
        uint8_t* db = static_cast<uint8_t*>(draken_malloc(dl));
        if (!db) throw std::bad_alloc();
        for (uint32_t k = 0; k < dl; ++k)
            db[k] = BetweenOp<lo_incl, hi_incl>::apply(
                        static_cast<int64_t>(data[k]), lo, hi) ? 1u : 0u;
        VecResult r;
        try { r = cmp_dict_bool_result(db, v); }
        catch (...) { draken_free(db); throw; }
        draken_free(db);
        return r;
    }

    uint8_t* dst      = cmp_alloc_bool_buf(n);
    uint8_t* out_null = nullptr;
    if (src_null != nullptr) {
        try { out_null = cmp_copy_validity(src_null, n); }
        catch (...) { draken_free(dst); throw; }
    }

    using Op = BetweenOp<lo_incl, hi_incl>;
    const uint32_t whole_bytes = n >> 3;

    if (src_null == nullptr) {
        for (uint32_t b = 0; b < whole_bytes; ++b) {
            const uint32_t base = b << 3;
            dst[b] = static_cast<uint8_t>(
                (static_cast<unsigned>(Op::apply(static_cast<int64_t>(data[v.selection[base+0]]), lo, hi)) << 0) |
                (static_cast<unsigned>(Op::apply(static_cast<int64_t>(data[v.selection[base+1]]), lo, hi)) << 1) |
                (static_cast<unsigned>(Op::apply(static_cast<int64_t>(data[v.selection[base+2]]), lo, hi)) << 2) |
                (static_cast<unsigned>(Op::apply(static_cast<int64_t>(data[v.selection[base+3]]), lo, hi)) << 3) |
                (static_cast<unsigned>(Op::apply(static_cast<int64_t>(data[v.selection[base+4]]), lo, hi)) << 4) |
                (static_cast<unsigned>(Op::apply(static_cast<int64_t>(data[v.selection[base+5]]), lo, hi)) << 5) |
                (static_cast<unsigned>(Op::apply(static_cast<int64_t>(data[v.selection[base+6]]), lo, hi)) << 6) |
                (static_cast<unsigned>(Op::apply(static_cast<int64_t>(data[v.selection[base+7]]), lo, hi)) << 7));
        }
        for (uint32_t i = whole_bytes << 3; i < n; ++i)
            if (Op::apply(static_cast<int64_t>(data[v.selection[i]]), lo, hi))
                dst[i >> 3] |= static_cast<uint8_t>(1u << (i & 7));
    } else {
        for (uint32_t b = 0; b < whole_bytes; ++b) {
            const uint32_t base = b << 3;
            const uint8_t m = static_cast<uint8_t>(
                (static_cast<unsigned>(Op::apply(static_cast<int64_t>(data[v.selection[base+0]]), lo, hi)) << 0) |
                (static_cast<unsigned>(Op::apply(static_cast<int64_t>(data[v.selection[base+1]]), lo, hi)) << 1) |
                (static_cast<unsigned>(Op::apply(static_cast<int64_t>(data[v.selection[base+2]]), lo, hi)) << 2) |
                (static_cast<unsigned>(Op::apply(static_cast<int64_t>(data[v.selection[base+3]]), lo, hi)) << 3) |
                (static_cast<unsigned>(Op::apply(static_cast<int64_t>(data[v.selection[base+4]]), lo, hi)) << 4) |
                (static_cast<unsigned>(Op::apply(static_cast<int64_t>(data[v.selection[base+5]]), lo, hi)) << 5) |
                (static_cast<unsigned>(Op::apply(static_cast<int64_t>(data[v.selection[base+6]]), lo, hi)) << 6) |
                (static_cast<unsigned>(Op::apply(static_cast<int64_t>(data[v.selection[base+7]]), lo, hi)) << 7));
            dst[b] = static_cast<uint8_t>(m & src_null[b]);
        }
        for (uint32_t i = whole_bytes << 3; i < n; ++i)
            if ((src_null[i >> 3] >> (i & 7)) & 1u)
                if (Op::apply(static_cast<int64_t>(data[v.selection[i]]), lo, hi))
                    dst[i >> 3] |= static_cast<uint8_t>(1u << (i & 7));
    }

    VecResult r;
    r.data = dst; r.validity = out_null;
    r.selection = draken_identity_sel(n); r.owns_selection = false;
    r.data_length = n; r.length = n; r.type = DRAKEN_BOOL;
    r.flags = static_cast<uint8_t>(DRAKEN_SEL_IDENTITY | DRAKEN_SEL_PERMUTATION);
    return r;
}

template<typename T>
static inline VecResult fixed_int_between(
    const DrakenVector& v, int64_t lo, int64_t hi, bool lo_incl, bool hi_incl)
{
    if (lo_incl) {
        if (hi_incl) return fi_between_impl<T, true,  true >(v, lo, hi);
        else         return fi_between_impl<T, true,  false>(v, lo, hi);
    } else {
        if (hi_incl) return fi_between_impl<T, false, true >(v, lo, hi);
        else         return fi_between_impl<T, false, false>(v, lo, hi);
    }
}

// ===========================================================================
// IN_LIST — sign-extend T → int64_t → uint64_t, hash, probe set
// §1 EXCEPTION (same as int64): hash-only, no key verification.
// ===========================================================================

template<typename T>
static inline VecResult fixed_int_in_list(
    const DrakenVector& v,
    const opteryx::carchar::CarcharSet& set)
{
    const uint32_t n        = v.length;
    const T*       data     = static_cast<const T*>(v.data);
    const uint8_t* src_null = v.validity;

    if (draken_is_constant(&v)) {
        uint64_t raw = static_cast<uint64_t>(static_cast<int64_t>(data[0]));
        uint64_t h;
        simd_hash_i64(&raw, &h, 1);
        return cmp_constant_bool_result(set.contains(h), src_null, n);
    }

    if (draken_is_dict(&v)) {
        const uint32_t dl = v.data_length;
        uint8_t* db = static_cast<uint8_t*>(draken_malloc(dl));
        if (!db) throw std::bad_alloc();
        uint64_t scratch[1024], hashes[1024];
        uint32_t done = 0;
        while (done < dl) {
            const uint32_t block = (dl - done < 1024u) ? (dl - done) : 1024u;
            for (uint32_t j = 0; j < block; ++j)
                scratch[j] = static_cast<uint64_t>(
                    static_cast<int64_t>(data[done + j]));
            simd_hash_i64(scratch, hashes, block);
            for (uint32_t j = 0; j < block; ++j)
                db[done + j] = set.contains(hashes[j]) ? 1u : 0u;
            done += block;
        }
        VecResult r;
        try { r = cmp_dict_bool_result(db, v); }
        catch (...) { draken_free(db); throw; }
        draken_free(db);
        return r;
    }

    uint8_t* dst      = cmp_alloc_bool_buf(n);
    uint8_t* out_null = nullptr;
    if (src_null != nullptr) {
        try { out_null = cmp_copy_validity(src_null, n); }
        catch (...) { draken_free(dst); throw; }
    }

    uint64_t scratch[1024], hashes[1024];
    uint32_t i = 0;
    while (i < n) {
        const uint32_t block = (n - i < 1024u) ? (n - i) : 1024u;
        for (uint32_t j = 0; j < block; ++j)
            scratch[j] = static_cast<uint64_t>(
                static_cast<int64_t>(data[v.selection[i + j]]));
        simd_hash_i64(scratch, hashes, block);
        if (src_null == nullptr) {
            for (uint32_t j = 0; j < block; ++j)
                if (set.contains(hashes[j]))
                    dst[(i + j) >> 3] |= static_cast<uint8_t>(1u << ((i + j) & 7));
        } else {
            for (uint32_t j = 0; j < block; ++j) {
                const uint32_t row = i + j;
                if ((src_null[row >> 3] >> (row & 7)) & 1u)
                    if (set.contains(hashes[j]))
                        dst[row >> 3] |= static_cast<uint8_t>(1u << (row & 7));
            }
        }
        i += block;
    }

    VecResult r;
    r.data = dst; r.validity = out_null;
    r.selection = draken_identity_sel(n); r.owns_selection = false;
    r.data_length = n; r.length = n; r.type = DRAKEN_BOOL;
    r.flags = static_cast<uint8_t>(DRAKEN_SEL_IDENTITY | DRAKEN_SEL_PERMUTATION);
    return r;
}

// ===========================================================================
// PROMOTION — lossless widening for cross-width dispatch
// ===========================================================================

template<typename FromT, typename ToT, DrakenType ToTag>
static inline VecResult fi_promote(const DrakenVector& v) {
    const uint32_t n = v.length;
    const FromT* data = static_cast<const FromT*>(v.data);
    ToT* dst = fi_alloc<ToT>(n);
    for (uint32_t i = 0; i < n; ++i)
        dst[i] = static_cast<ToT>(data[v.selection[i]]);
    return fi_make_dense<ToT, ToTag>(dst, fi_copy_validity(v.validity, n), n);
}

static inline VecResult promote_i8_to_i16(const DrakenVector& v)  { return fi_promote<int8_t,  int16_t, DRAKEN_INT16>(v); }
static inline VecResult promote_i8_to_i32(const DrakenVector& v)  { return fi_promote<int8_t,  int32_t, DRAKEN_INT32>(v); }
static inline VecResult promote_i8_to_i64(const DrakenVector& v)  { return fi_promote<int8_t,  int64_t, DRAKEN_INT64>(v); }
static inline VecResult promote_i16_to_i32(const DrakenVector& v) { return fi_promote<int16_t, int32_t, DRAKEN_INT32>(v); }
static inline VecResult promote_i16_to_i64(const DrakenVector& v) { return fi_promote<int16_t, int64_t, DRAKEN_INT64>(v); }
static inline VecResult promote_i32_to_i64(const DrakenVector& v) { return fi_promote<int32_t, int64_t, DRAKEN_INT64>(v); }

// Promote v to target_type (must be wider than v.type for integer types).
static inline VecResult promote_narrow_int(const DrakenVector& v, DrakenType target) {
    if (v.type == DRAKEN_INT8) {
        if (target == DRAKEN_INT16) return promote_i8_to_i16(v);
        if (target == DRAKEN_INT32) return promote_i8_to_i32(v);
        if (target == DRAKEN_INT64) return promote_i8_to_i64(v);
    } else if (v.type == DRAKEN_INT16) {
        if (target == DRAKEN_INT32) return promote_i16_to_i32(v);
        if (target == DRAKEN_INT64) return promote_i16_to_i64(v);
    } else if (v.type == DRAKEN_INT32) {
        if (target == DRAKEN_INT64) return promote_i32_to_i64(v);
    }
    throw std::invalid_argument("promote_narrow_int: invalid type combination");
}

// ===========================================================================
// Public entry-point wrappers — instantiate templates for each width
// ===========================================================================

// --- INT8 ---
static inline void     hash_int8(const DrakenVector& v, uint64_t* o, uint32_t n) { fixed_int_hash<int8_t,  DRAKEN_INT8>(v, o, n); }
static inline VecResult i8_compare_scalar(const DrakenVector& v, int64_t s, int op) { return fixed_int_compare_scalar<int8_t>(v, s, op); }
static inline VecResult i8_compare_vector(const DrakenVector& a, const DrakenVector& b, int op) { return fixed_int_compare_vector<int8_t>(a, b, op); }
static inline uint32_t  i8_sum(const DrakenVector& v, int64_t* o) { return fixed_int_sum<int8_t>(v, o); }
static inline uint32_t  i8_min(const DrakenVector& v, int64_t* o) { return fixed_int_min<int8_t>(v, o); }
static inline uint32_t  i8_max(const DrakenVector& v, int64_t* o) { return fixed_int_max<int8_t>(v, o); }
static inline VecResult i8_add(const DrakenVector& a, const DrakenVector& b)   { return fixed_int_add<int8_t>(a, b); }
static inline VecResult i8_add_scalar(const DrakenVector& a, int64_t s)        { return fixed_int_add_scalar<int8_t>(a, s); }
static inline VecResult i8_sub(const DrakenVector& a, const DrakenVector& b)   { return fixed_int_sub<int8_t>(a, b); }
static inline VecResult i8_sub_scalar(const DrakenVector& a, int64_t s)        { return fixed_int_sub_scalar<int8_t>(a, s); }
static inline VecResult i8_mul(const DrakenVector& a, const DrakenVector& b)   { return fixed_int_mul<int8_t>(a, b); }
static inline VecResult i8_mul_scalar(const DrakenVector& a, int64_t s)        { return fixed_int_mul_scalar<int8_t>(a, s); }
static inline VecResult i8_div(const DrakenVector& a, const DrakenVector& b)   { return fixed_int_div<int8_t>(a, b); }
static inline VecResult i8_div_scalar(const DrakenVector& a, int64_t s)        { return fixed_int_div_scalar<int8_t>(a, s); }
static inline VecResult i8_mod(const DrakenVector& a, const DrakenVector& b)   { return fixed_int_mod<int8_t>(a, b); }
static inline VecResult i8_mod_scalar(const DrakenVector& a, int64_t s)        { return fixed_int_mod_scalar<int8_t>(a, s); }
static inline VecResult i8_neg(const DrakenVector& a)                           { return fixed_int_neg<int8_t>(a); }
static inline VecResult i8_take(const DrakenVector& v, const int32_t* idx, uint32_t n) { return fixed_int_take<int8_t,  DRAKEN_INT8>(v, idx, n); }

static inline void fi_copy_validity_range(uint8_t* dst, const uint8_t* src,
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

template<typename T, DrakenType TAG>
static inline VecResult fixed_int_slice(const DrakenVector& v, uint32_t start, uint32_t length) {
    const uint32_t n        = length;
    const T*       data     = static_cast<const T*>(v.data);
    const uint8_t* src_null = v.validity;

    T* dst = fi_alloc<T>(n);

    // Physical memcpy valid ONLY when selection is identity; data_length==length
    // also admits a PERMUTATION which would silently reorder. Require IDENTITY.
    if (draken_is_dense(&v) && (v.flags & DRAKEN_SEL_IDENTITY)) {
        std::memcpy(dst, data + start, n * sizeof(T));
    } else {
        for (uint32_t i = 0; i < n; ++i)
            dst[i] = data[v.selection[start + i]];
    }

    uint8_t* out_null = nullptr;
    if (src_null != nullptr && n > 0u) {
        const uint32_t nb = (n + 7u) >> 3;
        out_null = static_cast<uint8_t*>(draken_malloc(nb));
        if (!out_null) { draken_free(dst); throw std::bad_alloc(); }
        fi_copy_validity_range(out_null, src_null, start, n);
        out_null = fi_normalize_validity(out_null, n);
    }

    VecResult r;
    r.data           = dst;
    r.validity       = out_null;
    r.selection      = draken_identity_sel(n);
    r.owns_selection = false;
    r.data_length    = n;
    r.length         = n;
    r.type           = TAG;
    r.flags          = static_cast<uint8_t>(DRAKEN_SEL_IDENTITY | DRAKEN_SEL_PERMUTATION);
    return r;
}

static inline VecResult i8_slice(const DrakenVector& v, uint32_t s, uint32_t n)  { return fixed_int_slice<int8_t,  DRAKEN_INT8>(v, s, n); }
static inline VecResult i8_materialize(const DrakenVector& v)                    { return fixed_int_materialize<int8_t,  DRAKEN_INT8>(v); }
static inline VecResult i8_compress(const DrakenVector& v)                       { return fixed_int_compress<int8_t,  DRAKEN_INT8>(v); }
static inline VecResult i8_between(const DrakenVector& v, int64_t lo, int64_t hi, bool li, bool hi_i) { return fixed_int_between<int8_t>(v, lo, hi, li, hi_i); }
static inline VecResult i8_in_list(const DrakenVector& v, const opteryx::carchar::CarcharSet& s) { return fixed_int_in_list<int8_t>(v, s); }

// --- INT16 ---
static inline void     hash_int16(const DrakenVector& v, uint64_t* o, uint32_t n) { fixed_int_hash<int16_t, DRAKEN_INT16>(v, o, n); }
static inline VecResult i16_compare_scalar(const DrakenVector& v, int64_t s, int op) { return fixed_int_compare_scalar<int16_t>(v, s, op); }
static inline VecResult i16_compare_vector(const DrakenVector& a, const DrakenVector& b, int op) { return fixed_int_compare_vector<int16_t>(a, b, op); }
static inline uint32_t  i16_sum(const DrakenVector& v, int64_t* o) { return fixed_int_sum<int16_t>(v, o); }
static inline uint32_t  i16_min(const DrakenVector& v, int64_t* o) { return fixed_int_min<int16_t>(v, o); }
static inline uint32_t  i16_max(const DrakenVector& v, int64_t* o) { return fixed_int_max<int16_t>(v, o); }
static inline VecResult i16_add(const DrakenVector& a, const DrakenVector& b)   { return fixed_int_add<int16_t>(a, b); }
static inline VecResult i16_add_scalar(const DrakenVector& a, int64_t s)        { return fixed_int_add_scalar<int16_t>(a, s); }
static inline VecResult i16_sub(const DrakenVector& a, const DrakenVector& b)   { return fixed_int_sub<int16_t>(a, b); }
static inline VecResult i16_sub_scalar(const DrakenVector& a, int64_t s)        { return fixed_int_sub_scalar<int16_t>(a, s); }
static inline VecResult i16_mul(const DrakenVector& a, const DrakenVector& b)   { return fixed_int_mul<int16_t>(a, b); }
static inline VecResult i16_mul_scalar(const DrakenVector& a, int64_t s)        { return fixed_int_mul_scalar<int16_t>(a, s); }
static inline VecResult i16_div(const DrakenVector& a, const DrakenVector& b)   { return fixed_int_div<int16_t>(a, b); }
static inline VecResult i16_div_scalar(const DrakenVector& a, int64_t s)        { return fixed_int_div_scalar<int16_t>(a, s); }
static inline VecResult i16_mod(const DrakenVector& a, const DrakenVector& b)   { return fixed_int_mod<int16_t>(a, b); }
static inline VecResult i16_mod_scalar(const DrakenVector& a, int64_t s)        { return fixed_int_mod_scalar<int16_t>(a, s); }
static inline VecResult i16_neg(const DrakenVector& a)                           { return fixed_int_neg<int16_t>(a); }
static inline VecResult i16_take(const DrakenVector& v, const int32_t* idx, uint32_t n) { return fixed_int_take<int16_t, DRAKEN_INT16>(v, idx, n); }
static inline VecResult i16_slice(const DrakenVector& v, uint32_t s, uint32_t n) { return fixed_int_slice<int16_t, DRAKEN_INT16>(v, s, n); }
static inline VecResult i16_materialize(const DrakenVector& v)                    { return fixed_int_materialize<int16_t, DRAKEN_INT16>(v); }
static inline VecResult i16_compress(const DrakenVector& v)                       { return fixed_int_compress<int16_t, DRAKEN_INT16>(v); }
static inline VecResult i16_between(const DrakenVector& v, int64_t lo, int64_t hi, bool li, bool hi_i) { return fixed_int_between<int16_t>(v, lo, hi, li, hi_i); }
static inline VecResult i16_in_list(const DrakenVector& v, const opteryx::carchar::CarcharSet& s) { return fixed_int_in_list<int16_t>(v, s); }

// --- INT32 ---
static inline void     hash_int32(const DrakenVector& v, uint64_t* o, uint32_t n) { fixed_int_hash<int32_t, DRAKEN_INT32>(v, o, n); }
static inline VecResult i32_compare_scalar(const DrakenVector& v, int64_t s, int op) { return fixed_int_compare_scalar<int32_t>(v, s, op); }
static inline VecResult i32_compare_vector(const DrakenVector& a, const DrakenVector& b, int op) { return fixed_int_compare_vector<int32_t>(a, b, op); }
static inline uint32_t  i32_sum(const DrakenVector& v, int64_t* o) { return fixed_int_sum<int32_t>(v, o); }
static inline uint32_t  i32_min(const DrakenVector& v, int64_t* o) { return fixed_int_min<int32_t>(v, o); }
static inline uint32_t  i32_max(const DrakenVector& v, int64_t* o) { return fixed_int_max<int32_t>(v, o); }
static inline VecResult i32_add(const DrakenVector& a, const DrakenVector& b)   { return fixed_int_add<int32_t>(a, b); }
static inline VecResult i32_add_scalar(const DrakenVector& a, int64_t s)        { return fixed_int_add_scalar<int32_t>(a, s); }
static inline VecResult i32_sub(const DrakenVector& a, const DrakenVector& b)   { return fixed_int_sub<int32_t>(a, b); }
static inline VecResult i32_sub_scalar(const DrakenVector& a, int64_t s)        { return fixed_int_sub_scalar<int32_t>(a, s); }
static inline VecResult i32_mul(const DrakenVector& a, const DrakenVector& b)   { return fixed_int_mul<int32_t>(a, b); }
static inline VecResult i32_mul_scalar(const DrakenVector& a, int64_t s)        { return fixed_int_mul_scalar<int32_t>(a, s); }
static inline VecResult i32_div(const DrakenVector& a, const DrakenVector& b)   { return fixed_int_div<int32_t>(a, b); }
static inline VecResult i32_div_scalar(const DrakenVector& a, int64_t s)        { return fixed_int_div_scalar<int32_t>(a, s); }
static inline VecResult i32_mod(const DrakenVector& a, const DrakenVector& b)   { return fixed_int_mod<int32_t>(a, b); }
static inline VecResult i32_mod_scalar(const DrakenVector& a, int64_t s)        { return fixed_int_mod_scalar<int32_t>(a, s); }
static inline VecResult i32_neg(const DrakenVector& a)                           { return fixed_int_neg<int32_t>(a); }
static inline VecResult i32_take(const DrakenVector& v, const int32_t* idx, uint32_t n) { return fixed_int_take<int32_t, DRAKEN_INT32>(v, idx, n); }
static inline VecResult i32_slice(const DrakenVector& v, uint32_t s, uint32_t n) { return fixed_int_slice<int32_t, DRAKEN_INT32>(v, s, n); }
static inline VecResult i32_materialize(const DrakenVector& v)                    { return fixed_int_materialize<int32_t, DRAKEN_INT32>(v); }
static inline VecResult i32_compress(const DrakenVector& v)                       { return fixed_int_compress<int32_t, DRAKEN_INT32>(v); }
static inline VecResult i32_between(const DrakenVector& v, int64_t lo, int64_t hi, bool li, bool hi_i) { return fixed_int_between<int32_t>(v, lo, hi, li, hi_i); }
static inline VecResult i32_in_list(const DrakenVector& v, const opteryx::carchar::CarcharSet& s) { return fixed_int_in_list<int32_t>(v, s); }

}} // namespace draken::ops
