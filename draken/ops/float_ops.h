#pragma once
// draken/ops/float_ops.h — float32 / float64 kernels (Milestone D.7).
//
// CANONICALIZATION (applied at ingestion AND at arithmetic output):
//   -0.0 → +0.0  (fp_canon)
//   any NaN bit-pattern → canonical quiet NaN  (fp_canon)
//   After canon: hash uses raw bits directly; eq/ne use isnan guard only.
//   Arithmetic results are re-canonicalized so GROUP BY / hash on results is safe.
//
// SEMANTICS (architect-locked 2026-05-22):
//   NaN == NaN: true.  NaN > every finite and ±inf: true.
//   -0.0 == 0.0: true (canonicalized away at ingestion).
//   Float arithmetic: IEEE.  1.0/0.0 → +inf; -1.0/0.0 → -inf;
//     0.0/0.0 → NaN.  Do NOT apply int div0→0 rule.
//
// NULL vs NaN:
//   null = absent (validity bit clear) — skipped by reductions, NULL in TVL.
//   NaN  = a value (validity bit set)  — participates; treated as highest.
//
// TOTAL ORDER (NaN highest):
//   fp_total_lt(NaN, x)  → false  (NaN ≥ everything)
//   fp_total_lt(x, NaN)  → true   (non-NaN < NaN)
//   fp_total_eq(NaN,NaN) → true

#include <cstdint>
#include <cstring>
#include <cmath>
#include <limits>
#include <stdexcept>
#include <unordered_map>
#include <vector>

#include <new>        // std::bad_alloc / placement new — not reliably pulled in by <stdexcept> on stricter libc++
#include "core/buffers.h"
#include "core/alloc.h"
#include "core/vector_alloc.h"
#include "ops/vec_result.h"
#include "ops/dict_take.h"
#include "ops/slice_shape.h"  // slice_keep_dict — the shared keep-or-flatten rule
#include "ops/int64_compare.h"  // cmp_alloc_bool_buf, cmp_copy_validity, cmp_and_validity
#include "simd_hash.h"          // simd_hash_i64, NULL_HASH
#include "carchar_set.hpp"      // CarcharSet

namespace draken { namespace ops {

// ===========================================================================
// Bit-level helpers — safe against -ffast-math (use memcpy, not type punning).
// ===========================================================================

static inline bool fp_isnan_bits(float x) noexcept {
    uint32_t bits;
    std::memcpy(&bits, &x, 4);
    return (bits & 0x7F800000u) == 0x7F800000u && (bits & 0x007FFFFFu) != 0u;
}

static inline bool fp_isnan_bits(double x) noexcept {
    uint64_t bits;
    std::memcpy(&bits, &x, 8);
    return (bits & 0x7FF0000000000000ULL) == 0x7FF0000000000000ULL
        && (bits & 0x000FFFFFFFFFFFFFULL) != 0ULL;
}

// Canonical quiet NaN bits: float=0x7FC00000, double=0x7FF8000000000000.
static inline float fp_canon(float x) noexcept {
    uint32_t bits;
    std::memcpy(&bits, &x, 4);
    // NaN: exponent all-1, mantissa non-zero → canonical quiet NaN
    if ((bits & 0x7F800000u) == 0x7F800000u && (bits & 0x007FFFFFu) != 0u) {
        const uint32_t qnan = 0x7FC00000u;
        float r; std::memcpy(&r, &qnan, 4); return r;
    }
    // -0.0: sign bit set, everything else zero → +0.0
    if (bits == 0x80000000u) return 0.0f;
    return x;
}

static inline double fp_canon(double x) noexcept {
    uint64_t bits;
    std::memcpy(&bits, &x, 8);
    if ((bits & 0x7FF0000000000000ULL) == 0x7FF0000000000000ULL
        && (bits & 0x000FFFFFFFFFFFFFULL) != 0ULL) {
        const uint64_t qnan = 0x7FF8000000000000ULL;
        double r; std::memcpy(&r, &qnan, 8); return r;
    }
    if (bits == 0x8000000000000000ULL) return 0.0;
    return x;
}

// Bit-cast to uint64 for hashing (caller must supply canonical value).
static inline uint64_t fp_bits64(float x) noexcept {
    uint32_t bits; std::memcpy(&bits, &x, 4);
    return static_cast<uint64_t>(bits);
}

static inline uint64_t fp_bits64(double x) noexcept {
    uint64_t bits; std::memcpy(&bits, &x, 8); return bits;
}

// ===========================================================================
// Total-order compare helpers (NaN highest, -0.0 == 0.0 after canon).
// ===========================================================================

template<typename T>
static inline bool fp_total_eq(T a, T b) noexcept {
    const bool an = fp_isnan_bits(a), bn = fp_isnan_bits(b);
    return an ? bn : (!bn && a == b);
}

template<typename T>
static inline bool fp_total_lt(T a, T b) noexcept {
    if (fp_isnan_bits(a)) return false;
    if (fp_isnan_bits(b)) return true;
    return a < b;
}

template<typename T> static inline bool fp_total_le(T a, T b) noexcept { return !fp_total_lt(b, a); }
template<typename T> static inline bool fp_total_gt(T a, T b) noexcept { return fp_total_lt(b, a); }
template<typename T> static inline bool fp_total_ge(T a, T b) noexcept { return !fp_total_lt(a, b); }
template<typename T> static inline bool fp_total_ne(T a, T b) noexcept { return !fp_total_eq(a, b); }

// ===========================================================================
// Internal allocation helpers (fp_ prefix to avoid ODR clashes).
// ===========================================================================

static inline bool fp_row_valid(const uint8_t* validity, uint32_t i) noexcept {
    return (validity == nullptr) || ((validity[i >> 3] >> (i & 7)) & 1u);
}

static inline void fp_set_valid_bit(uint8_t* bitmap, uint32_t i) noexcept {
    bitmap[i >> 3] |= static_cast<uint8_t>(1u << (i & 7));
}

static inline uint8_t* fp_copy_validity(const uint8_t* src, uint32_t n) {
    if (src == nullptr) return nullptr;
    const uint32_t nb = (n + 7u) >> 3;
    uint8_t* dst = static_cast<uint8_t*>(draken_malloc(nb > 0u ? nb : 1u));
    if (!dst) throw std::bad_alloc();
    std::memcpy(dst, src, nb);
    return dst;
}

static inline uint8_t* fp_normalize_validity(uint8_t* validity, uint32_t n) noexcept {
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

static inline uint8_t* fp_combine_validity(
    const uint8_t* a, const uint8_t* b, uint32_t n)
{
    if (a == nullptr && b == nullptr) return nullptr;
    const uint32_t nb = (n + 7u) >> 3;
    uint8_t* out = static_cast<uint8_t*>(draken_malloc(nb > 0u ? nb : 1u));
    if (!out) throw std::bad_alloc();
    if (a != nullptr && b != nullptr)
        for (uint32_t k = 0; k < nb; ++k) out[k] = a[k] & b[k];
    else if (a != nullptr) std::memcpy(out, a, nb);
    else                   std::memcpy(out, b, nb);
    // Drop a fully-valid result so downstream takes the validity==nullptr path.
    return fp_normalize_validity(out, n);
}

template<typename T>
static inline T* fp_alloc(uint32_t n) {
    T* p = static_cast<T*>(draken_malloc((n > 0u ? n : 1u) * sizeof(T)));
    if (!p) throw std::bad_alloc();
    return p;
}

template<typename T, DrakenType TAG>
static inline VecResult fp_make_dense(T* data, uint8_t* validity, uint32_t n) {
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
// HASH — canonical bits → simd_hash_i64.
// Branchless null-select matches int64 pattern for consistency.
// ===========================================================================

template<typename T, DrakenType TAG>
static inline void float_hash(const DrakenVector& v, uint64_t* out, uint32_t n) {
    if (n == 0) return;
    const T*       data     = static_cast<const T*>(v.data);
    const uint8_t* validity = v.validity;
    uint64_t scratch[1024];

    uint32_t i = 0;
    while (i < n) {
        const uint32_t block = (n - i < 1024u) ? (n - i) : 1024u;
        if (validity != nullptr) {
            for (uint32_t j = 0; j < block; ++j) {
                const uint32_t row = i + j;
                const uint64_t is_valid = (validity[row >> 3] >> (row & 7)) & 1u;
                const uint64_t bits = fp_bits64(data[v.selection[row]]);
                scratch[j] = (bits * is_valid) | (NULL_HASH * (1u - is_valid));
            }
        } else {
            for (uint32_t j = 0; j < block; ++j)
                scratch[j] = fp_bits64(data[v.selection[i + j]]);
        }
        simd_hash_i64(scratch, out + i, block);
        i += block;
    }
}

// ===========================================================================
// COMPARE SCALAR — 6 ops, total-order semantics (NaN highest).
// ===========================================================================

struct FpCmpEq { template<typename T> static bool apply(T a, T b) noexcept { return fp_total_eq(a, b); } };
struct FpCmpNe { template<typename T> static bool apply(T a, T b) noexcept { return fp_total_ne(a, b); } };
struct FpCmpGt { template<typename T> static bool apply(T a, T b) noexcept { return fp_total_gt(a, b); } };
struct FpCmpGe { template<typename T> static bool apply(T a, T b) noexcept { return fp_total_ge(a, b); } };
struct FpCmpLt { template<typename T> static bool apply(T a, T b) noexcept { return fp_total_lt(a, b); } };
struct FpCmpLe { template<typename T> static bool apply(T a, T b) noexcept { return fp_total_le(a, b); } };

// Operand-swapped op: `a OP b` == `b FpReversedOp<OP> a`. Lets a constant LEFT
// operand reduce to the scalar path against the non-constant RIGHT operand.
template<typename Op> struct FpReversedOp;
template<> struct FpReversedOp<FpCmpEq> { using type = FpCmpEq; };
template<> struct FpReversedOp<FpCmpNe> { using type = FpCmpNe; };
template<> struct FpReversedOp<FpCmpGt> { using type = FpCmpLt; };
template<> struct FpReversedOp<FpCmpGe> { using type = FpCmpLe; };
template<> struct FpReversedOp<FpCmpLt> { using type = FpCmpGt; };
template<> struct FpReversedOp<FpCmpLe> { using type = FpCmpGe; };

// Inner kernel: 8-way byte-pack. Identity==true indexes data[pos] directly
// (vectorisable); false gathers data[selection[pos]]. Same answer (hint, §11).
template<typename T, typename Op, bool Identity>
static inline void fp_cmp_scalar_kernel(
    const T* data, const uint32_t* selection, T scalar,
    const uint8_t* src_null, uint8_t* dst, uint32_t n)
{
    const uint32_t whole_bytes = n >> 3;
    auto at = [&](uint32_t pos) -> T {
        if constexpr (Identity) return data[pos];
        else                    return data[selection[pos]];
    };
    if (src_null == nullptr) {
        for (uint32_t b = 0; b < whole_bytes; ++b) {
            const uint32_t base = b << 3;
            dst[b] = static_cast<uint8_t>(
                (static_cast<unsigned>(Op::template apply<T>(at(base+0), scalar)) << 0) |
                (static_cast<unsigned>(Op::template apply<T>(at(base+1), scalar)) << 1) |
                (static_cast<unsigned>(Op::template apply<T>(at(base+2), scalar)) << 2) |
                (static_cast<unsigned>(Op::template apply<T>(at(base+3), scalar)) << 3) |
                (static_cast<unsigned>(Op::template apply<T>(at(base+4), scalar)) << 4) |
                (static_cast<unsigned>(Op::template apply<T>(at(base+5), scalar)) << 5) |
                (static_cast<unsigned>(Op::template apply<T>(at(base+6), scalar)) << 6) |
                (static_cast<unsigned>(Op::template apply<T>(at(base+7), scalar)) << 7));
        }
        for (uint32_t i = whole_bytes << 3; i < n; ++i)
            if (Op::template apply<T>(at(i), scalar))
                dst[i >> 3] |= static_cast<uint8_t>(1u << (i & 7));
    } else {
        for (uint32_t b = 0; b < whole_bytes; ++b) {
            const uint32_t base = b << 3;
            const uint8_t m = static_cast<uint8_t>(
                (static_cast<unsigned>(Op::template apply<T>(at(base+0), scalar)) << 0) |
                (static_cast<unsigned>(Op::template apply<T>(at(base+1), scalar)) << 1) |
                (static_cast<unsigned>(Op::template apply<T>(at(base+2), scalar)) << 2) |
                (static_cast<unsigned>(Op::template apply<T>(at(base+3), scalar)) << 3) |
                (static_cast<unsigned>(Op::template apply<T>(at(base+4), scalar)) << 4) |
                (static_cast<unsigned>(Op::template apply<T>(at(base+5), scalar)) << 5) |
                (static_cast<unsigned>(Op::template apply<T>(at(base+6), scalar)) << 6) |
                (static_cast<unsigned>(Op::template apply<T>(at(base+7), scalar)) << 7));
            dst[b] = static_cast<uint8_t>(m & src_null[b]);
        }
        for (uint32_t i = whole_bytes << 3; i < n; ++i)
            if ((src_null[i >> 3] >> (i & 7)) & 1u)
                if (Op::template apply<T>(at(i), scalar))
                    dst[i >> 3] |= static_cast<uint8_t>(1u << (i & 7));
    }
}

template<typename T, typename Op>
static inline VecResult fp_compare_scalar_impl(const DrakenVector& v, T scalar) {
    const uint32_t n        = v.length;
    const T*       data     = static_cast<const T*>(v.data);
    const uint8_t* src_null = v.validity;

    // Constant: one predicate broadcast (CLAUDE.md §11, approved 2026-06-11).
    if (draken_is_constant(&v))
        return cmp_constant_bool_result(Op::template apply<T>(data[0], scalar), src_null, n);

    // Dict: evaluate the predicate once per unique value, scatter through codes.
    // Uses the identical Op as the dense path, so NaN/-0 handling matches exactly.
    if (draken_is_dict(&v)) {
        const uint32_t dl = v.data_length;
        uint8_t* db = static_cast<uint8_t*>(draken_malloc(dl));
        if (!db) throw std::bad_alloc();
        for (uint32_t k = 0; k < dl; ++k)
            db[k] = Op::template apply<T>(data[k], scalar) ? 1u : 0u;
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

    if (v.flags & DRAKEN_SEL_IDENTITY)
        fp_cmp_scalar_kernel<T, Op, true >(data, v.selection, scalar, src_null, dst, n);
    else
        fp_cmp_scalar_kernel<T, Op, false>(data, v.selection, scalar, src_null, dst, n);

    VecResult r;
    r.data = dst; r.validity = out_null;
    r.selection = draken_identity_sel(n); r.owns_selection = false;
    r.data_length = n; r.length = n; r.type = DRAKEN_BOOL;
    r.flags = static_cast<uint8_t>(DRAKEN_SEL_IDENTITY | DRAKEN_SEL_PERMUTATION);
    return r;
}

// float_compare_scalar: scalar arrives as double, narrowed to T and canonicalized.
template<typename T>
static inline VecResult float_compare_scalar(const DrakenVector& v, double scalar_d, int op) {
    const T scalar = fp_canon(static_cast<T>(scalar_d));
    switch (op) {
        case 0: return fp_compare_scalar_impl<T, FpCmpEq>(v, scalar);
        case 1: return fp_compare_scalar_impl<T, FpCmpNe>(v, scalar);
        case 2: return fp_compare_scalar_impl<T, FpCmpGt>(v, scalar);
        case 3: return fp_compare_scalar_impl<T, FpCmpGe>(v, scalar);
        case 4: return fp_compare_scalar_impl<T, FpCmpLt>(v, scalar);
        default: return fp_compare_scalar_impl<T, FpCmpLe>(v, scalar);
    }
}

// ===========================================================================
// COMPARE VECTOR — same type T, total-order semantics.
// Reuses CmpVecFn signature (matches int slot type).
// ===========================================================================

template<typename T, typename Op>
static inline VecResult fp_compare_vector_impl(
    const DrakenVector& a, const DrakenVector& b)
{
    const uint32_t n = a.length;
    if (b.length != n)
        throw std::invalid_argument("fp_compare_vector: operand lengths must match");
    const T* ad = static_cast<const T*>(a.data);
    const T* bd = static_cast<const T*>(b.data);

    // constant ⇄ non-constant → reduce to the scalar path (dict/dense fast paths
    // on the non-constant side; covers WHERE float_col <op> literal). Skip when
    // the constant is NULL — left to the validity-aware generic path. All six
    // ops, with the operand swap for a constant LEFT operand.
    if (draken_is_constant(&b) && b.validity == nullptr)
        return fp_compare_scalar_impl<T, Op>(a, bd[0]);
    if (draken_is_constant(&a) && a.validity == nullptr)
        return fp_compare_scalar_impl<T, typename FpReversedOp<Op>::type>(b, ad[0]);

    uint8_t* out_null = cmp_and_validity(a.validity, b.validity, n);
    uint8_t* dst = nullptr;
    try { dst = cmp_alloc_bool_buf(n); }
    catch (...) { if (out_null) draken_free(out_null); throw; }

    const uint32_t whole_bytes = n >> 3;
    if (out_null == nullptr) {
        for (uint32_t bk = 0; bk < whole_bytes; ++bk) {
            const uint32_t base = bk << 3;
            dst[bk] = static_cast<uint8_t>(
                (static_cast<unsigned>(Op::template apply<T>(ad[a.selection[base+0]], bd[b.selection[base+0]])) << 0) |
                (static_cast<unsigned>(Op::template apply<T>(ad[a.selection[base+1]], bd[b.selection[base+1]])) << 1) |
                (static_cast<unsigned>(Op::template apply<T>(ad[a.selection[base+2]], bd[b.selection[base+2]])) << 2) |
                (static_cast<unsigned>(Op::template apply<T>(ad[a.selection[base+3]], bd[b.selection[base+3]])) << 3) |
                (static_cast<unsigned>(Op::template apply<T>(ad[a.selection[base+4]], bd[b.selection[base+4]])) << 4) |
                (static_cast<unsigned>(Op::template apply<T>(ad[a.selection[base+5]], bd[b.selection[base+5]])) << 5) |
                (static_cast<unsigned>(Op::template apply<T>(ad[a.selection[base+6]], bd[b.selection[base+6]])) << 6) |
                (static_cast<unsigned>(Op::template apply<T>(ad[a.selection[base+7]], bd[b.selection[base+7]])) << 7));
        }
        for (uint32_t i = whole_bytes << 3; i < n; ++i)
            if (Op::template apply<T>(ad[a.selection[i]], bd[b.selection[i]]))
                dst[i >> 3] |= static_cast<uint8_t>(1u << (i & 7));
    } else {
        for (uint32_t bk = 0; bk < whole_bytes; ++bk) {
            const uint32_t base = bk << 3;
            const uint8_t m = static_cast<uint8_t>(
                (static_cast<unsigned>(Op::template apply<T>(ad[a.selection[base+0]], bd[b.selection[base+0]])) << 0) |
                (static_cast<unsigned>(Op::template apply<T>(ad[a.selection[base+1]], bd[b.selection[base+1]])) << 1) |
                (static_cast<unsigned>(Op::template apply<T>(ad[a.selection[base+2]], bd[b.selection[base+2]])) << 2) |
                (static_cast<unsigned>(Op::template apply<T>(ad[a.selection[base+3]], bd[b.selection[base+3]])) << 3) |
                (static_cast<unsigned>(Op::template apply<T>(ad[a.selection[base+4]], bd[b.selection[base+4]])) << 4) |
                (static_cast<unsigned>(Op::template apply<T>(ad[a.selection[base+5]], bd[b.selection[base+5]])) << 5) |
                (static_cast<unsigned>(Op::template apply<T>(ad[a.selection[base+6]], bd[b.selection[base+6]])) << 6) |
                (static_cast<unsigned>(Op::template apply<T>(ad[a.selection[base+7]], bd[b.selection[base+7]])) << 7));
            dst[bk] = static_cast<uint8_t>(m & out_null[bk]);
        }
        for (uint32_t i = whole_bytes << 3; i < n; ++i)
            if ((out_null[i >> 3] >> (i & 7)) & 1u)
                if (Op::template apply<T>(ad[a.selection[i]], bd[b.selection[i]]))
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
static inline VecResult float_compare_vector(
    const DrakenVector& a, const DrakenVector& b, int op)
{
    switch (op) {
        case 0: return fp_compare_vector_impl<T, FpCmpEq>(a, b);
        case 1: return fp_compare_vector_impl<T, FpCmpNe>(a, b);
        case 2: return fp_compare_vector_impl<T, FpCmpGt>(a, b);
        case 3: return fp_compare_vector_impl<T, FpCmpGe>(a, b);
        case 4: return fp_compare_vector_impl<T, FpCmpLt>(a, b);
        default: return fp_compare_vector_impl<T, FpCmpLe>(a, b);
    }
}

// ===========================================================================
// REDUCTIONS: sum/min/max.
// Returns count of non-null rows. NaN is a value (counted); null is skipped.
// Output is double for both float32 and float64.
// ===========================================================================

// One fold over a vector's valid rows into an accumulator with eight
// independent lanes. A single running value is one dependency chain; eight let
// the steps overlap (and pack into SIMD lanes). Validity is read a 64-row word at
// a time: all-valid words take the unrolled loop, all-null words are skipped,
// mixed words walk their set bits. Returns the valid-row count.
template<typename T, bool Identity, typename Acc>
static inline void fp_fold_rows(const T* data, const uint32_t* sel,
                                uint32_t lo, uint32_t hi, Acc& acc) noexcept {
    auto at = [&](uint32_t i) -> T {
        if constexpr (Identity) return data[i];
        else                    return data[sel[i]];
    };
    uint32_t i = lo;
    for (; i + 8u <= hi; i += 8u)
        for (uint32_t k = 0; k < 8u; ++k) acc.step(k, at(i + k));
    for (; i < hi; ++i) acc.step(0, at(i));
}

template<typename T, bool Identity, typename Acc>
static inline uint32_t fp_fold_valid(const DrakenVector& v, Acc& acc) noexcept {
    const uint32_t n        = v.length;
    const T*       data     = static_cast<const T*>(v.data);
    const uint32_t* sel     = v.selection;
    const uint8_t* validity = v.validity;
    if (validity == nullptr) {
        fp_fold_rows<T, Identity>(data, sel, 0, n, acc);
        return n;
    }
    uint32_t c = 0;
    const uint32_t words = n >> 6;
    for (uint32_t w = 0; w < words; ++w) {
        uint64_t m;
        std::memcpy(&m, validity + (static_cast<size_t>(w) << 3), sizeof(m));
        const uint32_t base = w << 6;
        if (m == ~0ULL) {
            fp_fold_rows<T, Identity>(data, sel, base, base + 64u, acc);
            c += 64u;
        } else if (m != 0) {
            c += static_cast<uint32_t>(__builtin_popcountll(m));
            while (m) {
                const uint32_t j = base + static_cast<uint32_t>(__builtin_ctzll(m));
                acc.step(0, Identity ? data[j] : data[sel[j]]);
                m &= m - 1;
            }
        }
    }
    for (uint32_t i = words << 6; i < n; ++i) {
        if ((validity[i >> 3] >> (i & 7)) & 1u) {
            acc.step(0, Identity ? data[i] : data[sel[i]]);
            ++c;
        }
    }
    return c;
}

template<typename T, typename Acc>
static inline uint32_t fp_fold(const DrakenVector& v, Acc& acc) noexcept {
    return (v.flags & DRAKEN_SEL_IDENTITY) ? fp_fold_valid<T, true>(v, acc)
                                           : fp_fold_valid<T, false>(v, acc);
}

// sum: summation order differs from a left fold in the low bits only.
struct FpSumAcc {
    double s[8] = {0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0};
    template<typename T> void step(uint32_t k, T x) noexcept { s[k] += static_cast<double>(x); }
    double result() const noexcept {
        return ((s[0] + s[1]) + (s[2] + s[3])) + ((s[4] + s[5]) + (s[6] + s[7]));
    }
};

// min in total order (NaN highest): a NaN never wins `x < m`, so the lanes hold
// the smallest non-NaN; no non-NaN value at all (all-NaN or empty) → NaN.
template<typename T>
struct FpMinAcc {
    T m[8];
    bool real[8] = {false, false, false, false, false, false, false, false};
    FpMinAcc() noexcept { for (T& x : m) x = std::numeric_limits<T>::infinity(); }
    void step(uint32_t k, T x) noexcept { m[k] = (x < m[k]) ? x : m[k]; real[k] |= (x == x); }
    double result() const noexcept {
        T r = m[0];
        bool any = real[0];
        for (uint32_t k = 1; k < 8u; ++k) { r = (m[k] < r) ? m[k] : r; any |= real[k]; }
        return any ? static_cast<double>(r) : std::numeric_limits<double>::quiet_NaN();
    }
};

// max in total order (NaN highest): any NaN wins; otherwise the largest value,
// -inf when there are no values.
template<typename T>
struct FpMaxAcc {
    T m[8];
    bool nan[8] = {false, false, false, false, false, false, false, false};
    FpMaxAcc() noexcept { for (T& x : m) x = -std::numeric_limits<T>::infinity(); }
    void step(uint32_t k, T x) noexcept { m[k] = (x > m[k]) ? x : m[k]; nan[k] |= (x != x); }
    double result() const noexcept {
        T r = m[0];
        bool any = nan[0];
        for (uint32_t k = 1; k < 8u; ++k) { r = (m[k] > r) ? m[k] : r; any |= nan[k]; }
        return any ? std::numeric_limits<double>::quiet_NaN() : static_cast<double>(r);
    }
};

// Σ(x-K) and Σ(x-K)² as doubles (the STDDEV/VARIANCE moments, SHIFTED by K — the
// caller's per-group shift, normally the group's first value). Raw Σx/Σx² lost every
// significant digit to cancellation when the values were large against their spread:
// STDDEV_SAMP(1e9 + {1..9}) answered 0.0. The variance formula is shift-invariant, so
// only the accumulation changes. Generic over the element type (integer columns too).
struct FpMomentsAcc {
    double shift = 0.0;
    double s[8] = {0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0};
    double q[8] = {0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0};
    template<typename T> void step(uint32_t k, T x) noexcept {
        const double d = static_cast<double>(x) - shift;
        s[k] += d;
        q[k] += d * d;
    }
};

template<typename T>
static inline uint32_t numeric_moments(const DrakenVector& v, double shift, double* sum,
                                       double* sumsq) noexcept {
    FpMomentsAcc acc;
    acc.shift = shift;
    const uint32_t count = fp_fold<T>(v, acc);
    *sum   = ((acc.s[0] + acc.s[1]) + (acc.s[2] + acc.s[3])) + ((acc.s[4] + acc.s[5]) + (acc.s[6] + acc.s[7]));
    *sumsq = ((acc.q[0] + acc.q[1]) + (acc.q[2] + acc.q[3])) + ((acc.q[4] + acc.q[5]) + (acc.q[6] + acc.q[7]));
    return count;
}

// Logical rows [lo, lo + n) of a numeric vector, widened to double.
template<typename T>
static inline void fp_load_rows(const DrakenVector& v, uint32_t lo, uint32_t n, double* out) noexcept {
    const T* data = static_cast<const T*>(v.data);
    if (v.flags & DRAKEN_SEL_IDENTITY) {
        for (uint32_t i = 0; i < n; ++i) out[i] = static_cast<double>(data[lo + i]);
    } else {
        for (uint32_t i = 0; i < n; ++i) out[i] = static_cast<double>(data[v.selection[lo + i]]);
    }
}

// False for a type with no plain numeric storage (temporal, DECIMAL, BOOL, ...).
static inline bool fp_load_numeric(const DrakenVector& v, uint32_t lo, uint32_t n, double* out) noexcept {
    switch (v.type) {
        case DRAKEN_FLOAT64: fp_load_rows<double>(v, lo, n, out);   return true;
        case DRAKEN_FLOAT32: fp_load_rows<float>(v, lo, n, out);    return true;
        case DRAKEN_INT8:    fp_load_rows<int8_t>(v, lo, n, out);   return true;
        case DRAKEN_INT16:   fp_load_rows<int16_t>(v, lo, n, out);  return true;
        case DRAKEN_INT32:   fp_load_rows<int32_t>(v, lo, n, out);  return true;
        case DRAKEN_INT64:   fp_load_rows<int64_t>(v, lo, n, out);  return true;
        case DRAKEN_UINT8:   fp_load_rows<uint8_t>(v, lo, n, out);  return true;
        case DRAKEN_UINT16:  fp_load_rows<uint16_t>(v, lo, n, out); return true;
        case DRAKEN_UINT32:  fp_load_rows<uint32_t>(v, lo, n, out); return true;
        case DRAKEN_UINT64:  fp_load_rows<uint64_t>(v, lo, n, out); return true;
        default:             return false;
    }
}

// Σx, Σx², Σy, Σy², Σxy and the pair count over rows where BOTH operands are
// valid (the CORR moments). Both vectors are read in 1024-row blocks widened to
// double, so each column type is dispatched once per block, not per pair.
struct PairMoments {
    double sx = 0.0, sxx = 0.0, sy = 0.0, syy = 0.0, sxy = 0.0;
    uint64_t n = 0;
};

// CORR's moments, shifted by (kx, ky) exactly as FpMomentsAcc is by its K.
struct FpPairAcc {
    double kx = 0.0, ky = 0.0;
    double sx[8] = {}, sxx[8] = {}, sy[8] = {}, syy[8] = {}, sxy[8] = {};
    void step(uint32_t k, double a, double b) noexcept {
        a -= kx;
        b -= ky;
        sx[k] += a; sxx[k] += a * a; sy[k] += b; syy[k] += b * b; sxy[k] += a * b;
    }
    static double fold(const double* s) noexcept {
        return ((s[0] + s[1]) + (s[2] + s[3])) + ((s[4] + s[5]) + (s[6] + s[7]));
    }
};

static inline bool numeric_pair_moments(const DrakenVector& x, const DrakenVector& y,
                                        double kx, double ky, PairMoments* out) noexcept {
    constexpr uint32_t B = 1024;
    double bx[B], by[B];
    FpPairAcc acc;
    acc.kx = kx;
    acc.ky = ky;
    uint64_t count = 0;
    const uint32_t n = x.length;
    const bool all_valid = x.validity == nullptr && y.validity == nullptr;
    auto word = [](const uint8_t* bits, uint32_t w) noexcept -> uint64_t {
        if (bits == nullptr) return ~0ULL;
        uint64_t m;
        std::memcpy(&m, bits + (static_cast<size_t>(w) << 3), sizeof(m));
        return m;
    };
    for (uint32_t lo = 0; lo < n; lo += B) {
        const uint32_t m = (n - lo < B) ? (n - lo) : B;
        if (!fp_load_numeric(x, lo, m, bx) || !fp_load_numeric(y, lo, m, by)) return false;
        uint32_t i = 0;
        if (all_valid) {
            for (; i + 8u <= m; i += 8u)
                for (uint32_t k = 0; k < 8u; ++k) acc.step(k, bx[i + k], by[i + k]);
            for (; i < m; ++i) acc.step(0, bx[i], by[i]);
            count += m;
            continue;
        }
        // lo is a multiple of 64, so block row i is bit (i & 63) of word (lo + i) >> 6.
        for (; i + 64u <= m; i += 64u) {
            const uint32_t w = (lo + i) >> 6;
            uint64_t bits = word(x.validity, w) & word(y.validity, w);
            if (bits == ~0ULL) {
                for (uint32_t j = 0; j < 64u; j += 8u)
                    for (uint32_t k = 0; k < 8u; ++k) acc.step(k, bx[i + j + k], by[i + j + k]);
                count += 64u;
            } else {
                count += static_cast<uint64_t>(__builtin_popcountll(bits));
                while (bits) {
                    const uint32_t j = i + static_cast<uint32_t>(__builtin_ctzll(bits));
                    acc.step(0, bx[j], by[j]);
                    bits &= bits - 1;
                }
            }
        }
        for (; i < m; ++i) {
            const uint32_t r = lo + i;
            if (fp_row_valid(x.validity, r) && fp_row_valid(y.validity, r)) {
                acc.step(0, bx[i], by[i]);
                ++count;
            }
        }
    }
    out->sx  = FpPairAcc::fold(acc.sx);
    out->sxx = FpPairAcc::fold(acc.sxx);
    out->sy  = FpPairAcc::fold(acc.sy);
    out->syy = FpPairAcc::fold(acc.syy);
    out->sxy = FpPairAcc::fold(acc.sxy);
    out->n   = count;
    return true;
}

template<typename T>
static inline uint32_t float_sum(const DrakenVector& v, double* out_value) noexcept {
    FpSumAcc acc;
    const uint32_t count = fp_fold<T>(v, acc);
    *out_value = fp_canon(acc.result());
    return count;
}

template<typename T>
static inline uint32_t float_min(const DrakenVector& v, double* out_value) noexcept {
    FpMinAcc<T> acc;
    const uint32_t count = fp_fold<T>(v, acc);
    *out_value = fp_canon(acc.result());
    return count;
}

template<typename T>
static inline uint32_t float_max(const DrakenVector& v, double* out_value) noexcept {
    FpMaxAcc<T> acc;
    const uint32_t count = fp_fold<T>(v, acc);
    *out_value = fp_canon(acc.result());
    return count;
}

// ===========================================================================
// ARITHMETIC — IEEE semantics. Results are fp_canon'd so hashing stays safe.
// div: IEEE → ±inf or NaN on div-by-zero (NOT int's div0→0 rule).
// mod: std::fmod → NaN if divisor is zero.
// Result type stays T (float32→float32, float64→float64).
// ===========================================================================

template<typename T, DrakenType TAG>
static inline VecResult float_add(const DrakenVector& a, const DrakenVector& b) {
    if (a.length != b.length) throw std::invalid_argument("float_add: length mismatch");
    const uint32_t n = a.length;
    const T* ad = static_cast<const T*>(a.data);
    const T* bd = static_cast<const T*>(b.data);
    T* dst = fp_alloc<T>(n);
    for (uint32_t i = 0; i < n; ++i)
        dst[i] = fp_canon(ad[a.selection[i]] + bd[b.selection[i]]);
    return fp_make_dense<T, TAG>(dst, fp_combine_validity(a.validity, b.validity, n), n);
}

template<typename T, DrakenType TAG>
static inline VecResult float_add_scalar(const DrakenVector& a, double scalar_d) {
    const uint32_t n = a.length;
    const T* ad = static_cast<const T*>(a.data);
    const T sv = fp_canon(static_cast<T>(scalar_d));
    T* dst = fp_alloc<T>(n);
    for (uint32_t i = 0; i < n; ++i) dst[i] = fp_canon(ad[a.selection[i]] + sv);
    return fp_make_dense<T, TAG>(dst, fp_copy_validity(a.validity, n), n);
}

template<typename T, DrakenType TAG>
static inline VecResult float_sub(const DrakenVector& a, const DrakenVector& b) {
    if (a.length != b.length) throw std::invalid_argument("float_sub: length mismatch");
    const uint32_t n = a.length;
    const T* ad = static_cast<const T*>(a.data);
    const T* bd = static_cast<const T*>(b.data);
    T* dst = fp_alloc<T>(n);
    for (uint32_t i = 0; i < n; ++i)
        dst[i] = fp_canon(ad[a.selection[i]] - bd[b.selection[i]]);
    return fp_make_dense<T, TAG>(dst, fp_combine_validity(a.validity, b.validity, n), n);
}

template<typename T, DrakenType TAG>
static inline VecResult float_sub_scalar(const DrakenVector& a, double scalar_d) {
    const uint32_t n = a.length;
    const T* ad = static_cast<const T*>(a.data);
    const T sv = fp_canon(static_cast<T>(scalar_d));
    T* dst = fp_alloc<T>(n);
    for (uint32_t i = 0; i < n; ++i) dst[i] = fp_canon(ad[a.selection[i]] - sv);
    return fp_make_dense<T, TAG>(dst, fp_copy_validity(a.validity, n), n);
}

template<typename T, DrakenType TAG>
static inline VecResult float_mul(const DrakenVector& a, const DrakenVector& b) {
    if (a.length != b.length) throw std::invalid_argument("float_mul: length mismatch");
    const uint32_t n = a.length;
    const T* ad = static_cast<const T*>(a.data);
    const T* bd = static_cast<const T*>(b.data);
    T* dst = fp_alloc<T>(n);
    for (uint32_t i = 0; i < n; ++i)
        dst[i] = fp_canon(ad[a.selection[i]] * bd[b.selection[i]]);
    return fp_make_dense<T, TAG>(dst, fp_combine_validity(a.validity, b.validity, n), n);
}

template<typename T, DrakenType TAG>
static inline VecResult float_mul_scalar(const DrakenVector& a, double scalar_d) {
    const uint32_t n = a.length;
    const T* ad = static_cast<const T*>(a.data);
    const T sv = fp_canon(static_cast<T>(scalar_d));
    T* dst = fp_alloc<T>(n);
    for (uint32_t i = 0; i < n; ++i) dst[i] = fp_canon(ad[a.selection[i]] * sv);
    return fp_make_dense<T, TAG>(dst, fp_copy_validity(a.validity, n), n);
}

// div: IEEE — 1.0/0.0 → +inf, -1.0/0.0 → -inf, 0.0/0.0 → NaN.
template<typename T, DrakenType TAG>
static inline VecResult float_div(const DrakenVector& a, const DrakenVector& b) {
    if (a.length != b.length) throw std::invalid_argument("float_div: length mismatch");
    const uint32_t n = a.length;
    const T* ad = static_cast<const T*>(a.data);
    const T* bd = static_cast<const T*>(b.data);
    T* dst = fp_alloc<T>(n);
    for (uint32_t i = 0; i < n; ++i)
        dst[i] = fp_canon(ad[a.selection[i]] / bd[b.selection[i]]);
    return fp_make_dense<T, TAG>(dst, fp_combine_validity(a.validity, b.validity, n), n);
}

template<typename T, DrakenType TAG>
static inline VecResult float_div_scalar(const DrakenVector& a, double scalar_d) {
    const uint32_t n = a.length;
    const T* ad = static_cast<const T*>(a.data);
    const T sv = fp_canon(static_cast<T>(scalar_d));
    T* dst = fp_alloc<T>(n);
    for (uint32_t i = 0; i < n; ++i) dst[i] = fp_canon(ad[a.selection[i]] / sv);
    return fp_make_dense<T, TAG>(dst, fp_copy_validity(a.validity, n), n);
}

// mod: std::fmod — NaN if b == 0 (IEEE, different from int mod0→0).
template<typename T, DrakenType TAG>
static inline VecResult float_mod(const DrakenVector& a, const DrakenVector& b) {
    if (a.length != b.length) throw std::invalid_argument("float_mod: length mismatch");
    const uint32_t n = a.length;
    const T* ad = static_cast<const T*>(a.data);
    const T* bd = static_cast<const T*>(b.data);
    T* dst = fp_alloc<T>(n);
    for (uint32_t i = 0; i < n; ++i)
        dst[i] = fp_canon(std::fmod(ad[a.selection[i]], bd[b.selection[i]]));
    return fp_make_dense<T, TAG>(dst, fp_combine_validity(a.validity, b.validity, n), n);
}

template<typename T, DrakenType TAG>
static inline VecResult float_mod_scalar(const DrakenVector& a, double scalar_d) {
    const uint32_t n = a.length;
    const T* ad = static_cast<const T*>(a.data);
    const T sv = fp_canon(static_cast<T>(scalar_d));
    T* dst = fp_alloc<T>(n);
    for (uint32_t i = 0; i < n; ++i)
        dst[i] = fp_canon(std::fmod(ad[a.selection[i]], sv));
    return fp_make_dense<T, TAG>(dst, fp_copy_validity(a.validity, n), n);
}

template<typename T, DrakenType TAG>
static inline VecResult float_neg(const DrakenVector& a) {
    const uint32_t n = a.length;
    const T* ad = static_cast<const T*>(a.data);
    T* dst = fp_alloc<T>(n);
    for (uint32_t i = 0; i < n; ++i) dst[i] = fp_canon(-ad[a.selection[i]]);
    return fp_make_dense<T, TAG>(dst, fp_copy_validity(a.validity, n), n);
}

// ===========================================================================
// GATHER — take / materialize / dictionary_encode (result stays type T).
// dictionary_encode uses bit-cast key for NaN-safe deduplication.
// ===========================================================================

template<typename T, DrakenType TAG>
static inline VecResult float_take(
    const DrakenVector& v, const int32_t* indices, uint32_t n_indices)
{
    const uint32_t n        = n_indices;
    const T*       data     = static_cast<const T*>(v.data);
    const uint8_t* src_null = v.validity;

    // Compression-preserving + compacting gather (see ops/dict_take.h).
    if (v.data_length <= n && draken_is_compressed(&v))
        return fixed_dict_compact_take<T, TAG>(v, indices, n);

    T* dst = fp_alloc<T>(n);
    uint8_t* out_null = nullptr;
    if (src_null != nullptr && n > 0u) {
        const uint32_t nb = (n + 7u) >> 3;
        out_null = static_cast<uint8_t*>(draken_malloc(nb));
        if (!out_null) { draken_free(dst); throw std::bad_alloc(); }
        std::memset(out_null, 0, nb);
    }

    for (uint32_t i = 0; i < n; ++i) {
        const uint32_t src_idx = static_cast<uint32_t>(indices[i]);
        if (!fp_row_valid(src_null, src_idx)) {
            dst[i] = T(0);
        } else {
            dst[i] = data[v.selection[src_idx]];
            if (out_null != nullptr) fp_set_valid_bit(out_null, i);
        }
    }

    out_null = fp_normalize_validity(out_null, n);

    VecResult r;
    r.data = dst; r.validity = out_null;
    r.selection = draken_identity_sel(n); r.owns_selection = false;
    r.data_length = n; r.length = n; r.type = TAG;
    r.flags = static_cast<uint8_t>(DRAKEN_SEL_IDENTITY | DRAKEN_SEL_PERMUTATION);
    return r;
}

template<typename T, DrakenType TAG>
static inline VecResult float_materialize(const DrakenVector& v) {
    const uint32_t n        = v.length;
    const T*       data     = static_cast<const T*>(v.data);
    const uint8_t* src_null = v.validity;

    T* dst = fp_alloc<T>(n);
    for (uint32_t i = 0; i < n; ++i) {
        if (src_null != nullptr && !((src_null[i >> 3] >> (i & 7)) & 1u))
            dst[i] = T(0);
        else
            dst[i] = data[v.selection[i]];
    }

    VecResult r;
    r.data = dst; r.validity = fp_copy_validity(src_null, n);
    r.selection = draken_identity_sel(n); r.owns_selection = false;
    r.data_length = n; r.length = n; r.type = TAG;
    r.flags = static_cast<uint8_t>(DRAKEN_SEL_IDENTITY | DRAKEN_SEL_PERMUTATION);
    return r;
}

template<typename T, DrakenType TAG>
static inline VecResult float_dictionary_encode(const DrakenVector& v) {
    const uint32_t n        = v.length;
    const T*       data     = static_cast<const T*>(v.data);
    const uint8_t* src_null = v.validity;

    if (n == 0) {
        T* d = fp_alloc<T>(1); d[0] = T(0);
        VecResult r;
        r.data = d; r.validity = nullptr;
        r.selection = draken_identity_sel(0); r.owns_selection = false;
        r.data_length = 0; r.length = 0; r.type = TAG; r.flags = 0;
        return r;
    }

    // Key on canonical bits: NaN deduplicates correctly since all are canonical.
    std::unordered_map<uint64_t, uint32_t> bits_to_code;
    bits_to_code.reserve(n < 256u ? n : 256u);
    std::vector<T> dict_values;

    for (uint32_t i = 0; i < n; ++i) {
        if (!fp_row_valid(src_null, i)) continue;
        T val = data[v.selection[i]];
        uint64_t key = fp_bits64(val);
        if (bits_to_code.find(key) == bits_to_code.end()) {
            bits_to_code[key] = static_cast<uint32_t>(dict_values.size());
            dict_values.push_back(val);
        }
    }

    const uint32_t dict_size = static_cast<uint32_t>(dict_values.size());

    if (dict_size == 0) {
        T* d = fp_alloc<T>(1); d[0] = T(0);
        uint8_t* out_null = nullptr;
        if (src_null != nullptr) {
            const uint32_t nb = (n + 7u) >> 3;
            out_null = static_cast<uint8_t*>(draken_malloc(nb));
            if (!out_null) { draken_free(d); throw std::bad_alloc(); }
            std::memcpy(out_null, src_null, nb);
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
        codes[i] = fp_row_valid(src_null, i)
            ? bits_to_code.at(fp_bits64(data[v.selection[i]])) : 0u;

    uint8_t* out_null = nullptr;
    if (src_null != nullptr) {
        const uint32_t nb = (n + 7u) >> 3;
        out_null = static_cast<uint8_t*>(draken_malloc(nb));
        if (!out_null) { draken_free(dict_buf); draken_free(codes); throw std::bad_alloc(); }
        std::memcpy(out_null, src_null, nb);
    }

    VecResult r;
    r.data = dict_buf; r.validity = out_null;
    r.selection = codes; r.owns_selection = true;
    r.data_length = dict_size; r.length = n; r.type = TAG; r.flags = 0;
    return r;
}

// ===========================================================================
// BETWEEN — total-order bounds check (NaN is highest).
// ===========================================================================

template<typename T, bool lo_incl, bool hi_incl>
static inline VecResult fp_between_impl(const DrakenVector& v, T lo, T hi) {
    const uint32_t n        = v.length;
    const T*       data     = static_cast<const T*>(v.data);
    const uint8_t* src_null = v.validity;

    // Per-value BETWEEN predicate (identical to the dense loop below, so the
    // shape fast paths produce byte-identical answers incl. NaN/-0).
    auto pred = [&](T val) -> bool {
        const bool lo_ok = lo_incl ? fp_total_le(lo, val) : fp_total_lt(lo, val);
        const bool hi_ok = hi_incl ? fp_total_le(val, hi) : fp_total_lt(val, hi);
        return lo_ok && hi_ok;
    };

    // Constant / dict shape fast paths (CLAUDE.md §11, approved 2026-06-11).
    if (draken_is_constant(&v))
        return cmp_constant_bool_result(pred(data[0]), src_null, n);
    if (draken_is_dict(&v)) {
        const uint32_t dl = v.data_length;
        uint8_t* db = static_cast<uint8_t*>(draken_malloc(dl));
        if (!db) throw std::bad_alloc();
        for (uint32_t k = 0; k < dl; ++k) db[k] = pred(data[k]) ? 1u : 0u;
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

    const uint32_t whole_bytes = n >> 3;

    if (src_null == nullptr) {
        for (uint32_t b = 0; b < whole_bytes; ++b) {
            const uint32_t base = b << 3;
            dst[b] = static_cast<uint8_t>(
                (static_cast<unsigned>((lo_incl ? fp_total_le(lo, data[v.selection[base+0]]) : fp_total_lt(lo, data[v.selection[base+0]])) && (hi_incl ? fp_total_le(data[v.selection[base+0]], hi) : fp_total_lt(data[v.selection[base+0]], hi))) << 0) |
                (static_cast<unsigned>((lo_incl ? fp_total_le(lo, data[v.selection[base+1]]) : fp_total_lt(lo, data[v.selection[base+1]])) && (hi_incl ? fp_total_le(data[v.selection[base+1]], hi) : fp_total_lt(data[v.selection[base+1]], hi))) << 1) |
                (static_cast<unsigned>((lo_incl ? fp_total_le(lo, data[v.selection[base+2]]) : fp_total_lt(lo, data[v.selection[base+2]])) && (hi_incl ? fp_total_le(data[v.selection[base+2]], hi) : fp_total_lt(data[v.selection[base+2]], hi))) << 2) |
                (static_cast<unsigned>((lo_incl ? fp_total_le(lo, data[v.selection[base+3]]) : fp_total_lt(lo, data[v.selection[base+3]])) && (hi_incl ? fp_total_le(data[v.selection[base+3]], hi) : fp_total_lt(data[v.selection[base+3]], hi))) << 3) |
                (static_cast<unsigned>((lo_incl ? fp_total_le(lo, data[v.selection[base+4]]) : fp_total_lt(lo, data[v.selection[base+4]])) && (hi_incl ? fp_total_le(data[v.selection[base+4]], hi) : fp_total_lt(data[v.selection[base+4]], hi))) << 4) |
                (static_cast<unsigned>((lo_incl ? fp_total_le(lo, data[v.selection[base+5]]) : fp_total_lt(lo, data[v.selection[base+5]])) && (hi_incl ? fp_total_le(data[v.selection[base+5]], hi) : fp_total_lt(data[v.selection[base+5]], hi))) << 5) |
                (static_cast<unsigned>((lo_incl ? fp_total_le(lo, data[v.selection[base+6]]) : fp_total_lt(lo, data[v.selection[base+6]])) && (hi_incl ? fp_total_le(data[v.selection[base+6]], hi) : fp_total_lt(data[v.selection[base+6]], hi))) << 6) |
                (static_cast<unsigned>((lo_incl ? fp_total_le(lo, data[v.selection[base+7]]) : fp_total_lt(lo, data[v.selection[base+7]])) && (hi_incl ? fp_total_le(data[v.selection[base+7]], hi) : fp_total_lt(data[v.selection[base+7]], hi))) << 7));
        }
        for (uint32_t i = whole_bytes << 3; i < n; ++i) {
            const T val = data[v.selection[i]];
            const bool lo_ok = lo_incl ? fp_total_le(lo, val) : fp_total_lt(lo, val);
            const bool hi_ok = hi_incl ? fp_total_le(val, hi) : fp_total_lt(val, hi);
            if (lo_ok && hi_ok) dst[i >> 3] |= static_cast<uint8_t>(1u << (i & 7));
        }
    } else {
        for (uint32_t b = 0; b < whole_bytes; ++b) {
            const uint32_t base = b << 3;
            const uint8_t m = static_cast<uint8_t>(
                (static_cast<unsigned>((lo_incl ? fp_total_le(lo, data[v.selection[base+0]]) : fp_total_lt(lo, data[v.selection[base+0]])) && (hi_incl ? fp_total_le(data[v.selection[base+0]], hi) : fp_total_lt(data[v.selection[base+0]], hi))) << 0) |
                (static_cast<unsigned>((lo_incl ? fp_total_le(lo, data[v.selection[base+1]]) : fp_total_lt(lo, data[v.selection[base+1]])) && (hi_incl ? fp_total_le(data[v.selection[base+1]], hi) : fp_total_lt(data[v.selection[base+1]], hi))) << 1) |
                (static_cast<unsigned>((lo_incl ? fp_total_le(lo, data[v.selection[base+2]]) : fp_total_lt(lo, data[v.selection[base+2]])) && (hi_incl ? fp_total_le(data[v.selection[base+2]], hi) : fp_total_lt(data[v.selection[base+2]], hi))) << 2) |
                (static_cast<unsigned>((lo_incl ? fp_total_le(lo, data[v.selection[base+3]]) : fp_total_lt(lo, data[v.selection[base+3]])) && (hi_incl ? fp_total_le(data[v.selection[base+3]], hi) : fp_total_lt(data[v.selection[base+3]], hi))) << 3) |
                (static_cast<unsigned>((lo_incl ? fp_total_le(lo, data[v.selection[base+4]]) : fp_total_lt(lo, data[v.selection[base+4]])) && (hi_incl ? fp_total_le(data[v.selection[base+4]], hi) : fp_total_lt(data[v.selection[base+4]], hi))) << 4) |
                (static_cast<unsigned>((lo_incl ? fp_total_le(lo, data[v.selection[base+5]]) : fp_total_lt(lo, data[v.selection[base+5]])) && (hi_incl ? fp_total_le(data[v.selection[base+5]], hi) : fp_total_lt(data[v.selection[base+5]], hi))) << 5) |
                (static_cast<unsigned>((lo_incl ? fp_total_le(lo, data[v.selection[base+6]]) : fp_total_lt(lo, data[v.selection[base+6]])) && (hi_incl ? fp_total_le(data[v.selection[base+6]], hi) : fp_total_lt(data[v.selection[base+6]], hi))) << 6) |
                (static_cast<unsigned>((lo_incl ? fp_total_le(lo, data[v.selection[base+7]]) : fp_total_lt(lo, data[v.selection[base+7]])) && (hi_incl ? fp_total_le(data[v.selection[base+7]], hi) : fp_total_lt(data[v.selection[base+7]], hi))) << 7));
            dst[b] = static_cast<uint8_t>(m & src_null[b]);
        }
        for (uint32_t i = whole_bytes << 3; i < n; ++i) {
            if ((src_null[i >> 3] >> (i & 7)) & 1u) {
                const T val = data[v.selection[i]];
                const bool lo_ok = lo_incl ? fp_total_le(lo, val) : fp_total_lt(lo, val);
                const bool hi_ok = hi_incl ? fp_total_le(val, hi) : fp_total_lt(val, hi);
                if (lo_ok && hi_ok) dst[i >> 3] |= static_cast<uint8_t>(1u << (i & 7));
            }
        }
    }

    VecResult r;
    r.data = dst; r.validity = out_null;
    r.selection = draken_identity_sel(n); r.owns_selection = false;
    r.data_length = n; r.length = n; r.type = DRAKEN_BOOL;
    r.flags = static_cast<uint8_t>(DRAKEN_SEL_IDENTITY | DRAKEN_SEL_PERMUTATION);
    return r;
}

template<typename T>
static inline VecResult float_between(
    const DrakenVector& v, double lo_d, double hi_d, bool lo_incl, bool hi_incl)
{
    const T lo = fp_canon(static_cast<T>(lo_d));
    const T hi = fp_canon(static_cast<T>(hi_d));
    if (lo_incl) {
        if (hi_incl) return fp_between_impl<T, true,  true >(v, lo, hi);
        else         return fp_between_impl<T, true,  false>(v, lo, hi);
    } else {
        if (hi_incl) return fp_between_impl<T, false, true >(v, lo, hi);
        else         return fp_between_impl<T, false, false>(v, lo, hi);
    }
}

// ===========================================================================
// IN_LIST — hash-based; same canonical bits → simd_hash_i64 path as hash().
// §1 EXCEPTION: hash-only, no key verification (same as int64/string).
// ===========================================================================

template<typename T>
static inline VecResult float_in_list(
    const DrakenVector& v,
    const opteryx::carchar::CarcharSet& set)
{
    const uint32_t n        = v.length;
    const T*       data     = static_cast<const T*>(v.data);
    const uint8_t* src_null = v.validity;

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
            scratch[j] = fp_bits64(data[v.selection[i + j]]);
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
// Public entry-point wrappers — type-erased to match TypeOps function pointer types.
// ===========================================================================

// --- FLOAT32 ---
static inline void     hash_float32(const DrakenVector& v, uint64_t* o, uint32_t n) { float_hash<float, DRAKEN_FLOAT32>(v, o, n); }
static inline uint32_t f32_sum(const DrakenVector& v, double* o)                     { return float_sum<float>(v, o); }
static inline uint32_t f32_min(const DrakenVector& v, double* o)                     { return float_min<float>(v, o); }
static inline uint32_t f32_max(const DrakenVector& v, double* o)                     { return float_max<float>(v, o); }
static inline VecResult f32_add(const DrakenVector& a, const DrakenVector& b)        { return float_add<float, DRAKEN_FLOAT32>(a, b); }
static inline VecResult f32_add_scalar(const DrakenVector& a, double s)              { return float_add_scalar<float, DRAKEN_FLOAT32>(a, s); }
static inline VecResult f32_sub(const DrakenVector& a, const DrakenVector& b)        { return float_sub<float, DRAKEN_FLOAT32>(a, b); }
static inline VecResult f32_sub_scalar(const DrakenVector& a, double s)              { return float_sub_scalar<float, DRAKEN_FLOAT32>(a, s); }
static inline VecResult f32_mul(const DrakenVector& a, const DrakenVector& b)        { return float_mul<float, DRAKEN_FLOAT32>(a, b); }
static inline VecResult f32_mul_scalar(const DrakenVector& a, double s)              { return float_mul_scalar<float, DRAKEN_FLOAT32>(a, s); }
static inline VecResult f32_div(const DrakenVector& a, const DrakenVector& b)        { return float_div<float, DRAKEN_FLOAT32>(a, b); }
static inline VecResult f32_div_scalar(const DrakenVector& a, double s)              { return float_div_scalar<float, DRAKEN_FLOAT32>(a, s); }
static inline VecResult f32_mod(const DrakenVector& a, const DrakenVector& b)        { return float_mod<float, DRAKEN_FLOAT32>(a, b); }
static inline VecResult f32_mod_scalar(const DrakenVector& a, double s)              { return float_mod_scalar<float, DRAKEN_FLOAT32>(a, s); }
static inline VecResult f32_neg(const DrakenVector& a)                               { return float_neg<float, DRAKEN_FLOAT32>(a); }
static inline VecResult f32_take(const DrakenVector& v, const int32_t* idx, uint32_t n) { return float_take<float, DRAKEN_FLOAT32>(v, idx, n); }

static inline void fp_copy_validity_range(uint8_t* dst, const uint8_t* src,
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
static inline VecResult float_slice(const DrakenVector& v, uint32_t start, uint32_t length) {
    const uint32_t n        = length;
    const T*       data     = static_cast<const T*>(v.data);
    const uint8_t* src_null = v.validity;

    uint8_t* out_null = nullptr;
    if (src_null != nullptr && n > 0u) {
        const uint32_t nb = (n + 7u) >> 3;
        out_null = static_cast<uint8_t*>(draken_malloc(nb));
        if (!out_null) throw std::bad_alloc();
        fp_copy_validity_range(out_null, src_null, start, n);
        out_null = fp_normalize_validity(out_null, n);
    }

    // Keep the source's dictionary when that copies strictly fewer bytes than
    // flattening it. One rule for every fixed-width family (ops/slice_shape.h);
    // the width arithmetic there makes this unreachable for types <= 4 bytes.
    VecResult kept;
    if (slice_keep_dict<T>(v, start, n, out_null, TAG, kept)) return kept;


    T* dst = fp_alloc<T>(n);

    // Physical memcpy valid ONLY when selection is identity; data_length==length
    // also admits a PERMUTATION which would silently reorder. Require IDENTITY.
    if (draken_is_dense(&v) && (v.flags & DRAKEN_SEL_IDENTITY)) {
        std::memcpy(dst, data + start, n * sizeof(T));
    } else {
        for (uint32_t i = 0; i < n; ++i)
            dst[i] = data[v.selection[start + i]];
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

static inline VecResult f32_slice(const DrakenVector& v, uint32_t s, uint32_t n) { return float_slice<float,  DRAKEN_FLOAT32>(v, s, n); }
static inline VecResult f32_materialize(const DrakenVector& v)                       { return float_materialize<float, DRAKEN_FLOAT32>(v); }
static inline VecResult f32_dictionary_encode(const DrakenVector& v)                          { return float_dictionary_encode<float, DRAKEN_FLOAT32>(v); }
static inline VecResult f32_compare_scalar(const DrakenVector& v, double s, int op)  { return float_compare_scalar<float>(v, s, op); }
static inline VecResult f32_compare_vector(const DrakenVector& a, const DrakenVector& b, int op) { return float_compare_vector<float>(a, b, op); }
static inline VecResult f32_between(const DrakenVector& v, double lo, double hi, bool li, bool hi_i) { return float_between<float>(v, lo, hi, li, hi_i); }
static inline VecResult f32_in_list(const DrakenVector& v, const opteryx::carchar::CarcharSet& s) { return float_in_list<float>(v, s); }

// --- FLOAT64 ---
static inline void     hash_float64(const DrakenVector& v, uint64_t* o, uint32_t n) { float_hash<double, DRAKEN_FLOAT64>(v, o, n); }
static inline uint32_t f64_sum(const DrakenVector& v, double* o)                     { return float_sum<double>(v, o); }
static inline uint32_t f64_min(const DrakenVector& v, double* o)                     { return float_min<double>(v, o); }
static inline uint32_t f64_max(const DrakenVector& v, double* o)                     { return float_max<double>(v, o); }
static inline VecResult f64_add(const DrakenVector& a, const DrakenVector& b)        { return float_add<double, DRAKEN_FLOAT64>(a, b); }
static inline VecResult f64_add_scalar(const DrakenVector& a, double s)              { return float_add_scalar<double, DRAKEN_FLOAT64>(a, s); }
static inline VecResult f64_sub(const DrakenVector& a, const DrakenVector& b)        { return float_sub<double, DRAKEN_FLOAT64>(a, b); }
static inline VecResult f64_sub_scalar(const DrakenVector& a, double s)              { return float_sub_scalar<double, DRAKEN_FLOAT64>(a, s); }
static inline VecResult f64_mul(const DrakenVector& a, const DrakenVector& b)        { return float_mul<double, DRAKEN_FLOAT64>(a, b); }
static inline VecResult f64_mul_scalar(const DrakenVector& a, double s)              { return float_mul_scalar<double, DRAKEN_FLOAT64>(a, s); }
static inline VecResult f64_div(const DrakenVector& a, const DrakenVector& b)        { return float_div<double, DRAKEN_FLOAT64>(a, b); }
static inline VecResult f64_div_scalar(const DrakenVector& a, double s)              { return float_div_scalar<double, DRAKEN_FLOAT64>(a, s); }
static inline VecResult f64_mod(const DrakenVector& a, const DrakenVector& b)        { return float_mod<double, DRAKEN_FLOAT64>(a, b); }
static inline VecResult f64_mod_scalar(const DrakenVector& a, double s)              { return float_mod_scalar<double, DRAKEN_FLOAT64>(a, s); }
static inline VecResult f64_neg(const DrakenVector& a)                               { return float_neg<double, DRAKEN_FLOAT64>(a); }
static inline VecResult f64_take(const DrakenVector& v, const int32_t* idx, uint32_t n) { return float_take<double, DRAKEN_FLOAT64>(v, idx, n); }
static inline VecResult f64_slice(const DrakenVector& v, uint32_t s, uint32_t n) { return float_slice<double, DRAKEN_FLOAT64>(v, s, n); }
static inline VecResult f64_materialize(const DrakenVector& v)                       { return float_materialize<double, DRAKEN_FLOAT64>(v); }
static inline VecResult f64_dictionary_encode(const DrakenVector& v)                          { return float_dictionary_encode<double, DRAKEN_FLOAT64>(v); }
static inline VecResult f64_compare_scalar(const DrakenVector& v, double s, int op)  { return float_compare_scalar<double>(v, s, op); }
static inline VecResult f64_compare_vector(const DrakenVector& a, const DrakenVector& b, int op) { return float_compare_vector<double>(a, b, op); }
static inline VecResult f64_between(const DrakenVector& v, double lo, double hi, bool li, bool hi_i) { return float_between<double>(v, lo, hi, li, hi_i); }
static inline VecResult f64_in_list(const DrakenVector& v, const opteryx::carchar::CarcharSet& s) { return float_in_list<double>(v, s); }

}} // namespace draken::ops
