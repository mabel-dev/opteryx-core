#pragma once
// draken/ops/decimal_arith.h — Scale-aware arithmetic kernels for DRAKEN_DECIMAL.
//
// Physical storage: int64 unscaled values. Logical scale and precision live in the
// LogicalType descriptor carried on VectorOwner (not on DrakenVector).
//
// Scale rules (PostgreSQL, per E.32 architect call §2.1):
//   add / sub:  result_scale = max(sa, sb)
//   mul:        result_scale = sa + sb  (raises OverflowError if sa+sb > 18)
//   div:        result_scale passed in by caller = max(sa+6, 6), capped at 18
//   mod:        result_scale = sa
//   neg:        result_scale = sa  (implicit — caller propagates descriptor)
//
// Overflow semantics (per E.32 architect call §2.2 / §2.3):
//   All overflow raises std::overflow_error — never silent wrap.
//   div/mod by zero raises std::domain_error.
//   neg(INT64_MIN) raises std::overflow_error.
//
// Division rounding: half-even (banker's rounding) per draken-boost-math memory.
//
// Null propagation: any null input → null output row (binary ops);
//   neg: result_valid[i] = a_valid[i].
//
// Access pattern: data[selection[i]] for i in [0, length) — uniform, no shape dispatch.

#include <stdint.h>
#include <string.h>
#include <stdexcept>
#include <climits>
#include "core/buffers.h"
#include "core/alloc.h"
#include "core/vector_alloc.h"
#include "ops/vec_result.h"
#include "ops/int64_arithmetic.h"   // combine_validity, copy_validity, alloc_i64

namespace draken { namespace ops {

// ---------------------------------------------------------------------------
// Powers-of-10 table — index i gives 10^i for i in [0, 18].
// All values fit in int64 (10^18 < INT64_MAX ≈ 9.22e18).
// ---------------------------------------------------------------------------
static const int64_t kDecPow10[19] = {
    1LL,
    10LL,
    100LL,
    1000LL,
    10000LL,
    100000LL,
    1000000LL,
    10000000LL,
    100000000LL,
    1000000000LL,
    10000000000LL,
    100000000000LL,
    1000000000000LL,
    10000000000000LL,
    100000000000000LL,
    1000000000000000LL,
    10000000000000000LL,
    100000000000000000LL,
    1000000000000000000LL,
};

// ---------------------------------------------------------------------------
// int128 helpers
// ---------------------------------------------------------------------------

// Build a dense-identity VecResult with type DRAKEN_DECIMAL.
static inline VecResult make_decimal_result(
    int64_t* data, uint8_t* validity, uint32_t n)
{
    VecResult r;
    r.data           = data;
    r.validity       = validity;
    r.selection      = draken_identity_sel(n);
    r.owns_selection = false;
    r.data_length    = n;
    r.length         = n;
    r.type           = DRAKEN_DECIMAL;
    r.flags          = static_cast<uint8_t>(DRAKEN_SEL_IDENTITY | DRAKEN_SEL_PERMUTATION);
    return r;
}

// Safely multiply v by 10, writing result to out. Returns false on int128 overflow.
static inline bool i128_mul10(__int128 v, __int128& out) {
    // INT128_MAX ≈ 1.7e38; overflow when |v| > INT128_MAX / 10 ≈ 1.7e37.
    // Compute limit as a constant __int128 expression.
    static const __int128 LIMIT =
        ((__int128)9223372036854775807LL << 63) | (__int128)0x7FFFFFFFFFFFFFFFuLL;
    // More precisely: INT128_MAX = 2^127 - 1.  LIMIT = INT128_MAX / 10.
    // We compute: if |v| > LIMIT, return false.
    const __int128 limit = ((__int128)1 << 126) / 5; // approx 2^126/5 ≈ 1.70e37
    if (v > limit || v < -limit) return false;
    out = v * 10;
    return true;
}

// Scale v by 10^e, writing result to out. Returns false if int128 would overflow.
// e must be in [0, 38].
static inline bool i128_scale(__int128 v, int e, __int128& out) {
    out = v;
    for (int k = 0; k < e; ++k) {
        if (!i128_mul10(out, out)) return false;
    }
    return true;
}

// True if v fits in a signed int64.
static inline bool i128_fits_i64(__int128 v) {
    return v >= (__int128)INT64_MIN && v <= (__int128)INT64_MAX;
}

// Half-even (banker's) rounding of floor(num / den).
// den > 0 required. num may be negative.
static inline __int128 half_even_div(__int128 num, __int128 den) {
    __int128 q = num / den;   // C++ truncates toward zero
    __int128 r = num % den;   // remainder has sign of num
    if (r == 0) return q;

    const __int128 abs_r   = (r < 0) ? -r : r;
    const __int128 two_r   = abs_r * 2;  // safe: abs_r < den so 2*abs_r < 2*den, no overflow for den≤INT64

    if (two_r < den) {
        return q;           // |r| < den/2: round toward zero
    } else if (two_r > den) {
        return (num < 0) ? q - 1 : q + 1;  // |r| > den/2: round away from zero
    } else {
        // |r| == den/2: tie — round to even
        if ((q % 2) == 0) return q;
        return (num < 0) ? q - 1 : q + 1;
    }
}

// ---------------------------------------------------------------------------
// ADD: result_scale = max(sa, sb)
// Lower-scale operand aligned by multiplying by 10^(result_scale - its_scale).
// int128 intermediate; raises on overflow.
// ---------------------------------------------------------------------------
static inline VecResult dec_add(
    const DrakenVector& a, uint8_t sa,
    const DrakenVector& b, uint8_t sb)
{
    if (a.length != b.length)
        throw std::invalid_argument("dec_add: length mismatch");
    const uint32_t n = a.length;
    const int64_t* ad = static_cast<const int64_t*>(a.data);
    const int64_t* bd = static_cast<const int64_t*>(b.data);
    int64_t* dst = alloc_i64(n);

    const int delta_a = (sa < sb) ? (int)sb - sa : 0;
    const int delta_b = (sb < sa) ? (int)sa - sb : 0;

    for (uint32_t i = 0; i < n; ++i) {
        __int128 av, bv;
        if (!i128_scale((__int128)ad[a.selection[i]], delta_a, av))
            throw std::overflow_error(
                "dec_add: operand overflows int128 during scale alignment");
        if (!i128_scale((__int128)bd[b.selection[i]], delta_b, bv))
            throw std::overflow_error(
                "dec_add: operand overflows int128 during scale alignment");
        const __int128 rv = av + bv;
        if (!i128_fits_i64(rv))
            throw std::overflow_error("dec_add: result overflows DECIMAL(18) storage");
        dst[i] = static_cast<int64_t>(rv);
    }
    return make_decimal_result(dst, combine_validity(a.validity, b.validity, n), n);
}

// ---------------------------------------------------------------------------
// SUB: result_scale = max(sa, sb)
// ---------------------------------------------------------------------------
static inline VecResult dec_sub(
    const DrakenVector& a, uint8_t sa,
    const DrakenVector& b, uint8_t sb)
{
    if (a.length != b.length)
        throw std::invalid_argument("dec_sub: length mismatch");
    const uint32_t n = a.length;
    const int64_t* ad = static_cast<const int64_t*>(a.data);
    const int64_t* bd = static_cast<const int64_t*>(b.data);
    int64_t* dst = alloc_i64(n);

    const int delta_a = (sa < sb) ? (int)sb - sa : 0;
    const int delta_b = (sb < sa) ? (int)sa - sb : 0;

    for (uint32_t i = 0; i < n; ++i) {
        __int128 av, bv;
        if (!i128_scale((__int128)ad[a.selection[i]], delta_a, av))
            throw std::overflow_error(
                "dec_sub: operand overflows int128 during scale alignment");
        if (!i128_scale((__int128)bd[b.selection[i]], delta_b, bv))
            throw std::overflow_error(
                "dec_sub: operand overflows int128 during scale alignment");
        const __int128 rv = av - bv;
        if (!i128_fits_i64(rv))
            throw std::overflow_error("dec_sub: result overflows DECIMAL(18) storage");
        dst[i] = static_cast<int64_t>(rv);
    }
    return make_decimal_result(dst, combine_validity(a.validity, b.validity, n), n);
}

// ---------------------------------------------------------------------------
// MUL: result_scale = sa + sb.
// Raises if sa + sb > 18 (result scale overflows DECIMAL(18) storage).
// int128 multiply; raises if result doesn't fit in int64.
// ---------------------------------------------------------------------------
static inline VecResult dec_mul(
    const DrakenVector& a, uint8_t sa,
    const DrakenVector& b, uint8_t sb)
{
    if (a.length != b.length)
        throw std::invalid_argument("dec_mul: length mismatch");
    if ((int)sa + (int)sb > 18)
        throw std::overflow_error(
            "dec_mul: result scale (sa+sb) would exceed 18; use lower-scale operands");
    const uint32_t n = a.length;
    const int64_t* ad = static_cast<const int64_t*>(a.data);
    const int64_t* bd = static_cast<const int64_t*>(b.data);
    int64_t* dst = alloc_i64(n);

    for (uint32_t i = 0; i < n; ++i) {
        const __int128 rv =
            (__int128)ad[a.selection[i]] * (__int128)bd[b.selection[i]];
        if (!i128_fits_i64(rv))
            throw std::overflow_error("dec_mul: result overflows int64 storage");
        dst[i] = static_cast<int64_t>(rv);
    }
    return make_decimal_result(dst, combine_validity(a.validity, b.validity, n), n);
}

// ---------------------------------------------------------------------------
// DIV: result_scale = max(sa+6, 6) capped at 18 — computed and passed in by caller.
//
// Scaling formula: result_unscaled = round(a_unscaled * 10^e / b_unscaled)
//   where e = sb - sa + result_scale.
//   Proof: a_real/b_real = (a_u/10^sa) / (b_u/10^sb) = a_u * 10^(sb-sa) / b_u.
//   result_unscaled (at result_scale) = a_real/b_real * 10^result_scale
//                                     = a_u * 10^(sb-sa+result_scale) / b_u.
//   e = sb - sa + result_scale ≥ sb - sa + sa + 6 = sb + 6 ≥ 6 > 0 always
//   (for uncapped result_scale; with cap at 18 the guarantee still holds since
//   result_scale ≥ 6 ≥ 0 and any valid pair sa,sb yields e ≥ 0).
//
// Rounding: half-even.
// Raises on div-by-zero, int128 overflow, or int64 result overflow.
// ---------------------------------------------------------------------------
static inline VecResult dec_div(
    const DrakenVector& a, uint8_t sa,
    const DrakenVector& b, uint8_t sb,
    uint8_t result_scale)
{
    if (a.length != b.length)
        throw std::invalid_argument("dec_div: length mismatch");
    const uint32_t n = a.length;
    const int64_t* ad = static_cast<const int64_t*>(a.data);
    const int64_t* bd = static_cast<const int64_t*>(b.data);
    int64_t* dst = alloc_i64(n);

    const int e = (int)sb - (int)sa + (int)result_scale;
    // e ≥ 0 by design (see header); e ≤ 18+18 = 36.

    for (uint32_t i = 0; i < n; ++i) {
        // Skip computation for null rows — combine_validity marks output null.
        const bool a_null = a.validity && !((a.validity[i >> 3] >> (i & 7)) & 1u);
        const bool b_null = b.validity && !((b.validity[i >> 3] >> (i & 7)) & 1u);
        if (a_null || b_null) { dst[i] = 0; continue; }

        const int64_t bv = bd[b.selection[i]];
        if (bv == 0)
            throw std::domain_error("dec_div: division by zero");
        __int128 num;
        if (!i128_scale((__int128)ad[a.selection[i]], e, num))
            throw std::overflow_error("dec_div: numerator overflows int128 during scaling");
        const __int128 rv = half_even_div(num, (__int128)bv);
        if (!i128_fits_i64(rv))
            throw std::overflow_error("dec_div: result overflows int64 storage");
        dst[i] = static_cast<int64_t>(rv);
    }
    return make_decimal_result(dst, combine_validity(a.validity, b.validity, n), n);
}

// ---------------------------------------------------------------------------
// MOD: result_scale = sa.
// Aligns b to scale sa before computing remainder (C truncation semantics).
//   If sa >= sb: b_aligned = b_unscaled * 10^(sa - sb)
//   If sa <  sb: b_aligned = b_unscaled / 10^(sb - sa)  (truncate)
// result = a_unscaled % b_aligned, at scale sa.
// Raises on mod-by-zero or int128/int64 overflow.
// ---------------------------------------------------------------------------
static inline VecResult dec_mod(
    const DrakenVector& a, uint8_t sa,
    const DrakenVector& b, uint8_t sb)
{
    if (a.length != b.length)
        throw std::invalid_argument("dec_mod: length mismatch");
    const uint32_t n = a.length;
    const int64_t* ad = static_cast<const int64_t*>(a.data);
    const int64_t* bd = static_cast<const int64_t*>(b.data);
    int64_t* dst = alloc_i64(n);

    for (uint32_t i = 0; i < n; ++i) {
        // Skip computation for null rows — combine_validity marks output null.
        const bool a_null = a.validity && !((a.validity[i >> 3] >> (i & 7)) & 1u);
        const bool b_null = b.validity && !((b.validity[i >> 3] >> (i & 7)) & 1u);
        if (a_null || b_null) { dst[i] = 0; continue; }

        const int64_t bv = bd[b.selection[i]];
        __int128 b_aligned;
        if (sa >= sb) {
            const int delta = (int)sa - (int)sb;
            if (!i128_scale((__int128)bv, delta, b_aligned))
                throw std::overflow_error(
                    "dec_mod: b operand overflows int128 during scale alignment");
        } else {
            const int delta = (int)sb - (int)sa;
            const int64_t div = (delta <= 18) ? kDecPow10[delta] : kDecPow10[18];
            b_aligned = (__int128)bv / (__int128)div;
        }
        if (b_aligned == 0)
            throw std::domain_error("dec_mod: modulus is zero after scale alignment");
        const __int128 rv = (__int128)ad[a.selection[i]] % b_aligned;
        if (!i128_fits_i64(rv))
            throw std::overflow_error("dec_mod: result overflows int64 storage");
        dst[i] = static_cast<int64_t>(rv);
    }
    return make_decimal_result(dst, combine_validity(a.validity, b.validity, n), n);
}

// ---------------------------------------------------------------------------
// NEG: unary negation. result_scale = sa (propagated by caller).
// Raises if any input row == INT64_MIN (negation would overflow int64).
// ---------------------------------------------------------------------------
static inline VecResult dec_neg(const DrakenVector& a) {
    const uint32_t n = a.length;
    const int64_t* ad = static_cast<const int64_t*>(a.data);
    int64_t* dst = alloc_i64(n);

    for (uint32_t i = 0; i < n; ++i) {
        const int64_t v = ad[a.selection[i]];
        if (v == INT64_MIN)
            throw std::overflow_error(
                "dec_neg: cannot negate INT64_MIN (would overflow DECIMAL(18) storage)");
        dst[i] = -v;
    }
    return make_decimal_result(dst, copy_validity(a.validity, n), n);
}

}} // namespace draken::ops
