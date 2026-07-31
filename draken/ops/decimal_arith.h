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
#include <new>        // std::bad_alloc / placement new — not reliably pulled in by <stdexcept> on stricter libc++
#include "core/buffers.h"
#include "core/alloc.h"
#include "core/vector_alloc.h"
#include "ops/vec_result.h"
#include "ops/int64_arithmetic.h"   // combine_validity, copy_validity, alloc_i64
#include "ops/int64_compare.h"      // cmp_alloc_bool_buf, cmp_copy_validity, cmp_and_validity

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

// ---------------------------------------------------------------------------
// int128-backed DECIMAL (DRAKEN_DECIMAL128) — the "correct-but-scalar" tier for
// logical DECIMAL with p > 18 (doc 06). Physical `data` is __int128 unscaled.
// ---------------------------------------------------------------------------

// Allocate an int128 buffer of n elements (16 B/elem) via the draken allocator.
static inline __int128* alloc_i128(uint32_t n) {
    if (n == 0) n = 1;  // always non-null pointer
    __int128* p = static_cast<__int128*>(draken_malloc(n * sizeof(__int128)));
    if (!p) throw std::bad_alloc();
    return p;
}

// Build a dense-identity VecResult with type DRAKEN_DECIMAL128.
static inline VecResult make_decimal128_result(
    __int128* data, uint8_t* validity, uint32_t n)
{
    VecResult r;
    r.data           = data;
    r.validity       = validity;
    r.selection      = draken_identity_sel(n);
    r.owns_selection = false;
    r.data_length    = n;
    r.length         = n;
    r.type           = DRAKEN_DECIMAL128;
    r.flags          = static_cast<uint8_t>(DRAKEN_SEL_IDENTITY | DRAKEN_SEL_PERMUTATION);
    return r;
}

// Widen an int64-backed vector (DRAKEN_DECIMAL or DRAKEN_INT64 — both store int64
// unscaled values) to a dense int128 DRAKEN_DECIMAL128 VecResult. Resolves the
// selection (uniform data[selection[i]] access, §11) and copies the per-logical-row
// validity bitmap unchanged (validity is indexed by logical row, so it carries over).
// The unscaled value is widened verbatim — scale is unchanged, the caller supplies it
// to the dec128_* kernel. Caller owns the returned data/validity buffers.
static inline VecResult widen_i64_to_dec128(const DrakenVector& v) {
    const uint32_t n = v.length;
    const int64_t* sd = static_cast<const int64_t*>(v.data);
    __int128* dst = alloc_i128(n);
    for (uint32_t i = 0; i < n; ++i)
        dst[i] = static_cast<__int128>(sd[v.selection[i]]);
    return make_decimal128_result(dst, copy_validity(v.validity, n), n);
}

// Widen an INT8/INT16/INT32 vector to a dense int64 buffer (sign-extending),
// resolving the source shape via uniform data[selection[i]] access (§11). Brings a
// narrow-int operand up to the tier the DECIMAL(int64)×INT64 and DECIMAL128
// promotion kernels already read (they stride int64/int128 only) — this is the
// widening the closure used to do in Python before calling those same kernels.
// Caller owns the returned data/validity buffers.
static inline VecResult widen_narrow_int_to_i64(const DrakenVector& v) {
    const uint32_t n = v.length;
    int64_t* dst = alloc_i64(n);
    switch (v.type) {
        case DRAKEN_INT8: {
            const int8_t* sd = static_cast<const int8_t*>(v.data);
            for (uint32_t i = 0; i < n; ++i) dst[i] = static_cast<int64_t>(sd[v.selection[i]]);
            break;
        }
        case DRAKEN_INT16: {
            const int16_t* sd = static_cast<const int16_t*>(v.data);
            for (uint32_t i = 0; i < n; ++i) dst[i] = static_cast<int64_t>(sd[v.selection[i]]);
            break;
        }
        case DRAKEN_INT32: {
            const int32_t* sd = static_cast<const int32_t*>(v.data);
            for (uint32_t i = 0; i < n; ++i) dst[i] = static_cast<int64_t>(sd[v.selection[i]]);
            break;
        }
        default:
            throw std::invalid_argument("widen_narrow_int_to_i64: expected INT8/16/32");
    }
    VecResult r;
    r.data           = dst;
    r.validity       = copy_validity(v.validity, n);
    r.selection      = draken_identity_sel(n);
    r.owns_selection = false;
    r.data_length    = n;
    r.length         = n;
    r.type           = DRAKEN_INT64;
    r.flags          = static_cast<uint8_t>(DRAKEN_SEL_IDENTITY | DRAKEN_SEL_PERMUTATION);
    return r;
}

// E33 — widen a DRAKEN_UINT64 vector to int128 DRAKEN_DECIMAL128. Zero-extends
// (not sign-extends): a UINT64 value >= 2^63 is always a large POSITIVE __int128,
// never negative — this is the matrix's escape valve for UINT64 paired with a
// signed integer (no fixed-width signed type holds the full UINT64 range, but
// __int128 does, with room to spare). Mirrors widen_i64_to_dec128 exactly except
// for the source type and the zero- vs sign-extension.
static inline VecResult widen_u64_to_dec128(const DrakenVector& v) {
    const uint32_t n = v.length;
    const uint64_t* sd = static_cast<const uint64_t*>(v.data);
    __int128* dst = alloc_i128(n);
    for (uint32_t i = 0; i < n; ++i)
        dst[i] = static_cast<__int128>(sd[v.selection[i]]);
    return make_decimal128_result(dst, copy_validity(v.validity, n), n);
}

// Safely multiply v by 10, writing result to out. Returns false on int128 overflow.
static inline bool i128_mul10(__int128 v, __int128& out) {
    // INT128_MAX = 2^127 - 1 ≈ 1.7e38; overflow when |v| > INT128_MAX / 10 ≈ 1.7e37.
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

// Half-even (banker's) rounding of num / den. Sign-robust: either operand may
// be negative. Rounds the *magnitude* |num|/|den| half-to-even, then reapplies
// the result sign = sign(num) XOR sign(den). This equals half-even on the real
// value because evenness is preserved under negation. Mirrors dec128_div, which
// is correct precisely because it works on unsigned magnitudes.
static inline __int128 half_even_div(__int128 num, __int128 den) {
    const bool neg = (num < 0) ^ (den < 0);
    // Negation is safe: num fits int128 per the i128_scale overflow guard (and
    // is never INT128_MIN — see dec_div), and |den| ≤ INT64_MAX by construction.
    const __int128 unum = (num < 0) ? -num : num;
    const __int128 uden = (den < 0) ? -den : den;

    __int128 q = unum / uden;   // q ≥ 0
    const __int128 r = unum % uden;   // 0 ≤ r < uden
    if (r != 0) {
        const __int128 two_r = r * 2;  // safe: r < uden ≤ INT64_MAX, no overflow
        if (two_r > uden || (two_r == uden && (q & 1) != 0))
            q += 1;            // round away from zero (in magnitude)
    }
    return neg ? -q : q;
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
// DECIMAL128 ADD / SUB — int128-backed operands and result (DRAKEN_DECIMAL128).
// result_scale = max(sa, sb); lower-scale operand aligned by *10^delta. Unlike the
// int64 path there is no fits-i64 truncation — the result stays int128; only a
// genuine int128 overflow (two ~10^38 operands) raises.
// ---------------------------------------------------------------------------
static inline VecResult dec128_add(
    const DrakenVector& a, uint8_t sa,
    const DrakenVector& b, uint8_t sb)
{
    if (a.length != b.length)
        throw std::invalid_argument("dec128_add: length mismatch");
    const uint32_t n = a.length;
    const __int128* ad = static_cast<const __int128*>(a.data);
    const __int128* bd = static_cast<const __int128*>(b.data);
    __int128* dst = alloc_i128(n);

    const int delta_a = (sa < sb) ? (int)sb - sa : 0;
    const int delta_b = (sb < sa) ? (int)sa - sb : 0;

    for (uint32_t i = 0; i < n; ++i) {
        __int128 av, bv;
        if (!i128_scale(ad[a.selection[i]], delta_a, av))
            throw std::overflow_error("dec128_add: operand overflows int128 during scale alignment");
        if (!i128_scale(bd[b.selection[i]], delta_b, bv))
            throw std::overflow_error("dec128_add: operand overflows int128 during scale alignment");
        const __int128 rv = av + bv;
        // signed add overflow: operands same sign, result differs.
        if (((av ^ rv) & (bv ^ rv)) < 0)
            throw std::overflow_error("dec128_add: result overflows int128 (DECIMAL128) storage");
        dst[i] = rv;
    }
    return make_decimal128_result(dst, combine_validity(a.validity, b.validity, n), n);
}

static inline VecResult dec128_sub(
    const DrakenVector& a, uint8_t sa,
    const DrakenVector& b, uint8_t sb)
{
    if (a.length != b.length)
        throw std::invalid_argument("dec128_sub: length mismatch");
    const uint32_t n = a.length;
    const __int128* ad = static_cast<const __int128*>(a.data);
    const __int128* bd = static_cast<const __int128*>(b.data);
    __int128* dst = alloc_i128(n);

    const int delta_a = (sa < sb) ? (int)sb - sa : 0;
    const int delta_b = (sb < sa) ? (int)sa - sb : 0;

    for (uint32_t i = 0; i < n; ++i) {
        __int128 av, bv;
        if (!i128_scale(ad[a.selection[i]], delta_a, av))
            throw std::overflow_error("dec128_sub: operand overflows int128 during scale alignment");
        if (!i128_scale(bd[b.selection[i]], delta_b, bv))
            throw std::overflow_error("dec128_sub: operand overflows int128 during scale alignment");
        const __int128 rv = av - bv;
        // signed sub overflow: operands differ in sign, result differs from minuend.
        if (((av ^ bv) & (av ^ rv)) < 0)
            throw std::overflow_error("dec128_sub: result overflows int128 (DECIMAL128) storage");
        dst[i] = rv;
    }
    return make_decimal128_result(dst, combine_validity(a.validity, b.validity, n), n);
}

// 128×128 → 256 unsigned multiply (schoolbook over 64-bit limbs): the product is
// hi·2^128 + lo. There is no native __int256, so the wide intermediate is carried in
// two unsigned __int128 limbs. Used by dec128_mul to detect overflow before narrowing
// the result back to int128 storage.
static inline void umul128_256(unsigned __int128 a, unsigned __int128 b,
                               unsigned __int128& hi, unsigned __int128& lo) {
    const uint64_t a0 = (uint64_t)a, a1 = (uint64_t)(a >> 64);
    const uint64_t b0 = (uint64_t)b, b1 = (uint64_t)(b >> 64);
    const unsigned __int128 p00 = (unsigned __int128)a0 * b0;
    const unsigned __int128 p01 = (unsigned __int128)a0 * b1;
    const unsigned __int128 p10 = (unsigned __int128)a1 * b0;
    const unsigned __int128 p11 = (unsigned __int128)a1 * b1;
    const unsigned __int128 mid = p01 + p10;
    const uint64_t mid_carry = (mid < p01) ? 1u : 0u;          // u128 add carry-out (bit 128)
    const unsigned __int128 lo_part = p00 + (mid << 64);
    const uint64_t lo_carry = (lo_part < p00) ? 1u : 0u;
    lo = lo_part;
    hi = p11 + (mid >> 64) + ((unsigned __int128)mid_carry << 64) + lo_carry;
}

// ---------------------------------------------------------------------------
// DECIMAL128 MUL: result_scale = sa + sb (raises if > 38). The full 128×128
// product is computed in a 256-bit intermediate; the result must fit signed int128
// (|result| < 2^127), else raises — no silent truncation.
// ---------------------------------------------------------------------------
static inline VecResult dec128_mul(
    const DrakenVector& a, uint8_t sa,
    const DrakenVector& b, uint8_t sb)
{
    if (a.length != b.length)
        throw std::invalid_argument("dec128_mul: length mismatch");
    if ((int)sa + (int)sb > 38)
        throw std::overflow_error(
            "dec128_mul: result scale (sa+sb) would exceed 38; use lower-scale operands");
    const uint32_t n = a.length;
    const __int128* ad = static_cast<const __int128*>(a.data);
    const __int128* bd = static_cast<const __int128*>(b.data);
    __int128* dst = alloc_i128(n);

    for (uint32_t i = 0; i < n; ++i) {
        const __int128 av = ad[a.selection[i]];
        const __int128 bv = bd[b.selection[i]];
        const bool neg = (av < 0) ^ (bv < 0);
        // |operand| < 10^38 < 2^127, so negation never hits INT128_MIN.
        const unsigned __int128 ua = (av < 0) ? (unsigned __int128)(-av) : (unsigned __int128)av;
        const unsigned __int128 ub = (bv < 0) ? (unsigned __int128)(-bv) : (unsigned __int128)bv;
        unsigned __int128 hi, lo;
        umul128_256(ua, ub, hi, lo);
        // Result fits signed int128 iff the high limb is zero and lo's top bit is clear.
        if (hi != 0 || (lo >> 127) != 0)
            throw std::overflow_error("dec128_mul: result overflows int128 (DECIMAL128) storage");
        dst[i] = neg ? -static_cast<__int128>(lo) : static_cast<__int128>(lo);
    }
    return make_decimal128_result(dst, combine_validity(a.validity, b.validity, n), n);
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

// ---------------------------------------------------------------------------
// COMPARE: scale-aware magnitude comparison (the previously-deferred "pt2"
// work). Operands may have different scales — they are aligned to the common
// scale = max(sa, sb) in int128 by cross-scaling, then compared. This is exact
// for every scale pair and never requires the literal to be storable at the
// column scale (so `l_discount > 0.055` against a scale-2 column compares
// magnitudes correctly rather than rejecting the literal).
//
// An INT64 operand is a scale-0 decimal: the caller passes scale 0 and its
// raw int64 data, which these kernels read identically to a decimal payload.
//
// Result is a bit-packed DRAKEN_BOOL VecResult (LSB-first), dense-identity
// selection — matching int64_compare.h's contract and null semantics
// (null input row → result bit 0, validity bit 0).
//
// op codes: 0=eq 1=ne 2=gt 3=ge 4=lt 5=le
// ---------------------------------------------------------------------------
static inline bool dec_cmp_apply(int op, __int128 a, __int128 b) noexcept {
    switch (op) {
        case 0: return a == b;
        case 1: return a != b;
        case 2: return a >  b;
        case 3: return a >= b;
        case 4: return a <  b;
        default: return a <= b;  // 5 = le
    }
}

// 10^e as int128, for e in [0, 38]. e is bounded by the scale difference
// (≤ 18), so this never overflows int128 here.
static inline __int128 dec_pow10_i128(int e) noexcept {
    __int128 p = 1;
    for (int k = 0; k < e; ++k) p *= 10;
    return p;
}

// ---------------------------------------------------------------------------
// DECIMAL128 DIV / MOD — int128-backed operands and result (DRAKEN_DECIMAL128).
// These live below dec_pow10_i128 because they use it (and umul128_256 above).
// ---------------------------------------------------------------------------

// Unsigned 256-bit (hi:lo) divided by 128-bit divisor d, via restoring binary
// long division (256 iterations — the "correct-but-scalar" tier; there is no
// native 256/128 divide). Writes the quotient's low 128 bits to quo and the
// remainder (< d) to rem. Returns false if the true quotient needs more than
// 128 bits (overflow). REQUIRES d != 0 and d < 2^127 — guaranteed for DECIMAL128
// unscaled values (|v| < 10^38 < 2^127), which keeps the running remainder below
// 2^128 after each `(rem << 1) | bit` step.
static inline bool udivmod_256_by_128(
    unsigned __int128 hi, unsigned __int128 lo, unsigned __int128 d,
    unsigned __int128& quo, unsigned __int128& rem) {
    unsigned __int128 q = 0;
    unsigned __int128 r = 0;
    bool overflow = false;
    for (int i = 255; i >= 0; --i) {
        const unsigned bit = (i >= 128)
            ? (unsigned)((hi >> (i - 128)) & 1)
            : (unsigned)((lo >> i) & 1);
        r = (r << 1) | bit;          // r < d < 2^127 ⟹ (r<<1)|bit < 2^128, fits u128
        if (r >= d) {
            r -= d;
            if (i >= 128) overflow = true;                 // quotient bit ≥ 128
            else q |= ((unsigned __int128)1 << i);
        }
    }
    quo = q;
    rem = r;
    return !overflow;
}

// DIV: result_scale passed in by caller (max(sa+6,6) capped at 38).
//   result_unscaled = round(|a| * 10^e / |b|), e = sb - sa + result_scale.
// The numerator |a|·10^e can need up to 256 bits, carried via umul128_256, then
// reduced by the 256/128 divide above. Half-even rounding. Raises on div-by-zero,
// on e outside [0,38] (numerator scaling would need >38 extra digits — beyond the
// 256-bit intermediate; an honest scalar-tier limit, not a silent truncation), and
// on int128 result overflow.
static inline VecResult dec128_div(
    const DrakenVector& a, uint8_t sa,
    const DrakenVector& b, uint8_t sb,
    uint8_t result_scale)
{
    if (a.length != b.length)
        throw std::invalid_argument("dec128_div: length mismatch");
    const uint32_t n = a.length;
    const __int128* ad = static_cast<const __int128*>(a.data);
    const __int128* bd = static_cast<const __int128*>(b.data);
    __int128* dst = alloc_i128(n);

    const int e = (int)sb - (int)sa + (int)result_scale;
    if (e < 0 || e > 38)
        throw std::overflow_error(
            "dec128_div: numerator scale exponent out of range (>38 digits); "
            "operand scales too extreme for the int128 divide");
    const unsigned __int128 pe = (unsigned __int128)dec_pow10_i128(e);  // fits int128 (e≤38)

    for (uint32_t i = 0; i < n; ++i) {
        const bool a_null = a.validity && !((a.validity[i >> 3] >> (i & 7)) & 1u);
        const bool b_null = b.validity && !((b.validity[i >> 3] >> (i & 7)) & 1u);
        if (a_null || b_null) { dst[i] = 0; continue; }

        const __int128 av = ad[a.selection[i]];
        const __int128 bv = bd[b.selection[i]];
        if (bv == 0)
            throw std::domain_error("dec128_div: division by zero");

        const bool neg = (av < 0) ^ (bv < 0);
        // |operand| < 10^38 < 2^127, so negation never hits INT128_MIN.
        const unsigned __int128 ua = (av < 0) ? (unsigned __int128)(-av) : (unsigned __int128)av;
        const unsigned __int128 ub = (bv < 0) ? (unsigned __int128)(-bv) : (unsigned __int128)bv;

        unsigned __int128 hi, lo;
        umul128_256(ua, pe, hi, lo);                 // numerator = |a| * 10^e (256-bit)

        unsigned __int128 q, r;
        if (!udivmod_256_by_128(hi, lo, ub, q, r))
            throw std::overflow_error("dec128_div: result overflows int128 (DECIMAL128) storage");

        // half-even rounding: r < |b| < 2^127 ⟹ 2*r < 2^128 fits u128.
        const unsigned __int128 two_r = r << 1;
        if (two_r > ub || (two_r == ub && (q & 1) != 0))
            q += 1;

        if ((q >> 127) != 0)                         // must fit signed int128
            throw std::overflow_error("dec128_div: result overflows int128 (DECIMAL128) storage");
        dst[i] = neg ? -static_cast<__int128>(q) : static_cast<__int128>(q);
    }
    return make_decimal128_result(dst, combine_validity(a.validity, b.validity, n), n);
}

// MOD: result_scale = sa. b aligned to scale sa (C truncation semantics), then
// `a % b_aligned`. Native int128 modulo — the result fits int128 (|r| < |b_aligned|),
// so no 256-bit intermediate is needed. Raises on mod-by-zero or int128 overflow
// during b's scale alignment.
static inline VecResult dec128_mod(
    const DrakenVector& a, uint8_t sa,
    const DrakenVector& b, uint8_t sb)
{
    if (a.length != b.length)
        throw std::invalid_argument("dec128_mod: length mismatch");
    const uint32_t n = a.length;
    const __int128* ad = static_cast<const __int128*>(a.data);
    const __int128* bd = static_cast<const __int128*>(b.data);
    __int128* dst = alloc_i128(n);

    for (uint32_t i = 0; i < n; ++i) {
        const bool a_null = a.validity && !((a.validity[i >> 3] >> (i & 7)) & 1u);
        const bool b_null = b.validity && !((b.validity[i >> 3] >> (i & 7)) & 1u);
        if (a_null || b_null) { dst[i] = 0; continue; }

        const __int128 bv = bd[b.selection[i]];
        __int128 b_aligned;
        if (sa >= sb) {
            if (!i128_scale(bv, (int)sa - (int)sb, b_aligned))
                throw std::overflow_error(
                    "dec128_mod: b operand overflows int128 during scale alignment");
        } else {
            const int delta = (int)sb - (int)sa;
            const __int128 div = dec_pow10_i128(delta <= 38 ? delta : 38);
            b_aligned = bv / div;
        }
        if (b_aligned == 0)
            throw std::domain_error("dec128_mod: modulus is zero after scale alignment");
        dst[i] = ad[a.selection[i]] % b_aligned;     // sign of dividend (a)
    }
    return make_decimal128_result(dst, combine_validity(a.validity, b.validity, n), n);
}

// E33 — scale-0 truncating integer divide, for the UINT64×INT64 promotion path
// ONLY (see binop_dispatch.cpp's int_arith_op UINT64/INT64 branch): both
// operands there are always freshly-widened int64/uint64 values at scale 0,
// never a real DECIMAL column, so no scale alignment is needed or attempted.
// Deliberately NOT general DECIMAL128 INT_DIVIDE — dec128_div computes true,
// scale-expanding decimal division (wrong semantics for a truncating integer
// op). Follows the established C-truncation-toward-zero, divide-by-zero->0
// integer convention (i64_div, fixed_int_ops.h) rather than dec128_div's
// raise-on-zero decimal convention.
static inline VecResult dec128_int_divide(
    const DrakenVector& a, const DrakenVector& b)
{
    if (a.length != b.length)
        throw std::invalid_argument("dec128_int_divide: length mismatch");
    const uint32_t n = a.length;
    const __int128* ad = static_cast<const __int128*>(a.data);
    const __int128* bd = static_cast<const __int128*>(b.data);
    __int128* dst = alloc_i128(n);
    for (uint32_t i = 0; i < n; ++i) {
        const __int128 bv = bd[b.selection[i]];
        dst[i] = (bv == 0) ? 0 : (ad[a.selection[i]] / bv);   // truncates toward zero
    }
    return make_decimal128_result(dst, combine_validity(a.validity, b.validity, n), n);
}

// E33 — scale-0 truncating integer modulo. Same scope/caller/convention notes
// as dec128_int_divide above (mod-by-zero -> 0, sign of dividend — matches
// i64_mod, NOT dec128_mod's raise-on-zero decimal convention).
static inline VecResult dec128_int_mod(
    const DrakenVector& a, const DrakenVector& b)
{
    if (a.length != b.length)
        throw std::invalid_argument("dec128_int_mod: length mismatch");
    const uint32_t n = a.length;
    const __int128* ad = static_cast<const __int128*>(a.data);
    const __int128* bd = static_cast<const __int128*>(b.data);
    __int128* dst = alloc_i128(n);
    for (uint32_t i = 0; i < n; ++i) {
        const __int128 bv = bd[b.selection[i]];
        dst[i] = (bv == 0) ? 0 : (ad[a.selection[i]] % bv);   // sign of dividend
    }
    return make_decimal128_result(dst, combine_validity(a.validity, b.validity, n), n);
}

static inline VecResult dec_compare_scalar(
    const DrakenVector& a, uint8_t sa,
    __int128 b_unscaled, uint8_t sb, int op)
{
    const uint32_t n        = a.length;
    const int64_t* ad       = static_cast<const int64_t*>(a.data);
    const uint8_t* src_null = a.validity;

    uint8_t* dst = cmp_alloc_bool_buf(n);  // zero-initialised
    uint8_t* out_null = nullptr;
    if (src_null != nullptr) {
        try { out_null = cmp_copy_validity(src_null, n); }
        catch (...) { draken_free(dst); throw; }
    }

    const int cs = (sa >= sb) ? (int)sa : (int)sb;
    const __int128 fa = dec_pow10_i128(cs - (int)sa);
    const __int128 b_aligned = b_unscaled * dec_pow10_i128(cs - (int)sb);

    for (uint32_t i = 0; i < n; ++i) {
        // Null row → leave result bit 0 (out_null already marks it null).
        if (src_null != nullptr && !((src_null[i >> 3] >> (i & 7)) & 1u))
            continue;
        const __int128 av = (__int128)ad[a.selection[i]] * fa;
        if (dec_cmp_apply(op, av, b_aligned))
            dst[i >> 3] |= static_cast<uint8_t>(1u << (i & 7));
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

static inline VecResult dec_compare_vector(
    const DrakenVector& a, uint8_t sa,
    const DrakenVector& b, uint8_t sb, int op)
{
    if (a.length != b.length)
        throw std::invalid_argument("dec_compare_vector: length mismatch");
    const uint32_t n  = a.length;
    const int64_t* ad = static_cast<const int64_t*>(a.data);
    const int64_t* bd = static_cast<const int64_t*>(b.data);

    uint8_t* dst      = cmp_alloc_bool_buf(n);
    uint8_t* out_null = cmp_and_validity(a.validity, b.validity, n);

    const int cs = (sa >= sb) ? (int)sa : (int)sb;
    const __int128 fa = dec_pow10_i128(cs - (int)sa);
    const __int128 fb = dec_pow10_i128(cs - (int)sb);

    for (uint32_t i = 0; i < n; ++i) {
        if (out_null != nullptr && !((out_null[i >> 3] >> (i & 7)) & 1u))
            continue;
        const __int128 av = (__int128)ad[a.selection[i]] * fa;
        const __int128 bv = (__int128)bd[b.selection[i]] * fb;
        if (dec_cmp_apply(op, av, bv))
            dst[i >> 3] |= static_cast<uint8_t>(1u << (i & 7));
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

// ---------------------------------------------------------------------------
// DECIMAL128 comparison — int128 operands, scale-aware and EXACT for every scale
// pair via a 256-bit cross-scaled magnitude compare (no overflow, no runtime throw).
// Compares a·10^da vs b·10^db where da/db bring both to the common scale.
// ---------------------------------------------------------------------------
static inline int dec128_three_way_scaled(
    __int128 a, int da, __int128 b, int db) noexcept {
    if (a == 0 && b == 0) return 0;
    const bool an = a < 0, bn = b < 0;
    if (an != bn) return an ? -1 : 1;  // negative < non-negative
    const unsigned __int128 ua = an ? (unsigned __int128)(-a) : (unsigned __int128)a;
    const unsigned __int128 ub = bn ? (unsigned __int128)(-b) : (unsigned __int128)b;
    unsigned __int128 hiA, loA, hiB, loB;
    umul128_256(ua, (unsigned __int128)dec_pow10_i128(da), hiA, loA);
    umul128_256(ub, (unsigned __int128)dec_pow10_i128(db), hiB, loB);
    const int mag = (hiA != hiB) ? (hiA < hiB ? -1 : 1)
                  : (loA != loB) ? (loA < loB ? -1 : 1) : 0;
    return an ? -mag : mag;  // both negative: larger magnitude ⇒ smaller value
}

static inline bool dec128_cmp_apply3(int op, int cmp) noexcept {
    switch (op) {
        case 0: return cmp == 0;
        case 1: return cmp != 0;
        case 2: return cmp >  0;
        case 3: return cmp >= 0;
        case 4: return cmp <  0;
        default: return cmp <= 0;  // 5 = le
    }
}

static inline VecResult dec128_compare_scalar(
    const DrakenVector& a, uint8_t sa,
    __int128 b_unscaled, uint8_t sb, int op)
{
    const uint32_t n        = a.length;
    const __int128* ad      = static_cast<const __int128*>(a.data);
    const uint8_t* src_null = a.validity;
    uint8_t* dst = cmp_alloc_bool_buf(n);
    uint8_t* out_null = nullptr;
    if (src_null != nullptr) {
        try { out_null = cmp_copy_validity(src_null, n); }
        catch (...) { draken_free(dst); throw; }
    }
    const int cs = (sa >= sb) ? (int)sa : (int)sb;
    const int da = cs - (int)sa, db = cs - (int)sb;
    for (uint32_t i = 0; i < n; ++i) {
        if (src_null != nullptr && !((src_null[i >> 3] >> (i & 7)) & 1u)) continue;
        if (dec128_cmp_apply3(op, dec128_three_way_scaled(ad[a.selection[i]], da, b_unscaled, db)))
            dst[i >> 3] |= static_cast<uint8_t>(1u << (i & 7));
    }
    VecResult r;
    r.data = dst; r.validity = out_null; r.selection = draken_identity_sel(n);
    r.owns_selection = false; r.data_length = n; r.length = n; r.type = DRAKEN_BOOL;
    r.flags = static_cast<uint8_t>(DRAKEN_SEL_IDENTITY | DRAKEN_SEL_PERMUTATION);
    return r;
}

static inline VecResult dec128_compare_vector(
    const DrakenVector& a, uint8_t sa,
    const DrakenVector& b, uint8_t sb, int op)
{
    if (a.length != b.length)
        throw std::invalid_argument("dec128_compare_vector: length mismatch");
    const uint32_t n   = a.length;
    const __int128* ad = static_cast<const __int128*>(a.data);
    const __int128* bd = static_cast<const __int128*>(b.data);
    uint8_t* dst      = cmp_alloc_bool_buf(n);
    uint8_t* out_null = cmp_and_validity(a.validity, b.validity, n);
    const int cs = (sa >= sb) ? (int)sa : (int)sb;
    const int da = cs - (int)sa, db = cs - (int)sb;
    for (uint32_t i = 0; i < n; ++i) {
        if (out_null != nullptr && !((out_null[i >> 3] >> (i & 7)) & 1u)) continue;
        if (dec128_cmp_apply3(op, dec128_three_way_scaled(
                ad[a.selection[i]], da, bd[b.selection[i]], db)))
            dst[i >> 3] |= static_cast<uint8_t>(1u << (i & 7));
    }
    VecResult r;
    r.data = dst; r.validity = out_null; r.selection = draken_identity_sel(n);
    r.owns_selection = false; r.data_length = n; r.length = n; r.type = DRAKEN_BOOL;
    r.flags = static_cast<uint8_t>(DRAKEN_SEL_IDENTITY | DRAKEN_SEL_PERMUTATION);
    return r;
}

}} // namespace draken::ops
