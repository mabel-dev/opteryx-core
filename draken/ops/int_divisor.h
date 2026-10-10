#pragma once
// draken/ops/int_divisor.h — signed INT64 division by a divisor that is fixed for
// a whole batch but not known at compile time (a query literal: `col % 7`,
// `col DIV n`, TIME_BUCKET's period).
//
// The compiler already strength-reduces a COMPILE-TIME divisor to multiply-high
// + shift; a runtime divisor costs one hardware divide per row (x86 64-bit idiv
// ~10-90 cycles depending on the core; AArch64 sdiv ~7-9). This precomputes the
// same magic multiplier once per batch (Granlund & Montgomery 1994; Hacker's
// Delight 2nd ed. §10-1, "magic" for signed W=64) so each row is one smulh plus
// a few ALU ops. Written here, not vendored (§4).
//
// Contract: i64_divisor(d) requires |d| >= 2 (d may be INT64_MIN). Callers route
// d in {-1, 0, 1} through the uniform path, which owns the divide-by-zero error
// and INT64_MIN / -1 overflow. Results are bit-identical to C `/` and `%`
// (truncation toward zero, remainder takes the dividend's sign).
//
// Constant-divisor dispatch is a §11 shape specialisation, ratified by the
// architect 2026-10-10 for integer DIV/MOD and TIME_BUCKET. It must never change
// the answer: the uniform path stays the correctness contract.

#include <stdint.h>

namespace draken { namespace ops {

struct I64Divisor {
    int64_t  d;        // the divisor itself (|d| >= 2)
    int64_t  magic;    // M
    uint64_t add_sel;  // all-ones when the dividend is added/subtracted after mulhi
    uint64_t add_neg;  // all-ones when that correction is a SUBTRACTION
    int      shift;    // s
};

static inline I64Divisor i64_divisor(int64_t d) noexcept {
    const uint64_t two63 = 1ull << 63;
    const uint64_t ud  = static_cast<uint64_t>(d);
    const uint64_t ad  = (d < 0) ? (0ull - ud) : ud;              // |d|, exact for INT64_MIN
    const uint64_t t   = two63 + (ud >> 63);
    const uint64_t anc = t - 1u - t % ad;                          // |nc|
    int p = 63;
    uint64_t q1 = two63 / anc, r1 = two63 - q1 * anc;
    uint64_t q2 = two63 / ad,  r2 = two63 - q2 * ad;
    uint64_t delta;
    do {
        ++p;
        q1 <<= 1; r1 <<= 1; if (r1 >= anc) { ++q1; r1 -= anc; }
        q2 <<= 1; r2 <<= 1; if (r2 >= ad)  { ++q2; r2 -= ad; }
        delta = ad - r2;
    } while (q1 < delta || (q1 == delta && r1 == 0u));
    I64Divisor v;
    v.d = d;
    const uint64_t um = q2 + 1u;
    v.magic = static_cast<int64_t>(d < 0 ? (0ull - um) : um);
    v.shift = p - 64;
    // Hacker's Delight: q += n when d > 0 && M < 0; q -= n when d < 0 && M > 0.
    const bool add = (d > 0 && v.magic < 0);
    const bool sub = (d < 0 && v.magic > 0);
    v.add_sel = (add || sub) ? ~0ull : 0ull;
    v.add_neg = sub ? ~0ull : 0ull;
    return v;
}

// n / d, truncated toward zero. Wrapping arithmetic is done in uint64_t (no UB).
static inline int64_t i64_div_by(const I64Divisor& v, int64_t n) noexcept {
    const int64_t hi = static_cast<int64_t>(
        (static_cast<__int128>(v.magic) * static_cast<__int128>(n)) >> 64);
    const uint64_t un   = static_cast<uint64_t>(n);
    const uint64_t corr = ((un ^ v.add_neg) - v.add_neg) & v.add_sel;   // +n, -n or 0
    int64_t q = static_cast<int64_t>(static_cast<uint64_t>(hi) + corr);
    q >>= v.shift;                                                       // arithmetic
    return q + static_cast<int64_t>(static_cast<uint64_t>(q) >> 63);     // +1 if negative
}

// n % d (sign of the dividend, as C `%`). |q * d| <= |n| so nothing overflows.
static inline int64_t i64_mod_by(const I64Divisor& v, int64_t n) noexcept {
    const int64_t q = i64_div_by(v, n);
    return static_cast<int64_t>(static_cast<uint64_t>(n) -
                                static_cast<uint64_t>(q) * static_cast<uint64_t>(v.d));
}

// floor(n / d) (toward −∞) — temporal bucketing.
static inline int64_t i64_floor_div_by(const I64Divisor& v, int64_t n) noexcept {
    const int64_t q = i64_div_by(v, n);
    const int64_t r = static_cast<int64_t>(static_cast<uint64_t>(n) -
                                           static_cast<uint64_t>(q) * static_cast<uint64_t>(v.d));
    return q - static_cast<int64_t>((r != 0) & ((r ^ v.d) < 0));
}

}} // namespace draken::ops
