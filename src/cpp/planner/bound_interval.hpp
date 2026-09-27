// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// See the License at http://www.apache.org/licenses/LICENSE-2.0
// Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

// src/cpp/planner/bound_interval.hpp — the values and intervals predicate bound
// derivation reasons over (native plan graph Q8, M-d2).
//
// A bound is a literal's value as the planner has always held it: an integer, a
// float, a byte string, or - carried through unchanged, never computed on - a
// boolean, a DECIMAL or anything else a literal can be. Integers are int128 so
// the derivation's arithmetic (a pre-image divides, a widening subtracts, a
// temporal rule multiplies by ticks) is exact where Python's was; a result that
// would leave int128 DECLINES the rule, which costs a read, never a row.
//
// Comparisons are Python's: an int and a float compare EXACTLY (never through a
// lossy conversion), a NaN compares false with everything, byte strings compare
// bytewise.

#pragma once

#include <cmath>
#include <cstdint>
#include <cstring>
#include <optional>
#include <string>
#include <vector>

#include "planner/expr_arena.hpp"

namespace opteryx::planner {

enum class BKind : uint8_t {
    INT,      // int (int128)
    FLOAT,    // float
    BYTES,    // bytes
    BOOL,     // bool - carried, never computed on
    OPAQUE,   // Decimal, tuple, ... - carried as its literal, never computed on
};

struct BVal {
    BKind kind = BKind::INT;
    __int128 i = 0;
    double d = 0.0;
    std::string bytes;
    // The literal this value IS, when it is one unchanged (always for BOOL and
    // OPAQUE): pruning ordinalizes and compares the literal's own value.
    const LiteralValue* literal = nullptr;

    static BVal of_int(__int128 v) { BVal b; b.kind = BKind::INT; b.i = v; return b; }
    static BVal of_float(double v) { BVal b; b.kind = BKind::FLOAT; b.d = v; return b; }
    static BVal of_bytes(std::string v) { BVal b; b.kind = BKind::BYTES; b.bytes = std::move(v); return b; }

    bool number() const { return kind == BKind::INT || kind == BKind::FLOAT; }
};

// A literal's value as a bound; nullopt for SQL NULL (`col > NULL` bounds
// nothing).
inline std::optional<BVal> bval_of_literal(const LiteralValue& lit) {
    BVal b;
    b.literal = &lit;
    switch (lit.tag) {
        case LITERAL_NULL:
        case LITERAL_NONE:
            return std::nullopt;
        case LITERAL_INT64:
            b.kind = BKind::INT;
            b.i = lit.i;
            return b;
        case LITERAL_UINT64:
            b.kind = BKind::INT;
            b.i = static_cast<__int128>(static_cast<uint64_t>(lit.i));
            return b;
        case LITERAL_DOUBLE:
            b.kind = BKind::FLOAT;
            b.d = lit.d;
            return b;
        case LITERAL_BYTES:
            b.kind = BKind::BYTES;
            b.bytes = lit.bytes;
            return b;
        case LITERAL_BOOL:
            b.kind = BKind::BOOL;
            b.i = lit.i != 0;
            return b;
        default:
            b.kind = BKind::OPAQUE;
            return b;
    }
}

// --- exact comparison ------------------------------------------------------

// -1 / 0 / 1, or 2 when unordered (a NaN is involved).
inline int cmp_int_double(__int128 i, double d) {
    if (std::isnan(d)) return 2;
    static const double two127 = std::ldexp(1.0, 127);
    if (d >= two127) return -1;
    if (d < -two127) return 1;
    const double fl = std::floor(d);
    const __int128 fi = static_cast<__int128>(fl);
    if (i < fi) return -1;
    if (i > fi) return 1;
    return d > fl ? -1 : 0;
}

// Order of two numbers, or of two byte strings; 2 when unordered. The caller
// has checked the pair is comparable (numbers with numbers, bytes with bytes,
// bools with bools).
inline int cmp(const BVal& a, const BVal& b) {
    if (a.number() && b.number()) {
        if (a.kind == BKind::INT && b.kind == BKind::INT) return a.i < b.i ? -1 : (a.i > b.i ? 1 : 0);
        if (a.kind == BKind::INT) return cmp_int_double(a.i, b.d);
        if (b.kind == BKind::INT) {
            const int r = cmp_int_double(b.i, a.d);
            return r == 2 ? 2 : -r;
        }
        if (std::isnan(a.d) || std::isnan(b.d)) return 2;
        return a.d < b.d ? -1 : (a.d > b.d ? 1 : 0);
    }
    if (a.kind == BKind::BYTES && b.kind == BKind::BYTES) {
        const int r = a.bytes.compare(b.bytes);
        return r < 0 ? -1 : (r > 0 ? 1 : 0);
    }
    if (a.kind == BKind::BOOL && b.kind == BKind::BOOL) return a.i < b.i ? -1 : (a.i > b.i ? 1 : 0);
    return 2;
}

inline bool lt(const BVal& a, const BVal& b) { return cmp(a, b) == -1; }
inline bool eq(const BVal& a, const BVal& b) { return cmp(a, b) == 0; }

// Python's `_comparable_pair`: two numbers, or two byte strings.
inline bool comparable_pair(const BVal& a, const BVal& b) {
    return (a.number() && b.number()) || (a.kind == BKind::BYTES && b.kind == BKind::BYTES);
}

// --- intervals ---------------------------------------------------------------

// (lower, lower_closed, upper, upper_closed); an absent bound is unbounded on
// that side and its flag is meaningless.
struct Interval {
    std::optional<BVal> lo;
    bool lo_closed = true;
    std::optional<BVal> hi;
    bool hi_closed = true;
};

inline bool numeric_interval(const Interval& iv) {
    return (!iv.lo || iv.lo->number()) && (!iv.hi || iv.hi->number());
}

// --- exact arithmetic (Python semantics; nullopt = outside int128, declined) ----

inline double as_double(const BVal& v) { return v.kind == BKind::INT ? static_cast<double>(v.i) : v.d; }

inline std::optional<BVal> sub(const BVal& a, const BVal& b) {
    if (a.kind == BKind::INT && b.kind == BKind::INT) {
        __int128 r;
        if (__builtin_sub_overflow(a.i, b.i, &r)) return std::nullopt;
        return BVal::of_int(r);
    }
    return BVal::of_float(as_double(a) - as_double(b));
}

inline std::optional<BVal> add(const BVal& a, const BVal& b) {
    if (a.kind == BKind::INT && b.kind == BKind::INT) {
        __int128 r;
        if (__builtin_add_overflow(a.i, b.i, &r)) return std::nullopt;
        return BVal::of_int(r);
    }
    return BVal::of_float(as_double(a) + as_double(b));
}

inline std::optional<BVal> mul(const BVal& a, const BVal& b) {
    if (a.kind == BKind::INT && b.kind == BKind::INT) {
        __int128 r;
        if (__builtin_mul_overflow(a.i, b.i, &r)) return std::nullopt;
        return BVal::of_int(r);
    }
    return BVal::of_float(as_double(a) * as_double(b));
}

inline std::optional<BVal> negate(const BVal& a) {
    if (a.kind == BKind::INT) {
        if (a.i == (static_cast<__int128>(1) << 126) * -2) return std::nullopt;
        return BVal::of_int(-a.i);
    }
    return BVal::of_float(-a.d);
}

// Python floor division of ints.
inline __int128 int_floordiv(__int128 n, __int128 d) {
    __int128 q = n / d;
    if ((n % d != 0) && ((n < 0) != (d < 0))) --q;
    return q;
}

inline bool exactly_representable(__int128 n, __int128 d) {
    const __int128 limit = static_cast<__int128>(1) << 53;
    return (n < 0 ? -n : n) < limit && (d < 0 ? -d : d) < limit;
}

// Largest value <= n/d - a SAFE lower bound (predicate_bounds._floor_div).
inline std::optional<BVal> floor_div(const BVal& n, const BVal& d) {
    if (n.kind == BKind::INT && d.kind == BKind::INT) {
        if (d.i == 0) return std::nullopt;
        if (n.i % d.i == 0) return BVal::of_int(n.i / d.i);
        if (!exactly_representable(n.i, d.i)) return BVal::of_int(int_floordiv(n.i, d.i));
    }
    const double q = as_double(n) / as_double(d);
    return BVal::of_float(std::nextafter(q, -INFINITY));
}

// Smallest value >= n/d - a SAFE upper bound (predicate_bounds._ceil_div).
inline std::optional<BVal> ceil_div(const BVal& n, const BVal& d) {
    if (n.kind == BKind::INT && d.kind == BKind::INT) {
        if (d.i == 0) return std::nullopt;
        if (n.i % d.i == 0) return BVal::of_int(n.i / d.i);
        if (!exactly_representable(n.i, d.i)) return BVal::of_int(-int_floordiv(-n.i, d.i));
    }
    const double q = as_double(n) / as_double(d);
    return BVal::of_float(std::nextafter(q, INFINITY));
}

// math.floor / math.ceil to an int; nullopt for a NaN, an infinity or a value
// outside int128 (where Python's answer is an integer this cannot hold).
inline std::optional<BVal> to_int(double v, bool ceiling) {
    if (!std::isfinite(v)) return std::nullopt;
    const double r = ceiling ? std::ceil(v) : std::floor(v);
    if (std::fabs(r) >= std::ldexp(1.0, 126)) return std::nullopt;
    return BVal::of_int(static_cast<__int128>(r));
}

inline std::optional<BVal> floor_int(const BVal& v) {
    return v.kind == BKind::INT ? std::optional<BVal>(v) : to_int(v.d, false);
}

inline std::optional<BVal> ceil_int(const BVal& v) {
    return v.kind == BKind::INT ? std::optional<BVal>(v) : to_int(v.d, true);
}

// --- interval rules ----------------------------------------------------------

// Interval on x given `iv` holds for y = multiplier * x + addend; bounds move
// OUTWARD. nullopt: declined.
inline std::optional<Interval> affine_preimage(const Interval& iv, const BVal& multiplier, const BVal& addend) {
    const BVal zero = BVal::of_int(0);
    if (eq(multiplier, zero)) return std::nullopt;
    std::optional<BVal> low, high;
    if (iv.lo) { low = sub(*iv.lo, addend); if (!low) return std::nullopt; }
    if (iv.hi) { high = sub(*iv.hi, addend); if (!high) return std::nullopt; }
    Interval out;
    if (lt(zero, multiplier)) {
        if (low) { out.lo = floor_div(*low, multiplier); if (!out.lo) return std::nullopt; }
        if (high) { out.hi = ceil_div(*high, multiplier); if (!out.hi) return std::nullopt; }
        out.lo_closed = iv.lo_closed;
        out.hi_closed = iv.hi_closed;
        return out;
    }
    if (low) { out.hi = ceil_div(*low, multiplier); if (!out.hi) return std::nullopt; }
    if (high) { out.lo = floor_div(*high, multiplier); if (!out.lo) return std::nullopt; }
    out.lo_closed = iv.hi_closed;
    out.hi_closed = iv.lo_closed;
    return out;
}

inline std::optional<Interval> widen(const Interval& iv, const BVal& margin) {
    Interval out;
    if (iv.lo) { out.lo = sub(*iv.lo, margin); if (!out.lo) return std::nullopt; }
    if (iv.hi) { out.hi = add(*iv.hi, margin); if (!out.hi) return std::nullopt; }
    out.lo_closed = true;
    out.hi_closed = true;
    return out;
}

// y = FLOOR(x): inclusive low, EXCLUSIVE high.
inline std::optional<Interval> floor_preimage(const Interval& iv) {
    Interval out;
    const BVal one = BVal::of_int(1);
    if (iv.lo) {
        if (iv.lo_closed) {
            out.lo = ceil_int(*iv.lo);
        } else {
            auto f = floor_int(*iv.lo);
            if (!f) return std::nullopt;
            out.lo = add(*f, one);
        }
        if (!out.lo) return std::nullopt;
    }
    if (iv.hi) {
        if (iv.hi_closed) {
            auto f = floor_int(*iv.hi);
            if (!f) return std::nullopt;
            out.hi = add(*f, one);
        } else {
            out.hi = ceil_int(*iv.hi);
        }
        if (!out.hi) return std::nullopt;
    }
    out.lo_closed = true;
    out.hi_closed = false;
    return out;
}

// y = CEILING(x): EXCLUSIVE low, inclusive high.
inline std::optional<Interval> ceiling_preimage(const Interval& iv) {
    Interval out;
    const BVal one = BVal::of_int(1);
    if (iv.lo) {
        if (iv.lo_closed) {
            auto c = ceil_int(*iv.lo);
            if (!c) return std::nullopt;
            out.lo = sub(*c, one);
        } else {
            out.lo = floor_int(*iv.lo);
        }
        if (!out.lo) return std::nullopt;
    }
    if (iv.hi) {
        if (iv.hi_closed) {
            out.hi = floor_int(*iv.hi);
        } else {
            auto c = ceil_int(*iv.hi);
            if (!c) return std::nullopt;
            out.hi = sub(*c, one);
        }
        if (!out.hi) return std::nullopt;
    }
    out.lo_closed = false;
    out.hi_closed = true;
    return out;
}

// y = ABS(x): only an upper bound carries information.
inline std::optional<Interval> abs_preimage(const Interval& iv) {
    if (!iv.hi || !iv.hi->number()) return std::nullopt;
    if (lt(*iv.hi, BVal::of_int(0))) return std::nullopt;
    Interval out;
    out.lo = negate(*iv.hi);
    if (!out.lo) return std::nullopt;
    out.lo_closed = iv.hi_closed;
    out.hi = iv.hi;
    out.hi_closed = iv.hi_closed;
    return out;
}

// y = SIGN(x), whose range is {-1, 0, 1}.
inline std::optional<Interval> sign_preimage(const Interval& iv) {
    auto admits = [&](int point) {
        const BVal p = BVal::of_int(point);
        if (iv.lo) {
            if (lt(p, *iv.lo) || (eq(p, *iv.lo) && !iv.lo_closed)) return false;
        }
        if (iv.hi) {
            if (lt(*iv.hi, p) || (eq(p, *iv.hi) && !iv.hi_closed)) return false;
        }
        return true;
    };
    const bool negative = admits(-1), zero = admits(0), positive = admits(1);
    if (!(negative || zero || positive)) return std::nullopt;
    Interval out;
    if (negative) {
        out.lo_closed = true;
    } else {
        out.lo = BVal::of_int(0);
        out.lo_closed = zero;
    }
    if (positive) {
        out.hi_closed = true;
    } else {
        out.hi = BVal::of_int(0);
        out.hi_closed = zero;
    }
    return out;
}

// --- byte strings ------------------------------------------------------------

inline bool is_ascii(const std::string& s) {
    for (unsigned char c : s) {
        if (c >= 0x80) return false;
    }
    return true;
}

// Smallest byte string strictly greater than every string starting with `v`;
// nullopt when none exists (empty, all 0x7F) or `v` is not ASCII.
inline std::optional<std::string> text_successor(const std::string& v) {
    if (!is_ascii(v)) return std::nullopt;
    std::string s = v;
    while (!s.empty() && static_cast<unsigned char>(s.back()) >= 0x7F) s.pop_back();
    if (s.empty()) return std::nullopt;
    s.back() = static_cast<char>(static_cast<unsigned char>(s.back()) + 1);
    return s;
}

// [prefix, successor(prefix)) - every string starting with `prefix`.
inline std::optional<Interval> prefix_interval(const BVal& prefix) {
    if (prefix.kind != BKind::BYTES || prefix.bytes.empty()) return std::nullopt;
    Interval out;
    out.lo = BVal::of_bytes(prefix.bytes);
    out.lo_closed = true;
    auto successor = text_successor(prefix.bytes);
    if (successor) {
        out.hi = BVal::of_bytes(*successor);
        out.hi_closed = false;
    }
    return out;
}

// y = x[:width] (a left truncation).
inline std::optional<Interval> truncating_preimage(const Interval& iv, int64_t width) {
    if (iv.lo && iv.lo->kind != BKind::BYTES) return std::nullopt;
    if (iv.hi && iv.hi->kind != BKind::BYTES) return std::nullopt;
    Interval out;
    out.lo = iv.lo;
    out.lo_closed = iv.lo_closed;
    if (iv.hi) {
        const std::string& upper = iv.hi->bytes;
        auto successor = text_successor(upper.substr(0, static_cast<size_t>(std::min<int64_t>(width, static_cast<int64_t>(upper.size())))));
        if (successor) {
            out.hi = BVal::of_bytes(*successor);
            out.hi_closed = false;
        }
    }
    if (!out.lo && !out.hi) return std::nullopt;
    return out;
}

// Tightest single interval containing every one of `arms` (callers guarantee
// the bounds are mutually comparable).
inline Interval hull(const std::vector<Interval>& arms) {
    Interval out;
    if (arms.empty()) return out;
    bool all_lo = true, all_hi = true;
    for (const Interval& a : arms) {
        all_lo = all_lo && a.lo.has_value();
        all_hi = all_hi && a.hi.has_value();
    }
    if (all_lo) {
        const BVal* lower = &*arms[0].lo;
        for (const Interval& a : arms) {
            if (lt(*a.lo, *lower)) lower = &*a.lo;   // min(): the first minimal
        }
        bool closed = false;
        for (const Interval& a : arms) {
            if (eq(*a.lo, *lower) && a.lo_closed) closed = true;
        }
        out.lo = *lower;
        out.lo_closed = closed;
    }
    if (all_hi) {
        const BVal* upper = &*arms[0].hi;
        for (const Interval& a : arms) {
            if (lt(*upper, *a.hi)) upper = &*a.hi;   // max(): the first maximal
        }
        bool closed = false;
        for (const Interval& a : arms) {
            if (eq(*a.hi, *upper) && a.hi_closed) closed = true;
        }
        out.hi = *upper;
        out.hi_closed = closed;
    }
    return out;
}

}  // namespace opteryx::planner
