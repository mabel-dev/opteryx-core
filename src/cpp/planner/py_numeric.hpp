// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// See the License at http://www.apache.org/licenses/LICENSE-2.0
// Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

// src/cpp/planner/py_numeric.hpp — Python's numeric semantics, natively.
//
// Planner arithmetic ported from Python must give Python's answer to the bit:
// the estimates it feeds are compared across the port and must not move. These
// are the operations where C++'s obvious spelling differs from Python's:
//   - `a / b` of integers is the exact quotient correctly rounded, which a
//     double division of the converted operands is not once they pass 2^53;
//   - `float(bytes)` has its own grammar (ASCII whitespace, PEP 515
//     underscores, inf / infinity / nan) and is locale-independent;
//   - `float(Decimal)` is the correctly rounded value of the exact decimal;
//   - `max(a, b)` / `min(a, b)` return the FIRST argument on a tie or when a
//     comparison is unordered (NaN);
//   - `bytes.decode("utf-8")` refuses overlongs, surrogates and > U+10FFFF.
// Code including this must be compiled without FMA contraction
// (ESTIMATOR_FP_FLAGS): Python never fuses a*b+c.

#pragma once

#include <cmath>
#include <cstdint>
#include <stdexcept>
#include <string>
#include <vector>

#include "fast_float/fast_float.h"

namespace opteryx::planner::py {

inline int bit_length(unsigned __int128 x) {
    int n = 0;
    while (x != 0) {
        x >>= 1;
        ++n;
    }
    return n;
}

// `a / b` for integers: the exact quotient, correctly rounded (half to even).
inline double true_divide(__int128 a, __int128 b) {
    if (b == 0) throw std::domain_error("true_divide: division by zero");
    const bool negative = (a < 0) != (b < 0);
    unsigned __int128 ua = a < 0 ? static_cast<unsigned __int128>(-a) : static_cast<unsigned __int128>(a);
    unsigned __int128 ub = b < 0 ? static_cast<unsigned __int128>(-b) : static_cast<unsigned __int128>(b);
    if (ua == 0) return negative ? -0.0 : 0.0;
    constexpr unsigned __int128 kExact = static_cast<unsigned __int128>(1) << 53;
    if (ua < kExact && ub < kExact) {
        const double q = static_cast<double>(ua) / static_cast<double>(ub);
        return negative ? -q : q;
    }
    // Scale so the integer quotient holds 54 or 55 bits, then round to 53.
    const int shift = 54 - (bit_length(ua) - bit_length(ub));
    if (shift > 0) {
        ua <<= shift;
    } else if (shift < 0) {
        ub <<= -shift;
    }
    unsigned __int128 q = ua / ub;
    const bool sticky = (ua % ub) != 0;
    const int extra = bit_length(q) - 53;
    unsigned __int128 mantissa = q >> extra;
    const unsigned __int128 dropped = q & ((static_cast<unsigned __int128>(1) << extra) - 1);
    const unsigned __int128 half = static_cast<unsigned __int128>(1) << (extra - 1);
    if (dropped > half || (dropped == half && (sticky || (mantissa & 1)))) mantissa += 1;
    const double result = std::ldexp(static_cast<double>(mantissa), extra - shift);
    return negative ? -result : result;
}

// `int(x)` for a finite double: truncation toward zero.
inline int64_t int_of(double x) { return static_cast<int64_t>(x); }

// `max(a, b)` / `min(a, b)` over floats.
inline double max(double a, double b) { return b > a ? b : a; }
inline double min(double a, double b) { return b < a ? b : a; }

namespace numeric_detail {

inline bool ascii_space(char c) {
    return c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == '\x0b' || c == '\x0c';
}
inline bool digit(char c) { return c >= '0' && c <= '9'; }
inline char lower(char c) { return (c >= 'A' && c <= 'Z') ? static_cast<char>(c - 'A' + 'a') : c; }

inline bool equals_folded(const std::string& s, size_t from, const char* word) {
    size_t k = 0;
    for (; word[k] != '\0'; ++k) {
        if (from + k >= s.size() || lower(s[from + k]) != word[k]) return false;
    }
    return from + k == s.size();
}

}  // namespace numeric_detail

// `float(value)` for a bytes `value`: false where Python raises ValueError.
inline bool float_from_bytes(const std::string& value, double& out) {
    using namespace numeric_detail;
    size_t begin = 0;
    size_t end = value.size();
    while (begin < end && ascii_space(value[begin])) ++begin;
    while (end > begin + 1 && ascii_space(value[end - 1])) --end;
    // Underscores: only between two digits; removed before parsing.
    std::string s;
    s.reserve(end - begin);
    char prev = '\0';
    for (size_t p = begin; p < end; ++p) {
        const char c = value[p];
        if (c == '_') {
            if (!digit(prev)) return false;
        } else {
            if (prev == '_' && !digit(c)) return false;
            s.push_back(c);
        }
        prev = c;
    }
    if (prev == '_') return false;
    if (s.empty()) return false;

    size_t pos = 0;
    bool negative = false;
    if (s[pos] == '+' || s[pos] == '-') {
        negative = s[pos] == '-';
        ++pos;
    }
    if (equals_folded(s, pos, "inf") || equals_folded(s, pos, "infinity")) {
        out = negative ? -INFINITY : INFINITY;
        return true;
    }
    if (equals_folded(s, pos, "nan")) {
        out = negative ? -std::fabs(NAN) : std::fabs(NAN);
        return true;
    }
    // digits ['.' digits] [(e|E) [sign] digits], at least one mantissa digit
    const size_t body = pos;
    size_t mantissa_digits = 0;
    while (pos < s.size() && digit(s[pos])) ++pos, ++mantissa_digits;
    if (pos < s.size() && s[pos] == '.') {
        ++pos;
        while (pos < s.size() && digit(s[pos])) ++pos, ++mantissa_digits;
    }
    if (mantissa_digits == 0) return false;
    if (pos < s.size() && (s[pos] == 'e' || s[pos] == 'E')) {
        ++pos;
        if (pos < s.size() && (s[pos] == '+' || s[pos] == '-')) ++pos;
        size_t exponent_digits = 0;
        while (pos < s.size() && digit(s[pos])) ++pos, ++exponent_digits;
        if (exponent_digits == 0) return false;
    }
    if (pos != s.size()) return false;
    double parsed = 0.0;
    const auto result = fast_float::from_chars(s.data() + body, s.data() + s.size(), parsed);
    // out of range is not an error to Python: it is inf, or zero
    if (result.ptr != s.data() + s.size()) return false;
    if (result.ec != std::errc() && result.ec != std::errc::result_out_of_range) return false;
    out = negative ? -parsed : parsed;
    return true;
}

// `float(Decimal)` for a finite decimal given as its unscaled value (the two's
// complement halves of an int128) and exponent: the exact value, correctly
// rounded.
inline double decimal_to_double(int64_t lo, int64_t hi, int32_t exponent) {
    const unsigned __int128 bits =
        (static_cast<unsigned __int128>(static_cast<uint64_t>(hi)) << 64) | static_cast<uint64_t>(lo);
    const __int128 unscaled = static_cast<__int128>(bits);
    const bool negative = unscaled < 0;
    unsigned __int128 magnitude =
        negative ? static_cast<unsigned __int128>(-unscaled) : static_cast<unsigned __int128>(unscaled);
    std::string digits;
    do {
        digits.insert(digits.begin(), static_cast<char>('0' + static_cast<int>(magnitude % 10)));
        magnitude /= 10;
    } while (magnitude != 0);
    digits += 'e';
    digits += std::to_string(exponent);
    double parsed = 0.0;
    const auto result = fast_float::from_chars(digits.data(), digits.data() + digits.size(), parsed);
    if (result.ptr != digits.data() + digits.size() ||
        (result.ec != std::errc() && result.ec != std::errc::result_out_of_range)) {
        throw std::logic_error("decimal_to_double: unparseable decimal");
    }
    return negative ? -parsed : parsed;
}

// `bytes.decode("utf-8")`'s code points: false where Python raises
// UnicodeDecodeError.
inline bool utf8_code_points(const std::string& text, std::vector<uint32_t>& out) {
    out.clear();
    const auto* p = reinterpret_cast<const unsigned char*>(text.data());
    const size_t n = text.size();
    size_t i = 0;
    while (i < n) {
        const unsigned char b0 = p[i];
        if (b0 < 0x80) {
            out.push_back(b0);
            ++i;
            continue;
        }
        size_t len;
        uint32_t cp;
        uint32_t minimum;
        if (b0 >= 0xC2 && b0 <= 0xDF) {
            len = 2; cp = b0 & 0x1F; minimum = 0x80;
        } else if (b0 >= 0xE0 && b0 <= 0xEF) {
            len = 3; cp = b0 & 0x0F; minimum = 0x800;
        } else if (b0 >= 0xF0 && b0 <= 0xF4) {
            len = 4; cp = b0 & 0x07; minimum = 0x10000;
        } else {
            return false;
        }
        if (i + len > n) return false;
        for (size_t k = 1; k < len; ++k) {
            const unsigned char b = p[i + k];
            if ((b & 0xC0) != 0x80) return false;
            cp = (cp << 6) | (b & 0x3F);
        }
        if (cp < minimum || cp > 0x10FFFF || (cp >= 0xD800 && cp <= 0xDFFF)) return false;
        out.push_back(cp);
        i += len;
    }
    return true;
}

}  // namespace opteryx::planner::py
