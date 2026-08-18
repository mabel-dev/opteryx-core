#pragma once
// draken/core/decimal_text.h — canonical exact text -> fixed-point DECIMAL parse.
//
// This is the ONE place the text form of a DECIMAL is interpreted, for the same
// reason draken/core/ipv4.h is the one place an IPv4 literal is interpreted: a
// second parser is free to drift on exactly the inputs the policy exists to
// refuse, and a producer and a consumer disagreeing about whether "1.005" fits
// DECIMAL(18, 2) is a silent wrong answer, not a formatting difference.
//
// Lifted verbatim out of draken_cast_string_to_decimal (ops/kernels/cast_string.cpp),
// which now calls it, so the CAST kernel and any other producer cannot diverge.
//
// Accepted syntax mirrors the literal path (`decimal.Decimal(text.strip())` in
// _parse_decimal / _build_decimal_closure, casts.pyx) so a cast over a COLUMN and
// the same cast over a LITERAL cannot disagree:
//   [ws] [+|-] digits [ . digits] [ (e|E) [+|-] digits ] [ws]
// with at least one mantissa digit ('1.' and '.5' are both accepted). Infinity and
// NaN are rejected — neither has a fixed-point representation, and the literal path
// rejects them too (at quantize).
//
// Value policy — a declared type is a contract, not a hint:
//   - fractional digits beyond the declared scale FAIL LOUD when they would be
//     dropped; trailing zeros re-pad silently ('1.250' -> DECIMAL(10,2) is 1.25).
//   - a magnitude outside the declared precision FAILS LOUD, never wraps.
//   - malformed text FAILS LOUD.
// The CALLER decides what a failure means (the kernel raises, or maps the row to
// NULL under TRY_CAST) — this header never decides that.
//
// LIMIT, deliberate and loud: a mantissa carrying more than 38 significant digits
// is refused as an overflow even when a negative exponent would bring it back into
// range ('1e39' * 1e-30). 38 digits IS the DECIMAL precision ceiling, so no
// representable target loses reachable values; the alternative is arbitrary-
// precision accumulation for inputs no DECIMAL column can store.
//
// Pure C++, header-only, no Python, no allocation.

#include <cstdint>

namespace draken {
namespace decimal_text {

// Parse dispositions. The caller maps these to its own error surface.
enum Status : int {
    OK        = 0,
    MALFORMED = 1,   // not a number at all
    OVERFLOW_  = 2,  // magnitude does not fit the declared precision
    SCALE     = 3,   // more fractional digits than the declared scale can hold
};

// ASCII whitespace recognized when trimming numeric strings (matches the
// nanobind string->float64 path).
inline bool is_ascii_space(uint8_t c) noexcept {
    return c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == '\f' || c == '\v';
}

inline __int128 pow10_i128(int k) noexcept {
    __int128 r = 1;
    while (k-- > 0) r *= 10;
    return r;
}

// Parse `text[0..length)` as a DECIMAL(precision, scale) unscaled value.
//
// On Status::OK, *out holds the SIGNED unscaled mantissa at `scale` — the value
// the DECIMAL (int64 tier) or DECIMAL128 (__int128 tier) column stores. On any
// other status *out is unspecified and the caller must not read it.
//
// `precision` must be 1..38; a precision outside that range is the caller's bug
// (a DECIMAL column cannot exist with it) and is reported as MALFORMED rather
// than silently clamped.
inline int parse(const uint8_t* text, uint32_t length,
                 uint8_t precision, uint8_t scale, __int128* out) noexcept {
    if (precision == 0u || precision > 38u) return MALFORMED;

    const __int128 dec_lim = pow10_i128(static_cast<int>(precision));
    // Accumulation ceiling: 10^38 is the widest DECIMAL, and __int128 holds it
    // with room for the *10+d step below without ever wrapping.
    const __int128 acc_lim = pow10_i128(38);
    const int target_scale = static_cast<int>(scale);

    uint32_t p = 0u;
    uint32_t end = length;
    while (p < end && is_ascii_space(text[p])) ++p;
    while (end > p && is_ascii_space(text[end - 1u])) --end;

    int status = OK;
    bool negative = false;
    if (p < end && (text[p] == '+' || text[p] == '-')) {
        negative = (text[p] == '-');
        ++p;
    }

    __int128 mag = 0;
    int frac_digits = 0;
    uint32_t digits_seen = 0u;
    bool seen_point = false;
    for (; p < end; ++p) {
        const uint8_t ch = text[p];
        if (ch == '.') {
            if (seen_point) { status = MALFORMED; break; }
            seen_point = true;
            continue;
        }
        if (ch == 'e' || ch == 'E') break;
        if (ch < '0' || ch > '9') { status = MALFORMED; break; }
        ++digits_seen;
        if (seen_point) ++frac_digits;
        if (mag >= acc_lim) { status = OVERFLOW_; continue; }
        mag = mag * 10 + static_cast<__int128>(ch - '0');
    }
    if (status == OK && digits_seen == 0u) status = MALFORMED;

    int exponent = 0;
    if (status == OK && p < end && (text[p] == 'e' || text[p] == 'E')) {
        ++p;
        bool exp_neg = false;
        if (p < end && (text[p] == '+' || text[p] == '-')) {
            exp_neg = (text[p] == '-');
            ++p;
        }
        uint32_t exp_digits = 0u;
        for (; p < end; ++p) {
            const uint8_t ch = text[p];
            if (ch < '0' || ch > '9') { status = MALFORMED; break; }
            ++exp_digits;
            // Clamped: any |exponent| past 1000 is already decided by the
            // precision/scale checks below, and this keeps the int bounded.
            if (exponent < 1000) exponent = exponent * 10 + (ch - '0');
        }
        if (exp_digits == 0u) status = MALFORMED;
        if (exp_neg) exponent = -exponent;
    } else if (status == OK && p != end) {
        status = MALFORMED;
    }

    __int128 unscaled = 0;
    if (status == OK) {
        // unscaled = mag * 10^(target_scale + exponent - frac_digits)
        const int shift = target_scale + exponent - frac_digits;
        if (mag == 0) {
            unscaled = 0;
        } else if (shift > 38) {
            status = OVERFLOW_;
        } else if (shift < -38) {
            status = SCALE;
        } else if (shift >= 0) {
            const __int128 factor = pow10_i128(shift);
            if (mag > (dec_lim - 1) / factor) status = OVERFLOW_;
            else unscaled = mag * factor;
        } else {
            const __int128 factor = pow10_i128(-shift);
            // Exact only: digits that would be DROPPED are an error, trailing
            // zeros divide away cleanly.
            if (mag % factor != 0) status = SCALE;
            else unscaled = mag / factor;
        }
    }
    // unscaled is still the MAGNITUDE here (the sign is applied below), so
    // only the upper bound can be crossed.
    if (status == OK && unscaled >= dec_lim) status = OVERFLOW_;

    if (status != OK) return status;

    *out = negative ? -unscaled : unscaled;
    return OK;
}

}  // namespace decimal_text
}  // namespace draken
