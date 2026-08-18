#pragma once
// rugo/src/declared_parse.hpp — the strict per-value parse for a declared type.
//
// One implementation shared by the JSONL and CSV readers so a column declared
// UINT8 means the same thing in both, and so a value that a query would refuse
// to CAST is not quietly accepted at read time by one format and not the other.
//
// CONTRACT: `parse_into` returns false on ANY value that does not fit the
// declared type. The caller turns that into an error naming the column and row.
// Nothing here degrades, widens, rounds, or nulls a bad value — a declared
// schema is a contract, and the caller declared it precisely so a mismatch is
// visible instead of silently reinterpreted.
//
// Text forms route through draken's own parsers, never a local reimplementation:
//   IPV4      draken/core/ipv4.h        (dotted-quad ONLY — see below)
//   DATE      draken/core/iso_datetime.h
//   TIMESTAMP draken/core/iso_datetime.h
//   DECIMAL   draken/core/decimal_text.h
// so a value read here and the same value CAST in a query cannot disagree.
//
// IPV4 IS TEXT-ONLY (architect's ruling, 2026-08-18). A bare integer in a column
// declared IPV4 is REFUSED, even though the 32 bits would be unambiguous: the
// integer spelling is a leak of the storage representation, and the whole reason
// this parser is draken's is that a reader and an access rule must never
// disagree about which address a value denotes. Shorthand ("10.1") and
// leading-zero/octal forms ("010.1.1.1") are refused by ipv4.h for the same
// reason.
//
// TIMESTAMP IS ISO-TEXT-ONLY (same ruling), for the same reason: an epoch
// integer is a storage spelling, not a timestamp. Conversion from the parsed
// microseconds to a declared unit is EXACT-OR-REFUSE — a value carrying more
// precision than the declared unit can hold fails rather than truncating, which
// is the same policy DECIMAL applies to fractional digits beyond its scale.

#include <cstdint>
#include <cmath>
#include <cstring>
#include <limits>
#include <system_error>

#include "fast_float/fast_float.h"

#include "declared_type.hpp"
#include "core/ipv4.h"
#include "core/iso_datetime.h"
#include "core/decimal_text.h"

namespace rugo {

// Bytes per element in the buffer `parse_into` writes to. BOOL is a 1-bit-per-row
// bitmap and has no element size; callers size it as (n + 7) / 8 and must not
// call this for it.
inline size_t declared_elem_size(DrakenType t) noexcept {
    switch (t) {
        case DRAKEN_INT8:
        case DRAKEN_UINT8:       return 1;
        case DRAKEN_INT16:
        case DRAKEN_UINT16:      return 2;
        case DRAKEN_INT32:
        case DRAKEN_UINT32:
        case DRAKEN_FLOAT32:
        case DRAKEN_DATE32:      return 4;
        case DRAKEN_INT64:
        case DRAKEN_UINT64:
        case DRAKEN_FLOAT64:
        case DRAKEN_TIMESTAMP64:
        case DRAKEN_DECIMAL:     return 8;
        case DRAKEN_DECIMAL128:  return 16;
        default:                 return 0;   // string family / not fixed width
    }
}

inline bool declared_is_string(DrakenType t) noexcept {
    return t == DRAKEN_VARCHAR || t == DRAKEN_NVARCHAR || t == DRAKEN_VARBINARY;
}

namespace detail {

// Strict signed decimal integer. No leading '+', no whitespace, no trailing
// junk, overflow-checked against int64. Deliberately not fast_parse_int64: that
// accepts forms (leading '+') JSON does not produce and a declared column should
// not silently take.
inline bool strict_int64(const uint8_t* p, uint32_t len, int64_t* out) noexcept {
    if (len == 0) return false;
    uint32_t i = 0;
    bool neg = false;
    if (p[0] == '-') { neg = true; i = 1; if (len == 1) return false; }
    uint64_t mag = 0;
    for (; i < len; ++i) {
        if (p[i] < '0' || p[i] > '9') return false;
        if (mag > (UINT64_C(0xFFFFFFFFFFFFFFFF) - static_cast<uint64_t>(p[i] - '0')) / 10u)
            return false;
        mag = mag * 10u + static_cast<uint64_t>(p[i] - '0');
    }
    const uint64_t limit = neg ? UINT64_C(9223372036854775808) : UINT64_C(9223372036854775807);
    if (mag > limit) return false;
    *out = neg ? -static_cast<int64_t>(mag) : static_cast<int64_t>(mag);
    return true;
}

// Strict unsigned decimal integer. A leading '-' is REFUSED rather than wrapped:
// a negative value in a column declared unsigned is a schema mismatch, and
// wrapping it would turn -1 into 4294967295 with nothing to show for it.
inline bool strict_uint64(const uint8_t* p, uint32_t len, uint64_t* out) noexcept {
    if (len == 0) return false;
    uint64_t mag = 0;
    for (uint32_t i = 0; i < len; ++i) {
        if (p[i] < '0' || p[i] > '9') return false;
        if (mag > (UINT64_C(0xFFFFFFFFFFFFFFFF) - static_cast<uint64_t>(p[i] - '0')) / 10u)
            return false;
        mag = mag * 10u + static_cast<uint64_t>(p[i] - '0');
    }
    *out = mag;
    return true;
}

inline bool strict_double(const uint8_t* p, uint32_t len, double* out) noexcept {
    if (len == 0) return false;
    const char* first = reinterpret_cast<const char*>(p);
    const char* last = first + len;
    auto answer = fast_float::from_chars(first, last, *out);
    return answer.ec == std::errc() && answer.ptr == last;
}

inline bool strict_bool(const uint8_t* p, uint32_t len, bool* out) noexcept {
    if (len == 4 && std::memcmp(p, "true", 4) == 0)  { *out = true;  return true; }
    if (len == 5 && std::memcmp(p, "false", 5) == 0) { *out = false; return true; }
    return false;
}

// Microseconds -> the declared unit, EXACT OR REFUSE. Truncating here would make
// a declared TIMESTAMP[s] silently drop the sub-second part of every value it
// was handed, which is the same class of silent loss DECIMAL's scale check
// refuses.
inline bool micros_to_unit(int64_t us, uint8_t unit, int64_t* out) noexcept {
    switch (unit) {
        case 0:  if (us % 1000000LL != 0) return false; *out = us / 1000000LL; return true;
        case 1:  if (us % 1000LL != 0)    return false; *out = us / 1000LL;    return true;
        case 2:  *out = us; return true;
        case 3:
            if (us > INT64_MAX / 1000LL || us < INT64_MIN / 1000LL) return false;
            *out = us * 1000LL;
            return true;
        default: return false;
    }
}

}  // namespace detail

// Parse one value into element slot `index` of `buffer`.
//
// `buffer` is `declared_elem_size(dt.type)`-sized elements, EXCEPT for
// DRAKEN_BOOL where it is a 1-bit-per-row bitmap and `index` selects the bit.
// The bitmap must be zeroed by the caller; a false value writes no bit.
//
// Returns false if the value does not fit the declared type. `buffer` may have
// been written at `index` in that case, so the caller must treat the whole
// column as failed (which every caller does — the read raises).
inline bool declared_parse_into(const DeclaredType& dt, const uint8_t* p, uint32_t len,
                                void* buffer, uint32_t index) noexcept {
    // IPV4 FIRST, before the physical switch: it shares UINT32's tag, so a
    // physical-tag dispatch would fall into the unsigned-integer arm and accept
    // the very integer spelling that arm exists to refuse. Dispatching on the
    // descriptor here is what makes "dotted-quad only" true rather than aspirational.
    if (dt.logical_kind == LK_IPV4) {
        if (dt.type != DRAKEN_UINT32) return false;
        uint32_t addr;
        if (!draken::ipv4::parse(p, len, &addr)) return false;
        static_cast<uint32_t*>(buffer)[index] = addr;
        return true;
    }
    switch (dt.type) {
        case DRAKEN_INT8:
        case DRAKEN_INT16:
        case DRAKEN_INT32:
        case DRAKEN_INT64: {
            int64_t v;
            if (!detail::strict_int64(p, len, &v)) return false;
            if (dt.type == DRAKEN_INT8) {
                if (v < INT8_MIN || v > INT8_MAX) return false;
                static_cast<int8_t*>(buffer)[index] = static_cast<int8_t>(v);
            } else if (dt.type == DRAKEN_INT16) {
                if (v < INT16_MIN || v > INT16_MAX) return false;
                static_cast<int16_t*>(buffer)[index] = static_cast<int16_t>(v);
            } else if (dt.type == DRAKEN_INT32) {
                if (v < INT32_MIN || v > INT32_MAX) return false;
                static_cast<int32_t*>(buffer)[index] = static_cast<int32_t>(v);
            } else {
                static_cast<int64_t*>(buffer)[index] = v;
            }
            return true;
        }
        case DRAKEN_UINT8:
        case DRAKEN_UINT16:
        case DRAKEN_UINT32:
        case DRAKEN_UINT64: {
            uint64_t v;
            if (!detail::strict_uint64(p, len, &v)) return false;
            if (dt.type == DRAKEN_UINT8) {
                if (v > UINT8_MAX) return false;
                static_cast<uint8_t*>(buffer)[index] = static_cast<uint8_t>(v);
            } else if (dt.type == DRAKEN_UINT16) {
                if (v > UINT16_MAX) return false;
                static_cast<uint16_t*>(buffer)[index] = static_cast<uint16_t>(v);
            } else if (dt.type == DRAKEN_UINT32) {
                // A UINT32 carrying the IPV4 descriptor never reaches here — it
                // is dispatched on the descriptor at the top of this function.
                if (v > UINT32_MAX) return false;
                static_cast<uint32_t*>(buffer)[index] = static_cast<uint32_t>(v);
            } else {
                static_cast<uint64_t*>(buffer)[index] = v;
            }
            return true;
        }
        case DRAKEN_FLOAT32: {
            double d;
            if (!detail::strict_double(p, len, &d)) return false;
            const float f = static_cast<float>(d);
            // A finite double that becomes infinite as a float is out of range,
            // not a rounding difference.
            if (std::isinf(f) && !std::isinf(d)) return false;
            static_cast<float*>(buffer)[index] = f;
            return true;
        }
        case DRAKEN_FLOAT64: {
            double d;
            if (!detail::strict_double(p, len, &d)) return false;
            static_cast<double*>(buffer)[index] = d;
            return true;
        }
        case DRAKEN_BOOL: {
            bool b;
            if (!detail::strict_bool(p, len, &b)) return false;
            if (b) static_cast<uint8_t*>(buffer)[index >> 3] |= static_cast<uint8_t>(1u << (index & 7u));
            return true;
        }
        case DRAKEN_DATE32: {
            const int32_t days = draken::iso_datetime::parse_iso_date(p, len);
            if (days == INT32_MIN) return false;
            static_cast<int32_t*>(buffer)[index] = days;
            return true;
        }
        case DRAKEN_TIMESTAMP64: {
            int year, month, day, hour, minute, second, usec;
            if (!draken::iso_datetime::parse_iso_timestamp(
                    p, len, &year, &month, &day, &hour, &minute, &second, &usec))
                return false;
            const int64_t us =
                draken::iso_datetime::civil_to_micros(year, month, day, hour, minute, second, usec);
            int64_t scaled;
            if (!detail::micros_to_unit(us, dt.unit, &scaled)) return false;
            static_cast<int64_t*>(buffer)[index] = scaled;
            return true;
        }
        case DRAKEN_DECIMAL:
        case DRAKEN_DECIMAL128: {
            __int128 unscaled = 0;
            if (draken::decimal_text::parse(p, len, dt.precision, dt.scale, &unscaled)
                    != draken::decimal_text::OK)
                return false;
            if (dt.type == DRAKEN_DECIMAL128) {
                std::memcpy(static_cast<uint8_t*>(buffer) + static_cast<size_t>(index) * 16u,
                            &unscaled, 16u);
            } else {
                const int64_t w = static_cast<int64_t>(unscaled);
                static_cast<int64_t*>(buffer)[index] = w;
            }
            return true;
        }
        default:
            break;
    }
    return false;   // string family and anything without a fixed-width arm
}

}  // namespace rugo
