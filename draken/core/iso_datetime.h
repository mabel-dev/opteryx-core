#pragma once
// draken/core/iso_datetime.h — canonical ISO-8601 date/timestamp text parsing and
// the civil-date <-> days-since-epoch conversion both of them stand on.
//
// The ONE place ISO text is interpreted, for the same reason draken/core/ipv4.h is
// the one place an IPv4 literal is interpreted: a second parser drifts on exactly
// the forms the first one refuses, and two components disagreeing about what a
// timestamp string means is a silent wrong answer.
//
// Lifted verbatim out of the cast kernels, which now call it:
//   parse_iso_date      <- ops/kernels/cast_string.cpp   (draken_cast_string_to_date32)
//   parse_iso_timestamp <- ops/kernels/cast_temporal.cpp (draken_cast_string_to_timestamp)
//   civil_to_days       <- BOTH of the above, which each carried their own
//                          internal-linkage copy (one returning int, one int64_t)
//                          precisely because "no cross-TU header exists for it yet".
//                          This is that header.
//
// Timestamps are NAIVE: an offset or 'Z' suffix is a parse ERROR, not something
// to discard. Opteryx timestamps carry no zone, so silently dropping an offset
// would shift every value by it.
//
// Pure C++, header-only, no Python, no allocation.

#include <cstdint>

namespace draken {
namespace iso_datetime {

// Howard Hinnant's civil_from_days inverse: proleptic Gregorian (y, m, d) to
// days since 1970-01-01. Valid for any y/m/d the callers admit; no range check
// (the callers validate month/day before calling).
inline int64_t civil_to_days(int y, int m, int d) noexcept {
    y -= (m <= 2);
    const int64_t era = (y >= 0 ? y : y - 399) / 400;
    const int64_t yoe = static_cast<int64_t>(y) - era * 400;
    const int64_t doy = (153 * static_cast<int64_t>(m + (m > 2 ? -3 : 9)) + 2) / 5 + d - 1;
    const int64_t doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    return era * 146097 + doe - 719468;
}

// "YYYY-MM-DD" -> days since epoch. Returns INT32_MIN on any parse error.
//
// The sentinel is INT32_MIN rather than a bool-plus-out-param because DATE32's
// physical storage is int32 days and INT32_MIN is ~5.8 million years before the
// epoch — outside anything a DATE32 column can hold.
inline int32_t parse_iso_date(const uint8_t* s, uint32_t len) noexcept {
    uint32_t k = 0;
    int year = 0, month = 0, day = 0;

    while (k < len && s[k] != '-') {
        if (s[k] < '0' || s[k] > '9') return INT32_MIN;
        year = year * 10 + (s[k] - '0');
        ++k;
    }
    if (k >= len || s[k] != '-') return INT32_MIN;
    ++k;

    while (k < len && s[k] != '-') {
        if (s[k] < '0' || s[k] > '9') return INT32_MIN;
        month = month * 10 + (s[k] - '0');
        ++k;
    }
    if (k >= len || s[k] != '-') return INT32_MIN;
    ++k;

    while (k < len) {
        if (s[k] < '0' || s[k] > '9') return INT32_MIN;
        day = day * 10 + (s[k] - '0');
        ++k;
    }

    if (year < 1 || month < 1 || month > 12 || day < 1 || day > 31)
        return INT32_MIN;
    return static_cast<int32_t>(civil_to_days(year, month, day));
}

// Strict ISO-8601 timestamp parse: "YYYY-MM-DD", or "YYYY-MM-DD" plus a time
// with either a 'T' or a space separator ("YYYY-MM-DDTHH:MM[:SS[.ffffff]]").
// No timezone offset — see the naive-timestamp note at the top of this file.
// Returns false on any parse error; on false the out-params are unspecified
// except that the time fields have already been zeroed.
inline bool parse_iso_timestamp(const uint8_t* s, uint32_t len,
                                int* year, int* month, int* day,
                                int* hour, int* minute, int* second, int* usec) noexcept {
    *hour = 0; *minute = 0; *second = 0; *usec = 0;
    uint32_t k = 0;
    int y = 0, m = 0, d = 0;
    while (k < len && s[k] != '-') {
        if (s[k] < '0' || s[k] > '9') return false;
        y = y * 10 + (s[k] - '0'); ++k;
    }
    if (k >= len || s[k] != '-') return false;
    ++k;
    while (k < len && s[k] != '-') {
        if (s[k] < '0' || s[k] > '9') return false;
        m = m * 10 + (s[k] - '0'); ++k;
    }
    if (k >= len || s[k] != '-') return false;
    ++k;
    while (k < len && (s[k] >= '0' && s[k] <= '9')) {
        d = d * 10 + (s[k] - '0'); ++k;
    }
    *year = y; *month = m; *day = d;
    if (k == len) return (m >= 1 && m <= 12 && d >= 1 && d <= 31);
    if (s[k] != 'T' && s[k] != ' ') return false;
    ++k;

    int hh = 0, mi = 0, ss = 0, us = 0;
    uint32_t hstart = k;
    while (k < len && s[k] != ':') {
        if (s[k] < '0' || s[k] > '9') return false;
        hh = hh * 10 + (s[k] - '0'); ++k;
    }
    if (k == hstart || k >= len || s[k] != ':') return false;
    ++k;
    uint32_t mstart = k;
    while (k < len && s[k] != ':') {
        if (s[k] < '0' || s[k] > '9') return false;
        mi = mi * 10 + (s[k] - '0'); ++k;
    }
    if (k == mstart) return false;
    if (k < len && s[k] == ':') {
        ++k;
        uint32_t sstart = k;
        while (k < len && s[k] != '.') {
            if (s[k] < '0' || s[k] > '9') return false;
            ss = ss * 10 + (s[k] - '0'); ++k;
        }
        if (k == sstart) return false;
        if (k < len && s[k] == '.') {
            ++k;
            int ndigits = 0;
            while (k < len) {
                if (s[k] < '0' || s[k] > '9') return false;
                if (ndigits >= 6) return false;
                us = us * 10 + (s[k] - '0'); ++ndigits; ++k;
            }
            if (ndigits == 0) return false;
            for (int p = ndigits; p < 6; ++p) us *= 10;
        }
    }
    if (k != len) return false;  // trailing offset/'Z' etc. -> not supported
    if (hh > 23 || mi > 59 || ss > 59) return false;
    *hour = hh; *minute = mi; *second = ss; *usec = us;
    return m >= 1 && m <= 12 && d >= 1 && d <= 31;
}

// Microseconds since epoch from already-validated civil fields — the tail of
// the string->TIMESTAMP64 conversion, shared so a second caller cannot get the
// multipliers subtly wrong.
inline int64_t civil_to_micros(int year, int month, int day,
                               int hour, int minute, int second, int usec) noexcept {
    return civil_to_days(year, month, day) * 86400000000LL
         + static_cast<int64_t>(hour) * 3600000000LL
         + static_cast<int64_t>(minute) * 60000000LL
         + static_cast<int64_t>(second) * 1000000LL
         + usec;
}

}  // namespace iso_datetime
}  // namespace draken
