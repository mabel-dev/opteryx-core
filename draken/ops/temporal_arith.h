#pragma once
// draken/ops/temporal_arith.h — calendar-aware temporal batch ops (Milestone E.13, Phase 12, C′).
//
// Three dispatching batch kernels for SQL calendar-aware temporal functions:
//
//   date_trunc_batch — floor to calendar boundary (year/quarter/month/week/day/hour/minute/second)
//   date_diff_batch  — signed difference in SQL units (matches old vector_date_diff.pyx semantics)
//
// Calendar math uses Howard Hinnant's proleptic Gregorian algorithm:
//   Reference: https://howardhinnant.github.io/date_algorithms.html
//
// All batch kernels take pre-gathered flat arrays (caller gathers via data[selection[i]]).
// Null rows must be skipped by the caller; output slots for nulls are left as 0.
//
// Unit codes: 0=seconds, 1=milliseconds, 2=microseconds (hot path), 3=nanoseconds.
// No dynamic dispatch inside kernels; unit_code is resolved once at function entry.

#include <cstdint>
#include <cstdlib>

// ---------------------------------------------------------------------------
// Howard Hinnant's proleptic Gregorian day ↔ date conversion.
// Reference: https://howardhinnant.github.io/date_algorithms.html#civil_from_days
// Handles negative (pre-epoch) days correctly.
// ---------------------------------------------------------------------------

// Convert days-since-epoch → (year, month [1-12], day [1-31]).
static inline void ta_days_to_ymd(int64_t z, int* year, int* month, int* day) noexcept {
    z += 719468;
    const int64_t era = (z >= 0 ? z : z - 146096) / 146097;
    const int     doe = static_cast<int>(z - era * 146097);
    const int     yoe = (doe - doe/1460 + doe/36524 - doe/146096) / 365;
    *year  = yoe + static_cast<int>(era * 400);
    const int doy = doe - (365 * yoe + yoe/4 - yoe/100);
    const int mp  = (5 * doy + 2) / 153;
    *day   = doy - (153 * mp + 2) / 5 + 1;
    *month = mp < 10 ? mp + 3 : mp - 9;
    if (*month <= 2) *year += 1;
}

// Convert (year, month [1-12], day [1-31]) → days-since-epoch.
static inline int64_t ta_ymd_to_days(int y, int m, int d) noexcept {
    if (m <= 2) { y -= 1; m += 12; }
    const int era = (y >= 0 ? y : y - 399) / 400;
    const int yoe = y - era * 400;
    const int doy = (153 * (m - 3) + 2) / 5 + d - 1;
    const int doe = yoe * 365 + yoe/4 - yoe/100 + doy;
    return static_cast<int64_t>(era) * 146097 + doe - 719468;
}

// ---------------------------------------------------------------------------
// Floor division toward −∞ (C++ / truncates toward 0; Python // floors).
// Matches Python's // semantics for negative timestamps.
// ---------------------------------------------------------------------------

static inline int64_t ta_floor_div(int64_t a, int64_t b) noexcept {
    const int64_t q = a / b;
    return q - (((a ^ b) < 0) & (q * b != a));
}

// ---------------------------------------------------------------------------
// Unit-scale helpers
// ---------------------------------------------------------------------------

static inline int64_t ta_ticks_per_second(int unit_code) noexcept {
    switch (unit_code) {
        case 0:  return          1LL;  // seconds
        case 1:  return       1000LL;  // milliseconds
        case 2:  return    1000000LL;  // microseconds
        case 3:  return 1000000000LL;  // nanoseconds
        default: return    1000000LL;
    }
}

static inline int64_t ta_ticks_per_day(int unit_code) noexcept {
    switch (unit_code) {
        case 0:  return         86400LL;
        case 1:  return      86400000LL;
        case 2:  return   86400000000LL;
        case 3:  return 86400000000000LL;
        default: return   86400000000LL;
    }
}

// ---------------------------------------------------------------------------
// date_trunc_batch
//
// Truncates `n` pre-gathered int64 tick values to a calendar boundary.
// Input: src[0..n) = gathered tick values (nulls already skipped by caller).
// Output: out[0..n) = truncated tick values (caller pre-zeros for null rows).
//
// trunc_kind encoding:
//   0=year  1=month  2=quarter  3=week  4=day  5=hour  6=minute  7=second
//
// Calendar kinds (0-3): decompose to ymd via Howard Hinnant, rebuild epoch days.
// Sub-day kinds (4-7): pure integer alignment, no calendar lookup.
//
// EPOCH_WEEKDAY = 4: 1970-01-01 was Thursday (0-indexed from Monday = 4 for Thursday).
// ISO week starts Monday. Week trunc floors to the Monday on or before the input day.
// ---------------------------------------------------------------------------

static inline void date_trunc_batch(
    const int64_t* src, uint32_t n, int unit_code, int trunc_kind, int64_t* out) noexcept
{
    const int64_t tps = ta_ticks_per_second(unit_code);
    const int64_t tpd = ta_ticks_per_day(unit_code);
    const int64_t tph = tps * 3600LL;
    const int64_t tpm = tps * 60LL;

    for (uint32_t i = 0u; i < n; ++i) {
        const int64_t v = src[i];
        int64_t result;
        switch (trunc_kind) {
            case 4: // day — most common
                result = ta_floor_div(v, tpd) * tpd;
                break;
            case 5: // hour
                result = ta_floor_div(v, tph) * tph;
                break;
            case 6: // minute
                result = ta_floor_div(v, tpm) * tpm;
                break;
            case 7: // second — no-op for whole-second precision, strips sub-second otherwise
                result = ta_floor_div(v, tps) * tps;
                break;
            case 3: { // week — floor to ISO Monday (EPOCH_WEEKDAY=4=Thursday, so Mon=0..Sun=6)
                const int64_t days = ta_floor_div(v, tpd);
                const int64_t d2m  = ((days - 4LL) % 7 + 7) % 7;
                result = (days - d2m) * tpd;
                break;
            }
            case 0: { // year
                const int64_t secs = ta_floor_div(v, tps);
                const int64_t days = ta_floor_div(secs, 86400LL);
                int yr, mo, dy;
                ta_days_to_ymd(days, &yr, &mo, &dy);
                result = ta_ymd_to_days(yr, 1, 1) * tpd;
                break;
            }
            case 1: { // month
                const int64_t secs = ta_floor_div(v, tps);
                const int64_t days = ta_floor_div(secs, 86400LL);
                int yr, mo, dy;
                ta_days_to_ymd(days, &yr, &mo, &dy);
                result = ta_ymd_to_days(yr, mo, 1) * tpd;
                break;
            }
            case 2: { // quarter
                const int64_t secs = ta_floor_div(v, tps);
                const int64_t days = ta_floor_div(secs, 86400LL);
                int yr, mo, dy;
                ta_days_to_ymd(days, &yr, &mo, &dy);
                const int q_mo = ((mo - 1) / 3) * 3 + 1;
                result = ta_ymd_to_days(yr, q_mo, 1) * tpd;
                break;
            }
            default:
                result = v;
                break;
        }
        out[i] = result;
    }
}

// ---------------------------------------------------------------------------
// date_diff_batch
//
// Computes signed difference (end[i] - start[i]) in the requested unit.
// Both arrays are pre-gathered (caller resolves selection[]).
// unit_code_s / unit_code_e: unit codes for start and end vectors respectively.
// Null rows skipped by caller; their output slots remain 0.
//
// diff_kind encoding (plural form to match old vector_date_diff.pyx):
//   0 = microseconds   1 = milliseconds   2 = seconds
//   3 = minutes        4 = hours          5 = days
//   6 = weeks          7 = months         8 = quarters    9 = years
//
// Sub-month/year arithmetic: both inputs normalised to microseconds then divided.
// Month/quarter/year: approximate via days÷30, days÷91, days÷365 — exactly matching
// the old vector_date_diff.pyx semantics. All divisions use floor semantics (Python //).
// ---------------------------------------------------------------------------

static inline void date_diff_batch(
    const int64_t* start, const int64_t* end, uint32_t n,
    int unit_code_s, int unit_code_e, int diff_kind, int64_t* out) noexcept
{
    const int64_t US_PER_DAY = 86400000000LL;

    for (uint32_t i = 0u; i < n; ++i) {
        // Normalise both values to microseconds (floor toward -∞ for nanoseconds).
        int64_t sv, ev;
        switch (unit_code_s) {
            case 0: sv = start[i] * 1000000LL; break;
            case 1: sv = start[i] * 1000LL;    break;
            case 2: sv = start[i];              break;
            case 3: sv = ta_floor_div(start[i], 1000LL); break;
            default: sv = start[i]; break;
        }
        switch (unit_code_e) {
            case 0: ev = end[i] * 1000000LL; break;
            case 1: ev = end[i] * 1000LL;    break;
            case 2: ev = end[i];              break;
            case 3: ev = ta_floor_div(end[i], 1000LL); break;
            default: ev = end[i]; break;
        }

        int64_t result;
        switch (diff_kind) {
            case 0: result = ev - sv;                                            break;
            case 1: result = ta_floor_div(ev - sv,         1000LL);             break;
            case 2: result = ta_floor_div(ev - sv,      1000000LL);             break;
            case 3: result = ta_floor_div(ev - sv,     60000000LL);             break;
            case 4: result = ta_floor_div(ev - sv,   3600000000LL);             break;
            case 5: result = ta_floor_div(ev - sv, US_PER_DAY);                 break;
            case 6: result = ta_floor_div(ta_floor_div(ev - sv, US_PER_DAY), 7LL);   break;
            case 7: result = ta_floor_div(ta_floor_div(ev - sv, US_PER_DAY), 30LL);  break;
            case 8: result = ta_floor_div(ta_floor_div(ev - sv, US_PER_DAY), 91LL);  break;
            case 9: result = ta_floor_div(ta_floor_div(ev - sv, US_PER_DAY), 365LL); break;
            default: result = 0; break;
        }
        out[i] = result;
    }
}
