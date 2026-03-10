/**
 * simd_datepart.cpp — Fast date-part extraction for int64 timestamp vectors.
 *
 * Design notes
 * ─────────────
 *  The critical optimisation here is NOT SIMD load/store but constant-divisor
 *  arithmetic: when the divisor is a compile-time literal the compiler replaces
 *  the expensive SDIV/UDIV instruction with a UMULH+shift sequence (ARM64) or
 *  MULQ+shift (x86-64).  On Apple Silicon this saves ~5 cycles per division.
 *
 *  NEON / AVX2 cannot vectorise 64-bit integer division — there is no
 *  vdivq_s64 or _mm256_div_epi64.  Attempts to use load/extract/repack
 *  wrappers around scalar arithmetic add overhead and are net slower.
 *
 *  The scalar path below gains the full constant-divisor benefit via a
 *  switch-outside-loop pattern: each loop body sees a literal constant so
 *  the compiler applies magic-number division.  The loop itself is 4-unrolled
 *  which provides enough independent operations for the OOO scheduler.
 *
 *  The simd::select_dispatch infrastructure is retained for API consistency
 *  and so a genuine SIMD path can be dropped in later (e.g. float-conversion
 *  trick for narrow timestamp ranges), but no NEON/AVX2 specialisation is
 *  registered here.
 *
 * unit_code encoding (matches vector_date_part.pyx):
 *   0 = seconds
 *   1 = milliseconds
 *   2 = microseconds  (Q19 hot path, 60 000 000 µs / minute)
 *   3 = nanoseconds
 */

#include "simd_datepart.h"
#include "simd_dispatch.h"
#include "cpu_features.h"

#include <atomic>
#include <cstdint>
#include <cstddef>

// ===========================================================================
// 4-unrolled scalar loop with a compile-time (div, mod) pair.
// The compiler generates UMULH+shift instead of SDIV for each literal divisor.
// ===========================================================================
#define EXTRACT_LOOP_4(div, mod)                                                    \
    do {                                                                            \
        size_t _i = 0;                                                              \
        for (; _i + 3 < n; _i += 4) {                                              \
            dst[_i]   = (int64_t)((uint64_t)src[_i]   / (uint64_t)(div) % (uint64_t)(mod)); \
            dst[_i+1] = (int64_t)((uint64_t)src[_i+1] / (uint64_t)(div) % (uint64_t)(mod)); \
            dst[_i+2] = (int64_t)((uint64_t)src[_i+2] / (uint64_t)(div) % (uint64_t)(mod)); \
            dst[_i+3] = (int64_t)((uint64_t)src[_i+3] / (uint64_t)(div) % (uint64_t)(mod)); \
        }                                                                           \
        for (; _i < n; ++_i)                                                        \
            dst[_i] = (int64_t)((uint64_t)src[_i] / (uint64_t)(div) % (uint64_t)(mod)); \
    } while (0)

// ===========================================================================
// MINUTE  (modulus 60)
// ===========================================================================

static void minute_scalar(const int64_t* src, int64_t* dst, size_t n, int unit_code) {
    switch (unit_code) {
        case 0:  EXTRACT_LOOP_4(60LL,            60LL); break;
        case 1:  EXTRACT_LOOP_4(60000LL,         60LL); break;
        case 2:  EXTRACT_LOOP_4(60000000LL,      60LL); break;
        case 3:  EXTRACT_LOOP_4(60000000000LL,   60LL); break;
        default: EXTRACT_LOOP_4(60000000LL,      60LL); break;
    }
}

// ===========================================================================
// HOUR  (modulus 24)
// ===========================================================================

static void hour_scalar(const int64_t* src, int64_t* dst, size_t n, int unit_code) {
    switch (unit_code) {
        case 0:  EXTRACT_LOOP_4(3600LL,            24LL); break;
        case 1:  EXTRACT_LOOP_4(3600000LL,         24LL); break;
        case 2:  EXTRACT_LOOP_4(3600000000LL,      24LL); break;
        case 3:  EXTRACT_LOOP_4(3600000000000LL,   24LL); break;
        default: EXTRACT_LOOP_4(3600000000LL,      24LL); break;
    }
}

// ===========================================================================
// SECOND  (modulus 60)
// ===========================================================================

static void second_scalar(const int64_t* src, int64_t* dst, size_t n, int unit_code) {
    switch (unit_code) {
        case 0:  EXTRACT_LOOP_4(1LL,          60LL); break;
        case 1:  EXTRACT_LOOP_4(1000LL,       60LL); break;
        case 2:  EXTRACT_LOOP_4(1000000LL,    60LL); break;
        case 3:  EXTRACT_LOOP_4(1000000000LL, 60LL); break;
        default: EXTRACT_LOOP_4(1000000LL,    60LL); break;
    }
}

// ===========================================================================
// Calendar decomposition helpers
//
// Howard Hinnant's civil calendar algorithm, using int32_t arithmetic.
// Days since 1970-01-01 fits comfortably in int32_t (range ≈ ±4000 years),
// which halves the arithmetic cost vs int64_t on ARM64 and x86-64.
//
// Floor division of timestamp → days:
//   q = ts / units_per_day   (compiler emits UMULH for literal constant)
//   r = ts - q * units_per_day
//   days = q + (r >> 63)     // arithmetic right-shift: -1 if r<0, else 0
//
// Each helper computes only the fields it needs; year() skips mp/m/d;
// month() skips yr and d; dom() skips yr.
// ===========================================================================

static const int32_t GREGORIAN_CUM_DAYS[12] =
    {0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334};

// ---------------------------------------------------------------------------
// Core helper: compute (era, doe, yoe, doy) from days.
// Shared prefix reused by all four calendar fields.
// All values fit in uint32_t for practical timestamp ranges (±4000 years).
// Using uint32_t lets the compiler choose tighter unsigned magic constants.
// ---------------------------------------------------------------------------
static inline void days_to_era_parts(int32_t d,
    uint32_t* era_out, uint32_t* doe_out,
    uint32_t* yoe_out, uint32_t* doy_out) noexcept {
    const uint32_t z   = (uint32_t)(d + 719468);  // shift epoch to 0000-03-01
    const uint32_t era = z / 146097u;
    const uint32_t doe = z - era * 146097u;
    const uint32_t yoe = (doe - doe/1460u + doe/36524u - doe/146096u) / 365u;
    *era_out = era;
    *doe_out = doe;
    *yoe_out = yoe;
    *doy_out = doe - (365u*yoe + yoe/4u - yoe/100u);
}

static inline int32_t days_to_year(int32_t d) noexcept {
    uint32_t era, doe, yoe, doy;
    days_to_era_parts(d, &era, &doe, &yoe, &doy);
    // month ≤ 2 ↔ March-based doy ≥ 306 — skip mp/m/d entirely.
    return (int32_t)(yoe + era * 400u) + (int32_t)(doy >= 306u);
}

static inline int32_t days_to_month(int32_t d) noexcept {
    uint32_t era, doe, yoe, doy;
    days_to_era_parts(d, &era, &doe, &yoe, &doy);
    (void)era; (void)yoe;
    const uint32_t mp = (5u*doy + 2u) / 153u;
    return (int32_t)(mp < 10u ? mp + 3u : mp - 9u);
}

static inline int32_t days_to_dom(int32_t d) noexcept {
    uint32_t era, doe, yoe, doy;
    days_to_era_parts(d, &era, &doe, &yoe, &doy);
    (void)era; (void)yoe;
    const uint32_t mp = (5u*doy + 2u) / 153u;
    return (int32_t)(doy - (153u*mp + 2u)/5u + 1u);
}

static inline int32_t days_to_quarter(int32_t d) noexcept {
    return (days_to_month(d) - 1) / 3 + 1;
}

static inline int32_t days_to_doy(int32_t d) noexcept {
    uint32_t era, doe, yoe, doy_m;
    days_to_era_parts(d, &era, &doe, &yoe, &doy_m);
    const uint32_t mp  = (5u*doy_m + 2u) / 153u;
    const uint32_t m   = mp < 10u ? mp + 3u : mp - 9u;
    const uint32_t dom = doy_m - (153u*mp + 2u)/5u + 1u;
    const int32_t  y   = (int32_t)(yoe + era * 400u) + (int32_t)(m <= 2u);
    int32_t doy = (int32_t)(GREGORIAN_CUM_DAYS[m-1] + dom);
    const int32_t leap = ((y % 4 == 0) & (y % 100 != 0)) | (y % 400 == 0);
    if ((int32_t)m > 2) doy += leap;
    return doy;
}

// ===========================================================================
// 4-unrolled calendar loop with compile-time days-per-unit constant (dpu).
//
// ts → floor_div → int32 days → per-field calendar helper → int64 output.
//
// Floor division (branching-free):
//   q = ts / _U                  (UMULH for literal constant _U)
//   r = ts - q * _U
//   days = q + (r >> 63)         (+0 if r≥0, -1 if r<0 → floor correction)
// ===========================================================================
#define CAL_LOOP_4(fn, dpu)                                                          \
    do {                                                                              \
        const int64_t _U = (dpu);                                                    \
        size_t _i = 0;                                                                \
        for (; _i + 3 < n; _i += 4) {                                                \
            int64_t _q0 = src[_i  ] / _U, _q1 = src[_i+1] / _U,                    \
                    _q2 = src[_i+2] / _U, _q3 = src[_i+3] / _U;                    \
            dst[_i  ] = (fn)((int32_t)(_q0 + ((src[_i  ] - _q0*_U) >> 63)));        \
            dst[_i+1] = (fn)((int32_t)(_q1 + ((src[_i+1] - _q1*_U) >> 63)));        \
            dst[_i+2] = (fn)((int32_t)(_q2 + ((src[_i+2] - _q2*_U) >> 63)));        \
            dst[_i+3] = (fn)((int32_t)(_q3 + ((src[_i+3] - _q3*_U) >> 63)));        \
        }                                                                             \
        for (; _i < n; ++_i) {                                                        \
            int64_t _q = src[_i] / _U;                                               \
            dst[_i] = (fn)((int32_t)(_q + ((src[_i] - _q*_U) >> 63)));              \
        }                                                                             \
    } while(0)

// ===========================================================================
// YEAR
// ===========================================================================
static void year_scalar(const int64_t* src, int64_t* dst, size_t n, int unit_code) {
    switch (unit_code) {
        case 0:  CAL_LOOP_4(days_to_year, 86400LL);           break;
        case 1:  CAL_LOOP_4(days_to_year, 86400000LL);        break;
        case 2:  CAL_LOOP_4(days_to_year, 86400000000LL);     break;
        case 3:  CAL_LOOP_4(days_to_year, 86400000000000LL);  break;
        default: CAL_LOOP_4(days_to_year, 86400000000LL);     break;
    }
}

// ===========================================================================
// MONTH
// ===========================================================================
static void month_scalar(const int64_t* src, int64_t* dst, size_t n, int unit_code) {
    switch (unit_code) {
        case 0:  CAL_LOOP_4(days_to_month, 86400LL);           break;
        case 1:  CAL_LOOP_4(days_to_month, 86400000LL);        break;
        case 2:  CAL_LOOP_4(days_to_month, 86400000000LL);     break;
        case 3:  CAL_LOOP_4(days_to_month, 86400000000000LL);  break;
        default: CAL_LOOP_4(days_to_month, 86400000000LL);     break;
    }
}

// ===========================================================================
// DAY OF MONTH
// ===========================================================================
static void dom_scalar(const int64_t* src, int64_t* dst, size_t n, int unit_code) {
    switch (unit_code) {
        case 0:  CAL_LOOP_4(days_to_dom, 86400LL);           break;
        case 1:  CAL_LOOP_4(days_to_dom, 86400000LL);        break;
        case 2:  CAL_LOOP_4(days_to_dom, 86400000000LL);     break;
        case 3:  CAL_LOOP_4(days_to_dom, 86400000000000LL);  break;
        default: CAL_LOOP_4(days_to_dom, 86400000000LL);     break;
    }
}

// ===========================================================================
// QUARTER
// ===========================================================================
static void quarter_scalar(const int64_t* src, int64_t* dst, size_t n, int unit_code) {
    switch (unit_code) {
        case 0:  CAL_LOOP_4(days_to_quarter, 86400LL);           break;
        case 1:  CAL_LOOP_4(days_to_quarter, 86400000LL);        break;
        case 2:  CAL_LOOP_4(days_to_quarter, 86400000000LL);     break;
        case 3:  CAL_LOOP_4(days_to_quarter, 86400000000000LL);  break;
        default: CAL_LOOP_4(days_to_quarter, 86400000000LL);     break;
    }
}

// ===========================================================================
// DAY OF YEAR
// ===========================================================================
static void doy_scalar(const int64_t* src, int64_t* dst, size_t n, int unit_code) {
    switch (unit_code) {
        case 0:  CAL_LOOP_4(days_to_doy, 86400LL);           break;
        case 1:  CAL_LOOP_4(days_to_doy, 86400000LL);        break;
        case 2:  CAL_LOOP_4(days_to_doy, 86400000000LL);     break;
        case 3:  CAL_LOOP_4(days_to_doy, 86400000000000LL);  break;
        default: CAL_LOOP_4(days_to_doy, 86400000000LL);     break;
    }
}

// ===========================================================================
// Public dispatch functions
// No NEON/AVX2 candidates: 64-bit integer division cannot be vectorised via
// NEON (no vdivq_s64) or AVX2 (no _mm256_div_epi64).  The constant-divisor
// scalar path is the optimal implementation on all current targets.
// ===========================================================================

using dp_fn_t = void (*)(const int64_t*, int64_t*, size_t, int);

void simd_datepart_minute(const int64_t* src, int64_t* dst, size_t n, int unit_code) {
    static std::atomic<dp_fn_t> cache{nullptr};
    dp_fn_t fn = simd::select_dispatch<dp_fn_t>(cache, {}, minute_scalar);
    fn(src, dst, n, unit_code);
}

void simd_datepart_hour(const int64_t* src, int64_t* dst, size_t n, int unit_code) {
    static std::atomic<dp_fn_t> cache{nullptr};
    dp_fn_t fn = simd::select_dispatch<dp_fn_t>(cache, {}, hour_scalar);
    fn(src, dst, n, unit_code);
}

void simd_datepart_second(const int64_t* src, int64_t* dst, size_t n, int unit_code) {
    static std::atomic<dp_fn_t> cache{nullptr};
    dp_fn_t fn = simd::select_dispatch<dp_fn_t>(cache, {}, second_scalar);
    fn(src, dst, n, unit_code);
}

void simd_datepart_year(const int64_t* src, int64_t* dst, size_t n, int unit_code) {
    static std::atomic<dp_fn_t> cache{nullptr};
    dp_fn_t fn = simd::select_dispatch<dp_fn_t>(cache, {}, year_scalar);
    fn(src, dst, n, unit_code);
}

void simd_datepart_month(const int64_t* src, int64_t* dst, size_t n, int unit_code) {
    static std::atomic<dp_fn_t> cache{nullptr};
    dp_fn_t fn = simd::select_dispatch<dp_fn_t>(cache, {}, month_scalar);
    fn(src, dst, n, unit_code);
}

void simd_datepart_day(const int64_t* src, int64_t* dst, size_t n, int unit_code) {
    static std::atomic<dp_fn_t> cache{nullptr};
    dp_fn_t fn = simd::select_dispatch<dp_fn_t>(cache, {}, dom_scalar);
    fn(src, dst, n, unit_code);
}

void simd_datepart_quarter(const int64_t* src, int64_t* dst, size_t n, int unit_code) {
    static std::atomic<dp_fn_t> cache{nullptr};
    dp_fn_t fn = simd::select_dispatch<dp_fn_t>(cache, {}, quarter_scalar);
    fn(src, dst, n, unit_code);
}

void simd_datepart_dayofyear(const int64_t* src, int64_t* dst, size_t n, int unit_code) {
    static std::atomic<dp_fn_t> cache{nullptr};
    dp_fn_t fn = simd::select_dispatch<dp_fn_t>(cache, {}, doy_scalar);
    fn(src, dst, n, unit_code);
}

