#pragma once

#include <cstddef>
#include <cstdint>

#ifdef __cplusplus
extern "C++" {
#endif

/**
 * SIMD-accelerated date-part extraction for microsecond-timestamp vectors.
 *
 * Each function processes N consecutive int64 timestamps and writes the
 * requested date-part (0-based) into dst.
 *
 * unit_code encodes the timestamp precision:
 *   0 = seconds
 *   1 = milliseconds
 *   2 = microseconds  (Q19 hot path)
 *   3 = nanoseconds
 *
 * Runtime dispatch selects NEON (ARM64), AVX2 (x86-64), or scalar.
 * All three SIMD paths use compile-time-constant divisors so the compiler
 * emits UMULH/MULQ-based magic-number division instead of UDIV.
 */

/** Extract minute-of-hour (0-59) from a timestamp vector. */
void simd_datepart_minute(const int64_t* src, int64_t* dst, size_t n, int unit_code);

/** Extract hour-of-day (0-23) from a timestamp vector. */
void simd_datepart_hour(const int64_t* src, int64_t* dst, size_t n, int unit_code);

/** Extract second-of-minute (0-59) from a timestamp vector. */
void simd_datepart_second(const int64_t* src, int64_t* dst, size_t n, int unit_code);

/** Extract year from a timestamp vector (Howard Hinnant, int32 calendar arithmetic). */
void simd_datepart_year(const int64_t* src, int64_t* dst, size_t n, int unit_code);

/** Extract month-of-year (1-12) from a timestamp vector. */
void simd_datepart_month(const int64_t* src, int64_t* dst, size_t n, int unit_code);

/** Extract day-of-month (1-31) from a timestamp vector. */
void simd_datepart_day(const int64_t* src, int64_t* dst, size_t n, int unit_code);

/** Extract quarter (1-4) from a timestamp vector. */
void simd_datepart_quarter(const int64_t* src, int64_t* dst, size_t n, int unit_code);

/** Extract day-of-year (1-366) from a timestamp vector. */
void simd_datepart_dayofyear(const int64_t* src, int64_t* dst, size_t n, int unit_code);

#ifdef __cplusplus
}
#endif
