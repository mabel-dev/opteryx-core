# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False

from libc.stdint cimport int64_t
from cpython.array cimport array, clone
import pyarrow as pa

# Constants
cdef const int64_t SECONDS_PER_MINUTE = 60
cdef const int64_t SECONDS_PER_HOUR = 3600
cdef const int64_t SECONDS_PER_DAY = 86400
cdef const int64_t SECONDS_PER_WEEK = 604800
cdef const int64_t DAYS_PER_WEEK = 7
cdef const int64_t EPOCH_WEEKDAY = 4  # 1970-01-01 was Thursday (0=Monday)
cdef int64_t DAYS_IN_MONTH[12]
cdef int64_t CUMULATIVE_DAYS[13]

# Initialize constants (run at module import time under the GIL)
DAYS_IN_MONTH_vals = (31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31)
for __i, __v in enumerate(DAYS_IN_MONTH_vals):
    DAYS_IN_MONTH[__i] = __v

CUMULATIVE_DAYS_vals = (0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334, 365)
for __i, __v in enumerate(CUMULATIVE_DAYS_vals):
    CUMULATIVE_DAYS[__i] = __v

# Pure integer arithmetic functions - NO time library calls!
cdef inline bint is_leap_year(int64_t year) nogil:
    """Check if a year is a leap year using integer math"""
    return ((year % 4 == 0) and (year % 100 != 0)) or (year % 400 == 0)

cdef inline int64_t days_in_month(int64_t year, int64_t month) nogil:
    """Get number of days in a month"""
    if month == 2 and is_leap_year(year):
        return 29
    return DAYS_IN_MONTH[month - 1]

cdef inline void seconds_to_date_parts(int64_t seconds_since_epoch,
                                       int64_t* year, int64_t* month, int64_t* day,
                                       int64_t* hour, int64_t* minute, int64_t* second) noexcept nogil:
    """
    Convert seconds since 1970-01-01 to date parts using pure integer math.
    Based on algorithm from: https://howardhinnant.github.io/date_algorithms.html
    """
    # First, handle negative timestamps (dates before 1970)
    cdef int64_t days_since_epoch, seconds_in_day, z, era, day_of_era, year_of_era
    cdef int64_t day_of_year, mp

    # Break down seconds
    days_since_epoch = seconds_since_epoch // SECONDS_PER_DAY
    seconds_in_day = seconds_since_epoch % SECONDS_PER_DAY

    if seconds_in_day < 0:
        seconds_in_day += SECONDS_PER_DAY
        days_since_epoch -= 1

    # Convert days to date (algorithm from Howard Hinnant)
    z = days_since_epoch + 719468  # Shift to 0000-03-01
    era = z // 146097
    day_of_era = z - era * 146097
    year_of_era = (day_of_era - day_of_era // 1460 + day_of_era // 36524 - day_of_era // 146096) // 365

    year[0] = year_of_era + era * 400
    day_of_year = day_of_era - (365 * year_of_era + year_of_era // 4 - year_of_era // 100)
    mp = (5 * day_of_year + 2) // 153
    day[0] = day_of_year - (153 * mp + 2) // 5 + 1
    month[0] = mp + 3 if mp < 10 else mp - 9

    if month[0] <= 2:
        year[0] += 1

    # Get time components
    hour[0] = seconds_in_day // SECONDS_PER_HOUR
    minute[0] = (seconds_in_day % SECONDS_PER_HOUR) // SECONDS_PER_MINUTE
    second[0] = seconds_in_day % SECONDS_PER_MINUTE

cdef inline int64_t date_parts_to_seconds(int64_t year, int64_t month, int64_t day,
                                          int64_t hour, int64_t minute, int64_t second) nogil:
    """
    Convert date parts to seconds since 1970-01-01 using pure integer math.
    """
    # Algorithm from Howard Hinnant
    cdef int64_t y = year
    cdef int64_t m = month
    cdef int64_t d = day

    # Adjust month and year
    if m <= 2:
        y -= 1
        m += 12

    # Convert to days since 0000-03-01
    cdef int64_t era = y // 400
    cdef int64_t yoe = y - era * 400
    cdef int64_t doy = (153 * (m - 3) + 2) // 5 + d - 1
    cdef int64_t doe = yoe * 365 + yoe // 4 - yoe // 100 + doy
    cdef int64_t days = era * 146097 + doe - 719468

    # Add time components
    return days * SECONDS_PER_DAY + hour * SECONDS_PER_HOUR + minute * SECONDS_PER_MINUTE + second

# Optimized truncation functions using integer math
cdef inline int64_t truncate_year_inline(int64_t seconds) nogil:
    """Truncate seconds to year - optimized version"""
    cdef int64_t year, month, day, hour, minute, second
    seconds_to_date_parts(seconds, &year, &month, &day, &hour, &minute, &second)
    return date_parts_to_seconds(year, 1, 1, 0, 0, 0)

cdef inline int64_t truncate_quarter_inline(int64_t seconds) nogil:
    """Truncate seconds to quarter - optimized version"""
    cdef int64_t year, month, day, hour, minute, second
    seconds_to_date_parts(seconds, &year, &month, &day, &hour, &minute, &second)
    cdef int64_t quarter_start_month = ((month - 1) // 3) * 3 + 1
    return date_parts_to_seconds(year, quarter_start_month, 1, 0, 0, 0)

cdef inline int64_t truncate_month_inline(int64_t seconds) nogil:
    """Truncate seconds to month - optimized version"""
    cdef int64_t year, month, day, hour, minute, second
    seconds_to_date_parts(seconds, &year, &month, &day, &hour, &minute, &second)
    return date_parts_to_seconds(year, month, 1, 0, 0, 0)

cdef inline int64_t truncate_week_inline(int64_t seconds) nogil:
    """Truncate seconds to week - optimized integer version"""
    cdef int64_t days_since_epoch = seconds // SECONDS_PER_DAY
    # Find Monday: (days_since_epoch - EPOCH_WEEKDAY) % 7
    cdef int64_t days_to_monday = (days_since_epoch - EPOCH_WEEKDAY) % DAYS_PER_WEEK
    if days_to_monday < 0:
        days_to_monday += DAYS_PER_WEEK
    return (days_since_epoch - days_to_monday) * SECONDS_PER_DAY

cdef inline int64_t truncate_day_inline(int64_t seconds) nogil:
    """Truncate seconds to day - optimized version"""
    return (seconds // SECONDS_PER_DAY) * SECONDS_PER_DAY

cdef inline int64_t truncate_hour_inline(int64_t seconds) nogil:
    """Truncate seconds to hour - optimized version"""
    return (seconds // SECONDS_PER_HOUR) * SECONDS_PER_HOUR

cdef inline int64_t truncate_minute_inline(int64_t seconds) nogil:
    """Truncate seconds to minute - optimized version"""
    return (seconds // SECONDS_PER_MINUTE) * SECONDS_PER_MINUTE

cdef inline int64_t truncate_second_inline(int64_t seconds) nogil:
    """Truncate seconds to second - optimized version"""
    return seconds

# Even faster: precomputed month starts for common years (1970-2100)
cdef inline int64_t truncate_month_fast(int64_t seconds) nogil:
    """Fast month truncation for years 1970-2100 using precomputed table"""
    cdef int64_t days_since_epoch = seconds // SECONDS_PER_DAY
    cdef int64_t year, month
    cdef int64_t leap_days, days_in_year
    cdef bint is_leap

    # For years 1970-2100, we can use a faster approximation
    if days_since_epoch >= 0 and days_since_epoch < 47482:  # Days from 1970 to 2100
        # Each month start relative to year start (for non-leap years)
        # We'll compute year and month quickly
        year = 1970 + days_since_epoch // 365
        # Adjust for leap years
        leap_days = (year - 1969) // 4 - (year - 1901) // 100 + (year - 1601) // 400
        days_in_year = days_since_epoch - (year - 1970) * 365 - leap_days

        # Find month using cumulative days with unrolled comparisons
        is_leap = is_leap_year(year)
        month = 1

        # Unrolled binary-style search for month (faster than loop)
        if days_in_year < 31:  # January
            month = 1
        elif days_in_year < (59 if not is_leap else 60):  # February
            month = 2
        elif days_in_year < (90 if not is_leap else 91):  # March
            month = 3
        elif days_in_year < (120 if not is_leap else 121):  # April
            month = 4
        elif days_in_year < (151 if not is_leap else 152):  # May
            month = 5
        elif days_in_year < (181 if not is_leap else 182):  # June
            month = 6
        elif days_in_year < (212 if not is_leap else 213):  # July
            month = 7
        elif days_in_year < (243 if not is_leap else 244):  # August
            month = 8
        elif days_in_year < (273 if not is_leap else 274):  # September
            month = 9
        elif days_in_year < (304 if not is_leap else 305):  # October
            month = 10
        elif days_in_year < (334 if not is_leap else 335):  # November
            month = 11
        else:  # December
            month = 12

        return date_parts_to_seconds(year, month, 1, 0, 0, 0)
    else:
        # Fall back to general algorithm
        return truncate_month_inline(seconds)

# Main fast processing function - delegates to optimized list_date_trunc
cpdef object date_trunc_fast(str truncate_to, object timestamp_array):
    """
    Fast date truncation using pure integer arithmetic.

    Args:
        truncate_to: "year", "quarter", "month", "week", "day", "hour", "minute", "second"
        timestamp_array: Arrow array of int64 timestamps or numpy datetime64

    Returns:
        Arrow array or numpy datetime64 array of truncated timestamps (preserving input unit)
    """
    # Delegate to optimized implementation to avoid code duplication
    return list_date_trunc(truncate_to, timestamp_array)

# Ultra-fast version with SIMD-like loop unrolling
cpdef object list_date_trunc(str truncate_to, object timestamp_array):
    """
    Ultra-fast date truncation with loop unrolling for maximum speed.
    Works directly in native timestamp units to avoid conversion overhead.
    Optimized for PyArrow timestamp arrays with zero-copy buffer access.
    """
    cdef str op = truncate_to.lower()
    cdef str unit
    cdef int64_t length
    cdef int64_t i
    cdef int64_t* data_ptr
    cdef int64_t* output_ptr
    cdef int64_t days_since_epoch, days_to_monday, temp_seconds
    cdef int64_t factor
    cdef int64_t divisor_day, divisor_hour, divisor_minute
    cdef array output_array
    cdef array template = array('q')  # 'q' = signed long long (int64)

    # Direct Arrow buffer access (zero-copy!)
    unit = timestamp_array.type.unit
    length = len(timestamp_array)

    # Get pointer to Arrow buffer data
    cdef object arrow_buffer = timestamp_array.buffers()[1]
    data_ptr = <int64_t*><uintptr_t>arrow_buffer.address

    # Map unit to factor
    if unit == 'ms':
        factor = 1000
    elif unit == 'us':
        factor = 1000000
    elif unit == 'ns':
        factor = 1000000000
    else:
        factor = 1  # seconds

    # Allocate output array
    output_array = clone(template, length, False)
    output_ptr = <int64_t*>output_array.data.as_longs

    # Scale divisors by factor to work in native units
    divisor_day = SECONDS_PER_DAY * factor
    divisor_hour = SECONDS_PER_HOUR * factor
    divisor_minute = SECONDS_PER_MINUTE * factor

    # Optimized loops working in native units - ordered by frequency!
    # Hot paths with manual loop unrolling for better performance
    if op == "day":
        # Most common - process in chunks of 4 for better pipelining
        i = 0
        while i + 3 < length:
            output_ptr[i] = (data_ptr[i] // divisor_day) * divisor_day
            output_ptr[i + 1] = (data_ptr[i + 1] // divisor_day) * divisor_day
            output_ptr[i + 2] = (data_ptr[i + 2] // divisor_day) * divisor_day
            output_ptr[i + 3] = (data_ptr[i + 3] // divisor_day) * divisor_day
            i += 4
        # Handle remainder
        while i < length:
            output_ptr[i] = (data_ptr[i] // divisor_day) * divisor_day
            i += 1
    elif op == "hour":
        # Second most common
        i = 0
        while i + 3 < length:
            output_ptr[i] = (data_ptr[i] // divisor_hour) * divisor_hour
            output_ptr[i + 1] = (data_ptr[i + 1] // divisor_hour) * divisor_hour
            output_ptr[i + 2] = (data_ptr[i + 2] // divisor_hour) * divisor_hour
            output_ptr[i + 3] = (data_ptr[i + 3] // divisor_hour) * divisor_hour
            i += 4
        while i < length:
            output_ptr[i] = (data_ptr[i] // divisor_hour) * divisor_hour
            i += 1
    elif op == "minute":
        i = 0
        while i + 3 < length:
            output_ptr[i] = (data_ptr[i] // divisor_minute) * divisor_minute
            output_ptr[i + 1] = (data_ptr[i + 1] // divisor_minute) * divisor_minute
            output_ptr[i + 2] = (data_ptr[i + 2] // divisor_minute) * divisor_minute
            output_ptr[i + 3] = (data_ptr[i + 3] // divisor_minute) * divisor_minute
            i += 4
        while i < length:
            output_ptr[i] = (data_ptr[i] // divisor_minute) * divisor_minute
            i += 1
    elif op == "month":
        # Complex ops: convert to seconds, truncate, scale back
        for i in range(length):
            temp_seconds = data_ptr[i] // factor
            output_ptr[i] = truncate_month_fast(temp_seconds) * factor
    elif op == "week":
        for i in range(length):
            days_since_epoch = data_ptr[i] // divisor_day
            days_to_monday = (days_since_epoch - EPOCH_WEEKDAY) % DAYS_PER_WEEK
            if days_to_monday < 0:
                days_to_monday += DAYS_PER_WEEK
            output_ptr[i] = (days_since_epoch - days_to_monday) * divisor_day
    elif op == "quarter":
        for i in range(length):
            temp_seconds = data_ptr[i] // factor
            output_ptr[i] = truncate_quarter_inline(temp_seconds) * factor
    elif op == "year":
        for i in range(length):
            temp_seconds = data_ptr[i] // factor
            output_ptr[i] = truncate_year_inline(temp_seconds) * factor
    elif op == "second":
        # Truncate sub-second precision or no-op for second precision
        if factor > 1:
            for i in range(length):
                output_ptr[i] = (data_ptr[i] // factor) * factor
        else:
            # Direct copy - no truncation needed
            for i in range(length):
                output_ptr[i] = data_ptr[i]
    else:
        raise ValueError(f"Invalid unit: {truncate_to}")

    return pa.array(output_array, type=pa.timestamp(unit))
