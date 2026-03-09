# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False

"""
Compiled DATEPART extraction for Draken vectors.

MUST be included AFTER vector_date_trunc.pyx in vector_ops.pyx.
Relies on helpers defined there:
  seconds_to_date_parts(), is_leap_year(), DAYS_IN_MONTH[], CUMULATIVE_DAYS[],
  SECONDS_PER_MINUTE, SECONDS_PER_HOUR, SECONDS_PER_DAY, EPOCH_WEEKDAY.

Phase 3 Implementation:
- No NumPy or PyArrow in the Draken-vector hot path
- Pure integer arithmetic for all extraction logic (sub-second and calendar)
- Loop unrolling for minute/hour/second (simple modulo ops)
- from_sequence() cimport for zero-copy Int64Vector construction
- Full calendar-unit coverage: year/month/day/dayofweek/dayofyear/quarter
- GC safety: _arrow_data_buf is overridden to the cpython.array backing store

Phase 4 (Future):
- AVX2/NEON SIMD intrinsics for 2-4x additional speedup
- OpenMP parallel extraction for >10 M row vectors
"""

from libc.stdint cimport int64_t, int32_t, uint8_t, uint16_t, uint32_t
from libc.stddef cimport size_t
from cpython.array cimport array, clone

from opteryx.draken.vectors.timestamp_vector cimport TimestampVector
from opteryx.draken.vectors.int64_vector cimport Int64Vector, from_sequence as int64_from_sequence
from opteryx.draken.vectors.dictionary_vector cimport DictionaryVector

# ---------------------------------------------------------------------------
# SIMD-accelerated kernels for minute / hour / second extraction.
# Runtime dispatch (NEON / AVX2 / scalar) is handled inside the C++ layer.
# ---------------------------------------------------------------------------
cdef extern from "simd_datepart.h":
    void simd_datepart_minute(const int64_t* src, int64_t* dst, size_t n, int unit_code) noexcept nogil
    void simd_datepart_hour  (const int64_t* src, int64_t* dst, size_t n, int unit_code) noexcept nogil
    void simd_datepart_second(const int64_t* src, int64_t* dst, size_t n, int unit_code) noexcept nogil

# ---------------------------------------------------------------------------
# Sub-second unit constants unique to this module
# (SECONDS_PER_* and EPOCH_WEEKDAY come from vector_date_trunc.pyx)
# ---------------------------------------------------------------------------
cdef const int64_t MICROSECONDS_PER_SECOND  = 1_000_000
cdef const int64_t MICROSECONDS_PER_MINUTE  = 60_000_000
cdef const int64_t MICROSECONDS_PER_HOUR    = 3_600_000_000
cdef const int64_t MICROSECONDS_PER_DAY     = 86_400_000_000

cdef const int64_t MILLISECONDS_PER_SECOND  = 1_000
cdef const int64_t MILLISECONDS_PER_MINUTE  = 60_000
cdef const int64_t MILLISECONDS_PER_HOUR    = 3_600_000
cdef const int64_t MILLISECONDS_PER_DAY     = 86_400_000

cdef const int64_t NANOSECONDS_PER_SECOND   = 1_000_000_000
cdef const int64_t NANOSECONDS_PER_MINUTE   = 60_000_000_000
cdef const int64_t NANOSECONDS_PER_HOUR     = 3_600_000_000_000
cdef const int64_t NANOSECONDS_PER_DAY      = 86_400_000_000_000

# Typed bounds used in _detect_seconds_divisor (all values fit in int64_t).
# Written with explicit <int64_t> casts so they remain C numeric constants
# inside noexcept nogil contexts.
cdef const int64_t _SEC_UPPER  = <int64_t>10000000000        # 10^10
cdef const int64_t _MSEC_LOWER = <int64_t>1000000000000      # 10^12
cdef const int64_t _MSEC_UPPER = <int64_t>10000000000000     # 10^13
cdef const int64_t _USEC_LOWER = <int64_t>1000000000000000   # 10^15
cdef const int64_t _USEC_UPPER = <int64_t>10000000000000000  # 10^16
cdef const int64_t _NSEC_LOWER = <int64_t>1000000000000000000  # 10^18


# ---------------------------------------------------------------------------
# PRECISION DETECTION: convert any raw int64 timestamp to seconds
# ---------------------------------------------------------------------------

cdef inline int64_t _detect_seconds_divisor(int64_t sample_val) noexcept nogil:
    """Return the divisor that converts a raw int64 timestamp value to seconds.

    Uses magnitude of the sample value to determine precision.
    Handles zero (epoch) and negative values gracefully.
    Returns 0 only for values that fall below the seconds-range lower bound
    (i.e. < 1e9 absolute) — callers should treat 0 as an error.
    """
    if sample_val < 0:
        sample_val = -sample_val
    if sample_val == 0:
        return 1  # Epoch (1970-01-01) — treat as seconds
    if 1_000_000_000 <= sample_val < _SEC_UPPER:
        return 1
    elif _MSEC_LOWER <= sample_val < _MSEC_UPPER:
        return MILLISECONDS_PER_SECOND
    elif _USEC_LOWER <= sample_val < _USEC_UPPER:
        return MICROSECONDS_PER_SECOND
    elif sample_val >= _NSEC_LOWER:
        return NANOSECONDS_PER_SECOND
    return 0  # below seconds range — cannot determine


# ---------------------------------------------------------------------------
# Internal helpers for Int64Vector kernel precision detection
# ---------------------------------------------------------------------------

cdef inline int64_t _find_seconds_divisor_int64(
        int64_t* data_ptr, int64_t length) noexcept nogil:
    """Scan first element (and fall back to searching) for precision detection."""
    cdef int64_t i = 0
    cdef int64_t sd
    while i < length:
        sd = _detect_seconds_divisor(data_ptr[i])
        if sd != 0:
            return sd
        i += 1
    return 1  # All zeros — default to seconds


# ===========================================================================
# MACRO: _mk_result(output_array, length) — build Int64Vector from array
#   result = int64_from_sequence(<int64_t[:length:1]>output_ptr)
#   result._arrow_data_buf = output_array   (keep backing store alive)
# Used inline in every kernel below.
# ===========================================================================


# ===========================================================================
# SECTION 1 — TimestampVector kernels
# ===========================================================================

# ---------------------------------------------------------------------------
# 1a. Sub-second parts: minute, hour, second  (pure modulo arithmetic)
# ---------------------------------------------------------------------------

cpdef Int64Vector vector_datepart_minute(TimestampVector timestamps):
    """Extract minute (0–59) from TimestampVector via SIMD dispatch."""
    cdef str unit = timestamps.timestamp_unit
    cdef int64_t length = <int64_t>timestamps.ptr.length
    cdef int64_t* data_ptr = <int64_t*>timestamps.ptr.data
    cdef int64_t empty_sentinel = 0

    # unit_code: 0=s, 1=ms, 2=us, 3=ns  (matches simd_datepart.h)
    cdef int unit_code
    if unit == 'us':
        unit_code = 2
    elif unit == 'ms':
        unit_code = 1
    elif unit == 'ns':
        unit_code = 3
    else:
        unit_code = 0

    if length == 0:
        return int64_from_sequence(<int64_t[:0:1]>&empty_sentinel)

    cdef array template = array('l')
    cdef array output_array = clone(template, length, False)
    cdef int64_t* output_ptr = <int64_t*>output_array.data.as_longs

    simd_datepart_minute(data_ptr, output_ptr, <size_t>length, unit_code)

    cdef Int64Vector result = int64_from_sequence(<int64_t[:length:1]>output_ptr)
    result._arrow_data_buf = output_array
    return result


cpdef Int64Vector vector_datepart_hour(TimestampVector timestamps):
    """Extract hour (0–23) from TimestampVector via SIMD dispatch."""
    cdef str unit = timestamps.timestamp_unit
    cdef int64_t length = <int64_t>timestamps.ptr.length
    cdef int64_t* data_ptr = <int64_t*>timestamps.ptr.data
    cdef int64_t empty_sentinel = 0

    cdef int unit_code
    if unit == 'us':
        unit_code = 2
    elif unit == 'ms':
        unit_code = 1
    elif unit == 'ns':
        unit_code = 3
    else:
        unit_code = 0

    if length == 0:
        return int64_from_sequence(<int64_t[:0:1]>&empty_sentinel)

    cdef array template = array('l')
    cdef array output_array = clone(template, length, False)
    cdef int64_t* output_ptr = <int64_t*>output_array.data.as_longs

    simd_datepart_hour(data_ptr, output_ptr, <size_t>length, unit_code)

    cdef Int64Vector result = int64_from_sequence(<int64_t[:length:1]>output_ptr)
    result._arrow_data_buf = output_array
    return result


cpdef Int64Vector vector_datepart_second(TimestampVector timestamps):
    """Extract second (0–59) from TimestampVector via SIMD dispatch."""
    cdef str unit = timestamps.timestamp_unit
    cdef int64_t length = <int64_t>timestamps.ptr.length
    cdef int64_t* data_ptr = <int64_t*>timestamps.ptr.data
    cdef int64_t empty_sentinel = 0

    cdef int unit_code
    if unit == 'us':
        unit_code = 2
    elif unit == 'ms':
        unit_code = 1
    elif unit == 'ns':
        unit_code = 3
    else:
        unit_code = 0

    if length == 0:
        return int64_from_sequence(<int64_t[:0:1]>&empty_sentinel)

    cdef array template = array('l')
    cdef array output_array = clone(template, length, False)
    cdef int64_t* output_ptr = <int64_t*>output_array.data.as_longs

    simd_datepart_second(data_ptr, output_ptr, <size_t>length, unit_code)

    cdef Int64Vector result = int64_from_sequence(<int64_t[:length:1]>output_ptr)
    result._arrow_data_buf = output_array
    return result


# ---------------------------------------------------------------------------
# 1b. Calendar parts for TimestampVector
#     seconds_to_date_parts(), is_leap_year(), CUMULATIVE_DAYS[] are from
#     vector_date_trunc.pyx (included before this file in vector_ops.pyx).
# ---------------------------------------------------------------------------

cpdef Int64Vector vector_datepart_year(TimestampVector timestamps):
    """Extract year from TimestampVector."""
    cdef str unit = timestamps.timestamp_unit
    cdef int64_t length = <int64_t>timestamps.ptr.length
    cdef int64_t* data_ptr = <int64_t*>timestamps.ptr.data
    cdef int64_t empty_sentinel = 0

    cdef int64_t sd  # seconds divisor
    if unit == 'us':
        sd = MICROSECONDS_PER_SECOND
    elif unit == 'ms':
        sd = MILLISECONDS_PER_SECOND
    elif unit == 'ns':
        sd = NANOSECONDS_PER_SECOND
    else:
        sd = 1

    if length == 0:
        return int64_from_sequence(<int64_t[:0:1]>&empty_sentinel)

    cdef array template = array('l')
    cdef array output_array = clone(template, length, False)
    cdef int64_t* output_ptr = <int64_t*>output_array.data.as_longs
    cdef int64_t i, year, month, day, hour, minute, second

    for i in range(length):
        seconds_to_date_parts(data_ptr[i] // sd,
                              &year, &month, &day, &hour, &minute, &second)
        output_ptr[i] = year

    cdef Int64Vector result = int64_from_sequence(<int64_t[:length:1]>output_ptr)
    result._arrow_data_buf = output_array
    return result


cpdef Int64Vector vector_datepart_month(TimestampVector timestamps):
    """Extract month (1–12) from TimestampVector."""
    cdef str unit = timestamps.timestamp_unit
    cdef int64_t length = <int64_t>timestamps.ptr.length
    cdef int64_t* data_ptr = <int64_t*>timestamps.ptr.data
    cdef int64_t empty_sentinel = 0

    cdef int64_t sd
    if unit == 'us':
        sd = MICROSECONDS_PER_SECOND
    elif unit == 'ms':
        sd = MILLISECONDS_PER_SECOND
    elif unit == 'ns':
        sd = NANOSECONDS_PER_SECOND
    else:
        sd = 1

    if length == 0:
        return int64_from_sequence(<int64_t[:0:1]>&empty_sentinel)

    cdef array template = array('l')
    cdef array output_array = clone(template, length, False)
    cdef int64_t* output_ptr = <int64_t*>output_array.data.as_longs
    cdef int64_t i, year, month, day, hour, minute, second

    for i in range(length):
        seconds_to_date_parts(data_ptr[i] // sd,
                              &year, &month, &day, &hour, &minute, &second)
        output_ptr[i] = month

    cdef Int64Vector result = int64_from_sequence(<int64_t[:length:1]>output_ptr)
    result._arrow_data_buf = output_array
    return result


cpdef Int64Vector vector_datepart_day(TimestampVector timestamps):
    """Extract day-of-month (1–31) from TimestampVector."""
    cdef str unit = timestamps.timestamp_unit
    cdef int64_t length = <int64_t>timestamps.ptr.length
    cdef int64_t* data_ptr = <int64_t*>timestamps.ptr.data
    cdef int64_t empty_sentinel = 0

    cdef int64_t sd
    if unit == 'us':
        sd = MICROSECONDS_PER_SECOND
    elif unit == 'ms':
        sd = MILLISECONDS_PER_SECOND
    elif unit == 'ns':
        sd = NANOSECONDS_PER_SECOND
    else:
        sd = 1

    if length == 0:
        return int64_from_sequence(<int64_t[:0:1]>&empty_sentinel)

    cdef array template = array('l')
    cdef array output_array = clone(template, length, False)
    cdef int64_t* output_ptr = <int64_t*>output_array.data.as_longs
    cdef int64_t i, year, month, day, hour, minute, second

    for i in range(length):
        seconds_to_date_parts(data_ptr[i] // sd,
                              &year, &month, &day, &hour, &minute, &second)
        output_ptr[i] = day

    cdef Int64Vector result = int64_from_sequence(<int64_t[:length:1]>output_ptr)
    result._arrow_data_buf = output_array
    return result


cpdef Int64Vector vector_datepart_dayofweek(TimestampVector timestamps):
    """Extract day-of-week (0=Monday … 6=Sunday) from TimestampVector.

    Uses pure integer arithmetic: days_since_epoch + EPOCH_WEEKDAY (=4, Thursday)
    then modulo 7.  Negative timestamps (pre-epoch) are handled correctly.
    """
    cdef str unit = timestamps.timestamp_unit
    cdef int64_t length = <int64_t>timestamps.ptr.length
    cdef int64_t* data_ptr = <int64_t*>timestamps.ptr.data
    cdef int64_t empty_sentinel = 0

    cdef int64_t day_divisor
    if unit == 'us':
        day_divisor = MICROSECONDS_PER_DAY
    elif unit == 'ms':
        day_divisor = MILLISECONDS_PER_DAY
    elif unit == 'ns':
        day_divisor = NANOSECONDS_PER_DAY
    else:
        day_divisor = SECONDS_PER_DAY

    if length == 0:
        return int64_from_sequence(<int64_t[:0:1]>&empty_sentinel)

    cdef array template = array('l')
    cdef array output_array = clone(template, length, False)
    cdef int64_t* output_ptr = <int64_t*>output_array.data.as_longs
    cdef int64_t i, d

    for i in range(length):
        d = (data_ptr[i] // day_divisor + EPOCH_WEEKDAY) % 7
        if d < 0:
            d += 7
        output_ptr[i] = d

    cdef Int64Vector result = int64_from_sequence(<int64_t[:length:1]>output_ptr)
    result._arrow_data_buf = output_array
    return result


cpdef Int64Vector vector_datepart_dayofyear(TimestampVector timestamps):
    """Extract day-of-year (1–366) from TimestampVector."""
    cdef str unit = timestamps.timestamp_unit
    cdef int64_t length = <int64_t>timestamps.ptr.length
    cdef int64_t* data_ptr = <int64_t*>timestamps.ptr.data
    cdef int64_t empty_sentinel = 0

    cdef int64_t sd
    if unit == 'us':
        sd = MICROSECONDS_PER_SECOND
    elif unit == 'ms':
        sd = MILLISECONDS_PER_SECOND
    elif unit == 'ns':
        sd = NANOSECONDS_PER_SECOND
    else:
        sd = 1

    if length == 0:
        return int64_from_sequence(<int64_t[:0:1]>&empty_sentinel)

    cdef array template = array('l')
    cdef array output_array = clone(template, length, False)
    cdef int64_t* output_ptr = <int64_t*>output_array.data.as_longs
    cdef int64_t i, year, month, day, hour, minute, second, doy

    for i in range(length):
        seconds_to_date_parts(data_ptr[i] // sd,
                              &year, &month, &day, &hour, &minute, &second)
        doy = CUMULATIVE_DAYS[month - 1] + day
        if month > 2 and is_leap_year(year):
            doy += 1
        output_ptr[i] = doy

    cdef Int64Vector result = int64_from_sequence(<int64_t[:length:1]>output_ptr)
    result._arrow_data_buf = output_array
    return result


cpdef Int64Vector vector_datepart_quarter(TimestampVector timestamps):
    """Extract quarter (1–4) from TimestampVector."""
    cdef str unit = timestamps.timestamp_unit
    cdef int64_t length = <int64_t>timestamps.ptr.length
    cdef int64_t* data_ptr = <int64_t*>timestamps.ptr.data
    cdef int64_t empty_sentinel = 0

    cdef int64_t sd
    if unit == 'us':
        sd = MICROSECONDS_PER_SECOND
    elif unit == 'ms':
        sd = MILLISECONDS_PER_SECOND
    elif unit == 'ns':
        sd = NANOSECONDS_PER_SECOND
    else:
        sd = 1

    if length == 0:
        return int64_from_sequence(<int64_t[:0:1]>&empty_sentinel)

    cdef array template = array('l')
    cdef array output_array = clone(template, length, False)
    cdef int64_t* output_ptr = <int64_t*>output_array.data.as_longs
    cdef int64_t i, year, month, day, hour, minute, second

    for i in range(length):
        seconds_to_date_parts(data_ptr[i] // sd,
                              &year, &month, &day, &hour, &minute, &second)
        output_ptr[i] = (month - 1) // 3 + 1

    cdef Int64Vector result = int64_from_sequence(<int64_t[:length:1]>output_ptr)
    result._arrow_data_buf = output_array
    return result


# ===========================================================================
# SECTION 2 — Int64Vector kernels (Unix timestamp integers, any precision)
# ===========================================================================

# ---------------------------------------------------------------------------
# 2a. Sub-second parts: minute, hour, second
# ---------------------------------------------------------------------------

cpdef Int64Vector vector_datepart_minute_i64(Int64Vector int64_vec):
    """Extract minute (0–59) from Int64Vector with automatic precision detection."""
    cdef int64_t length = <int64_t>int64_vec.ptr.length
    cdef int64_t* data_ptr = <int64_t*>int64_vec.ptr.data
    cdef int64_t empty_sentinel = 0

    if length == 0:
        return int64_from_sequence(<int64_t[:0:1]>&empty_sentinel)

    cdef int64_t sd = _find_seconds_divisor_int64(data_ptr, length)

    cdef int64_t divisor
    if sd == 1:
        divisor = SECONDS_PER_MINUTE
    elif sd == MILLISECONDS_PER_SECOND:
        divisor = MILLISECONDS_PER_MINUTE
    elif sd == MICROSECONDS_PER_SECOND:
        divisor = MICROSECONDS_PER_MINUTE
    else:
        divisor = NANOSECONDS_PER_MINUTE

    cdef array template = array('l')
    cdef array output_array = clone(template, length, False)
    cdef int64_t* output_ptr = <int64_t*>output_array.data.as_longs
    cdef int64_t i = 0

    while i + 3 < length:
        output_ptr[i]     = (data_ptr[i]     // divisor) % 60
        output_ptr[i + 1] = (data_ptr[i + 1] // divisor) % 60
        output_ptr[i + 2] = (data_ptr[i + 2] // divisor) % 60
        output_ptr[i + 3] = (data_ptr[i + 3] // divisor) % 60
        i += 4
    while i < length:
        output_ptr[i] = (data_ptr[i] // divisor) % 60
        i += 1

    cdef Int64Vector result = int64_from_sequence(<int64_t[:length:1]>output_ptr)
    result._arrow_data_buf = output_array
    return result


cpdef Int64Vector vector_datepart_hour_i64(Int64Vector int64_vec):
    """Extract hour (0–23) from Int64Vector with automatic precision detection."""
    cdef int64_t length = <int64_t>int64_vec.ptr.length
    cdef int64_t* data_ptr = <int64_t*>int64_vec.ptr.data
    cdef int64_t empty_sentinel = 0

    if length == 0:
        return int64_from_sequence(<int64_t[:0:1]>&empty_sentinel)

    cdef int64_t sd = _find_seconds_divisor_int64(data_ptr, length)

    cdef int64_t divisor
    if sd == 1:
        divisor = SECONDS_PER_HOUR
    elif sd == MILLISECONDS_PER_SECOND:
        divisor = MILLISECONDS_PER_HOUR
    elif sd == MICROSECONDS_PER_SECOND:
        divisor = MICROSECONDS_PER_HOUR
    else:
        divisor = NANOSECONDS_PER_HOUR

    cdef array template = array('l')
    cdef array output_array = clone(template, length, False)
    cdef int64_t* output_ptr = <int64_t*>output_array.data.as_longs
    cdef int64_t i = 0

    while i + 3 < length:
        output_ptr[i]     = (data_ptr[i]     // divisor) % 24
        output_ptr[i + 1] = (data_ptr[i + 1] // divisor) % 24
        output_ptr[i + 2] = (data_ptr[i + 2] // divisor) % 24
        output_ptr[i + 3] = (data_ptr[i + 3] // divisor) % 24
        i += 4
    while i < length:
        output_ptr[i] = (data_ptr[i] // divisor) % 24
        i += 1

    cdef Int64Vector result = int64_from_sequence(<int64_t[:length:1]>output_ptr)
    result._arrow_data_buf = output_array
    return result


cpdef Int64Vector vector_datepart_second_i64(Int64Vector int64_vec):
    """Extract second (0–59) from Int64Vector with automatic precision detection."""
    cdef int64_t length = <int64_t>int64_vec.ptr.length
    cdef int64_t* data_ptr = <int64_t*>int64_vec.ptr.data
    cdef int64_t empty_sentinel = 0

    if length == 0:
        return int64_from_sequence(<int64_t[:0:1]>&empty_sentinel)

    cdef int64_t sd = _find_seconds_divisor_int64(data_ptr, length)

    cdef array template = array('l')
    cdef array output_array = clone(template, length, False)
    cdef int64_t* output_ptr = <int64_t*>output_array.data.as_longs
    cdef int64_t i = 0

    if sd == 1:
        while i + 3 < length:
            output_ptr[i]     = data_ptr[i]     % 60
            output_ptr[i + 1] = data_ptr[i + 1] % 60
            output_ptr[i + 2] = data_ptr[i + 2] % 60
            output_ptr[i + 3] = data_ptr[i + 3] % 60
            i += 4
        while i < length:
            output_ptr[i] = data_ptr[i] % 60
            i += 1
    else:
        while i + 3 < length:
            output_ptr[i]     = (data_ptr[i]     // sd) % 60
            output_ptr[i + 1] = (data_ptr[i + 1] // sd) % 60
            output_ptr[i + 2] = (data_ptr[i + 2] // sd) % 60
            output_ptr[i + 3] = (data_ptr[i + 3] // sd) % 60
            i += 4
        while i < length:
            output_ptr[i] = (data_ptr[i] // sd) % 60
            i += 1

    cdef Int64Vector result = int64_from_sequence(<int64_t[:length:1]>output_ptr)
    result._arrow_data_buf = output_array
    return result


# ---------------------------------------------------------------------------
# 2b. Calendar parts for Int64Vector
# ---------------------------------------------------------------------------

cpdef Int64Vector vector_datepart_year_i64(Int64Vector int64_vec):
    """Extract year from Int64Vector (auto-detects timestamp precision)."""
    cdef int64_t length = <int64_t>int64_vec.ptr.length
    cdef int64_t* data_ptr = <int64_t*>int64_vec.ptr.data
    cdef int64_t empty_sentinel = 0

    if length == 0:
        return int64_from_sequence(<int64_t[:0:1]>&empty_sentinel)

    cdef int64_t sd = _find_seconds_divisor_int64(data_ptr, length)
    cdef array template = array('l')
    cdef array output_array = clone(template, length, False)
    cdef int64_t* output_ptr = <int64_t*>output_array.data.as_longs
    cdef int64_t i, year, month, day, hour, minute, second

    for i in range(length):
        seconds_to_date_parts(data_ptr[i] // sd,
                              &year, &month, &day, &hour, &minute, &second)
        output_ptr[i] = year

    cdef Int64Vector result = int64_from_sequence(<int64_t[:length:1]>output_ptr)
    result._arrow_data_buf = output_array
    return result


cpdef Int64Vector vector_datepart_month_i64(Int64Vector int64_vec):
    """Extract month (1–12) from Int64Vector (auto-detects precision)."""
    cdef int64_t length = <int64_t>int64_vec.ptr.length
    cdef int64_t* data_ptr = <int64_t*>int64_vec.ptr.data
    cdef int64_t empty_sentinel = 0

    if length == 0:
        return int64_from_sequence(<int64_t[:0:1]>&empty_sentinel)

    cdef int64_t sd = _find_seconds_divisor_int64(data_ptr, length)
    cdef array template = array('l')
    cdef array output_array = clone(template, length, False)
    cdef int64_t* output_ptr = <int64_t*>output_array.data.as_longs
    cdef int64_t i, year, month, day, hour, minute, second

    for i in range(length):
        seconds_to_date_parts(data_ptr[i] // sd,
                              &year, &month, &day, &hour, &minute, &second)
        output_ptr[i] = month

    cdef Int64Vector result = int64_from_sequence(<int64_t[:length:1]>output_ptr)
    result._arrow_data_buf = output_array
    return result


cpdef Int64Vector vector_datepart_day_i64(Int64Vector int64_vec):
    """Extract day-of-month (1–31) from Int64Vector (auto-detects precision)."""
    cdef int64_t length = <int64_t>int64_vec.ptr.length
    cdef int64_t* data_ptr = <int64_t*>int64_vec.ptr.data
    cdef int64_t empty_sentinel = 0

    if length == 0:
        return int64_from_sequence(<int64_t[:0:1]>&empty_sentinel)

    cdef int64_t sd = _find_seconds_divisor_int64(data_ptr, length)
    cdef array template = array('l')
    cdef array output_array = clone(template, length, False)
    cdef int64_t* output_ptr = <int64_t*>output_array.data.as_longs
    cdef int64_t i, year, month, day, hour, minute, second

    for i in range(length):
        seconds_to_date_parts(data_ptr[i] // sd,
                              &year, &month, &day, &hour, &minute, &second)
        output_ptr[i] = day

    cdef Int64Vector result = int64_from_sequence(<int64_t[:length:1]>output_ptr)
    result._arrow_data_buf = output_array
    return result


cpdef Int64Vector vector_datepart_dayofweek_i64(Int64Vector int64_vec):
    """Extract day-of-week (0=Monday … 6=Sunday) from Int64Vector."""
    cdef int64_t length = <int64_t>int64_vec.ptr.length
    cdef int64_t* data_ptr = <int64_t*>int64_vec.ptr.data
    cdef int64_t empty_sentinel = 0

    if length == 0:
        return int64_from_sequence(<int64_t[:0:1]>&empty_sentinel)

    cdef int64_t sd = _find_seconds_divisor_int64(data_ptr, length)
    cdef int64_t day_divisor
    if sd == 1:
        day_divisor = SECONDS_PER_DAY
    elif sd == MILLISECONDS_PER_SECOND:
        day_divisor = MILLISECONDS_PER_DAY
    elif sd == MICROSECONDS_PER_SECOND:
        day_divisor = MICROSECONDS_PER_DAY
    else:
        day_divisor = NANOSECONDS_PER_DAY

    cdef array template = array('l')
    cdef array output_array = clone(template, length, False)
    cdef int64_t* output_ptr = <int64_t*>output_array.data.as_longs
    cdef int64_t i, d

    for i in range(length):
        d = (data_ptr[i] // day_divisor + EPOCH_WEEKDAY) % 7
        if d < 0:
            d += 7
        output_ptr[i] = d

    cdef Int64Vector result = int64_from_sequence(<int64_t[:length:1]>output_ptr)
    result._arrow_data_buf = output_array
    return result


cpdef Int64Vector vector_datepart_dayofyear_i64(Int64Vector int64_vec):
    """Extract day-of-year (1–366) from Int64Vector (auto-detects precision)."""
    cdef int64_t length = <int64_t>int64_vec.ptr.length
    cdef int64_t* data_ptr = <int64_t*>int64_vec.ptr.data
    cdef int64_t empty_sentinel = 0

    if length == 0:
        return int64_from_sequence(<int64_t[:0:1]>&empty_sentinel)

    cdef int64_t sd = _find_seconds_divisor_int64(data_ptr, length)
    cdef array template = array('l')
    cdef array output_array = clone(template, length, False)
    cdef int64_t* output_ptr = <int64_t*>output_array.data.as_longs
    cdef int64_t i, year, month, day, hour, minute, second, doy

    for i in range(length):
        seconds_to_date_parts(data_ptr[i] // sd,
                              &year, &month, &day, &hour, &minute, &second)
        doy = CUMULATIVE_DAYS[month - 1] + day
        if month > 2 and is_leap_year(year):
            doy += 1
        output_ptr[i] = doy

    cdef Int64Vector result = int64_from_sequence(<int64_t[:length:1]>output_ptr)
    result._arrow_data_buf = output_array
    return result


cpdef Int64Vector vector_datepart_quarter_i64(Int64Vector int64_vec):
    """Extract quarter (1–4) from Int64Vector (auto-detects precision)."""
    cdef int64_t length = <int64_t>int64_vec.ptr.length
    cdef int64_t* data_ptr = <int64_t*>int64_vec.ptr.data
    cdef int64_t empty_sentinel = 0

    if length == 0:
        return int64_from_sequence(<int64_t[:0:1]>&empty_sentinel)

    cdef int64_t sd = _find_seconds_divisor_int64(data_ptr, length)
    cdef array template = array('l')
    cdef array output_array = clone(template, length, False)
    cdef int64_t* output_ptr = <int64_t*>output_array.data.as_longs
    cdef int64_t i, year, month, day, hour, minute, second

    for i in range(length):
        seconds_to_date_parts(data_ptr[i] // sd,
                              &year, &month, &day, &hour, &minute, &second)
        output_ptr[i] = (month - 1) // 3 + 1

    cdef Int64Vector result = int64_from_sequence(<int64_t[:length:1]>output_ptr)
    result._arrow_data_buf = output_array
    return result


# ===========================================================================
# SECTION 3 — DictionaryVector kernels
#
# These implement O(V) extraction (extract from V dictionary values, not
# N rows).  Currently BLOCKED by missing DictionaryVector API:
#   dict_vec.values(), dict_vec.values_array(), dict_vec.indices_array(),
#   DictionaryVector.from_arrays() — none exist yet.
#
# They will always raise TypeError which is caught in temporal.py,
# falling through to the Arrow slow-path decode.  The code is kept
# here as the target implementation for when the API is extended.
# ===========================================================================

cpdef object vector_datepart_minute_dict(DictionaryVector dict_vec):
    """Extract minute from DictionaryVector — O(V) optimization (future)."""
    cdef int value_type_id = dict_vec.dictionary_value_type
    cdef int INT64_TYPE_ID = 4
    cdef int TIMESTAMP_TYPE_ID = 22

    if value_type_id == INT64_TYPE_ID:
        values_vec = dict_vec.values()
        extracted_values = vector_datepart_minute_i64(values_vec)
        indices_array = dict_vec.indices_array()
        return DictionaryVector.from_arrays(indices_array, extracted_values.to_numpy())
    elif value_type_id == TIMESTAMP_TYPE_ID:
        values_vec = dict_vec.values()
        extracted_values = vector_datepart_minute(values_vec)
        indices_array = dict_vec.indices_array()
        return DictionaryVector.from_arrays(indices_array, extracted_values.to_numpy())
    else:
        raise TypeError(
            f"Cannot extract from DictionaryVector with value type_id={value_type_id}. "
            f"Supported types: INT64 (4), TIMESTAMP (22)."
        )


cpdef object vector_datepart_hour_dict(DictionaryVector dict_vec):
    """Extract hour from DictionaryVector — O(V) optimization (future)."""
    cdef int value_type_id = dict_vec.dictionary_value_type
    cdef int INT64_TYPE_ID = 4
    cdef int TIMESTAMP_TYPE_ID = 22

    if value_type_id == INT64_TYPE_ID:
        values_vec = dict_vec.values()
        extracted_values = vector_datepart_hour_i64(values_vec)
        indices_array = dict_vec.indices_array()
        return DictionaryVector.from_arrays(indices_array, extracted_values.to_numpy())
    elif value_type_id == TIMESTAMP_TYPE_ID:
        values_vec = dict_vec.values()
        extracted_values = vector_datepart_hour(values_vec)
        indices_array = dict_vec.indices_array()
        return DictionaryVector.from_arrays(indices_array, extracted_values.to_numpy())
    else:
        raise TypeError(
            f"Cannot extract from DictionaryVector with value type_id={value_type_id}. "
            f"Supported types: INT64 (4), TIMESTAMP (22)."
        )


cpdef object vector_datepart_second_dict(DictionaryVector dict_vec):
    """Extract second from DictionaryVector — O(V) optimization (future)."""
    cdef int value_type_id = dict_vec.dictionary_value_type
    cdef int INT64_TYPE_ID = 4
    cdef int TIMESTAMP_TYPE_ID = 22

    if value_type_id == INT64_TYPE_ID:
        values_vec = dict_vec.values()
        extracted_values = vector_datepart_second_i64(values_vec)
        indices_array = dict_vec.indices_array()
        return DictionaryVector.from_arrays(indices_array, extracted_values.to_numpy())
    elif value_type_id == TIMESTAMP_TYPE_ID:
        values_vec = dict_vec.values()
        extracted_values = vector_datepart_second(values_vec)
        indices_array = dict_vec.indices_array()
        return DictionaryVector.from_arrays(indices_array, extracted_values.to_numpy())
    else:
        raise TypeError(
            f"Cannot extract from DictionaryVector with value type_id={value_type_id}. "
            f"Supported types: INT64 (4), TIMESTAMP (22)."
        )
