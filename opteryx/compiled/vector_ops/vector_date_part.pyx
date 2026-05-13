# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False
# cython: optimize.use_switch=True
# cython: optimize.unpack_method_calls=True

"""
Compiled DATEPART extraction for Draken vectors.

MUST be included AFTER vector_date_trunc.pyx in vector_ops.pyx.
Relies on helpers defined there:
  seconds_to_date_parts(), is_leap_year(), DAYS_IN_MONTH[], CUMULATIVE_DAYS[],
  SECONDS_PER_MINUTE, SECONDS_PER_HOUR, SECONDS_PER_DAY, EPOCH_WEEKDAY.

Phase 3 Implementation:
- Pure integer arithmetic for all extraction logic (sub-second and calendar)
- Loop unrolling for minute/hour/second (simple modulo ops)
- from_sequence() cimport for zero-copy Int64Vector construction
- Full calendar-unit coverage: year/month/day/dayofweek/dayofyear/quarter
- GC safety: result buffers are backed by the cpython.array store

Phase 4 (Future):
- AVX2/NEON SIMD intrinsics for 2-4x additional speedup
- OpenMP parallel extraction for >10 M row vectors
"""

from libc.stdint cimport int64_t
from libc.stddef cimport size_t
from cpython.array cimport array, clone

from draken.core.buffers cimport DictAccessor
from draken.vectors.timestamp_vector cimport TimestampVector
from draken.vectors.int64_vector cimport Int64Vector, from_packed_dict as int64_from_packed_dict, from_sequence as int64_from_sequence
from draken.vectors.scalar_constructors cimport from_scalar
from draken.vectors.vector cimport Vector
from draken.core.buffers cimport DRAKEN_INT64

# ---------------------------------------------------------------------------
# SIMD-accelerated kernels for minute / hour / second extraction.
# Runtime dispatch (NEON / AVX2 / scalar) is handled inside the C++ layer.
# ---------------------------------------------------------------------------
cdef extern from "simd_datepart.h":
    void simd_datepart_minute   (const int64_t* src, int64_t* dst, size_t n, int unit_code) noexcept nogil
    void simd_datepart_hour     (const int64_t* src, int64_t* dst, size_t n, int unit_code) noexcept nogil
    void simd_datepart_second   (const int64_t* src, int64_t* dst, size_t n, int unit_code) noexcept nogil
    void simd_datepart_year     (const int64_t* src, int64_t* dst, size_t n, int unit_code) noexcept nogil
    void simd_datepart_month    (const int64_t* src, int64_t* dst, size_t n, int unit_code) noexcept nogil
    void simd_datepart_day      (const int64_t* src, int64_t* dst, size_t n, int unit_code) noexcept nogil
    void simd_datepart_quarter  (const int64_t* src, int64_t* dst, size_t n, int unit_code) noexcept nogil
    void simd_datepart_dayofyear(const int64_t* src, int64_t* dst, size_t n, int unit_code) noexcept nogil


cdef inline int _timestamp_unit_code(str unit):
    if unit == 'us':
        return 2
    if unit == 'ms':
        return 1
    if unit == 'ns':
        return 3
    return 0

# ---------------------------------------------------------------------------
# Sub-second unit constants unique to this module
# (SECONDS_PER_* and EPOCH_WEEKDAY come from vector_date_trunc.pyx)
# ---------------------------------------------------------------------------
cdef const int64_t MICROSECONDS_PER_SECOND = 1_000_000
cdef const int64_t MICROSECONDS_PER_MINUTE = 60_000_000
cdef const int64_t MICROSECONDS_PER_HOUR = 3_600_000_000
cdef const int64_t MICROSECONDS_PER_DAY = 86_400_000_000

cdef const int64_t MILLISECONDS_PER_SECOND = 1_000
cdef const int64_t MILLISECONDS_PER_MINUTE = 60_000
cdef const int64_t MILLISECONDS_PER_HOUR = 3_600_000
cdef const int64_t MILLISECONDS_PER_DAY = 86_400_000

cdef const int64_t NANOSECONDS_PER_SECOND = 1_000_000_000
cdef const int64_t NANOSECONDS_PER_MINUTE = 60_000_000_000
cdef const int64_t NANOSECONDS_PER_HOUR = 3_600_000_000_000
cdef const int64_t NANOSECONDS_PER_DAY = 86_400_000_000_000

# Typed bounds used in _detect_seconds_divisor (all values fit in int64_t).
# Written with explicit <int64_t> casts so they remain C numeric constants
# inside noexcept nogil contexts.
cdef const int64_t _SEC_UPPER = <int64_t>10000000000  # 10^10
cdef const int64_t _MSEC_LOWER = <int64_t>1000000000000      # 10^12
cdef const int64_t _MSEC_UPPER = <int64_t>10000000000000     # 10^13
cdef const int64_t _USEC_LOWER = <int64_t>1000000000000000   # 10^15
cdef const int64_t _USEC_UPPER = <int64_t>10000000000000000  # 10^16
cdef const int64_t _NSEC_LOWER = <int64_t>1000000000000000000  # 10^18


cdef inline int _seconds_divisor_unit_code(int64_t seconds_divisor) noexcept nogil:
    if seconds_divisor == 1:
        return 0
    if seconds_divisor == MILLISECONDS_PER_SECOND:
        return 1
    if seconds_divisor == MICROSECONDS_PER_SECOND:
        return 2
    return 3


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


cdef inline bint _is_constant_encoded(object vec):
    """Check if vector is constant-encoded (encoding==3)."""
    return getattr(vec, "encoding", None) == 3


cdef inline int64_t _constant_scalar_value_i64(object vec):
    """Extract scalar value from constant-encoded vector."""
    if len(vec) == 0:
        return 0
    return vec[0]


cdef Int64Vector _datepart_i64_dict_subsecond(Int64Vector int64_vec, int part_kind):
    cdef DictAccessor* dict_accessor = (<Vector>int64_vec).dict_accessor()
    cdef Py_ssize_t row_count
    cdef Py_ssize_t dict_size
    cdef int64_t* dictionary_ptr
    cdef int64_t seconds_divisor
    cdef int64_t divisor
    cdef Py_ssize_t i
    cdef array template
    cdef array output_array
    cdef int64_t* output_ptr
    cdef Int64Vector result

    if dict_accessor == NULL:
        return None

    row_count = <Py_ssize_t>dict_accessor.length
    dict_size = <Py_ssize_t>dict_accessor.dict_values.length
    dictionary_ptr = <int64_t*>dict_accessor.dict_values.data

    if row_count == 0:
        return int64_from_packed_dict(
            dict_accessor.codes,
            dict_accessor.code_width,
            0,
            dictionary_ptr,
            dict_size,
            dict_accessor.row_nulls,
            False,
        )

    seconds_divisor = _find_seconds_divisor_int64(dictionary_ptr, dict_size)
    if part_kind == 0:
        if seconds_divisor == 1:
            divisor = SECONDS_PER_MINUTE
        elif seconds_divisor == MILLISECONDS_PER_SECOND:
            divisor = MILLISECONDS_PER_MINUTE
        elif seconds_divisor == MICROSECONDS_PER_SECOND:
            divisor = MICROSECONDS_PER_MINUTE
        else:
            divisor = NANOSECONDS_PER_MINUTE
    elif part_kind == 1:
        if seconds_divisor == 1:
            divisor = SECONDS_PER_HOUR
        elif seconds_divisor == MILLISECONDS_PER_SECOND:
            divisor = MILLISECONDS_PER_HOUR
        elif seconds_divisor == MICROSECONDS_PER_SECOND:
            divisor = MICROSECONDS_PER_HOUR
        else:
            divisor = NANOSECONDS_PER_HOUR
    else:
        divisor = seconds_divisor

    template = array('l')
    output_array = clone(template, dict_size, False)
    output_ptr = <int64_t*>output_array.data.as_longs

    if part_kind == 2 and seconds_divisor == 1:
        for i in range(dict_size):
            output_ptr[i] = dictionary_ptr[i] % 60
    else:
        for i in range(dict_size):
            if part_kind == 0:
                output_ptr[i] = (dictionary_ptr[i] // divisor) % 60
            elif part_kind == 1:
                output_ptr[i] = (dictionary_ptr[i] // divisor) % 24
            else:
                output_ptr[i] = (dictionary_ptr[i] // divisor) % 60

    result = int64_from_packed_dict(
        dict_accessor.codes,
        dict_accessor.code_width,
        row_count,
        <const int64_t*>output_ptr,
        dict_size,
        dict_accessor.row_nulls,
        int64_vec.ordered,
    )
    pass
    return result


cdef Int64Vector _datepart_i64_dict_calendar(Int64Vector int64_vec, int part_kind):
    cdef DictAccessor* dict_accessor = (<Vector>int64_vec).dict_accessor()
    cdef Py_ssize_t row_count
    cdef Py_ssize_t dict_size
    cdef int64_t* dictionary_ptr
    cdef int64_t seconds_divisor
    cdef int unit_code
    cdef int64_t day_divisor
    cdef Py_ssize_t i
    cdef int64_t d
    cdef array template
    cdef array output_array
    cdef int64_t* output_ptr
    cdef Int64Vector result

    if dict_accessor == NULL:
        return None

    row_count = <Py_ssize_t>dict_accessor.length
    dict_size = <Py_ssize_t>dict_accessor.dict_values.length
    dictionary_ptr = <int64_t*>dict_accessor.dict_values.data

    if row_count == 0:
        return int64_from_packed_dict(
            dict_accessor.codes,
            dict_accessor.code_width,
            0,
            dictionary_ptr,
            dict_size,
            dict_accessor.row_nulls,
            int64_vec.ordered,
        )

    template = array('l')
    output_array = clone(template, dict_size, False)
    output_ptr = <int64_t*>output_array.data.as_longs

    seconds_divisor = _find_seconds_divisor_int64(dictionary_ptr, dict_size)

    if part_kind == 3:
        if seconds_divisor == 1:
            day_divisor = SECONDS_PER_DAY
        elif seconds_divisor == MILLISECONDS_PER_SECOND:
            day_divisor = MILLISECONDS_PER_DAY
        elif seconds_divisor == MICROSECONDS_PER_SECOND:
            day_divisor = MICROSECONDS_PER_DAY
        else:
            day_divisor = NANOSECONDS_PER_DAY

        for i in range(dict_size):
            d = (dictionary_ptr[i] // day_divisor + EPOCH_WEEKDAY) % 7
            if d < 0:
                d += 7
            output_ptr[i] = d
    else:
        unit_code = _seconds_divisor_unit_code(seconds_divisor)
        if part_kind == 0:
            simd_datepart_year(dictionary_ptr, output_ptr, <size_t>dict_size, unit_code)
        elif part_kind == 1:
            simd_datepart_month(dictionary_ptr, output_ptr, <size_t>dict_size, unit_code)
        elif part_kind == 2:
            simd_datepart_day(dictionary_ptr, output_ptr, <size_t>dict_size, unit_code)
        elif part_kind == 4:
            simd_datepart_dayofyear(dictionary_ptr, output_ptr, <size_t>dict_size, unit_code)
        else:
            simd_datepart_quarter(dictionary_ptr, output_ptr, <size_t>dict_size, unit_code)

    result = int64_from_packed_dict(
        dict_accessor.codes,
        dict_accessor.code_width,
        row_count,
        <const int64_t*>output_ptr,
        dict_size,
        dict_accessor.row_nulls,
        int64_vec.ordered,
    )
    pass
    return result


# ===========================================================================
# Dictionary fast-paths for TimestampVector kernels
# ---------------------------------------------------------------------------
# These mirror _datepart_i64_dict_subsecond / _datepart_i64_dict_calendar but
# for TimestampVector.  The key simplification: we know the timestamp unit
# directly from ts_vec.timestamp_unit, so no heuristic precision detection is
# needed.  unit_code from _timestamp_unit_code maps to the SIMD convention:
#   0=s, 1=ms, 2=us, 3=ns
# ===========================================================================

cdef Int64Vector _datepart_ts_dict_subsecond(TimestampVector ts_vec, int part_kind):
    cdef DictAccessor* dict_accessor = (<Vector>ts_vec).dict_accessor()
    cdef Py_ssize_t row_count
    cdef Py_ssize_t dict_size
    cdef int64_t* dictionary_ptr
    cdef int unit_code
    cdef int64_t divisor
    cdef Py_ssize_t i
    cdef array template
    cdef array output_array
    cdef int64_t* output_ptr
    cdef Int64Vector result

    if dict_accessor == NULL:
        return None

    row_count = <Py_ssize_t>dict_accessor.length
    dict_size = <Py_ssize_t>dict_accessor.dict_values.length
    dictionary_ptr = <int64_t*>dict_accessor.dict_values.data

    if row_count == 0:
        return int64_from_packed_dict(
            dict_accessor.codes,
            dict_accessor.code_width,
            0,
            dictionary_ptr,
            dict_size,
            dict_accessor.row_nulls,
            False,
        )

    unit_code = _timestamp_unit_code(ts_vec.timestamp_unit)

    if part_kind == 0:  # minute
        if unit_code == 0:
            divisor = SECONDS_PER_MINUTE
        elif unit_code == 1:
            divisor = MILLISECONDS_PER_MINUTE
        elif unit_code == 2:
            divisor = MICROSECONDS_PER_MINUTE
        else:
            divisor = NANOSECONDS_PER_MINUTE
    elif part_kind == 1:  # hour
        if unit_code == 0:
            divisor = SECONDS_PER_HOUR
        elif unit_code == 1:
            divisor = MILLISECONDS_PER_HOUR
        elif unit_code == 2:
            divisor = MICROSECONDS_PER_HOUR
        else:
            divisor = NANOSECONDS_PER_HOUR
    else:  # second
        if unit_code == 0:
            divisor = 1
        elif unit_code == 1:
            divisor = MILLISECONDS_PER_SECOND
        elif unit_code == 2:
            divisor = MICROSECONDS_PER_SECOND
        else:
            divisor = NANOSECONDS_PER_SECOND

    template = array('l')
    output_array = clone(template, dict_size, False)
    output_ptr = <int64_t*>output_array.data.as_longs

    if part_kind == 0:
        for i in range(dict_size):
            output_ptr[i] = (dictionary_ptr[i] // divisor) % 60
    elif part_kind == 1:
        for i in range(dict_size):
            output_ptr[i] = (dictionary_ptr[i] // divisor) % 24
    else:  # second
        if unit_code == 0:  # seconds precision — direct modulo
            for i in range(dict_size):
                output_ptr[i] = dictionary_ptr[i] % 60
        else:
            for i in range(dict_size):
                output_ptr[i] = (dictionary_ptr[i] // divisor) % 60

    result = int64_from_packed_dict(
        dict_accessor.codes,
        dict_accessor.code_width,
        row_count,
        <const int64_t*>output_ptr,
        dict_size,
        dict_accessor.row_nulls,
        False,
    )
    pass
    return result


cdef Int64Vector _datepart_ts_dict_calendar(TimestampVector ts_vec, int part_kind):
    cdef DictAccessor* dict_accessor = (<Vector>ts_vec).dict_accessor()
    cdef Py_ssize_t row_count
    cdef Py_ssize_t dict_size
    cdef int64_t* dictionary_ptr
    cdef int unit_code
    cdef int64_t day_divisor
    cdef Py_ssize_t i
    cdef int64_t d
    cdef array template
    cdef array output_array
    cdef int64_t* output_ptr
    cdef Int64Vector result

    if dict_accessor == NULL:
        return None

    row_count = <Py_ssize_t>dict_accessor.length
    dict_size = <Py_ssize_t>dict_accessor.dict_values.length
    dictionary_ptr = <int64_t*>dict_accessor.dict_values.data

    if row_count == 0:
        return int64_from_packed_dict(
            dict_accessor.codes,
            dict_accessor.code_width,
            0,
            dictionary_ptr,
            dict_size,
            dict_accessor.row_nulls,
            False,
        )

    unit_code = _timestamp_unit_code(ts_vec.timestamp_unit)

    template = array('l')
    output_array = clone(template, dict_size, False)
    output_ptr = <int64_t*>output_array.data.as_longs

    if part_kind == 3:  # dayofweek — pure integer arithmetic, no SIMD kernel
        if unit_code == 0:
            day_divisor = SECONDS_PER_DAY
        elif unit_code == 1:
            day_divisor = MILLISECONDS_PER_DAY
        elif unit_code == 2:
            day_divisor = MICROSECONDS_PER_DAY
        else:
            day_divisor = NANOSECONDS_PER_DAY

        for i in range(dict_size):
            d = (dictionary_ptr[i] // day_divisor + EPOCH_WEEKDAY) % 7
            if d < 0:
                d += 7
            output_ptr[i] = d
    else:
        if part_kind == 0:
            simd_datepart_year(dictionary_ptr, output_ptr, <size_t>dict_size, unit_code)
        elif part_kind == 1:
            simd_datepart_month(dictionary_ptr, output_ptr, <size_t>dict_size, unit_code)
        elif part_kind == 2:
            simd_datepart_day(dictionary_ptr, output_ptr, <size_t>dict_size, unit_code)
        elif part_kind == 4:
            simd_datepart_dayofyear(dictionary_ptr, output_ptr, <size_t>dict_size, unit_code)
        else:  # quarter
            simd_datepart_quarter(dictionary_ptr, output_ptr, <size_t>dict_size, unit_code)

    result = int64_from_packed_dict(
        dict_accessor.codes,
        dict_accessor.code_width,
        row_count,
        <const int64_t*>output_ptr,
        dict_size,
        dict_accessor.row_nulls,
        False,
    )
    pass
    return result


# ===========================================================================
# MACRO: _mk_result(output_array, length) — build Int64Vector from array
#   result = int64_from_sequence(<int64_t[:length:1]>output_ptr)
# Used inline in every kernel below.
# ===========================================================================


# ===========================================================================
# SECTION 1 — TimestampVector kernels
# ===========================================================================

# ---------------------------------------------------------------------------
# 1a. Sub-second parts: minute, hour, second  (pure modulo arithmetic)
# ---------------------------------------------------------------------------

cpdef Int64Vector vector_datepart_minute(TimestampVector timestamps, Int64Vector out=None):
    """Extract minute (0–59) from TimestampVector via SIMD dispatch."""
    cdef Int64Vector dict_result = _datepart_ts_dict_subsecond(timestamps, 0)
    cdef str unit = timestamps.timestamp_unit
    cdef int64_t length = <int64_t>timestamps.ptr.length
    cdef int64_t* data_ptr = <int64_t*>timestamps.ptr.data
    cdef int64_t empty_sentinel = 0

    if dict_result is not None:
        return dict_result

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

    cdef int64_t* output_ptr
    cdef array template
    cdef array output_array
    cdef bint reuse_out = out is not None

    if reuse_out:
        if <int64_t>out.ptr.length != length:
            raise ValueError(f"out length {out.ptr.length} != input length {length}")
        output_ptr = <int64_t*>out.ptr.data
    else:
        template = array('l')
        output_array = clone(template, length, False)
        output_ptr = <int64_t*>output_array.data.as_longs

    simd_datepart_minute(data_ptr, output_ptr, <size_t>length, unit_code)

    if reuse_out:
        return out
    cdef Int64Vector result = int64_from_sequence(<int64_t[:length:1]>output_ptr)
    pass
    return result


cpdef Int64Vector vector_datepart_hour(TimestampVector timestamps, Int64Vector out=None):
    """Extract hour (0–23) from TimestampVector via SIMD dispatch."""
    cdef Int64Vector dict_result = _datepart_ts_dict_subsecond(timestamps, 1)
    cdef str unit = timestamps.timestamp_unit
    cdef int64_t length = <int64_t>timestamps.ptr.length
    cdef int64_t* data_ptr = <int64_t*>timestamps.ptr.data
    cdef int64_t empty_sentinel = 0

    if dict_result is not None:
        return dict_result

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

    cdef int64_t* output_ptr
    cdef array template
    cdef array output_array
    cdef bint reuse_out = out is not None

    if reuse_out:
        if <int64_t>out.ptr.length != length:
            raise ValueError(f"out length {out.ptr.length} != input length {length}")
        output_ptr = <int64_t*>out.ptr.data
    else:
        template = array('l')
        output_array = clone(template, length, False)
        output_ptr = <int64_t*>output_array.data.as_longs

    simd_datepart_hour(data_ptr, output_ptr, <size_t>length, unit_code)

    if reuse_out:
        return out
    cdef Int64Vector result = int64_from_sequence(<int64_t[:length:1]>output_ptr)
    pass
    return result


cpdef Int64Vector vector_datepart_second(TimestampVector timestamps, Int64Vector out=None):
    """Extract second (0–59) from TimestampVector via SIMD dispatch."""
    cdef Int64Vector dict_result = _datepart_ts_dict_subsecond(timestamps, 2)
    cdef str unit = timestamps.timestamp_unit
    cdef int64_t length = <int64_t>timestamps.ptr.length
    cdef int64_t* data_ptr = <int64_t*>timestamps.ptr.data
    cdef int64_t empty_sentinel = 0

    if dict_result is not None:
        return dict_result

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

    cdef int64_t* output_ptr
    cdef array template
    cdef array output_array
    cdef bint reuse_out = out is not None

    if reuse_out:
        if <int64_t>out.ptr.length != length:
            raise ValueError(f"out length {out.ptr.length} != input length {length}")
        output_ptr = <int64_t*>out.ptr.data
    else:
        template = array('l')
        output_array = clone(template, length, False)
        output_ptr = <int64_t*>output_array.data.as_longs

    simd_datepart_second(data_ptr, output_ptr, <size_t>length, unit_code)

    if reuse_out:
        return out
    cdef Int64Vector result = int64_from_sequence(<int64_t[:length:1]>output_ptr)
    pass
    return result


# ---------------------------------------------------------------------------
# 1b. Calendar parts for TimestampVector
#     seconds_to_date_parts(), is_leap_year(), CUMULATIVE_DAYS[] are from
#     vector_date_trunc.pyx (included before this file in vector_ops.pyx).
# ---------------------------------------------------------------------------

cpdef Int64Vector vector_datepart_year(TimestampVector timestamps, Int64Vector out=None):
    """Extract year from TimestampVector."""
    cdef Int64Vector dict_result = _datepart_ts_dict_calendar(timestamps, 0)
    cdef str unit = timestamps.timestamp_unit
    cdef int64_t length = <int64_t>timestamps.ptr.length
    cdef int64_t* data_ptr = <int64_t*>timestamps.ptr.data
    cdef int64_t empty_sentinel = 0
    cdef int unit_code = _timestamp_unit_code(unit)

    if dict_result is not None:
        return dict_result

    if length == 0:
        return int64_from_sequence(<int64_t[:0:1]>&empty_sentinel)

    cdef int64_t* output_ptr
    cdef array template
    cdef array output_array
    cdef bint reuse_out = out is not None

    if reuse_out:
        if <int64_t>out.ptr.length != length:
            raise ValueError(f"out length {out.ptr.length} != input length {length}")
        output_ptr = <int64_t*>out.ptr.data
    else:
        template = array('l')
        output_array = clone(template, length, False)
        output_ptr = <int64_t*>output_array.data.as_longs

    simd_datepart_year(data_ptr, output_ptr, <size_t>length, unit_code)
    if reuse_out:
        return out
    cdef Int64Vector result = int64_from_sequence(<int64_t[:length:1]>output_ptr)
    pass
    return result


cpdef Int64Vector vector_datepart_month(TimestampVector timestamps, Int64Vector out=None):
    """Extract month (1–12) from TimestampVector."""
    cdef Int64Vector dict_result = _datepart_ts_dict_calendar(timestamps, 1)
    cdef str unit = timestamps.timestamp_unit
    cdef int64_t length = <int64_t>timestamps.ptr.length
    cdef int64_t* data_ptr = <int64_t*>timestamps.ptr.data
    cdef int64_t empty_sentinel = 0
    cdef int unit_code = _timestamp_unit_code(unit)

    if dict_result is not None:
        return dict_result

    if length == 0:
        return int64_from_sequence(<int64_t[:0:1]>&empty_sentinel)

    cdef int64_t* output_ptr
    cdef array template
    cdef array output_array
    cdef bint reuse_out = out is not None

    if reuse_out:
        if <int64_t>out.ptr.length != length:
            raise ValueError(f"out length {out.ptr.length} != input length {length}")
        output_ptr = <int64_t*>out.ptr.data
    else:
        template = array('l')
        output_array = clone(template, length, False)
        output_ptr = <int64_t*>output_array.data.as_longs

    simd_datepart_month(data_ptr, output_ptr, <size_t>length, unit_code)
    if reuse_out:
        return out
    cdef Int64Vector result = int64_from_sequence(<int64_t[:length:1]>output_ptr)
    pass
    return result


cpdef Int64Vector vector_datepart_day(TimestampVector timestamps, Int64Vector out=None):
    """Extract day-of-month (1–31) from TimestampVector."""
    cdef Int64Vector dict_result = _datepart_ts_dict_calendar(timestamps, 2)
    cdef str unit = timestamps.timestamp_unit
    cdef int64_t length = <int64_t>timestamps.ptr.length
    cdef int64_t* data_ptr = <int64_t*>timestamps.ptr.data
    cdef int64_t empty_sentinel = 0
    cdef int unit_code = _timestamp_unit_code(unit)

    if dict_result is not None:
        return dict_result

    if length == 0:
        return int64_from_sequence(<int64_t[:0:1]>&empty_sentinel)

    cdef int64_t* output_ptr
    cdef array template
    cdef array output_array
    cdef bint reuse_out = out is not None

    if reuse_out:
        if <int64_t>out.ptr.length != length:
            raise ValueError(f"out length {out.ptr.length} != input length {length}")
        output_ptr = <int64_t*>out.ptr.data
    else:
        template = array('l')
        output_array = clone(template, length, False)
        output_ptr = <int64_t*>output_array.data.as_longs

    simd_datepart_day(data_ptr, output_ptr, <size_t>length, unit_code)
    if reuse_out:
        return out
    cdef Int64Vector result = int64_from_sequence(<int64_t[:length:1]>output_ptr)
    pass
    return result


cpdef Int64Vector vector_datepart_dayofweek(TimestampVector timestamps, Int64Vector out=None):
    """Extract day-of-week (0=Monday … 6=Sunday) from TimestampVector.

    Uses pure integer arithmetic: days_since_epoch + EPOCH_WEEKDAY (=4, Thursday)
    then modulo 7.  Negative timestamps (pre-epoch) are handled correctly.
    """
    cdef Int64Vector dict_result = _datepart_ts_dict_calendar(timestamps, 3)
    cdef str unit = timestamps.timestamp_unit
    cdef int64_t length = <int64_t>timestamps.ptr.length
    cdef int64_t* data_ptr = <int64_t*>timestamps.ptr.data
    cdef int64_t empty_sentinel = 0

    if dict_result is not None:
        return dict_result

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

    cdef int64_t* output_ptr
    cdef array template
    cdef array output_array
    cdef bint reuse_out = out is not None
    cdef int64_t i, d

    if reuse_out:
        if <int64_t>out.ptr.length != length:
            raise ValueError(f"out length {out.ptr.length} != input length {length}")
        output_ptr = <int64_t*>out.ptr.data
    else:
        template = array('l')
        output_array = clone(template, length, False)
        output_ptr = <int64_t*>output_array.data.as_longs

    for i in range(length):
        d = (data_ptr[i] // day_divisor + EPOCH_WEEKDAY) % 7
        if d < 0:
            d += 7
        output_ptr[i] = d

    if reuse_out:
        return out
    cdef Int64Vector result = int64_from_sequence(<int64_t[:length:1]>output_ptr)
    pass
    return result


cpdef Int64Vector vector_datepart_dayofyear(TimestampVector timestamps, Int64Vector out=None):
    """Extract day-of-year (1–366) from TimestampVector."""
    cdef Int64Vector dict_result = _datepart_ts_dict_calendar(timestamps, 4)
    cdef str unit = timestamps.timestamp_unit
    cdef int64_t length = <int64_t>timestamps.ptr.length
    cdef int64_t* data_ptr = <int64_t*>timestamps.ptr.data
    cdef int64_t empty_sentinel = 0
    cdef int unit_code = _timestamp_unit_code(unit)

    if dict_result is not None:
        return dict_result

    if length == 0:
        return int64_from_sequence(<int64_t[:0:1]>&empty_sentinel)

    cdef int64_t* output_ptr
    cdef array template
    cdef array output_array
    cdef bint reuse_out = out is not None

    if reuse_out:
        if <int64_t>out.ptr.length != length:
            raise ValueError(f"out length {out.ptr.length} != input length {length}")
        output_ptr = <int64_t*>out.ptr.data
    else:
        template = array('l')
        output_array = clone(template, length, False)
        output_ptr = <int64_t*>output_array.data.as_longs

    simd_datepart_dayofyear(data_ptr, output_ptr, <size_t>length, unit_code)
    if reuse_out:
        return out
    cdef Int64Vector result = int64_from_sequence(<int64_t[:length:1]>output_ptr)
    result._arrow_data_buf = output_array
    return result


cpdef Int64Vector vector_datepart_quarter(TimestampVector timestamps, Int64Vector out=None):
    """Extract quarter (1–4) from TimestampVector."""
    cdef Int64Vector dict_result = _datepart_ts_dict_calendar(timestamps, 5)
    cdef str unit = timestamps.timestamp_unit
    cdef int64_t length = <int64_t>timestamps.ptr.length
    cdef int64_t* data_ptr = <int64_t*>timestamps.ptr.data
    cdef int64_t empty_sentinel = 0
    cdef int unit_code = _timestamp_unit_code(unit)

    if dict_result is not None:
        return dict_result

    if length == 0:
        return int64_from_sequence(<int64_t[:0:1]>&empty_sentinel)

    cdef int64_t* output_ptr
    cdef array template
    cdef array output_array
    cdef bint reuse_out = out is not None

    if reuse_out:
        if <int64_t>out.ptr.length != length:
            raise ValueError(f"out length {out.ptr.length} != input length {length}")
        output_ptr = <int64_t*>out.ptr.data
    else:
        template = array('l')
        output_array = clone(template, length, False)
        output_ptr = <int64_t*>output_array.data.as_longs

    simd_datepart_quarter(data_ptr, output_ptr, <size_t>length, unit_code)
    if reuse_out:
        return out
    cdef Int64Vector result = int64_from_sequence(<int64_t[:length:1]>output_ptr)
    result._arrow_data_buf = output_array
    return result


# ===========================================================================
# SECTION 2 — Int64Vector kernels (Unix timestamp integers, any precision)
# ===========================================================================

# ---------------------------------------------------------------------------
# 2a. Sub-second parts: minute, hour, second
# ---------------------------------------------------------------------------

cpdef Int64Vector vector_datepart_minute_i64(object int64_vec):
    """Extract minute (0–59) from Int64Vector with automatic precision detection."""
    cdef int64_t length = <int64_t>(<Int64Vector>int64_vec).ptr.length
    cdef int64_t val
    cdef int64_t sd
    cdef int64_t divisor
    cdef int64_t result_val
    cdef array template
    cdef array output_array
    cdef int64_t* output_ptr
    cdef int64_t* data_ptr
    cdef Int64Vector dict_result
    cdef Int64Vector result
    cdef int64_t i
    cdef int64_t empty_sentinel = 0

    if _is_constant_encoded(int64_vec):
        val = _constant_scalar_value_i64(int64_vec)
        if length == 0:
            return int64_from_sequence(<int64_t[:0:1]>&empty_sentinel)
        sd = _detect_seconds_divisor(val)
        if sd == 0:
            sd = 1
        if sd == 1:
            divisor = SECONDS_PER_MINUTE
        elif sd == MILLISECONDS_PER_SECOND:
            divisor = MILLISECONDS_PER_MINUTE
        elif sd == MICROSECONDS_PER_SECOND:
            divisor = MICROSECONDS_PER_MINUTE
        else:
            divisor = NANOSECONDS_PER_MINUTE
        result_val = (val // divisor) % 60
        return from_scalar(result_val, length, dtype=DRAKEN_INT64)

    dict_result = _datepart_i64_dict_subsecond(<Int64Vector>int64_vec, 0)
    if dict_result is not None:
        return dict_result

    if length == 0:
        return int64_from_sequence(<int64_t[:0:1]>&empty_sentinel)

    data_ptr = <int64_t*>(<Int64Vector>int64_vec).ptr.data
    sd = _find_seconds_divisor_int64(data_ptr, length)

    if sd == 1:
        divisor = SECONDS_PER_MINUTE
    elif sd == MILLISECONDS_PER_SECOND:
        divisor = MILLISECONDS_PER_MINUTE
    elif sd == MICROSECONDS_PER_SECOND:
        divisor = MICROSECONDS_PER_MINUTE
    else:
        divisor = NANOSECONDS_PER_MINUTE

    template = array('l')
    output_array = clone(template, length, False)
    output_ptr = <int64_t*>output_array.data.as_longs
    i = 0

    while i + 3 < length:
        output_ptr[i] = (data_ptr[i] // divisor) % 60
        output_ptr[i + 1] = (data_ptr[i + 1] // divisor) % 60
        output_ptr[i + 2] = (data_ptr[i + 2] // divisor) % 60
        output_ptr[i + 3] = (data_ptr[i + 3] // divisor) % 60
        i += 4
    while i < length:
        output_ptr[i] = (data_ptr[i] // divisor) % 60
        i += 1

    result = int64_from_sequence(<int64_t[:length:1]>output_ptr)
    result._arrow_data_buf = output_array
    return result


cpdef Int64Vector vector_datepart_hour_i64(object int64_vec):
    """Extract hour (0–23) from Int64Vector with automatic precision detection."""
    cdef int64_t length = <int64_t>(<Int64Vector>int64_vec).ptr.length
    cdef int64_t val
    cdef int64_t sd
    cdef int64_t divisor
    cdef int64_t result_val
    cdef array template
    cdef array output_array
    cdef int64_t* output_ptr
    cdef int64_t* data_ptr
    cdef Int64Vector dict_result
    cdef Int64Vector result
    cdef int64_t i
    cdef int64_t empty_sentinel = 0

    if _is_constant_encoded(int64_vec):
        val = _constant_scalar_value_i64(int64_vec)
        if length == 0:
            return int64_from_sequence(<int64_t[:0:1]>&empty_sentinel)
        sd = _detect_seconds_divisor(val)
        if sd == 0:
            sd = 1
        if sd == 1:
            divisor = SECONDS_PER_HOUR
        elif sd == MILLISECONDS_PER_SECOND:
            divisor = MILLISECONDS_PER_HOUR
        elif sd == MICROSECONDS_PER_SECOND:
            divisor = MICROSECONDS_PER_HOUR
        else:
            divisor = NANOSECONDS_PER_HOUR
        result_val = (val // divisor) % 24
        return from_scalar(result_val, length, dtype=DRAKEN_INT64)

    dict_result = _datepart_i64_dict_subsecond(<Int64Vector>int64_vec, 1)
    if dict_result is not None:
        return dict_result

    if length == 0:
        return int64_from_sequence(<int64_t[:0:1]>&empty_sentinel)

    data_ptr = <int64_t*>(<Int64Vector>int64_vec).ptr.data
    sd = _find_seconds_divisor_int64(data_ptr, length)

    if sd == 1:
        divisor = SECONDS_PER_HOUR
    elif sd == MILLISECONDS_PER_SECOND:
        divisor = MILLISECONDS_PER_HOUR
    elif sd == MICROSECONDS_PER_SECOND:
        divisor = MICROSECONDS_PER_HOUR
    else:
        divisor = NANOSECONDS_PER_HOUR

    template = array('l')
    output_array = clone(template, length, False)
    output_ptr = <int64_t*>output_array.data.as_longs
    i = 0

    while i + 3 < length:
        output_ptr[i] = (data_ptr[i] // divisor) % 24
        output_ptr[i + 1] = (data_ptr[i + 1] // divisor) % 24
        output_ptr[i + 2] = (data_ptr[i + 2] // divisor) % 24
        output_ptr[i + 3] = (data_ptr[i + 3] // divisor) % 24
        i += 4
    while i < length:
        output_ptr[i] = (data_ptr[i] // divisor) % 24
        i += 1

    result = int64_from_sequence(<int64_t[:length:1]>output_ptr)
    result._arrow_data_buf = output_array
    return result


cpdef Int64Vector vector_datepart_second_i64(object int64_vec):
    """Extract second (0–59) from Int64Vector with automatic precision detection."""
    cdef int64_t length = <int64_t>(<Int64Vector>int64_vec).ptr.length
    cdef int64_t val
    cdef int64_t sd
    cdef int64_t result_val
    cdef array template
    cdef array output_array
    cdef int64_t* output_ptr
    cdef int64_t* data_ptr
    cdef Int64Vector dict_result
    cdef Int64Vector result
    cdef int64_t i
    cdef int64_t empty_sentinel = 0

    if _is_constant_encoded(int64_vec):
        val = _constant_scalar_value_i64(int64_vec)
        if length == 0:
            return int64_from_sequence(<int64_t[:0:1]>&empty_sentinel)
        sd = _detect_seconds_divisor(val)
        if sd == 0:
            sd = 1
        if sd == 1:
            result_val = val % 60
        else:
            result_val = (val // sd) % 60
        return from_scalar(result_val, length, dtype=DRAKEN_INT64)

    dict_result = _datepart_i64_dict_subsecond(<Int64Vector>int64_vec, 2)
    if dict_result is not None:
        return dict_result

    if length == 0:
        return int64_from_sequence(<int64_t[:0:1]>&empty_sentinel)

    data_ptr = <int64_t*>(<Int64Vector>int64_vec).ptr.data
    sd = _find_seconds_divisor_int64(data_ptr, length)

    template = array('l')
    output_array = clone(template, length, False)
    output_ptr = <int64_t*>output_array.data.as_longs
    i = 0

    if sd == 1:
        while i + 3 < length:
            output_ptr[i] = data_ptr[i] % 60
            output_ptr[i + 1] = data_ptr[i + 1] % 60
            output_ptr[i + 2] = data_ptr[i + 2] % 60
            output_ptr[i + 3] = data_ptr[i + 3] % 60
            i += 4
        while i < length:
            output_ptr[i] = data_ptr[i] % 60
            i += 1
    else:
        while i + 3 < length:
            output_ptr[i] = (data_ptr[i] // sd) % 60
            output_ptr[i + 1] = (data_ptr[i + 1] // sd) % 60
            output_ptr[i + 2] = (data_ptr[i + 2] // sd) % 60
            output_ptr[i + 3] = (data_ptr[i + 3] // sd) % 60
            i += 4
        while i < length:
            output_ptr[i] = (data_ptr[i] // sd) % 60
            i += 1

    result = int64_from_sequence(<int64_t[:length:1]>output_ptr)
    result._arrow_data_buf = output_array
    return result


# ---------------------------------------------------------------------------
# 2b. Calendar parts for Int64Vector
# ---------------------------------------------------------------------------

cpdef Int64Vector vector_datepart_year_i64(object int64_vec):
    """Extract year from Int64Vector (auto-detects timestamp precision)."""
    cdef int64_t length = <int64_t>(<Int64Vector>int64_vec).ptr.length
    cdef int64_t val
    cdef int64_t result_val
    cdef array template
    cdef array output_array
    cdef int64_t* output_ptr
    cdef int64_t* data_ptr
    cdef int64_t sd
    cdef int unit_code
    cdef Int64Vector dict_result
    cdef Int64Vector result
    cdef int64_t empty_sentinel = 0

    if _is_constant_encoded(int64_vec):
        val = _constant_scalar_value_i64(int64_vec)
        if length == 0:
            return int64_from_sequence(<int64_t[:0:1]>&empty_sentinel)
        sd = _detect_seconds_divisor(val)
        if sd == 0:
            sd = 1
        unit_code = _seconds_divisor_unit_code(sd)
        template = array('l')
        output_array = clone(template, 1, False)
        output_ptr = <int64_t*>output_array.data.as_longs
        simd_datepart_year(&val, output_ptr, 1, unit_code)
        return from_scalar(output_ptr[0], length, dtype=DRAKEN_INT64)

    dict_result = _datepart_i64_dict_calendar(<Int64Vector>int64_vec, 0)
    if dict_result is not None:
        return dict_result

    if length == 0:
        return int64_from_sequence(<int64_t[:0:1]>&empty_sentinel)

    data_ptr = <int64_t*>(<Int64Vector>int64_vec).ptr.data
    sd = _find_seconds_divisor_int64(data_ptr, length)
    unit_code = _seconds_divisor_unit_code(sd)
    template = array('l')
    output_array = clone(template, length, False)
    output_ptr = <int64_t*>output_array.data.as_longs
    simd_datepart_year(data_ptr, output_ptr, <size_t>length, unit_code)
    result = int64_from_sequence(<int64_t[:length:1]>output_ptr)
    result._arrow_data_buf = output_array
    return result


cpdef Int64Vector vector_datepart_month_i64(object int64_vec):
    """Extract month (1–12) from Int64Vector (auto-detects precision)."""
    cdef int64_t length = <int64_t>(<Int64Vector>int64_vec).ptr.length
    cdef int64_t val
    cdef int64_t result_val
    cdef array template
    cdef array output_array
    cdef int64_t* output_ptr
    cdef int64_t* data_ptr
    cdef int64_t sd
    cdef int unit_code
    cdef Int64Vector dict_result
    cdef Int64Vector result
    cdef int64_t empty_sentinel = 0

    if _is_constant_encoded(int64_vec):
        val = _constant_scalar_value_i64(int64_vec)
        if length == 0:
            return int64_from_sequence(<int64_t[:0:1]>&empty_sentinel)
        sd = _detect_seconds_divisor(val)
        if sd == 0:
            sd = 1
        unit_code = _seconds_divisor_unit_code(sd)
        template = array('l')
        output_array = clone(template, 1, False)
        output_ptr = <int64_t*>output_array.data.as_longs
        simd_datepart_month(&val, output_ptr, 1, unit_code)
        return from_scalar(output_ptr[0], length, dtype=DRAKEN_INT64)

    dict_result = _datepart_i64_dict_calendar(<Int64Vector>int64_vec, 1)
    if dict_result is not None:
        return dict_result

    if length == 0:
        return int64_from_sequence(<int64_t[:0:1]>&empty_sentinel)

    data_ptr = <int64_t*>(<Int64Vector>int64_vec).ptr.data
    sd = _find_seconds_divisor_int64(data_ptr, length)
    unit_code = _seconds_divisor_unit_code(sd)
    template = array('l')
    output_array = clone(template, length, False)
    output_ptr = <int64_t*>output_array.data.as_longs
    simd_datepart_month(data_ptr, output_ptr, <size_t>length, unit_code)
    result = int64_from_sequence(<int64_t[:length:1]>output_ptr)
    result._arrow_data_buf = output_array
    return result


cpdef Int64Vector vector_datepart_day_i64(object int64_vec):
    """Extract day-of-month (1–31) from Int64Vector (auto-detects precision)."""
    cdef int64_t length = <int64_t>(<Int64Vector>int64_vec).ptr.length
    cdef int64_t val
    cdef int64_t result_val
    cdef array template
    cdef array output_array
    cdef int64_t* output_ptr
    cdef int64_t* data_ptr
    cdef int64_t sd
    cdef int unit_code
    cdef Int64Vector dict_result
    cdef Int64Vector result
    cdef int64_t empty_sentinel = 0

    if _is_constant_encoded(int64_vec):
        val = _constant_scalar_value_i64(int64_vec)
        if length == 0:
            return int64_from_sequence(<int64_t[:0:1]>&empty_sentinel)
        sd = _detect_seconds_divisor(val)
        if sd == 0:
            sd = 1
        unit_code = _seconds_divisor_unit_code(sd)
        template = array('l')
        output_array = clone(template, 1, False)
        output_ptr = <int64_t*>output_array.data.as_longs
        simd_datepart_day(&val, output_ptr, 1, unit_code)
        return from_scalar(output_ptr[0], length, dtype=DRAKEN_INT64)

    dict_result = _datepart_i64_dict_calendar(<Int64Vector>int64_vec, 2)
    if dict_result is not None:
        return dict_result

    if length == 0:
        return int64_from_sequence(<int64_t[:0:1]>&empty_sentinel)

    data_ptr = <int64_t*>(<Int64Vector>int64_vec).ptr.data
    sd = _find_seconds_divisor_int64(data_ptr, length)
    unit_code = _seconds_divisor_unit_code(sd)
    template = array('l')
    output_array = clone(template, length, False)
    output_ptr = <int64_t*>output_array.data.as_longs
    simd_datepart_day(data_ptr, output_ptr, <size_t>length, unit_code)
    result = int64_from_sequence(<int64_t[:length:1]>output_ptr)
    result._arrow_data_buf = output_array
    return result


cpdef Int64Vector vector_datepart_dayofweek_i64(object int64_vec):
    """Extract day-of-week (0=Monday … 6=Sunday) from Int64Vector."""
    cdef int64_t length = <int64_t>(<Int64Vector>int64_vec).ptr.length
    cdef int64_t val
    cdef int64_t sd
    cdef int64_t day_divisor
    cdef int64_t result_val
    cdef int64_t d
    cdef array template
    cdef array output_array
    cdef int64_t* output_ptr
    cdef int64_t* data_ptr
    cdef Int64Vector dict_result
    cdef Int64Vector result
    cdef int64_t i
    cdef int64_t empty_sentinel = 0

    if _is_constant_encoded(int64_vec):
        val = _constant_scalar_value_i64(int64_vec)
        if length == 0:
            return int64_from_sequence(<int64_t[:0:1]>&empty_sentinel)
        sd = _detect_seconds_divisor(val)
        if sd == 0:
            sd = 1
        if sd == 1:
            day_divisor = SECONDS_PER_DAY
        elif sd == MILLISECONDS_PER_SECOND:
            day_divisor = MILLISECONDS_PER_DAY
        elif sd == MICROSECONDS_PER_SECOND:
            day_divisor = MICROSECONDS_PER_DAY
        else:
            day_divisor = NANOSECONDS_PER_DAY
        d = (val // day_divisor + EPOCH_WEEKDAY) % 7
        if d < 0:
            d += 7
        return from_scalar(d, length, dtype=DRAKEN_INT64)

    dict_result = _datepart_i64_dict_calendar(<Int64Vector>int64_vec, 3)
    if dict_result is not None:
        return dict_result

    if length == 0:
        return int64_from_sequence(<int64_t[:0:1]>&empty_sentinel)

    data_ptr = <int64_t*>(<Int64Vector>int64_vec).ptr.data
    sd = _find_seconds_divisor_int64(data_ptr, length)
    if sd == 1:
        day_divisor = SECONDS_PER_DAY
    elif sd == MILLISECONDS_PER_SECOND:
        day_divisor = MILLISECONDS_PER_DAY
    elif sd == MICROSECONDS_PER_SECOND:
        day_divisor = MICROSECONDS_PER_DAY
    else:
        day_divisor = NANOSECONDS_PER_DAY

    template = array('l')
    output_array = clone(template, length, False)
    output_ptr = <int64_t*>output_array.data.as_longs

    for i in range(length):
        d = (data_ptr[i] // day_divisor + EPOCH_WEEKDAY) % 7
        if d < 0:
            d += 7
        output_ptr[i] = d

    result = int64_from_sequence(<int64_t[:length:1]>output_ptr)
    result._arrow_data_buf = output_array
    return result


cpdef Int64Vector vector_datepart_dayofyear_i64(object int64_vec):
    """Extract day-of-year (1–366) from Int64Vector (auto-detects precision)."""
    cdef int64_t length = <int64_t>(<Int64Vector>int64_vec).ptr.length
    cdef int64_t val
    cdef int64_t sd
    cdef int unit_code
    cdef array template
    cdef array output_array
    cdef int64_t* output_ptr
    cdef int64_t* data_ptr
    cdef Int64Vector dict_result
    cdef Int64Vector result
    cdef int64_t empty_sentinel = 0

    if _is_constant_encoded(int64_vec):
        val = _constant_scalar_value_i64(int64_vec)
        if length == 0:
            return int64_from_sequence(<int64_t[:0:1]>&empty_sentinel)
        sd = _detect_seconds_divisor(val)
        if sd == 0:
            sd = 1
        unit_code = _seconds_divisor_unit_code(sd)
        template = array('l')
        output_array = clone(template, 1, False)
        output_ptr = <int64_t*>output_array.data.as_longs
        simd_datepart_dayofyear(&val, output_ptr, 1, unit_code)
        return from_scalar(output_ptr[0], length, dtype=DRAKEN_INT64)

    dict_result = _datepart_i64_dict_calendar(<Int64Vector>int64_vec, 4)
    if dict_result is not None:
        return dict_result

    if length == 0:
        return int64_from_sequence(<int64_t[:0:1]>&empty_sentinel)

    data_ptr = <int64_t*>(<Int64Vector>int64_vec).ptr.data
    sd = _find_seconds_divisor_int64(data_ptr, length)
    unit_code = _seconds_divisor_unit_code(sd)
    template = array('l')
    output_array = clone(template, length, False)
    output_ptr = <int64_t*>output_array.data.as_longs
    simd_datepart_dayofyear(data_ptr, output_ptr, <size_t>length, unit_code)
    result = int64_from_sequence(<int64_t[:length:1]>output_ptr)
    result._arrow_data_buf = output_array
    return result


cpdef Int64Vector vector_datepart_quarter_i64(object int64_vec):
    """Extract quarter (1–4) from Int64Vector (auto-detects precision)."""
    cdef int64_t length = <int64_t>(<Int64Vector>int64_vec).ptr.length
    cdef int64_t val
    cdef int64_t sd
    cdef int unit_code
    cdef array template
    cdef array output_array
    cdef int64_t* output_ptr
    cdef int64_t* data_ptr
    cdef Int64Vector dict_result
    cdef Int64Vector result
    cdef int64_t empty_sentinel = 0

    if _is_constant_encoded(int64_vec):
        val = _constant_scalar_value_i64(int64_vec)
        if length == 0:
            return int64_from_sequence(<int64_t[:0:1]>&empty_sentinel)
        sd = _detect_seconds_divisor(val)
        if sd == 0:
            sd = 1
        unit_code = _seconds_divisor_unit_code(sd)
        template = array('l')
        output_array = clone(template, 1, False)
        output_ptr = <int64_t*>output_array.data.as_longs
        simd_datepart_quarter(&val, output_ptr, 1, unit_code)
        return from_scalar(output_ptr[0], length, dtype=DRAKEN_INT64)

    dict_result = _datepart_i64_dict_calendar(<Int64Vector>int64_vec, 5)
    if dict_result is not None:
        return dict_result

    if length == 0:
        return int64_from_sequence(<int64_t[:0:1]>&empty_sentinel)

    data_ptr = <int64_t*>(<Int64Vector>int64_vec).ptr.data
    sd = _find_seconds_divisor_int64(data_ptr, length)
    unit_code = _seconds_divisor_unit_code(sd)
    template = array('l')
    output_array = clone(template, length, False)
    output_ptr = <int64_t*>output_array.data.as_longs
    simd_datepart_quarter(data_ptr, output_ptr, <size_t>length, unit_code)
    result = int64_from_sequence(<int64_t[:length:1]>output_ptr)
    result._arrow_data_buf = output_array
    return result
