# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False

"""
Format timestamps as strings using strftime pattern.

Returns a Python list of formatted strings (including None for nulls).
"""

from libc.stdint cimport int64_t, int32_t
from libc.time cimport time_t, gmtime_r, strftime, tm

from opteryx.compiled.draken.vectors.timestamp_vector cimport TimestampVector
from opteryx.compiled.draken.vectors.date32_vector cimport Date32Vector


cpdef list vector_date_format(object temporal_vec, bytes pattern):
    """
    Format timestamps or dates as strings.

    Args:
        temporal_vec: TimestampVector or Date32Vector
        pattern: bytes pattern (strftime format)

    Returns:
        List of formatted strings (None for nulls).
    """
    cdef list result = []
    cdef bytes pattern_str = pattern
    cdef TimestampVector ts_vec
    cdef Date32Vector date_vec
    cdef int64_t length
    cdef int64_t* ts_data
    cdef int32_t* date_data
    cdef uint8_t* null_bitmap
    cdef int64_t ts_value, seconds, i, divisor
    cdef int32_t date_val
    cdef tm time_struct
    cdef time_t t
    cdef char[256] buf
    cdef str unit

    # Handle TimestampVector
    if isinstance(temporal_vec, TimestampVector):
        ts_vec = <TimestampVector>temporal_vec
        length = ts_vec.ptr.length
        ts_data = <int64_t*>ts_vec.ptr.data
        null_bitmap = ts_vec.ptr.null_bitmap
        unit = ts_vec.timestamp_unit

        # Convert to seconds based on unit
        if unit == "us":
            divisor = 1000000  # MICROSECONDS_PER_SECOND
        elif unit == "ms":
            divisor = 1000
        elif unit == "ns":
            divisor = 1000000000
        else:
            divisor = 1

        for i in range(length):
            # Check for null
            if null_bitmap != NULL and not ((null_bitmap[i >> 3] >> (i & 7)) & 1):
                result.append(None)
            else:
                ts_value = ts_data[i]
                seconds = ts_value // divisor
                t = <time_t>seconds
                gmtime_r(&t, &time_struct)

                # Format the time
                strftime(buf, 256, pattern_str, &time_struct)
                result.append(buf.decode("utf-8"))

        return result

    # Handle Date32Vector
    if isinstance(temporal_vec, Date32Vector):
        date_vec = <Date32Vector>temporal_vec
        length = date_vec.ptr.length
        date_data = <int32_t*>date_vec.ptr.data
        null_bitmap = date_vec.ptr.null_bitmap

        for i in range(length):
            # Check for null
            if null_bitmap != NULL and not ((null_bitmap[i >> 3] >> (i & 7)) & 1):
                result.append(None)
            else:
                date_val = date_data[i]
                t = <time_t>(<int64_t>date_val * 86400)  # SECONDS_PER_DAY
                gmtime_r(&t, &time_struct)

                # Format the time
                strftime(buf, 256, pattern_str, &time_struct)
                result.append(buf.decode("utf-8"))

        return result

    raise TypeError(f"Expected TimestampVector or Date32Vector, got {type(temporal_vec).__name__}")
