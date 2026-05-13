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
Convert timestamps and dates to Unix time (seconds since epoch).

Returns an Int64Vector.
"""

from libc.stdint cimport int32_t, int64_t, uint8_t
from libc.stdlib cimport malloc, free
from libc.string cimport memset

from draken.vectors.timestamp_vector cimport TimestampVector
from draken.vectors.date32_vector cimport Date32Vector
from draken.vectors.int64_vector cimport Int64Vector, from_sequence as int64_from_sequence
from draken.core.buffers cimport DrakenFixedBuffer


cpdef Int64Vector vector_unixtime_timestamp(TimestampVector ts_vec):
    """
    Convert TimestampVector to Unix time (seconds since epoch).

    Args:
        ts_vec: TimestampVector

    Returns:
        Int64Vector of Unix timestamps (seconds).
    """
    cdef DrakenFixedBuffer* ptr = ts_vec.ptr
    cdef int64_t length = ptr.length
    cdef int64_t* data = <int64_t*>ptr.data
    cdef uint8_t* null_bitmap = ptr.null_bitmap
    cdef str unit = ts_vec.timestamp_unit

    # Determine divisor based on timestamp unit
    cdef int64_t divisor
    if unit == "us":
        divisor = 1000000  # MICROSECONDS_PER_SECOND
    elif unit == "ms":
        divisor = 1000  # MILLISECONDS_PER_SECOND
    elif unit == "ns":
        divisor = 1000000000  # NANOSECONDS_PER_SECOND
    else:  # Assume seconds
        divisor = 1

    # Allocate result buffer
    cdef int64_t* result_data = <int64_t*>malloc(length * sizeof(int64_t))
    if result_data == NULL:
        raise MemoryError()
    memset(result_data, 0, length * sizeof(int64_t))
    cdef int64_t[::1] result_view = <int64_t[:length]>result_data

    cdef int64_t i

    try:
        for i in range(length):
            # Check for null
            if null_bitmap != NULL and not ((null_bitmap[i >> 3] >> (i & 7)) & 1):
                result_view[i] = 0  # Nulls become 0 in the bitmap
            else:
                # Convert to seconds
                result_view[i] = data[i] // divisor

        return int64_from_sequence(result_view)
    finally:
        free(result_data)


cpdef Int64Vector vector_unixtime_date32(Date32Vector date_vec):
    """
    Convert Date32Vector (days since epoch) to Unix time (seconds since epoch).

    Args:
        date_vec: Date32Vector

    Returns:
        Int64Vector of Unix timestamps (seconds).
    """
    cdef DrakenFixedBuffer* ptr = date_vec.ptr
    cdef int64_t length = ptr.length
    cdef int32_t* data = <int32_t*>ptr.data
    cdef uint8_t* null_bitmap = ptr.null_bitmap

    # Allocate result buffer
    cdef int64_t* result_data = <int64_t*>malloc(length * sizeof(int64_t))
    if result_data == NULL:
        raise MemoryError()
    memset(result_data, 0, length * sizeof(int64_t))
    cdef int64_t[::1] result_view = <int64_t[:length]>result_data

    cdef int64_t i
    cdef int32_t days

    try:
        for i in range(length):
            # Check for null
            if null_bitmap != NULL and not ((null_bitmap[i >> 3] >> (i & 7)) & 1):
                result_view[i] = 0  # Nulls become 0 in the bitmap
            else:
                # Convert days to seconds
                days = data[i]
                result_view[i] = <int64_t>days * 86400  # SECONDS_PER_DAY

        return int64_from_sequence(result_view)
    finally:
        free(result_data)


cpdef object vector_unixtime(object temporal_vec):
    """
    Convert timestamp or date vector to Unix time (seconds since epoch).

    Dispatch wrapper that handles both TimestampVector and Date32Vector.

    Args:
        temporal_vec: TimestampVector or Date32Vector

    Returns:
        Int64Vector of Unix timestamps.
    """
    if isinstance(temporal_vec, TimestampVector):
        return vector_unixtime_timestamp(<TimestampVector>temporal_vec)
    elif isinstance(temporal_vec, Date32Vector):
        return vector_unixtime_date32(<Date32Vector>temporal_vec)
    else:
        raise TypeError(f"Expected TimestampVector or Date32Vector, got {type(temporal_vec).__name__}")
