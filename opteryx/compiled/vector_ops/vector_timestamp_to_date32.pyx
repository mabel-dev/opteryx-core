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
Convert TimestampVector to Date32Vector by truncating time portion.

Pure Draken conversion without pyarrow dependency.
"""

from libc.stdint cimport int32_t, int64_t, uint8_t

from draken.vectors.date32_vector cimport Date32Vector
from draken.vectors.timestamp_vector cimport TimestampVector
from draken.core.buffers cimport DrakenFixedBuffer


cpdef Date32Vector vector_timestamp_to_date32(TimestampVector ts_vec):
    """
    Convert TimestampVector to Date32Vector by truncating time portion.

    Timestamps are stored in microseconds since epoch.
    Date32 is stored in days since epoch.
    Simply divide by microseconds per day.

    Args:
        ts_vec: TimestampVector (microseconds since epoch)

    Returns:
        Date32Vector with time portion truncated (days since epoch).
    """
    cdef DrakenFixedBuffer* ptr = ts_vec.ptr
    cdef int64_t length = ptr.length
    cdef int64_t* ts_data = <int64_t*>ptr.data
    cdef uint8_t* null_bitmap = ptr.null_bitmap

    # Create result date vector
    cdef Date32Vector result = Date32Vector(length)
    cdef int32_t* date_data = <int32_t*>result.ptr.data
    cdef uint8_t* result_null = result.ptr.null_bitmap

    cdef int64_t i
    cdef int64_t ts_val

    # Copy nulls and convert data
    for i in range(length):
        if null_bitmap != NULL and not ((null_bitmap[i >> 3] >> (i & 7)) & 1):
            # Null value - mark as null in result
            if result_null != NULL:
                result_null[i >> 3] &= ~(1 << (i & 7))
        else:
            # Non-null value - convert microseconds to days
            ts_val = ts_data[i]
            date_data[i] = <int32_t>(ts_val // 86400000000)  # MICROSECONDS_PER_DAY

    return result
