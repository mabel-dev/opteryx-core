# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False

"""
Convert Date32Vector to TimestampVector (microseconds since epoch).

Pure Draken conversion without pyarrow dependency.
"""

from libc.stdint cimport int32_t, int64_t

from opteryx.compiled.draken.vectors.date32_vector cimport Date32Vector
from opteryx.compiled.draken.vectors.timestamp_vector cimport TimestampVector
from opteryx.compiled.draken.core.buffers cimport DrakenFixedBuffer


cpdef TimestampVector vector_date32_to_timestamp(Date32Vector date_vec):
    """
    Convert Date32Vector to TimestampVector[us] (microseconds).

    Args:
        date_vec: Date32Vector (days since epoch)

    Returns:
        TimestampVector with microsecond precision.
    """
    cdef DrakenFixedBuffer* ptr = date_vec.ptr
    cdef int64_t length = ptr.length
    cdef int32_t* date_data = <int32_t*>ptr.data
    cdef uint8_t* null_bitmap = ptr.null_bitmap

    # Create result timestamp vector
    cdef TimestampVector result = TimestampVector(length)
    cdef int64_t* ts_data = <int64_t*>result.ptr.data
    cdef uint8_t* result_null = result.ptr.null_bitmap

    cdef int64_t i
    cdef int32_t date_val

    # Copy nulls and convert data
    for i in range(length):
        if null_bitmap != NULL and not ((null_bitmap[i >> 3] >> (i & 7)) & 1):
            # Null value - mark as null in result
            if result_null != NULL:
                result_null[i >> 3] &= ~(1 << (i & 7))
        else:
            # Non-null value - convert days to microseconds
            date_val = date_data[i]
            ts_data[i] = <int64_t>date_val * 86400000000  # MICROSECONDS_PER_DAY

    result.timestamp_unit = "us"
    return result
