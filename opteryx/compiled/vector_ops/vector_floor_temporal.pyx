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
Floor timestamps to a unit with optional magnitude.

This is similar to FLOOR_TEMPORAL in Arrow compute.
"""

from libc.stdint cimport int64_t, uint8_t
from libc.stdlib cimport malloc, free
from libc.string cimport memset

from draken.vectors.timestamp_vector cimport TimestampVector
from draken.vectors.integer64_vector cimport Integer64Vector, from_sequence as int64_from_sequence
from draken.core.buffers cimport DrakenFixedBuffer


cpdef TimestampVector vector_floor_temporal(object temporal_vec, int64_t magnitude, str units):
    """
    Floor timestamps to a unit with optional magnitude.

    Args:
        temporal_vec: TimestampVector
        magnitude: How many units to floor to (e.g., 5 for "5 minutes")
        units: The unit name ("second", "minute", "hour", "day")

    Returns:
        TimestampVector with floored timestamps.
    """
    if not isinstance(temporal_vec, TimestampVector):
        raise TypeError(f"Expected TimestampVector, got {type(temporal_vec).__name__}")

    cdef TimestampVector ts_vec = <TimestampVector>temporal_vec
    cdef DrakenFixedBuffer* ptr = ts_vec.ptr
    cdef int64_t length = ptr.length
    cdef int64_t* data = <int64_t*>ptr.data
    cdef uint8_t* null_bitmap = ptr.null_bitmap
    cdef str unit = ts_vec.timestamp_unit
    cdef int64_t divisor, floor_unit_us, floor_period, i, ts_val
    cdef int64_t* result_data
    cdef int64_t[::1] result_view
    cdef TimestampVector result
    cdef int64_t* result_ptr
    cdef int64_t j

    # Determine divisor to convert to the base unit
    if unit == "us":
        divisor = 1
    elif unit == "ms":
        divisor = 1000
    elif unit == "ns":
        divisor = 1000000000
    else:  # Assume seconds
        divisor = MICROSECONDS_PER_SECOND

    # Determine the floor unit in microseconds
    units_lower = units.lower()

    if units_lower in ("second", "seconds"):
        floor_unit_us = 1000000  # MICROSECONDS_PER_SECOND
    elif units_lower in ("minute", "minutes"):
        floor_unit_us = 60000000  # MICROSECONDS_PER_MINUTE
    elif units_lower in ("hour", "hours"):
        floor_unit_us = 3600000000  # MICROSECONDS_PER_HOUR
    elif units_lower in ("day", "days"):
        floor_unit_us = 86400000000  # MICROSECONDS_PER_DAY
    else:
        raise ValueError(f"Unsupported unit: {units}")

    # Allocate result buffer
    result_data = <int64_t*>malloc(length * sizeof(int64_t))
    if result_data == NULL:
        raise MemoryError()
    memset(result_data, 0, length * sizeof(int64_t))
    result_view = <int64_t[:length]>result_data

    # Compute floor period in native units
    floor_period = floor_unit_us * magnitude // divisor

    try:
        for i in range(length):
            # Check for null
            if null_bitmap != NULL and not ((null_bitmap[i >> 3] >> (i & 7)) & 1):
                result_view[i] = 0  # Null handling via bitmap
            else:
                ts_val = data[i]
                # Floor to the nearest period
                result_view[i] = (ts_val // floor_period) * floor_period

        # Create result vector
        result = TimestampVector(length)
        result.timestamp_unit = unit

        # Copy processed data into result
        result_ptr = <int64_t*>result.ptr.data
        for j in range(length):
            result_ptr[j] = result_view[j]

        return result
    finally:
        free(result_data)
