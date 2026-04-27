# cython: language_level=3

"""
Typed RLE Builder declarations for building Run-Length Encoded vectors.

Each builder type (RLEBuilderInt64, RLEBuilderFloat64, etc.) accumulates runs
as append/append_repeated calls are made, then returns raw C pointers for
vector construction. No Python objects involved.
"""

from libc.stdint cimport int8_t, int16_t, int32_t, int64_t, uint8_t, uint32_t
from libc.stddef cimport size_t


# Int64 Builder
cdef class RLEBuilderInt64:
    cdef int64_t* _run_values
    cdef int32_t* _run_lengths
    cdef size_t _num_runs
    cdef size_t _capacity
    cdef int64_t _last_value
    cdef int32_t _current_run_length
    cdef bint _has_value

    cdef void append(self, int64_t value) nogil except *
    cdef void append_repeated(self, int64_t value, int32_t count) nogil except *
    cdef tuple finish(self)  # Returns (run_values, run_lengths, num_runs)


# Int32 Builder
cdef class RLEBuilderInt32:
    cdef int32_t* _run_values
    cdef int32_t* _run_lengths
    cdef size_t _num_runs
    cdef size_t _capacity
    cdef int32_t _last_value
    cdef int32_t _current_run_length
    cdef bint _has_value

    cdef void append(self, int32_t value) nogil except *
    cdef void append_repeated(self, int32_t value, int32_t count) nogil except *
    cdef tuple finish(self)


# Int16 Builder
cdef class RLEBuilderInt16:
    cdef int16_t* _run_values
    cdef int32_t* _run_lengths
    cdef size_t _num_runs
    cdef size_t _capacity
    cdef int16_t _last_value
    cdef int32_t _current_run_length
    cdef bint _has_value

    cdef void append(self, int16_t value) nogil except *
    cdef void append_repeated(self, int16_t value, int32_t count) nogil except *
    cdef tuple finish(self)


# Int8 Builder
cdef class RLEBuilderInt8:
    cdef int8_t* _run_values
    cdef int32_t* _run_lengths
    cdef size_t _num_runs
    cdef size_t _capacity
    cdef int8_t _last_value
    cdef int32_t _current_run_length
    cdef bint _has_value

    cdef void append(self, int8_t value) nogil except *
    cdef void append_repeated(self, int8_t value, int32_t count) nogil except *
    cdef tuple finish(self)


# Float64 Builder
cdef class RLEBuilderFloat64:
    cdef double* _run_values
    cdef int32_t* _run_lengths
    cdef size_t _num_runs
    cdef size_t _capacity
    cdef double _last_value
    cdef int32_t _current_run_length
    cdef bint _has_value

    cdef void append(self, double value) nogil except *
    cdef void append_repeated(self, double value, int32_t count) nogil except *
    cdef tuple finish(self)


# Float32 Builder
cdef class RLEBuilderFloat32:
    cdef float* _run_values
    cdef int32_t* _run_lengths
    cdef size_t _num_runs
    cdef size_t _capacity
    cdef float _last_value
    cdef int32_t _current_run_length
    cdef bint _has_value

    cdef void append(self, float value) nogil except *
    cdef void append_repeated(self, float value, int32_t count) nogil except *
    cdef tuple finish(self)


# Bool Builder
cdef class RLEBuilderBool:
    cdef uint8_t* _run_values
    cdef int32_t* _run_lengths
    cdef size_t _num_runs
    cdef size_t _capacity
    cdef uint8_t _last_value
    cdef int32_t _current_run_length
    cdef bint _has_value

    cdef void append(self, uint8_t value) nogil except *
    cdef void append_repeated(self, uint8_t value, int32_t count) nogil except *
    cdef tuple finish(self)


# Date32 Builder (int32_t days since epoch)
cdef class RLEBuilderDate32:
    cdef int32_t* _run_values
    cdef int32_t* _run_lengths
    cdef size_t _num_runs
    cdef size_t _capacity
    cdef int32_t _last_value
    cdef int32_t _current_run_length
    cdef bint _has_value

    cdef void append(self, int32_t value) nogil except *
    cdef void append_repeated(self, int32_t value, int32_t count) nogil except *
    cdef tuple finish(self)


# Timestamp Builder (int64_t microseconds since epoch)
cdef class RLEBuilderTimestamp:
    cdef int64_t* _run_values
    cdef int32_t* _run_lengths
    cdef size_t _num_runs
    cdef size_t _capacity
    cdef int64_t _last_value
    cdef int32_t _current_run_length
    cdef bint _has_value

    cdef void append(self, int64_t value) nogil except *
    cdef void append_repeated(self, int64_t value, int32_t count) nogil except *
    cdef tuple finish(self)


# Time32 Builder (int32_t milliseconds)
cdef class RLEBuilderTime32:
    cdef int32_t* _run_values
    cdef int32_t* _run_lengths
    cdef size_t _num_runs
    cdef size_t _capacity
    cdef int32_t _last_value
    cdef int32_t _current_run_length
    cdef bint _has_value

    cdef void append(self, int32_t value) nogil except *
    cdef void append_repeated(self, int32_t value, int32_t count) nogil except *
    cdef tuple finish(self)


# Time64 Builder (int64_t nanoseconds)
cdef class RLEBuilderTime64:
    cdef int64_t* _run_values
    cdef int32_t* _run_lengths
    cdef size_t _num_runs
    cdef size_t _capacity
    cdef int64_t _last_value
    cdef int32_t _current_run_length
    cdef bint _has_value

    cdef void append(self, int64_t value) nogil except *
    cdef void append_repeated(self, int64_t value, int32_t count) nogil except *
    cdef tuple finish(self)


# Interval Builder (int64_t months + days + microseconds packed)
cdef class RLEBuilderInterval:
    cdef int64_t* _run_values
    cdef int32_t* _run_lengths
    cdef size_t _num_runs
    cdef size_t _capacity
    cdef int64_t _last_value
    cdef int32_t _current_run_length
    cdef bint _has_value

    cdef void append(self, int64_t value) nogil except *
    cdef void append_repeated(self, int64_t value, int32_t count) nogil except *
    cdef tuple finish(self)


# String Builder
cdef class RLEBuilderString:
    """String builder for variable-width RLE. Accumulates distinct strings and runs."""
    cdef object _values        # List of bytes objects (distinct strings)
    cdef int32_t* _run_lengths # Run lengths for each distinct value
    cdef size_t _num_runs
    cdef size_t _capacity
    cdef object _last_value
    cdef int32_t _current_run_length
    cdef bint _has_value

    cdef void append(self, object value) nogil except *  # Note: nogil but may release/acquire for string ops
    cdef void append_repeated(self, object value, int32_t count) nogil except *
    cdef tuple finish(self)  # Returns (values, run_lengths, num_runs)
