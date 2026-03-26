# cython: language_level=3
# cython: boundscheck=False
# cython: wraparound=False
# cython: nonecheck=False
# cython: cdivision=True
# cython: infer_types=True

from libc.stdint cimport int64_t, uint8_t
from opteryx.compiled.draken.core.buffers cimport DictAccessor, DrakenFixedBuffer
from opteryx.compiled.aggregations.kernels.utils cimport _bitmap_is_valid, _read_integer_value
from opteryx.compiled.aggregations.vector_readers cimport _dict_accessor_read_int_value


cdef void avg_i64_accumulate(
    double* avg_sums,
    int64_t* avg_counts,
    const int64_t* state_indices,
    const int64_t* values,
    const uint8_t* value_nulls,
    Py_ssize_t row_count,
) noexcept nogil:
    """
    Accumulate AVG for a plain int64 value column (single-aggregate path).

    Accumulates into double avg_sums (widening) and increments avg_counts.
    """
    cdef Py_ssize_t i
    cdef int64_t sidx
    for i in range(row_count):
        if _bitmap_is_valid(value_nulls, i):
            sidx = state_indices[i]
            avg_sums[sidx] = avg_sums[sidx] + <double> values[i]
            avg_counts[sidx] = avg_counts[sidx] + 1


cdef void avg_i64_accumulate_from_dict(
    double* avg_sums,
    int64_t* avg_counts,
    const int64_t* state_indices,
    DictAccessor* accessor,
    Py_ssize_t row_count,
) except *:
    """
    Accumulate AVG from a dictionary-encoded int64 value column (single-aggregate path).

    Not nogil because _dict_accessor_read_int_value is except*.
    """
    cdef Py_ssize_t i
    cdef int64_t val
    cdef int64_t sidx
    cdef uint8_t* value_nulls = accessor.row_nulls
    for i in range(row_count):
        if _bitmap_is_valid(value_nulls, i):
            val = _dict_accessor_read_int_value(accessor, i)
            sidx = state_indices[i]
            avg_sums[sidx] = avg_sums[sidx] + <double> val
            avg_counts[sidx] = avg_counts[sidx] + 1


cdef void avg_integer_accumulate(
    double* avg_sums,
    int64_t* avg_counts,
    const int64_t* state_indices,
    DrakenFixedBuffer* value_ptr,
    Py_ssize_t row_count,
) noexcept nogil:
    """
    Accumulate AVG for a generic integer value column (int8/16/32/64) via _read_integer_value.
    Single-aggregate path.
    """
    cdef Py_ssize_t i
    cdef int64_t val
    cdef int64_t sidx
    cdef uint8_t* value_nulls = <uint8_t*> value_ptr.null_bitmap
    for i in range(row_count):
        if _bitmap_is_valid(value_nulls, i):
            val = _read_integer_value(value_ptr, i)
            sidx = state_indices[i]
            avg_sums[sidx] = avg_sums[sidx] + <double> val
            avg_counts[sidx] = avg_counts[sidx] + 1


cdef void avg_i64_multi_accumulate(
    double* multi_avg_sums,
    int64_t* multi_avg_counts,
    const int64_t* state_indices,
    const int64_t* values,
    const uint8_t* value_nulls,
    Py_ssize_t row_count,
    Py_ssize_t multi_agg_count,
    Py_ssize_t agg_idx,
) noexcept nogil:
    """
    Accumulate AVG for a plain int64 value column (multi-aggregate path).
    """
    cdef Py_ssize_t i
    cdef Py_ssize_t offset
    for i in range(row_count):
        if _bitmap_is_valid(value_nulls, i):
            offset = state_indices[i] * multi_agg_count + agg_idx
            multi_avg_sums[offset] = multi_avg_sums[offset] + <double> values[i]
            multi_avg_counts[offset] = multi_avg_counts[offset] + 1


cdef void avg_i64_multi_accumulate_from_dict(
    double* multi_avg_sums,
    int64_t* multi_avg_counts,
    const int64_t* state_indices,
    DictAccessor* accessor,
    Py_ssize_t row_count,
    Py_ssize_t multi_agg_count,
    Py_ssize_t agg_idx,
) except *:
    """
    Accumulate AVG from a dictionary-encoded int64 value column (multi-aggregate path).

    Not nogil because _dict_accessor_read_int_value is except*.
    """
    cdef Py_ssize_t i
    cdef Py_ssize_t offset
    cdef int64_t val
    cdef uint8_t* value_nulls = accessor.row_nulls
    for i in range(row_count):
        if _bitmap_is_valid(value_nulls, i):
            val = _dict_accessor_read_int_value(accessor, i)
            offset = state_indices[i] * multi_agg_count + agg_idx
            multi_avg_sums[offset] = multi_avg_sums[offset] + <double> val
            multi_avg_counts[offset] = multi_avg_counts[offset] + 1


cdef void avg_integer_multi_accumulate(
    double* multi_avg_sums,
    int64_t* multi_avg_counts,
    const int64_t* state_indices,
    DrakenFixedBuffer* value_ptr,
    Py_ssize_t row_count,
    Py_ssize_t multi_agg_count,
    Py_ssize_t agg_idx,
) noexcept nogil:
    """
    Accumulate AVG for a generic integer value column (int8/16/32/64) via _read_integer_value.
    Multi-aggregate path.
    """
    cdef Py_ssize_t i
    cdef Py_ssize_t offset
    cdef int64_t val
    cdef uint8_t* value_nulls = <uint8_t*> value_ptr.null_bitmap
    for i in range(row_count):
        if _bitmap_is_valid(value_nulls, i):
            val = _read_integer_value(value_ptr, i)
            offset = state_indices[i] * multi_agg_count + agg_idx
            multi_avg_sums[offset] = multi_avg_sums[offset] + <double> val
            multi_avg_counts[offset] = multi_avg_counts[offset] + 1
