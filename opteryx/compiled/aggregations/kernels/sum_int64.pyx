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
from opteryx.compiled.aggregations.aggregations_state_classes cimport PerAggregateSumInt64State


cdef void sum_i64_accumulate(
    int64_t* i64_state,
    int64_t* seen,
    const int64_t* state_indices,
    const int64_t* values,
    const uint8_t* value_nulls,
    Py_ssize_t row_count,
) noexcept nogil:
    """
    Accumulate SUM for a plain int64 value column (single-aggregate path).
    """
    cdef Py_ssize_t i
    cdef int64_t sidx
    for i in range(row_count):
        if _bitmap_is_valid(value_nulls, i):
            sidx = state_indices[i]
            i64_state[sidx] = i64_state[sidx] + values[i]
            seen[sidx] = 1


cdef void sum_i64_accumulate_from_dict(
    int64_t* i64_state,
    int64_t* seen,
    const int64_t* state_indices,
    DictAccessor* accessor,
    Py_ssize_t row_count,
) except *:
    """
    Accumulate SUM from a dictionary-encoded int64 value column (single-aggregate path).

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
            i64_state[sidx] = i64_state[sidx] + val
            seen[sidx] = 1


cdef void sum_integer_accumulate(
    int64_t* i64_state,
    int64_t* seen,
    const int64_t* state_indices,
    DrakenFixedBuffer* value_ptr,
    Py_ssize_t row_count,
) noexcept nogil:
    """
    Accumulate SUM for a generic integer value column (int8/16/32/64) via _read_integer_value.
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
            i64_state[sidx] = i64_state[sidx] + val
            seen[sidx] = 1


cdef void sum_i64_multi_accumulate(
    int64_t* multi_i64_state,
    int64_t* multi_seen,
    const int64_t* state_indices,
    const int64_t* values,
    const uint8_t* value_nulls,
    Py_ssize_t row_count,
    Py_ssize_t multi_agg_count,
    Py_ssize_t agg_idx,
) noexcept nogil:
    """
    Accumulate SUM for a plain int64 value column (multi-aggregate path).
    """
    cdef Py_ssize_t i
    cdef Py_ssize_t offset
    for i in range(row_count):
        if _bitmap_is_valid(value_nulls, i):
            offset = state_indices[i] * multi_agg_count + agg_idx
            multi_i64_state[offset] = multi_i64_state[offset] + values[i]
            multi_seen[offset] = 1


cdef void sum_i64_multi_accumulate_from_dict(
    int64_t* multi_i64_state,
    int64_t* multi_seen,
    const int64_t* state_indices,
    DictAccessor* accessor,
    Py_ssize_t row_count,
    Py_ssize_t multi_agg_count,
    Py_ssize_t agg_idx,
) except *:
    """
    Accumulate SUM from a dictionary-encoded int64 value column (multi-aggregate path).

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
            multi_i64_state[offset] = multi_i64_state[offset] + val
            multi_seen[offset] = 1


cdef void sum_integer_multi_accumulate(
    int64_t* multi_i64_state,
    int64_t* multi_seen,
    const int64_t* state_indices,
    DrakenFixedBuffer* value_ptr,
    Py_ssize_t row_count,
    Py_ssize_t multi_agg_count,
    Py_ssize_t agg_idx,
) noexcept nogil:
    """
    Accumulate SUM for a generic integer value column (int8/16/32/64) via _read_integer_value.
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
            multi_i64_state[offset] = multi_i64_state[offset] + val
            multi_seen[offset] = 1


cdef void sum_i64_multi_accumulate_per_aggregate(
    object state_obj,
    const int64_t* state_indices,
    const int64_t* values,
    const uint8_t* value_nulls,
    Py_ssize_t row_count,
) noexcept:
    """
    Accumulate SUM for a plain int64 value column using per-aggregate state object.

    For each row i in [0, row_count):
      if value_nulls[i] is valid: values[state_indices[i]] += values[i]; seen[state_indices[i]] = 1

    Direct indexing by state_index without offset math.
    """
    cdef Py_ssize_t i
    cdef int64_t sidx
    cdef PerAggregateSumInt64State state_obj_cast = <PerAggregateSumInt64State>state_obj
    cdef int64_t* values_ptr = state_obj_cast.values.data()
    cdef int64_t* seen_ptr = state_obj_cast.seen.data()
    for i in range(row_count):
        if _bitmap_is_valid(value_nulls, i):
            sidx = state_indices[i]
            values_ptr[sidx] = values_ptr[sidx] + values[i]
            seen_ptr[sidx] = 1


cdef void sum_i64_multi_accumulate_from_dict_per_aggregate(
    object state_obj,
    const int64_t* state_indices,
    DictAccessor* accessor,
    Py_ssize_t row_count,
) except *:
    """
    Accumulate SUM from a dictionary-encoded int64 value column using per-aggregate state object.

    For each row i in [0, row_count):
      if value_nulls[i] is valid: values[state_indices[i]] += dict_value[i]; seen[state_indices[i]] = 1

    Not nogil because _dict_accessor_read_int_value is except*.
    Direct indexing by state_index without offset math.
    """
    cdef Py_ssize_t i
    cdef int64_t sidx
    cdef int64_t val
    cdef uint8_t* value_nulls = accessor.row_nulls
    cdef PerAggregateSumInt64State state_obj_cast = <PerAggregateSumInt64State>state_obj
    cdef int64_t* values_ptr = state_obj_cast.values.data()
    cdef int64_t* seen_ptr = state_obj_cast.seen.data()
    for i in range(row_count):
        if _bitmap_is_valid(value_nulls, i):
            val = _dict_accessor_read_int_value(accessor, i)
            sidx = state_indices[i]
            values_ptr[sidx] = values_ptr[sidx] + val
            seen_ptr[sidx] = 1


cdef void sum_integer_multi_accumulate_per_aggregate(
    object state_obj,
    const int64_t* state_indices,
    DrakenFixedBuffer* value_ptr,
    Py_ssize_t row_count,
) noexcept:
    """
    Accumulate SUM for a generic integer value column (int8/16/32/64) using per-aggregate state object.

    For each row i in [0, row_count):
      if value_nulls[i] is valid: values[state_indices[i]] += read_value[i]; seen[state_indices[i]] = 1

    Direct indexing by state_index without offset math.
    """
    cdef Py_ssize_t i
    cdef int64_t sidx
    cdef int64_t val
    cdef uint8_t* value_nulls = <uint8_t*> value_ptr.null_bitmap
    cdef PerAggregateSumInt64State state_obj_cast = <PerAggregateSumInt64State>state_obj
    cdef int64_t* values_ptr = state_obj_cast.values.data()
    cdef int64_t* seen_ptr = state_obj_cast.seen.data()
    for i in range(row_count):
        if _bitmap_is_valid(value_nulls, i):
            val = _read_integer_value(value_ptr, i)
            sidx = state_indices[i]
            values_ptr[sidx] = values_ptr[sidx] + val
            seen_ptr[sidx] = 1
