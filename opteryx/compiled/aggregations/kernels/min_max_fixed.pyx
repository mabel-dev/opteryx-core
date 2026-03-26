# cython: language_level=3
# cython: boundscheck=False
# cython: wraparound=False
# cython: nonecheck=False
# cython: cdivision=True
# cython: infer_types=True

from libc.stdint cimport int64_t, uint8_t
from opteryx.compiled.draken.core.buffers cimport DictAccessor, DrakenFixedBuffer
from opteryx.compiled.aggregations.kernels.utils cimport _bitmap_is_valid, _read_integer_value
from opteryx.compiled.aggregations.vector_readers cimport (
    _dict_accessor_read_float_value,
    _dict_accessor_read_int_value,
)


cdef void minmax_f64_accumulate(
    double* f64_state,
    int64_t* seen,
    const int64_t* state_indices,
    const double* values,
    const uint8_t* value_nulls,
    Py_ssize_t row_count,
    bint is_min,
) noexcept nogil:
    """
    Accumulate MIN or MAX for a plain float64 value column (single-aggregate path).

    For each row i: if valid, compare values[i] against f64_state[state_indices[i]],
    updating if is_min ? val < state : val > state.  Marks seen[sidx]=1 on first write.
    """
    cdef Py_ssize_t i
    cdef int64_t sidx
    cdef double val
    if is_min:
        for i in range(row_count):
            if _bitmap_is_valid(value_nulls, i):
                sidx = state_indices[i]
                val = values[i]
                if seen[sidx] == 0 or val < f64_state[sidx]:
                    f64_state[sidx] = val
                seen[sidx] = 1
    else:
        for i in range(row_count):
            if _bitmap_is_valid(value_nulls, i):
                sidx = state_indices[i]
                val = values[i]
                if seen[sidx] == 0 or val > f64_state[sidx]:
                    f64_state[sidx] = val
                seen[sidx] = 1


cdef void minmax_f64_accumulate_from_dict(
    double* f64_state,
    int64_t* seen,
    const int64_t* state_indices,
    DictAccessor* accessor,
    Py_ssize_t row_count,
    bint is_min,
) except *:
    """
    Accumulate MIN or MAX from a dictionary-encoded float64 value column (single-aggregate path).

    Not nogil because _dict_accessor_read_float_value is except*.
    """
    cdef Py_ssize_t i
    cdef int64_t sidx
    cdef double val
    cdef uint8_t* value_nulls = accessor.row_nulls
    if is_min:
        for i in range(row_count):
            if _bitmap_is_valid(value_nulls, i):
                val = _dict_accessor_read_float_value(accessor, i)
                sidx = state_indices[i]
                if seen[sidx] == 0 or val < f64_state[sidx]:
                    f64_state[sidx] = val
                seen[sidx] = 1
    else:
        for i in range(row_count):
            if _bitmap_is_valid(value_nulls, i):
                val = _dict_accessor_read_float_value(accessor, i)
                sidx = state_indices[i]
                if seen[sidx] == 0 or val > f64_state[sidx]:
                    f64_state[sidx] = val
                seen[sidx] = 1


cdef void minmax_i64_accumulate(
    int64_t* i64_state,
    int64_t* seen,
    const int64_t* state_indices,
    const int64_t* values,
    const uint8_t* value_nulls,
    Py_ssize_t row_count,
    bint is_min,
) noexcept nogil:
    """
    Accumulate MIN or MAX for a plain int64 value column (single-aggregate path).

    Handles int64 and timestamp64 (both stored as int64).
    """
    cdef Py_ssize_t i
    cdef int64_t sidx
    cdef int64_t val
    if is_min:
        for i in range(row_count):
            if _bitmap_is_valid(value_nulls, i):
                sidx = state_indices[i]
                val = values[i]
                if seen[sidx] == 0 or val < i64_state[sidx]:
                    i64_state[sidx] = val
                seen[sidx] = 1
    else:
        for i in range(row_count):
            if _bitmap_is_valid(value_nulls, i):
                sidx = state_indices[i]
                val = values[i]
                if seen[sidx] == 0 or val > i64_state[sidx]:
                    i64_state[sidx] = val
                seen[sidx] = 1


cdef void minmax_i64_accumulate_from_dict(
    int64_t* i64_state,
    int64_t* seen,
    const int64_t* state_indices,
    DictAccessor* accessor,
    Py_ssize_t row_count,
    bint is_min,
) except *:
    """
    Accumulate MIN or MAX from a dictionary-encoded int64 value column (single-aggregate path).

    Not nogil because _dict_accessor_read_int_value is except*.
    """
    cdef Py_ssize_t i
    cdef int64_t sidx
    cdef int64_t val
    cdef uint8_t* value_nulls = accessor.row_nulls
    if is_min:
        for i in range(row_count):
            if _bitmap_is_valid(value_nulls, i):
                val = _dict_accessor_read_int_value(accessor, i)
                sidx = state_indices[i]
                if seen[sidx] == 0 or val < i64_state[sidx]:
                    i64_state[sidx] = val
                seen[sidx] = 1
    else:
        for i in range(row_count):
            if _bitmap_is_valid(value_nulls, i):
                val = _dict_accessor_read_int_value(accessor, i)
                sidx = state_indices[i]
                if seen[sidx] == 0 or val > i64_state[sidx]:
                    i64_state[sidx] = val
                seen[sidx] = 1


cdef void minmax_integer_accumulate(
    int64_t* i64_state,
    int64_t* seen,
    const int64_t* state_indices,
    DrakenFixedBuffer* value_ptr,
    Py_ssize_t row_count,
    bint is_min,
) noexcept nogil:
    """
    Accumulate MIN or MAX for a generic integer value column (int8/16/32/64) (single-aggregate path).
    """
    cdef Py_ssize_t i
    cdef int64_t sidx
    cdef int64_t val
    cdef uint8_t* value_nulls = <uint8_t*> value_ptr.null_bitmap
    if is_min:
        for i in range(row_count):
            if _bitmap_is_valid(value_nulls, i):
                val = _read_integer_value(value_ptr, i)
                sidx = state_indices[i]
                if seen[sidx] == 0 or val < i64_state[sidx]:
                    i64_state[sidx] = val
                seen[sidx] = 1
    else:
        for i in range(row_count):
            if _bitmap_is_valid(value_nulls, i):
                val = _read_integer_value(value_ptr, i)
                sidx = state_indices[i]
                if seen[sidx] == 0 or val > i64_state[sidx]:
                    i64_state[sidx] = val
                seen[sidx] = 1


cdef void minmax_f64_multi_accumulate(
    double* multi_f64_state,
    int64_t* multi_seen,
    const int64_t* state_indices,
    const double* values,
    const uint8_t* value_nulls,
    Py_ssize_t row_count,
    Py_ssize_t multi_agg_count,
    Py_ssize_t agg_idx,
    bint is_min,
) noexcept nogil:
    """
    Accumulate MIN or MAX for a plain float64 value column (multi-aggregate path).
    """
    cdef Py_ssize_t i
    cdef Py_ssize_t offset
    cdef double val
    if is_min:
        for i in range(row_count):
            if _bitmap_is_valid(value_nulls, i):
                offset = state_indices[i] * multi_agg_count + agg_idx
                val = values[i]
                if multi_seen[offset] == 0 or val < multi_f64_state[offset]:
                    multi_f64_state[offset] = val
                multi_seen[offset] = 1
    else:
        for i in range(row_count):
            if _bitmap_is_valid(value_nulls, i):
                offset = state_indices[i] * multi_agg_count + agg_idx
                val = values[i]
                if multi_seen[offset] == 0 or val > multi_f64_state[offset]:
                    multi_f64_state[offset] = val
                multi_seen[offset] = 1


cdef void minmax_f64_multi_accumulate_from_dict(
    double* multi_f64_state,
    int64_t* multi_seen,
    const int64_t* state_indices,
    DictAccessor* accessor,
    Py_ssize_t row_count,
    Py_ssize_t multi_agg_count,
    Py_ssize_t agg_idx,
    bint is_min,
) except *:
    """
    Accumulate MIN or MAX from a dictionary-encoded float64 value column (multi-aggregate path).

    Not nogil because _dict_accessor_read_float_value is except*.
    """
    cdef Py_ssize_t i
    cdef Py_ssize_t offset
    cdef double val
    cdef uint8_t* value_nulls = accessor.row_nulls
    if is_min:
        for i in range(row_count):
            if _bitmap_is_valid(value_nulls, i):
                val = _dict_accessor_read_float_value(accessor, i)
                offset = state_indices[i] * multi_agg_count + agg_idx
                if multi_seen[offset] == 0 or val < multi_f64_state[offset]:
                    multi_f64_state[offset] = val
                multi_seen[offset] = 1
    else:
        for i in range(row_count):
            if _bitmap_is_valid(value_nulls, i):
                val = _dict_accessor_read_float_value(accessor, i)
                offset = state_indices[i] * multi_agg_count + agg_idx
                if multi_seen[offset] == 0 or val > multi_f64_state[offset]:
                    multi_f64_state[offset] = val
                multi_seen[offset] = 1


cdef void minmax_i64_multi_accumulate(
    int64_t* multi_i64_state,
    int64_t* multi_seen,
    const int64_t* state_indices,
    const int64_t* values,
    const uint8_t* value_nulls,
    Py_ssize_t row_count,
    Py_ssize_t multi_agg_count,
    Py_ssize_t agg_idx,
    bint is_min,
) noexcept nogil:
    """
    Accumulate MIN or MAX for a plain int64 value column (multi-aggregate path).
    """
    cdef Py_ssize_t i
    cdef Py_ssize_t offset
    cdef int64_t val
    if is_min:
        for i in range(row_count):
            if _bitmap_is_valid(value_nulls, i):
                offset = state_indices[i] * multi_agg_count + agg_idx
                val = values[i]
                if multi_seen[offset] == 0 or val < multi_i64_state[offset]:
                    multi_i64_state[offset] = val
                multi_seen[offset] = 1
    else:
        for i in range(row_count):
            if _bitmap_is_valid(value_nulls, i):
                offset = state_indices[i] * multi_agg_count + agg_idx
                val = values[i]
                if multi_seen[offset] == 0 or val > multi_i64_state[offset]:
                    multi_i64_state[offset] = val
                multi_seen[offset] = 1


cdef void minmax_i64_multi_accumulate_from_dict(
    int64_t* multi_i64_state,
    int64_t* multi_seen,
    const int64_t* state_indices,
    DictAccessor* accessor,
    Py_ssize_t row_count,
    Py_ssize_t multi_agg_count,
    Py_ssize_t agg_idx,
    bint is_min,
) except *:
    """
    Accumulate MIN or MAX from a dictionary-encoded int64 value column (multi-aggregate path).

    Not nogil because _dict_accessor_read_int_value is except*.
    """
    cdef Py_ssize_t i
    cdef Py_ssize_t offset
    cdef int64_t val
    cdef uint8_t* value_nulls = accessor.row_nulls
    if is_min:
        for i in range(row_count):
            if _bitmap_is_valid(value_nulls, i):
                val = _dict_accessor_read_int_value(accessor, i)
                offset = state_indices[i] * multi_agg_count + agg_idx
                if multi_seen[offset] == 0 or val < multi_i64_state[offset]:
                    multi_i64_state[offset] = val
                multi_seen[offset] = 1
    else:
        for i in range(row_count):
            if _bitmap_is_valid(value_nulls, i):
                val = _dict_accessor_read_int_value(accessor, i)
                offset = state_indices[i] * multi_agg_count + agg_idx
                if multi_seen[offset] == 0 or val > multi_i64_state[offset]:
                    multi_i64_state[offset] = val
                multi_seen[offset] = 1


cdef void minmax_integer_multi_accumulate(
    int64_t* multi_i64_state,
    int64_t* multi_seen,
    const int64_t* state_indices,
    DrakenFixedBuffer* value_ptr,
    Py_ssize_t row_count,
    Py_ssize_t multi_agg_count,
    Py_ssize_t agg_idx,
    bint is_min,
) noexcept nogil:
    """
    Accumulate MIN or MAX for a generic integer value column (multi-aggregate path).
    """
    cdef Py_ssize_t i
    cdef Py_ssize_t offset
    cdef int64_t val
    cdef uint8_t* value_nulls = <uint8_t*> value_ptr.null_bitmap
    if is_min:
        for i in range(row_count):
            if _bitmap_is_valid(value_nulls, i):
                val = _read_integer_value(value_ptr, i)
                offset = state_indices[i] * multi_agg_count + agg_idx
                if multi_seen[offset] == 0 or val < multi_i64_state[offset]:
                    multi_i64_state[offset] = val
                multi_seen[offset] = 1
    else:
        for i in range(row_count):
            if _bitmap_is_valid(value_nulls, i):
                val = _read_integer_value(value_ptr, i)
                offset = state_indices[i] * multi_agg_count + agg_idx
                if multi_seen[offset] == 0 or val > multi_i64_state[offset]:
                    multi_i64_state[offset] = val
                multi_seen[offset] = 1
