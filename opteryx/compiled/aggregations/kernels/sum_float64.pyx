# cython: language_level=3
# cython: boundscheck=False
# cython: wraparound=False
# cython: nonecheck=False
# cython: cdivision=True
# cython: infer_types=True

from libc.stdint cimport int64_t, uint8_t
from opteryx.compiled.draken.core.buffers cimport DictAccessor
from opteryx.compiled.aggregations.kernels.utils cimport _bitmap_is_valid
from opteryx.compiled.aggregations.vector_readers cimport _dict_accessor_read_float_value


cdef void sum_f64_accumulate(
    double* f64_state,
    int64_t* seen,
    const int64_t* state_indices,
    const double* values,
    const uint8_t* value_nulls,
    Py_ssize_t row_count,
) noexcept nogil:
    """
    Accumulate SUM for a plain float64 value column (single-aggregate path).

    For each row i in [0, row_count):
      if value_nulls[i] is valid:  f64_state[state_indices[i]] += values[i]; seen[state_indices[i]] = 1
    """
    cdef Py_ssize_t i
    cdef int64_t sidx
    for i in range(row_count):
        if _bitmap_is_valid(value_nulls, i):
            sidx = state_indices[i]
            f64_state[sidx] = f64_state[sidx] + values[i]
            seen[sidx] = 1


cdef void sum_f64_accumulate_from_dict(
    double* f64_state,
    int64_t* seen,
    const int64_t* state_indices,
    DictAccessor* accessor,
    Py_ssize_t row_count,
) except *:
    """
    Accumulate SUM from a dictionary-encoded float64 value column (single-aggregate path).

    Decodes each value via the DictAccessor, then accumulates into f64_state.
    Not nogil because _dict_accessor_read_float_value is except*.
    """
    cdef Py_ssize_t i
    cdef double val
    cdef int64_t sidx
    cdef uint8_t* value_nulls = accessor.row_nulls
    for i in range(row_count):
        if _bitmap_is_valid(value_nulls, i):
            val = _dict_accessor_read_float_value(accessor, i)
            sidx = state_indices[i]
            f64_state[sidx] = f64_state[sidx] + val
            seen[sidx] = 1


cdef void sum_f64_multi_accumulate(
    double* multi_f64_state,
    int64_t* multi_seen,
    const int64_t* state_indices,
    const double* values,
    const uint8_t* value_nulls,
    Py_ssize_t row_count,
    Py_ssize_t multi_agg_count,
    Py_ssize_t agg_idx,
) noexcept nogil:
    """
    Accumulate SUM for a plain float64 value column (multi-aggregate path).

    offset formula: state_indices[i] * multi_agg_count + agg_idx
    """
    cdef Py_ssize_t i
    cdef Py_ssize_t offset
    for i in range(row_count):
        if _bitmap_is_valid(value_nulls, i):
            offset = state_indices[i] * multi_agg_count + agg_idx
            multi_f64_state[offset] = multi_f64_state[offset] + values[i]
            multi_seen[offset] = 1


cdef void sum_f64_multi_accumulate_from_dict(
    double* multi_f64_state,
    int64_t* multi_seen,
    const int64_t* state_indices,
    DictAccessor* accessor,
    Py_ssize_t row_count,
    Py_ssize_t multi_agg_count,
    Py_ssize_t agg_idx,
) except *:
    """
    Accumulate SUM from a dictionary-encoded float64 value column (multi-aggregate path).

    Not nogil because _dict_accessor_read_float_value is except*.
    """
    cdef Py_ssize_t i
    cdef Py_ssize_t offset
    cdef double val
    cdef uint8_t* value_nulls = accessor.row_nulls
    for i in range(row_count):
        if _bitmap_is_valid(value_nulls, i):
            val = _dict_accessor_read_float_value(accessor, i)
            offset = state_indices[i] * multi_agg_count + agg_idx
            multi_f64_state[offset] = multi_f64_state[offset] + val
            multi_seen[offset] = 1
