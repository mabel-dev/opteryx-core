# cython: language_level=3

from libc.stdint cimport int64_t, uint8_t
from opteryx.draken.core.buffers cimport DictAccessor, DrakenFixedBuffer

cdef void minmax_f64_accumulate(
    double* f64_state,
    int64_t* seen,
    const int64_t* state_indices,
    const double* values,
    const uint8_t* value_nulls,
    Py_ssize_t row_count,
    bint is_min,
) noexcept nogil

cdef void minmax_f64_accumulate_from_dict(
    double* f64_state,
    int64_t* seen,
    const int64_t* state_indices,
    DictAccessor* accessor,
    Py_ssize_t row_count,
    bint is_min,
) except *

cdef void minmax_i64_accumulate(
    int64_t* i64_state,
    int64_t* seen,
    const int64_t* state_indices,
    const int64_t* values,
    const uint8_t* value_nulls,
    Py_ssize_t row_count,
    bint is_min,
) noexcept nogil

cdef void minmax_i64_accumulate_from_dict(
    int64_t* i64_state,
    int64_t* seen,
    const int64_t* state_indices,
    DictAccessor* accessor,
    Py_ssize_t row_count,
    bint is_min,
) except *

cdef void minmax_integer_accumulate(
    int64_t* i64_state,
    int64_t* seen,
    const int64_t* state_indices,
    DrakenFixedBuffer* value_ptr,
    Py_ssize_t row_count,
    bint is_min,
) noexcept nogil

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
) noexcept nogil

cdef void minmax_f64_multi_accumulate_from_dict(
    double* multi_f64_state,
    int64_t* multi_seen,
    const int64_t* state_indices,
    DictAccessor* accessor,
    Py_ssize_t row_count,
    Py_ssize_t multi_agg_count,
    Py_ssize_t agg_idx,
    bint is_min,
) except *

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
) noexcept nogil

cdef void minmax_i64_multi_accumulate_from_dict(
    int64_t* multi_i64_state,
    int64_t* multi_seen,
    const int64_t* state_indices,
    DictAccessor* accessor,
    Py_ssize_t row_count,
    Py_ssize_t multi_agg_count,
    Py_ssize_t agg_idx,
    bint is_min,
) except *

cdef void minmax_integer_multi_accumulate(
    int64_t* multi_i64_state,
    int64_t* multi_seen,
    const int64_t* state_indices,
    DrakenFixedBuffer* value_ptr,
    Py_ssize_t row_count,
    Py_ssize_t multi_agg_count,
    Py_ssize_t agg_idx,
    bint is_min,
) noexcept nogil
