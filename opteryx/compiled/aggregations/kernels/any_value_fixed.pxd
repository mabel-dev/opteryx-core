# cython: language_level=3

from libc.stdint cimport int64_t, uint8_t
from opteryx.draken.core.buffers cimport DictAccessor, DrakenFixedBuffer


cdef void any_value_fixed_accumulate(
    int64_t* state_values,
    int64_t* seen,
    const int64_t* state_indices,
    DrakenFixedBuffer* value_ptr,
    Py_ssize_t row_count,
) noexcept nogil

cdef void any_value_fixed_accumulate_from_dict(
    int64_t* state_values,
    int64_t* seen,
    const int64_t* state_indices,
    DictAccessor* accessor,
    Py_ssize_t row_count,
) except *

cdef void any_value_fixed_multi_accumulate(
    int64_t* multi_state_values,
    int64_t* multi_seen,
    const int64_t* state_indices,
    DrakenFixedBuffer* value_ptr,
    Py_ssize_t row_count,
    Py_ssize_t multi_agg_count,
    Py_ssize_t agg_idx,
) noexcept nogil

cdef void any_value_fixed_multi_accumulate_from_dict(
    int64_t* multi_state_values,
    int64_t* multi_seen,
    const int64_t* state_indices,
    DictAccessor* accessor,
    Py_ssize_t row_count,
    Py_ssize_t multi_agg_count,
    Py_ssize_t agg_idx,
) except *

cdef void any_value_fixed_integer_accumulate(
    int64_t* state_values,
    int64_t* seen,
    const int64_t* state_indices,
    DrakenFixedBuffer* value_ptr,
    Py_ssize_t row_count,
) noexcept nogil

cdef void any_value_fixed_integer_multi_accumulate(
    int64_t* multi_state_values,
    int64_t* multi_seen,
    const int64_t* state_indices,
    DrakenFixedBuffer* value_ptr,
    Py_ssize_t row_count,
    Py_ssize_t multi_agg_count,
    Py_ssize_t agg_idx,
) noexcept nogil
