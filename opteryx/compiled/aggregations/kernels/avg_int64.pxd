# cython: language_level=3

from libc.stdint cimport int64_t, uint8_t
from opteryx.draken.core.buffers cimport DictAccessor, DrakenFixedBuffer

cdef void avg_i64_accumulate(
    double* avg_sums,
    int64_t* avg_counts,
    const int64_t* state_indices,
    const int64_t* values,
    const uint8_t* value_nulls,
    Py_ssize_t row_count,
) noexcept nogil

cdef void avg_i64_accumulate_from_dict(
    double* avg_sums,
    int64_t* avg_counts,
    const int64_t* state_indices,
    DictAccessor* accessor,
    Py_ssize_t row_count,
) except *

cdef void avg_integer_accumulate(
    double* avg_sums,
    int64_t* avg_counts,
    const int64_t* state_indices,
    DrakenFixedBuffer* value_ptr,
    Py_ssize_t row_count,
) noexcept nogil

cdef void avg_i64_multi_accumulate(
    double* multi_avg_sums,
    int64_t* multi_avg_counts,
    const int64_t* state_indices,
    const int64_t* values,
    const uint8_t* value_nulls,
    Py_ssize_t row_count,
    Py_ssize_t multi_agg_count,
    Py_ssize_t agg_idx,
) noexcept nogil

cdef void avg_i64_multi_accumulate_from_dict(
    double* multi_avg_sums,
    int64_t* multi_avg_counts,
    const int64_t* state_indices,
    DictAccessor* accessor,
    Py_ssize_t row_count,
    Py_ssize_t multi_agg_count,
    Py_ssize_t agg_idx,
) except *

cdef void avg_integer_multi_accumulate(
    double* multi_avg_sums,
    int64_t* multi_avg_counts,
    const int64_t* state_indices,
    DrakenFixedBuffer* value_ptr,
    Py_ssize_t row_count,
    Py_ssize_t multi_agg_count,
    Py_ssize_t agg_idx,
) noexcept nogil
