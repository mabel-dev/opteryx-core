# cython: language_level=3

from libc.stdint cimport int64_t, uint8_t


cdef void count_accumulate(
    int64_t* counts,
    const int64_t* state_indices,
    const uint8_t* value_nulls,
    Py_ssize_t row_count,
) noexcept nogil

cdef void count_multi_accumulate(
    int64_t* multi_counts,
    const int64_t* state_indices,
    const uint8_t* value_nulls,
    Py_ssize_t row_count,
    Py_ssize_t multi_agg_count,
    Py_ssize_t agg_idx,
) noexcept nogil
