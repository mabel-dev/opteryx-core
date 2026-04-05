# cython: language_level=3

from libc.stdint cimport int64_t

cdef void count_star_accumulate(
    int64_t* counts,
    const int64_t* state_indices,
    Py_ssize_t row_count,
) noexcept nogil

cdef void count_star_multi_accumulate(
    int64_t* multi_counts,
    const int64_t* state_indices,
    Py_ssize_t row_count,
    Py_ssize_t multi_agg_count,
    Py_ssize_t agg_idx,
) noexcept nogil

cdef void count_star_multi_accumulate_per_aggregate(
    object state_obj,
    const int64_t* state_indices,
    Py_ssize_t row_count,
) noexcept
