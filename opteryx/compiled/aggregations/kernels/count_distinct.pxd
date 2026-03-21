# cython: language_level=3

from libc.stdint cimport int64_t, uint8_t, uint64_t


cdef void count_distinct_accumulate(
    list distinct_sets,
    int64_t* counts,
    const uint64_t* value_hashes,
    const uint8_t* value_nulls,
    const int64_t* state_indices,
    Py_ssize_t row_count,
) except *

cdef void count_distinct_multi_accumulate(
    list distinct_sets,
    int64_t* multi_counts,
    const uint64_t* value_hashes,
    const uint8_t* value_nulls,
    const int64_t* state_indices,
    Py_ssize_t row_count,
    Py_ssize_t multi_agg_count,
    Py_ssize_t agg_idx,
) except *
