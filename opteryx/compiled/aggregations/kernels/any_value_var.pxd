# cython: language_level=3

from libc.stdint cimport int64_t, uint8_t

cdef void any_value_var_accumulate(
    uint8_t* state_bytes,
    int64_t* state_starts,
    int64_t* state_lengths,
    int64_t* seen,
    const int64_t* state_indices,
    const char** values_data,
    const int64_t* values_lengths,
    const uint8_t* value_nulls,
    Py_ssize_t row_count,
) noexcept nogil

cdef void any_value_var_multi_accumulate(
    uint8_t* multi_state_bytes,
    int64_t* multi_state_starts,
    int64_t* multi_state_lengths,
    int64_t* multi_seen,
    const int64_t* state_indices,
    const char** values_data,
    const int64_t* values_lengths,
    const uint8_t* value_nulls,
    Py_ssize_t row_count,
    Py_ssize_t multi_agg_count,
    Py_ssize_t agg_idx,
) noexcept nogil
