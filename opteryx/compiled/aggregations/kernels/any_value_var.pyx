# cython: language_level=3
# cython: boundscheck=False
# cython: wraparound=False
# cython: nonecheck=False
# cython: cdivision=True
# cython: infer_types=True

"""
ANY_VALUE kernel for variable-width values.

This kernel keeps the first non-null value per state. For variable-width data,
the caller is expected to provide pointers into arena/object storage and the
kernel simply copies bytes into the provided state storage on the first hit.

Design intent:
- single-aggregate path: state_bytes/state_starts/state_lengths + seen bitmap
- multi-aggregate path: multi_state_bytes/multi_starts/multi_lengths + multi_seen
- null rows are skipped
- once a state has been set, subsequent rows are ignored
"""

from libc.stdint cimport int64_t, uint8_t
from libc.stddef cimport size_t
from libc.string cimport memcpy

from opteryx.compiled.aggregations.kernels.utils cimport _bitmap_is_valid


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
) noexcept nogil:
    """
    Keep the first non-null variable-width value for each state.

    Parameters
    ----------
    state_bytes:
        Flat arena-backed byte buffer for stored state values.
    state_starts / state_lengths:
        Per-state offsets and lengths into state_bytes.
    seen:
        Per-state first-value flag. Once set, the state is left unchanged.
    state_indices:
        Group-state index for each row.
    values_data / values_lengths:
        Row-wise pointers and lengths for the incoming variable-width values.
    value_nulls:
        Null bitmap for the input column. NULL means all rows are valid.
    row_count:
        Number of rows in this batch.
    """
    cdef Py_ssize_t i
    cdef int64_t sidx
    cdef const char* src
    cdef int64_t src_len

    for i in range(row_count):
        if not _bitmap_is_valid(value_nulls, i):
            continue

        sidx = state_indices[i]
        if seen[sidx] != 0:
            continue

        src = values_data[i]
        src_len = values_lengths[i]
        if src != NULL and src_len > 0:
            memcpy(
                <void*> (state_bytes + state_starts[sidx]),
                <const void*> src,
                <size_t> src_len,
            )
        state_lengths[sidx] = src_len
        seen[sidx] = 1


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
) noexcept nogil:
    """
    Keep the first non-null variable-width value for each (state, aggregate) slot.
    """
    cdef Py_ssize_t i
    cdef Py_ssize_t offset
    cdef const char* src
    cdef int64_t src_len

    for i in range(row_count):
        if not _bitmap_is_valid(value_nulls, i):
            continue

        offset = state_indices[i] * multi_agg_count + agg_idx
        if multi_seen[offset] != 0:
            continue

        src = values_data[i]
        src_len = values_lengths[i]
        if src != NULL and src_len > 0:
            memcpy(
                <void*> (multi_state_bytes + multi_state_starts[offset]),
                <const void*> src,
                <size_t> src_len,
            )
        multi_state_lengths[offset] = src_len
        multi_seen[offset] = 1
