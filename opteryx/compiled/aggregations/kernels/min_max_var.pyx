# cython: language_level=3
# cython: boundscheck=False
# cython: wraparound=False
# cython: nonecheck=False
# cython: cdivision=True
# cython: infer_types=True

"""
kernels/min_max_var.pyx

Temporary inert placeholders for variable-width MIN/MAX kernels.

These exist only to restore a clean build while Step D7 remains unwired in the
engine. The real string MIN/MAX path is still engine-owned, so these functions
must not be called in production code yet.

They intentionally do no work.
"""

from libc.stdint cimport int64_t, uint8_t
from opteryx.draken.core.buffers cimport DictAccessor


cdef void minmax_var_accumulate(
    int64_t* object_state_starts,
    int64_t* object_state_lengths,
    int64_t* seen,
    const int64_t* state_indices,
    const char* const* data_ptrs,
    const int64_t* data_lens,
    const uint8_t* value_nulls,
    Py_ssize_t row_count,
    bint is_min,
) noexcept nogil:
    return


cdef void minmax_var_accumulate_from_dict(
    int64_t* object_state_starts,
    int64_t* object_state_lengths,
    int64_t* seen,
    const int64_t* state_indices,
    DictAccessor* accessor,
    Py_ssize_t row_count,
    bint is_min,
) except *:
    return


cdef void minmax_var_multi_accumulate(
    int64_t* multi_object_state_starts,
    int64_t* multi_object_state_lengths,
    int64_t* multi_seen,
    const int64_t* state_indices,
    const char* const* data_ptrs,
    const int64_t* data_lens,
    const uint8_t* value_nulls,
    Py_ssize_t row_count,
    Py_ssize_t multi_agg_count,
    Py_ssize_t agg_idx,
    bint is_min,
) noexcept nogil:
    return


cdef void minmax_var_multi_accumulate_from_dict(
    int64_t* multi_object_state_starts,
    int64_t* multi_object_state_lengths,
    int64_t* multi_seen,
    const int64_t* state_indices,
    DictAccessor* accessor,
    Py_ssize_t row_count,
    Py_ssize_t multi_agg_count,
    Py_ssize_t agg_idx,
    bint is_min,
) except *:
    return
