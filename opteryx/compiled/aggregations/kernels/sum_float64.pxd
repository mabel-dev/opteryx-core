# cython: language_level=3

from libc.stdint cimport int64_t, uint8_t
from opteryx.compiled.draken.core.buffers cimport DictAccessor

cdef void sum_f64_accumulate(
    double* f64_state,
    int64_t* seen,
    const int64_t* state_indices,
    const double* values,
    const uint8_t* value_nulls,
    Py_ssize_t row_count,
) noexcept nogil

cdef void sum_f64_accumulate_from_dict(
    double* f64_state,
    int64_t* seen,
    const int64_t* state_indices,
    DictAccessor* accessor,
    Py_ssize_t row_count,
) except *

cdef void sum_f64_multi_accumulate(
    double* multi_f64_state,
    int64_t* multi_seen,
    const int64_t* state_indices,
    const double* values,
    const uint8_t* value_nulls,
    Py_ssize_t row_count,
    Py_ssize_t multi_agg_count,
    Py_ssize_t agg_idx,
) noexcept nogil

cdef void sum_f64_multi_accumulate_from_dict(
    double* multi_f64_state,
    int64_t* multi_seen,
    const int64_t* state_indices,
    DictAccessor* accessor,
    Py_ssize_t row_count,
    Py_ssize_t multi_agg_count,
    Py_ssize_t agg_idx,
) except *

# Phase 2: Per-aggregate kernel signatures
cdef void sum_f64_multi_accumulate_per_aggregate(
    object state_obj,
    const int64_t* state_indices,
    const double* values,
    const uint8_t* value_nulls,
    Py_ssize_t row_count,
) noexcept

cdef void sum_f64_multi_accumulate_from_dict_per_aggregate(
    object state_obj,
    const int64_t* state_indices,
    DictAccessor* accessor,
    Py_ssize_t row_count,
) except *
