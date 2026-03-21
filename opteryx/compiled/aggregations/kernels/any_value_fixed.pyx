# cython: language_level=3
# cython: boundscheck=False
# cython: wraparound=False
# cython: nonecheck=False
# cython: cdivision=True
# cython: infer_types=True

"""
kernels/any_value_fixed.pyx — ANY_VALUE accumulation for fixed-width values.

ANY_VALUE keeps the first non-null value seen for each group/state and ignores
all subsequent rows for that state.

This kernel is for fixed-width scalar storage only:
  - int64
  - float64
  - generic fixed-width integer vectors via DrakenFixedBuffer

The engine owns state initialization and finalization. The kernel only writes
the first valid row for each state.
"""

from libc.stdint cimport int64_t, uint8_t

from opteryx.draken.core.buffers cimport DictAccessor, DrakenFixedBuffer
from opteryx.compiled.aggregations.kernels.utils cimport _bitmap_is_valid
from opteryx.compiled.aggregations.kernels.utils cimport _read_integer_value


# ---------------------------------------------------------------------------
# Single-aggregate path
# ---------------------------------------------------------------------------

cdef void any_value_fixed_accumulate(
    int64_t* i64_state,
    int64_t* seen,
    const int64_t* state_indices,
    DrakenFixedBuffer* value_ptr,
    Py_ssize_t row_count,
) noexcept nogil:
    """
    Accumulate ANY_VALUE for a fixed-width int64-like column.

    The first non-null row for each state is stored, and later rows are skipped.
    """
    cdef Py_ssize_t i
    cdef int64_t sidx
    cdef int64_t val
    cdef int64_t* values = <int64_t*> value_ptr.data
    cdef uint8_t* value_nulls = <uint8_t*> value_ptr.null_bitmap

    for i in range(row_count):
        if _bitmap_is_valid(value_nulls, i):
            sidx = state_indices[i]
            if seen[sidx] == 0:
                val = values[i]
                i64_state[sidx] = val
                seen[sidx] = 1


cdef void any_value_fixed_accumulate_from_dict(
    int64_t* i64_state,
    int64_t* seen,
    const int64_t* state_indices,
    DictAccessor* accessor,
    Py_ssize_t row_count,
) except *:
    """
    Placeholder for dictionary-backed vectors.

    ANY_VALUE fixed-width is intended for direct fixed-width storage. The engine
    can route dictionary-encoded values through the generic object/var path if
    needed.
    """
    raise NotImplementedError("dictionary-backed ANY_VALUE fixed kernel is not implemented")


cdef void any_value_fixed_multi_accumulate(
    int64_t* multi_i64_state,
    int64_t* multi_seen,
    const int64_t* state_indices,
    DrakenFixedBuffer* value_ptr,
    Py_ssize_t row_count,
    Py_ssize_t multi_agg_count,
    Py_ssize_t agg_idx,
) noexcept nogil:
    """
    Accumulate ANY_VALUE for a fixed-width int64-like column in multi-agg mode.

    The first non-null row for each (state, agg_idx) slot is stored.
    """
    cdef Py_ssize_t i
    cdef Py_ssize_t offset
    cdef int64_t sidx
    cdef int64_t val
    cdef int64_t* values = <int64_t*> value_ptr.data
    cdef uint8_t* value_nulls = <uint8_t*> value_ptr.null_bitmap

    for i in range(row_count):
        if _bitmap_is_valid(value_nulls, i):
            sidx = state_indices[i]
            offset = sidx * multi_agg_count + agg_idx
            if multi_seen[offset] == 0:
                val = values[i]
                multi_i64_state[offset] = val
                multi_seen[offset] = 1


cdef void any_value_fixed_multi_accumulate_from_dict(
    int64_t* multi_i64_state,
    int64_t* multi_seen,
    const int64_t* state_indices,
    DictAccessor* accessor,
    Py_ssize_t row_count,
    Py_ssize_t multi_agg_count,
    Py_ssize_t agg_idx,
) except *:
    """
    Placeholder for dictionary-backed vectors.

    ANY_VALUE fixed-width is intended for direct fixed-width storage. The engine
    can route dictionary-encoded values through the generic object/var path if
    needed.
    """
    raise NotImplementedError("dictionary-backed ANY_VALUE fixed kernel is not implemented")


cdef void any_value_fixed_integer_accumulate(
    int64_t* i64_state,
    int64_t* seen,
    const int64_t* state_indices,
    DrakenFixedBuffer* value_ptr,
    Py_ssize_t row_count,
) noexcept nogil:
    """
    Accumulate ANY_VALUE for generic integer fixed-width vectors.
    """
    cdef Py_ssize_t i
    cdef int64_t sidx
    cdef int64_t val
    cdef uint8_t* value_nulls = <uint8_t*> value_ptr.null_bitmap

    for i in range(row_count):
        if _bitmap_is_valid(value_nulls, i):
            sidx = state_indices[i]
            if seen[sidx] == 0:
                val = _read_integer_value(value_ptr, i)
                i64_state[sidx] = val
                seen[sidx] = 1


cdef void any_value_fixed_integer_multi_accumulate(
    int64_t* multi_i64_state,
    int64_t* multi_seen,
    const int64_t* state_indices,
    DrakenFixedBuffer* value_ptr,
    Py_ssize_t row_count,
    Py_ssize_t multi_agg_count,
    Py_ssize_t agg_idx,
) noexcept nogil:
    """
    Accumulate ANY_VALUE for generic integer fixed-width vectors in multi-agg mode.
    """
    cdef Py_ssize_t i
    cdef Py_ssize_t offset
    cdef int64_t sidx
    cdef int64_t val
    cdef uint8_t* value_nulls = <uint8_t*> value_ptr.null_bitmap

    for i in range(row_count):
        if _bitmap_is_valid(value_nulls, i):
            sidx = state_indices[i]
            offset = sidx * multi_agg_count + agg_idx
            if multi_seen[offset] == 0:
                val = _read_integer_value(value_ptr, i)
                multi_i64_state[offset] = val
                multi_seen[offset] = 1
