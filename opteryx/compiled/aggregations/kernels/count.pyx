# cython: language_level=3
# cython: boundscheck=False
# cython: wraparound=False
# cython: nonecheck=False
# cython: cdivision=True
# cython: infer_types=True

"""
kernels/count.pyx — COUNT(col) accumulation kernels.

COUNT(col) is a null check: increment the counter if the validity bit is set.
The primitive is the validity bitmap, not the value type — a single file covers
every column type with no type specialisation.

Both functions are noexcept nogil: the only input is a null bitmap and a
pre-built state_indices array, so there are no Python objects in the hot path.

The engine is responsible for:
  - building state_indices (group-state index for each row)
  - obtaining the value column's null bitmap via _vector_null_bitmap(value_vector)
before calling into either kernel.
"""

from libc.stdint cimport int64_t, uint8_t

from opteryx.compiled.aggregations.kernels.utils cimport _bitmap_is_valid


# ---------------------------------------------------------------------------
# Single-aggregate path
# ---------------------------------------------------------------------------

cdef void count_accumulate(
    int64_t* counts,
    const int64_t* state_indices,
    const uint8_t* value_nulls,
    Py_ssize_t row_count,
) noexcept nogil:
    """
    Accumulate COUNT(col) for a single-aggregate path.

    counts          Per-state int64_t counter array (self._counts.data()).
    state_indices   Group-state index for each row.
    value_nulls     Null bitmap for the value column; NULL means all rows are
                    non-null (every row increments its state's counter).
    row_count       Number of rows in this morsel batch.

    For each row i where value_nulls[i] is valid (non-null),
    counts[state_indices[i]] is incremented by 1.
    """
    cdef Py_ssize_t i
    for i in range(row_count):
        if _bitmap_is_valid(value_nulls, i):
            counts[state_indices[i]] = counts[state_indices[i]] + 1


# ---------------------------------------------------------------------------
# Multi-aggregate path
# ---------------------------------------------------------------------------

cdef void count_multi_accumulate(
    int64_t* multi_counts,
    const int64_t* state_indices,
    const uint8_t* value_nulls,
    Py_ssize_t row_count,
    Py_ssize_t multi_agg_count,
    Py_ssize_t agg_idx,
) noexcept nogil:
    """
    Accumulate COUNT(col) for a multi-aggregate path.

    multi_counts    Flat int64_t counter array (self._multi_counts.data()).
                    The slot for (state, agg_idx) is at
                    state_index * multi_agg_count + agg_idx.
    state_indices   Group-state index for each row.
    value_nulls     Null bitmap for the value column; NULL means all rows are
                    non-null.
    row_count       Number of rows in this morsel batch.
    multi_agg_count Total number of aggregates in multi-agg mode.
    agg_idx         Which aggregate slot this call is servicing.

    For each row i where value_nulls[i] is valid (non-null),
    multi_counts[state_indices[i] * multi_agg_count + agg_idx] is
    incremented by 1.
    """
    cdef Py_ssize_t i
    cdef Py_ssize_t offset
    for i in range(row_count):
        if _bitmap_is_valid(value_nulls, i):
            offset = state_indices[i] * multi_agg_count + agg_idx
            multi_counts[offset] = multi_counts[offset] + 1
