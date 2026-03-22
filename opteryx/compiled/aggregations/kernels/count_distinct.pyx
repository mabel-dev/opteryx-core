# cython: language_level=3
# cython: boundscheck=False
# cython: wraparound=False
# cython: nonecheck=False
# cython: cdivision=True
# cython: infer_types=True

"""
kernels/count_distinct.pyx — COUNT(DISTINCT) accumulation kernels.

Both functions receive a *pre-hashed* uint64_t value array; the type-specific
divergence (Int64 direct reinterpret-cast, IntegerVector expand-to-u64,
morsel.hash() for strings/other) is performed by the engine before calling
into these kernels.

Uses CarcharSet which provides insert_many() that returns the count of newly
inserted values, allowing efficient batch insertion with immediate feedback
on how many distinct values were added.
"""

from libc.stdint cimport int64_t, uint8_t, uint64_t

from opteryx.compiled.aggregations.kernels.utils cimport _bitmap_is_valid

import numpy


# Import CarcharSet from nanobind binding
from opteryx.nanobind.carchar_native import CarcharSet


# ---------------------------------------------------------------------------
# Single-aggregate path
# ---------------------------------------------------------------------------

cdef void count_distinct_accumulate(
    list distinct_sets,
    int64_t* counts,
    const uint64_t* value_hashes,
    const uint8_t* value_nulls,
    const int64_t* state_indices,
    Py_ssize_t row_count,
) except *:
    """
    Accumulate COUNT(DISTINCT) for a pre-hashed value column (single-aggregate path).

    distinct_sets   Python list[CarcharSet], one entry per group state.
                    len(distinct_sets) == number of group states allocated so far.
    counts          Per-state int64_t counter array (self._counts.data()).
    value_hashes    uint64_t hash / bit-cast for every row of the value column.
    value_nulls     Null bitmap for the value column; NULL means all rows are non-null.
    state_indices   Group-state index for each row.
    row_count       Number of rows in this morsel batch.

    For each non-null row i the hash value_hashes[i] is inserted into the
    CarcharSet for group state state_indices[i].  counts[state] is
    incremented by the number of newly inserted values (carchar.insert_many
    returns the count of new insertions).
    """
    cdef Py_ssize_t n_states = len(distinct_sets)
    cdef Py_ssize_t state_idx
    cdef Py_ssize_t row_idx
    cdef int64_t sidx
    cdef list state_hashes
    cdef uint64_t[::1] temp_hashes_view
    cdef size_t new_count

    if n_states == 0 or row_count == 0:
        return

    # Collect hashes grouped by state
    state_hashes = [[] for _ in range(n_states)]
    for row_idx in range(row_count):
        if _bitmap_is_valid(value_nulls, row_idx):
            sidx = state_indices[row_idx]
            state_hashes[sidx].append(value_hashes[row_idx])

    # Insert batches per state and accumulate new count
    for state_idx in range(n_states):
        if state_hashes[state_idx]:
            temp_hashes_view = numpy.array(state_hashes[state_idx], dtype=numpy.uint64)
            new_count = distinct_sets[state_idx].insert_many(temp_hashes_view)
            counts[state_idx] = counts[state_idx] + <int64_t> new_count


# ---------------------------------------------------------------------------
# Multi-aggregate path
# ---------------------------------------------------------------------------

cdef void count_distinct_multi_accumulate(
    list distinct_sets,
    int64_t* multi_counts,
    const uint64_t* value_hashes,
    const uint8_t* value_nulls,
    const int64_t* state_indices,
    Py_ssize_t row_count,
    Py_ssize_t multi_agg_count,
    Py_ssize_t agg_idx,
) except *:
    """
    Accumulate COUNT(DISTINCT) for a pre-hashed value column (multi-aggregate path).

    distinct_sets   self._multi_distinct_sets — a flat Python list of length
                    (num_group_states * multi_agg_count).  Slots at index
                    (state * multi_agg_count + agg_idx) hold CarcharSet
                    objects for COUNT(DISTINCT) aggregates; all other slots
                    hold None and are not accessed by this function.
    multi_counts    Flat int64_t counter array (self._multi_counts.data()).
                    The slot for (state, agg_idx) is at
                    state * multi_agg_count + agg_idx.
    value_hashes    uint64_t hash / bit-cast for every row of the value column.
    value_nulls     Null bitmap for the value column; NULL means all rows are non-null.
    state_indices   Group-state index for each row.
    row_count       Number of rows in this morsel batch.
    multi_agg_count Total number of aggregates in multi-agg mode.
    agg_idx         Which aggregate slot this call is servicing.
    """
    cdef Py_ssize_t n_states
    cdef Py_ssize_t state_idx
    cdef Py_ssize_t row_idx
    cdef int64_t sidx
    cdef Py_ssize_t offset
    cdef list state_hashes
    cdef uint64_t[::1] temp_hashes_view
    cdef size_t new_count

    if multi_agg_count == 0 or row_count == 0:
        return

    n_states = len(distinct_sets) // multi_agg_count
    if n_states == 0:
        return

    # Collect hashes grouped by state for this aggregation index
    state_hashes = [[] for _ in range(n_states)]
    for row_idx in range(row_count):
        if _bitmap_is_valid(value_nulls, row_idx):
            sidx = state_indices[row_idx]
            state_hashes[sidx].append(value_hashes[row_idx])

    # Insert batches per state and accumulate new count
    for state_idx in range(n_states):
        if state_hashes[state_idx]:
            offset = state_idx * multi_agg_count + agg_idx
            temp_hashes_view = numpy.array(state_hashes[state_idx], dtype=numpy.uint64)
            new_count = distinct_sets[offset].insert_many(temp_hashes_view)
            multi_counts[offset] = multi_counts[offset] + <int64_t> new_count
