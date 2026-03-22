# cython: language_level=3
# cython: boundscheck=False
# cython: wraparound=False
# cython: nonecheck=False
# cython: cdivision=True
# cython: infer_types=True

"""
kernels/any_value_var.pyx — Variable-width ANY_VALUE accumulation kernels.

ANY_VALUE keeps the first non-null value seen per group state and ignores all
subsequent rows for that state.

The engine is responsible for the outer plumbing:
  1. Extracting per-row string pointers/lengths from the value column
     (via _extract_stringlike_key for StringVector / dict-string).
  2. Pre-allocating enough space in the arena:
       self._object_state_bytes.resize(current_size + max_bytes_needed)
  3. Passing a mutable arena_cursor pointer; the kernel advances it as it
     appends new values into the pre-allocated region.
  4. Trimming the arena after the call:
       self._object_state_bytes.resize(arena_cursor[0])

The kernel never calls push_back or any Python function — the entire
per-row loop is noexcept nogil.

Engine state types:
  - _object_state_starts  / _multi_object_state_starts  : vector[int32_t]
  - _object_state_lengths / _multi_object_state_lengths : vector[int32_t]
  - _seen                 / _multi_seen                 : vector[int64_t]

All pointer arguments must remain valid for the duration of the call (no
reallocation between pre-allocate and the call site).
"""

from libc.stdint cimport int32_t, int64_t, uint8_t
from libc.stddef cimport size_t
from libc.string cimport memcpy

from opteryx.compiled.aggregations.kernels.utils cimport _bitmap_is_valid


# ---------------------------------------------------------------------------
# Single-aggregate path
# ---------------------------------------------------------------------------

cdef void any_value_var_accumulate(
    uint8_t*          state_bytes,
    int32_t*          state_starts,
    int32_t*          state_lengths,
    size_t*           arena_cursor,
    int64_t*          seen,
    const int64_t*    state_indices,
    const char**      values_data,
    const Py_ssize_t* values_lengths,
    const uint8_t*    value_nulls,
    Py_ssize_t        row_count,
) noexcept nogil:
    """
    Accumulate ANY_VALUE for variable-width string values (single-agg path).

    Keeps the first non-null value seen for each group state; all subsequent
    rows for an already-set state are silently ignored.

    Parameters
    ----------
    state_bytes
        Pre-allocated arena buffer.  New bytes are written starting at
        state_bytes[arena_cursor[0]], which is advanced after each write.
    state_starts / state_lengths
        Per-state offsets (int32) and byte lengths into state_bytes.
    arena_cursor
        Mutable pointer to the current write position in state_bytes.
        The caller must have reserved at least sum(values_lengths[i] for unseen
        states) bytes beyond the initial cursor value before calling.
    seen
        Per-state flag: 0 = no value stored yet, 1 = value is present.
    state_indices
        Group-state index for each row.
    values_data / values_lengths
        Per-row string pointers and lengths.  NULL pointer or length <= 0
        means the row should be skipped.
    value_nulls
        Row null bitmap; NULL means all rows are non-null.
    row_count
        Number of rows in this morsel batch.
    """
    cdef Py_ssize_t  i
    cdef int64_t     sidx
    cdef const char* src
    cdef Py_ssize_t  src_len

    for i in range(row_count):
        if not _bitmap_is_valid(value_nulls, i):
            continue

        src     = values_data[i]
        src_len = values_lengths[i]

        if src == NULL or src_len <= 0:
            continue

        sidx = state_indices[i]

        if seen[sidx] != 0:
            # State already has a value — first-wins, skip this row.
            continue

        # First non-null value for this state: write to arena.
        state_starts[sidx]  = <int32_t> arena_cursor[0]
        state_lengths[sidx] = <int32_t> src_len
        memcpy(state_bytes + arena_cursor[0], src, <size_t> src_len)
        arena_cursor[0] += <size_t> src_len
        seen[sidx] = 1


# ---------------------------------------------------------------------------
# Multi-aggregate path
# ---------------------------------------------------------------------------

cdef void any_value_var_multi_accumulate(
    uint8_t*          state_bytes,
    int32_t*          state_starts,
    int32_t*          state_lengths,
    size_t*           arena_cursor,
    int64_t*          seen,
    const int64_t*    state_indices,
    const char**      values_data,
    const Py_ssize_t* values_lengths,
    const uint8_t*    value_nulls,
    Py_ssize_t        row_count,
    Py_ssize_t        multi_agg_count,
    Py_ssize_t        agg_idx,
) noexcept nogil:
    """
    Accumulate ANY_VALUE for variable-width string values (multi-agg path).

    The flat slot for (state, agg_idx) is:
        offset = state_index * multi_agg_count + agg_idx

    All pointer arguments carry the same semantics as the single-agg variant.
    """
    cdef Py_ssize_t  i
    cdef Py_ssize_t  offset
    cdef const char* src
    cdef Py_ssize_t  src_len

    for i in range(row_count):
        if not _bitmap_is_valid(value_nulls, i):
            continue

        src     = values_data[i]
        src_len = values_lengths[i]

        if src == NULL or src_len <= 0:
            continue

        offset = state_indices[i] * multi_agg_count + agg_idx

        if seen[offset] != 0:
            continue

        state_starts[offset]  = <int32_t> arena_cursor[0]
        state_lengths[offset] = <int32_t> src_len
        memcpy(state_bytes + arena_cursor[0], src, <size_t> src_len)
        arena_cursor[0] += <size_t> src_len
        seen[offset] = 1
