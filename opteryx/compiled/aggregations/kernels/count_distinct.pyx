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

CarcharSetWrapper is a Cython extension type whose _ptr field exposes the
underlying CarcharSet* directly.  The pointer-extraction phase (with GIL)
runs once per morsel; the per-row accumulation loop runs entirely without
the GIL via a noexcept C++ wrapper around CarcharSet::insert_or_ignore.
"""

from libc.stdint cimport int64_t, uint8_t, uint64_t
from libc.stdlib cimport malloc, free

from opteryx.compiled.structures.carchar_set cimport CarcharSet, CarcharSetWrapper
from opteryx.compiled.aggregations.kernels.utils cimport _bitmap_is_valid


# ---------------------------------------------------------------------------
# noexcept C++ wrapper
#
# CarcharSet::insert_or_ignore can trigger a resize that throws std::bad_alloc.
# We wrap it noexcept here (matching the prior implementation) so the hot
# accumulation loop can run without the GIL.  OOM will call std::terminate.
# ---------------------------------------------------------------------------
cdef extern from *:
    """
    #include "carchar_set.hpp"
    namespace opteryx_cd {
        static inline bool carchar_insert_new(

            opteryx::carchar::CarcharSet* s, uint64_t v
        ) noexcept {
            return s->insert_or_ignore(v);
        }
    }
    """
    bint _carchar_insert_new "opteryx_cd::carchar_insert_new"(
        CarcharSet* s,
        uint64_t v,
    ) noexcept nogil


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

    distinct_sets   Python list[CarcharSetWrapper], one entry per group state.
                    len(distinct_sets) == number of group states allocated so far.
    counts          Per-state int64_t counter array (self._counts.data()).
    value_hashes    uint64_t hash / bit-cast for every row of the value column.
    value_nulls     Null bitmap for the value column; NULL means all rows are non-null.
    state_indices   Group-state index for each row.
    row_count       Number of rows in this morsel batch.

    Pointer-extraction phase (with GIL): resolve each CarcharSetWrapper Python
    object to its underlying CarcharSet*.  Once sets_ptr is populated the
    per-row accumulation loop runs without the GIL.
    """
    cdef Py_ssize_t n_states = len(distinct_sets)
    cdef CarcharSet** sets_ptr = NULL
    cdef Py_ssize_t i
    cdef Py_ssize_t row_idx
    cdef int64_t sidx

    if n_states == 0 or row_count == 0:
        return

    sets_ptr = <CarcharSet**> malloc(n_states * sizeof(void*))
    if sets_ptr == NULL:
        raise MemoryError("count_distinct_accumulate: cannot allocate sets_ptr")

    try:
        # Resolve each CarcharSetWrapper Python object to its raw CarcharSet*.
        # This is the only section that touches Python objects; once sets_ptr
        # is populated the per-row loop runs without the GIL.
        for i in range(n_states):
            sets_ptr[i] = (<CarcharSetWrapper> distinct_sets[i])._ptr

        with nogil:
            for row_idx in range(row_count):
                if _bitmap_is_valid(value_nulls, row_idx):
                    sidx = state_indices[row_idx]
                    if _carchar_insert_new(sets_ptr[sidx], value_hashes[row_idx]):
                        counts[sidx] = counts[sidx] + 1
    finally:
        free(sets_ptr)


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
                    (state * multi_agg_count + agg_idx) hold CarcharSetWrapper
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
    cdef CarcharSet** sets_ptr = NULL
    cdef Py_ssize_t i
    cdef Py_ssize_t row_idx
    cdef int64_t sidx
    cdef Py_ssize_t offset

    if multi_agg_count == 0 or row_count == 0:
        return

    n_states = len(distinct_sets) // multi_agg_count
    if n_states == 0:
        return

    sets_ptr = <CarcharSet**> malloc(n_states * sizeof(void*))
    if sets_ptr == NULL:
        raise MemoryError("count_distinct_multi_accumulate: cannot allocate sets_ptr")

    try:
        # Resolve only the agg_idx slot for each state — skipping the None
        # entries that belong to other aggregate modes.
        for i in range(n_states):
            sets_ptr[i] = (<CarcharSetWrapper> distinct_sets[i * multi_agg_count + agg_idx])._ptr

        with nogil:
            for row_idx in range(row_count):
                if _bitmap_is_valid(value_nulls, row_idx):
                    sidx = state_indices[row_idx]
                    offset = sidx * multi_agg_count + agg_idx
                    if _carchar_insert_new(sets_ptr[sidx], value_hashes[row_idx]):
                        multi_counts[offset] = multi_counts[offset] + 1
    finally:
        free(sets_ptr)
