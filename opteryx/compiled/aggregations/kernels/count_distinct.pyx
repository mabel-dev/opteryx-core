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

FlatHashSet is a Python extension type, so the Python list cannot be passed
directly into a nogil loop.  Instead, each function extracts the address of
the underlying C++ flat_hash_set from every FlatHashSet slot before the
per-row loop, stores those raw C++ pointers in a malloc'd array, then runs
the per-row loop without the GIL.

An inline C++ helper (_fhs_insert_new) wraps flat_hash_set::insert() so
that we can test the "was-new" bool return value without needing to
materialise the pair<iterator,bool> in Cython.
"""

from libc.stdint cimport int64_t, uint8_t, uint64_t
from libc.stdlib cimport malloc, free

from opteryx.third_party.abseil.containers cimport FlatHashSet
from opteryx.third_party.abseil.containers cimport flat_hash_set, IdentityHash

from opteryx.compiled.aggregations.kernels.utils cimport _bitmap_is_valid


# ---------------------------------------------------------------------------
# Inline C++ helper
#
# flat_hash_set::insert returns pair<iterator, bool>; the bool indicates
# whether the element was newly inserted.  We wrap it here so the nogil
# per-row loop can test the result without materialising the pair in Cython.
# ---------------------------------------------------------------------------
cdef extern from *:
    """
    namespace opteryx_cd {
        static inline bool fhs_insert_new(
            absl::flat_hash_set<uint64_t, IdentityHash>& s,
            uint64_t v
        ) noexcept {
            return s.insert(v).second;
        }
    }
    """
    bint _fhs_insert_new "opteryx_cd::fhs_insert_new"(
        flat_hash_set[uint64_t, IdentityHash]& s,
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

    distinct_sets   Python list[FlatHashSet], one entry per group state.
                    len(distinct_sets) == number of group states allocated so far.
    counts          Per-state int64_t counter array (self._counts.data()).
    value_hashes    uint64_t hash / bit-cast for every row of the value column.
    value_nulls     Null bitmap for the value column; NULL means all rows are non-null.
    state_indices   Group-state index for each row.
    row_count       Number of rows in this morsel batch.

    For each non-null row i the hash value_hashes[i] is inserted into the
    FlatHashSet for group state state_indices[i].  counts[state] is
    incremented only when the value is new (not already seen for that state).
    """
    cdef Py_ssize_t n_states = len(distinct_sets)
    cdef flat_hash_set[uint64_t, IdentityHash]** sets_ptr = NULL
    cdef Py_ssize_t i
    cdef Py_ssize_t row_idx
    cdef int64_t sidx

    if n_states == 0 or row_count == 0:
        return

    sets_ptr = <flat_hash_set[uint64_t, IdentityHash]**> malloc(
        n_states * sizeof(void*)
    )
    if sets_ptr == NULL:
        raise MemoryError("count_distinct_accumulate: cannot allocate sets_ptr")

    try:
        # Resolve each FlatHashSet Python object to its underlying C++ set pointer.
        # This is the only section that touches Python objects; once sets_ptr is
        # populated the per-row loop can run without the GIL.
        for i in range(n_states):
            sets_ptr[i] = &((<FlatHashSet> distinct_sets[i])._set)

        with nogil:
            for row_idx in range(row_count):
                if _bitmap_is_valid(value_nulls, row_idx):
                    sidx = state_indices[row_idx]
                    if _fhs_insert_new(sets_ptr[sidx][0], value_hashes[row_idx]):
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
                    (state * multi_agg_count + agg_idx) hold FlatHashSet
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
    cdef flat_hash_set[uint64_t, IdentityHash]** sets_ptr = NULL
    cdef Py_ssize_t i
    cdef Py_ssize_t row_idx
    cdef int64_t sidx
    cdef Py_ssize_t offset

    if multi_agg_count == 0 or row_count == 0:
        return

    n_states = len(distinct_sets) // multi_agg_count
    if n_states == 0:
        return

    sets_ptr = <flat_hash_set[uint64_t, IdentityHash]**> malloc(
        n_states * sizeof(void*)
    )
    if sets_ptr == NULL:
        raise MemoryError("count_distinct_multi_accumulate: cannot allocate sets_ptr")

    try:
        # Resolve only the agg_idx slot for each state — skipping the None
        # entries that belong to other aggregate modes.
        for i in range(n_states):
            sets_ptr[i] = &(
                (<FlatHashSet> distinct_sets[i * multi_agg_count + agg_idx])._set
            )

        with nogil:
            for row_idx in range(row_count):
                if _bitmap_is_valid(value_nulls, row_idx):
                    sidx = state_indices[row_idx]
                    offset = sidx * multi_agg_count + agg_idx
                    if _fhs_insert_new(sets_ptr[sidx][0], value_hashes[row_idx]):
                        multi_counts[offset] = multi_counts[offset] + 1
    finally:
        free(sets_ptr)
