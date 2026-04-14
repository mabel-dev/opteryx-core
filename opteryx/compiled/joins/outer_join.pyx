# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False

"""
Outer join helper functions.

This module intentionally defines only the probe-side helper to avoid
signature collisions with the existing build-side helper exported from
`inner_join.pyx` when the joins package is consolidated by the build.
"""

from libc.stdint cimport uint64_t, int64_t

from opteryx.compiled.draken.vectors.int64_vector cimport Int64Vector
from opteryx.compiled.structures.hash_table cimport HashTable
from opteryx.compiled.draken.morsels.morsel cimport Morsel
from opteryx.compiled.morsel_ops.null_filter cimport non_null_row_indices


cpdef HashTable probe_side_hash_map(object relation, list join_columns):
    """
    Build a hash table for the join operations (probe-side) using Morsel hashing.

    Accepts either Arrow table or Morsel and returns a HashTable.
    """
    cdef HashTable ht = HashTable()
    cdef Int64Vector non_null_indices_vec
    cdef const int64_t* non_null_ptr
    cdef Py_ssize_t n_non_null
    cdef Py_ssize_t i
    cdef Morsel morsel
    cdef uint64_t[::1] row_hashes

    if isinstance(relation, Morsel):
        morsel = relation
    else:
        morsel = Morsel.from_arrow(relation)

    row_hashes = morsel.hash(join_columns)

    non_null_indices_vec = non_null_row_indices(relation, join_columns)
    non_null_ptr = <const int64_t*>non_null_indices_vec.dense_ptr()
    n_non_null = len(non_null_indices_vec)

    for i in range(n_non_null):
        ht.insert(row_hashes[non_null_ptr[i]], non_null_ptr[i])

    return ht
