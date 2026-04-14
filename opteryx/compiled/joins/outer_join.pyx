# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False

from libc.stdint cimport uint64_t, int64_t

from opteryx.compiled.draken.vectors.int64_vector cimport Int64Vector
from opteryx.compiled.structures.hash_table cimport HashTable
from opteryx.compiled.draken.morsels.morsel cimport Morsel
from opteryx.compiled.morsel_ops.null_filter cimport non_null_row_indices
from opteryx.utils.arrow import align_tables as _align_tables_arrow

cpdef HashTable probe_side_hash_map(object relation, list join_columns):
    """
    Build a hash table for the join operations (probe-side) using Morsel hashing.
    """
    cdef HashTable ht = HashTable()
    cdef Int64Vector non_null_indices_vec
    cdef const int64_t* non_null_ptr
    cdef Py_ssize_t n_non_null
    cdef Py_ssize_t i
    cdef Morsel morsel = Morsel.from_arrow(relation)
    cdef uint64_t[::1] row_hashes = morsel.hash(join_columns)

    non_null_indices_vec = non_null_row_indices(relation, join_columns)
    non_null_ptr = <const int64_t*>non_null_indices_vec.dense_ptr()
    n_non_null = len(non_null_indices_vec)

    # Insert into HashTable using row index + Draken-computed hash
    for i in range(n_non_null):
        ht.insert(row_hashes[non_null_ptr[i]], non_null_ptr[i])

    return ht


def right_join(
    left_relation,
    right_relation,
    left_columns: list,
    right_columns: list,
    left_hash,
    filter_index,
    columns=None
):
    """
    Perform a RIGHT JOIN.

    This implementation ensures that all rows from the right table are included in the result set,
    with rows from the left table matched where possible, and columns from the left table
    filled with NULLs where no match is found.

    Parameters:
        left_relation (pyarrow.Table): The left pyarrow.Table to join.
        right_relation (pyarrow.Table): The right pyarrow.Table to join.
        left_columns (list of str): Column names from the left table to join on.
        right_columns (list of str): Column names from the right table to join on.
        columns (list of str, optional): Columns to include in the result. If None, all columns are included.

    Yields:
        pyarrow.Table: A chunk of the result of the RIGHT JOIN operation.
    """
    # Build hash table of left side
    cdef HashTable left_hash_table = HashTable()
    cdef Py_ssize_t num_left_rows = left_relation.num_rows
    cdef Py_ssize_t i
    cdef Morsel left_morsel = Morsel.from_arrow(left_relation)
    cdef uint64_t[::1] left_hashes = left_morsel.hash(left_columns)

    for i in range(num_left_rows):
        left_hash_table.insert(left_hashes[i], i)

    cdef uint64_t[::1] right_hashes
    cdef Morsel right_morsel

    # Iterate over the right_relation in chunks
    for right_chunk in right_relation.to_batches(50_000):
        chunk_size = right_chunk.num_rows

        # Compute hashes for this right chunk using Draken
        right_morsel = Morsel.from_arrow(right_chunk)
        right_hashes = right_morsel.hash(right_columns)

        # Collect matches
        left_indexes = []
        right_indexes = []

        for i in range(chunk_size):
            left_matches = left_hash_table.get(right_hashes[i])
            if left_matches.size() > 0:
                left_indexes.extend(left_matches)
                right_indexes.extend([i] * len(left_matches))
            else:
                left_indexes.append(None)
                right_indexes.append(i)

        yield _align_tables_arrow(left_relation, right_chunk, left_indexes, right_indexes)
