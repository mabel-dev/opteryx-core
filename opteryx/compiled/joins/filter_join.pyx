# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False


from libc.stdint cimport int64_t, uint64_t
from libc.stdlib cimport malloc, free
from opteryx.compiled.table_ops.hash_ops cimport compute_row_hashes
from opteryx.compiled.structures.carchar_set cimport CarcharSetWrapper
from opteryx.compiled.structures.buffers cimport IntBuffer, Int32Buffer


cpdef CarcharSetWrapper filter_join_set(table, list columns=None, CarcharSetWrapper seen_hashes=None):
    cdef:
        Py_ssize_t num_rows = table.num_rows
        uint64_t* raw_hashes = <uint64_t*>malloc(num_rows * sizeof(uint64_t))
        list columns_of_interest = columns if columns else table.column_names
        Py_ssize_t row_idx

    if raw_hashes == NULL:
        raise MemoryError("Failed to allocate memory for hash buffers")

    cdef uint64_t[::1] row_hashes = <uint64_t[:num_rows]>raw_hashes

    compute_row_hashes(table, columns_of_interest, row_hashes)

    if seen_hashes is None:
        seen_hashes = CarcharSetWrapper()

    for row_idx in range(num_rows):
        seen_hashes.insert(row_hashes[row_idx])

    free(raw_hashes)
    return seen_hashes


cpdef semi_join(object relation, list join_columns, CarcharSetWrapper seen_hashes):
    cdef:
        Py_ssize_t num_rows = relation.num_rows
        Py_ssize_t row_idx
        Py_ssize_t count = 0
        uint64_t* raw_hashes = <uint64_t*>malloc(num_rows * sizeof(uint64_t))
        IntBuffer index_buffer = IntBuffer(num_rows)

    if raw_hashes == NULL:
        raise MemoryError("Failed to allocate memory for hash buffers")

    cdef uint64_t[::1] row_hashes = <uint64_t[:num_rows]>raw_hashes

    compute_row_hashes(relation, join_columns, row_hashes)

    for row_idx in range(num_rows):
        if seen_hashes.contains(row_hashes[row_idx]):
            index_buffer.append(row_idx)

    free(raw_hashes)

    if index_buffer.size() > 0:
        return relation.take(index_buffer.to_int32_buffer())
    else:
        return relation.slice(0, 0)


cpdef anti_join(object relation, list join_columns, CarcharSetWrapper seen_hashes):
    cdef:
        Py_ssize_t num_rows = relation.num_rows
        Py_ssize_t row_idx
        uint64_t* raw_hashes = <uint64_t*>malloc(num_rows * sizeof(uint64_t))
        IntBuffer index_buffer = IntBuffer(num_rows)

    if raw_hashes == NULL:
        raise MemoryError("Failed to allocate memory for hash buffers")

    cdef uint64_t[::1] row_hashes = <uint64_t[:num_rows]>raw_hashes

    compute_row_hashes(relation, join_columns, row_hashes)

    for row_idx in range(num_rows):
        if not seen_hashes.contains(row_hashes[row_idx]):
            index_buffer.append(row_idx)

    free(raw_hashes)

    if index_buffer.size() > 0:
        return relation.take(index_buffer.to_int32_buffer())
    else:
        return relation.slice(0, 0)
