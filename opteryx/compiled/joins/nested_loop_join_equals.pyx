# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False

from libc.stdint cimport int64_t, uint64_t
from libc.stdlib cimport malloc, free

from opteryx.compiled.draken.vectors.int64_vector cimport Int64Vector
from opteryx.compiled.structures.buffers cimport IntBuffer, Int32Buffer
from opteryx.compiled.table_ops.hash_ops cimport compute_row_hashes
from opteryx.compiled.table_ops.null_avoidant_ops cimport non_null_row_indices

cpdef tuple nested_loop_join(left_relation, right_relation, list left_columns, list right_columns):
    """
    Perform a buffer-aware nested loop join using Arrow buffer hashing.

    This implementation is optimized for small relations where building a hash map would be
    more expensive than a quadratic scan.
    """
    cdef Int64Vector left_non_null_indices_vec = non_null_row_indices(left_relation, left_columns)
    cdef Int64Vector right_non_null_indices_vec = non_null_row_indices(right_relation, right_columns)

    cdef const int64_t* left_non_null_ptr = <const int64_t*>left_non_null_indices_vec.dense_ptr()
    cdef const int64_t* right_non_null_ptr = <const int64_t*>right_non_null_indices_vec.dense_ptr()

    cdef Py_ssize_t nl = len(left_non_null_indices_vec)
    cdef Py_ssize_t nr = len(right_non_null_indices_vec)

    cdef IntBuffer left_indexes = IntBuffer()
    cdef IntBuffer right_indexes = IntBuffer()

    if nl == 0 or nr == 0:
        return left_indexes.to_int32_buffer(), right_indexes.to_int32_buffer()

    cdef uint64_t* left_raw_hashes = <uint64_t*>malloc(left_relation.num_rows * sizeof(uint64_t))
    cdef uint64_t* right_raw_hashes = <uint64_t*>malloc(right_relation.num_rows * sizeof(uint64_t))
    if left_raw_hashes == NULL or right_raw_hashes == NULL:
        if left_raw_hashes != NULL:
            free(left_raw_hashes)
        if right_raw_hashes != NULL:
            free(right_raw_hashes)
        raise MemoryError("Failed to allocate memory for hash buffers")

    cdef uint64_t[::1] left_hashes = <uint64_t[:left_relation.num_rows]>left_raw_hashes
    cdef uint64_t[::1] right_hashes = <uint64_t[:right_relation.num_rows]>right_raw_hashes
    cdef int64_t i, j, left_row, right_row
    cdef uint64_t left_hash, right_hash

    compute_row_hashes(left_relation, left_columns, left_hashes)
    compute_row_hashes(right_relation, right_columns, right_hashes)

    if nl <= nr:
        for i in range(nl):
            left_row = left_non_null_ptr[i]
            left_hash = left_hashes[left_row]
            for j in range(nr):
                right_row = right_non_null_ptr[j]
                if left_hash == right_hashes[right_row]:
                    left_indexes.append(left_row)
                    right_indexes.append(right_row)
    else:
        for j in range(nr):
            right_row = right_non_null_ptr[j]
            right_hash = right_hashes[right_row]
            for i in range(nl):
                left_row = left_non_null_ptr[i]
                if right_hash == left_hashes[left_row]:
                    left_indexes.append(left_row)
                    right_indexes.append(right_row)

    free(left_raw_hashes)
    free(right_raw_hashes)

    return left_indexes.to_int32_buffer(), right_indexes.to_int32_buffer()
