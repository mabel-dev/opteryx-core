# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False

from array import array

from libc.stdint cimport int64_t, uint64_t
from libc.stddef cimport size_t
from libc.stdlib cimport malloc, free
from libcpp.vector cimport vector

from time import perf_counter_ns
cimport cython

from opteryx.third_party.abseil.containers cimport (
    FlatHashMap,
    IdentityHash,
    flat_hash_map,
)
from opteryx.compiled.draken.vectors.int64_vector cimport Int64Vector
from opteryx.compiled.structures.buffers cimport CIntBuffer, IntBuffer, Int32Buffer
from opteryx.compiled.draken.morsels.morsel cimport Morsel
from opteryx.compiled.morsel_ops.null_filter cimport non_null_row_indices

cdef extern from "join_kernels.h":
    void inner_join_probe(
        flat_hash_map[uint64_t, vector[int64_t], IdentityHash]* left_map,
        const int64_t* non_null_indices,
        size_t non_null_count,
        const uint64_t* row_hashes,
        size_t row_hash_count,
        CIntBuffer* left_out,
        CIntBuffer* right_out
    ) nogil

cdef public long long last_hash_time_ns = 0
cdef public long long last_probe_time_ns = 0
cdef public long long last_materialize_time_ns = 0
cdef public Py_ssize_t last_rows_hashed = 0
cdef public Py_ssize_t last_candidate_rows = 0
cdef public Py_ssize_t last_result_rows = 0


cpdef tuple inner_join(object right_relation, list join_columns, FlatHashMap left_hash_table):
    """
    Perform an inner join between a right-hand relation and a pre-built left-side hash table.
    This function uses precomputed hashes and avoids null rows for optimal speed.
    """
    global last_hash_time_ns, last_probe_time_ns, last_materialize_time_ns
    global last_rows_hashed, last_candidate_rows, last_result_rows
    cdef IntBuffer left_indexes = IntBuffer()
    cdef IntBuffer right_indexes = IntBuffer()
    cdef int64_t num_rows = right_relation.num_rows
    cdef Int64Vector non_null_indices_vec = non_null_row_indices(right_relation, join_columns)
    cdef const int64_t* non_null_ptr = <const int64_t*>non_null_indices_vec.dense_ptr()
    cdef Py_ssize_t candidate_count = len(non_null_indices_vec)

    if candidate_count == 0 or num_rows == 0:
        last_hash_time_ns = 0
        last_probe_time_ns = 0
        last_rows_hashed = num_rows
        last_candidate_rows = candidate_count
        last_result_rows = 0
        last_materialize_time_ns = 0
        return left_indexes.to_int32_buffer(), right_indexes.to_int32_buffer()

    cdef uint64_t[::1] row_hashes
    cdef long long t_start = perf_counter_ns()
    cdef Morsel _m

    # Prefer Draken Morsel.hash() for per-row hashing.
    if isinstance(right_relation, Morsel):
        row_hashes = right_relation.hash(join_columns)
    else:
        _m = Morsel.from_arrow(right_relation)
        row_hashes = _m.hash(join_columns)

    cdef long long t_after_hash = perf_counter_ns()
    last_hash_time_ns = t_after_hash - t_start

    # Probe using precomputed hashes (nogil)
    with nogil:
        with cython.boundscheck(False):
            inner_join_probe(
                &left_hash_table._map,
                non_null_ptr,
                <size_t>candidate_count,
                &row_hashes[0],
                <size_t>num_rows,
                left_indexes.c_buffer,
                right_indexes.c_buffer,
            )

    cdef long long t_after_probe = perf_counter_ns()
    last_probe_time_ns = t_after_probe - t_after_hash
    last_rows_hashed = num_rows
    last_candidate_rows = candidate_count

    # Return matched row indices from both sides
    cdef long long t_before_numpy = perf_counter_ns()
    cdef Int32Buffer left_int32 = left_indexes.to_int32_buffer()
    cdef Int32Buffer right_int32 = right_indexes.to_int32_buffer()
    cdef long long t_after_numpy = perf_counter_ns()
    last_result_rows = left_int32.size()
    last_materialize_time_ns = t_after_numpy - t_before_numpy

    return left_int32, right_int32


cpdef tuple get_last_inner_join_metrics():
    """Return instrumentation captured during the most recent inner join call."""
    return (
        last_hash_time_ns,
        last_probe_time_ns,
        last_rows_hashed,
        last_candidate_rows,
        last_result_rows,
        last_materialize_time_ns,
    )


cpdef FlatHashMap build_side_hash_map(object relation, list join_columns):
    """
    Builds a hash map from non-null rows of the given relation using the specified join columns.
    Used to support hash-based joins.
    """
    cdef FlatHashMap ht = FlatHashMap()
    cdef int64_t num_rows = relation.num_rows
    cdef Int64Vector non_null_indices_vec = non_null_row_indices(relation, join_columns)
    cdef const int64_t* non_null_ptr = <const int64_t*>non_null_indices_vec.dense_ptr()
    cdef Py_ssize_t n_non_null = len(non_null_indices_vec)

    cdef uint64_t[::1] row_hashes
    cdef int64_t i, row_idx
    cdef Morsel _m

    if isinstance(relation, Morsel):
        row_hashes = relation.hash(join_columns)
    else:
        _m = Morsel.from_arrow(relation)
        row_hashes = _m.hash(join_columns)

    for i in range(n_non_null):
        row_idx = non_null_ptr[i]
        ht.insert(row_hashes[row_idx], row_idx)

    return ht


cpdef object build_side_carchar_map(
    object relation,
    list join_columns,
    double probe_load_factor=0.35,
):
    """
    Build a Carchar-backed map suitable for carchar-based join probing.
    """
    cdef object carchar_native
    cdef object ht
    cdef int64_t num_rows = relation.num_rows
    cdef Int64Vector non_null_indices_vec = non_null_row_indices(relation, join_columns)
    cdef const int64_t* non_null_ptr = <const int64_t*>non_null_indices_vec.dense_ptr()
    cdef Py_ssize_t n_non_null = len(non_null_indices_vec)

    cdef uint64_t[::1] row_hashes
    cdef int64_t i, row_idx
    cdef Morsel _m

    if isinstance(relation, Morsel):
        row_hashes = relation.hash(join_columns)
    else:
        _m = Morsel.from_arrow(relation)
        row_hashes = _m.hash(join_columns)

    # Prepare buffers for carchar native insert_batch
    cdef int64_t* indices_buf = <int64_t*>malloc(n_non_null * sizeof(int64_t))
    cdef uint64_t* hashes_buf = <uint64_t*>malloc(n_non_null * sizeof(uint64_t))

    if indices_buf == NULL or hashes_buf == NULL:
        if indices_buf != NULL:
            free(indices_buf)
        if hashes_buf != NULL:
            free(hashes_buf)
        raise MemoryError("Failed to allocate memory for index/hash buffers")

    cdef int64_t[::1] indices_view = <int64_t[:n_non_null]>indices_buf
    cdef uint64_t[::1] hashes_view = <uint64_t[:n_non_null]>hashes_buf

    for i in range(n_non_null):
        row_idx = non_null_ptr[i]
        indices_view[i] = row_idx
        hashes_view[i] = row_hashes[row_idx]

    try:
        import opteryx.compiled.nanobind.carchar_native as carchar_native
        ht = carchar_native.CarcharJoinEngine(
            int(n_non_null),
            0,
            0.80,
            probe_load_factor,
        )
        ht.insert_batch(hashes_view, indices_view)
        ht.seal()
        return ht
    finally:
        free(indices_buf)
        free(hashes_buf)


cpdef tuple inner_join_carchar(object right_relation, list join_columns, object left_hash_table):
    """
    Inner join specialized for carchar-backed left-side structures.
    """
    global last_hash_time_ns, last_probe_time_ns, last_materialize_time_ns
    global last_rows_hashed, last_candidate_rows, last_result_rows
    cdef int64_t num_rows = right_relation.num_rows
    cdef Int64Vector non_null_indices_vec = non_null_row_indices(right_relation, join_columns)
    cdef const int64_t* non_null_ptr = <const int64_t*>non_null_indices_vec.dense_ptr()
    cdef Py_ssize_t candidate_count = len(non_null_indices_vec)
    cdef IntBuffer left_indexes = IntBuffer()
    cdef IntBuffer right_indexes = IntBuffer()

    if candidate_count == 0 or num_rows == 0:
        last_hash_time_ns = 0
        last_probe_time_ns = 0
        last_rows_hashed = num_rows
        last_candidate_rows = candidate_count
        last_result_rows = 0
        last_materialize_time_ns = 0
        return left_indexes.to_int32_buffer(), right_indexes.to_int32_buffer()

    cdef uint64_t[::1] row_hashes
    cdef long long t_start = perf_counter_ns()
    cdef Morsel _m

    if isinstance(right_relation, Morsel):
        row_hashes = right_relation.hash(join_columns)
    else:
        _m = Morsel.from_arrow(right_relation)
        row_hashes = _m.hash(join_columns)

    cdef long long t_after_hash = perf_counter_ns()
    last_hash_time_ns = t_after_hash - t_start

    # Prepare probe buffers for candidate rows
    cdef int64_t* probe_rows_buf = <int64_t*>malloc(candidate_count * sizeof(int64_t))
    cdef uint64_t* probe_hashes_buf = <uint64_t*>malloc(candidate_count * sizeof(uint64_t))

    if probe_rows_buf == NULL or probe_hashes_buf == NULL:
        if probe_rows_buf != NULL:
            free(probe_rows_buf)
        if probe_hashes_buf != NULL:
            free(probe_hashes_buf)
        raise MemoryError("Failed to allocate memory for probe buffers")

    cdef int64_t[::1] probe_rows_view = <int64_t[:candidate_count]>probe_rows_buf
    cdef uint64_t[::1] probe_hashes_view = <uint64_t[:candidate_count]>probe_hashes_buf
    cdef Py_ssize_t i

    for i in range(candidate_count):
        probe_rows_view[i] = non_null_ptr[i]
        probe_hashes_view[i] = row_hashes[non_null_ptr[i]]

    cdef long long t_before_probe = perf_counter_ns()
    # left_hash_table is expected to be a carchar_native engine-like object
    result_left, result_right = left_hash_table.probe_join_indices(probe_hashes_view, probe_rows_view)
    cdef long long t_after_probe = perf_counter_ns()

    free(probe_rows_buf)
    free(probe_hashes_buf)

    last_probe_time_ns = t_after_probe - t_before_probe
    last_rows_hashed = num_rows
    last_candidate_rows = candidate_count
    last_result_rows = len(result_left) if result_left is not None else 0
    last_materialize_time_ns = 0

    return result_left, result_right
