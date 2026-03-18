# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False

from array import array

from cpython.mem cimport PyMem_Malloc, PyMem_Free

from libc.math cimport isnan
from libc.stddef cimport size_t
from libc.stdint cimport int32_t, int64_t, uint32_t, uint64_t
from libcpp.utility cimport pair
from libcpp.vector cimport vector

from time import perf_counter_ns

from opteryx.compiled.structures.bloom_filter cimport BloomFilter
from opteryx.draken.morsels.align cimport align_tables
from opteryx.draken.morsels.morsel cimport Morsel
from opteryx.draken.vectors.bool_vector cimport BoolVector
from opteryx.draken.vectors.date32_vector cimport Date32Vector
from opteryx.draken.vectors.float64_vector cimport Float64Vector
from opteryx.draken.vectors.int64_vector cimport Int64Vector
from opteryx.draken.vectors.integer_vector cimport IntegerVector
from opteryx.draken.vectors.string_vector cimport StringVector
from opteryx.draken.vectors.string_vector cimport _StringVectorView
from opteryx.draken.vectors.time_vector cimport TimeVector
from opteryx.draken.vectors.timestamp_vector cimport TimestampVector
from opteryx.compiled.structures.bloom_filter import BloomFilter as PyBloomFilter


cdef extern from "carchar.hpp" namespace "opteryx::carchar":
    cdef cppclass CarcharJoinEngine:
        CarcharJoinEngine(
            size_t expected_entries,
            size_t partition_bits,
            double load_factor,
            double probe_load_factor
        ) except +
        void insert_batch(const uint64_t* keys, const int64_t* row_ids, size_t length) except +
        void seal() except +
        pair[vector[int64_t], vector[int64_t]] probe_join_indices(
            const uint64_t* keys,
            const int64_t* probe_rows,
            size_t length
        ) except +


cdef long long NULL_INT64_SENTINEL = -9223372036854775808

cdef public long long last_draken_inner_join_hash_time_ns = 0
cdef public long long last_draken_inner_join_probe_time_ns = 0
cdef public long long last_draken_inner_join_materialize_time_ns = 0
cdef public long long last_draken_inner_join_align_time_ns = 0
cdef public long long last_draken_inner_join_build_bloom_time_ns = 0
cdef public long long last_draken_inner_join_bloom_filter_time_ns = 0
cdef public Py_ssize_t last_draken_inner_join_rows_hashed = 0
cdef public Py_ssize_t last_draken_inner_join_candidate_rows = 0
cdef public Py_ssize_t last_draken_inner_join_result_rows = 0
cdef public Py_ssize_t last_draken_inner_join_rows_eliminated_by_bloom_filter = 0


cdef class DrakenCarcharJoinMap:
    cdef CarcharJoinEngine* engine
    cdef object bloom_filter

    def __cinit__(self, Py_ssize_t expected_entries=0, double probe_load_factor=0.35):
        self.engine = new CarcharJoinEngine(
            <size_t>max(0, expected_entries),
            <size_t>0,
            <double>0.80,
            probe_load_factor,
        )
        self.bloom_filter = None

    def __dealloc__(self):
        if self.engine is not NULL:
            del self.engine
            self.engine = NULL

    cpdef void seal(self):
        self.engine.seal()

    cpdef bint has_bloom_filter(self):
        return self.bloom_filter is not None


cdef inline bytes _column_name_bytes(object column_name):
    if isinstance(column_name, bytes):
        return column_name
    return str(column_name).encode("utf8")


cdef inline bint _row_valid_from_null_mask(object null_mask, Py_ssize_t row_index):
    return null_mask[row_index] == 0


cdef inline bint _float_row_valid(Float64Vector float_vector, object null_mask, Py_ssize_t row_index):
    if null_mask is not None and null_mask[row_index] != 0:
        return False
    return not isnan((<double*> float_vector.ptr.data)[row_index])


cdef inline bint _row_valid_generic(object row_values, Py_ssize_t row_index):
    cdef object value = row_values[row_index]
    if value is None:
        return False
    if isinstance(value, float) and value != value:
        return False
    return True


cdef inline void _append_valid_rows_and_hashes(
    Morsel relation,
    list join_columns,
    uint64_t[::1] row_hashes,
    vector[uint64_t]& valid_hashes,
    vector[int64_t]& valid_rows,
):
    cdef Py_ssize_t num_rows = relation.num_rows
    cdef list vectors = []
    cdef list null_masks = []
    cdef list string_views = []
    cdef list kinds = []
    cdef object column_name
    cdef object vector_obj
    cdef Py_ssize_t row_index
    cdef Py_ssize_t column_index
    cdef int kind
    cdef bint valid

    valid_hashes.reserve(num_rows)
    valid_rows.reserve(num_rows)

    for column_name in join_columns:
        vector_obj = relation.column(_column_name_bytes(column_name))
        vectors.append(vector_obj)
        null_masks.append(None)
        string_views.append(None)

        if isinstance(vector_obj, Float64Vector):
            kinds.append(1)
            null_masks[-1] = (<Float64Vector> vector_obj).is_null()
        elif isinstance(vector_obj, StringVector):
            kinds.append(2)
            string_views[-1] = (<StringVector> vector_obj).view()
        elif isinstance(vector_obj, (
            Int64Vector,
            IntegerVector,
            BoolVector,
            Date32Vector,
            TimestampVector,
            TimeVector,
        )):
            kinds.append(3)
            null_masks[-1] = vector_obj.is_null()
        else:
            kinds.append(5)

    for row_index in range(num_rows):
        valid = True
        for column_index in range(len(vectors)):
            kind = kinds[column_index]
            if kind == 1:
                if not _float_row_valid(
                    <Float64Vector> vectors[column_index],
                    null_masks[column_index],
                    row_index,
                ):
                    valid = False
                    break
            elif kind == 2:
                if (<_StringVectorView> string_views[column_index]).is_null(row_index):
                    valid = False
                    break
            elif kind == 3:
                if not _row_valid_from_null_mask(
                    null_masks[column_index],
                    row_index,
                ):
                    valid = False
                    break
            else:
                if not _row_valid_generic(vectors[column_index], row_index):
                    valid = False
                    break

        if valid:
            valid_rows.push_back(<int64_t> row_index)
            valid_hashes.push_back(row_hashes[row_index])


cdef inline void _append_bloom_filtered_rows_and_hashes(
    Morsel relation,
    list join_columns,
    uint64_t[::1] row_hashes,
    BloomFilter bloom_filter,
    vector[uint64_t]& candidate_hashes,
    vector[int64_t]& candidate_rows,
):
    cdef Py_ssize_t num_rows = relation.num_rows
    cdef list vectors = []
    cdef list null_masks = []
    cdef list string_views = []
    cdef list kinds = []
    cdef object column_name
    cdef object vector_obj
    cdef Py_ssize_t row_index
    cdef Py_ssize_t column_index
    cdef int kind
    cdef bint valid
    cdef uint64_t hash_value

    candidate_hashes.reserve(num_rows)
    candidate_rows.reserve(num_rows)

    for column_name in join_columns:
        vector_obj = relation.column(_column_name_bytes(column_name))
        vectors.append(vector_obj)
        null_masks.append(None)
        string_views.append(None)

        if isinstance(vector_obj, Float64Vector):
            kinds.append(1)
            null_masks[-1] = (<Float64Vector> vector_obj).is_null()
        elif isinstance(vector_obj, StringVector):
            kinds.append(2)
            string_views[-1] = (<StringVector> vector_obj).view()
        elif isinstance(vector_obj, (
            Int64Vector,
            IntegerVector,
            BoolVector,
            Date32Vector,
            TimestampVector,
            TimeVector,
        )):
            kinds.append(3)
            null_masks[-1] = vector_obj.is_null()
        else:
            kinds.append(5)

    for row_index in range(num_rows):
        valid = True
        for column_index in range(len(vectors)):
            kind = kinds[column_index]
            if kind == 1:
                if not _float_row_valid(
                    <Float64Vector> vectors[column_index],
                    null_masks[column_index],
                    row_index,
                ):
                    valid = False
                    break
            elif kind == 2:
                if (<_StringVectorView> string_views[column_index]).is_null(row_index):
                    valid = False
                    break
            elif kind == 3:
                if not _row_valid_from_null_mask(
                    null_masks[column_index],
                    row_index,
                ):
                    valid = False
                    break
            else:
                if not _row_valid_generic(vectors[column_index], row_index):
                    valid = False
                    break

        if not valid:
            continue

        hash_value = row_hashes[row_index]
        if bloom_filter._possibly_contains(hash_value):
            candidate_rows.push_back(<int64_t> row_index)
            candidate_hashes.push_back(hash_value)


cdef inline BloomFilter _build_bloom_filter_from_hashes(const vector[uint64_t]& row_hashes):
    cdef size_t length = row_hashes.size()
    cdef BloomFilter bloom_filter
    cdef size_t i

    if length == 0 or length > <size_t>16_000_000:
        return None

    bloom_filter = <BloomFilter>PyBloomFilter(<uint32_t>length)
    for i in range(length):
        bloom_filter._add(row_hashes[i])
    return bloom_filter


cdef object _int32_array_from_vector(const vector[int64_t]& values):
    cdef Py_ssize_t length = <Py_ssize_t> values.size()
    cdef object out = array('i', [0]) * length
    cdef int32_t[::1] out_view = out
    cdef Py_ssize_t i

    for i in range(length):
        out_view[i] = <int32_t> values[i]

    return out


cdef inline int32_t[::1] _int32_view_from_vector(
    const vector[int64_t]& values,
    int32_t** buffer_out,
) except *:
    cdef Py_ssize_t length = <Py_ssize_t> values.size()
    cdef int32_t* out_ptr = NULL
    cdef Py_ssize_t i

    buffer_out[0] = NULL
    if length == 0:
        return <int32_t[:0]> NULL

    out_ptr = <int32_t*>PyMem_Malloc(length * sizeof(int32_t))
    if out_ptr == NULL:
        raise MemoryError()

    for i in range(length):
        out_ptr[i] = <int32_t> values[i]

    buffer_out[0] = out_ptr
    return <int32_t[:length]> out_ptr


cpdef DrakenCarcharJoinMap build_side_carchar_morsel_map(
    Morsel relation,
    list join_columns,
    double probe_load_factor=0.35,
):
    global last_draken_inner_join_build_bloom_time_ns
    cdef DrakenCarcharJoinMap ht
    cdef Py_ssize_t num_rows = relation.num_rows
    cdef uint64_t[::1] row_hashes
    cdef vector[uint64_t] valid_hashes
    cdef vector[int64_t] valid_rows
    cdef long long bloom_start

    ht = DrakenCarcharJoinMap(num_rows, probe_load_factor)
    last_draken_inner_join_build_bloom_time_ns = 0
    if num_rows == 0:
        ht.seal()
        return ht

    row_hashes = relation.hash(join_columns)
    _append_valid_rows_and_hashes(relation, join_columns, row_hashes, valid_hashes, valid_rows)

    if valid_rows.size() != 0:
        ht.engine.insert_batch(
            &valid_hashes[0],
            &valid_rows[0],
            <size_t> valid_rows.size(),
        )
        if valid_hashes.size() <= <size_t>16_000_000:
            bloom_start = perf_counter_ns()
            ht.bloom_filter = _build_bloom_filter_from_hashes(valid_hashes)
            last_draken_inner_join_build_bloom_time_ns = perf_counter_ns() - bloom_start

    ht.seal()
    return ht


cpdef tuple inner_join_carchar_morsel(
    Morsel right_relation,
    list join_columns,
    DrakenCarcharJoinMap left_hash_table,
):
    global last_draken_inner_join_hash_time_ns
    global last_draken_inner_join_probe_time_ns
    global last_draken_inner_join_materialize_time_ns
    global last_draken_inner_join_align_time_ns
    global last_draken_inner_join_bloom_filter_time_ns
    global last_draken_inner_join_rows_hashed
    global last_draken_inner_join_candidate_rows
    global last_draken_inner_join_result_rows
    global last_draken_inner_join_rows_eliminated_by_bloom_filter

    cdef Py_ssize_t num_rows = right_relation.num_rows
    cdef uint64_t[::1] row_hashes
    cdef vector[uint64_t] probe_hashes
    cdef vector[int64_t] probe_rows
    cdef pair[vector[int64_t], vector[int64_t]] matches
    cdef object left_indices
    cdef object right_indices
    cdef long long t_start
    cdef long long t_after_hash
    cdef long long t_after_probe
    cdef long long t_after_materialize
    cdef long long bloom_start
    cdef long long bloom_end
    cdef BloomFilter bloom_filter

    if num_rows == 0:
        last_draken_inner_join_hash_time_ns = 0
        last_draken_inner_join_probe_time_ns = 0
        last_draken_inner_join_materialize_time_ns = 0
        last_draken_inner_join_align_time_ns = 0
        last_draken_inner_join_bloom_filter_time_ns = 0
        last_draken_inner_join_rows_hashed = 0
        last_draken_inner_join_candidate_rows = 0
        last_draken_inner_join_result_rows = 0
        last_draken_inner_join_rows_eliminated_by_bloom_filter = 0
        return array('i'), array('i')

    t_start = perf_counter_ns()
    row_hashes = right_relation.hash(join_columns)
    t_after_hash = perf_counter_ns()

    if left_hash_table.bloom_filter is not None:
        bloom_filter = <BloomFilter>left_hash_table.bloom_filter
        bloom_start = perf_counter_ns()
        _append_bloom_filtered_rows_and_hashes(
            right_relation,
            join_columns,
            row_hashes,
            bloom_filter,
            probe_hashes,
            probe_rows,
        )
        bloom_end = perf_counter_ns()
        last_draken_inner_join_bloom_filter_time_ns = bloom_end - bloom_start
        last_draken_inner_join_rows_eliminated_by_bloom_filter = (
            num_rows - <Py_ssize_t>probe_rows.size()
        )
    else:
        _append_valid_rows_and_hashes(
            right_relation,
            join_columns,
            row_hashes,
            probe_hashes,
            probe_rows,
        )
        bloom_end = perf_counter_ns()
        last_draken_inner_join_bloom_filter_time_ns = 0
        last_draken_inner_join_rows_eliminated_by_bloom_filter = 0

    if probe_rows.size() == 0:
        last_draken_inner_join_hash_time_ns = t_after_hash - t_start
        last_draken_inner_join_probe_time_ns = 0
        last_draken_inner_join_materialize_time_ns = 0
        last_draken_inner_join_align_time_ns = 0
        last_draken_inner_join_bloom_filter_time_ns = last_draken_inner_join_bloom_filter_time_ns
        last_draken_inner_join_rows_hashed = num_rows
        last_draken_inner_join_candidate_rows = 0
        last_draken_inner_join_result_rows = 0
        return array('i'), array('i')

    matches = left_hash_table.engine.probe_join_indices(
        &probe_hashes[0],
        &probe_rows[0],
        <size_t> probe_rows.size(),
    )
    t_after_probe = perf_counter_ns()
    left_indices = _int32_array_from_vector(matches.first)
    right_indices = _int32_array_from_vector(matches.second)
    t_after_materialize = perf_counter_ns()

    last_draken_inner_join_hash_time_ns = t_after_hash - t_start
    last_draken_inner_join_probe_time_ns = t_after_probe - bloom_end
    last_draken_inner_join_materialize_time_ns = t_after_materialize - t_after_probe
    last_draken_inner_join_align_time_ns = 0
    last_draken_inner_join_rows_hashed = num_rows
    last_draken_inner_join_candidate_rows = probe_rows.size()
    last_draken_inner_join_result_rows = matches.first.size()

    return left_indices, right_indices


cpdef object inner_join_carchar_morsel_aligned(
    Morsel left_relation,
    Morsel right_relation,
    list join_columns,
    DrakenCarcharJoinMap left_hash_table,
):
    global last_draken_inner_join_hash_time_ns
    global last_draken_inner_join_probe_time_ns
    global last_draken_inner_join_materialize_time_ns
    global last_draken_inner_join_align_time_ns
    global last_draken_inner_join_bloom_filter_time_ns
    global last_draken_inner_join_rows_hashed
    global last_draken_inner_join_candidate_rows
    global last_draken_inner_join_result_rows
    global last_draken_inner_join_rows_eliminated_by_bloom_filter

    cdef Py_ssize_t num_rows = right_relation.num_rows
    cdef uint64_t[::1] row_hashes
    cdef vector[uint64_t] probe_hashes
    cdef vector[int64_t] probe_rows
    cdef pair[vector[int64_t], vector[int64_t]] matches
    cdef long long t_start
    cdef long long t_after_hash
    cdef long long t_after_probe
    cdef long long t_before_align = 0
    cdef long long bloom_start
    cdef long long bloom_end
    cdef BloomFilter bloom_filter
    cdef int32_t* left_indices_ptr = NULL
    cdef int32_t* right_indices_ptr = NULL
    cdef int32_t[::1] left_indices_view
    cdef int32_t[::1] right_indices_view

    if num_rows == 0:
        last_draken_inner_join_hash_time_ns = 0
        last_draken_inner_join_probe_time_ns = 0
        last_draken_inner_join_materialize_time_ns = 0
        last_draken_inner_join_align_time_ns = 0
        last_draken_inner_join_bloom_filter_time_ns = 0
        last_draken_inner_join_rows_hashed = 0
        last_draken_inner_join_candidate_rows = 0
        last_draken_inner_join_result_rows = 0
        last_draken_inner_join_rows_eliminated_by_bloom_filter = 0
        return None

    t_start = perf_counter_ns()
    row_hashes = right_relation.hash(join_columns)
    t_after_hash = perf_counter_ns()

    if left_hash_table.bloom_filter is not None:
        bloom_filter = <BloomFilter>left_hash_table.bloom_filter
        bloom_start = perf_counter_ns()
        _append_bloom_filtered_rows_and_hashes(
            right_relation,
            join_columns,
            row_hashes,
            bloom_filter,
            probe_hashes,
            probe_rows,
        )
        bloom_end = perf_counter_ns()
        last_draken_inner_join_bloom_filter_time_ns = bloom_end - bloom_start
        last_draken_inner_join_rows_eliminated_by_bloom_filter = (
            num_rows - <Py_ssize_t>probe_rows.size()
        )
    else:
        _append_valid_rows_and_hashes(
            right_relation,
            join_columns,
            row_hashes,
            probe_hashes,
            probe_rows,
        )
        bloom_end = perf_counter_ns()
        last_draken_inner_join_bloom_filter_time_ns = 0
        last_draken_inner_join_rows_eliminated_by_bloom_filter = 0

    if probe_rows.size() == 0:
        last_draken_inner_join_hash_time_ns = t_after_hash - t_start
        last_draken_inner_join_probe_time_ns = 0
        last_draken_inner_join_materialize_time_ns = 0
        last_draken_inner_join_align_time_ns = 0
        last_draken_inner_join_rows_hashed = num_rows
        last_draken_inner_join_candidate_rows = 0
        last_draken_inner_join_result_rows = 0
        return None

    matches = left_hash_table.engine.probe_join_indices(
        &probe_hashes[0],
        &probe_rows[0],
        <size_t> probe_rows.size(),
    )
    t_after_probe = perf_counter_ns()

    last_draken_inner_join_hash_time_ns = t_after_hash - t_start
    last_draken_inner_join_probe_time_ns = t_after_probe - bloom_end
    last_draken_inner_join_rows_hashed = num_rows
    last_draken_inner_join_candidate_rows = probe_rows.size()
    last_draken_inner_join_result_rows = matches.first.size()

    if matches.first.size() == 0:
        last_draken_inner_join_materialize_time_ns = 0
        last_draken_inner_join_align_time_ns = 0
        return None

    try:
        left_indices_view = _int32_view_from_vector(matches.first, &left_indices_ptr)
        right_indices_view = _int32_view_from_vector(matches.second, &right_indices_ptr)
        last_draken_inner_join_materialize_time_ns = perf_counter_ns() - t_after_probe
        t_before_align = perf_counter_ns()
        return align_tables(left_relation, right_relation, left_indices_view, right_indices_view)
    finally:
        if left_indices_ptr != NULL:
            PyMem_Free(left_indices_ptr)
        if right_indices_ptr != NULL:
            PyMem_Free(right_indices_ptr)
        if t_before_align != 0:
            last_draken_inner_join_align_time_ns = perf_counter_ns() - t_before_align


cpdef tuple get_last_draken_inner_join_metrics():
    return (
        last_draken_inner_join_hash_time_ns,
        last_draken_inner_join_probe_time_ns,
        last_draken_inner_join_bloom_filter_time_ns,
        last_draken_inner_join_rows_hashed,
        last_draken_inner_join_candidate_rows,
        last_draken_inner_join_result_rows,
        last_draken_inner_join_materialize_time_ns,
        last_draken_inner_join_align_time_ns,
        last_draken_inner_join_rows_eliminated_by_bloom_filter,
        last_draken_inner_join_build_bloom_time_ns,
    )
