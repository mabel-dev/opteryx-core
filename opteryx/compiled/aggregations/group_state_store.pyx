# cython: language_level=3
# cython: boundscheck=False
# cython: wraparound=False
# cython: nonecheck=False
# cython: cdivision=True
# cython: infer_types=True

from opteryx.draken.morsels.morsel cimport Morsel
from opteryx.draken.core.buffers cimport DrakenFixedBuffer
from opteryx.draken.core.buffers cimport DrakenConstantBuffer
from opteryx.draken.vectors.dictionary_vector cimport DictionaryVector
from opteryx.draken.vectors.constant_vector cimport ConstantVector
from opteryx.compiled.aggregations.aggregate_kernels cimport AGG_AVG
from opteryx.compiled.aggregations.aggregate_kernels cimport AGG_COUNT
from opteryx.compiled.aggregations.aggregate_kernels cimport AGG_COUNT_DISTINCT
from opteryx.compiled.aggregations.aggregate_kernels cimport AGG_COUNT_STAR
from opteryx.compiled.aggregations.aggregate_kernels cimport AGG_HASH_ONE
from opteryx.compiled.aggregations.aggregate_kernels cimport AGG_MAX
from opteryx.compiled.aggregations.aggregate_kernels cimport AGG_MIN
from opteryx.compiled.aggregations.aggregate_kernels cimport AGG_SUM
from opteryx.compiled.aggregations.aggregate_kernels cimport finalize_state
from opteryx.compiled.aggregations.aggregate_kernels cimport new_state
from opteryx.compiled.aggregations.aggregate_kernels cimport update_state
from opteryx.draken.vectors.int64_vector cimport Int64Vector
from opteryx.draken.vectors.float64_vector cimport Float64Vector
from opteryx.draken.vectors.integer_vector cimport IntegerVector

from libc.stdint cimport int8_t, int16_t, int32_t, int64_t, uint8_t, uint64_t
from libc.stdlib cimport malloc, free
from libc.stddef cimport size_t
from cython.operator cimport dereference, preincrement
from opteryx.third_party.abseil.containers cimport IdentityHash
from opteryx.third_party.abseil.containers cimport flat_hash_map
from opteryx.third_party.abseil.containers cimport flat_hash_set

cdef int MODE_GENERAL = 0
cdef int FAST_VALUE_UNKNOWN = 0
cdef int FAST_VALUE_INT64 = 1
cdef int FAST_VALUE_FLOAT64 = 2
cdef int FAST_VALUE_INT_NARROW = 3
cdef object _MISSING = object()


cdef uint64_t* _widen_integer_vector_to_u64(
    IntegerVector iv,
    Py_ssize_t n,
    uint8_t** out_null_bitmap,
) except NULL:
    """Widen IntegerVector data to a malloc'd uint64_t[n] buffer (sign-extended to int64).

    Sets *out_null_bitmap to iv.ptr.null_bitmap (NULL for non-nullable columns).
    Caller MUST free() the returned pointer.
    """
    cdef DrakenFixedBuffer* ptr = iv.ptr
    cdef size_t itemsize = ptr.itemsize
    cdef uint64_t* buf = <uint64_t*>malloc(n * sizeof(uint64_t))
    cdef Py_ssize_t i
    cdef int8_t* d8
    cdef int16_t* d16
    cdef int32_t* d32

    if buf == NULL:
        raise MemoryError("_widen_integer_vector_to_u64: malloc failed")

    out_null_bitmap[0] = ptr.null_bitmap

    if itemsize == 1:
        d8 = <int8_t*>ptr.data
        for i in range(n):
            buf[i] = <uint64_t>(<int64_t>d8[i])
    elif itemsize == 2:
        d16 = <int16_t*>ptr.data
        for i in range(n):
            buf[i] = <uint64_t>(<int64_t>d16[i])
    else:
        d32 = <int32_t*>ptr.data
        for i in range(n):
            buf[i] = <uint64_t>(<int64_t>d32[i])

    return buf


cdef class GroupStateStore:
    """
    Compiled state store for grouped aggregation.

    Notes:
    - this is the first kernel slice for Phase 2.
    - state/value semantics match the Python ShuffleGroupByOperation behavior.
    - hot loops are in Cython; data values remain Python objects for now.
    """

    cdef list _group_by_columns
    cdef list _agg_aliases
    cdef list _agg_function_codes
    cdef list _agg_columns
    cdef dict _states
    cdef dict _hash_keys
    cdef Py_ssize_t _rows_seen
    cdef int _single_mode
    cdef object _single_column
    cdef bint _int64_count_star_mode
    cdef bint _int64_count_star_seen_null
    cdef int64_t _int64_count_star_null_count
    cdef flat_hash_map[uint64_t, int64_t] _int64_count_star_counts
    cdef int _int64_typed_mode
    cdef int _int64_typed_value_type
    cdef flat_hash_map[uint64_t, int64_t] _int64_typed_rows
    cdef flat_hash_map[uint64_t, int64_t] _int64_typed_i64
    cdef flat_hash_map[uint64_t, double] _int64_typed_f64
    cdef flat_hash_map[uint64_t, int64_t] _int64_typed_count
    cdef flat_hash_map[uint64_t, uint8_t] _int64_typed_seen
    cdef bint _int64_count_distinct_mode
    cdef bint _int64_count_distinct_seen_null_key
    cdef int64_t _int64_count_distinct_null_key_count
    cdef flat_hash_map[uint64_t, int64_t] _int64_count_distinct_counts
    cdef flat_hash_map[uint64_t, flat_hash_set[uint64_t, IdentityHash]] _int64_count_distinct_seen
    cdef flat_hash_set[uint64_t, IdentityHash] _int64_count_distinct_null_key_seen
    cdef object _specialized_kernel
    cdef uint64_t _dict_groupby_fastpath_hits
    cdef uint64_t _dict_groupby_fastpath_fallbacks
    cdef uint64_t _constant_groupby_fastpath_hits
    cdef uint64_t _constant_groupby_fastpath_fallbacks

    def __cinit__(self, list group_by_columns, list aggregations):
        cdef object aggregation
        cdef str function
        cdef object column

        self._group_by_columns = group_by_columns
        self._agg_aliases = []
        self._agg_function_codes = []
        self._agg_columns = []
        self._states = {}
        self._hash_keys = {}
        self._rows_seen = 0
        self._single_mode = MODE_GENERAL
        self._single_column = None
        self._int64_count_star_mode = False
        self._int64_count_star_seen_null = False
        self._int64_count_star_null_count = 0
        self._int64_typed_mode = MODE_GENERAL
        self._int64_typed_value_type = FAST_VALUE_UNKNOWN
        self._int64_count_distinct_mode = False
        self._int64_count_distinct_seen_null_key = False
        self._int64_count_distinct_null_key_count = 0
        self._specialized_kernel = None
        self._dict_groupby_fastpath_hits = 0
        self._dict_groupby_fastpath_fallbacks = 0
        self._constant_groupby_fastpath_hits = 0
        self._constant_groupby_fastpath_fallbacks = 0

        for aggregation in aggregations:
            function = aggregation[1]
            column = aggregation[2]
            self._agg_aliases.append(aggregation[0])
            self._agg_columns.append(column)
            if function == "count":
                if column is None:
                    self._agg_function_codes.append(AGG_COUNT_STAR)
                else:
                    self._agg_function_codes.append(AGG_COUNT)
            elif function == "sum":
                self._agg_function_codes.append(AGG_SUM)
            elif function == "min":
                self._agg_function_codes.append(AGG_MIN)
            elif function == "max":
                self._agg_function_codes.append(AGG_MAX)
            elif function == "mean" or function == "avg":
                self._agg_function_codes.append(AGG_AVG)
            elif function == "count_distinct" or function == "distinct":
                self._agg_function_codes.append(AGG_COUNT_DISTINCT)
            elif function == "hash_one":
                self._agg_function_codes.append(AGG_HASH_ONE)
            else:
                raise ValueError(f"unsupported aggregation function '{function}'")

        # Fast path for common single-aggregate shapes.
        # Excludes HASH_ONE because its sentinel is owned by aggregate_kernels.
        if len(self._agg_function_codes) == 1:
            if self._agg_function_codes[0] in (
                AGG_COUNT_STAR,
                AGG_COUNT,
                AGG_SUM,
                AGG_MIN,
                AGG_MAX,
                AGG_AVG,
                AGG_COUNT_DISTINCT,
            ):
                self._single_mode = self._agg_function_codes[0]
                self._single_column = self._agg_columns[0]

        # Specialized path for COUNT(*) grouped by a single int64 key.
        # This avoids Python key objects and Python dict increments per input row.
        if (
            len(self._group_by_columns) == 1
            and len(self._agg_function_codes) == 1
            and self._agg_function_codes[0] == AGG_COUNT_STAR
        ):
            self._int64_count_star_mode = True
        elif (
            len(self._group_by_columns) == 1
            and len(self._agg_function_codes) == 1
            and self._agg_function_codes[0] in (
                AGG_COUNT,
                AGG_SUM,
                AGG_MIN,
                AGG_MAX,
                AGG_AVG,
                AGG_HASH_ONE,
            )
        ):
            self._int64_typed_mode = self._agg_function_codes[0]
        elif (
            len(self._group_by_columns) == 1
            and len(self._agg_function_codes) == 1
            and self._agg_function_codes[0] == AGG_COUNT_DISTINCT
        ):
            self._int64_count_distinct_mode = True

        try:
            from opteryx.compiled.aggregations.group_by_draken import build_specialized_kernel
            self._specialized_kernel = build_specialized_kernel(
                self._group_by_columns,
                self._agg_function_codes,
                self._agg_columns,
            )
        except Exception:
            self._specialized_kernel = None

        if self._specialized_kernel is not None:
            self._int64_count_star_mode = False
            self._int64_typed_mode = MODE_GENERAL
            self._int64_count_distinct_mode = False

    @property
    def rows_seen(self):
        return self._rows_seen

    @property
    def dict_groupby_fastpath_hits(self):
        return self._dict_groupby_fastpath_hits

    @property
    def dict_groupby_fastpath_fallbacks(self):
        return self._dict_groupby_fastpath_fallbacks

    @property
    def constant_groupby_fastpath_hits(self):
        return self._constant_groupby_fastpath_hits

    @property
    def constant_groupby_fastpath_fallbacks(self):
        return self._constant_groupby_fastpath_fallbacks

    @property
    def readings(self):
        """Return telemetry dict compatible with CarcharGroupStateEngine"""
        return {
            "feature_groupby_engine_carchar": 0,
            "feature_groupby_engine_constant": 0,
            "feature_groupby_engine_legacy": 1,  # GroupStateStore is the legacy backend
            "feature_groupby_engine_multi_key_fixed": 0,
            "feature_groupby_engine_multi_key_object": 0,
            "draken_dict_groupby_fastpath_hits": self._dict_groupby_fastpath_hits,
            "draken_dict_groupby_fastpath_fallbacks": self._dict_groupby_fastpath_fallbacks,
            "draken_constant_groupby_fastpath_hits": self._constant_groupby_fastpath_hits,
            "draken_constant_groupby_fastpath_fallbacks": self._constant_groupby_fastpath_fallbacks,
            "draken_constant_groupby_output_vector_hits": 0,
            "draken_constant_groupby_output_vector_fallbacks": 0,
            "groupby_key_store_bytes": 0,
            "groupby_key_store_limit_bytes": 0,
        }

    cpdef void ingest(self, Morsel morsel):
        cdef Py_ssize_t row_count
        cdef Py_ssize_t row_idx
        cdef Py_ssize_t key_count
        cdef Py_ssize_t agg_count
        cdef Py_ssize_t key_idx
        cdef Py_ssize_t agg_idx
        cdef list key_vectors
        cdef list key_parts
        cdef dict source_vectors
        cdef list value_vectors
        cdef list agg_function_codes
        cdef list agg_columns
        cdef object key_vector0
        cdef object key
        cdef object state
        cdef object states
        cdef object single_vector
        cdef int single_mode
        cdef object single_column
        cdef object column
        cdef object vector
        cdef object value
        cdef int function_code
        cdef Int64Vector key_int64_vector
        cdef DrakenFixedBuffer* key_ptr
        cdef int64_t* key_data
        cdef uint8_t* key_nulls
        cdef int64_t key_value
        cdef uint64_t key_u64
        cdef Float64Vector value_f64_vector
        cdef Int64Vector value_i64_vector
        cdef DrakenFixedBuffer* value_ptr
        cdef int64_t* value_i64_data
        cdef double* value_f64_data
        cdef uint8_t* value_nulls
        cdef object value_vector0
        cdef double value_f64
        cdef int64_t value_i64
        cdef int typed_mode
        cdef int typed_value_type
        cdef uint64_t[::1] key_hashes
        cdef uint64_t key_hash
        cdef uint64_t distinct_value_u64
        cdef IntegerVector value_int_vector
        cdef DrakenFixedBuffer* int_value_ptr
        cdef uint64_t* _narrow_key_buf
        cdef uint64_t* _narrow_value_buf
        cdef bint dict_fastpath_candidate = False
        cdef object dict_candidate_vector
        cdef ConstantVector key_constant_vector
        cdef DrakenConstantBuffer* key_const_ptr
        cdef uint8_t* key_const_nulls
        cdef object key_const_scalar
        cdef Py_ssize_t key_const_valid_rows
        cdef Py_ssize_t key_const_null_rows
        cdef bint key_const_valid
        cdef object key_const_state
        cdef object key_const_null_state

        if morsel is None or morsel.num_rows == 0:
            return

        _narrow_key_buf = NULL
        _narrow_value_buf = NULL

        row_count = morsel.num_rows
        self._rows_seen += row_count

        if self._specialized_kernel is not None:
            if len(self._group_by_columns) == 1 and len(self._agg_function_codes) == 1:
                dict_candidate_vector = morsel.column(self._group_by_columns[0])
                if isinstance(dict_candidate_vector, DictionaryVector):
                    dict_fastpath_candidate = True
                elif self._agg_function_codes[0] == AGG_COUNT_DISTINCT and self._agg_columns[0] is not None:
                    dict_candidate_vector = morsel.column(self._agg_columns[0])
                    if isinstance(dict_candidate_vector, DictionaryVector):
                        dict_fastpath_candidate = True

            if self._specialized_kernel.ingest(morsel):
                if dict_fastpath_candidate:
                    self._dict_groupby_fastpath_hits += 1
                return
            if dict_fastpath_candidate:
                self._dict_groupby_fastpath_fallbacks += 1
            self._specialized_kernel = None

        single_mode = self._single_mode

        key_vectors = []
        for column in self._group_by_columns:
            key_vectors.append(morsel.column(column))
        key_count = len(key_vectors)
        if key_count == 1:
            key_vector0 = key_vectors[0]

        # Constant group-key fast path for the common single-aggregate shapes.
        # This avoids per-row hash/key materialization when all rows share one
        # logical key value (with optional NULL-key rows via bitmap).
        if key_count == 1 and isinstance(key_vector0, ConstantVector):
            if single_mode == AGG_COUNT_STAR:
                key_constant_vector = <ConstantVector>key_vector0
                key_const_ptr = key_constant_vector.ptr
                key_const_nulls = <uint8_t*>key_const_ptr.null_bitmap
                key_const_scalar = key_constant_vector.scalar_value()
                key_const_valid_rows = 0
                key_const_null_rows = 0

                if key_const_nulls == NULL:
                    key_const_valid_rows = row_count
                else:
                    for row_idx in range(row_count):
                        key_const_valid = ((key_const_nulls[row_idx >> 3] >> (row_idx & 7)) & 1) != 0
                        if key_const_valid:
                            key_const_valid_rows += 1
                        else:
                            key_const_null_rows += 1

                if key_const_valid_rows > 0:
                    key_const_state = self._states.get(key_const_scalar)
                    if key_const_state is None:
                        self._states[key_const_scalar] = key_const_valid_rows
                    else:
                        self._states[key_const_scalar] = key_const_state + key_const_valid_rows

                if key_const_null_rows > 0:
                    key_const_null_state = self._states.get(None)
                    if key_const_null_state is None:
                        self._states[None] = key_const_null_rows
                    else:
                        self._states[None] = key_const_null_state + key_const_null_rows

                self._constant_groupby_fastpath_hits += 1
                return

            if single_mode == AGG_COUNT:
                key_constant_vector = <ConstantVector>key_vector0
                key_const_ptr = key_constant_vector.ptr
                key_const_nulls = <uint8_t*>key_const_ptr.null_bitmap
                key_const_scalar = key_constant_vector.scalar_value()
                single_column = self._single_column
                single_vector = morsel.column(single_column)

                if key_const_nulls == NULL:
                    key_const_state = self._states.get(key_const_scalar)
                    if key_const_state is None:
                        key_const_state = 0
                    for row_idx in range(row_count):
                        value = single_vector[row_idx]
                        if value is not None:
                            key_const_state += 1
                    self._states[key_const_scalar] = key_const_state
                else:
                    key_const_state = self._states.get(key_const_scalar)
                    if key_const_state is None:
                        key_const_state = 0
                    key_const_null_state = self._states.get(None)
                    if key_const_null_state is None:
                        key_const_null_state = 0
                    key_const_valid_rows = 0
                    key_const_null_rows = 0

                    for row_idx in range(row_count):
                        key_const_valid = ((key_const_nulls[row_idx >> 3] >> (row_idx & 7)) & 1) != 0
                        value = single_vector[row_idx]
                        if key_const_valid:
                            key_const_valid_rows += 1
                            if value is not None:
                                key_const_state += 1
                        else:
                            key_const_null_rows += 1
                            if value is not None:
                                key_const_null_state += 1

                    if key_const_valid_rows > 0:
                        self._states[key_const_scalar] = key_const_state
                    if key_const_null_rows > 0:
                        self._states[None] = key_const_null_state

                self._constant_groupby_fastpath_hits += 1
                return

            if single_mode in (AGG_SUM, AGG_MIN, AGG_MAX, AGG_AVG, AGG_COUNT_DISTINCT):
                key_constant_vector = <ConstantVector>key_vector0
                key_const_ptr = key_constant_vector.ptr
                key_const_nulls = <uint8_t*>key_const_ptr.null_bitmap
                key_const_scalar = key_constant_vector.scalar_value()
                single_column = self._single_column
                single_vector = morsel.column(single_column)

                key_const_state = self._states.get(key_const_scalar, _MISSING)
                key_const_null_state = self._states.get(None, _MISSING)
                key_const_valid_rows = 0
                key_const_null_rows = 0

                for row_idx in range(row_count):
                    if key_const_nulls == NULL:
                        key_const_valid = True
                    else:
                        key_const_valid = ((key_const_nulls[row_idx >> 3] >> (row_idx & 7)) & 1) != 0

                    value = single_vector[row_idx]

                    if key_const_valid:
                        key_const_valid_rows += 1
                        if single_mode == AGG_SUM:
                            if key_const_state is _MISSING:
                                key_const_state = None
                            if value is not None:
                                key_const_state = value if key_const_state is None else key_const_state + value
                        elif single_mode == AGG_MIN:
                            if key_const_state is _MISSING:
                                key_const_state = None
                            if value is not None:
                                if key_const_state is None or value < key_const_state:
                                    key_const_state = value
                        elif single_mode == AGG_MAX:
                            if key_const_state is _MISSING:
                                key_const_state = None
                            if value is not None:
                                if key_const_state is None or value > key_const_state:
                                    key_const_state = value
                        elif single_mode == AGG_AVG:
                            if key_const_state is _MISSING:
                                key_const_state = [0, 0]
                            if value is not None:
                                key_const_state[0] += value
                                key_const_state[1] += 1
                        else:  # AGG_COUNT_DISTINCT
                            if key_const_state is _MISSING:
                                key_const_state = set()
                            if value is not None:
                                key_const_state.add(value)
                    else:
                        key_const_null_rows += 1
                        if single_mode == AGG_SUM:
                            if key_const_null_state is _MISSING:
                                key_const_null_state = None
                            if value is not None:
                                key_const_null_state = (
                                    value if key_const_null_state is None else key_const_null_state + value
                                )
                        elif single_mode == AGG_MIN:
                            if key_const_null_state is _MISSING:
                                key_const_null_state = None
                            if value is not None:
                                if key_const_null_state is None or value < key_const_null_state:
                                    key_const_null_state = value
                        elif single_mode == AGG_MAX:
                            if key_const_null_state is _MISSING:
                                key_const_null_state = None
                            if value is not None:
                                if key_const_null_state is None or value > key_const_null_state:
                                    key_const_null_state = value
                        elif single_mode == AGG_AVG:
                            if key_const_null_state is _MISSING:
                                key_const_null_state = [0, 0]
                            if value is not None:
                                key_const_null_state[0] += value
                                key_const_null_state[1] += 1
                        else:  # AGG_COUNT_DISTINCT
                            if key_const_null_state is _MISSING:
                                key_const_null_state = set()
                            if value is not None:
                                key_const_null_state.add(value)

                if key_const_valid_rows > 0:
                    self._states[key_const_scalar] = key_const_state
                if key_const_null_rows > 0:
                    self._states[None] = key_const_null_state

                self._constant_groupby_fastpath_hits += 1
                return

            self._constant_groupby_fastpath_fallbacks += 1

        if self._int64_count_star_mode:
            if key_count != 1 or not isinstance(key_vector0, (Int64Vector, IntegerVector)):
                # This shape is not compatible with the integer COUNT(*) fast path.
                self._int64_count_star_mode = False
            else:
                if isinstance(key_vector0, Int64Vector):
                    key_int64_vector = <Int64Vector>key_vector0
                    key_ptr = key_int64_vector.ptr
                    key_data = <int64_t*>key_ptr.data
                    key_nulls = <uint8_t*>key_ptr.null_bitmap
                else:
                    _narrow_key_buf = _widen_integer_vector_to_u64(
                        <IntegerVector>key_vector0, row_count, &key_nulls)
                    key_data = <int64_t*>_narrow_key_buf

                if self._int64_count_star_counts.size() == 0 and row_count > 0:
                    self._int64_count_star_counts.reserve(<size_t>(row_count * 2))

                if key_nulls == NULL:
                    for row_idx in range(row_count):
                        key_value = key_data[row_idx]
                        self._int64_count_star_counts[<uint64_t>key_value] += 1
                else:
                    for row_idx in range(row_count):
                        if (key_nulls[row_idx >> 3] >> (row_idx & 7)) & 1:
                            key_value = key_data[row_idx]
                            self._int64_count_star_counts[<uint64_t>key_value] += 1
                        else:
                            self._int64_count_star_seen_null = True
                            self._int64_count_star_null_count += 1

                if _narrow_key_buf != NULL:
                    free(_narrow_key_buf)
                    _narrow_key_buf = NULL
                return

        if self._int64_count_distinct_mode:
            if key_count != 1 or not isinstance(key_vector0, (Int64Vector, IntegerVector)):
                self._int64_count_distinct_mode = False
            else:
                value_vector0 = morsel.column(self._single_column)
                if not isinstance(value_vector0, (Int64Vector, IntegerVector)):
                    self._int64_count_distinct_mode = False
                else:
                    if isinstance(key_vector0, Int64Vector):
                        key_int64_vector = <Int64Vector>key_vector0
                        key_ptr = key_int64_vector.ptr
                        key_data = <int64_t*>key_ptr.data
                        key_nulls = <uint8_t*>key_ptr.null_bitmap
                    else:
                        _narrow_key_buf = _widen_integer_vector_to_u64(
                            <IntegerVector>key_vector0, row_count, &key_nulls)
                        key_data = <int64_t*>_narrow_key_buf

                    if isinstance(value_vector0, Int64Vector):
                        value_i64_vector = <Int64Vector>value_vector0
                        value_ptr = value_i64_vector.ptr
                        value_i64_data = <int64_t*>value_ptr.data
                        value_nulls = <uint8_t*>value_ptr.null_bitmap
                    else:
                        _narrow_value_buf = _widen_integer_vector_to_u64(
                            <IntegerVector>value_vector0, row_count, &value_nulls)
                        value_i64_data = <int64_t*>_narrow_value_buf

                    if self._int64_count_distinct_counts.size() == 0 and row_count > 0:
                        self._int64_count_distinct_counts.reserve(<size_t>(row_count * 2))

                    if key_nulls == NULL and value_nulls == NULL:
                        for row_idx in range(row_count):
                            key_u64 = <uint64_t>key_data[row_idx]
                            distinct_value_u64 = <uint64_t>value_i64_data[row_idx]
                            if self._int64_count_distinct_seen[key_u64].insert(distinct_value_u64).second:
                                self._int64_count_distinct_counts[key_u64] += 1
                    else:
                        for row_idx in range(row_count):
                            if value_nulls != NULL and not ((value_nulls[row_idx >> 3] >> (row_idx & 7)) & 1):
                                continue
                            distinct_value_u64 = <uint64_t>value_i64_data[row_idx]
                            if key_nulls == NULL or ((key_nulls[row_idx >> 3] >> (row_idx & 7)) & 1):
                                key_u64 = <uint64_t>key_data[row_idx]
                                if self._int64_count_distinct_seen[key_u64].insert(distinct_value_u64).second:
                                    self._int64_count_distinct_counts[key_u64] += 1
                            else:
                                self._int64_count_distinct_seen_null_key = True
                                if self._int64_count_distinct_null_key_seen.insert(distinct_value_u64).second:
                                    self._int64_count_distinct_null_key_count += 1

                    if _narrow_key_buf != NULL:
                        free(_narrow_key_buf)
                        _narrow_key_buf = NULL
                    if _narrow_value_buf != NULL:
                        free(_narrow_value_buf)
                        _narrow_value_buf = NULL
                    return

        typed_mode = self._int64_typed_mode
        if typed_mode != MODE_GENERAL:
            # Typed path is valid for single non-null integer (int64 or narrow) keys.
            if key_count != 1 or not isinstance(key_vector0, (Int64Vector, IntegerVector)):
                self._int64_typed_mode = MODE_GENERAL
                self._int64_typed_value_type = FAST_VALUE_UNKNOWN
                typed_mode = MODE_GENERAL
            else:
                if isinstance(key_vector0, Int64Vector):
                    key_int64_vector = <Int64Vector>key_vector0
                    key_ptr = key_int64_vector.ptr
                    key_data = <int64_t*>key_ptr.data
                    key_nulls = <uint8_t*>key_ptr.null_bitmap
                else:
                    _narrow_key_buf = _widen_integer_vector_to_u64(
                        <IntegerVector>key_vector0, row_count, &key_nulls)
                    key_data = <int64_t*>_narrow_key_buf

                # Any key bitmap (even if all-valid) routes through generic mode.
                if key_nulls != NULL:
                    if _narrow_key_buf != NULL:
                        free(_narrow_key_buf)
                        _narrow_key_buf = NULL
                    self._int64_typed_mode = MODE_GENERAL
                    self._int64_typed_value_type = FAST_VALUE_UNKNOWN
                    typed_mode = MODE_GENERAL
                else:
                    value_vector0 = None
                    typed_value_type = self._int64_typed_value_type
                    if typed_mode != AGG_COUNT:
                        value_vector0 = morsel.column(self._single_column)
                        if typed_value_type == FAST_VALUE_UNKNOWN:
                            if isinstance(value_vector0, Int64Vector):
                                typed_value_type = FAST_VALUE_INT64
                            elif isinstance(value_vector0, Float64Vector):
                                typed_value_type = FAST_VALUE_FLOAT64
                            elif isinstance(value_vector0, IntegerVector):
                                typed_value_type = FAST_VALUE_INT_NARROW
                            else:
                                typed_value_type = FAST_VALUE_UNKNOWN
                            self._int64_typed_value_type = typed_value_type

                    if typed_mode == AGG_COUNT:
                        value_vector0 = morsel.column(self._single_column)
                        if isinstance(value_vector0, Int64Vector):
                            value_i64_vector = <Int64Vector>value_vector0
                            value_ptr = value_i64_vector.ptr
                            value_nulls = <uint8_t*>value_ptr.null_bitmap
                            for row_idx in range(row_count):
                                key_u64 = <uint64_t>key_data[row_idx]
                                self._int64_typed_rows[key_u64] += 1
                                if value_nulls == NULL or ((value_nulls[row_idx >> 3] >> (row_idx & 7)) & 1):
                                    self._int64_typed_i64[key_u64] += 1
                        elif isinstance(value_vector0, Float64Vector):
                            value_f64_vector = <Float64Vector>value_vector0
                            value_ptr = value_f64_vector.ptr
                            value_nulls = <uint8_t*>value_ptr.null_bitmap
                            for row_idx in range(row_count):
                                key_u64 = <uint64_t>key_data[row_idx]
                                self._int64_typed_rows[key_u64] += 1
                                if value_nulls == NULL or ((value_nulls[row_idx >> 3] >> (row_idx & 7)) & 1):
                                    self._int64_typed_i64[key_u64] += 1
                        elif isinstance(value_vector0, IntegerVector):
                            # COUNT(narrow_int_col): only need null bitmap, no data widening
                            value_int_vector = <IntegerVector>value_vector0
                            int_value_ptr = value_int_vector.ptr
                            value_nulls = <uint8_t*>int_value_ptr.null_bitmap
                            for row_idx in range(row_count):
                                key_u64 = <uint64_t>key_data[row_idx]
                                self._int64_typed_rows[key_u64] += 1
                                if value_nulls == NULL or ((value_nulls[row_idx >> 3] >> (row_idx & 7)) & 1):
                                    self._int64_typed_i64[key_u64] += 1
                        else:
                            if _narrow_key_buf != NULL:
                                free(_narrow_key_buf)
                                _narrow_key_buf = NULL
                            self._int64_typed_mode = MODE_GENERAL
                            self._int64_typed_value_type = FAST_VALUE_UNKNOWN
                            typed_mode = MODE_GENERAL
                        if typed_mode != MODE_GENERAL:
                            if _narrow_key_buf != NULL:
                                free(_narrow_key_buf)
                                _narrow_key_buf = NULL
                            return
                    elif typed_value_type in (FAST_VALUE_INT64, FAST_VALUE_INT_NARROW) and isinstance(value_vector0, (Int64Vector, IntegerVector)):
                        # Int64 and narrow-int values share inner loops via aliased pointer.
                        if isinstance(value_vector0, Int64Vector):
                            value_i64_vector = <Int64Vector>value_vector0
                            value_ptr = value_i64_vector.ptr
                            value_i64_data = <int64_t*>value_ptr.data
                            value_nulls = <uint8_t*>value_ptr.null_bitmap
                        else:
                            _narrow_value_buf = _widen_integer_vector_to_u64(
                                <IntegerVector>value_vector0, row_count, &value_nulls)
                            value_i64_data = <int64_t*>_narrow_value_buf

                        if typed_mode == AGG_SUM:
                            for row_idx in range(row_count):
                                key_u64 = <uint64_t>key_data[row_idx]
                                self._int64_typed_rows[key_u64] += 1
                                if value_nulls == NULL or ((value_nulls[row_idx >> 3] >> (row_idx & 7)) & 1):
                                    self._int64_typed_i64[key_u64] += value_i64_data[row_idx]
                                    self._int64_typed_seen[key_u64] = 1
                        elif typed_mode == AGG_MIN:
                            for row_idx in range(row_count):
                                key_u64 = <uint64_t>key_data[row_idx]
                                self._int64_typed_rows[key_u64] += 1
                                if value_nulls == NULL or ((value_nulls[row_idx >> 3] >> (row_idx & 7)) & 1):
                                    value_i64 = value_i64_data[row_idx]
                                    if self._int64_typed_seen[key_u64] == 0 or value_i64 < self._int64_typed_i64[key_u64]:
                                        self._int64_typed_i64[key_u64] = value_i64
                                    self._int64_typed_seen[key_u64] = 1
                        elif typed_mode == AGG_MAX:
                            for row_idx in range(row_count):
                                key_u64 = <uint64_t>key_data[row_idx]
                                self._int64_typed_rows[key_u64] += 1
                                if value_nulls == NULL or ((value_nulls[row_idx >> 3] >> (row_idx & 7)) & 1):
                                    value_i64 = value_i64_data[row_idx]
                                    if self._int64_typed_seen[key_u64] == 0 or value_i64 > self._int64_typed_i64[key_u64]:
                                        self._int64_typed_i64[key_u64] = value_i64
                                    self._int64_typed_seen[key_u64] = 1
                        elif typed_mode == AGG_AVG:
                            for row_idx in range(row_count):
                                key_u64 = <uint64_t>key_data[row_idx]
                                self._int64_typed_rows[key_u64] += 1
                                if value_nulls == NULL or ((value_nulls[row_idx >> 3] >> (row_idx & 7)) & 1):
                                    self._int64_typed_f64[key_u64] += <double>value_i64_data[row_idx]
                                    self._int64_typed_count[key_u64] += 1
                        elif typed_mode == AGG_HASH_ONE:
                            for row_idx in range(row_count):
                                key_u64 = <uint64_t>key_data[row_idx]
                                self._int64_typed_rows[key_u64] += 1
                                if self._int64_typed_seen[key_u64] == 1:
                                    continue
                                if value_nulls == NULL or ((value_nulls[row_idx >> 3] >> (row_idx & 7)) & 1):
                                    self._int64_typed_i64[key_u64] = value_i64_data[row_idx]
                                    self._int64_typed_seen[key_u64] = 1

                        if _narrow_key_buf != NULL:
                            free(_narrow_key_buf)
                            _narrow_key_buf = NULL
                        if _narrow_value_buf != NULL:
                            free(_narrow_value_buf)
                            _narrow_value_buf = NULL
                        return
                    elif typed_value_type == FAST_VALUE_FLOAT64 and isinstance(value_vector0, Float64Vector):
                        value_f64_vector = <Float64Vector>value_vector0
                        value_ptr = value_f64_vector.ptr
                        value_f64_data = <double*>value_ptr.data
                        value_nulls = <uint8_t*>value_ptr.null_bitmap

                        if typed_mode == AGG_SUM:
                            for row_idx in range(row_count):
                                key_u64 = <uint64_t>key_data[row_idx]
                                self._int64_typed_rows[key_u64] += 1
                                if value_nulls == NULL or ((value_nulls[row_idx >> 3] >> (row_idx & 7)) & 1):
                                    self._int64_typed_f64[key_u64] += value_f64_data[row_idx]
                                    self._int64_typed_seen[key_u64] = 1
                        elif typed_mode == AGG_MIN:
                            for row_idx in range(row_count):
                                key_u64 = <uint64_t>key_data[row_idx]
                                self._int64_typed_rows[key_u64] += 1
                                if value_nulls == NULL or ((value_nulls[row_idx >> 3] >> (row_idx & 7)) & 1):
                                    value_f64 = value_f64_data[row_idx]
                                    if self._int64_typed_seen[key_u64] == 0 or value_f64 < self._int64_typed_f64[key_u64]:
                                        self._int64_typed_f64[key_u64] = value_f64
                                    self._int64_typed_seen[key_u64] = 1
                        elif typed_mode == AGG_MAX:
                            for row_idx in range(row_count):
                                key_u64 = <uint64_t>key_data[row_idx]
                                self._int64_typed_rows[key_u64] += 1
                                if value_nulls == NULL or ((value_nulls[row_idx >> 3] >> (row_idx & 7)) & 1):
                                    value_f64 = value_f64_data[row_idx]
                                    if self._int64_typed_seen[key_u64] == 0 or value_f64 > self._int64_typed_f64[key_u64]:
                                        self._int64_typed_f64[key_u64] = value_f64
                                    self._int64_typed_seen[key_u64] = 1
                        elif typed_mode == AGG_AVG:
                            for row_idx in range(row_count):
                                key_u64 = <uint64_t>key_data[row_idx]
                                self._int64_typed_rows[key_u64] += 1
                                if value_nulls == NULL or ((value_nulls[row_idx >> 3] >> (row_idx & 7)) & 1):
                                    self._int64_typed_f64[key_u64] += value_f64_data[row_idx]
                                    self._int64_typed_count[key_u64] += 1

                        if _narrow_key_buf != NULL:
                            free(_narrow_key_buf)
                            _narrow_key_buf = NULL
                        return

                    # Unsupported typed value vector/type for this morsel;
                    # disable typed mode and continue through generic mode.
                    if _narrow_key_buf != NULL:
                        free(_narrow_key_buf)
                        _narrow_key_buf = NULL
                    self._int64_typed_mode = MODE_GENERAL
                    self._int64_typed_value_type = FAST_VALUE_UNKNOWN
                    typed_mode = MODE_GENERAL

        if key_count > 0:
            key_hashes = morsel.hash(columns=self._group_by_columns)

        if single_mode != MODE_GENERAL:
            if single_mode == AGG_COUNT_STAR:
                if key_count == 0:
                    key = ()
                    state = self._states.get(key)
                    if state is None:
                        self._states[key] = row_count
                    else:
                        self._states[key] = state + row_count
                    return
                for row_idx in range(row_count):
                    key_hash = key_hashes[row_idx]
                    key = key_hash
                    state = self._states.get(key)
                    if state is None:
                        if key_count == 1:
                            self._hash_keys[key] = key_vector0[row_idx]
                        else:
                            key_parts = []
                            for key_idx in range(key_count):
                                key_parts.append(key_vectors[key_idx][row_idx])
                            self._hash_keys[key] = tuple(key_parts)
                        self._states[key] = 1
                    else:
                        self._states[key] = state + 1
                return

            if single_mode == AGG_COUNT:
                single_column = self._single_column
                single_vector = morsel.column(single_column)
                if key_count == 0:
                    key = ()
                    state = self._states.get(key)
                    if state is None:
                        state = 0
                    for row_idx in range(row_count):
                        value = single_vector[row_idx]
                        if value is not None:
                            state += 1
                    self._states[key] = state
                    return
                for row_idx in range(row_count):
                    key_hash = key_hashes[row_idx]
                    key = key_hash
                    value = single_vector[row_idx]
                    state = self._states.get(key)
                    if state is None:
                        if key_count == 1:
                            self._hash_keys[key] = key_vector0[row_idx]
                        else:
                            key_parts = []
                            for key_idx in range(key_count):
                                key_parts.append(key_vectors[key_idx][row_idx])
                            self._hash_keys[key] = tuple(key_parts)
                        state = 0
                    if value is not None:
                        state += 1
                    self._states[key] = state
                return

            single_vector = None
            single_column = self._single_column
            single_vector = morsel.column(single_column)

            for row_idx in range(row_count):
                if key_count == 0:
                    key = ()
                else:
                    key_hash = key_hashes[row_idx]
                    key = key_hash

                value = single_vector[row_idx]
                state = self._states.get(key, _MISSING)
                if state is _MISSING and key_count > 0:
                    if key_count == 1:
                        self._hash_keys[key] = key_vector0[row_idx]
                    else:
                        key_parts = []
                        for key_idx in range(key_count):
                            key_parts.append(key_vectors[key_idx][row_idx])
                        self._hash_keys[key] = tuple(key_parts)

                if single_mode == AGG_SUM:
                    if state is _MISSING:
                        state = None
                    if value is not None:
                        state = value if state is None else state + value
                    self._states[key] = state
                elif single_mode == AGG_MIN:
                    if state is _MISSING:
                        state = None
                    if value is not None:
                        if state is None or value < state:
                            state = value
                    self._states[key] = state
                elif single_mode == AGG_MAX:
                    if state is _MISSING:
                        state = None
                    if value is not None:
                        if state is None or value > state:
                            state = value
                    self._states[key] = state
                elif single_mode == AGG_AVG:
                    if state is _MISSING:
                        state = [0, 0]
                    if value is not None:
                        state[0] += value
                        state[1] += 1
                    self._states[key] = state
                elif single_mode == AGG_COUNT_DISTINCT:
                    if state is _MISSING:
                        state = set()
                    if value is not None:
                        state.add(value)
                    self._states[key] = state
                else:
                    # Defensive: should not be reachable because __cinit__ controls single_mode.
                    if state is _MISSING:
                        state = new_state(single_mode)
                    self._states[key] = update_state(single_mode, state, value)
            return

        source_vectors = {}
        for column in self._agg_columns:
            if column is None:
                continue
            if column not in source_vectors:
                source_vectors[column] = morsel.column(column)

        # Note: General mode aggregation uses Python dict _states (slower than specialized paths).
        # TODO: Optimize by storing aggregation states in C++ containers like specialized modes do.

        agg_count = len(self._agg_function_codes)
        agg_function_codes = self._agg_function_codes
        agg_columns = self._agg_columns
        value_vectors = [None] * agg_count
        for agg_idx in range(agg_count):
            column = agg_columns[agg_idx]
            if column is None:
                continue
            value_vectors[agg_idx] = source_vectors[column]

        for row_idx in range(row_count):
            if key_count == 0:
                key = ()
            else:
                key_hash = key_hashes[row_idx]
                key = key_hash

            states = self._states.get(key)
            if states is None:
                if key_count > 0:
                    if key_count == 1:
                        self._hash_keys[key] = key_vector0[row_idx]
                    else:
                        key_parts = []
                        for key_idx in range(key_count):
                            key_parts.append(key_vectors[key_idx][row_idx])
                        self._hash_keys[key] = tuple(key_parts)
                states = []
                for agg_idx in range(agg_count):
                    states.append(new_state(agg_function_codes[agg_idx]))
                self._states[key] = states

            for agg_idx in range(agg_count):
                vector = value_vectors[agg_idx]
                if vector is None:
                    value = None
                else:
                    value = vector[row_idx]
                function_code = agg_function_codes[agg_idx]
                states[agg_idx] = update_state(function_code, states[agg_idx], value)

    cpdef list finalize_rows(self):
        cdef list rows
        cdef list finalized_values
        cdef object key
        cdef object states
        cdef object out_key
        cdef Py_ssize_t agg_idx
        cdef Py_ssize_t agg_count
        cdef Py_ssize_t key_count
        cdef list agg_function_codes
        cdef int single_mode
        cdef object finalized_value
        cdef flat_hash_map[uint64_t, int64_t].iterator count_it
        cdef flat_hash_map[uint64_t, int64_t].iterator rows_it
        cdef uint64_t key_u64
        cdef int typed_mode
        cdef int typed_value_type
        cdef int64_t c

        single_mode = self._single_mode

        if self._specialized_kernel is not None:
            return self._specialized_kernel.finalize_rows()

        if self._int64_count_star_mode:
            if (
                self._int64_count_star_counts.size() == 0
                and not self._int64_count_star_seen_null
            ):
                if self._states:
                    self._int64_count_star_mode = False
                elif self._group_by_columns:
                    return []
            if self._int64_count_star_mode:
                rows = []
                count_it = self._int64_count_star_counts.begin()
                while count_it != self._int64_count_star_counts.end():
                    rows.append(
                            (
                                (<int64_t>dereference(count_it).first,),
                                [dereference(count_it).second],
                            )
                    )
                    preincrement(count_it)
                if self._int64_count_star_seen_null:
                    rows.append(((None,), [self._int64_count_star_null_count]))
                return rows

        if self._int64_count_distinct_mode:
            if self._int64_count_distinct_counts.size() == 0:
                if self._states:
                    self._int64_count_distinct_mode = False
                elif self._group_by_columns and not self._int64_count_distinct_seen_null_key:
                    return []
            if self._int64_count_distinct_mode:
                rows = []
                count_it = self._int64_count_distinct_counts.begin()
                while count_it != self._int64_count_distinct_counts.end():
                    rows.append(
                        (
                            (<int64_t>dereference(count_it).first,),
                            [dereference(count_it).second],
                        )
                    )
                    preincrement(count_it)
                if self._int64_count_distinct_seen_null_key:
                    rows.append(((None,), [self._int64_count_distinct_null_key_count]))
                return rows

        typed_mode = self._int64_typed_mode
        typed_value_type = self._int64_typed_value_type
        if typed_mode != MODE_GENERAL:
            if self._int64_typed_rows.size() == 0:
                if self._states:
                    self._int64_typed_mode = MODE_GENERAL
                    typed_mode = MODE_GENERAL
                elif self._group_by_columns:
                    return []
            if typed_mode != MODE_GENERAL:
                rows = []
                rows_it = self._int64_typed_rows.begin()
                while rows_it != self._int64_typed_rows.end():
                    key_u64 = dereference(rows_it).first
                    if typed_mode == AGG_COUNT:
                        finalized_value = self._int64_typed_i64[key_u64]
                    elif typed_mode == AGG_SUM:
                        if self._int64_typed_seen[key_u64] == 0:
                            finalized_value = None
                        elif typed_value_type == FAST_VALUE_FLOAT64:
                            finalized_value = self._int64_typed_f64[key_u64]
                        else:
                            finalized_value = self._int64_typed_i64[key_u64]
                    elif typed_mode == AGG_MIN:
                        if self._int64_typed_seen[key_u64] == 0:
                            finalized_value = None
                        elif typed_value_type == FAST_VALUE_FLOAT64:
                            finalized_value = self._int64_typed_f64[key_u64]
                        else:
                            finalized_value = self._int64_typed_i64[key_u64]
                    elif typed_mode == AGG_MAX:
                        if self._int64_typed_seen[key_u64] == 0:
                            finalized_value = None
                        elif typed_value_type == FAST_VALUE_FLOAT64:
                            finalized_value = self._int64_typed_f64[key_u64]
                        else:
                            finalized_value = self._int64_typed_i64[key_u64]
                    elif typed_mode == AGG_AVG:
                        c = self._int64_typed_count[key_u64]
                        finalized_value = None if c == 0 else (self._int64_typed_f64[key_u64] / c)
                    elif typed_mode == AGG_HASH_ONE:
                        if self._int64_typed_seen[key_u64] == 0:
                            finalized_value = None
                        elif typed_value_type == FAST_VALUE_FLOAT64:
                            finalized_value = self._int64_typed_f64[key_u64]
                        else:
                            finalized_value = self._int64_typed_i64[key_u64]
                    else:
                        finalized_value = None

                    rows.append(((<int64_t>key_u64,), [finalized_value]))
                    preincrement(rows_it)
                return rows

        if not self._states:
            if self._group_by_columns:
                return []
            if single_mode == MODE_GENERAL:
                self._states[()] = [new_state(function_code) for function_code in self._agg_function_codes]
            else:
                self._states[()] = new_state(single_mode)

        rows = []
        key_count = len(self._group_by_columns)

        if single_mode != MODE_GENERAL:
            for key, state in self._states.items():
                finalized_value = finalize_state(single_mode, state)
                if key_count == 0:
                    out_key = ()
                elif key_count == 1:
                    out_key = (self._hash_keys.get(key, key),)
                else:
                    out_key = self._hash_keys.get(key, key)
                rows.append((out_key, [finalized_value]))
            return rows

        agg_function_codes = self._agg_function_codes
        agg_count = len(agg_function_codes)

        for key, states in self._states.items():
            finalized_values = []
            for agg_idx in range(agg_count):
                finalized_values.append(
                    finalize_state(agg_function_codes[agg_idx], states[agg_idx])
                )
            if key_count == 0:
                out_key = ()
            elif key_count == 1:
                out_key = (self._hash_keys.get(key, key),)
            else:
                out_key = self._hash_keys.get(key, key)
            rows.append((out_key, finalized_values))
        return rows

    cpdef object finalize_fast_columns(self):
        cdef Py_ssize_t n
        cdef Py_ssize_t idx
        cdef flat_hash_map[uint64_t, int64_t].iterator count_it
        cdef flat_hash_map[uint64_t, int64_t].iterator rows_it
        cdef object keys
        cdef object counts
        cdef object values
        cdef int64_t[::1] key_view
        cdef int64_t[::1] count_view
        cdef int64_t[::1] value_i64_view
        cdef double[::1] value_f64_view
        cdef uint64_t key_u64
        cdef int typed_mode
        cdef int typed_value_type
        cdef int64_t c

        if self._specialized_kernel is not None:
            return self._specialized_kernel.finalize_fast_columns()

        # Fast output path for int64-key COUNT(*) and COUNT(DISTINCT int64).
        if self._int64_count_star_mode:
            if self._int64_count_star_seen_null:
                return None
            n = <Py_ssize_t>self._int64_count_star_counts.size()
            from array import array
            keys = array("q", [0]) * n
            counts = array("q", [0]) * n
            key_view = keys
            count_view = counts

            count_it = self._int64_count_star_counts.begin()
            idx = 0
            while count_it != self._int64_count_star_counts.end():
                key_view[idx] = <int64_t>dereference(count_it).first
                count_view[idx] = dereference(count_it).second
                idx += 1
                preincrement(count_it)

            return keys, counts

        if self._int64_count_distinct_mode:
            if self._int64_count_distinct_seen_null_key:
                return None

            from array import array

            n = <Py_ssize_t>self._int64_count_distinct_counts.size()
            keys = array("q", [0]) * n
            counts = array("q", [0]) * n
            key_view = keys
            count_view = counts

            count_it = self._int64_count_distinct_counts.begin()
            idx = 0
            while count_it != self._int64_count_distinct_counts.end():
                key_view[idx] = <int64_t>dereference(count_it).first
                count_view[idx] = dereference(count_it).second
                idx += 1
                preincrement(count_it)

            return keys, counts

        # Fast output path for typed int64-key single-aggregate modes.
        typed_mode = self._int64_typed_mode
        if typed_mode == MODE_GENERAL:
            return None

        n = <Py_ssize_t>self._int64_typed_rows.size()
        if n == 0:
            from array import array
            return array("q"), array("q")

        from array import array

        typed_value_type = self._int64_typed_value_type
        keys = array("q", [0]) * n
        key_view = keys

        if typed_mode == AGG_COUNT:
            values = array("q", [0]) * n
            value_i64_view = values
            rows_it = self._int64_typed_rows.begin()
            idx = 0
            while rows_it != self._int64_typed_rows.end():
                key_u64 = dereference(rows_it).first
                key_view[idx] = <int64_t>key_u64
                value_i64_view[idx] = self._int64_typed_i64[key_u64]
                idx += 1
                preincrement(rows_it)
            return keys, values

        if typed_mode in (AGG_SUM, AGG_MIN, AGG_MAX, AGG_HASH_ONE):
            # Any unseen value implies NULL output for that group; fallback to
            # finalize_rows to preserve NULL semantics.
            rows_it = self._int64_typed_rows.begin()
            while rows_it != self._int64_typed_rows.end():
                key_u64 = dereference(rows_it).first
                if self._int64_typed_seen[key_u64] == 0:
                    return None
                preincrement(rows_it)

            if typed_value_type == FAST_VALUE_FLOAT64:
                values = array("d", [0.0]) * n
                value_f64_view = values
                rows_it = self._int64_typed_rows.begin()
                idx = 0
                while rows_it != self._int64_typed_rows.end():
                    key_u64 = dereference(rows_it).first
                    key_view[idx] = <int64_t>key_u64
                    value_f64_view[idx] = self._int64_typed_f64[key_u64]
                    idx += 1
                    preincrement(rows_it)
                return keys, values

            values = array("q", [0]) * n
            value_i64_view = values
            rows_it = self._int64_typed_rows.begin()
            idx = 0
            while rows_it != self._int64_typed_rows.end():
                key_u64 = dereference(rows_it).first
                key_view[idx] = <int64_t>key_u64
                value_i64_view[idx] = self._int64_typed_i64[key_u64]
                idx += 1
                preincrement(rows_it)
            return keys, values

        if typed_mode == AGG_AVG:
            values = array("d", [0.0]) * n
            value_f64_view = values
            rows_it = self._int64_typed_rows.begin()
            idx = 0
            while rows_it != self._int64_typed_rows.end():
                key_u64 = dereference(rows_it).first
                c = self._int64_typed_count[key_u64]
                if c == 0:
                    return None
                key_view[idx] = <int64_t>key_u64
                value_f64_view[idx] = self._int64_typed_f64[key_u64] / c
                idx += 1
                preincrement(rows_it)
            return keys, values

        return None

    cpdef object finalize_fast_columns_chunked(self, Py_ssize_t chunk_size=65536):
        cdef list chunks
        cdef Py_ssize_t total
        cdef Py_ssize_t produced
        cdef Py_ssize_t this_chunk
        cdef Py_ssize_t idx
        cdef flat_hash_map[uint64_t, int64_t].iterator count_it
        cdef flat_hash_map[uint64_t, int64_t].iterator rows_it
        cdef object keys
        cdef object values
        cdef int64_t[::1] key_view
        cdef int64_t[::1] value_i64_view
        cdef double[::1] value_f64_view
        cdef uint64_t key_u64
        cdef int typed_mode
        cdef int typed_value_type
        cdef int64_t c

        if chunk_size <= 0:
            raise ValueError("chunk_size must be positive")

        if self._specialized_kernel is not None and hasattr(self._specialized_kernel, "finalize_fast_columns_chunked"):
            return self._specialized_kernel.finalize_fast_columns_chunked(chunk_size)

        chunks = []

        if self._int64_count_star_mode:
            if self._int64_count_star_seen_null:
                return None
            total = <Py_ssize_t>self._int64_count_star_counts.size()
            produced = 0
            count_it = self._int64_count_star_counts.begin()
            from array import array
            while produced < total:
                this_chunk = chunk_size if (total - produced) > chunk_size else (total - produced)
                keys = array("q", [0]) * this_chunk
                values = array("q", [0]) * this_chunk
                key_view = keys
                value_i64_view = values
                idx = 0
                while idx < this_chunk and count_it != self._int64_count_star_counts.end():
                    key_view[idx] = <int64_t>dereference(count_it).first
                    value_i64_view[idx] = dereference(count_it).second
                    idx += 1
                    preincrement(count_it)
                chunks.append((keys, values))
                produced += this_chunk
            return chunks

        if self._int64_count_distinct_mode:
            if self._int64_count_distinct_seen_null_key:
                return None
            total = <Py_ssize_t>self._int64_count_distinct_counts.size()
            produced = 0
            count_it = self._int64_count_distinct_counts.begin()
            from array import array
            while produced < total:
                this_chunk = chunk_size if (total - produced) > chunk_size else (total - produced)
                keys = array("q", [0]) * this_chunk
                values = array("q", [0]) * this_chunk
                key_view = keys
                value_i64_view = values
                idx = 0
                while idx < this_chunk and count_it != self._int64_count_distinct_counts.end():
                    key_view[idx] = <int64_t>dereference(count_it).first
                    value_i64_view[idx] = dereference(count_it).second
                    idx += 1
                    preincrement(count_it)
                chunks.append((keys, values))
                produced += this_chunk
            return chunks

        typed_mode = self._int64_typed_mode
        if typed_mode == MODE_GENERAL:
            return None

        total = <Py_ssize_t>self._int64_typed_rows.size()
        if total == 0:
            return []

        # preserve null semantics; any null result requires fallback finalize_rows.
        if typed_mode in (AGG_SUM, AGG_MIN, AGG_MAX, AGG_HASH_ONE):
            rows_it = self._int64_typed_rows.begin()
            while rows_it != self._int64_typed_rows.end():
                key_u64 = dereference(rows_it).first
                if self._int64_typed_seen[key_u64] == 0:
                    return None
                preincrement(rows_it)
        elif typed_mode == AGG_AVG:
            rows_it = self._int64_typed_rows.begin()
            while rows_it != self._int64_typed_rows.end():
                key_u64 = dereference(rows_it).first
                c = self._int64_typed_count[key_u64]
                if c == 0:
                    return None
                preincrement(rows_it)

        typed_value_type = self._int64_typed_value_type
        rows_it = self._int64_typed_rows.begin()
        produced = 0
        from array import array
        while produced < total:
            this_chunk = chunk_size if (total - produced) > chunk_size else (total - produced)
            keys = array("q", [0]) * this_chunk
            key_view = keys

            if typed_mode == AGG_AVG or typed_value_type == FAST_VALUE_FLOAT64:
                values = array("d", [0.0]) * this_chunk
                value_f64_view = values
                idx = 0
                while idx < this_chunk and rows_it != self._int64_typed_rows.end():
                    key_u64 = dereference(rows_it).first
                    key_view[idx] = <int64_t>key_u64
                    if typed_mode == AGG_COUNT:
                        value_f64_view[idx] = <double>self._int64_typed_i64[key_u64]
                    elif typed_mode == AGG_AVG:
                        c = self._int64_typed_count[key_u64]
                        value_f64_view[idx] = self._int64_typed_f64[key_u64] / c
                    else:
                        value_f64_view[idx] = self._int64_typed_f64[key_u64]
                    idx += 1
                    preincrement(rows_it)
            else:
                values = array("q", [0]) * this_chunk
                value_i64_view = values
                idx = 0
                while idx < this_chunk and rows_it != self._int64_typed_rows.end():
                    key_u64 = dereference(rows_it).first
                    key_view[idx] = <int64_t>key_u64
                    value_i64_view[idx] = self._int64_typed_i64[key_u64]
                    idx += 1
                    preincrement(rows_it)

            chunks.append((keys, values))
            produced += this_chunk

        return chunks

    def finalize_morsels(self, Py_ssize_t chunk_size=65536):
        """Generate Morsels from finalized rows for compatibility with CarcharGroupStateEngine"""
        from opteryx.draken.morsels.morsel import Morsel
        from opteryx.draken.interop.arrow import vector_from_sequence
        
        rows = self.finalize_rows()
        if not rows:
            # Empty result - return empty morsel with proper structure
            # Names: aggregation aliases + group by column names
            names = list(self._agg_aliases) if self._agg_aliases else []
            names.extend([col.decode('utf-8') if isinstance(col, bytes) else str(col) for col in self._group_by_columns])
            yield Morsel.from_vectors(names, [vector_from_sequence([]) for _ in names])
            return
        
        # Group rows into chunks and yield as Morsels
        for chunk_start in range(0, len(rows), chunk_size):
            chunk_end = min(chunk_start + chunk_size, len(rows))
            chunk_rows = rows[chunk_start:chunk_end]
            
            if not chunk_rows:
                continue
            
            # Extract number of aggregations and groups from first row
            # Row format is (key_tuple, [agg_value1, agg_value2, ...])
            first_row = chunk_rows[0]
            key_tuple = first_row[0]
            agg_values = first_row[1]
            
            agg_count = len(agg_values) if agg_values else 0 
            key_count = len(key_tuple) if key_tuple else 0
            total_cols = agg_count + key_count
            
            # Build columns
            columns = [[] for _ in range(total_cols)]
            
            for row in chunk_rows:
                key_tuple = row[0]
                agg_values = row[1]
                
                # Add aggregation values first
                for agg_idx, agg_val in enumerate(agg_values):
                    columns[agg_idx].append(agg_val)
                
                # Add key values
                for key_idx, key_val in enumerate(key_tuple):
                    columns[agg_count + key_idx].append(key_val)
            
            # Build vectors
            vectors = [vector_from_sequence(col) for col in columns]
            
            # Generate names (aggregation aliases + group by column names)
            names = []
            names.extend(self._agg_aliases if self._agg_aliases else [])
            names.extend([col.decode('utf-8') if isinstance(col, bytes) else str(col) for col in self._group_by_columns])
            
            morsel = Morsel.from_vectors(names, vectors)
            yield morsel

    cpdef object fast_finalize_unavailable_reason(self):
        cdef flat_hash_map[uint64_t, int64_t].iterator rows_it
        cdef uint64_t key_u64
        cdef int typed_mode
        cdef int64_t c

        if self._specialized_kernel is not None:
            if hasattr(self._specialized_kernel, "fast_finalize_unavailable_reason"):
                return self._specialized_kernel.fast_finalize_unavailable_reason()
            return "specialized kernel does not expose a fast-finalize diagnostic"

        if self._int64_count_star_mode:
            if self._int64_count_star_seen_null:
                return "valid result contains NULL group keys; current fast columns cannot represent NULL-key groups"
            return "fast COUNT(*) path is available"

        if self._int64_count_distinct_mode:
            if self._int64_count_distinct_seen_null_key:
                return "valid result contains NULL group keys for COUNT(DISTINCT); current fast columns cannot represent NULL-key groups"
            return "fast COUNT(DISTINCT) path is available"

        typed_mode = self._int64_typed_mode
        if typed_mode == MODE_GENERAL:
            return "typed fast mode disabled for this query/data shape (not single int64-group-key typed mode)"

        if typed_mode in (AGG_SUM, AGG_MIN, AGG_MAX, AGG_HASH_ONE):
            rows_it = self._int64_typed_rows.begin()
            while rows_it != self._int64_typed_rows.end():
                key_u64 = dereference(rows_it).first
                if self._int64_typed_seen[key_u64] == 0:
                    return "valid result has one or more groups with NULL output; typed fast columns currently require non-NULL outputs"
                preincrement(rows_it)
            return "typed fast path is available"

        if typed_mode == AGG_AVG:
            rows_it = self._int64_typed_rows.begin()
            while rows_it != self._int64_typed_rows.end():
                key_u64 = dereference(rows_it).first
                c = self._int64_typed_count[key_u64]
                if c == 0:
                    return "valid AVG result has one or more groups with no non-NULL values; typed fast columns currently require non-NULL outputs"
                preincrement(rows_it)
            return "typed AVG fast path is available"

        return "fast finalize path unavailable for this mode"
