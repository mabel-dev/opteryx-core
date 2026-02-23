# cython: language_level=3
# cython: boundscheck=False
# cython: wraparound=False
# cython: nonecheck=False
# cython: cdivision=True
# cython: infer_types=True

from opteryx.draken.morsels.morsel cimport Morsel
from opteryx.draken.core.buffers cimport DrakenFixedBuffer
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

from libc.stdint cimport int64_t, uint8_t, uint64_t
from libc.math cimport NAN
from libc.stddef cimport size_t
from cython.operator cimport dereference, preincrement
from opteryx.third_party.abseil.containers cimport IdentityHash
from opteryx.third_party.abseil.containers cimport flat_hash_map
from opteryx.third_party.abseil.containers cimport flat_hash_set

cdef int MODE_GENERAL = 0
cdef int FAST_VALUE_UNKNOWN = 0
cdef int FAST_VALUE_INT64 = 1
cdef int FAST_VALUE_FLOAT64 = 2
cdef object _MISSING = object()


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

    @property
    def rows_seen(self):
        return self._rows_seen

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

        if morsel is None or morsel.num_rows == 0:
            return

        row_count = morsel.num_rows
        self._rows_seen += row_count

        single_mode = self._single_mode

        key_vectors = []
        for column in self._group_by_columns:
            key_vectors.append(morsel.column(column))
        key_count = len(key_vectors)
        if key_count == 1:
            key_vector0 = key_vectors[0]

        if self._int64_count_star_mode:
            if key_count != 1 or not isinstance(key_vector0, Int64Vector):
                # This shape is not compatible with the int64 COUNT(*) fast path.
                self._int64_count_star_mode = False
            else:
                key_int64_vector = <Int64Vector>key_vector0
                key_ptr = key_int64_vector.ptr
                key_data = <int64_t*>key_ptr.data
                key_nulls = <uint8_t*>key_ptr.null_bitmap

                if self._int64_count_star_counts.size() == 0 and row_count > 0:
                    self._int64_count_star_counts.reserve(<size_t>(row_count * 2))

                if key_nulls == NULL:
                    for row_idx in range(row_count):
                        key_value = key_data[row_idx]
                        self._int64_count_star_counts[<uint64_t>key_value] += 1
                    return

                for row_idx in range(row_count):
                    if (key_nulls[row_idx >> 3] >> (row_idx & 7)) & 1:
                        key_value = key_data[row_idx]
                        self._int64_count_star_counts[<uint64_t>key_value] += 1
                    else:
                        self._int64_count_star_seen_null = True
                        self._int64_count_star_null_count += 1
                return

        if self._int64_count_distinct_mode:
            if key_count != 1 or not isinstance(key_vector0, Int64Vector):
                self._int64_count_distinct_mode = False
            else:
                value_vector0 = morsel.column(self._single_column)
                if not isinstance(value_vector0, Int64Vector):
                    self._int64_count_distinct_mode = False
                else:
                    key_int64_vector = <Int64Vector>key_vector0
                    key_ptr = key_int64_vector.ptr
                    key_data = <int64_t*>key_ptr.data
                    key_nulls = <uint8_t*>key_ptr.null_bitmap

                    value_i64_vector = <Int64Vector>value_vector0
                    value_ptr = value_i64_vector.ptr
                    value_i64_data = <int64_t*>value_ptr.data
                    value_nulls = <uint8_t*>value_ptr.null_bitmap

                    if self._int64_count_distinct_counts.size() == 0 and row_count > 0:
                        self._int64_count_distinct_counts.reserve(<size_t>(row_count * 2))

                    if key_nulls == NULL and value_nulls == NULL:
                        for row_idx in range(row_count):
                            key_u64 = <uint64_t>key_data[row_idx]
                            distinct_value_u64 = <uint64_t>value_i64_data[row_idx]
                            if self._int64_count_distinct_seen[key_u64].insert(distinct_value_u64).second:
                                self._int64_count_distinct_counts[key_u64] += 1
                        return

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
                    return

        typed_mode = self._int64_typed_mode
        if typed_mode != MODE_GENERAL:
            # Typed path is only valid for single non-null int64 keys.
            if key_count != 1 or not isinstance(key_vector0, Int64Vector):
                self._int64_typed_mode = MODE_GENERAL
                self._int64_typed_value_type = FAST_VALUE_UNKNOWN
                typed_mode = MODE_GENERAL
            else:
                key_int64_vector = <Int64Vector>key_vector0
                key_ptr = key_int64_vector.ptr
                key_data = <int64_t*>key_ptr.data
                key_nulls = <uint8_t*>key_ptr.null_bitmap

                # Any key bitmap (even if all-valid) routes through generic mode in v1.
                if key_nulls != NULL:
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
                            return
                        elif isinstance(value_vector0, Float64Vector):
                            value_f64_vector = <Float64Vector>value_vector0
                            value_ptr = value_f64_vector.ptr
                            value_nulls = <uint8_t*>value_ptr.null_bitmap
                            for row_idx in range(row_count):
                                key_u64 = <uint64_t>key_data[row_idx]
                                self._int64_typed_rows[key_u64] += 1
                                if value_nulls == NULL or ((value_nulls[row_idx >> 3] >> (row_idx & 7)) & 1):
                                    self._int64_typed_i64[key_u64] += 1
                            return
                    elif typed_value_type == FAST_VALUE_INT64 and isinstance(value_vector0, Int64Vector):
                        value_i64_vector = <Int64Vector>value_vector0
                        value_ptr = value_i64_vector.ptr
                        value_i64_data = <int64_t*>value_ptr.data
                        value_nulls = <uint8_t*>value_ptr.null_bitmap

                        if typed_mode == AGG_SUM:
                            for row_idx in range(row_count):
                                key_u64 = <uint64_t>key_data[row_idx]
                                self._int64_typed_rows[key_u64] += 1
                                if value_nulls == NULL or ((value_nulls[row_idx >> 3] >> (row_idx & 7)) & 1):
                                    self._int64_typed_i64[key_u64] += value_i64_data[row_idx]
                                    self._int64_typed_seen[key_u64] = 1
                            return
                        elif typed_mode == AGG_MIN:
                            for row_idx in range(row_count):
                                key_u64 = <uint64_t>key_data[row_idx]
                                self._int64_typed_rows[key_u64] += 1
                                if value_nulls == NULL or ((value_nulls[row_idx >> 3] >> (row_idx & 7)) & 1):
                                    value_i64 = value_i64_data[row_idx]
                                    if self._int64_typed_seen[key_u64] == 0 or value_i64 < self._int64_typed_i64[key_u64]:
                                        self._int64_typed_i64[key_u64] = value_i64
                                    self._int64_typed_seen[key_u64] = 1
                            return
                        elif typed_mode == AGG_MAX:
                            for row_idx in range(row_count):
                                key_u64 = <uint64_t>key_data[row_idx]
                                self._int64_typed_rows[key_u64] += 1
                                if value_nulls == NULL or ((value_nulls[row_idx >> 3] >> (row_idx & 7)) & 1):
                                    value_i64 = value_i64_data[row_idx]
                                    if self._int64_typed_seen[key_u64] == 0 or value_i64 > self._int64_typed_i64[key_u64]:
                                        self._int64_typed_i64[key_u64] = value_i64
                                    self._int64_typed_seen[key_u64] = 1
                            return
                        elif typed_mode == AGG_AVG:
                            for row_idx in range(row_count):
                                key_u64 = <uint64_t>key_data[row_idx]
                                self._int64_typed_rows[key_u64] += 1
                                if value_nulls == NULL or ((value_nulls[row_idx >> 3] >> (row_idx & 7)) & 1):
                                    self._int64_typed_f64[key_u64] += <double>value_i64_data[row_idx]
                                    self._int64_typed_count[key_u64] += 1
                            return
                        elif typed_mode == AGG_HASH_ONE:
                            for row_idx in range(row_count):
                                key_u64 = <uint64_t>key_data[row_idx]
                                self._int64_typed_rows[key_u64] += 1
                                if self._int64_typed_seen[key_u64] == 1:
                                    continue
                                if value_nulls == NULL or ((value_nulls[row_idx >> 3] >> (row_idx & 7)) & 1):
                                    self._int64_typed_i64[key_u64] = value_i64_data[row_idx]
                                    self._int64_typed_seen[key_u64] = 1
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
                            return
                        elif typed_mode == AGG_MIN:
                            for row_idx in range(row_count):
                                key_u64 = <uint64_t>key_data[row_idx]
                                self._int64_typed_rows[key_u64] += 1
                                if value_nulls == NULL or ((value_nulls[row_idx >> 3] >> (row_idx & 7)) & 1):
                                    value_f64 = value_f64_data[row_idx]
                                    if self._int64_typed_seen[key_u64] == 0 or value_f64 < self._int64_typed_f64[key_u64]:
                                        self._int64_typed_f64[key_u64] = value_f64
                                    self._int64_typed_seen[key_u64] = 1
                            return
                        elif typed_mode == AGG_MAX:
                            for row_idx in range(row_count):
                                key_u64 = <uint64_t>key_data[row_idx]
                                self._int64_typed_rows[key_u64] += 1
                                if value_nulls == NULL or ((value_nulls[row_idx >> 3] >> (row_idx & 7)) & 1):
                                    value_f64 = value_f64_data[row_idx]
                                    if self._int64_typed_seen[key_u64] == 0 or value_f64 > self._int64_typed_f64[key_u64]:
                                        self._int64_typed_f64[key_u64] = value_f64
                                    self._int64_typed_seen[key_u64] = 1
                            return
                        elif typed_mode == AGG_AVG:
                            for row_idx in range(row_count):
                                key_u64 = <uint64_t>key_data[row_idx]
                                self._int64_typed_rows[key_u64] += 1
                                if value_nulls == NULL or ((value_nulls[row_idx >> 3] >> (row_idx & 7)) & 1):
                                    self._int64_typed_f64[key_u64] += value_f64_data[row_idx]
                                    self._int64_typed_count[key_u64] += 1
                            return

                    # Unsupported typed value vector/type for this morsel;
                    # disable typed mode and continue through generic mode.
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

        if self._int64_count_star_mode:
            if (
                self._int64_count_star_counts.size() == 0
                and not self._int64_count_star_seen_null
            ):
                if self._group_by_columns:
                    return []

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
                if self._group_by_columns and not self._int64_count_distinct_seen_null_key:
                    return []

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
                if self._group_by_columns:
                    return []
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
        cdef object keys
        cdef object counts
        cdef int64_t[::1] key_view
        cdef int64_t[::1] count_view

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

        if not self._int64_count_distinct_mode:
            return None
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
