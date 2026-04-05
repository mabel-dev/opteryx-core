# cython: language_level=3
# cython: boundscheck=False
# cython: wraparound=False
# cython: nonecheck=False
# cython: cdivision=True
# cython: infer_types=True

from libc.stddef cimport size_t
from libc.stdint cimport int32_t, int64_t, uint8_t, uint32_t, uint64_t

from opteryx.compiled.structures.bloom_filter cimport BloomFilter
from opteryx.compiled.structures.carchar_set cimport CarcharSetWrapper

from opteryx.compiled.aggregations.group_by_key_helpers cimport _append_multi_fixed_payload_key_from_vectors
from opteryx.compiled.aggregations.group_by_key_helpers cimport _append_multi_payload_key
from opteryx.compiled.aggregations.group_by_key_helpers cimport _append_single_payload_key
from opteryx.compiled.aggregations.group_by_telemetry cimport record_groupby_key_store_bytes


cdef int MODE_CARCHAR = 2
cdef int AGG_COUNT_DISTINCT = 7


cdef inline Py_ssize_t _state_count(object self) noexcept:
    if self._mode == MODE_CARCHAR and self._key_payload_offsets.size() > 0:
        return <Py_ssize_t>(self._key_payload_offsets.size() - 1)
    return <Py_ssize_t> self._group_key_values.size()


cdef inline Py_ssize_t _multi_offset(object self, int64_t state_index, Py_ssize_t agg_idx) noexcept:
    return <Py_ssize_t>state_index * self._multi_agg_count + agg_idx


cdef inline void _initialize_per_aggregate_states(object self) except *:
    cdef Py_ssize_t agg_idx

    self._object_state.append(None)
    self._object_state_starts.push_back(0)
    self._object_state_lengths.push_back(0)

    if self._agg_mode == AGG_COUNT_DISTINCT:
        self._distinct_sets.append(CarcharSetWrapper())

    if self._multi_agg_count > 0:
        for agg_idx in range(self._multi_agg_count):
            self._multi_counts.push_back(0)
            self._multi_i64_state.push_back(0)
            self._multi_f64_state.push_back(0.0)
            self._multi_seen.push_back(0)
            self._multi_avg_sums.push_back(0.0)
            self._multi_avg_counts.push_back(0)
            self._multi_object_state.append(None)
            self._multi_object_state_starts.push_back(0)
            self._multi_object_state_lengths.push_back(0)
            if self._multi_agg_modes[agg_idx] == AGG_COUNT_DISTINCT:
                self._multi_distinct_sets.append(CarcharSetWrapper())
            else:
                self._multi_distinct_sets.append(None)
    else:
        self._counts.push_back(0)
        self._i64_state.push_back(0)
        self._f64_state.push_back(0.0)
        self._seen.push_back(0)
        self._avg_sums.push_back(0.0)
        self._avg_counts.push_back(0)


cdef inline void _grow_per_aggregate_states(object self, Py_ssize_t additional_states) except *:
    cdef Py_ssize_t idx

    if additional_states <= 0:
        return

    for idx in range(additional_states):
        _initialize_per_aggregate_states(self)


cdef inline void _assert_per_aggregate_state_sizes(object self) except *:
    cdef Py_ssize_t state_count = _state_count(self)
    cdef Py_ssize_t expected_multi = state_count * self._multi_agg_count

    if <Py_ssize_t>self._object_state_starts.size() != state_count:
        raise RuntimeError("group-by object state starts size mismatch")
    if <Py_ssize_t>self._object_state_lengths.size() != state_count:
        raise RuntimeError("group-by object state lengths size mismatch")
    if len(self._object_state) != state_count:
        raise RuntimeError("group-by object state list size mismatch")

    if self._multi_agg_count > 0:
        if <Py_ssize_t>self._multi_counts.size() != expected_multi:
            raise RuntimeError("group-by multi counts size mismatch")
        if <Py_ssize_t>self._multi_i64_state.size() != expected_multi:
            raise RuntimeError("group-by multi int64 state size mismatch")
        if <Py_ssize_t>self._multi_f64_state.size() != expected_multi:
            raise RuntimeError("group-by multi float64 state size mismatch")
        if <Py_ssize_t>self._multi_seen.size() != expected_multi:
            raise RuntimeError("group-by multi seen size mismatch")
        if <Py_ssize_t>self._multi_avg_sums.size() != expected_multi:
            raise RuntimeError("group-by multi avg sums size mismatch")
        if <Py_ssize_t>self._multi_avg_counts.size() != expected_multi:
            raise RuntimeError("group-by multi avg counts size mismatch")
        if <Py_ssize_t>self._multi_object_state_starts.size() != expected_multi:
            raise RuntimeError("group-by multi object state starts size mismatch")
        if <Py_ssize_t>self._multi_object_state_lengths.size() != expected_multi:
            raise RuntimeError("group-by multi object state lengths size mismatch")
        if len(self._multi_object_state) != expected_multi:
            raise RuntimeError("group-by multi object state list size mismatch")
        if len(self._multi_distinct_sets) != expected_multi:
            raise RuntimeError("group-by multi distinct sets size mismatch")
    else:
        if <Py_ssize_t>self._counts.size() != state_count:
            raise RuntimeError("group-by counts size mismatch")
        if <Py_ssize_t>self._i64_state.size() != state_count:
            raise RuntimeError("group-by int64 state size mismatch")
        if <Py_ssize_t>self._f64_state.size() != state_count:
            raise RuntimeError("group-by float64 state size mismatch")
        if <Py_ssize_t>self._seen.size() != state_count:
            raise RuntimeError("group-by seen size mismatch")
        if <Py_ssize_t>self._avg_sums.size() != state_count:
            raise RuntimeError("group-by avg sums size mismatch")
        if <Py_ssize_t>self._avg_counts.size() != state_count:
            raise RuntimeError("group-by avg counts size mismatch")
        if self._agg_mode == AGG_COUNT_DISTINCT and len(self._distinct_sets) != state_count:
            raise RuntimeError("group-by distinct sets size mismatch")


cdef inline object _get_per_aggregate_state(object self, int64_t state_index, Py_ssize_t agg_idx):
    cdef Py_ssize_t offset

    if self._multi_agg_count > 0:
        offset = _multi_offset(self, state_index, agg_idx)
        return (
            self._multi_counts[offset],
            self._multi_i64_state[offset],
            self._multi_f64_state[offset],
            self._multi_seen[offset],
            self._multi_avg_sums[offset],
            self._multi_avg_counts[offset],
            self._multi_object_state[offset],
            self._multi_distinct_sets[offset],
        )

    return (
        self._counts[state_index],
        self._i64_state[state_index],
        self._f64_state[state_index],
        self._seen[state_index],
        self._avg_sums[state_index],
        self._avg_counts[state_index],
        self._object_state[state_index],
        self._distinct_sets[state_index] if self._agg_mode == AGG_COUNT_DISTINCT else None,
    )


cdef inline void _bloom_record_new_state(object self, uint64_t row_hash) noexcept:
    if self._use_bloom:
        self._groupby_bloom._add(row_hash)
    else:
        self._bloom_hashes.push_back(row_hash)


cdef inline bint _bloom_might_contain(object self, uint64_t h) noexcept:
    if not self._use_bloom:
        return True
    return self._groupby_bloom._possibly_contains_fast(h)


cdef void _maybe_init_bloom(object self) except *:
    cdef Py_ssize_t state_count = _state_count(self)
    cdef size_t estimated_total
    cdef size_t i

    if self._use_bloom or state_count == 0:
        return

    estimated_total = min(<size_t>state_count * 200, <size_t>200_000_000)
    self._groupby_bloom = BloomFilter(<uint32_t>estimated_total)
    for i in range(self._bloom_hashes.size()):
        self._groupby_bloom._add(self._bloom_hashes[i])
    self._bloom_hashes.clear()
    self._use_bloom = True


cdef inline int64_t _insert_fixed_state_known_miss(
    object self,
    uint64_t row_hash,
    int64_t key_value,
    int64_t key_valid_flag,
) except *:
    cdef int64_t payload_ref = <int64_t> _state_count(self)
    cdef size_t key_store_bytes

    self._index.insert_new(row_hash, payload_ref)
    self._group_key_values.push_back(key_value)
    self._group_key_valid.push_back(key_valid_flag)

    _initialize_per_aggregate_states(self)

    key_store_bytes = <size_t> self._key_payload_bytes.size()
    record_groupby_key_store_bytes(self, key_store_bytes)
    if (
        self._key_store_limit_bytes is not None
        and key_store_bytes > <size_t> self._key_store_limit_bytes
    ):
        raise MemoryError("group key store exceeded configured limit")

    _bloom_record_new_state(self, row_hash)
    return payload_ref


cdef inline int64_t _insert_encoded_state_known_miss(
    object self,
    uint64_t row_hash,
    const char* data_ptr,
    Py_ssize_t data_len,
    int64_t key_valid_flag,
) except *:
    cdef int64_t payload_ref = <int64_t> _state_count(self)
    cdef size_t key_store_bytes

    self._index.insert_new(row_hash, payload_ref)
    _append_single_payload_key(self, data_ptr, data_len, key_valid_flag)

    _initialize_per_aggregate_states(self)

    key_store_bytes = <size_t> self._key_payload_bytes.size()
    record_groupby_key_store_bytes(self, key_store_bytes)
    if (
        self._key_store_limit_bytes is not None
        and key_store_bytes > <size_t> self._key_store_limit_bytes
    ):
        raise MemoryError("group key store exceeded configured limit")

    _bloom_record_new_state(self, row_hash)
    return payload_ref


cdef inline int64_t _insert_multi_encoded_state_known_miss(
    object self,
    uint64_t row_hash,
    list key_vectors,
    Py_ssize_t row_idx,
) except *:
    cdef int64_t payload_ref = <int64_t> _state_count(self)
    cdef size_t key_store_bytes

    self._index.insert_new(row_hash, payload_ref)
    _append_multi_payload_key(self, key_vectors, row_idx)

    key_store_bytes = <size_t> self._key_payload_bytes.size()
    record_groupby_key_store_bytes(self, key_store_bytes)

    _initialize_per_aggregate_states(self)

    if (
        self._key_store_limit_bytes is not None
        and key_store_bytes > <size_t> self._key_store_limit_bytes
    ):
        raise MemoryError("group key store exceeded configured limit")

    _bloom_record_new_state(self, row_hash)
    return payload_ref


cdef inline int64_t _find_or_insert_state(
    object self,
    uint64_t row_hash,
    int64_t key_value,
    int64_t key_valid_flag,
) except *:
    cdef int64_t payload_ref = -1

    if _bloom_might_contain(self, row_hash) and self._index.lookup_fast(row_hash, payload_ref):
        return payload_ref

    return _insert_fixed_state_known_miss(self, row_hash, key_value, key_valid_flag)


cdef inline int64_t _find_or_insert_encoded_state(
    object self,
    uint64_t row_hash,
    const char* data_ptr,
    Py_ssize_t data_len,
    int64_t key_valid_flag,
) except *:
    cdef int64_t payload_ref = -1

    if _bloom_might_contain(self, row_hash) and self._index.lookup_fast(row_hash, payload_ref):
        return payload_ref

    return _insert_encoded_state_known_miss(self, row_hash, data_ptr, data_len, key_valid_flag)


cdef inline int64_t _find_or_insert_multi_fixed_state_from_vectors(
    object self,
    uint64_t row_hash,
    list key_vectors,
    Py_ssize_t row_idx,
) except *:
    cdef int64_t payload_ref = -1
    cdef size_t key_store_bytes

    if _bloom_might_contain(self, row_hash) and self._index.lookup_fast(row_hash, payload_ref):
        return payload_ref

    payload_ref = <int64_t> _state_count(self)
    self._index.insert_new(row_hash, payload_ref)
    _append_multi_fixed_payload_key_from_vectors(self, key_vectors, row_idx)

    _initialize_per_aggregate_states(self)

    key_store_bytes = <size_t> self._key_payload_bytes.size()
    record_groupby_key_store_bytes(self, key_store_bytes)
    if (
        self._key_store_limit_bytes is not None
        and key_store_bytes > <size_t> self._key_store_limit_bytes
    ):
        raise MemoryError("group key store exceeded configured limit")

    _bloom_record_new_state(self, row_hash)
    return payload_ref


cdef inline int64_t _find_or_insert_multi_encoded_state(
    object self,
    uint64_t row_hash,
    list key_vectors,
    Py_ssize_t row_idx,
) except *:
    cdef int64_t payload_ref = -1

    if _bloom_might_contain(self, row_hash) and self._index.lookup_fast(row_hash, payload_ref):
        return payload_ref

    return _insert_multi_encoded_state_known_miss(self, row_hash, key_vectors, row_idx)
