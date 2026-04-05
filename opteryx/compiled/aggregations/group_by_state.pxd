from libc.stdint cimport int64_t, uint64_t


cdef inline Py_ssize_t _state_count(object self) noexcept

cdef inline void _initialize_per_aggregate_states(object self) except *
cdef inline void _grow_per_aggregate_states(object self, Py_ssize_t additional_states) except *
cdef inline void _assert_per_aggregate_state_sizes(object self) except *

cdef inline void _bloom_record_new_state(object self, uint64_t row_hash) noexcept
cdef inline bint _bloom_might_contain(object self, uint64_t h) noexcept
cdef void _maybe_init_bloom(object self) except *

cdef inline int64_t _insert_fixed_state_known_miss(
    object self,
    uint64_t row_hash,
    int64_t key_value,
    int64_t key_valid_flag,
) except *

cdef inline int64_t _insert_encoded_state_known_miss(
    object self,
    uint64_t row_hash,
    const char* data_ptr,
    Py_ssize_t data_len,
    int64_t key_valid_flag,
) except *

cdef inline int64_t _insert_multi_encoded_state_known_miss(
    object self,
    uint64_t row_hash,
    list key_vectors,
    Py_ssize_t row_idx,
) except *

cdef inline int64_t _find_or_insert_state(
    object self,
    uint64_t row_hash,
    int64_t key_value,
    int64_t key_valid_flag,
) except *

cdef inline int64_t _find_or_insert_encoded_state(
    object self,
    uint64_t row_hash,
    const char* data_ptr,
    Py_ssize_t data_len,
    int64_t key_valid_flag,
) except *

cdef inline int64_t _find_or_insert_multi_fixed_state_from_vectors(
    object self,
    uint64_t row_hash,
    list key_vectors,
    Py_ssize_t row_idx,
) except *

cdef inline int64_t _find_or_insert_multi_encoded_state(
    object self,
    uint64_t row_hash,
    list key_vectors,
    Py_ssize_t row_idx,
) except *
