from libc.stdint cimport int64_t

cdef inline int64_t _read_dictionary_fixed_key(
    object self,
    object key_vector,
    Py_ssize_t row_idx,
    int64_t* key_valid_flag,
) except *

cdef inline void _append_single_encoded_key(
    object self,
    const char* data_ptr,
    Py_ssize_t data_len,
    int64_t valid_flag,
) except *

cdef inline void _append_multi_encoded_key(
    object self,
    Py_ssize_t key_idx,
    const char* data_ptr,
    Py_ssize_t data_len,
    int64_t valid_flag,
) except *

cdef inline int64_t _extract_stringlike_key(
    object self,
    object key_vector,
    Py_ssize_t row_idx,
    const char** data_ptr,
    Py_ssize_t* data_len,
) except *

cdef inline void _append_single_payload_key(
    object self,
    const char* data_ptr,
    Py_ssize_t data_len,
    int64_t key_valid_flag,
) except *

cdef inline void _append_single_fixed_payload_key(
    object self,
    int64_t key_value,
    int64_t key_valid_flag,
) except *

cdef inline void _append_multi_fixed_payload_key_from_vectors(
    object self,
    list key_vectors,
    Py_ssize_t row_idx,
) except *

cdef inline void _append_multi_payload_key(
    object self,
    list key_vectors,
    Py_ssize_t row_idx,
) except *
