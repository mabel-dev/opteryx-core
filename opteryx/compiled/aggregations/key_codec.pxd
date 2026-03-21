# cython: language_level=3

from libc.stdint cimport int64_t, uint8_t
from libcpp.vector cimport vector
from libcpp.string cimport string

cdef bint append_single_fixed_key_record(
    vector[uint8_t]& payload_bytes,
    vector[int64_t]& payload_offsets,
    int64_t value,
    int64_t valid_flag,
) except *
cdef bint append_single_encoded_key_record(
    vector[uint8_t]& payload_bytes,
    vector[int64_t]& payload_offsets,
    const char* data_ptr,
    Py_ssize_t data_len,
    int64_t valid_flag,
) except *
cdef bint append_multi_key_record(
    vector[uint8_t]& payload_bytes,
    vector[int64_t]& payload_offsets,
    vector[int64_t]& fixed_values,
    vector[int64_t]& fixed_valids,
    vector[string]& encoded_values,
    vector[int64_t]& encoded_valids,
) except *
cdef bint decode_single_fixed_key_record(
    const vector[uint8_t]& payload_bytes,
    const vector[int64_t]& payload_offsets,
    Py_ssize_t state_index,
    int64_t* value_out,
    int64_t* valid_flag_out,
) except *
cdef bint decode_single_encoded_key_record(
    const vector[uint8_t]& payload_bytes,
    const vector[int64_t]& payload_offsets,
    Py_ssize_t state_index,
    string& value_out,
    int64_t* valid_flag_out,
) except *
cdef bint decode_multi_key_record(
    const vector[uint8_t]& payload_bytes,
    const vector[int64_t]& payload_offsets,
    Py_ssize_t state_index,
    vector[int64_t]& fixed_values,
    vector[int64_t]& fixed_valids,
    vector[string]& encoded_values,
    vector[int64_t]& encoded_valids,
) except *
cpdef tuple decode_single_payload_key(
    vector[uint8_t]& payload_bytes,
    vector[int64_t]& payload_offsets,
    Py_ssize_t state_index,
    int64_t single_key_kind,
)
cpdef tuple decode_multi_payload_keys(
    vector[uint8_t]& payload_bytes,
    vector[int64_t]& payload_offsets,
    list multi_key_kinds,
    Py_ssize_t state_index,
)
