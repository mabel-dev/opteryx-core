# cython: language_level=3
# cython: boundscheck=False
# cython: wraparound=False

# Moved from opteryx/compiled/aggregations/key_codec.pyx
# Group-key record serialization — thin Cython shim over group_key_codec.hpp

from cpython.bytes cimport PyBytes_FromStringAndSize
from libc.stddef cimport size_t
from libc.stdint cimport int32_t, int64_t, uint8_t
from libcpp.string cimport string
from libcpp.vector cimport vector


cdef int KEY_MULTI_FIXED_INT = 1
cdef int KEY_MULTI_FIXED_DATE32 = 2
cdef int KEY_MULTI_FIXED_TIME32 = 3
cdef int KEY_MULTI_FIXED_TIME64 = 4
cdef int KEY_MULTI_FIXED_TIMESTAMP64 = 5
cdef int KEY_MULTI_ENCODED_STRING = 6


cdef extern from "group_key_codec.hpp" namespace "opteryx::group_key_codec":
    bint append_single_fixed_record(
        vector[uint8_t]& payload_bytes,
        vector[int64_t]& payload_offsets,
        int64_t value,
        int64_t valid_flag,
    ) except +
    bint append_single_encoded_record(
        vector[uint8_t]& payload_bytes,
        vector[int64_t]& payload_offsets,
        const char* data_ptr,
        size_t data_len,
        int64_t valid_flag,
    ) except +
    bint append_multi_record(
        vector[uint8_t]& payload_bytes,
        vector[int64_t]& payload_offsets,
        const vector[int64_t]& fixed_values,
        const vector[int64_t]& fixed_valids,
        const vector[string]& encoded_values,
        const vector[int64_t]& encoded_valids,
    ) except +
    bint decode_single_fixed_record(
        const vector[uint8_t]& payload_bytes,
        const vector[int64_t]& payload_offsets,
        size_t state_index,
        int64_t* value_out,
        int64_t* valid_flag_out,
    ) except +
    bint decode_single_encoded_record(
        const vector[uint8_t]& payload_bytes,
        const vector[int64_t]& payload_offsets,
        size_t state_index,
        string& value_out,
        int64_t* valid_flag_out,
    ) except +
    bint decode_multi_record(
        const vector[uint8_t]& payload_bytes,
        const vector[int64_t]& payload_offsets,
        size_t state_index,
        vector[int64_t]& fixed_values_out,
        vector[int64_t]& fixed_valids_out,
        vector[string]& encoded_values_out,
        vector[int64_t]& encoded_valids_out,
    ) except +


cdef inline bint _append_single_fixed_key_record(
    vector[uint8_t]& payload_bytes,
    vector[int64_t]& payload_offsets,
    int64_t value,
    int64_t valid_flag,
) except *:
    return append_single_fixed_record(payload_bytes, payload_offsets, value, valid_flag)


cdef inline bint _append_single_encoded_key_record(
    vector[uint8_t]& payload_bytes,
    vector[int64_t]& payload_offsets,
    const char* data_ptr,
    Py_ssize_t data_len,
    int64_t valid_flag,
) except *:
    return append_single_encoded_record(
        payload_bytes, payload_offsets, data_ptr, <size_t>data_len, valid_flag
    )


cdef inline bint _append_multi_key_record(
    vector[uint8_t]& payload_bytes,
    vector[int64_t]& payload_offsets,
    vector[int64_t]& fixed_values,
    vector[int64_t]& fixed_valids,
    vector[string]& encoded_values,
    vector[int64_t]& encoded_valids,
) except *:
    return append_multi_record(
        payload_bytes, payload_offsets,
        fixed_values, fixed_valids,
        encoded_values, encoded_valids,
    )


cdef inline bint _decode_single_fixed_key_record(
    const vector[uint8_t]& payload_bytes,
    const vector[int64_t]& payload_offsets,
    Py_ssize_t state_index,
    int64_t* value_out,
    int64_t* valid_flag_out,
) except *:
    return decode_single_fixed_record(
        payload_bytes, payload_offsets, <size_t>state_index, value_out, valid_flag_out
    )


cdef inline bint _decode_single_encoded_key_record(
    const vector[uint8_t]& payload_bytes,
    const vector[int64_t]& payload_offsets,
    Py_ssize_t state_index,
    string& value_out,
    int64_t* valid_flag_out,
) except *:
    return decode_single_encoded_record(
        payload_bytes, payload_offsets, <size_t>state_index, value_out, valid_flag_out
    )


cdef inline bint _decode_multi_key_record(
    const vector[uint8_t]& payload_bytes,
    const vector[int64_t]& payload_offsets,
    Py_ssize_t state_index,
    vector[int64_t]& fixed_values,
    vector[int64_t]& fixed_valids,
    vector[string]& encoded_values,
    vector[int64_t]& encoded_valids,
) except *:
    return decode_multi_record(
        payload_bytes, payload_offsets, <size_t>state_index,
        fixed_values, fixed_valids, encoded_values, encoded_valids,
    )
