# cython: language_level=3
# cython: boundscheck=False
# cython: wraparound=False

"""Native zpp_bits record serialization for grouped aggregation keys."""

import struct

from cpython.bytes cimport PyBytes_FromStringAndSize
from libc.stddef cimport size_t
from libc.stdint cimport int64_t, uint8_t
from libcpp.string cimport string
from libcpp.vector cimport vector


cdef int KEY_MULTI_FIXED_INT = 1
cdef int KEY_MULTI_FIXED_DATE32 = 2
cdef int KEY_MULTI_FIXED_TIME32 = 3
cdef int KEY_MULTI_FIXED_TIME64 = 4
cdef int KEY_MULTI_FIXED_TIMESTAMP64 = 5
cdef int KEY_MULTI_ENCODED_STRING = 6


cdef extern from "zpp_key_codec.hpp" namespace "opteryx::zpp_key_codec":
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


cdef bint append_single_fixed_key_record(
    vector[uint8_t]& payload_bytes,
    vector[int64_t]& payload_offsets,
    int64_t value,
    int64_t valid_flag,
) except *:
    return append_single_fixed_record(payload_bytes, payload_offsets, value, valid_flag)


cdef bint append_single_encoded_key_record(
    vector[uint8_t]& payload_bytes,
    vector[int64_t]& payload_offsets,
    const char* data_ptr,
    Py_ssize_t data_len,
    int64_t valid_flag,
) except *:
    return append_single_encoded_record(
        payload_bytes, payload_offsets, data_ptr, <size_t> data_len, valid_flag
    )


cdef bint append_multi_key_record(
    vector[uint8_t]& payload_bytes,
    vector[int64_t]& payload_offsets,
    vector[int64_t]& fixed_values,
    vector[int64_t]& fixed_valids,
    vector[string]& encoded_values,
    vector[int64_t]& encoded_valids,
) except *:
    return append_multi_record(
        payload_bytes,
        payload_offsets,
        fixed_values,
        fixed_valids,
        encoded_values,
        encoded_valids,
    )


cdef bint decode_single_fixed_key_record(
    const vector[uint8_t]& payload_bytes,
    const vector[int64_t]& payload_offsets,
    Py_ssize_t state_index,
    int64_t* value_out,
    int64_t* valid_flag_out,
) except *:
    return decode_single_fixed_record(
        payload_bytes,
        payload_offsets,
        <size_t> state_index,
        value_out,
        valid_flag_out,
    )


cdef bint decode_single_encoded_key_record(
    const vector[uint8_t]& payload_bytes,
    const vector[int64_t]& payload_offsets,
    Py_ssize_t state_index,
    string& value_out,
    int64_t* valid_flag_out,
) except *:
    return decode_single_encoded_record(
        payload_bytes,
        payload_offsets,
        <size_t> state_index,
        value_out,
        valid_flag_out,
    )


cdef bint decode_multi_key_record(
    const vector[uint8_t]& payload_bytes,
    const vector[int64_t]& payload_offsets,
    Py_ssize_t state_index,
    vector[int64_t]& fixed_values,
    vector[int64_t]& fixed_valids,
    vector[string]& encoded_values,
    vector[int64_t]& encoded_valids,
) except *:
    return decode_multi_record(
        payload_bytes,
        payload_offsets,
        <size_t> state_index,
        fixed_values,
        fixed_valids,
        encoded_values,
        encoded_valids,
    )


cpdef tuple decode_single_payload_key(
    vector[uint8_t]& payload_bytes,
    vector[int64_t]& payload_offsets,
    Py_ssize_t state_index,
    int64_t single_key_kind,
):
    cdef int64_t valid_flag
    cdef int64_t key_value
    cdef string raw

    if single_key_kind == KEY_MULTI_ENCODED_STRING:
        if not decode_single_encoded_key_record(
            payload_bytes, payload_offsets, state_index, raw, &valid_flag
        ):
            raise RuntimeError("failed to decode encoded key payload")
        if valid_flag == 0:
            return None, 0
        raw_bytes = <bytes> PyBytes_FromStringAndSize(raw.data(), raw.size())
        return raw_bytes.decode("utf-8"), 1

    if not decode_single_fixed_key_record(
        payload_bytes, payload_offsets, state_index, &key_value, &valid_flag
    ):
        raise RuntimeError("failed to decode fixed key payload")
    return (key_value if valid_flag != 0 else None), (1 if valid_flag != 0 else 0)


cpdef tuple decode_multi_payload_keys(
    vector[uint8_t]& payload_bytes,
    vector[int64_t]& payload_offsets,
    list multi_key_kinds,
    Py_ssize_t state_index,
):
    cdef Py_ssize_t key_idx
    cdef int64_t key_kind
    cdef bytes raw_bytes
    cdef Py_ssize_t fixed_idx = 0
    cdef Py_ssize_t encoded_idx = 0
    cdef vector[int64_t] fixed_values
    cdef vector[int64_t] fixed_valids
    cdef vector[string] encoded_values
    cdef vector[int64_t] encoded_valids
    cdef list key_values = []
    cdef list key_valids = []

    if not decode_multi_key_record(
        payload_bytes,
        payload_offsets,
        state_index,
        fixed_values,
        fixed_valids,
        encoded_values,
        encoded_valids,
    ):
        raise RuntimeError("failed to decode multi-key payload")

    for key_idx in range(len(multi_key_kinds)):
        key_kind = <int64_t> multi_key_kinds[key_idx]
        if (
            key_kind == KEY_MULTI_FIXED_INT
            or key_kind == KEY_MULTI_FIXED_DATE32
            or key_kind == KEY_MULTI_FIXED_TIME32
            or key_kind == KEY_MULTI_FIXED_TIME64
            or key_kind == KEY_MULTI_FIXED_TIMESTAMP64
        ):
            if fixed_idx >= <Py_ssize_t> fixed_values.size():
                raise RuntimeError("decoded fixed key payload shorter than key schema")
            key_valids.append(1 if fixed_valids[fixed_idx] != 0 else 0)
            key_values.append(fixed_values[fixed_idx] if fixed_valids[fixed_idx] != 0 else None)
            fixed_idx += 1
        else:
            if encoded_idx >= <Py_ssize_t> encoded_values.size():
                raise RuntimeError("decoded encoded key payload shorter than key schema")
            key_valids.append(1 if encoded_valids[encoded_idx] != 0 else 0)
            if encoded_valids[encoded_idx] == 0:
                key_values.append(None)
            else:
                raw_bytes = <bytes> PyBytes_FromStringAndSize(
                    encoded_values[encoded_idx].data(),
                    encoded_values[encoded_idx].size(),
                )
                key_values.append(raw_bytes.decode("utf-8"))
            encoded_idx += 1

    return tuple(key_values), tuple(key_valids)


cpdef bytes serialize_key_components(list components):
    """
    Serialize a list of key components to bytes.

    Each component can be: int, float, str, bytes, None, or date-like objects.
    Returns a bytes object suitable for use as a dict key.
    """
    serialized = []

    for component in components:
        if component is None:
            serialized.append(bytes([0]))
        elif isinstance(component, bool):
            serialized.append(bytes([3]))
            serialized.append(bytes([1 if component else 0]))
        elif isinstance(component, int):
            serialized.append(bytes([1]))
            serialized.append(struct.pack("<q", component))
        elif isinstance(component, float):
            serialized.append(bytes([2]))
            serialized.append(struct.pack("<d", component))
        elif isinstance(component, (str, bytes)):
            serialized.append(bytes([4]))
            if isinstance(component, str):
                b = component.encode("utf-8")
            else:
                b = component
            serialized.append(struct.pack("<I", len(b)))
            serialized.append(b)
        else:
            serialized.append(bytes([4]))
            b = str(component).encode("utf-8")
            serialized.append(struct.pack("<I", len(b)))
            serialized.append(b)

    result = struct.pack("<I", len(components))
    for component_bytes in serialized:
        result += component_bytes
    return result


cpdef list deserialize_key_components(bytes data):
    """
    Deserialize key components from bytes back to Python objects.
    """
    cdef Py_ssize_t pos = 0
    cdef list result = []
    cdef Py_ssize_t count
    cdef Py_ssize_t length
    cdef int type_tag
    cdef object value

    if len(data) < 4:
        raise ValueError("Invalid serialized key: too short")

    count = struct.unpack("<I", data[0:4])[0]
    pos = 4

    for _ in range(count):
        if pos >= len(data):
            raise ValueError("Invalid serialized key: truncated")

        type_tag = data[pos]
        pos += 1

        if type_tag == 0:
            result.append(None)
        elif type_tag == 1:
            if pos + 8 > len(data):
                raise ValueError("Invalid serialized key: truncated int64")
            value = struct.unpack("<q", data[pos:pos + 8])[0]
            result.append(int(value))
            pos += 8
        elif type_tag == 2:
            if pos + 8 > len(data):
                raise ValueError("Invalid serialized key: truncated float64")
            value = struct.unpack("<d", data[pos:pos + 8])[0]
            result.append(float(value))
            pos += 8
        elif type_tag == 3:
            if pos + 1 > len(data):
                raise ValueError("Invalid serialized key: truncated bool")
            result.append(bool(data[pos]))
            pos += 1
        elif type_tag == 4:
            if pos + 4 > len(data):
                raise ValueError("Invalid serialized key: truncated length")
            length = struct.unpack("<I", data[pos:pos + 4])[0]
            pos += 4
            if pos + length > len(data):
                raise ValueError("Invalid serialized key: truncated data")
            result.append(bytes(data[pos:pos + length]))
            pos += length
        else:
            raise ValueError(f"Unknown type tag in serialized key: {type_tag}")

    return result
