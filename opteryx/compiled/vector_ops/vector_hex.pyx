# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False
# cython: optimize.use_switch=True
# cython: optimize.unpack_method_calls=True

"""Vectorized HEX (base16) encoding/decoding using mabel.base16 Cython module."""

from draken.vectors.string_vector cimport StringVector, from_packed_dict
from draken.vectors import string_vector as string_vector_module
from draken.core.buffers cimport DrakenVarBuffer, DRAKEN_ENCODING_DICTIONARY
from opteryx.third_party.mabel.base16 import encode as b16_encode, decode as b16_decode


cpdef StringVector vector_hex_encode(StringVector data):
    cdef DrakenVarBuffer* ptr = data.ptr
    cdef Py_ssize_t n = ptr.length
    cdef Py_ssize_t i
    cdef int32_t start, end
    cdef uint8_t* null_bm
    cdef bytes input_bytes, encoded_bytes
    cdef DrakenVarBuffer* dict_ptr
    cdef Py_ssize_t dict_size
    cdef DrakenVarBuffer* ndp

    builder = string_vector_module.StringVectorBuilder.with_estimate(n, 32)

    if data._has_const:
        if data._const_is_null:
            for i in range(n):
                builder.append_null()
        else:
            const_val = data._const_value
            input_bytes = bytes(const_val.data[:const_val.length])
            encoded_bytes = b16_encode(input_bytes)
            for i in range(n):
                builder.append(encoded_bytes)
        return builder.finish()

    if data._encoding == DRAKEN_ENCODING_DICTIONARY:
        dict_ptr = data._dict_values
        dict_size = dict_ptr.length
        dict_builder = string_vector_module.StringVectorBuilder.with_estimate(dict_size, 32)
        for i in range(dict_size):
            start = dict_ptr.offsets[i]
            end = dict_ptr.offsets[i + 1]
            input_bytes = bytes(dict_ptr.data[start:end])
            dict_builder.append(b16_encode(input_bytes))
        new_dict_sv = dict_builder.finish()
        ndp = (<StringVector>new_dict_sv).ptr
        return from_packed_dict(
            data._dict_codes, data._dict_code_width, n,
            ndp.offsets, <const uint8_t*>ndp.data, dict_size,
            data._dict_accessor.row_nulls,
        )

    null_bm = ptr.null_bitmap
    for i in range(n):
        if null_bm != NULL and not ((null_bm[i >> 3] >> (i & 7)) & 1):
            builder.append_null()
        else:
            start = ptr.offsets[i]
            end = ptr.offsets[i + 1]
            input_bytes = bytes(ptr.data[start:end])
            builder.append(b16_encode(input_bytes))

    return builder.finish()


cpdef StringVector vector_hex_decode(StringVector data):
    cdef DrakenVarBuffer* ptr = data.ptr
    cdef Py_ssize_t n = ptr.length
    cdef Py_ssize_t i
    cdef int32_t start, end
    cdef uint8_t* null_bm
    cdef bytes input_bytes, decoded_bytes
    cdef DrakenVarBuffer* dict_ptr
    cdef Py_ssize_t dict_size
    cdef DrakenVarBuffer* ndp

    builder = string_vector_module.StringVectorBuilder.with_estimate(n, 32)

    if data._has_const:
        if data._const_is_null:
            for i in range(n):
                builder.append_null()
        else:
            const_val = data._const_value
            input_bytes = bytes(const_val.data[:const_val.length])
            decoded_bytes = b16_decode(input_bytes)
            for i in range(n):
                builder.append(decoded_bytes)
        return builder.finish()

    if data._encoding == DRAKEN_ENCODING_DICTIONARY:
        dict_ptr = data._dict_values
        dict_size = dict_ptr.length
        dict_builder = string_vector_module.StringVectorBuilder.with_estimate(dict_size, 32)
        for i in range(dict_size):
            start = dict_ptr.offsets[i]
            end = dict_ptr.offsets[i + 1]
            input_bytes = bytes(dict_ptr.data[start:end])
            dict_builder.append(b16_decode(input_bytes))
        new_dict_sv = dict_builder.finish()
        ndp = (<StringVector>new_dict_sv).ptr
        return from_packed_dict(
            data._dict_codes, data._dict_code_width, n,
            ndp.offsets, <const uint8_t*>ndp.data, dict_size,
            data._dict_accessor.row_nulls,
        )

    null_bm = ptr.null_bitmap
    for i in range(n):
        if null_bm != NULL and not ((null_bm[i >> 3] >> (i & 7)) & 1):
            builder.append_null()
        else:
            start = ptr.offsets[i]
            end = ptr.offsets[i + 1]
            input_bytes = bytes(ptr.data[start:end])
            builder.append(b16_decode(input_bytes))

    return builder.finish()
