# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False

"""Vectorized Base64 encoding/decoding using alantsd.base64 Cython module."""

from opteryx.compiled.draken.vectors.string_vector cimport StringVector
from opteryx.compiled.draken.vectors import string_vector as string_vector_module
from opteryx.compiled.draken.core.buffers cimport DrakenVarBuffer
from opteryx.third_party.alantsd.base64 import encode as b64_encode, decode as b64_decode


cpdef StringVector vector_base64_encode(StringVector data):
    """Vectorized Base64 encoding.

    Parameters
    ----------
    data : StringVector
        Input strings to encode

    Returns
    -------
    StringVector
        Base64-encoded strings (NULLs preserved)
    """
    cdef DrakenVarBuffer* ptr = data.ptr
    cdef Py_ssize_t n = ptr.length
    cdef Py_ssize_t i
    cdef int32_t start, end
    cdef uint8_t* null_bm
    cdef bytes input_bytes, encoded_bytes

    builder = string_vector_module.StringVectorBuilder.with_estimate(n, 32)

    if data._has_const:
        if data._const_is_null:
            for i in range(n):
                builder.append_null()
        else:
            const_val = data._const_value
            input_bytes = bytes(const_val.data[:const_val.length])
            encoded_bytes = b64_encode(input_bytes)
            for i in range(n):
                builder.append(encoded_bytes)
    else:
        null_bm = ptr.null_bitmap
        for i in range(n):
            if null_bm != NULL and not ((null_bm[i >> 3] >> (i & 7)) & 1):
                builder.append_null()
            else:
                start = ptr.offsets[i]
                end = ptr.offsets[i + 1]
                input_bytes = bytes(ptr.data[start:end])
                encoded_bytes = b64_encode(input_bytes)
                builder.append(encoded_bytes)

    return builder.finish()


cpdef StringVector vector_base64_decode(StringVector data):
    """Vectorized Base64 decoding.

    Parameters
    ----------
    data : StringVector
        Base64-encoded strings to decode

    Returns
    -------
    StringVector
        Decoded strings (NULLs preserved)
    """
    cdef DrakenVarBuffer* ptr = data.ptr
    cdef Py_ssize_t n = ptr.length
    cdef Py_ssize_t i
    cdef int32_t start, end
    cdef uint8_t* null_bm
    cdef bytes input_bytes, decoded_bytes

    builder = string_vector_module.StringVectorBuilder.with_estimate(n, 32)

    if data._has_const:
        if data._const_is_null:
            for i in range(n):
                builder.append_null()
        else:
            const_val = data._const_value
            input_bytes = bytes(const_val.data[:const_val.length])
            decoded_bytes = b64_decode(input_bytes)
            for i in range(n):
                builder.append(decoded_bytes)
    else:
        null_bm = ptr.null_bitmap
        for i in range(n):
            if null_bm != NULL and not ((null_bm[i >> 3] >> (i & 7)) & 1):
                builder.append_null()
            else:
                start = ptr.offsets[i]
                end = ptr.offsets[i + 1]
                input_bytes = bytes(ptr.data[start:end])
                decoded_bytes = b64_decode(input_bytes)
                builder.append(decoded_bytes)

    return builder.finish()
