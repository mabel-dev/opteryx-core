# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False
# cython: optimize.use_switch=True
# cython: optimize.unpack_method_calls=True

from libc.stdint cimport int32_t, uint8_t

from draken.vectors.string_vector cimport StringVector, from_packed_dict
from draken.vectors import string_vector as string_vector_module
from draken.core.buffers cimport DrakenVarBuffer


cpdef StringVector vector_reverse(StringVector vec):
    """Reverse each string element in a StringVector (Unicode codepoint-aware)."""
    cdef Py_ssize_t n = vec.ptr.length
    cdef Py_ssize_t i
    cdef int32_t start, end
    cdef bytes raw
    cdef str text
    cdef bytes result
    cdef DrakenVarBuffer* ptr
    cdef uint8_t* null_bm
    cdef Py_ssize_t dict_size
    cdef DrakenVarBuffer* ndp

    # Constant encoding: process once, replicate
    if vec._has_const:
        builder = string_vector_module.StringVectorBuilder.with_estimate(n, 16)
        if vec._const_is_null or vec._const_value == NULL:
            for i in range(n):
                builder.append_null()
        else:
            raw = bytes(<uint8_t*>vec._const_value.data)[:vec._const_value.length]
            text = raw.decode('utf-8', errors='replace')
            result = text[::-1].encode('utf-8')
            for i in range(n):
                builder.append(result)
        return builder.finish()

    # Dictionary encoding: transform each unique entry, repack with same codes
    if vec._encoding == DRAKEN_ENCODING_DICTIONARY:
        dict_size = vec._dict_values.length
        dict_builder = string_vector_module.StringVectorBuilder.with_estimate(dict_size, 16)
        for i in range(dict_size):
            start = vec._dict_values.offsets[i]
            end = vec._dict_values.offsets[i + 1]
            raw = bytes(<uint8_t*>vec._dict_values.data + start)[:end - start]
            text = raw.decode('utf-8', errors='replace')
            dict_builder.append(text[::-1].encode('utf-8'))
        new_dict_sv = dict_builder.finish()
        ndp = (<StringVector>new_dict_sv).ptr
        return from_packed_dict(
            vec._dict_codes, vec._dict_code_width, n,
            ndp.offsets, <const uint8_t*>ndp.data, dict_size,
            vec._dict_accessor.row_nulls,
        )

    # Dense encoding: row by row
    builder = string_vector_module.StringVectorBuilder.with_estimate(n, 16)
    ptr = vec.ptr
    null_bm = ptr.null_bitmap
    for i in range(n):
        if null_bm != NULL and not ((null_bm[i >> 3] >> (i & 7)) & 1):
            builder.append_null()
            continue
        start = ptr.offsets[i]
        end = ptr.offsets[i + 1]
        raw = bytes(<uint8_t*>ptr.data + start)[:end - start]
        text = raw.decode('utf-8', errors='replace')
        builder.append(text[::-1].encode('utf-8'))
    return builder.finish()
