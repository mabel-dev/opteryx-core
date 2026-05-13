# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False
# cython: optimize.use_switch=True
# cython: optimize.unpack_method_calls=True

from draken.vectors.string_vector cimport StringVector, DrakenVarBuffer
from draken.vectors.bool_vector cimport BoolVector
from draken.core.buffers cimport DRAKEN_ENCODING_DICTIONARY
from cpython.bytes cimport PyBytes_FromStringAndSize
from libc.string cimport memset, memcpy
from libc.stdlib cimport malloc, free


cpdef BoolVector vector_rlike(StringVector vec, bytes pattern):
    """Return mask: 1 if element matches regex pattern, else 0. Propagates NULLs.

    Optimized for dictionary-encoded vectors: tests each unique value once.
    """
    import re
    cdef DrakenVarBuffer* ptr = vec.ptr
    cdef Py_ssize_t n = ptr.length
    cdef Py_ssize_t nbytes = (n + 7) >> 3
    cdef uint8_t* nb_ptr = ptr.null_bitmap
    cdef BoolVector out = BoolVector(<size_t>n)
    cdef uint8_t* dst = <uint8_t*> out.ptr.data
    cdef uint8_t* out_null = NULL
    cdef uint8_t mask
    cdef int32_t start, end, str_len
    cdef Py_ssize_t i, dict_idx, dict_size
    cdef uint32_t code
    cdef bytes cell_bytes
    cdef DrakenVarBuffer* dict_values_buf
    cdef const uint8_t* dict_data
    cdef uint8_t* dict_rlike_results = NULL
    cdef const uint8_t* dict_codes
    cdef uint8_t dict_code_width
    cdef uint8_t* dict_row_nulls

    compiled = re.compile(pattern)

    if vec._has_const:
        if vec._const_is_null:
            return _constant_bool_result(n, False, True)
        cell_bytes = PyBytes_FromStringAndSize(<char*>vec._const_value.data, vec._const_value.length)
        return _constant_bool_result(n, compiled.search(cell_bytes) is not None, False)

    memset(dst, 0, nbytes)
    if nb_ptr != NULL and nbytes != 0:
        out_null = <uint8_t*> malloc(nbytes)
        if out_null == NULL:
            raise MemoryError()
        memcpy(out_null, nb_ptr, nbytes)
        if (n & 7) != 0:
            mask = <uint8_t>((1 << (n & 7)) - 1)
            out_null[nbytes - 1] &= mask
        out.ptr.null_bitmap = out_null
    else:
        out.ptr.null_bitmap = NULL

    try:
        # Dictionary-encoded path: check each unique value once
        if vec._encoding == DRAKEN_ENCODING_DICTIONARY:
            dict_values_buf = vec._dict_values
            if dict_values_buf == NULL or dict_values_buf.data == NULL:
                return out  # Fallback to empty result

            dict_size = <Py_ssize_t>dict_values_buf.length
            dict_codes = vec._dict_codes
            if dict_codes == NULL or dict_size == 0:
                return out  # Fallback to empty result

            dict_code_width = vec._dict_code_width
            dict_row_nulls = vec.ptr.null_bitmap
            dict_data = <const uint8_t*>dict_values_buf.data

            # Allocate results array for each dictionary entry
            dict_rlike_results = <uint8_t*>malloc(dict_size)
            if dict_rlike_results == NULL:
                raise MemoryError()

            # Test each unique dictionary value once
            for dict_idx in range(dict_size):
                start = dict_values_buf.offsets[dict_idx]
                end = dict_values_buf.offsets[dict_idx + 1]
                str_len = end - start
                cell_bytes = PyBytes_FromStringAndSize(<char*>dict_data + start, <Py_ssize_t>str_len)
                if compiled.search(cell_bytes) is not None:
                    dict_rlike_results[dict_idx] = 1
                else:
                    dict_rlike_results[dict_idx] = 0

            # Scatter results by code index
            for i in range(n):
                if dict_row_nulls != NULL and ((dict_row_nulls[i >> 3] >> (i & 7)) & 1) == 0:
                    continue
                code = _read_packed_code(dict_codes, dict_code_width, i)
                if dict_rlike_results[code]:
                    dst[i >> 3] |= (1 << (i & 7))

        # Dense vector path (non-dictionary, non-constant)
        else:
            for i in range(n):
                if nb_ptr != NULL and ((nb_ptr[i >> 3] >> (i & 7)) & 1) == 0:
                    continue
                start = ptr.offsets[i]
                end = ptr.offsets[i + 1]
                str_len = end - start
                cell_bytes = PyBytes_FromStringAndSize(<char*>ptr.data + start, <Py_ssize_t>str_len)
                if compiled.search(cell_bytes):
                    dst[i >> 3] |= (1 << (i & 7))
    finally:
        if dict_rlike_results != NULL:
            free(dict_rlike_results)

    return out
