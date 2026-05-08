# cython: language_level=3, boundscheck=False, wraparound=False, cdivision=True, initializedcheck=False

from draken.vectors.string_vector cimport StringVector, DrakenVarBuffer
from draken.vectors.bool_vector cimport BoolVector
from draken.core.buffers cimport DRAKEN_ENCODING_DICTIONARY
from cpython.bytes cimport PyBytes_AS_STRING
from libc.string cimport memset, memcpy
from libc.stdlib cimport malloc, free


cdef inline bint _sv_byte_equals(uint8_t left, uint8_t right, bint ignore_case) noexcept nogil:
    """Compare two bytes, optionally case-insensitive."""
    if ignore_case:
        return _sv_ascii_lower(left) == _sv_ascii_lower(right)
    return left == right


cdef bint _sv_sql_like_match(
    const uint8_t* text,
    Py_ssize_t text_len,
    const uint8_t* pattern,
    Py_ssize_t pattern_len,
    bint ignore_case,
) noexcept nogil:
    """SQL LIKE matcher supporting % and _ wildcards and backslash escaping."""
    cdef Py_ssize_t ti = 0
    cdef Py_ssize_t pi = 0
    cdef Py_ssize_t last_pct = -1
    cdef Py_ssize_t last_match = 0
    cdef uint8_t pc

    while ti < text_len:
        if pi < pattern_len:
            pc = pattern[pi]
            if pc == 92 and (pi + 1) < pattern_len:  # backslash escape
                if _sv_byte_equals(text[ti], pattern[pi + 1], ignore_case):
                    ti += 1
                    pi += 2
                    continue
            elif pc == 95:  # "_" wildcard
                ti += 1
                pi += 1
                continue
            elif pc == 37:  # "%" wildcard
                last_pct = pi
                pi += 1
                last_match = ti
                continue
            elif _sv_byte_equals(text[ti], pc, ignore_case):
                ti += 1
                pi += 1
                continue

        if last_pct != -1:
            last_match += 1
            ti = last_match
            pi = last_pct + 1
            continue
        return False

    while pi < pattern_len and pattern[pi] == 37:
        pi += 1

    return pi == pattern_len


cpdef BoolVector vector_like(StringVector vec, bytes pattern, bint ignore_case=False):
    """Return mask: 1 if element matches SQL LIKE pattern, else 0. Propagates NULLs.

    Optimized for dictionary-encoded vectors: tests each unique value once.
    """
    cdef DrakenVarBuffer* ptr = vec.ptr
    cdef Py_ssize_t n = ptr.length
    cdef Py_ssize_t nbytes = (n + 7) >> 3
    cdef uint8_t* nb_ptr = ptr.null_bitmap
    cdef BoolVector out = BoolVector(<size_t>n)
    cdef uint8_t* dst = <uint8_t*> out.ptr.data
    cdef uint8_t* out_null = NULL
    cdef uint8_t mask
    cdef char* pat_ptr = PyBytes_AS_STRING(pattern)
    cdef Py_ssize_t pat_len = len(pattern)
    cdef int32_t start, end, str_len
    cdef Py_ssize_t i, dict_idx, dict_size
    cdef uint32_t code
    cdef DrakenVarBuffer* dict_values_buf
    cdef const uint8_t* dict_data
    cdef uint8_t* dict_like_results = NULL
    cdef const uint8_t* dict_codes
    cdef uint8_t dict_code_width
    cdef uint8_t* dict_row_nulls

    if vec._has_const:
        if vec._const_is_null:
            return _constant_bool_result(n, False, True)
        return _constant_bool_result(
            n,
            _sv_sql_like_match(
                <const uint8_t*>vec._const_value.data,
                vec._const_value.length,
                <const uint8_t*>pat_ptr,
                pat_len,
                ignore_case,
            ),
            False,
        )

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
            dict_like_results = <uint8_t*>malloc(dict_size)
            if dict_like_results == NULL:
                raise MemoryError()

            # Test each unique dictionary value once
            for dict_idx in range(dict_size):
                start = dict_values_buf.offsets[dict_idx]
                end = dict_values_buf.offsets[dict_idx + 1]
                str_len = end - start

                if _sv_sql_like_match(
                    dict_data + start, <Py_ssize_t>str_len,
                    <const uint8_t*>pat_ptr, pat_len, ignore_case,
                ):
                    dict_like_results[dict_idx] = 1
                else:
                    dict_like_results[dict_idx] = 0

            # Scatter results by code index
            for i in range(n):
                if dict_row_nulls != NULL and ((dict_row_nulls[i >> 3] >> (i & 7)) & 1) == 0:
                    continue
                code = _read_packed_code(dict_codes, dict_code_width, i)
                if dict_like_results[code]:
                    dst[i >> 3] |= (1 << (i & 7))

        # Dense vector path (non-dictionary, non-constant)
        else:
            for i in range(n):
                if nb_ptr != NULL and ((nb_ptr[i >> 3] >> (i & 7)) & 1) == 0:
                    continue
                start = ptr.offsets[i]
                end = ptr.offsets[i + 1]
                str_len = end - start
                if _sv_sql_like_match(
                    <const uint8_t*>ptr.data + start, <Py_ssize_t>str_len,
                    <const uint8_t*>pat_ptr, pat_len, ignore_case,
                ):
                    dst[i >> 3] |= (1 << (i & 7))
    finally:
        if dict_like_results != NULL:
            free(dict_like_results)

    return out
