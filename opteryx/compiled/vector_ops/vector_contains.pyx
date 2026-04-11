# cython: language_level=3, boundscheck=False, wraparound=False, cdivision=True, initializedcheck=False

from opteryx.compiled.draken.vectors.string_vector cimport StringVector, DrakenVarBuffer
from opteryx.compiled.draken.vectors.bool_vector cimport BoolVector
from opteryx.compiled.draken.core.buffers cimport DRAKEN_ENCODING_DICTIONARY
from cpython.bytes cimport PyBytes_AS_STRING
from libc.string cimport memset, memcpy
from libc.stdlib cimport malloc, free
from libc.stddef cimport size_t


# Volnitsky algorithm declarations for substring search
cdef extern from "volnitsky.h":
    cppclass VolnitskyTable:
        pass
    VolnitskyTable* volnitsky_alloc() noexcept nogil
    void volnitsky_free(VolnitskyTable* t) noexcept nogil
    void volnitsky_build(VolnitskyTable* t, const uint8_t* pat, size_t len) nogil
    bint volnitsky_contains_cs(const uint8_t* hay, size_t hay_len,
                                const uint8_t* pat, size_t pat_len,
                                const VolnitskyTable* table) noexcept nogil
    bint volnitsky_contains_ci(const uint8_t* hay, size_t hay_len,
                                const uint8_t* pat_lower, size_t pat_len,
                                const VolnitskyTable* table) noexcept nogil


cdef bint _sv_contains_cs(
    const uint8_t* haystack,
    Py_ssize_t hay_len,
    const uint8_t* needle,
    Py_ssize_t ndl_len,
    const VolnitskyTable* tbl,
) noexcept nogil:
    """Case-sensitive substring search using Volnitsky algorithm."""
    return volnitsky_contains_cs(haystack, <size_t>hay_len, needle, <size_t>ndl_len, tbl)


cdef bint _sv_contains_ci(
    const uint8_t* haystack,
    Py_ssize_t hay_len,
    const uint8_t* needle_lower,
    Py_ssize_t ndl_len,
    const VolnitskyTable* tbl,
) noexcept nogil:
    """Case-insensitive substring search using Volnitsky algorithm."""
    return volnitsky_contains_ci(haystack, <size_t>hay_len, needle_lower, <size_t>ndl_len, tbl)


cpdef BoolVector vector_contains(StringVector vec, bytes substr, bint ignore_case=False):
    """Return mask: 1 if element contains substr, else 0. Propagates NULLs.

    Optimized for:
    - Dictionary-encoded vectors: tests each unique value once
    - Case-insensitive: pre-lowers entire buffer before comparison
    """
    cdef DrakenVarBuffer* ptr = vec.ptr
    cdef Py_ssize_t n = ptr.length
    cdef Py_ssize_t nbytes = (n + 7) >> 3
    cdef uint8_t* nb_ptr = ptr.null_bitmap
    cdef BoolVector out = BoolVector(<size_t>n)
    cdef uint8_t* dst = <uint8_t*> out.ptr.data
    cdef uint8_t* out_null = NULL
    cdef uint8_t mask
    cdef char* ndl_ptr_char = PyBytes_AS_STRING(substr)
    cdef Py_ssize_t ndl_len = len(substr)
    cdef uint8_t* ndl_lower = NULL
    cdef int32_t start, end, str_len
    cdef Py_ssize_t i, j, dict_idx, dict_size
    cdef uint32_t code
    cdef uint8_t byte
    cdef DrakenVarBuffer* dict_values_buf
    cdef const uint8_t* dict_data
    cdef uint8_t* dict_contains_results = NULL
    cdef const uint8_t* dict_codes
    cdef uint8_t dict_code_width
    cdef uint8_t* dict_row_nulls
    cdef uint8_t* data_lower = NULL
    cdef Py_ssize_t data_len
    cdef VolnitskyTable* tbl = NULL

    # Constant vector case
    if vec._has_const:
        if vec._const_is_null:
            return _constant_bool_result(n, False, True)
        if ignore_case and ndl_len > 0:
            ndl_lower = <uint8_t*>malloc(<size_t>ndl_len)
            if ndl_lower == NULL:
                raise MemoryError()
            for j in range(ndl_len):
                ndl_lower[j] = _sv_ascii_lower(<uint8_t>ndl_ptr_char[j])
        tbl = volnitsky_alloc()
        if tbl == NULL:
            if ndl_lower != NULL:
                free(ndl_lower)
            raise MemoryError()
        if ignore_case and ndl_lower != NULL:
            volnitsky_build(tbl, ndl_lower, <size_t>ndl_len)
        else:
            volnitsky_build(tbl, <const uint8_t*>ndl_ptr_char, <size_t>ndl_len)
        try:
            if ignore_case:
                return _constant_bool_result(
                    n,
                    _sv_contains_ci(
                        <const uint8_t*>vec._const_value.data,
                        vec._const_value.length,
                        ndl_lower if ndl_lower != NULL else <uint8_t*>ndl_ptr_char,
                        ndl_len,
                        tbl,
                    ),
                    False,
                )
            return _constant_bool_result(
                n,
                _sv_contains_cs(
                    <const uint8_t*>vec._const_value.data,
                    vec._const_value.length,
                    <const uint8_t*>ndl_ptr_char,
                    ndl_len,
                    tbl,
                ),
                False,
            )
        finally:
            volnitsky_free(tbl)
            tbl = NULL
            if ndl_lower != NULL:
                free(ndl_lower)

    # Setup output null bitmap
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

    # Pre-lowercase needle once
    if ignore_case and ndl_len > 0:
        ndl_lower = <uint8_t*>malloc(<size_t>ndl_len)
        if ndl_lower == NULL:
            raise MemoryError()
        for j in range(ndl_len):
            ndl_lower[j] = _sv_ascii_lower(<uint8_t>ndl_ptr_char[j])

    # Build Volnitsky table once for all elements in this morsel
    tbl = volnitsky_alloc()
    if tbl == NULL:
        if ndl_lower != NULL:
            free(ndl_lower)
        raise MemoryError()
    if ignore_case and ndl_lower != NULL:
        volnitsky_build(tbl, ndl_lower, <size_t>ndl_len)
    else:
        volnitsky_build(tbl, <const uint8_t*>ndl_ptr_char, <size_t>ndl_len)

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
            dict_contains_results = <uint8_t*>malloc(dict_size)
            if dict_contains_results == NULL:
                raise MemoryError()

            # Test each unique dictionary value once
            for dict_idx in range(dict_size):
                start = dict_values_buf.offsets[dict_idx]
                end = dict_values_buf.offsets[dict_idx + 1]
                str_len = end - start

                if ignore_case:
                    if _sv_contains_ci(
                        dict_data + start, <Py_ssize_t>str_len,
                        ndl_lower if ndl_lower != NULL else <uint8_t*>ndl_ptr_char,
                        ndl_len,
                        tbl,
                    ):
                        dict_contains_results[dict_idx] = 1
                    else:
                        dict_contains_results[dict_idx] = 0
                else:
                    if _sv_contains_cs(
                        dict_data + start, <Py_ssize_t>str_len,
                        <const uint8_t*>ndl_ptr_char,
                        ndl_len,
                        tbl,
                    ):
                        dict_contains_results[dict_idx] = 1
                    else:
                        dict_contains_results[dict_idx] = 0

            # Scatter results by code index
            for i in range(n):
                if dict_row_nulls != NULL and ((dict_row_nulls[i >> 3] >> (i & 7)) & 1) == 0:
                    continue
                code = _read_packed_code(dict_codes, dict_code_width, i)
                if dict_contains_results[code]:
                    dst[i >> 3] |= (1 << (i & 7))

        # Dense vector path (non-dictionary, non-constant)
        else:
            # For case-insensitive: pre-lowercase entire buffer once
            if ignore_case and ptr.data != NULL:
                data_len = ptr.offsets[n]
                data_lower = <uint8_t*>malloc(data_len)
                if data_lower == NULL:
                    raise MemoryError()
                # Copy and lowercase entire buffer in one pass
                for j in range(data_len):
                    data_lower[j] = _sv_ascii_lower((<const uint8_t*>ptr.data)[j])

            # Process each row
            for i in range(n):
                if nb_ptr != NULL and ((nb_ptr[i >> 3] >> (i & 7)) & 1) == 0:
                    continue
                start = ptr.offsets[i]
                end = ptr.offsets[i + 1]
                str_len = end - start

                if ignore_case:
                    # Use pre-lowercased buffer for case-sensitive search
                    if _sv_contains_cs(
                        data_lower + start, <Py_ssize_t>str_len,
                        ndl_lower if ndl_lower != NULL else <uint8_t*>ndl_ptr_char,
                        ndl_len,
                        tbl,
                    ):
                        dst[i >> 3] |= (1 << (i & 7))
                else:
                    if _sv_contains_cs(
                        <const uint8_t*>ptr.data + start, <Py_ssize_t>str_len,
                        <const uint8_t*>ndl_ptr_char, ndl_len,
                        tbl,
                    ):
                        dst[i >> 3] |= (1 << (i & 7))

    finally:
        volnitsky_free(tbl)
        tbl = NULL
        if ndl_lower != NULL:
            free(ndl_lower)
        if data_lower != NULL:
            free(data_lower)
        if dict_contains_results != NULL:
            free(dict_contains_results)

    return out
