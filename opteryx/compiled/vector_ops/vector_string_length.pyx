# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False
# cython: optimize.use_switch=True
# cython: optimize.unpack_method_calls=True

from libc.stdint cimport int32_t, int64_t, uint8_t, uint32_t
from libc.stdlib cimport malloc, free
from cpython.array cimport array, clone

from draken.vectors.string_vector cimport StringVector
from draken.vectors.int64_vector cimport Int64Vector, from_sequence as int64_from_sequence
from draken.core.buffers cimport DrakenVarBuffer, DrakenConstantStringPayload, DRAKEN_ENCODING_DICTIONARY


cpdef Int64Vector vector_string_length(StringVector vec):
    """Return byte-length of each string in a StringVector."""
    cdef DrakenVarBuffer* ptr = vec.ptr
    cdef DrakenVarBuffer* dv
    cdef Py_ssize_t n = ptr.length
    cdef Py_ssize_t dict_size
    cdef uint8_t* null_bm
    cdef uint8_t* codes
    cdef uint8_t code_width
    cdef uint32_t code
    cdef int64_t* dict_lengths
    cdef int64_t[::1] rview
    cdef Py_ssize_t i
    cdef DrakenConstantStringPayload* const_val

    cdef array template = array('q')  # 'q' = signed long long (int64)
    cdef array result_array = clone(template, n, False)
    rview = result_array

    if vec._has_const:
        if not vec._const_is_null and vec._const_value != NULL:
            const_val = vec._const_value
            for i in range(n):
                rview[i] = const_val.length
        else:
            for i in range(n):
                rview[i] = 0
        return int64_from_sequence(rview)

    if vec._encoding == DRAKEN_ENCODING_DICTIONARY and ptr.data == NULL:
        # Compute one length per dict entry, then scatter to rows via codes.
        # D << N so this is O(D + N) with a much smaller constant than O(N * string_work).
        dv = vec._dict_values
        dict_size = <Py_ssize_t>dv.length
        dict_lengths = <int64_t*>malloc(<size_t>dict_size * sizeof(int64_t))
        if dict_lengths == NULL:
            raise MemoryError()
        codes = vec._dict_codes
        code_width = vec._dict_code_width
        try:
            for i in range(dict_size):
                dict_lengths[i] = dv.offsets[i + 1] - dv.offsets[i]
            null_bm = ptr.null_bitmap
            for i in range(n):
                if null_bm != NULL and not ((null_bm[i >> 3] >> (i & 7)) & 1):
                    rview[i] = 0
                else:
                    code = _decode_dict_code(codes, code_width, i)
                    rview[i] = dict_lengths[code]
        finally:
            free(dict_lengths)
        return int64_from_sequence(rview)

    null_bm = ptr.null_bitmap
    for i in range(n):
        if null_bm != NULL and not ((null_bm[i >> 3] >> (i & 7)) & 1):
            rview[i] = 0
        else:
            rview[i] = ptr.offsets[i + 1] - ptr.offsets[i]

    return int64_from_sequence(rview)
