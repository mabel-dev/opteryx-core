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
from draken.core.buffers cimport DrakenVarBuffer, DrakenConstantStringPayload, DrakenVector


cpdef Int64Vector vector_string_length(StringVector vec):
    """Return byte-length of each string in a StringVector."""
    cdef DrakenVector* uv = vec.unified()
    cdef Py_ssize_t n = <Py_ssize_t>uv.length
    cdef Py_ssize_t dict_size
    cdef uint8_t* null_bm
    cdef uint32_t code
    cdef int64_t* dict_lengths
    cdef int64_t[::1] rview
    cdef DrakenVarBuffer* dv
    cdef DrakenConstantStringPayload* csp

    cdef array template = array('q')  # 'q' = signed long long (int64)
    cdef array result_array = clone(template, n, False)
    rview = result_array

    if uv.data_length == 1:  # constant
        if uv.validity == NULL:  # non-null constant
            csp = <DrakenConstantStringPayload*>uv.data
            for i in range(n):
                rview[i] = <int64_t>csp.length
        else:
            for i in range(n):
                rview[i] = 0
        return int64_from_sequence(rview)

    if uv.selection != NULL:  # dictionary
        # Compute one length per dict entry, then scatter to rows via codes.
        # D << N so this is O(D + N) with a much smaller constant than O(N * string_work).
        dv = <DrakenVarBuffer*>uv.data
        dict_size = <Py_ssize_t>dv.length
        dict_lengths = <int64_t*>malloc(<size_t>dict_size * sizeof(int64_t))
        if dict_lengths == NULL:
            raise MemoryError()
        try:
            for i in range(dict_size):
                dict_lengths[i] = dv.offsets[i + 1] - dv.offsets[i]
            null_bm = uv.validity
            for i in range(n):
                if null_bm != NULL and not ((null_bm[i >> 3] >> (i & 7)) & 1):
                    rview[i] = 0
                else:
                    code = _decode_dict_code(<uint8_t*>uv.selection, uv.sel_width, i)
                    rview[i] = dict_lengths[code]
        finally:
            free(dict_lengths)
        return int64_from_sequence(rview)

    # Dense
    dv = <DrakenVarBuffer*>uv.data
    null_bm = uv.validity
    for i in range(n):
        if null_bm != NULL and not ((null_bm[i >> 3] >> (i & 7)) & 1):
            rview[i] = 0
        else:
            rview[i] = dv.offsets[i + 1] - dv.offsets[i]

    return int64_from_sequence(rview)
