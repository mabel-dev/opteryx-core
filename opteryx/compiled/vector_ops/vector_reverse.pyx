# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False

from libc.stdint cimport int32_t, uint8_t

from opteryx.draken.vectors.string_vector cimport StringVector
from opteryx.draken.vectors import string_vector as string_vector_module
from opteryx.draken.core.buffers cimport DrakenVarBuffer


cpdef StringVector vector_reverse(StringVector vec):
    """Reverse each string element in a StringVector (Unicode codepoint-aware)."""
    cdef DrakenVarBuffer* ptr = vec.ptr
    cdef Py_ssize_t n = ptr.length
    cdef Py_ssize_t i
    cdef int32_t start, end
    cdef bytes raw
    cdef str text
    cdef bytes result
    cdef uint8_t* null_bm = ptr.null_bitmap

    builder = string_vector_module.StringVectorBuilder.with_estimate(n, 16)

    for i in range(n):
        if null_bm != NULL and not ((null_bm[i >> 3] >> (i & 7)) & 1):
            builder.append_null()
        else:
            start = ptr.offsets[i]
            end = ptr.offsets[i + 1]
            raw = bytes(<uint8_t*>ptr.data + start)[:end - start]
            text = raw.decode('utf-8', errors='replace')
            result = text[::-1].encode('utf-8')
            builder.append(result)

    return builder.finish()
