# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False

from libc.stdint cimport int32_t, uint8_t

from opteryx.compiled.draken.vectors.string_vector cimport StringVector
from opteryx.compiled.draken.vectors import string_vector as string_vector_module
from opteryx.compiled.draken.core.buffers cimport DrakenVarBuffer


cpdef StringVector vector_soundex(StringVector vec):
    """
    Compute Soundex codes for each element of a StringVector.

    Returns:
        StringVector: Soundex codes (e.g. 'A123' or '0000' for empty).
    """
    from opteryx.third_party.fuzzy import soundex

    cdef DrakenVarBuffer* ptr = vec.ptr
    cdef Py_ssize_t n = ptr.length
    cdef Py_ssize_t i
    cdef int32_t start, end
    cdef uint8_t* null_bm = ptr.null_bitmap
    cdef bytes raw
    cdef str text, code

    builder = string_vector_module.StringVectorBuilder.with_estimate(n, 4)

    for i in range(n):
        if null_bm != NULL and not ((null_bm[i >> 3] >> (i & 7)) & 1):
            builder.append_null()
        else:
            start = ptr.offsets[i]
            end = ptr.offsets[i + 1]
            if end == start:
                builder.append_null()
            else:
                raw = bytes(<uint8_t*>ptr.data + start)[:end - start]
                text = raw.decode("utf-8", "replace")
                code = soundex(text)
                builder.append(code.encode("utf-8") if code else b"")

    return builder.finish()
