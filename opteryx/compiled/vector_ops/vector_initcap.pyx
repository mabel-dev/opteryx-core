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


cdef inline str _initcap_string(str text):
    cdef Py_ssize_t i, length = len(text)
    if length == 0:
        return text
    cdef list builder = []
    cdef str ch
    cdef bint in_word = False
    for i in range(length):
        ch = text[i]
        if ch.isalpha():
            builder.append(ch.upper() if not in_word else ch.lower())
            in_word = True
        elif ch.isdigit():
            builder.append(ch)
            in_word = True
        else:
            builder.append(ch)
            in_word = False
    return "".join(builder)


cpdef StringVector vector_initcap(StringVector vec):
    """Apply INITCAP transformation to each element of a StringVector."""
    cdef DrakenVarBuffer* ptr = vec.ptr
    cdef Py_ssize_t n = ptr.length
    cdef Py_ssize_t i
    cdef int32_t start, end
    cdef bytes raw
    cdef str text, transformed
    cdef uint8_t* null_bm = ptr.null_bitmap

    builder = string_vector_module.StringVectorBuilder.with_estimate(n, 16)

    for i in range(n):
        if null_bm != NULL and not ((null_bm[i >> 3] >> (i & 7)) & 1):
            builder.append_null()
        else:
            start = ptr.offsets[i]
            end = ptr.offsets[i + 1]
            raw = bytes(<uint8_t*>ptr.data + start)[:end - start]
            try:
                text = raw.decode("utf-8")
            except UnicodeDecodeError:
                text = raw.decode("utf-8", "replace")
            transformed = _initcap_string(text)
            builder.append(transformed.encode("utf-8"))

    return builder.finish()
