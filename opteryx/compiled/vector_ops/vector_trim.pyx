# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False

from opteryx.draken.vectors.string_vector cimport StringVector
from opteryx.draken.vectors import string_vector as string_vector_module
from opteryx.draken.core.buffers cimport DrakenVarBuffer
from libc.stdint cimport int32_t, uint8_t


cdef inline str _chars_to_str(object chars):
    cdef DrakenVarBuffer* ptr
    cdef uint8_t* null_bm
    cdef int32_t start, end
    cdef Py_ssize_t i

    if chars is None:
        return None
    if isinstance(chars, str):
        return chars
    if isinstance(chars, bytes):
        return chars.decode("utf-8", "replace")

    if isinstance(chars, StringVector):
        ptr = (<StringVector>chars).ptr
        null_bm = ptr.null_bitmap
        for i in range(ptr.length):
            if null_bm != NULL and not ((null_bm[i >> 3] >> (i & 7)) & 1):
                continue
            start = ptr.offsets[i]
            end = ptr.offsets[i + 1]
            return bytes(<uint8_t*>ptr.data + start)[:end - start].decode("utf-8", "replace")
        return None

    if hasattr(chars, "__iter__") and not isinstance(chars, (str, bytes)):
        try:
            first = next(iter(chars))
        except StopIteration:
            return None
        return _chars_to_str(first)

    try:
        return str(chars)
    except Exception:
        return None


cpdef StringVector vector_trim(StringVector vec, object chars=None):
    cdef DrakenVarBuffer* ptr = vec.ptr
    cdef Py_ssize_t n = ptr.length
    cdef uint8_t* null_bm = ptr.null_bitmap

    cdef str trim_chars = _chars_to_str(chars)
    cdef Py_ssize_t i
    cdef int32_t start, end
    cdef bytes raw
    cdef str text, result

    builder = string_vector_module.StringVectorBuilder.with_estimate(n, 16)

    for i in range(n):
        if null_bm != NULL and not ((null_bm[i >> 3] >> (i & 7)) & 1):
            builder.append_null()
            continue

        start = ptr.offsets[i]
        end = ptr.offsets[i + 1]
        raw = bytes(<uint8_t*>ptr.data + start)[:end - start]
        text = raw.decode("utf-8", "replace")

        if trim_chars is None:
            result = text.strip()
        else:
            result = text.strip(trim_chars)

        builder.append(result.encode("utf-8"))

    return builder.finish()


cpdef StringVector vector_ltrim(StringVector vec, object chars=None):
    cdef DrakenVarBuffer* ptr = vec.ptr
    cdef Py_ssize_t n = ptr.length
    cdef uint8_t* null_bm = ptr.null_bitmap

    cdef str trim_chars = _chars_to_str(chars)
    cdef Py_ssize_t i
    cdef int32_t start, end
    cdef bytes raw
    cdef str text, result

    builder = string_vector_module.StringVectorBuilder.with_estimate(n, 16)

    for i in range(n):
        if null_bm != NULL and not ((null_bm[i >> 3] >> (i & 7)) & 1):
            builder.append_null()
            continue

        start = ptr.offsets[i]
        end = ptr.offsets[i + 1]
        raw = bytes(<uint8_t*>ptr.data + start)[:end - start]
        text = raw.decode("utf-8", "replace")

        if trim_chars is None:
            result = text.lstrip()
        else:
            result = text.lstrip(trim_chars)

        builder.append(result.encode("utf-8"))

    return builder.finish()


cpdef StringVector vector_rtrim(StringVector vec, object chars=None):
    cdef DrakenVarBuffer* ptr = vec.ptr
    cdef Py_ssize_t n = ptr.length
    cdef uint8_t* null_bm = ptr.null_bitmap

    cdef str trim_chars = _chars_to_str(chars)
    cdef Py_ssize_t i
    cdef int32_t start, end
    cdef bytes raw
    cdef str text, result

    builder = string_vector_module.StringVectorBuilder.with_estimate(n, 16)

    for i in range(n):
        if null_bm != NULL and not ((null_bm[i >> 3] >> (i & 7)) & 1):
            builder.append_null()
            continue

        start = ptr.offsets[i]
        end = ptr.offsets[i + 1]
        raw = bytes(<uint8_t*>ptr.data + start)[:end - start]
        text = raw.decode("utf-8", "replace")

        if trim_chars is None:
            result = text.rstrip()
        else:
            result = text.rstrip(trim_chars)

        builder.append(result.encode("utf-8"))

    return builder.finish()
