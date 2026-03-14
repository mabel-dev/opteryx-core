# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False

from libc.stdint cimport int32_t, uint8_t

from opteryx.draken.core.buffers cimport DrakenArrayBuffer, DrakenVarBuffer
from opteryx.draken.vectors.array_vector cimport ArrayVector
from opteryx.draken.vectors.string_vector cimport StringVector
from opteryx.draken.vectors import string_vector as string_vector_module

# NOTE: DrakenArrayBuffer.values is NEVER set for arrow-backed ArrayVectors.
# The child is stored in the Python-level `_child` attribute as a StringVector.
# Always access arr._child, cast to StringVector, then use .ptr.


cdef inline bytes _child_bytes(DrakenVarBuffer* child_ptr, int32_t j):
    """Extract bytes for child element j from a DrakenVarBuffer."""
    cdef int32_t start = child_ptr.offsets[j]
    cdef int32_t end = child_ptr.offsets[j + 1]
    return bytes(<uint8_t*>child_ptr.data + start)[:end - start]


cpdef StringVector vector_concat_array(ArrayVector arr):
    """
    CONCAT(array_col): for each row, concatenate all child string elements with
    no separator. NULL rows produce NULL output; NULL child elements are skipped.

    Returns a StringVector of length == arr.length.
    """
    cdef DrakenArrayBuffer* arr_ptr = arr.ptr
    # ptr.values is NOT set for arrow-backed ArrayVectors — child lives in _child.
    cdef StringVector child_sv = <StringVector>arr._child
    cdef DrakenVarBuffer* child_ptr = child_sv.ptr
    cdef Py_ssize_t n = arr_ptr.length
    cdef uint8_t* row_null_bm = arr_ptr.null_bitmap
    cdef uint8_t* child_null_bm = child_ptr.null_bitmap
    cdef Py_ssize_t i
    cdef int32_t child_start, child_end, j

    builder = string_vector_module.StringVectorBuilder.with_estimate(n, 32)

    for i in range(n):
        # NULL list row
        if row_null_bm != NULL and not ((row_null_bm[i >> 3] >> (i & 7)) & 1):
            builder.append_null()
            continue

        child_start = arr_ptr.offsets[i]
        child_end = arr_ptr.offsets[i + 1]

        parts = []
        for j in range(child_start, child_end):
            # Skip NULL child elements
            if child_null_bm != NULL and not ((child_null_bm[j >> 3] >> (j & 7)) & 1):
                continue
            parts.append(_child_bytes(child_ptr, j))

        builder.append(b"".join(parts))

    return builder.finish()


cpdef StringVector vector_concat_ws_array(bytes sep, ArrayVector arr):
    """
    CONCAT_WS(sep, array_col): for each row, join child string elements with
    the given separator. NULL rows produce NULL output; NULL child elements are
    skipped (not included in the join).

    Returns a StringVector of length == arr.length.
    """
    cdef DrakenArrayBuffer* arr_ptr = arr.ptr
    # ptr.values is NOT set for arrow-backed ArrayVectors — child lives in _child.
    cdef StringVector child_sv = <StringVector>arr._child
    cdef DrakenVarBuffer* child_ptr = child_sv.ptr
    cdef Py_ssize_t n = arr_ptr.length
    cdef uint8_t* row_null_bm = arr_ptr.null_bitmap
    cdef uint8_t* child_null_bm = child_ptr.null_bitmap
    cdef Py_ssize_t i
    cdef int32_t child_start, child_end, j

    builder = string_vector_module.StringVectorBuilder.with_estimate(n, 32)

    for i in range(n):
        if row_null_bm != NULL and not ((row_null_bm[i >> 3] >> (i & 7)) & 1):
            builder.append_null()
            continue

        child_start = arr_ptr.offsets[i]
        child_end = arr_ptr.offsets[i + 1]

        parts = []
        for j in range(child_start, child_end):
            if child_null_bm != NULL and not ((child_null_bm[j >> 3] >> (j & 7)) & 1):
                continue
            parts.append(_child_bytes(child_ptr, j))

        builder.append(sep.join(parts))

    return builder.finish()
