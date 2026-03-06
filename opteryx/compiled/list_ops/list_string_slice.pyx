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


cpdef StringVector list_string_slice_left(StringVector vec, object length):
    """
    Slice each string from the left (beginning) up to 'length' bytes.

    Parameters:
        vec: StringVector of strings.
        length: int scalar or iterable of ints — number of bytes to keep.

    Returns:
        StringVector: sliced strings.
    """
    cdef DrakenVarBuffer* ptr = vec.ptr
    cdef Py_ssize_t n = ptr.length
    cdef uint8_t* null_bm = ptr.null_bitmap
    cdef Py_ssize_t i
    cdef int32_t start, end, row_len
    cdef int slice_len

    # Normalize length to a list
    cdef list length_list
    if hasattr(length, "__iter__") and not isinstance(length, (str, bytes)):
        try:
            length_list = list(length)
        except TypeError:
            length_list = [int(length)] * n
    else:
        length_list = [int(length)] * n

    builder = string_vector_module.StringVectorBuilder.with_estimate(n, 8)

    for i in range(n):
        if null_bm != NULL and not ((null_bm[i >> 3] >> (i & 7)) & 1):
            builder.append_null()
        else:
            start = ptr.offsets[i]
            end = ptr.offsets[i + 1]
            row_len = end - start
            slice_len = int(length_list[i])
            if slice_len < 0:
                slice_len = max(0, row_len + slice_len)
            if slice_len > row_len:
                slice_len = row_len
            builder.append_bytes(<const char*>ptr.data + start, slice_len)

    return builder.finish()


cpdef StringVector list_string_slice_right(StringVector vec, object length):
    """
    Slice each string from the right (end) keeping 'length' bytes.

    Parameters:
        vec: StringVector of strings.
        length: int scalar or iterable of ints — number of bytes to keep from the right.

    Returns:
        StringVector: sliced strings.
    """
    cdef DrakenVarBuffer* ptr = vec.ptr
    cdef Py_ssize_t n = ptr.length
    cdef uint8_t* null_bm = ptr.null_bitmap
    cdef Py_ssize_t i
    cdef int32_t start, end, row_len
    cdef int slice_len, actual_start

    cdef list length_list
    if hasattr(length, "__iter__") and not isinstance(length, (str, bytes)):
        try:
            length_list = list(length)
        except TypeError:
            length_list = [int(length)] * n
    else:
        length_list = [int(length)] * n

    builder = string_vector_module.StringVectorBuilder.with_estimate(n, 8)

    for i in range(n):
        if null_bm != NULL and not ((null_bm[i >> 3] >> (i & 7)) & 1):
            builder.append_null()
        else:
            start = ptr.offsets[i]
            end = ptr.offsets[i + 1]
            row_len = end - start
            slice_len = int(length_list[i])
            if slice_len < 0:
                slice_len = 0
            if slice_len > row_len:
                slice_len = row_len
            actual_start = row_len - slice_len
            builder.append_bytes(
                <const char*>ptr.data + start + actual_start, slice_len
            )

    return builder.finish()
