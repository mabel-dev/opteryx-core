# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False

from libc.stdint cimport int32_t, uint8_t

from opteryx.compiled.draken.vectors.string_vector cimport StringVector
from opteryx.compiled.draken.vectors.int64_vector cimport Int64Vector, from_sequence as int64_from_sequence
from opteryx.compiled.draken.vectors import string_vector as string_vector_module
from opteryx.compiled.draken.core.buffers cimport DrakenVarBuffer, DrakenConstantStringPayload

# ---------------------------------------------------------------------------
# additional utilities
# ---------------------------------------------------------------------------

cpdef Int64Vector vector_string_length(StringVector vec):
    """Return byte-length of each string in a StringVector."""
    cdef DrakenVarBuffer* ptr = vec.ptr
    cdef Py_ssize_t n = ptr.length
    cdef uint8_t* null_bm
    cdef numpy.ndarray[int64_t, ndim=1] result = numpy.zeros(n, dtype=numpy.int64)
    cdef int64_t[::1] rview = result
    cdef Py_ssize_t i
    cdef DrakenConstantStringPayload* const_val

    if vec._has_const:
        if not vec._const_is_null and vec._const_value != NULL:
            const_val = vec._const_value
            for i in range(n):
                rview[i] = const_val.length
        # else: all-null → zeros (already initialised)
        return int64_from_sequence(rview)

    null_bm = ptr.null_bitmap
    for i in range(n):
        if null_bm != NULL and not ((null_bm[i >> 3] >> (i & 7)) & 1):
            rview[i] = 0
        else:
            rview[i] = ptr.offsets[i + 1] - ptr.offsets[i]

    return int64_from_sequence(rview)

cpdef StringVector vector_string_slice_left(StringVector vec, object length):
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
    cdef uint8_t* null_bm
    cdef Py_ssize_t i
    cdef int32_t start, end, row_len
    cdef int slice_len
    cdef DrakenConstantStringPayload* const_val
    cdef int32_t const_len

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

    if vec._has_const:
        if vec._const_is_null or vec._const_value == NULL:
            for i in range(n):
                builder.append_null()
        else:
            const_val = vec._const_value
            const_len = const_val.length
            for i in range(n):
                slice_len = int(length_list[i])
                if slice_len < 0:
                    slice_len = max(0, const_len + slice_len)
                if slice_len > const_len:
                    slice_len = const_len
                builder.append_bytes(<const char*>const_val.data, slice_len)
        return builder.finish()

    null_bm = ptr.null_bitmap
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


cpdef StringVector vector_string_slice_right(StringVector vec, object length):
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
    cdef uint8_t* null_bm
    cdef Py_ssize_t i
    cdef int32_t start, end, row_len
    cdef int slice_len, actual_start
    cdef DrakenConstantStringPayload* const_val
    cdef int32_t const_len

    cdef list length_list
    if hasattr(length, "__iter__") and not isinstance(length, (str, bytes)):
        try:
            length_list = list(length)
        except TypeError:
            length_list = [int(length)] * n
    else:
        length_list = [int(length)] * n

    builder = string_vector_module.StringVectorBuilder.with_estimate(n, 8)

    if vec._has_const:
        if vec._const_is_null or vec._const_value == NULL:
            for i in range(n):
                builder.append_null()
        else:
            const_val = vec._const_value
            const_len = const_val.length
            for i in range(n):
                slice_len = int(length_list[i])
                if slice_len < 0:
                    slice_len = 0
                if slice_len > const_len:
                    slice_len = const_len
                actual_start = const_len - slice_len
                builder.append_bytes(<const char*>const_val.data + actual_start, slice_len)
        return builder.finish()

    null_bm = ptr.null_bitmap
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
