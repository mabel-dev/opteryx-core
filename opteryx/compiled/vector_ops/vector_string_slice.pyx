# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False

from libc.stdint cimport int32_t, int64_t, uint8_t
from cpython.array cimport array, clone

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
    cdef int64_t[::1] rview
    cdef Py_ssize_t i
    cdef DrakenConstantStringPayload* const_val

    # Use array module to allocate data (Cython will manage it)
    cdef array template = array('q')  # 'q' = signed long long (int64)
    cdef array result_array = clone(template, n, False)
    rview = result_array

    if vec._has_const:
        if not vec._const_is_null and vec._const_value != NULL:
            const_val = vec._const_value
            for i in range(n):
                rview[i] = const_val.length
        else:
            # all-null → zeros
            for i in range(n):
                rview[i] = 0
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
        length: StringVector or int — number of bytes to keep.

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

    # Convert StringVector to list of integers
    cdef list length_list
    if isinstance(length, StringVector):
        # Convert StringVector elements to integers
        try:
            length_list = [int(str(s)) if s is not None else 0 for s in length]
        except (ValueError, TypeError):
            length_list = [0] * n
    elif hasattr(length, "__iter__") and not isinstance(length, (str, bytes)):
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
        length: StringVector or int — number of bytes to keep from the right.

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
    if isinstance(length, StringVector):
        # Convert StringVector elements to integers
        try:
            length_list = [int(str(s)) if s is not None else 0 for s in length]
        except (ValueError, TypeError):
            length_list = [0] * n
    elif hasattr(length, "__iter__") and not isinstance(length, (str, bytes)):
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
