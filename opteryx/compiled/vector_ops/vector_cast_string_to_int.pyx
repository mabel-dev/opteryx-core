# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False

from libc.stdint cimport int32_t, int64_t
from libc.stdlib cimport malloc, free

from libc.string cimport memset
from opteryx.compiled.draken.vectors.string_vector cimport StringVector, StringRow, string_vec_get_at
from opteryx.compiled.draken.vectors.int64_vector cimport Int64Vector, from_sequence


cdef inline int64_t parse_int64(const char* data, int32_t length) except -1:
    cdef int64_t value = 0
    cdef int sign = 1
    cdef int32_t i = 0
    cdef char c

    if length > 0 and data[0] == 45:  # '-'
        sign = -1
        i = 1

    while i < length:
        c = data[i]
        if c < 48 or c > 57:
            raise ValueError("Invalid digit in integer literal")
        value = value * 10 + (c - 48)
        i += 1

    return sign * value


cpdef Int64Vector vector_cast_bytes_to_int(StringVector vec):
    """Parse each element of a StringVector as a decimal integer.

    Directly produces an Int64Vector, avoiding PyArrow intermediaries and
    Python object overhead in the hot loop.
    """
    cdef Py_ssize_t n = vec.ptr.length
    if n == 0:
        return Int64Vector(0)

    cdef int64_t* out = <int64_t*> malloc(n * sizeof(int64_t))
    if out == NULL:
        raise MemoryError()

    cdef uint8_t* null_bitmap = NULL
    cdef Py_ssize_t nb_size = (n + 7) >> 3
    cdef Py_ssize_t i
    cdef StringRow row

    # Initialise null bitmap to all valid (1s) if input has nulls
    if vec.ptr.null_bitmap != NULL:
        null_bitmap = <uint8_t*> malloc(nb_size)
        if null_bitmap == NULL:
            free(out)
            raise MemoryError()
        memset(null_bitmap, 0xFF, nb_size)

    for i in range(n):
        row = string_vec_get_at(vec, i)
        if row.is_null:
            out[i] = 0
            if null_bitmap != NULL:
                null_bitmap[i >> 3] &= ~(1 << (i & 7))
        else:
            out[i] = parse_int64(row.data, <int32_t>row.length)

    # Wrap raw buffers into Int64Vector
    cdef int64_t[::1] view = <int64_t[:n]>out
    cdef Int64Vector result = from_sequence(view)

    # Attach null bitmap if we created one
    if null_bitmap != NULL:
        result.ptr.null_bitmap = null_bitmap
        # _arrow_null_buf anchors the memory to the Python object
        import pyarrow as pa
        result._arrow_null_buf = pa.py_buffer(<Py_ssize_t>null_bitmap, nb_size)

    return result


cpdef Int64Vector vector_cast_ascii_to_int(StringVector vec):
    """Same as vector_cast_bytes_to_int (StringVector is always UTF-8/ASCII)."""
    return vector_cast_bytes_to_int(vec)
