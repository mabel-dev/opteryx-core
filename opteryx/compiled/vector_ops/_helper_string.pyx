# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False
# cython: optimize.use_switch=True
# cython: optimize.unpack_method_calls=True

"""Common helper functions for string vector operations."""

from draken.vectors.bool_vector cimport BoolVector
from libc.stdint cimport uint32_t
from libc.string cimport memset
from libc.stdlib cimport malloc, free


cdef inline uint8_t _sv_ascii_lower(uint8_t b) noexcept nogil:
    """Convert ASCII byte to lowercase. Unsigned arithmetic handles non-ASCII."""
    return b + (32 * ((b - 65U) <= 25U))


cdef BoolVector _constant_bool_result(Py_ssize_t n, bint value, bint has_nulls):
    """Create a constant BoolVector."""
    cdef BoolVector result = BoolVector(<size_t>n)
    cdef uint8_t* data = <uint8_t*>result.ptr.data
    cdef Py_ssize_t nbytes = (n + 7) >> 3

    if value:
        memset(data, 0xFF, nbytes)
        if (n & 7) != 0:
            data[nbytes - 1] = <uint8_t>((1 << (n & 7)) - 1)
    else:
        memset(data, 0, nbytes)

    if has_nulls:
        result.ptr.null_bitmap = <uint8_t*>malloc(nbytes)
        if result.ptr.null_bitmap == NULL:
            raise MemoryError()
        memset(result.ptr.null_bitmap, 0xFF, nbytes)
        if (n & 7) != 0:
            result.ptr.null_bitmap[nbytes - 1] = <uint8_t>((1 << (n & 7)) - 1)

    return result
