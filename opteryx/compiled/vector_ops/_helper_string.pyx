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
from libc.stdint cimport uint16_t, uint32_t
from libc.string cimport memset
from libc.stdlib cimport malloc, free


cdef inline uint8_t _sv_ascii_lower(uint8_t b) noexcept nogil:
    """Convert ASCII byte to lowercase. Unsigned arithmetic handles non-ASCII."""
    return b + (32 * ((b - 65U) <= 25U))


cdef inline uint32_t _decode_dict_code(const uint8_t* codes, uint8_t code_width, Py_ssize_t row_idx) noexcept:
    """Decode a packed dict code for row_idx (code_width: 1, 2, or 4 bytes).

    Shared across all string kernels that walk dict-encoded StringVectors.
    """
    if code_width == 1:
        return codes[row_idx]
    if code_width == 2:
        return (<const uint16_t*>codes)[row_idx]
    return (<const uint32_t*>codes)[row_idx]


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
