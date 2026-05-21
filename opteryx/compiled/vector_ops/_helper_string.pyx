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


cdef BoolVector _all_null_bool(Py_ssize_t n):
    """BoolVector with every logical row NULL.

    SQL: `x LIKE/RLIKE/InStr NULL` is NULL for every row regardless of `x`.
    The data bits are irrelevant (masked); the null bitmap is all-zero
    (0 == null per the DrakenVector validity convention).
    """
    cdef BoolVector result = BoolVector(<size_t>n)
    cdef Py_ssize_t nbytes = (n + 7) >> 3
    cdef uint8_t* res_null
    memset(<uint8_t*>result.ptr.data, 0, nbytes)
    if nbytes != 0:
        res_null = <uint8_t*>malloc(nbytes)
        if res_null == NULL:
            raise MemoryError()
        memset(res_null, 0, nbytes)
        # _set_null_bitmap updates BOTH ptr.null_bitmap and _unified_view.validity;
        # assigning ptr.null_bitmap alone leaves the cached unified view stale and
        # to_pylist() (which reads unified().validity) would miss the nulls.
        result._set_null_bitmap(res_null)
    return result


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
