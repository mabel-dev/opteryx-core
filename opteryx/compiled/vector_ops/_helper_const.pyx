# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False
# cython: optimize.use_switch=True
# cython: optimize.unpack_method_calls=True

# Shared helper functions used by compiled vector_ops kernels.

from libc.stdint cimport uint8_t


cdef inline bint _is_null(uint8_t* null_bitmap, Py_ssize_t idx) noexcept:
    """Check if element at idx is NULL in Draken null bitmap (bit=1 valid, bit=0 null)."""
    if null_bitmap == NULL:
        return False
    return not ((null_bitmap[idx >> 3] >> (idx & 7)) & 1)
