# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False
# cython: optimize.use_switch=True
# cython: optimize.unpack_method_calls=True

"""
Native bitwise AND helper for Draken integer vectors.

This module implements vectorized bitwise AND operations on Integer64Vector:
- Bitwise AND

All operations:
- Operate natively on Draken buffers
- Propagate NULLs explicitly (NULL input → NULL output)
- Use static dispatch with explicit specialization
- Release the GIL as early as possible
"""

from libc.stdint cimport int64_t, uint8_t
from libc.stdlib cimport malloc, free
from libc.string cimport memset

from draken.vectors.integer64_vector cimport Integer64Vector
from draken.core.buffers cimport DrakenFixedBuffer
from draken.interop.vector_sequence cimport vector_from_sequence



cpdef object vector_bitwise_shift_left(Integer64Vector left, Integer64Vector right):
    """Left shift Integer64Vector elements by positions specified in right.

    Arithmetic left shift (shift-left): shifts bits of left[i] left by right[i] positions.
    Equivalent to left[i] << right[i]. Vacated low-order bits are filled with zeros.
    NOT a circular shift - bits shifted out on the left are lost.
    Shift amounts are bounded to [0, 63] to avoid undefined behavior.

    Parameters:
        left: Integer64Vector values to shift.
        right: Integer64Vector shift distances (in bits).

    Returns:
        Integer64Vector with left-shifted result. NULL propagates from either input.
        Out-of-range shift amounts (< 0 or > 63) produce NULL.

    Raises:
        ValueError: If vectors have mismatched lengths.
    """
    cdef DrakenFixedBuffer* lp = left.ptr
    cdef DrakenFixedBuffer* rp = right.ptr
    cdef Py_ssize_t n = lp.length

    if n != <Py_ssize_t>rp.length:
        raise ValueError("Left shift: mismatched vector lengths")

    cdef int64_t* l_data = <int64_t*>lp.data
    cdef int64_t* r_data = <int64_t*>rp.data
    cdef uint8_t* l_null = lp.null_bitmap
    cdef uint8_t* r_null = rp.null_bitmap

    cdef list result = []
    cdef Py_ssize_t i
    cdef int64_t shift_amount

    for i in range(n):
        if _is_null(l_null, i) or _is_null(r_null, i):
            result.append(None)
        else:
            shift_amount = r_data[i]
            if shift_amount < 0 or shift_amount > 63:
                result.append(None)
            else:
                result.append(l_data[i] << shift_amount)

    return vector_from_sequence(result)
