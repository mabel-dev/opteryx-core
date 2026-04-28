# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False

"""
Native bitwise shift-right helpers for Draken integer vectors.

This module implements vectorized right shift operations on Int64Vector:
- Bitwise arithmetic right shift (sign-extending)

Key implementation details:
- Uses arithmetic right shift (>>) which preserves the sign bit
- Shift amounts are bounded to [0, 63] to avoid undefined behavior
- NULL propagates from either input
- Uses static dispatch with explicit specialization
- Releases the GIL as early as possible
"""

from libc.stdint cimport int64_t, uint8_t
from libc.stdlib cimport malloc, free
from libc.string cimport memset

from draken.vectors.int64_vector cimport Int64Vector
from draken.core.buffers cimport DrakenFixedBuffer
from draken.interop.vector_sequence cimport vector_from_sequence


cpdef object vector_bitwise_shift_right(Int64Vector left, Int64Vector right):
    """
    Arithmetic right shift Int64Vector elements by positions specified in right.

    This performs an arithmetic right shift (sign-extending shift), not a logical
    or circular shift. The sign bit is preserved and propagated into the vacated
    high-order bits.

    For signed integers in two's complement representation:
      - right shift by n is approximately equivalent to floor(x / 2^n)
      - preserves the sign of the number
      - e.g., -8 >> 1 = -4 (not 2^63 - 4)

    Shift amounts are bounded to [0, 63] to avoid undefined behavior. Shift amounts
    outside this range produce NULL.

    Parameters:
        left: Int64Vector values to shift.
        right: Int64Vector shift distances (in bits).

    Returns:
        Int64Vector with right-shifted result. NULL propagates from either input.
        Out-of-range shift amounts (< 0 or > 63) produce NULL.

    Raises:
        ValueError: If vectors have mismatched lengths.

    Examples:
        >>> vector_bitwise_shift_right([16, 32, -16], [1, 1, 1])
        [8, 16, -8]
        >>> vector_bitwise_shift_right([1, -1], [63])
        [0, -1]   # -1 stays -1 (all bits set, sign bit propagates)
    """
    cdef DrakenFixedBuffer* lp = left.ptr
    cdef DrakenFixedBuffer* rp = right.ptr
    cdef Py_ssize_t n = lp.length

    if n != <Py_ssize_t>rp.length:
        raise ValueError("Arithmetic right shift: mismatched vector lengths")

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
                # Arithmetic right shift: preserves sign bit
                result.append(l_data[i] >> shift_amount)

    return vector_from_sequence(result)
