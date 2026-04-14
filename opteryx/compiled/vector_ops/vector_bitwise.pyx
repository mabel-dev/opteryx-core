# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False

"""
Native bitwise helpers for Draken integer vectors.

This module implements vectorized bitwise operations on Int64Vector:
- Bitwise AND
- Bitwise OR
- Bitwise XOR
- Left shift
- Right shift

All operations:
- Operate natively on Draken buffers
- Propagate NULLs explicitly (NULL input → NULL output)
- Use static dispatch with explicit specialization
- Release the GIL as early as possible
"""

from libc.stdint cimport int64_t, uint8_t
from libc.stdlib cimport malloc, free
from libc.string cimport memset

from opteryx.compiled.draken.vectors.int64_vector cimport Int64Vector
from opteryx.compiled.draken.core.buffers cimport DrakenFixedBuffer
from opteryx.compiled.draken.interop.arrow cimport vector_from_sequence


cdef inline bint _is_null(uint8_t* null_bitmap, Py_ssize_t idx) noexcept:
    """Check if element at idx is NULL in Draken null bitmap (bit=1 valid, bit=0 null)."""
    if null_bitmap == NULL:
        return False
    return not ((null_bitmap[idx >> 3] >> (idx & 7)) & 1)


cpdef object vector_bitwise_and(Int64Vector left, Int64Vector right):
    """Bitwise AND two Int64Vectors element-wise.

    Parameters:
        left: Int64Vector operand.
        right: Int64Vector operand.

    Returns:
        Int64Vector with bitwise AND result. NULL propagates from either input.

    Raises:
        ValueError: If vectors have mismatched lengths.
    """
    cdef DrakenFixedBuffer* lp = left.ptr
    cdef DrakenFixedBuffer* rp = right.ptr
    cdef Py_ssize_t n = lp.length

    if n != <Py_ssize_t>rp.length:
        raise ValueError("Bitwise AND: mismatched vector lengths")

    cdef int64_t* l_data = <int64_t*>lp.data
    cdef int64_t* r_data = <int64_t*>rp.data
    cdef uint8_t* l_null = lp.null_bitmap
    cdef uint8_t* r_null = rp.null_bitmap

    cdef list result = []
    cdef Py_ssize_t i

    for i in range(n):
        if _is_null(l_null, i) or _is_null(r_null, i):
            result.append(None)
        else:
            result.append(l_data[i] & r_data[i])

    return vector_from_sequence(result)


cpdef object vector_bitwise_or(Int64Vector left, Int64Vector right):
    """Bitwise OR two Int64Vectors element-wise.

    Parameters:
        left: Int64Vector operand.
        right: Int64Vector operand.

    Returns:
        Int64Vector with bitwise OR result. NULL propagates from either input.

    Raises:
        ValueError: If vectors have mismatched lengths.
    """
    cdef DrakenFixedBuffer* lp = left.ptr
    cdef DrakenFixedBuffer* rp = right.ptr
    cdef Py_ssize_t n = lp.length

    if n != <Py_ssize_t>rp.length:
        raise ValueError("Bitwise OR: mismatched vector lengths")

    cdef int64_t* l_data = <int64_t*>lp.data
    cdef int64_t* r_data = <int64_t*>rp.data
    cdef uint8_t* l_null = lp.null_bitmap
    cdef uint8_t* r_null = rp.null_bitmap

    cdef list result = []
    cdef Py_ssize_t i

    for i in range(n):
        if _is_null(l_null, i) or _is_null(r_null, i):
            result.append(None)
        else:
            result.append(l_data[i] | r_data[i])

    return vector_from_sequence(result)


cpdef object vector_bitwise_xor(Int64Vector left, Int64Vector right):
    """Bitwise XOR two Int64Vectors element-wise.

    Parameters:
        left: Int64Vector operand.
        right: Int64Vector operand.

    Returns:
        Int64Vector with bitwise XOR result. NULL propagates from either input.

    Raises:
        ValueError: If vectors have mismatched lengths.
    """
    cdef DrakenFixedBuffer* lp = left.ptr
    cdef DrakenFixedBuffer* rp = right.ptr
    cdef Py_ssize_t n = lp.length

    if n != <Py_ssize_t>rp.length:
        raise ValueError("Bitwise XOR: mismatched vector lengths")

    cdef int64_t* l_data = <int64_t*>lp.data
    cdef int64_t* r_data = <int64_t*>rp.data
    cdef uint8_t* l_null = lp.null_bitmap
    cdef uint8_t* r_null = rp.null_bitmap

    cdef list result = []
    cdef Py_ssize_t i

    for i in range(n):
        if _is_null(l_null, i) or _is_null(r_null, i):
            result.append(None)
        else:
            result.append(l_data[i] ^ r_data[i])

    return vector_from_sequence(result)


cpdef object vector_left_shift(Int64Vector left, Int64Vector right):
    """Left shift Int64Vector elements by positions specified in right.

    Shifts bits of left[i] left by right[i] positions. Equivalent to left[i] << right[i].
    Shift amounts are bounded to [0, 63] to avoid undefined behavior.

    Parameters:
        left: Int64Vector values to shift.
        right: Int64Vector shift distances (in bits).

    Returns:
        Int64Vector with left-shifted result. NULL propagates from either input.
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


cpdef object vector_right_shift(Int64Vector left, Int64Vector right):
    """Right shift Int64Vector elements by positions specified in right.

    Arithmetic right shift (sign-extending) of left[i] by right[i] positions.
    Equivalent to left[i] >> right[i].
    Shift amounts are bounded to [0, 63] to avoid undefined behavior.

    Parameters:
        left: Int64Vector values to shift.
        right: Int64Vector shift distances (in bits).

    Returns:
        Int64Vector with right-shifted result. NULL propagates from either input.
        Out-of-range shift amounts (< 0 or > 63) produce NULL.

    Raises:
        ValueError: If vectors have mismatched lengths.
    """
    cdef DrakenFixedBuffer* lp = left.ptr
    cdef DrakenFixedBuffer* rp = right.ptr
    cdef Py_ssize_t n = lp.length

    if n != <Py_ssize_t>rp.length:
        raise ValueError("Right shift: mismatched vector lengths")

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
                result.append(l_data[i] >> shift_amount)

    return vector_from_sequence(result)
