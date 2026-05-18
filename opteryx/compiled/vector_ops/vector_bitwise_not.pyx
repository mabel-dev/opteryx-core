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
Vectorized bitwise NOT operation for Integer64Vectors.

This module implements bitwise NOT (one's complement) on Integer64Vector elements:
- Inverts all bits of each element
- Propagates NULLs explicitly (NULL input → NULL output)
- Uses static dispatch with explicit specialization
- Releases the GIL as early as possible

Note: This module is part of the bitwise operations family.
See: vector_bitwise_or.pyx, vector_bitwise_and.pyx, vector_bitwise_xor.pyx,
     vector_bitwise_shift_left.pyx, vector_bitwise_shift_right.pyx
"""

from libc.stdint cimport int64_t, uint8_t

from draken.vectors.integer64_vector cimport Integer64Vector
from draken.core.buffers cimport DrakenVector, DrakenFixedBuffer
from draken.interop.vector_sequence cimport vector_from_sequence


cpdef object vector_bitwise_not(Integer64Vector operand):
    """Bitwise NOT (complement) an Integer64Vector element-wise.

    Performs bitwise NOT on each element: inverts all bits.
    NULL propagates from the input (any NULL → NULL output).

    Parameters:
        operand: Integer64Vector operand.

    Returns:
        Integer64Vector with bitwise NOT result.

    Example:
        >>> ~1  (00...01) → ~1 = -2 (11...10 in two's complement)
        >>> ~-1 (11...11) → ~(-1) = 0 (00...00 in two's complement)

    Note:
        Python's ~ operator on integers implements two's complement NOT.
        The result follows Python semantics: ~x = -x - 1
    """
    cdef DrakenVector* uv = operand.unified()
    cdef Py_ssize_t n = <Py_ssize_t>uv.length

    # Handle constant-encoded vector
    if uv.data_length == 1:  # constant
        if uv.validity != NULL:  # null constant
            return Integer64Vector.from_constant(None, n, is_null=True)
        return Integer64Vector.from_constant(~(<int64_t*>uv.data)[0], n)

    cdef DrakenFixedBuffer* op = operand.ptr
    cdef int64_t* op_data = <int64_t*>op.data
    cdef uint8_t* op_null = op.null_bitmap

    cdef list result = []
    cdef Py_ssize_t i

    for i in range(n):
        if _is_null(op_null, i):
            result.append(None)
        else:
            result.append(~op_data[i])

    return vector_from_sequence(result)
