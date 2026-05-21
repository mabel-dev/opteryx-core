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

from cpython.array cimport array, clone
from libc.stdint cimport int64_t, uint8_t

from draken.vectors.integer64_vector cimport Integer64Vector, from_packed_dict as int64_from_packed_dict
from draken.core.buffers cimport DrakenVector


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
    cdef Py_ssize_t slot_count = <Py_ssize_t>uv.data_length
    cdef int64_t* data = <int64_t*>uv.data
    cdef Py_ssize_t i

    cdef array template = array('l')
    cdef array output_array = clone(template, slot_count, False)
    cdef int64_t* output_ptr = <int64_t*>output_array.data.as_longs

    for i in range(slot_count):
        output_ptr[i] = ~data[i]

    return int64_from_packed_dict(
        <uint8_t*>uv.selection,
        4,
        n,
        <const int64_t*>output_ptr,
        slot_count,
        uv.validity,
        False,
    )
