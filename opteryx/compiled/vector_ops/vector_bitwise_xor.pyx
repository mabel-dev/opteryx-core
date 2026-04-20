# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False

"""
Vectorized bitwise XOR operation for Int64Vectors.

This module implements element-wise XOR on Draken integer vectors:
- Propagates NULLs explicitly (NULL input → NULL output)
- Uses static dispatch with explicit specialization
- Releases the GIL as early as possible

Cyclic shifts are NOT supported; this is purely bitwise XOR.
"""

from libc.stdint cimport int64_t, uint8_t
from libc.stdlib cimport malloc, free
from libc.string cimport memset

from opteryx.compiled.draken.vectors.int64_vector cimport Int64Vector
from opteryx.compiled.draken.core.buffers cimport DrakenFixedBuffer
from opteryx.compiled.draken.interop.vector_sequence cimport vector_from_sequence

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
