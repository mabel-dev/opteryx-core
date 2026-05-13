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
Native bitwise helpers for Draken integer vectors.

This module implements vectorized bitwise OR operation on Int64Vector:
- Bitwise OR

All operations:
- Operate natively on Draken buffers
- Propagate NULLs explicitly (NULL input → NULL output)
- Use static dispatch with explicit specialization
- Release the GIL as early as possible
"""

from libc.stdint cimport int64_t, uint8_t
from libc.stdlib cimport malloc, free
from libc.string cimport memset

from draken.vectors.int64_vector cimport Int64Vector
from draken.core.buffers cimport DrakenFixedBuffer
from draken.interop.vector_sequence cimport vector_from_sequence


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
