# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False
# cython: optimize.use_switch=True
# cython: optimize.unpack_method_calls=True

from cpython.bytes cimport PyBytes_FromStringAndSize
from libc.stdint cimport int32_t, uint8_t

from draken.core.buffers cimport DrakenArrayBuffer, DrakenVarBuffer
from draken.vectors.array_vector cimport ArrayVector
from draken.vectors.string_vector cimport StringVector
from draken.vectors import string_vector as string_vector_module

# NOTE: DrakenArrayBuffer.values is NEVER set for ArrayVectors.
# The child is stored in the Python-level `_child` attribute as a StringVector.
# Always access arr._child, cast to StringVector, then use .ptr.


cdef inline bytes _child_bytes(DrakenVarBuffer* child_ptr, int32_t j):
    """Extract bytes for child element j from a DrakenVarBuffer."""
    cdef int32_t start = child_ptr.offsets[j]
    cdef int32_t end = child_ptr.offsets[j + 1]
    return bytes(<uint8_t*>child_ptr.data + start)[:end - start]


cpdef StringVector vector_concat_array(ArrayVector arr):
    """
    CONCAT(array_col): for each row, concatenate all child string elements with
    no separator. NULL rows produce NULL output; NULL child elements are skipped.

    Returns a StringVector of length == arr.length.
    """
    cdef DrakenArrayBuffer* arr_ptr = arr.ptr
    # ptr.values is NOT set for ArrayVectors — child lives in _child.
    cdef StringVector child_sv = <StringVector>arr._child
    cdef DrakenVarBuffer* child_ptr = child_sv.ptr
    cdef Py_ssize_t n = arr_ptr.length
    cdef uint8_t* row_null_bm = arr_ptr.null_bitmap
    cdef uint8_t* child_null_bm = child_ptr.null_bitmap
    cdef Py_ssize_t i
    cdef int32_t child_start, child_end, j

    builder = string_vector_module.StringVectorBuilder.with_estimate(n, 32)

    for i in range(n):
        # NULL list row
        if row_null_bm != NULL and not ((row_null_bm[i >> 3] >> (i & 7)) & 1):
            builder.append_null()
            continue

        child_start = arr_ptr.offsets[i]
        child_end = arr_ptr.offsets[i + 1]

        parts = []
        for j in range(child_start, child_end):
            # Skip NULL child elements
            if child_null_bm != NULL and not ((child_null_bm[j >> 3] >> (j & 7)) & 1):
                continue
            parts.append(_child_bytes(child_ptr, j))

        builder.append(b"".join(parts))

    return builder.finish()


cpdef StringVector vector_concat_ws_array(bytes sep, ArrayVector arr):
    """
    CONCAT_WS(sep, array_col): for each row, join child string elements with
    the given separator. NULL rows produce NULL output; NULL child elements are
    skipped (not included in the join).

    Returns a StringVector of length == arr.length.
    """
    cdef DrakenArrayBuffer* arr_ptr = arr.ptr
    # ptr.values is NOT set for ArrayVectors — child lives in _child.
    cdef StringVector child_sv = <StringVector>arr._child
    cdef DrakenVarBuffer* child_ptr = child_sv.ptr
    cdef Py_ssize_t n = arr_ptr.length
    cdef uint8_t* row_null_bm = arr_ptr.null_bitmap
    cdef uint8_t* child_null_bm = child_ptr.null_bitmap
    cdef Py_ssize_t i
    cdef int32_t child_start, child_end, j

    builder = string_vector_module.StringVectorBuilder.with_estimate(n, 32)

    for i in range(n):
        if row_null_bm != NULL and not ((row_null_bm[i >> 3] >> (i & 7)) & 1):
            builder.append_null()
            continue

        child_start = arr_ptr.offsets[i]
        child_end = arr_ptr.offsets[i + 1]

        parts = []
        for j in range(child_start, child_end):
            if child_null_bm != NULL and not ((child_null_bm[j >> 3] >> (j & 7)) & 1):
                continue
            parts.append(_child_bytes(child_ptr, j))

        builder.append(sep.join(parts))

    return builder.finish()


cpdef object vector_string_concat_binary(object left, object right):
    """
    CONCAT(left, right): element-wise string concatenation of two operands.
    NULL in either operand yields NULL in the output.

    Accepts StringVector (any encoding: dense, constant, dictionary) or
    Python bytes/str/None scalars as either operand.  Returns a StringVector
    when at least one input is a StringVector, or a Python bytes object when
    both inputs are scalars.
    """
    cdef Py_ssize_t n = 0
    cdef Py_ssize_t i
    cdef StringRow left_row, right_row
    cdef bint left_is_vec = isinstance(left, StringVector)
    cdef bint right_is_vec = isinstance(right, StringVector)
    cdef bytes left_scalar = None
    cdef bytes right_scalar = None
    cdef bint left_null_scalar = False
    cdef bint right_null_scalar = False
    cdef bytes lb, rb

    # Determine output length
    if left_is_vec:
        n = <Py_ssize_t>len(<StringVector>left)
    elif right_is_vec:
        n = <Py_ssize_t>len(<StringVector>right)
    else:
        # Both scalars — return a scalar result (or None if either is null)
        if left is None or right is None:
            return None
        lb = left if isinstance(left, bytes) else str(left).encode("utf-8")
        rb = right if isinstance(right, bytes) else str(right).encode("utf-8")
        return lb + rb

    # Normalise scalar side to bytes (or record it as null)
    if not left_is_vec:
        if left is None:
            left_null_scalar = True
        elif isinstance(left, bytes):
            left_scalar = left
        else:
            left_scalar = str(left).encode("utf-8")

    if not right_is_vec:
        if right is None:
            right_null_scalar = True
        elif isinstance(right, bytes):
            right_scalar = right
        else:
            right_scalar = str(right).encode("utf-8")

    builder = string_vector_module.StringVectorBuilder.with_estimate(n, 32)

    for i in range(n):
        # --- left operand ---
        if left_is_vec:
            left_row = string_vec_get_at(<StringVector>left, i)
            if left_row.is_null:
                builder.append_null()
                continue
            lb = PyBytes_FromStringAndSize(left_row.data, left_row.length)
        else:
            if left_null_scalar:
                builder.append_null()
                continue
            lb = left_scalar

        # --- right operand ---
        if right_is_vec:
            right_row = string_vec_get_at(<StringVector>right, i)
            if right_row.is_null:
                builder.append_null()
                continue
            rb = PyBytes_FromStringAndSize(right_row.data, right_row.length)
        else:
            if right_null_scalar:
                builder.append_null()
                continue
            rb = right_scalar

        builder.append(lb + rb)

    return builder.finish()
