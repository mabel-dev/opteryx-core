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
Cast Integer64Vector or dictionary-encoded Integer64Vector to TimestampVector with unit conversion.

Transforms only dictionary values when dictionary-encoded, preserving codes/indices.
"""

from libc.stdint cimport int64_t, uint8_t, uint32_t, int32_t
from libc.stdlib cimport malloc, free
from libc.string cimport memcpy

from draken.vectors.integer64_vector cimport Integer64Vector
from draken.vectors.timestamp_vector cimport TimestampVector, timestamp_dict_from_raw
from draken.core.buffers cimport DrakenFixedBuffer, DrakenVarBuffer, DrakenVector, DRAKEN_TIMESTAMP64
from draken.core.var_vector cimport alloc_var_buffer

# C++ SIMD dispatch functions
cdef extern from "../../src/cpp/simd_timestamp_cast.h":
    void multiply_int64_simd(const int64_t* src, int64_t* dst, int64_t factor, size_t n) nogil
    void divide_int64_simd(const int64_t* src, int64_t* dst, int64_t divisor, size_t n) nogil


cpdef TimestampVector vector_cast_int64_to_timestamp(Integer64Vector int_vec, str unit="us"):
    """
    Cast Integer64Vector (dense or dictionary-encoded) to TimestampVector.

    For dictionary-encoded vectors: transforms only the dictionary, preserving codes.

    Args:
        int_vec: Integer64Vector (may be dictionary-encoded)
        unit: Unit of input values: 'ms', 's', 'us', 'ns', or 'days'

    Returns:
        TimestampVector (dictionary-encoded if input was)
    """
    cdef int64_t factor
    cdef bint use_divide = False

    if unit == "ms":
        factor = 1000
    elif unit == "s":
        factor = 1_000_000
    elif unit == "us":
        factor = 1
    elif unit == "ns":
        use_divide = True
        factor = 1000
    elif unit == "days":
        factor = 86_400_000_000
    else:
        raise ValueError(f"Unsupported timestamp unit: {unit!r}")

    cdef DrakenVector* uv = int_vec.unified()

    # Check if vector is dictionary-encoded
    if int_vec._unified_view.data_length < int_vec._unified_view.length:  # dictionary
        return _cast_dict_encoded(int_vec, uv, factor, use_divide)
    else:
        return _cast_dense(int_vec, factor, use_divide)


cdef TimestampVector _cast_dense(Integer64Vector int_vec, int64_t factor, bint use_divide):
    """Transform dense Integer64Vector to TimestampVector."""
    cdef DrakenFixedBuffer* ptr = int_vec.ptr
    cdef int64_t length = ptr.length
    cdef int64_t* int_data = <int64_t*>ptr.data
    cdef uint8_t* null_bitmap = ptr.null_bitmap

    cdef TimestampVector result = TimestampVector(length)
    cdef int64_t* ts_data = <int64_t*>result.ptr.data
    cdef uint8_t* result_null = result.ptr.null_bitmap

    if null_bitmap != NULL and result_null != NULL:
        memcpy(result_null, null_bitmap, (length + 7) >> 3)

    if factor == 1:
        memcpy(ts_data, int_data, length * sizeof(int64_t))
    elif use_divide:
        divide_int64_simd(int_data, ts_data, factor, length)
    else:
        multiply_int64_simd(int_data, ts_data, factor, length)

    result.timestamp_unit = "us"
    return result


cdef TimestampVector _cast_dict_encoded(Integer64Vector int_vec, DrakenVector* uv, int64_t factor, bint use_divide):
    """
    Transform dictionary-encoded Integer64Vector by casting only the dictionary.

    Returns dictionary-encoded TimestampVector with transformed dictionary, same codes.
    ptr.data is NULL (pure dict encoding — no dense materialization).
    """
    # Unified view: dict values in uv.data (raw int64_t), codes in uv.selection.
    # Per-dict-entry nulls are no longer stored separately; they're materialized
    # into the row-level validity bitmap (uv.validity) at decode time.
    cdef int64_t* dict_data = <int64_t*>uv.data
    cdef int64_t dict_size = <int64_t>uv.data_length
    cdef uint8_t* dict_nulls = NULL

    cdef const uint32_t* src_codes = uv.selection
    cdef int64_t num_rows = <int64_t>uv.length

    cdef int64_t i
    cdef DrakenVarBuffer* result_dict
    cdef int64_t* transformed_dict
    cdef uint32_t* codes_copy
    cdef Py_ssize_t code_bytes

    transformed_dict = <int64_t*>malloc(dict_size * sizeof(int64_t))
    if transformed_dict == NULL:
        raise MemoryError()

    try:
        # Transform only the dictionary values (SIMD dispatch)
        if factor == 1:
            memcpy(transformed_dict, dict_data, dict_size * sizeof(int64_t))
        elif use_divide:
            divide_int64_simd(dict_data, transformed_dict, factor, dict_size)
        else:
            multiply_int64_simd(dict_data, transformed_dict, factor, dict_size)

        # Copy uint32 codes
        code_bytes = num_rows * sizeof(uint32_t)
        if code_bytes > 0:
            codes_copy = <uint32_t*>malloc(code_bytes)
            if codes_copy == NULL:
                raise MemoryError()
            memcpy(codes_copy, src_codes, <size_t>code_bytes)
        else:
            codes_copy = NULL

        # Build dictionary storage (DrakenVarBuffer)
        result_dict = alloc_var_buffer(DRAKEN_TIMESTAMP64, <size_t>dict_size, <size_t>(dict_size * sizeof(int64_t)))
        result_dict.offsets[0] = 0
        for i in range(dict_size):
            result_dict.offsets[i + 1] = <int32_t>((i + 1) * sizeof(int64_t))
        memcpy(result_dict.data, transformed_dict, <size_t>(dict_size * sizeof(int64_t)))
        if dict_nulls != NULL:
            memcpy(result_dict.null_bitmap, dict_nulls, (dict_size + 7) >> 3)

        return timestamp_dict_from_raw(
            num_rows, codes_copy, result_dict,
            0, uv.validity, "us",
        )
    finally:
        free(transformed_dict)
