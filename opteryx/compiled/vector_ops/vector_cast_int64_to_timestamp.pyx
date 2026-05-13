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
Cast Int64Vector or dictionary-encoded Int64Vector to TimestampVector with unit conversion.

Transforms only dictionary values when dictionary-encoded, preserving codes/indices.
"""

from libc.stdint cimport int64_t, uint8_t, uint16_t, uint32_t, int32_t
from libc.stdlib cimport malloc, free
from libc.string cimport memcpy

from draken.vectors.int64_vector cimport Int64Vector
from draken.vectors.timestamp_vector cimport TimestampVector
from draken.core.buffers cimport DrakenFixedBuffer, DrakenVarBuffer, DRAKEN_ENCODING_DICTIONARY, DRAKEN_TIMESTAMP64
from draken.core.var_vector cimport alloc_var_buffer

# C++ SIMD dispatch functions
cdef extern from "../../src/cpp/simd_timestamp_cast.h":
    void multiply_int64_simd(const int64_t* src, int64_t* dst, int64_t factor, size_t n) nogil
    void divide_int64_simd(const int64_t* src, int64_t* dst, int64_t divisor, size_t n) nogil


cpdef TimestampVector vector_cast_int64_to_timestamp(Int64Vector int_vec, str unit="us"):
    """
    Cast Int64Vector (dense or dictionary-encoded) to TimestampVector.

    For dictionary-encoded vectors: transforms only the dictionary, preserving codes.

    Args:
        int_vec: Int64Vector (may be dictionary-encoded)
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

    # Check if vector is dictionary-encoded
    if int_vec._dict_codes != NULL:
        # Dictionary-encoded path: transform only dictionary values
        return _cast_dict_encoded(int_vec, factor, use_divide)
    else:
        # Dense path
        return _cast_dense(int_vec, factor, use_divide)


cdef TimestampVector _cast_dense(Int64Vector int_vec, int64_t factor, bint use_divide):
    """Transform dense Int64Vector to TimestampVector."""
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


cdef TimestampVector _cast_dict_encoded(Int64Vector int_vec, int64_t factor, bint use_divide):
    """
    Transform dictionary-encoded Int64Vector by casting only the dictionary.

    Returns dictionary-encoded TimestampVector with transformed dictionary, same codes.
    """
    cdef DrakenVarBuffer* dict_buf = int_vec._dict_values
    cdef int64_t* dict_data = <int64_t*>dict_buf.data
    cdef int64_t dict_size = dict_buf.length
    cdef uint8_t* dict_nulls = dict_buf.null_bitmap

    cdef uint8_t* codes_raw = int_vec._dict_codes
    cdef uint8_t code_width = int_vec._dict_code_width
    cdef int64_t num_rows = int_vec.ptr.length
    cdef uint8_t* row_nulls = int_vec.ptr.null_bitmap

    cdef int64_t i
    cdef uint32_t code
    cdef int64_t bitmap_bytes
    cdef TimestampVector result
    cdef int64_t* result_data
    cdef uint8_t* result_nulls
    cdef DrakenVarBuffer* result_dict
    cdef int64_t* transformed_dict
    cdef uint8_t* packed_codes
    cdef Py_ssize_t code_bytes

    # Allocate and transform dictionary
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

        # Create new TimestampVector and materialize with transformed dictionary
        result = TimestampVector(<size_t>num_rows)
        result.timestamp_unit = "us"
        result._unit_code = 1  # UNIT_US
        result_data = <int64_t*>result.ptr.data

        # Copy null bitmap if present
        if row_nulls != NULL:
            bitmap_bytes = (num_rows + 7) >> 3
            result.ptr.null_bitmap = <uint8_t*>malloc(bitmap_bytes)
            if result.ptr.null_bitmap == NULL:
                raise MemoryError()
            memcpy(result.ptr.null_bitmap, row_nulls, <size_t>bitmap_bytes)

        # Materialize by looking up transformed dictionary values using codes
        for i in range(num_rows):
            if row_nulls != NULL and ((row_nulls[i >> 3] >> (i & 7)) & 1) == 0:
                result_data[i] = 0
                continue
            # Read packed code with variable width
            if code_width == 1:
                code = <uint32_t>(<uint8_t*>codes_raw)[i]
            elif code_width == 2:
                code = <uint32_t>(<uint16_t*>codes_raw)[i]
            else:
                code = <uint32_t>(<uint32_t*>codes_raw)[i]
            result_data[i] = transformed_dict[code]

        # Allocate and copy packed codes
        code_bytes = num_rows * code_width
        if code_bytes > 0:
            packed_codes = <uint8_t*>malloc(code_bytes)
            if packed_codes == NULL:
                raise MemoryError()
            memcpy(packed_codes, codes_raw, <size_t>code_bytes)
        else:
            packed_codes = NULL

        # Set dictionary encoding fields
        result._dict_codes = packed_codes
        result._dict_code_width = code_width
        result._dict_ordered = int_vec._dict_ordered

        # Create dictionary storage (DrakenVarBuffer)
        result_dict = alloc_var_buffer(DRAKEN_TIMESTAMP64, <size_t>dict_size, <size_t>(dict_size * sizeof(int64_t)))
        result_dict.offsets[0] = 0
        for i in range(dict_size):
            result_dict.offsets[i + 1] = <int32_t>((i + 1) * sizeof(int64_t))
        memcpy(result_dict.data, transformed_dict, <size_t>(dict_size * sizeof(int64_t)))
        if dict_nulls != NULL:
            memcpy(result_dict.null_bitmap, dict_nulls, (dict_size + 7) >> 3)

        result._dict_values = result_dict
        result._encoding = DRAKEN_ENCODING_DICTIONARY

        return result
    finally:
        free(transformed_dict)
