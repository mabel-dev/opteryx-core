# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False
# cython: optimize.use_switch=True
# cython: optimize.unpack_method_calls=True

# Shared helper functions for constant-encoded string vectors
# Used by vector_trim, vector_uppercase, vector_lowercase, etc.

from draken.vectors.string_vector cimport StringVector
from draken.core.buffers cimport ConstAccessor, DrakenConstantStringPayload, DRAKEN_ENCODING_CONSTANT
from libc.stdint cimport int32_t, uint8_t


cdef inline ConstAccessor* _constant_string_accessor(StringVector vec) noexcept:
    if vec.encoding != DRAKEN_ENCODING_CONSTANT:
        return NULL
    return vec.const_accessor()


cdef inline bint _is_null(uint8_t* null_bitmap, Py_ssize_t idx) noexcept:
    """Check if element at idx is NULL in Draken null bitmap (bit=1 valid, bit=0 null)."""
    if null_bitmap == NULL:
        return False
    return not ((null_bitmap[idx >> 3] >> (idx & 7)) & 1)

cdef inline bint _constant_string_value(
    StringVector vec,
    const uint8_t** data_ptr,
    int32_t* data_len,
    Py_ssize_t* row_count,
) except? False:
    cdef ConstAccessor* accessor = _constant_string_accessor(vec)
    cdef DrakenConstantStringPayload* payload

    if accessor == NULL:
        return False

    row_count[0] = accessor.length
    if accessor.is_null != 0 or accessor.value_ptr == NULL:
        data_ptr[0] = NULL
        data_len[0] = 0
        return True

    payload = <DrakenConstantStringPayload*>accessor.value_ptr
    data_ptr[0] = payload.data
    data_len[0] = payload.length
    return True
