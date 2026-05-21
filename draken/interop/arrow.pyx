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
Arrow export helpers for Draken columnar buffers.

This module provides:
- Functions to expose DrakenFixedBuffer as ArrowArray and ArrowSchema
- Memory management utilities for Arrow C Data Interface structs

Draken does not ingest PyArrow (no from_arrow): data enters via the native
columnar path or from typed sequences. This module is export-only plus a few
type-mapping utilities.
"""

from libc.stdlib cimport free
from libc.stdlib cimport malloc

from draken.core.buffers cimport DrakenFixedBuffer
from draken.core.buffers cimport DrakenType
from draken.interop.arrow_c_data_interface cimport ARROW_FLAG_NULLABLE
from draken.interop.arrow_c_data_interface cimport ArrowArray
from draken.interop.arrow_c_data_interface cimport ArrowSchema
from draken.interop.vector_sequence cimport vector_from_sequence as generic_vector_from_sequence


cdef void release_arrow_array(ArrowArray* arr) noexcept:
    free(<void*>arr.buffers)
    free(arr)

cdef void release_arrow_schema(ArrowSchema* schema) noexcept:
    free(schema)

cdef void expose_draken_fixed_as_arrow(
    DrakenFixedBuffer* vec,
    ArrowArray** out_array,
    ArrowSchema** out_schema,
):
    cdef ArrowArray* arr = <ArrowArray*>malloc(sizeof(ArrowArray))
    cdef ArrowSchema* schema = <ArrowSchema*>malloc(sizeof(ArrowSchema))
    out_array[0] = arr
    out_schema[0] = schema

    # Fill ArrowArray
    arr.length = vec.length
    arr.null_count = -1
    arr.offset = 0
    arr.n_buffers = 2
    arr.n_children = 0
    arr.children = NULL
    arr.dictionary = NULL
    arr.release = release_arrow_array
    arr.private_data = NULL

    arr.buffers = <const void**>malloc(2 * sizeof(void*))
    arr.buffers[0] = <const void*>vec.null_bitmap
    arr.buffers[1] = vec.data

    # Fill ArrowSchema
    schema.format = b"l"
    schema.name = NULL
    schema.metadata = NULL
    schema.flags = ARROW_FLAG_NULLABLE if vec.null_bitmap != NULL else 0
    schema.n_children = 0
    schema.children = NULL
    schema.dictionary = NULL
    schema.release = release_arrow_schema
    schema.private_data = NULL


cdef object _orso_type_to_arrow(object orso_type):
    """Convert OrsoTypes enum to PyArrow type."""
    import pyarrow as pa
    from opteryx.types import OrsoTypes

    if orso_type is None:
        return None

    # Map OrsoTypes to PyArrow types
    type_map = {
        OrsoTypes.NULL: pa.null(),
        OrsoTypes.BOOLEAN: pa.bool_(),
        OrsoTypes.INTEGER: pa.int64(),
        OrsoTypes.DOUBLE: pa.float64(),
        OrsoTypes.VARCHAR: pa.string(),
        OrsoTypes.BLOB: pa.binary(),
        OrsoTypes.DATE: pa.date32(),
        OrsoTypes.TIMESTAMP: pa.timestamp('us'),
        OrsoTypes.INTERVAL: pa.duration('us'),
        OrsoTypes.DECIMAL: pa.decimal128(18, 10),
        OrsoTypes.ARRAY: pa.list_(pa.null()),
    }

    return type_map.get(orso_type, None)


cpdef object vector_from_sequence(object data, object dtype=None):
    return generic_vector_from_sequence(data, dtype)


cpdef DrakenType arrow_type_to_draken(object dtype):
    """
    Convert a PyArrow DataType to a DrakenType enum.
    Raises TypeError if unsupported.
    """
    import pyarrow as pa

    if pa.types.is_int8(dtype):
        return DrakenType.DRAKEN_INT8
    elif pa.types.is_int16(dtype):
        return DrakenType.DRAKEN_INT16
    elif pa.types.is_int32(dtype):
        return DrakenType.DRAKEN_INT32
    elif pa.types.is_int64(dtype):
        return DrakenType.DRAKEN_INT64
    elif pa.types.is_uint64(dtype):
        # Treat uint64 as int64 (reinterpret bits as signed)
        return DrakenType.DRAKEN_INT64
    elif pa.types.is_float32(dtype):
        return DrakenType.DRAKEN_FLOAT32
    elif pa.types.is_float64(dtype):
        return DrakenType.DRAKEN_FLOAT64
    elif pa.types.is_date32(dtype):
        return DrakenType.DRAKEN_DATE32
    elif pa.types.is_timestamp(dtype):
        return DrakenType.DRAKEN_TIMESTAMP64
    elif pa.types.is_interval(dtype):
        return DrakenType.DRAKEN_INTERVAL
    elif pa.types.is_boolean(dtype):
        return DrakenType.DRAKEN_BOOL
    elif pa.types.is_string(dtype) or pa.types.is_large_string(dtype) or pa.types.is_binary(dtype) or pa.types.is_large_binary(dtype):
        return DrakenType.DRAKEN_STRING
    elif pa.types.is_dictionary(dtype):
        if (
            pa.types.is_string(dtype.value_type)
            or pa.types.is_binary(dtype.value_type)
            or pa.types.is_int8(dtype.value_type)
            or pa.types.is_int16(dtype.value_type)
            or pa.types.is_int32(dtype.value_type)
            or pa.types.is_int64(dtype.value_type)
            or pa.types.is_uint64(dtype.value_type)
            or pa.types.is_float32(dtype.value_type)
            or pa.types.is_float64(dtype.value_type)
        ):
            return DrakenType.DRAKEN_DICTIONARY
        return DrakenType.DRAKEN_NON_NATIVE
    elif pa.types.is_list(dtype) or pa.types.is_large_list(dtype) or pa.types.is_fixed_size_list(dtype):
        return DrakenType.DRAKEN_ARRAY
    elif pa.types.is_fixed_size_binary(dtype) and dtype.byte_width == 16:
        return DrakenType.DRAKEN_INTERVAL

    return DrakenType.DRAKEN_NON_NATIVE
