# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False

"""
Arrow interoperability helpers for Draken columnar buffers.

This module provides:
- Functions to expose DrakenFixedBuffer as ArrowArray and ArrowSchema
- Memory management utilities for Arrow C Data Interface structs
- Conversion helpers for zero-copy Arrow integration

Used to enable efficient interchange between Draken and Apache Arrow for analytics and data science workflows.
"""

from libc.stdlib cimport free
from libc.stdlib cimport malloc
from libc.stdint cimport int64_t, uint8_t

from opteryx.compiled.draken.core.buffers cimport DrakenFixedBuffer
from opteryx.compiled.draken.core.buffers cimport DrakenType
from opteryx.compiled.draken.interop.arrow_c_data_interface cimport ARROW_FLAG_NULLABLE
from opteryx.compiled.draken.interop.arrow_c_data_interface cimport ArrowArray
from opteryx.compiled.draken.interop.arrow_c_data_interface cimport ArrowSchema
from opteryx.compiled.draken.vectors.bool_vector cimport from_arrow as bool_from_arrow
from opteryx.compiled.draken.vectors.float64_vector cimport from_arrow as float64_from_arrow
from opteryx.compiled.draken.vectors.int64_vector cimport from_arrow as int64_from_arrow
from opteryx.compiled.draken.vectors.integer_vector cimport from_arrow as integer_from_arrow
from opteryx.compiled.draken.vectors.string_vector cimport from_arrow as string_from_arrow
from opteryx.compiled.draken.vectors.string_vector cimport from_arrow_struct as string_from_arrow_struct
from opteryx.compiled.draken.vectors.date32_vector cimport from_arrow as date32_from_arrow
from opteryx.compiled.draken.vectors.timestamp_vector cimport from_arrow as timestamp_from_arrow
from opteryx.compiled.draken.vectors.time_vector cimport from_arrow as time_from_arrow
from opteryx.compiled.draken.vectors.interval_vector cimport (
    from_arrow_interval as interval_from_arrow_interval,
)
from opteryx.compiled.draken.vectors.interval_vector cimport (
    from_arrow_binary as interval_from_arrow_binary,
)
from opteryx.compiled.draken.vectors.array_vector cimport from_arrow as array_from_arrow
from opteryx.compiled.draken.vectors.vector_vector cimport from_arrow as vector_from_arrow_vector


from opteryx.compiled.draken.vectors.integer_vector cimport IntegerVector
from opteryx.compiled.draken.vectors.int64_vector cimport from_sequence as int64_from_sequence
from opteryx.compiled.draken.vectors.float64_vector cimport Float64Vector
from opteryx.compiled.draken.vectors.float64_vector cimport from_sequence as float64_from_sequence
from opteryx.compiled.draken.vectors.bool_vector cimport BoolVector
from opteryx.compiled.draken.vectors.bool_vector cimport from_sequence as bool_from_sequence
from opteryx.compiled.draken.vectors.scalar_constructors cimport from_sequence as constant_from_sequence
from opteryx.compiled.draken.vectors._decimal_vector cimport from_arrow as decimal_from_arrow
from opteryx.compiled.draken.interop.vector_sequence cimport vector_from_sequence as generic_vector_from_sequence

cdef object _typed_constant_from_arrow_value(object value_type, object value, Py_ssize_t length, bint is_null):
    import pyarrow as pa
    from opteryx.compiled.draken.vectors.bool_vector import BoolVector
    from opteryx.compiled.draken.vectors.date32_vector import Date32Vector
    from opteryx.compiled.draken.vectors.float64_vector import Float64Vector
    from opteryx.compiled.draken.vectors.integer_vector import IntegerVector
    from opteryx.compiled.draken.vectors.string_vector import StringVector
    from opteryx.compiled.draken.vectors.time_vector import TimeVector
    from opteryx.compiled.draken.vectors.timestamp_vector import TimestampVector

    if pa.types.is_int64(value_type):
        return IntegerVector.from_constant(value, length, is_null=is_null)
    if pa.types.is_uint64(value_type):
        # Treat uint64 constant as int64 (reinterpret bits as signed)
        return IntegerVector.from_constant(value, length, is_null=is_null)
    if pa.types.is_int8(value_type) or pa.types.is_int16(value_type) or pa.types.is_int32(value_type):
        return IntegerVector.from_constant(value, length, is_null=is_null)
    if pa.types.is_float32(value_type) or pa.types.is_float64(value_type):
        return Float64Vector.from_constant(value, length, is_null=is_null)
    if pa.types.is_boolean(value_type):
        return BoolVector.from_constant(value, length, is_null=is_null)
    if (
        pa.types.is_string(value_type)
        or pa.types.is_binary(value_type)
        or pa.types.is_large_string(value_type)
        or pa.types.is_large_binary(value_type)
    ):
        return StringVector.from_constant(value, length, is_null=is_null)
    if pa.types.is_date32(value_type):
        if is_null:
            return Date32Vector.from_constant(None, length, is_null=True)
        return Date32Vector.from_constant(
            pa.array([value], type=value_type).cast(pa.int32())[0].as_py(),
            length,
        )
    if pa.types.is_timestamp(value_type):
        if is_null:
            return TimestampVector.from_constant(None, length, is_null=True, timestamp_unit=value_type.unit)
        return TimestampVector.from_constant(
            pa.array([value], type=value_type).cast(pa.int64())[0].as_py(),
            length,
            timestamp_unit=value_type.unit,
        )
    if pa.types.is_time32(value_type):
        if is_null:
            return TimeVector.from_constant(None, length, is_null=True, is_time64=False)
        return TimeVector.from_constant(
            pa.array([value], type=value_type).cast(pa.int32())[0].as_py(),
            length,
            is_time64=False,
        )
    if pa.types.is_time64(value_type):
        if is_null:
            return TimeVector.from_constant(None, length, is_null=True, is_time64=True)
        return TimeVector.from_constant(
            pa.array([value], type=value_type).cast(pa.int64())[0].as_py(),
            length,
            is_time64=True,
        )
    return None


cdef object _maybe_constant_from_dictionary_array(object array):
    cdef Py_ssize_t length = len(array)
    cdef Py_ssize_t dict_size = len(array.dictionary)
    cdef object value_type = array.type.value_type

    if length == 0:
        return None
    if array.null_count == length:
        return _typed_constant_from_arrow_value(value_type, None, length, True)
    if array.null_count != 0:
        return None
    if dict_size != 1:
        return None
    return _typed_constant_from_arrow_value(value_type, array.dictionary[0].as_py(), length, False)


cdef object _maybe_constant_from_run_end_array(object array):
    cdef Py_ssize_t length = len(array)
    cdef object value_type = array.type.value_type

    if length == 0:
        return None
    if len(array.values) != 1:
        return None
    if len(array.run_ends) != 1:
        return None
    if array.run_ends[0].as_py() != length:
        return None
    if not array.values[0].is_valid:
        return _typed_constant_from_arrow_value(value_type, None, length, True)
    return _typed_constant_from_arrow_value(value_type, array.values[0].as_py(), length, False)

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


cpdef object vector_from_arrow(object array):
    import pyarrow as pa
    import pyarrow.compute as pc
    cdef object lengths
    cdef object raw_lengths
    cdef object length_value
    cdef object fixed_array
    cdef object const_vec = None
    cdef bint uniform_lengths
    cdef object dimension = None

    # Handle chunked arrays: single chunk is OK, multiple chunks not supported
    if hasattr(array, "num_chunks"):
        num_chunks = array.num_chunks
        if num_chunks > 1:
            raise ValueError(
                f"vector_from_arrow received ChunkedArray with {num_chunks} chunks. "
                f"Use Morsel.iter_from_arrow() to process tables with chunked columns, "
                f"or call table.combine_chunks() before conversion."
            )
        elif num_chunks == 1:
            # Single chunk - extract it
            array = array.chunk(0)
        # num_chunks == 0: empty array, proceed with it as-is

    pa_type = array.type
    if pa.types.is_dictionary(pa_type):
        const_vec = _maybe_constant_from_dictionary_array(array)
        if const_vec is not None:
            return const_vec
        return vector_from_arrow(array.dictionary_decode())
    if pa.types.is_run_end_encoded(pa_type):
        const_vec = _maybe_constant_from_run_end_array(array)
        if const_vec is not None:
            return const_vec
        return vector_from_arrow(pa.array(array.to_pylist(), type=pa_type.value_type))
    if pa_type == pa.null():
        return bool_from_arrow(pa.nulls(len(array), type=pa.bool_()))
    if pa_type.equals(pa.int64()):
        return int64_from_arrow(array)
    if pa_type.equals(pa.uint64()):
        # Treat uint64 as int64: reinterpret the 64-bit values as signed.
        # Values > 2^63-1 become negative when cast to int64, but this works
        # correctly for hashing and aggregation (including COUNT DISTINCT).
        return int64_from_arrow(array.cast(pa.int64()))
    if (
        pa_type.equals(pa.int8())
        or pa_type.equals(pa.int16())
        or pa_type.equals(pa.int32())
        or pa_type.equals(pa.uint8())
        or pa_type.equals(pa.uint16())
        or pa_type.equals(pa.uint32())
    ):
        return integer_from_arrow(array)
    if pa.types.is_interval(pa_type):
        return interval_from_arrow_interval(array)
    if pa.types.is_fixed_size_binary(pa_type) and pa_type.byte_width == 16:
        return interval_from_arrow_binary(array)
    if (
        pa_type.equals(pa.string())
        or pa_type.equals(pa.binary())
        or pa.types.is_large_string(pa_type)
        or pa.types.is_large_binary(pa_type)
    ):
        if pa.types.is_large_string(pa_type):
            array = array.cast(pa.string())
        elif pa.types.is_large_binary(pa_type):
            array = array.cast(pa.binary())
        return string_from_arrow(array)
    if pa_type.equals(pa.float64()):
        return float64_from_arrow(array)
    if pa_type.equals(pa.float32()) or pa_type.equals(pa.float16()):
        return float64_from_arrow(array.cast(pa.float64()))
    if pa_type.equals(pa.bool_()):
        return bool_from_arrow(array)
    if pa.types.is_date32(pa_type):
        return date32_from_arrow(array)
    if pa.types.is_date64(pa_type):
        # date64 stores milliseconds from epoch; cast to timestamp(ms) then to timestamp(us)
        return timestamp_from_arrow(array.cast(pa.timestamp("ms")).cast(pa.timestamp("us")))
    if pa.types.is_timestamp(pa_type):
        return timestamp_from_arrow(array)
    if pa.types.is_time32(pa_type) or pa.types.is_time64(pa_type):
        return time_from_arrow(array)
    if pa.types.is_fixed_size_list(pa_type):
        if pa.types.is_integer(pa_type.value_type) or pa.types.is_floating(pa_type.value_type):
            return vector_from_arrow_vector(array)
        return array_from_arrow(array)
    if pa.types.is_list(pa_type) or pa.types.is_large_list(pa_type):
        if pa.types.is_integer(pa_type.value_type) or pa.types.is_floating(pa_type.value_type):
            raw_lengths = pc.list_value_length(array).to_pylist()
            uniform_lengths = True
            for length_value in raw_lengths:
                if length_value is None:
                    continue
                if dimension is None:
                    dimension = length_value
                    continue
                if length_value != dimension:
                    uniform_lengths = False
                    break
            if uniform_lengths and dimension is not None and dimension > 0:
                try:
                    fixed_array = array.cast(pa.list_(pa_type.value_type, dimension))
                    return vector_from_arrow_vector(fixed_array)
                except Exception:
                    pass
        return array_from_arrow(array)
    if isinstance(pa_type, pa.StructType):
        return string_from_arrow_struct(array)
    if pa.types.is_decimal(pa_type):
        return decimal_from_arrow(array)

    raise NotImplementedError(
        f"vector_from_arrow: no native Draken handler for Arrow type {pa_type!r}. "
        "Add an explicit handler or cast to a supported type before calling vector_from_arrow."
    )


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
    elif pa.types.is_string(dtype) or pa.types.is_large_string(dtype):
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
