# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False

from libc.stddef cimport size_t

from draken.core.buffers cimport DRAKEN_BOOL
from draken.core.buffers cimport DRAKEN_DATE32
from draken.core.buffers cimport DRAKEN_FLOAT64
from draken.core.buffers cimport DRAKEN_INT8
from draken.core.buffers cimport DRAKEN_INT16
from draken.core.buffers cimport DRAKEN_INT32
from draken.core.buffers cimport DRAKEN_INT64
from draken.core.buffers cimport DRAKEN_STRING
from draken.core.buffers cimport DRAKEN_TIME32
from draken.core.buffers cimport DRAKEN_TIME64
from draken.core.buffers cimport DRAKEN_TIMESTAMP64


cdef inline object _coerce_literal_bytes(object literal):
    if literal is None:
        return None
    if hasattr(literal, "as_py"):
        try:
            literal = literal.as_py()
        except Exception:
            return None
    if isinstance(literal, (bytes, bytearray, memoryview)):
        try:
            return bytes(literal)
        except Exception:
            return None
    if isinstance(literal, str):
        try:
            return literal.encode("utf8")
        except Exception:
            return None
    return None


cdef int _value_type_from_dtype(object dtype):
    cdef int dtype_int
    if dtype is None:
        return -1

    if isinstance(dtype, int):
        dtype_int = <int>dtype
        if dtype_int in (
            DRAKEN_BOOL,
            DRAKEN_INT8,
            DRAKEN_INT16,
            DRAKEN_INT32,
            DRAKEN_INT64,
            DRAKEN_FLOAT64,
            DRAKEN_STRING,
            DRAKEN_DATE32,
            DRAKEN_TIME32,
            DRAKEN_TIME64,
            DRAKEN_TIMESTAMP64,
        ):
            return dtype_int
        return -1

    try:
        import pyarrow as pa

        if pa.types.is_boolean(dtype):
            return DRAKEN_BOOL
        if pa.types.is_int8(dtype):
            return DRAKEN_INT8
        if pa.types.is_int16(dtype):
            return DRAKEN_INT16
        if pa.types.is_int32(dtype):
            return DRAKEN_INT32
        if pa.types.is_int64(dtype) or pa.types.is_uint64(dtype):
            return DRAKEN_INT64
        if pa.types.is_integer(dtype):
            return DRAKEN_INT32
        if pa.types.is_floating(dtype):
            return DRAKEN_FLOAT64
        if pa.types.is_string(dtype) or pa.types.is_large_string(dtype) or pa.types.is_binary(dtype):
            return DRAKEN_STRING
        if pa.types.is_date32(dtype):
            return DRAKEN_DATE32
        if pa.types.is_time32(dtype):
            return DRAKEN_TIME32
        if pa.types.is_time64(dtype):
            return DRAKEN_TIME64
        if pa.types.is_timestamp(dtype):
            return DRAKEN_TIMESTAMP64
    except Exception:
        return -1

    return -1


cdef object _typed_constant_from_scalar(object value, size_t length, object dtype=None):
    cdef bint is_null = value is None
    cdef object scalar = value
    cdef object dtype_name_obj = ""
    cdef str dtype_name
    cdef object pa
    cdef object cast_type
    cdef object timestamp_type
    cdef object time_type
    cdef bint is_time64 = False

    if hasattr(scalar, "as_py"):
        try:
            scalar = scalar.as_py()
        except Exception:
            return None

    if isinstance(dtype, str):
        dtype_name = dtype.lower()
    elif dtype is not None:
        dtype_name_obj = getattr(dtype, "name", None)
        if dtype_name_obj is None:
            dtype_name = str(dtype).lower()
        else:
            dtype_name = str(dtype_name_obj).lower()
    else:
        dtype_name = ""

    if dtype in (DRAKEN_BOOL,) or dtype_name in ("bool", "boolean"):
        from draken.vectors.bool_vector import BoolVector

        return BoolVector.from_constant(False if is_null else scalar, length, is_null=is_null)

    if dtype in (DRAKEN_INT8, DRAKEN_INT16, DRAKEN_INT32) or dtype_name in (
        "int8",
        "int16",
        "int32",
        "uint8",
        "uint16",
        "uint32",
    ):
        from draken.vectors.integer_vector import IntegerVector

        return IntegerVector.from_constant(0 if is_null else scalar, length, is_null=is_null)

    if dtype in (DRAKEN_INT64,) or dtype_name in ("int64", "uint64"):
        from draken.vectors.int64_vector import Int64Vector

        return Int64Vector.from_constant(0 if is_null else scalar, length, is_null=is_null)

    if dtype in (DRAKEN_FLOAT64,) or dtype_name in ("float", "float32", "float64", "double"):
        from draken.vectors.float64_vector import Float64Vector

        return Float64Vector.from_constant(0.0 if is_null else scalar, length, is_null=is_null)

    if dtype in (DRAKEN_STRING,) or dtype_name in (
        "string",
        "large_string",
        "binary",
        "large_binary",
        "varchar",
        "blob",
    ):
        from draken.vectors.string_vector import StringVector

        return StringVector.from_constant(b"" if is_null else scalar, length, is_null=is_null)

    try:
        import pyarrow as pa

        if dtype is not None and not isinstance(dtype, str):
            if pa.types.is_boolean(dtype):
                from draken.vectors.bool_vector import BoolVector

                return BoolVector.from_constant(False if is_null else scalar, length, is_null=is_null)
            if pa.types.is_integer(dtype):
                if pa.types.is_int64(dtype) or pa.types.is_uint64(dtype):
                    from draken.vectors.int64_vector import Int64Vector

                    return Int64Vector.from_constant(0 if is_null else scalar, length, is_null=is_null)
                from draken.vectors.integer_vector import IntegerVector

                return IntegerVector.from_constant(0 if is_null else scalar, length, is_null=is_null)
            if pa.types.is_floating(dtype):
                from draken.vectors.float64_vector import Float64Vector

                return Float64Vector.from_constant(0.0 if is_null else scalar, length, is_null=is_null)
            if (
                pa.types.is_string(dtype)
                or pa.types.is_large_string(dtype)
                or pa.types.is_binary(dtype)
                or pa.types.is_large_binary(dtype)
            ):
                from draken.vectors.string_vector import StringVector

                return StringVector.from_constant(b"" if is_null else scalar, length, is_null=is_null)
            if pa.types.is_date32(dtype):
                from draken.vectors.date32_vector import Date32Vector

                if not is_null:
                    scalar = pa.array([scalar], type=pa.date32()).cast(pa.int32())[0].as_py()
                return Date32Vector.from_constant(0 if is_null else scalar, length, is_null=is_null)
            if pa.types.is_timestamp(dtype):
                from draken.vectors.timestamp_vector import TimestampVector

                timestamp_type = dtype
                if not is_null:
                    scalar = pa.array([scalar], type=timestamp_type).cast(pa.int64())[0].as_py()
                return TimestampVector.from_constant(
                    0 if is_null else scalar,
                    length,
                    is_null=is_null,
                    timestamp_unit=timestamp_type.unit,
                )
            if pa.types.is_time32(dtype) or pa.types.is_time64(dtype):
                from draken.vectors.time_vector import TimeVector

                time_type = dtype
                is_time64 = pa.types.is_time64(time_type)
                if not is_null:
                    cast_type = pa.int64() if is_time64 else pa.int32()
                    scalar = pa.array([scalar], type=time_type).cast(cast_type)[0].as_py()
                return TimeVector.from_constant(
                    0 if is_null else scalar,
                    length,
                    is_null=is_null,
                    is_time64=is_time64,
                )
    except Exception:
        pass

    if dtype is None:
        if isinstance(scalar, bool):
            from draken.vectors.bool_vector import BoolVector

            return BoolVector.from_constant(False if is_null else scalar, length, is_null=is_null)
        if isinstance(scalar, int):
            from draken.vectors.int64_vector import Int64Vector

            return Int64Vector.from_constant(0 if is_null else scalar, length, is_null=is_null)
        if isinstance(scalar, float):
            from draken.vectors.float64_vector import Float64Vector

            return Float64Vector.from_constant(0.0 if is_null else scalar, length, is_null=is_null)
        if isinstance(scalar, (bytes, str)):
            from draken.vectors.string_vector import StringVector

            return StringVector.from_constant(b"" if is_null else scalar, length, is_null=is_null)

    return None


cdef object _try_build_constant_from_sequence(object seq, object dtype):
    cdef Py_ssize_t n
    cdef Py_ssize_t i
    cdef object value
    cdef object base_value = None
    cdef bint has_value = False
    cdef int value_type

    if seq is None:
        return None
    n = len(seq)
    if n == 0:
        return None

    for i in range(n):
        value = seq[i]
        if hasattr(value, "as_py"):
            try:
                value = value.as_py()
            except Exception:
                return None
        if value is None:
            continue
        if not has_value:
            base_value = value
            has_value = True
        elif value != base_value:
            return None

    if not has_value:
        return from_scalar(None, <size_t>n, dtype=dtype)

    value_type = _value_type_from_dtype(dtype)
    if value_type < 0 and isinstance(base_value, bool):
        value_type = DRAKEN_BOOL
    elif value_type < 0 and isinstance(base_value, int):
        value_type = DRAKEN_INT64
    elif value_type < 0 and isinstance(base_value, float):
        value_type = DRAKEN_FLOAT64
    elif value_type < 0 and isinstance(base_value, (bytes, str)):
        value_type = DRAKEN_STRING

    if value_type < 0:
        return None

    for i in range(n):
        value = seq[i]
        if value is None:
            return None

    return from_scalar(base_value, <size_t>n, dtype=dtype)


cdef object from_sequence(object data, object dtype=None):
    cdef object seq

    if isinstance(data, (bytes, bytearray, memoryview, str, dict)):
        return None

    if isinstance(data, (list, tuple)):
        return _try_build_constant_from_sequence(data, dtype)

    if hasattr(data, "__iter__") and hasattr(data, "__len__"):
        try:
            seq = list(data)
        except Exception:
            return None
        return _try_build_constant_from_sequence(seq, dtype)

    return None


cpdef object from_scalar(object value, size_t length, object dtype=None):
    return _typed_constant_from_scalar(value, length, dtype)
