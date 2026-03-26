"""Cast operation kernels.

This module contains the core implementations for type casting operations (CAST, TRY_CAST).
These kernels are the source of truth for cast behavior and are used by both the legacy
function system and the new cast evaluation path.
"""

import datetime
import inspect

import numpy
import pyarrow
from orso.types import OrsoTypes


def safe(func, value, **kwargs):
    """Safely cast a value, returning None on error (for TRY_CAST/SAFE_CAST)."""
    try:
        return func(value, **kwargs)
    except (ValueError, TypeError, ArithmeticError, OverflowError):
        return None


def _unwrap_vector_value(value):
    if hasattr(value, "as_py"):
        value = value.as_py()
    if isinstance(value, numpy.ndarray):
        value = value.tolist()
    return value


def _normalize_scalar(value):
    if hasattr(value, "as_py"):
        value = value.as_py()
    if isinstance(value, numpy.ndarray):
        value = value.item() if value.ndim == 0 else value.tolist()
    if isinstance(value, numpy.generic):
        value = value.item()
    return value


def _is_nullish(value):
    if value is None:
        return True
    return isinstance(value, (float, numpy.floating)) and numpy.isnan(value)


def parse_timestamp_value(value):
    """
    Cast a single scalar-like value to a Python datetime.

    This extends the Orso TIMESTAMP parser with support for:
    - raw epoch integers/floats in s/ms/us/ns
    - numpy datetime64
    - Python date values
    - UTF-8 bytes
    """
    value = _normalize_scalar(value)

    if value is None:
        return None

    if isinstance(value, datetime.datetime):
        return value

    if isinstance(value, datetime.date):
        return datetime.datetime(value.year, value.month, value.day)

    if isinstance(value, numpy.datetime64):
        micros = int(value.astype("datetime64[us]").astype(numpy.int64))
        return datetime.datetime.fromtimestamp(
            micros / 1_000_000, tz=datetime.timezone.utc
        ).replace(tzinfo=None)

    if isinstance(value, (bytes, bytearray, memoryview)):
        value = bytes(value).decode("utf-8")

    if isinstance(value, str):
        return OrsoTypes.TIMESTAMP.parse(value)

    if isinstance(value, (int, float, numpy.integer, numpy.floating)):
        numeric = float(value)
        magnitude = abs(numeric)

        # Small integers are interpreted as epoch seconds. Larger magnitudes
        # are inferred as ms/us/ns epoch values.
        if magnitude >= 1e18:
            seconds = numeric / 1_000_000_000
        elif magnitude >= 1e15:
            seconds = numeric / 1_000_000
        elif magnitude >= 1e12:
            seconds = numeric / 1_000
        else:
            seconds = numeric

        return datetime.datetime.fromtimestamp(seconds, tz=datetime.timezone.utc).replace(
            tzinfo=None
        )

    return OrsoTypes.TIMESTAMP.parse(value)


def _parse_array_value(value, element_type, safe_cast=False):
    value = _unwrap_vector_value(value)

    if _is_nullish(value):
        return None

    if isinstance(value, pyarrow.Array):
        value = value.to_pylist()
    if isinstance(value, numpy.ndarray):
        value = value.item() if value.ndim == 0 else value.tolist()
    if isinstance(value, numpy.generic):
        value = value.item()

    if isinstance(value, (bytes, bytearray, memoryview)):
        value = bytes(value).decode("utf-8", errors="ignore")

    if isinstance(value, str):
        stripped = value.strip()
        if stripped.startswith("[") and stripped.endswith("]"):
            if safe_cast:
                return safe(OrsoTypes.ARRAY.parse, value, element_type=element_type)
            return OrsoTypes.ARRAY.parse(value, element_type=element_type)
        value = [value]
    elif isinstance(value, (list, tuple, set, frozenset)):
        value = list(value)
    else:
        value = [value]

    parser = OrsoTypes[element_type].parse
    result = []
    for element in value:
        element = _unwrap_vector_value(element)
        if _is_nullish(element):
            continue
        if safe_cast:
            converted = safe(parser, element)
            if converted is None:
                return None
        else:
            converted = parser(element)
        result.append(converted)
    return result


def try_cast(_type):
    """Cast a column to a specified type, returning None for failed conversions.

    This is used for TRY_CAST and SAFE_CAST operations.
    """

    def _inner(arr, *args):
        args = [a[0] for a in args]
        kwargs = {}

        caster = OrsoTypes[_type].parse

        sig = inspect.signature(caster)
        params = list(sig.parameters.values())[1:]  # skip the first param (`value`)

        kwargs = {param.name: arg for param, arg in zip(params, args)}

        if _type == "TIMESTAMP":
            return [safe(parse_timestamp_value, i) for i in arr]
        if _type == "ARRAY":
            return [_parse_array_value(i, args[0], safe_cast=True) for i in arr]
        if _type == "VECTOR":
            return [safe(caster, _unwrap_vector_value(i), **kwargs) for i in arr]
        return [safe(caster, i, **kwargs) for i in arr]

    return _inner


def cast(_type):
    """Cast a column to a specified type.

    This handles standard CAST operations with type-specific logic for DECIMAL,
    VARCHAR, BLOB, and ARRAY types.
    """

    def _inner(arr, *args):
        args = [a[0] for a in args]
        kwargs = {}

        def _cast_value(value):
            value = _normalize_scalar(value)
            if _is_nullish(value):
                return None
            return caster(value, **kwargs)

        # VARBINARY is not a canonical OrsoType — map to BLOB
        _resolved_type = "BLOB" if _type == "VARBINARY" else _type
        caster = OrsoTypes[_resolved_type].parse

        if _type == "DECIMAL":
            # DECIMAL requires special handling for precision and scale
            if len(args) == 2:
                kwargs["precision"] = args[0]
                kwargs["scale"] = args[1]
            elif len(args) == 1:
                kwargs["precision"] = args[0]
                kwargs["scale"] = 0
        elif _type in ("VARCHAR", "BLOB", "VARBINARY") and len(args) == 1:
            # VARCHAR and BLOB can take a single argument for length
            kwargs["length"] = args[0]
        elif _type == "ARRAY" and len(args) == 1:
            # ARRAY can take a single argument for the element type
            kwargs["element_type"] = args[0]

        if _type == "TIMESTAMP":
            return [parse_timestamp_value(i) for i in arr]
        if _type == "ARRAY":
            return [_parse_array_value(i, args[0], safe_cast=False) for i in arr]
        if _type == "VECTOR":
            return [caster(_unwrap_vector_value(i), **kwargs) for i in arr]
        return [_cast_value(i) for i in arr]

    return _inner


def _cast_to_binary_representation(
    arr, format_double_func, vector_cast_int64_func, vector_cast_uint64_func, caster_type, *args
):
    """Internal helper for casting to binary representations (VARCHAR and BLOB).

    Both VARCHAR and BLOB store identical binary data; only the format functions differ.
    This consolidates the identical logic paths.

    Args:
        arr: Input array
        format_double_func: Function to format float64 arrays (format_double_array_ascii or format_double_array_bytes)
        vector_cast_int64_func: Function to cast int64 arrays
        vector_cast_uint64_func: Function to cast uint64 arrays
        caster_type: Type constant for OrsoTypes lookup (e.g., OrsoTypes.VARCHAR)
        *args: Optional length argument
    """
    if hasattr(arr, "to_numpy"):
        arr = arr.to_numpy(False)

    if arr.dtype == numpy.float64:
        return format_double_func(arr)

    if arr.dtype == numpy.int64:
        from opteryx.compiled.draken.interop.arrow import vector_from_arrow

        return vector_cast_int64_func(vector_from_arrow(pyarrow.array(arr))).to_arrow()

    if arr.dtype == numpy.uint64:
        from opteryx.compiled.draken.interop.arrow import vector_from_arrow

        return vector_cast_uint64_func(
            vector_from_arrow(pyarrow.array(arr.view(numpy.int64)))
        ).to_arrow()

    caster = caster_type.parse
    kwargs = {}
    if len(args) == 1:
        kwargs["length"] = int(args[0])
    return [caster(i, **kwargs) if i is not None else None for i in arr]


def cast_to_varchar(arr, *args):
    """Cast array to VARCHAR (text) type.

    Uses optimized paths for float64 and int64 arrays when possible,
    falling back to generic conversion for other types.
    """
    from opteryx.compiled.vector_ops import vector_cast_int64_to_ascii
    from opteryx.compiled.vector_ops import vector_cast_uint64_to_ascii
    from opteryx.third_party.ulfjack.ryu import format_double_array_ascii

    return _cast_to_binary_representation(
        arr,
        format_double_array_ascii,
        vector_cast_int64_to_ascii,
        vector_cast_uint64_to_ascii,
        OrsoTypes.VARCHAR,
        *args,
    )


def cast_to_blob(arr, *args):
    """Cast array to BLOB (binary) type.

    Uses optimized paths for float64 and int64 arrays when possible,
    falling back to generic conversion for other types.
    """
    from opteryx.compiled.vector_ops import vector_cast_int64_to_bytes
    from opteryx.compiled.vector_ops import vector_cast_uint64_to_bytes
    from opteryx.third_party.ulfjack.ryu import format_double_array_bytes

    return _cast_to_binary_representation(
        arr,
        format_double_array_bytes,
        vector_cast_int64_to_bytes,
        vector_cast_uint64_to_bytes,
        OrsoTypes.BLOB,
        *args,
    )


def cast_to_double(arr, *args):
    """Cast array to DOUBLE (floating point) type.

    Casts an array to double precision floating point numbers.
    Uses fast C++ path for string parsing when available,
    optimized conversion for int64 arrays, and generic fallback.
    """
    from opteryx.third_party.fastfloat.fast_float import parse_ascii_array_to_double
    from opteryx.third_party.fastfloat.fast_float import parse_byte_array_to_double

    if hasattr(arr, "to_numpy"):
        arr = arr.to_numpy(False)
    if arr.dtype == numpy.float64:
        return arr
    if arr.dtype == numpy.int64:
        return arr.astype(numpy.float64)
    if numpy.issubdtype(arr.dtype, numpy.object_):
        if isinstance(arr[0], str):
            return parse_ascii_array_to_double(arr)
        elif isinstance(arr[0], bytes):
            return parse_byte_array_to_double(arr)
    if numpy.issubdtype(arr.dtype, numpy.str_):
        return parse_ascii_array_to_double(arr.astype(object))

    caster = OrsoTypes.DOUBLE.parse
    return [caster(i) if i is not None else None for i in arr]


def cast_to_int(arr, *args):
    """Cast array to INTEGER type.

    Uses optimized C++ paths for string/byte parsing and date conversion,
    with generic fallback for other types.
    """
    from opteryx.compiled.vector_ops import vector_cast_ascii_to_int
    from opteryx.compiled.vector_ops import vector_cast_bytes_to_int

    if hasattr(arr, "to_numpy"):
        arr = arr.to_numpy(False)
    if numpy.issubdtype(arr.dtype, numpy.object_):
        if isinstance(arr[0], str):
            from opteryx.compiled.draken.interop.arrow import vector_from_arrow

            return vector_cast_ascii_to_int(
                vector_from_arrow(pyarrow.array(arr, type=pyarrow.string()))
            ).to_arrow()
        elif isinstance(arr[0], bytes):
            from opteryx.compiled.draken.interop.arrow import vector_from_arrow

            return vector_cast_bytes_to_int(
                vector_from_arrow(pyarrow.array(arr, type=pyarrow.binary()))
            ).to_arrow()
    if numpy.issubdtype(arr.dtype, numpy.str_):
        from opteryx.compiled.draken.interop.arrow import vector_from_arrow

        return vector_cast_ascii_to_int(
            vector_from_arrow(pyarrow.array(arr.astype(object), type=pyarrow.string()))
        ).to_arrow()
    if numpy.issubdtype(arr.dtype, numpy.datetime64):
        arr = arr.astype("M8[us]")  # microseconds
        return arr.astype(numpy.int64)

    caster = OrsoTypes.INTEGER.parse
    return [caster(i) if i is not None else None for i in arr]
