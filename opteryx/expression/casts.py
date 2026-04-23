# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""Cast operation kernels.

This module contains the core implementations for type casting operations (CAST, TRY_CAST).
These kernels are the source of truth for cast behavior and are used by both the legacy
function system and the new cast evaluation path.

Architectural note (Phase 5.3.2):
- Expression layer functions are Draken-native: they receive Draken vectors or Python scalars
- PyArrow/NumPy in this module are used ONLY for interop and type construction
- If expression functions receive PyArrow/NumPy arrays, they raise AttributeError (fail-fast)
- Conversion from reader data (PyArrow) → Draken vectors happens at interop boundaries
"""

import datetime
import math

from opteryx.types import OrsoTypes
from opteryx.types._datetime_conversion import timestamp_to_int64_us
from opteryx.utils.vector_types import VectorType, get_vector_type
from opteryx.utils.vector_types import is_draken_vector as is_draken_vector_fn


def _is_nullish(value) -> bool:
    """Check if a value is None or represents null."""
    return value is None or (isinstance(value, float) and math.isnan(value))


def _unwrap_vector_value(value):
    """Unwrap PyArrow scalar wrappers to Python native types."""
    if hasattr(value, "as_py"):
        return value.as_py()
    return value


def parse_timestamp_value(value, unit=None):
    """Parse a timestamp value into a Python datetime object.

    For numeric values, a unit must be explicitly specified (ms, s, us).
    """
    if _is_nullish(value):
        return None

    if isinstance(value, datetime.datetime):
        return value

    if isinstance(value, datetime.date):
        return datetime.datetime.combine(value, datetime.time()).replace(tzinfo=None)

    if isinstance(value, (int, float)):
        if unit is None:
            raise TypeError(
                "Ambiguous cast: TIMESTAMP requires a unit. "
                "Use `::TIMESTAMP[ns]`, `::TIMESTAMP[ms]`, `::TIMESTAMP[s]`, `::TIMESTAMP[us]`, or `::TIMESTAMP[d]`."
            )

        numeric = float(value)
        if unit == "ns":
            seconds = numeric / 1_000_000_000
        elif unit == "ms":
            seconds = numeric / 1_000
        elif unit == "s":
            seconds = numeric
        elif unit == "us":
            seconds = numeric / 1_000_000
        elif unit == "days":
            seconds = numeric * 86_400
        else:
            raise ValueError(f"Unsupported timestamp unit: {unit!r}. Use 'ns', 'ms', 's', 'us', or 'days'.")

        return datetime.datetime.fromtimestamp(seconds, tz=datetime.timezone.utc).replace(
            tzinfo=None
        )

    return OrsoTypes.TIMESTAMP.parse(value)


def _parse_array_value(value, element_type, safe_cast=False):
    """Parse array values with element type coercion."""
    value = _unwrap_vector_value(value)

    if _is_nullish(value):
        return None

    # Duck-typed: handle any array-like with to_pylist() (Arrow arrays, Draken vectors)
    if hasattr(value, "to_pylist") and not isinstance(value, (bytes, str)):
        value = value.to_pylist()

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

    caster = OrsoTypes[element_type.name].parse
    return [caster(item) if item is not None else None for item in value]


def cast_to_double(arr, *args):
    """Cast array to DOUBLE (floating point) type.

    Per architectural contract (Phase 5.3.2 - fail-fast):
    - Primary: Draken vectors only
    - Fallback: Python scalars/lists
    - Fail: PyArrow/NumPy arrays (architectural invariant violation)

    Returns:
        Float64Vector or Python float/list
    """
    from opteryx.compiled.draken.vectors.float64_vector import from_sequence
    from opteryx.third_party.fastfloat.fast_float import (
        parse_ascii_array_to_double,
        parse_byte_array_to_double,
    )
    from opteryx.utils.vector_types import VectorType, get_vector_type

    # Primary path: Draken vectors
    if is_draken_vector_fn(arr):
        v_type = get_vector_type(arr)
        if v_type == VectorType.FLOAT64:
            return arr
        if v_type == VectorType.INT64:
            return from_sequence([float(v) if v is not None else None for v in arr.to_pylist()])
        if v_type == VectorType.STRING:
            return parse_ascii_array_to_double(arr.to_pylist())
        # Other Draken types: fall through to fallback

    # Fallback: Python scalar or list
    if isinstance(arr, (list, tuple)):
        caster = OrsoTypes.DOUBLE.parse
        return [caster(i) if i is not None else None for i in arr]

    if isinstance(arr, (int, float)):
        return OrsoTypes.DOUBLE.parse(arr)

    raise TypeError(f"Unsupported type for cast_to_double: {type(arr).__name__}")


def cast_to_int(arr, *args):
    """Cast array to INTEGER type.

    Per architectural contract (Phase 5.3.2 - fail-fast):
    - Primary: Draken vectors only
    - Fallback: Python scalars/lists
    - Fail: PyArrow/NumPy arrays (architectural invariant violation)

    Returns:
        Int64Vector or Python int/list
    """
    from opteryx.compiled.draken.vectors.int64_vector import from_sequence
    from opteryx.compiled.vector_ops import vector_cast_ascii_to_int
    from opteryx.utils.vector_types import VectorType, get_vector_type

    # Primary path: Draken vectors
    if is_draken_vector_fn(arr):
        v_type = get_vector_type(arr)
        if v_type == VectorType.INT64:
            return arr
        if v_type == VectorType.STRING:
            return vector_cast_ascii_to_int(arr)
        if v_type == VectorType.TIMESTAMP or v_type == VectorType.DATE32:
            from opteryx.expression.evaluator.type_coercion import _coerce_date32, _coerce_timestamp

            coerce = _coerce_timestamp if v_type == VectorType.TIMESTAMP else _coerce_date32
            return from_sequence([coerce(v) if v is not None else None for v in arr.to_pylist()])
        # Other Draken types: fall through to fallback

    # Fallback: Python scalar or list
    if isinstance(arr, (list, tuple)):
        caster = OrsoTypes.INTEGER.parse
        return [caster(i) if i is not None else None for i in arr]

    if isinstance(arr, int):
        return arr

    raise TypeError(f"Unsupported type for cast_to_int: {type(arr).__name__}")


def cast_to_varchar(arr, *args):
    """Cast array to VARCHAR type."""
    from opteryx.compiled.draken.vectors.string_vector import StringVector
    from opteryx.utils.vector_types import VectorType, get_vector_type

    if is_draken_vector_fn(arr):
        v_type = get_vector_type(arr)
        if v_type == VectorType.STRING:
            return arr
        # Other types: convert via to_pylist() and reconstruct
        return StringVector.from_list([str(v) if v is not None else None for v in arr.to_pylist()])

    if isinstance(arr, (list, tuple)):
        return StringVector.from_list([str(v) if v is not None else None for v in arr])

    if isinstance(arr, str):
        return arr

    raise TypeError(f"Unsupported type for cast_to_varchar: {type(arr).__name__}")


def cast_to_boolean(arr, *args):
    """Cast array to BOOLEAN type."""
    from opteryx.compiled.draken.vectors.bool_vector import BoolVector
    from opteryx.utils.vector_types import VectorType, get_vector_type

    if is_draken_vector_fn(arr):
        v_type = get_vector_type(arr)
        if v_type == VectorType.BOOL:
            return arr
        # Other types: convert via to_pylist()
        return BoolVector.from_list([bool(v) if v is not None else None for v in arr.to_pylist()])

    if isinstance(arr, (list, tuple)):
        return BoolVector.from_list([bool(v) if v is not None else None for v in arr])

    if isinstance(arr, bool):
        return arr

    raise TypeError(f"Unsupported type for cast_to_boolean: {type(arr).__name__}")


def cast_to_date(arr, *args):
    """Cast array to DATE type."""
    from opteryx.utils.vector_types import VectorType, get_vector_type

    if is_draken_vector_fn(arr):
        v_type = get_vector_type(arr)
        if v_type == VectorType.DATE32:
            return arr
        # Convert to date list and reconstruct
        caster = OrsoTypes.DATE.parse
        return [caster(v) if v is not None else None for v in arr.to_pylist()]

    if isinstance(arr, (list, tuple)):
        caster = OrsoTypes.DATE.parse
        return [caster(v) if v is not None else None for v in arr]

    if isinstance(arr, datetime.date):
        return arr

    raise TypeError(f"Unsupported type for cast_to_date: {type(arr).__name__}")


def safe(func, value, **kwargs):
    """Safely call a function with kwargs, returning None on exception."""
    try:
        return func(value, **kwargs)
    except Exception:
        return None


def cast(arr: any, _type: str, args: tuple = (), unit: str = None) -> any:
    """
    Create a casting function for the given type.

    This is a factory function that returns a callable that can be used to cast values.
    For TIMESTAMP casts from integers, unit is required: 'ms', 's', or 'us'.
    """

    def _inner(arr):
        from opteryx.compiled.draken.interop.vector_sequence import vector_from_sequence

        kwargs = {}

        def _cast_value(i):
            return caster(i, **kwargs)

        # VARBINARY is not a canonical OrsoType — map to BLOB
        _resolved_type = "VARBINARY" if _type == "VARBINARY" else _type
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
            # Vectorized path for temporal vector types
            if is_draken_vector_fn(arr) and unit is not None:
                v_type = get_vector_type(arr)
                if v_type in (VectorType.INT64, VectorType.DICTIONARY_ENCODED):
                    from opteryx.compiled.vector_ops import vector_cast_int64_to_timestamp
                    return vector_cast_int64_to_timestamp(arr, unit=unit)
                elif v_type == VectorType.TIMESTAMP:
                    # Timestamp-to-timestamp conversion: just return as-is if compatible unit
                    return arr
                elif v_type == VectorType.DATE32:
                    # Date32-to-timestamp conversion: multiply days by microseconds per day
                    from opteryx.compiled.vector_ops import vector_date32_to_timestamp
                    return vector_date32_to_timestamp(arr)

            # Fallback: parse path (for non-numeric or non-Draken inputs)
            result = [parse_timestamp_value(i, unit=unit) for i in arr]
            # Convert datetime objects to int64 microseconds, then create TimestampVector
            int64_values = [timestamp_to_int64_us(dt) if dt is not None else None for dt in result]
            int_vec = vector_from_sequence(int64_values, dtype=OrsoTypes.INTEGER)
            from opteryx.compiled.draken.vectors.timestamp_vector import (
                from_int64_vector as _from_int64,
            )

            return _from_int64(int_vec, timestamp_unit="us")
        if _type == "DATE":
            # Vectorized path for temporal vector types
            if is_draken_vector_fn(arr):
                v_type = get_vector_type(arr)
                if v_type == VectorType.DATE32:
                    # Date32-to-date conversion: just return as-is
                    return arr
                elif v_type == VectorType.TIMESTAMP:
                    # Timestamp-to-date conversion: divide by microseconds per day
                    from opteryx.compiled.vector_ops import vector_timestamp_to_date32
                    return vector_timestamp_to_date32(arr)
        if _type == "ARRAY":
            result = [_parse_array_value(i, args[0], safe_cast=False) for i in arr]
            return vector_from_sequence(result, dtype=OrsoTypes.ARRAY)
        if _type == "VECTOR":
            result = [caster(_unwrap_vector_value(i), **kwargs) for i in arr]
            return vector_from_sequence(result, dtype=OrsoTypes.VECTOR)
        result = [_cast_value(i) for i in arr]
        resolved = "BLOB" if _type == "VARBINARY" else _type
        return vector_from_sequence(result, dtype=OrsoTypes[resolved])

    return _inner
