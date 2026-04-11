"""Scalar-to-Draken-Vector conversion utilities (Step 2 of NumPy-Arrow eradication).

This module provides the canonical conversion path from scalar values to Draken vectors,
replacing intermediate numpy/pyarrow conversions during NumPy-Arrow eradication.

Design philosophy:
- Main API: scalar_to_draken_vector(scalar, dtype, length)
- Normalizes numpy/pyarrow scalars to native Python types
- Delegates to Draken's optimized vector_from_sequence (or PyArrow for temporal types)
- Clear error handling: fail fast on invalid conversions
- No external dependencies in public API (orso/numpy/pyarrow used only internally)

Architecture:
1. Normalize scalar (extract from numpy/pyarrow if needed)
2. Infer or validate target dtype
3. Wrap scalar in single-element list (or repeat for length > 1)
4. Route through appropriate conversion path:
   - Simple types (int, bool, string): vector_from_sequence() → Draken vector
   - Temporal types: PyArrow array → vector_from_arrow() → Draken vector (preserves encoding)
   - Complex types: PyArrow conversion as needed
5. Return optimized Draken vector (Int64Vector, StringVector, Date32Vector, etc.)
"""

import datetime
import decimal
import json
from typing import Any, Optional

from opteryx.compiled.draken.interop.arrow import vector_from_arrow, vector_from_sequence
from opteryx.types._orso_types import OrsoTypes
from opteryx.types._scalar_types import (
    ScalarType,
    classify_scalar,
    extract_python_scalar,
    is_null_scalar,
)

__all__ = ["scalar_to_draken_vector"]


def scalar_to_draken_vector(
    scalar: Any,
    dtype: Optional[OrsoTypes] = None,
    length: int = 1,
) -> Any:
    """
    Convert a scalar value to a Draken vector of specified type and length.

    Canonical conversion path replacing numpy/pyarrow intermediate conversions.
    Creates a vector with `length` copies of the scalar value, delegating to
    Draken's optimized vectorization for efficient execution.

    Replaces patterns like:
        # Old (with numpy/pyarrow intermediate)
        import pyarrow as pa
        vec = vector_from_arrow(pa.array([scalar] * length, type=pa.int64()))

        # New (direct)
        vec = scalar_to_draken_vector(scalar, OrsoTypes.INTEGER, length=length)

    Behavior:
    - Native Python scalars (preferred): bool, int, float, str, bytes, datetime, Decimal, None
    - numpy/pyarrow scalars (transition): extracted to native types automatically
    - Type coercion: respects target dtype, validates compatibility
    - Null handling: None → null vector of target type (if dtype specified)
    - Error handling: fail fast on invalid conversions (no silent fallbacks)

    Args:
        scalar: The scalar value to convert. Can be:
            - Native Python scalar: int, float, str, bool, bytes, datetime, Decimal, None
            - numpy/pyarrow scalar (auto-extracted to native type)
        dtype: Target OrsoType. If None, inferred from scalar's Python type.
            Must be an OrsoTypes enum value.
        length: Number of rows in output vector (default 1). The scalar is
            repeated `length` times. Draken optimizes this to a constant vector.

    Returns:
        Draken vector (specific subclass depends on dtype):
        - OrsoTypes.BOOLEAN → BoolVector
        - OrsoTypes.INTEGER → Int64Vector
        - OrsoTypes.DOUBLE → Float64Vector
        - OrsoTypes.VARCHAR → StringVector
        - OrsoTypes.BLOB → BinaryVector
        - OrsoTypes.DATE → Date32Vector
        - OrsoTypes.TIMESTAMP → TimestampVector
        - OrsoTypes.INTERVAL → IntervalVector
        - OrsoTypes.DECIMAL → DecimalVector
        - Others → appropriate Draken vector or ArrowVector wrapper

    Raises:
        TypeError: If scalar cannot be converted to target dtype.
        ValueError: If dtype is not an OrsoTypes enum value, or length < 1.
        RuntimeError: If Draken's vector_from_sequence() or vector_from_arrow() fails.

    Examples:
        >>> from opteryx.types import OrsoTypes
        >>> # Single scalar → length-1 vector
        >>> vec = scalar_to_draken_vector(42, OrsoTypes.INTEGER)
        >>> vec.to_arrow().to_pylist()
        [42]

        >>> # Constant vector: scalar repeated 10 times
        >>> vec = scalar_to_draken_vector("hello", OrsoTypes.VARCHAR, length=10)
        >>> len(vec.to_arrow())
        10

        >>> # Null vector of target type
        >>> vec = scalar_to_draken_vector(None, OrsoTypes.INTEGER, length=5)
        >>> vec.to_arrow().to_pylist()
        [None, None, None, None, None]

        >>> # Type inference: infer from scalar value
        >>> vec = scalar_to_draken_vector(3.14)
        >>> result = vec.to_arrow().to_pylist()
        >>> isinstance(result[0], float)
        True
    """
    # Validate length parameter
    if not isinstance(length, int) or length < 1:
        raise ValueError(f"length must be integer ≥ 1, got {length!r}")

    # Normalize numpy/pyarrow scalars to native Python types
    native_scalar = extract_python_scalar(scalar)

    # Infer dtype from scalar if not provided
    if dtype is None:
        dtype = _infer_orso_type(native_scalar)
        if dtype is None:
            raise TypeError(f"Cannot infer OrsoType for scalar: {native_scalar!r}")
    elif not isinstance(dtype, OrsoTypes):
        raise ValueError(f"dtype must be OrsoTypes, got {type(dtype).__name__}")

    # Validate scalar is compatible with target type (fail fast)
    if not is_null_scalar(native_scalar):
        _validate_scalar_for_type(native_scalar, dtype)

    # Normalize scalar to appropriate Python type for the target dtype
    normalized = _normalize_for_type(native_scalar, dtype)

    # Create sequence: repeat scalar `length` times
    data = [normalized] * length

    try:
        # Route to appropriate conversion path based on dtype
        # Temporal and complex types require Arrow for proper encoding
        if dtype in (OrsoTypes.DATE, OrsoTypes.TIME, OrsoTypes.TIMESTAMP, OrsoTypes.INTERVAL):
            arrow_array = _create_temporal_arrow_array(data, dtype)
            return vector_from_arrow(arrow_array)
        elif dtype == OrsoTypes.STRUCT:
            arrow_array = _create_struct_arrow_array(data)
            return vector_from_arrow(arrow_array)
        elif dtype == OrsoTypes.DECIMAL:
            arrow_array = _create_decimal_arrow_array(data, normalized)
            return vector_from_arrow(arrow_array)
        else:
            # Simple types: use Draken's optimized vector_from_sequence
            # It handles memoryview fast paths, constant detection, and Arrow fallback
            vec = vector_from_sequence(data, dtype=dtype)
            return vec
    except Exception as exc:
        raise RuntimeError(f"Draken vector conversion failed for {dtype.value}: {exc}") from exc


def _create_temporal_arrow_array(data: list, dtype: OrsoTypes) -> Any:
    """Create a PyArrow array for temporal types with correct encoding.

    Temporal types require explicit Arrow encoding to preserve precision
    and avoid corruption during serialization.
    """
    import pyarrow as pa

    if dtype == OrsoTypes.DATE:
        # Use pa.date32() for date values
        return pa.array(data, type=pa.date32())
    elif dtype == OrsoTypes.TIME:
        # Use pa.time64('ns') for time values (nanosecond precision)
        return pa.array(data, type=pa.time64("ns"))
    elif dtype == OrsoTypes.TIMESTAMP:
        # Use pa.timestamp('ns') for timestamp values (nanosecond precision)
        return pa.array(data, type=pa.timestamp("ns"))
    elif dtype == OrsoTypes.INTERVAL:
        # Use pa.duration('ns') for interval/timedelta values
        return pa.array(data, type=pa.duration("ns"))
    else:
        raise ValueError(f"Unsupported temporal type: {dtype}")


def _create_struct_arrow_array(data: list) -> Any:
    """Create a PyArrow array for struct types.

    Structs are represented as dicts in Python but need explicit struct
    schema in Arrow for proper encoding.
    """
    import pyarrow as pa

    # For now, infer schema from the first non-null dict
    first_dict = None
    for item in data:
        if item is not None and isinstance(item, dict):
            first_dict = item
            break

    if first_dict is None:
        # All nulls, create a generic struct array
        return pa.array(data, type=pa.struct([]))

    # Infer field types from first dict
    fields = []
    for key, value in first_dict.items():
        # Infer PyArrow type from value
        field_type = pa.infer(value) if value is not None else pa.null()
        fields.append(pa.field(key, field_type))

    struct_type = pa.struct(fields)
    return pa.array(data, type=struct_type)


def _create_decimal_arrow_array(data: list, normalized: Any) -> Any:
    """Create a PyArrow array for decimal types.

    Decimals need explicit precision and scale specification.
    Uses standard 38,18 precision if not otherwise specified.
    """
    import pyarrow as pa

    # Default precision and scale for DECIMAL type
    # Most systems use 38 digits total, 18 after decimal point
    precision = 38
    scale = 18

    # Try to infer better precision/scale from the data
    for item in data:
        if item is not None and isinstance(item, decimal.Decimal):
            # Get sign, digits, exponent
            sign, digits, exponent = item.as_tuple()
            # exponent is negative for fractional part
            num_digits = len(digits)
            frac_digits = -exponent if exponent < 0 else 0
            int_digits = num_digits - frac_digits

            # Update precision/scale to accommodate this value
            precision = max(precision, num_digits)
            scale = max(scale, frac_digits)

    decimal_type = pa.decimal128(precision, scale)
    return pa.array(data, type=decimal_type)


def _infer_orso_type(scalar: Any) -> Optional[OrsoTypes]:
    """
    Infer the best-fit OrsoType for a scalar value.

    Precedence: ScalarType classification → OrsoType mapping.

    Returns:
        OrsoTypes enum value, or None if type cannot be inferred.
    """
    if scalar is None:
        return OrsoTypes.NULL

    scalar_type = classify_scalar(scalar)
    if scalar_type is None:
        return None

    # Map ScalarType enum to OrsoType
    type_mapping = {
        ScalarType.NONE: OrsoTypes.NULL,
        ScalarType.BOOLEAN: OrsoTypes.BOOLEAN,
        ScalarType.INT8: OrsoTypes.INTEGER,
        ScalarType.INT16: OrsoTypes.INTEGER,
        ScalarType.INT32: OrsoTypes.INTEGER,
        ScalarType.INT64: OrsoTypes.INTEGER,
        ScalarType.UINT8: OrsoTypes.INTEGER,
        ScalarType.UINT16: OrsoTypes.INTEGER,
        ScalarType.UINT32: OrsoTypes.INTEGER,
        ScalarType.UINT64: OrsoTypes.INTEGER,
        ScalarType.FLOAT32: OrsoTypes.DOUBLE,
        ScalarType.FLOAT64: OrsoTypes.DOUBLE,
        ScalarType.DECIMAL: OrsoTypes.DECIMAL,
        ScalarType.STRING: OrsoTypes.VARCHAR,
        ScalarType.BYTES: OrsoTypes.BLOB,
        ScalarType.DATE: OrsoTypes.DATE,
        ScalarType.TIME: OrsoTypes.TIME,
        ScalarType.DATETIME: OrsoTypes.TIMESTAMP,
        ScalarType.DATETIME64: OrsoTypes.TIMESTAMP,
        ScalarType.TIMEDELTA: OrsoTypes.INTERVAL,
        ScalarType.TIMEDELTA64: OrsoTypes.INTERVAL,
        ScalarType.GENERIC_OBJECT: OrsoTypes.VARCHAR,
    }

    return type_mapping.get(scalar_type, OrsoTypes.VARCHAR)


def _validate_scalar_for_type(scalar: Any, dtype: OrsoTypes) -> None:
    """
    Validate that a scalar can be converted to the target type.

    Performs type checks to fail fast on incompatible conversions.
    None/null values are always valid.

    Args:
        scalar: The scalar value to validate (must not be None)
        dtype: Target OrsoType

    Raises:
        TypeError: If scalar cannot be converted to dtype
    """
    if scalar is None:
        return

    # Type-specific validation rules (fail fast on incompatibilities)
    if dtype == OrsoTypes.BOOLEAN:
        if not isinstance(scalar, (bool, int, float)):
            raise TypeError(
                f"Cannot convert {type(scalar).__name__!r} to BOOLEAN: "
                f"expected bool, int, or float, got {scalar!r}"
            )

    elif dtype == OrsoTypes.INTEGER:
        # Allow numeric types that can be converted to int
        # This includes int, float (truncates), Decimal, and bool
        if not isinstance(scalar, (bool, int, float, decimal.Decimal)):
            raise TypeError(
                f"Cannot convert {type(scalar).__name__!r} to INTEGER: "
                f"expected numeric type, got {scalar!r}"
            )

    elif dtype == OrsoTypes.DOUBLE:
        if not isinstance(scalar, (int, float, decimal.Decimal)):
            raise TypeError(
                f"Cannot convert {type(scalar).__name__!r} to DOUBLE: "
                f"expected int, float, or Decimal, got {scalar!r}"
            )

    elif dtype == OrsoTypes.VARCHAR:
        # VARCHAR accepts anything - it will be converted via str()
        # No validation needed
        pass

    elif dtype == OrsoTypes.BLOB:
        # Allow bytes-like types and strings (will be UTF-8 encoded)
        if not isinstance(scalar, (bytes, bytearray, memoryview, str)):
            raise TypeError(
                f"Cannot convert {type(scalar).__name__!r} to BLOB: "
                f"expected bytes or str, got {scalar!r}"
            )

    elif dtype == OrsoTypes.DATE:
        if not isinstance(scalar, (datetime.date, datetime.datetime)):
            raise TypeError(
                f"Cannot convert {type(scalar).__name__!r} to DATE: "
                f"expected datetime.date or datetime.datetime, got {scalar!r}"
            )

    elif dtype == OrsoTypes.TIME:
        if not isinstance(scalar, datetime.time):
            raise TypeError(
                f"Cannot convert {type(scalar).__name__!r} to TIME: "
                f"expected datetime.time, got {scalar!r}"
            )

    elif dtype == OrsoTypes.TIMESTAMP:
        if not isinstance(scalar, (datetime.datetime, datetime.date)):
            raise TypeError(
                f"Cannot convert {type(scalar).__name__!r} to TIMESTAMP: "
                f"expected datetime.datetime or datetime.date, got {scalar!r}"
            )

    elif dtype == OrsoTypes.INTERVAL:
        if not isinstance(scalar, datetime.timedelta):
            raise TypeError(
                f"Cannot convert {type(scalar).__name__!r} to INTERVAL: "
                f"expected datetime.timedelta, got {scalar!r}"
            )

    elif dtype == OrsoTypes.DECIMAL:
        if not isinstance(scalar, (decimal.Decimal, int, float)):
            raise TypeError(
                f"Cannot convert {type(scalar).__name__!r} to DECIMAL: "
                f"expected Decimal, int, or float, got {scalar!r}"
            )

    elif dtype == OrsoTypes.ARRAY:
        if not isinstance(scalar, (list, tuple)):
            raise TypeError(
                f"Cannot convert {type(scalar).__name__!r} to ARRAY: "
                f"expected list or tuple, got {scalar!r}"
            )

    elif dtype == OrsoTypes.STRUCT:
        # Only dict is valid for struct; list and other types are incompatible
        if not isinstance(scalar, dict):
            raise TypeError(
                f"Cannot convert {type(scalar).__name__!r} to STRUCT: expected dict, got {scalar!r}"
            )

    elif dtype == OrsoTypes.VECTOR:
        if not isinstance(scalar, (list, tuple)):
            raise TypeError(
                f"Cannot convert {type(scalar).__name__!r} to VECTOR: "
                f"expected list or tuple, got {scalar!r}"
            )

    elif dtype == OrsoTypes.JSONB:
        # JSONB accepts JSON-serializable types
        if not isinstance(scalar, (dict, list, str, int, float, bool)):
            raise TypeError(
                f"Cannot convert {type(scalar).__name__!r} to JSONB: "
                f"expected JSON-serializable type, got {scalar!r}"
            )


def _normalize_for_type(scalar: Any, dtype: OrsoTypes) -> Any:
    """
    Normalize a scalar to the appropriate Python type for the target dtype.

    This ensures that the scalar is in the "canonical" form for the target
    type before passing to Draken's vector_from_sequence() or PyArrow conversion.

    Args:
        scalar: The scalar value (may be None, or wrong type if coercible)
        dtype: Target OrsoType

    Returns:
        Scalar in canonical form for dtype (or None if scalar was None)
    """
    if scalar is None:
        return None

    # Type-specific normalization
    if dtype == OrsoTypes.BOOLEAN:
        return bool(scalar) if scalar is not None else None

    if dtype == OrsoTypes.INTEGER:
        return int(scalar) if scalar is not None else None

    if dtype == OrsoTypes.DOUBLE:
        return float(scalar) if scalar is not None else None

    if dtype == OrsoTypes.VARCHAR:
        return str(scalar) if scalar is not None else None

    if dtype == OrsoTypes.DECIMAL:
        if scalar is None:
            return None
        if isinstance(scalar, decimal.Decimal):
            return scalar
        return decimal.Decimal(str(scalar))

    # Date: extract from datetime if needed
    if dtype == OrsoTypes.DATE:
        if isinstance(scalar, datetime.datetime):
            return scalar.date()
        return scalar

    # Time: pass through
    if dtype == OrsoTypes.TIME:
        return scalar

    # Timestamp: promote date to datetime
    if dtype == OrsoTypes.TIMESTAMP:
        if isinstance(scalar, datetime.date) and not isinstance(scalar, datetime.datetime):
            return datetime.datetime.combine(scalar, datetime.time())
        return scalar

    # Interval: pass through
    if dtype == OrsoTypes.INTERVAL:
        return scalar

    # Blob: convert string to bytes if needed
    if dtype == OrsoTypes.BLOB:
        if isinstance(scalar, str):
            return scalar.encode("utf-8")
        return scalar

    # Array: convert to list if needed
    if dtype == OrsoTypes.ARRAY:
        return list(scalar) if not isinstance(scalar, list) else scalar

    # Struct: pass through (already validated as dict)
    if dtype == OrsoTypes.STRUCT:
        return scalar

    # Vector: convert to list if needed
    if dtype == OrsoTypes.VECTOR:
        return list(scalar) if not isinstance(scalar, list) else scalar

    # JSONB: serialize if needed
    if dtype == OrsoTypes.JSONB:
        if isinstance(scalar, str):
            return scalar
        return json.dumps(scalar)

    # NULL type
    if dtype == OrsoTypes.NULL:
        return None

    # Default: pass through
    return scalar
