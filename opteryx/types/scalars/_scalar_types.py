"""
Internal scalar type system.

Replaces numpy type checking with internal type identification.
This module provides canonical scalar type classification without runtime
dependency on numpy for type checks.

Key design: Use type() + dictionary lookup for common types, module/name
inspection only for external libraries. Minimize attribute access.
"""

import datetime
import decimal
from enum import Enum
from typing import Any, Optional


class ScalarType(Enum):
    """Canonical scalar type identifiers."""

    # Numeric types
    INT8 = "int8"
    INT16 = "int16"
    INT32 = "int32"
    INT64 = "int64"
    UINT8 = "uint8"
    UINT16 = "uint16"
    UINT32 = "uint32"
    UINT64 = "uint64"
    FLOAT32 = "float32"
    FLOAT64 = "float64"

    # Boolean
    BOOLEAN = "bool"

    # String/Bytes
    STRING = "string"
    BYTES = "bytes"

    # Temporal
    DATE = "date"
    TIME = "time"
    DATETIME = "datetime"
    DATETIME64 = "datetime64"  # Legacy numpy.datetime64
    TIMEDELTA = "timedelta"
    TIMEDELTA64 = "timedelta64"  # Legacy numpy.timedelta64

    # Decimal
    DECIMAL = "decimal"

    # Special
    NONE = "none"
    GENERIC_OBJECT = "generic_object"


# Type lookup dict: maps Python type objects directly to ScalarType
# Built at module load time; no external imports needed
_TYPE_LOOKUP = {
    type(None): ScalarType.NONE,
    bool: ScalarType.BOOLEAN,
    int: ScalarType.INT64,
    float: ScalarType.FLOAT64,
    str: ScalarType.STRING,
    bytes: ScalarType.BYTES,
    bytearray: ScalarType.BYTES,
    memoryview: ScalarType.BYTES,
    datetime.date: ScalarType.DATE,
    datetime.time: ScalarType.TIME,
    datetime.datetime: ScalarType.DATETIME,
    datetime.timedelta: ScalarType.TIMEDELTA,
    decimal.Decimal: ScalarType.DECIMAL,
}

# NumPy type names to ScalarType (for duck typing via module inspection)
_NUMPY_TYPE_MAP = {
    "datetime64": ScalarType.DATETIME64,
    "timedelta64": ScalarType.TIMEDELTA64,
    "bool_": ScalarType.BOOLEAN,
    "bool": ScalarType.BOOLEAN,  # newer numpy versions use 'bool' not 'bool_'
}


def classify_scalar(value: Any) -> Optional[ScalarType]:
    """
    Classify a scalar value to its canonical type using dictionary lookup.

    Fast path: direct type() lookup for built-in Python types.
    Slow path: module/name inspection for numpy/pyarrow types (duck typing).

    Replaces patterns like:
        isinstance(value, numpy.generic)
        isinstance(value, numpy.integer)
        isinstance(value, numpy.datetime64)

    Args:
        value: The scalar value to classify

    Returns:
        ScalarType enum value, or None if not a recognized scalar type

    Examples:
        >>> classify_scalar(42) == ScalarType.INT64
        >>> classify_scalar(3.14) == ScalarType.FLOAT64
        >>> classify_scalar(None) == ScalarType.NONE
    """
    value_type = type(value)

    # Fast path: direct type lookup for built-in Python types (O(1))
    try:
        return _TYPE_LOOKUP[value_type]
    except KeyError:
        pass

    # Slow path: check for numpy/pyarrow by inspecting type metadata
    # This only runs for types not in _TYPE_LOOKUP
    type_module = value_type.__module__
    type_name = value_type.__name__

    # NumPy types (duck typing by module prefix and type name)
    if type_module.startswith("numpy"):
        # Check known numpy type names first
        if type_name in _NUMPY_TYPE_MAP:
            return _NUMPY_TYPE_MAP[type_name]

        # Generic numpy numeric types by name suffix/pattern
        # Check uint BEFORE int, since "uint" contains "int"
        if "uint" in type_name:
            return ScalarType.UINT64
        if "int" in type_name:
            return ScalarType.INT64
        if "float" in type_name or type_name == "floating":
            return ScalarType.FLOAT64

        # Generic numpy scalar
        return ScalarType.GENERIC_OBJECT

    # Unknown or complex object (not a scalar)
    return None


def is_scalar(value: Any) -> bool:
    """Check if a value is a recognized scalar type."""
    scalar_type = classify_scalar(value)
    # Exclude GENERIC_OBJECT; it's for unknown types, not true scalars
    return scalar_type is not None and scalar_type != ScalarType.GENERIC_OBJECT


def is_numeric_scalar(value: Any) -> bool:
    """Check if a value is a numeric scalar (int, float, or numeric numpy types)."""
    scalar_type = classify_scalar(value)
    if scalar_type is None:
        return False

    numeric_types = {
        ScalarType.INT8,
        ScalarType.INT16,
        ScalarType.INT32,
        ScalarType.INT64,
        ScalarType.UINT8,
        ScalarType.UINT16,
        ScalarType.UINT32,
        ScalarType.UINT64,
        ScalarType.FLOAT32,
        ScalarType.FLOAT64,
    }
    return scalar_type in numeric_types


def is_temporal_scalar(value: Any) -> bool:
    """Check if a value is a temporal scalar (date, time, datetime, timedelta)."""
    scalar_type = classify_scalar(value)
    if scalar_type is None:
        return False

    temporal_types = {
        ScalarType.DATE,
        ScalarType.TIME,
        ScalarType.DATETIME,
        ScalarType.DATETIME64,
        ScalarType.TIMEDELTA,
        ScalarType.TIMEDELTA64,
    }
    return scalar_type in temporal_types


def is_null_scalar(value: Any) -> bool:
    """Check if a value represents null/None."""
    return classify_scalar(value) == ScalarType.NONE


def extract_python_scalar(value: Any) -> Any:
    """
    Convert numpy scalars to native Python types.

    Replaces patterns like:
        if isinstance(p, numpy.generic):
            p = p.item()

    Uses duck typing via getattr to check for conversion methods.

    Args:
        value: The value to extract

    Returns:
        Native Python scalar, or the original value if already native
    """
    # NumPy scalars and other types with .item(): use it
    item = getattr(value, "item", None)
    if item is not None and callable(item):
        try:
            return item()
        except (TypeError, ValueError, AttributeError):
            pass

    # Already a native Python type or unknown
    return value


def unwrap_scalar(value: Any) -> Any:
    """
    Aggressively unwrap scalar containers to their native Python value.

    Handles:
    - numpy.ndarray (0-d or 1-element)
    - numpy.generic scalars
    - Native Python scalars (pass-through)

    Args:
        value: The value to unwrap

    Returns:
        Native Python scalar
    """
    # Handle 0-d or 1-element arrays
    ndim = getattr(value, "ndim", None)
    if ndim is not None:
        if ndim == 0:
            item = getattr(value, "item", None)
            if item is not None and callable(item):
                try:
                    return item()
                except (TypeError, ValueError, AttributeError):
                    pass
        elif ndim == 1:
            size = getattr(value, "size", None)
            if size == 1:
                item = getattr(value, "item", None)
                if item is not None and callable(item):
                    try:
                        return item()
                    except (TypeError, ValueError, AttributeError):
                        pass

    # Handle tolist() for arrays
    tolist = getattr(value, "tolist", None)
    if tolist is not None and callable(tolist) and not isinstance(value, (str, bytes)):
        try:
            unwrapped = tolist()
            # If it's a list of one element, extract it
            if isinstance(unwrapped, list) and len(unwrapped) == 1:
                return unwrapped[0]
            return unwrapped
        except (TypeError, ValueError, AttributeError):
            pass

    # Use standard extraction for scalars
    return extract_python_scalar(value)
