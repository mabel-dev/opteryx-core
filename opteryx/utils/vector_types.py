"""Vector type registry and discrimination utilities.

This module provides centralized type discrimination for Draken vectors,
replacing scattered hasattr() checks and string class name comparisons
with explicit, efficient enum-based routing.

Philosophy: Performance > convenience. Explicit dispatch > magic.
"""

from enum import Enum, auto


class VectorType(Enum):
    """Enumerated vector types for explicit dispatch.

    Each Draken vector type has a dedicated enum value to enable:
    - Fast type-based routing in hot paths
    - Clear dispatch tables (e.g., in draken_compare)
    - Easy addition of new vector types
    - No runtime string comparisons
    """

    STRING = auto()
    INT64 = auto()
    INTEGER = auto()
    FLOAT64 = auto()
    BOOL = auto()
    TIMESTAMP = auto()
    DATE32 = auto()
    INTERVAL = auto()
    ARRAY = auto()
    VECTOR = auto()
    DECIMAL = auto()  # Decimal column, int64-backed with scale metadata

    CONSTANT_ENCODED = auto()  # Special: constant values encoded as typed vector
    DICTIONARY_ENCODED = auto()  # Special: dictionary-encoded categorical vectors
    UNKNOWN = auto()  # Unrecognized type (error condition)


def get_vector_type(obj) -> VectorType:
    """Discriminate vector type explicitly without hasattr() checks.

    This is the canonical entry point for type discrimination. Replaces:
    - hasattr(obj, "null_count") checks
    - obj.__class__.__name__ == "XVector" comparisons
    - scattered isinstance() chains

    Args:
        obj: Object to classify (likely a Draken vector or scalar)

    Returns:
        VectorType enum value for explicit routing in dispatch tables

    Examples:
        >>> from opteryx.utils.vector_types import get_vector_type, VectorType
        >>> from opteryx.compiled.draken.vectors import StringVector
        >>> vec = StringVector.from_constant("a", 3)
        >>> get_vector_type(vec) == VectorType.STRING
        True
    """
    cls_name = obj.__class__.__name__

    # Direct class name mapping - fast path for common cases
    TYPE_MAP = {
        "StringVector": VectorType.STRING,
        "Int64Vector": VectorType.INT64,
        "IntegerVector": VectorType.INTEGER,
        "Float64Vector": VectorType.FLOAT64,
        "BoolVector": VectorType.BOOL,
        "TimestampVector": VectorType.TIMESTAMP,
        "Date32Vector": VectorType.DATE32,
        "IntervalVector": VectorType.INTERVAL,
        "ArrayVector": VectorType.ARRAY,
        "VectorVector": VectorType.VECTOR,
        "DecimalVector": VectorType.DECIMAL,
    }

    if cls_name in TYPE_MAP:
        return TYPE_MAP[cls_name]

    # Special cases: constant/dictionary encoded vectors
    # These are detected by special flags rather than class name
    if hasattr(obj, "_is_constant_encoded") and obj._is_constant_encoded:
        return VectorType.CONSTANT_ENCODED
    if hasattr(obj, "_is_dictionary_encoded") and obj._is_dictionary_encoded:
        return VectorType.DICTIONARY_ENCODED

    return VectorType.UNKNOWN


def is_draken_vector(obj) -> bool:
    """Check if object is a native Draken vector (not a scalar or Arrow wrapper).

    Native Draken vectors are compiled/optimized vector types (StringVector,
    Int64Vector, etc.). This excludes:
    - Raw Python scalars (int, str, bool, etc.)

    Args:
        obj: Object to test

    Returns:
        True if obj is a Draken vector type, False otherwise
    """
    vec_type = get_vector_type(obj)
    return vec_type not in (VectorType.UNKNOWN,)


def is_scalar(obj) -> bool:
    """Check if object is a raw Python scalar (not a vector or array).

    Used to discriminate between scalars and vectors when both might have
    vector-like attributes. This replaces:
    - hasattr(obj, "null_count") checks (brittle, unreliable)
    - Complex isinstance chains

    Args:
        obj: Object to test

    Returns:
        True if obj is a raw Python scalar, False if it's a vector/array

    Examples:
        >>> is_scalar(42)
        True
        >>> is_scalar("hello")
        True
        >>> is_scalar(None)
        True
        >>> import datetime
        >>> is_scalar(datetime.date(2024, 1, 1))
        True
        >>> # For vectors, False:
        >>> from opteryx.compiled.draken.vectors import Int64Vector
        >>> vec = Int64Vector.from_constant(1, 3)
        >>> is_scalar(vec)
        False
    """
    import datetime
    import decimal

    if obj is None or isinstance(obj, (bool, int, float, str, bytes, bytearray)):
        return True
    if isinstance(obj, (datetime.date, datetime.time, datetime.datetime, datetime.timedelta)):
        return True
    if isinstance(obj, decimal.Decimal):
        return True
    return False
