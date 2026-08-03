"""Vector type registry and discrimination utilities.

This module provides centralized type discrimination for Draken vectors,
replacing scattered hasattr() checks and string class name comparisons
with explicit, efficient enum-based routing.

Discrimination is driven by the DrakenType tag a vector carries, NOT by its
Python class: draken has one unified `Vector` (plus `BoolVector` for boolean
results), so the class name says nothing about the column's type.

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

    UNKNOWN = auto()  # Unrecognized type (error condition)


# DrakenType tag name → dispatch slot. Module scope, built ONCE at import:
# get_vector_type() runs per expression evaluation in the comparison and
# arithmetic paths, so a dict literal inside the function would allocate on
# every call.
_DRAKEN_TYPE_MAP = {
    "INT64": VectorType.INT64,
    "INT32": VectorType.INTEGER,
    "INT16": VectorType.INTEGER,
    "INT8": VectorType.INTEGER,
    # E33 — unsigned ints collapse the same way the signed family does:
    # UINT64 alongside the other 64-bit-wide type, UINT8/16/32 alongside
    # the other narrow-int widths (LogicalCategory.INTEGER already
    # collapses all eight signed+unsigned widths into one SQL-facing
    # category — this mirrors that at the vector-dispatch level).
    "UINT64": VectorType.INT64,
    "UINT32": VectorType.INTEGER,
    "UINT16": VectorType.INTEGER,
    "UINT8": VectorType.INTEGER,
    "FLOAT64": VectorType.FLOAT64,
    "FLOAT32": VectorType.FLOAT64,
    "BOOL": VectorType.BOOL,
    "VARCHAR": VectorType.STRING,
    "NVARCHAR": VectorType.STRING,
    "VARBINARY": VectorType.STRING,
    "TIMESTAMP64": VectorType.TIMESTAMP,
    "DATE32": VectorType.DATE32,
    "INTERVAL": VectorType.INTERVAL,
    "ARRAY": VectorType.ARRAY,
    "DECIMAL": VectorType.DECIMAL,
    # DECIMAL128 (int128 tier) dispatches as a decimal — the scale-aware
    # compare/arithmetic kernels intercept the physical tier at the native
    # boundary, so the evaluator treats both tiers uniformly.
    "DECIMAL128": VectorType.DECIMAL,
    # VECTOR_FP16 is the ONLY embedding-vector tag draken carries — there
    # is no bare DrakenType.VECTOR. Without this entry an embedding column
    # (see opteryx/types/vectors/embeddings.py) classified as UNKNOWN, so
    # is_draken_vector() denied a real Draken vector.
    "VECTOR_FP16": VectorType.VECTOR,
}

# The two Cython shim classes that carry a DrakenType tag. Anything else is
# not a draken vector.
_VECTOR_CLASS_NAMES = frozenset(("Vector", "BoolVector"))


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
        >>> import draken.draken_native as dn
        >>> from draken.vectors.vector import Vector
        >>> vec = Vector(dn.vector_from_string_sequence([b"a", b"b"]))
        >>> get_vector_type(vec) == VectorType.STRING
        True
    """
    # All draken vectors are "Vector" or "BoolVector" (Cython shim).
    # Discriminate via the DrakenType tag on the object itself.
    if obj.__class__.__name__ in _VECTOR_CLASS_NAMES:
        draken_type = getattr(obj, "type", None)
        if draken_type is None:
            return VectorType.UNKNOWN
        return _DRAKEN_TYPE_MAP.get(draken_type.name, VectorType.UNKNOWN)

    return VectorType.UNKNOWN


def is_draken_vector(obj) -> bool:
    """Check if object is a native Draken vector (not a scalar).

    Native Draken vectors are the compiled `Vector` / `BoolVector` shims
    carrying a recognised DrakenType tag. This excludes:
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
        >>> import draken.draken_native as dn
        >>> from draken.vectors.vector import Vector
        >>> vec = Vector(dn.vector_from_sequence([1, 2, 3]))
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
