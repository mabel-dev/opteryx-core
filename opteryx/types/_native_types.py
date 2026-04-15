"""Native type registry - no external dependencies.

Provides type information for OrsoTypes using native Python identifiers.
Maps OrsoTypes to portable type strings that work with the rest of the system.

This module replaces the legacy type mappings that were previously
in _orso_types.py. It provides an abstraction layer that allows
future migrations without API churn.

Key features:
- Compatible with native dtype strings
- Clean, testable interface for type information
- Type identifiers are strings (portable across systems)

Design rationale:
- Using string identifiers keeps this module lightweight while remaining compatible with code that uses dtype strings
- The strings are identical to dtype names, so existing type
  systems (e.g. array construction and schema handling) work unchanged
- This is an internal API; external code uses OrsoTypes.native_type property
"""

from typing import Dict

__all__ = [
    "TYPE_OBJECT",
    "TYPE_BOOL",
    "TYPE_INT32",
    "TYPE_FLOAT64",
    "TYPE_DATETIME64_US",
    "TYPE_TIMEDELTA64_US",
    "ORSO_TO_NATIVE_TYPE",
    "get_native_type",
]

# Native type identifiers (plain strings)
TYPE_OBJECT = "object"
TYPE_BOOL = "bool"
TYPE_INT32 = "int32"
TYPE_FLOAT64 = "float64"
TYPE_DATETIME64_US = "datetime64[us]"
TYPE_TIMEDELTA64_US = "timedelta64[us]"


def _build_native_type_map() -> Dict[str, str]:
    """Build the OrsoType to native type mapping.

    This is built dynamically to avoid circular imports with _orso_types.py.
    The mapping is created once at module import time.

    Returns:
        Dictionary mapping OrsoType string names to native type identifiers
    """
    # Import here to avoid circular import at module load time
    from opteryx.types._orso_types import OrsoTypes

    return {
        OrsoTypes.NULL.value: TYPE_OBJECT,
        OrsoTypes.BOOLEAN.value: TYPE_BOOL,
        OrsoTypes.INTEGER.value: TYPE_INT32,
        OrsoTypes.DOUBLE.value: TYPE_FLOAT64,
        OrsoTypes.VARCHAR.value: TYPE_OBJECT,
        OrsoTypes.BLOB.value: TYPE_OBJECT,
        OrsoTypes.DATE.value: TYPE_OBJECT,
        OrsoTypes.TIME.value: TYPE_OBJECT,
        OrsoTypes.TIMESTAMP.value: TYPE_DATETIME64_US,
        OrsoTypes.INTERVAL.value: TYPE_TIMEDELTA64_US,
        OrsoTypes.DECIMAL.value: TYPE_OBJECT,
        OrsoTypes.ARRAY.value: TYPE_OBJECT,
        OrsoTypes.STRUCT.value: TYPE_OBJECT,
        OrsoTypes.VECTOR.value: TYPE_OBJECT,
        OrsoTypes.JSONB.value: TYPE_OBJECT,
    }


# Build the mapping at module import time
ORSO_TO_NATIVE_TYPE: Dict[str, str] = _build_native_type_map()


def get_native_type(orso_type_or_value: str) -> str:
    """Get the native type identifier for an OrsoType.

    Returns a string identifier compatible with numpy dtype strings
    and other type systems, without requiring numpy to be imported.

    Args:
        orso_type_or_value: Either an OrsoTypes enum or its string value

    Returns:
        String identifier (e.g., "int32", "float64", "datetime64[us]")

    Examples:
        >>> from opteryx.types import OrsoTypes
        >>> get_native_type(OrsoTypes.INTEGER.value)
        "int32"
        >>> get_native_type("TIMESTAMP")
        "datetime64[us]"
    """
    # Handle both OrsoTypes enum and string values
    key = orso_type_or_value.value if hasattr(orso_type_or_value, "value") else orso_type_or_value
    return ORSO_TO_NATIVE_TYPE.get(key, TYPE_OBJECT)
