"""Native type registry - no external dependencies.

Provides type information for LogicalCategory using native Python identifiers.
Maps LogicalCategory to portable type strings that work with the rest of the system.

This module replaces the legacy type mappings that were previously
in sql_type.py. It provides an abstraction layer that allows
future migrations without API churn.

Key features:
- Compatible with native dtype strings
- Clean, testable interface for type information
- Type identifiers are strings (portable across systems)

Design rationale:
- Using string identifiers keeps this module lightweight while remaining compatible with code that uses dtype strings
- The strings are identical to dtype names, so existing type
  systems (e.g. array construction and schema handling) work unchanged
- This is an internal API; external code uses LogicalCategory.native_type property
"""

from typing import Dict

__all__ = [
    "TYPE_OBJECT",
    "TYPE_BOOL",
    "TYPE_INT32",
    "TYPE_FLOAT64",
    "TYPE_DATETIME64_US",
    "TYPE_TIMEDELTA64_US",
    "SQL_TO_NATIVE_TYPE",
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
    """Build the LogicalCategory to native type mapping.

    This is built dynamically to avoid circular imports with sql_type.py.
    The mapping is created once at module import time.

    Returns:
        Dictionary mapping LogicalCategory string names to native type identifiers
    """
    # Import here to avoid circular import at module load time
    from opteryx.types.logical_type import LogicalCategory

    return {
        LogicalCategory.NULL.value: TYPE_OBJECT,
        LogicalCategory.BOOLEAN.value: TYPE_BOOL,
        LogicalCategory.INTEGER.value: TYPE_INT32,
        LogicalCategory.DOUBLE.value: TYPE_FLOAT64,
        LogicalCategory.VARCHAR.value: TYPE_OBJECT,
        LogicalCategory.NVARCHAR.value: TYPE_OBJECT,
        LogicalCategory.BLOB.value: TYPE_OBJECT,
        LogicalCategory.DATE.value: TYPE_OBJECT,
        LogicalCategory.TIME.value: TYPE_OBJECT,
        LogicalCategory.TIMESTAMP.value: TYPE_DATETIME64_US,
        LogicalCategory.INTERVAL.value: TYPE_TIMEDELTA64_US,
        LogicalCategory.DECIMAL.value: TYPE_OBJECT,
        LogicalCategory.ARRAY.value: TYPE_OBJECT,
        LogicalCategory.STRUCT.value: TYPE_OBJECT,
        LogicalCategory.VECTOR.value: TYPE_OBJECT,
        LogicalCategory.JSONB.value: TYPE_OBJECT,
        LogicalCategory.VARIANT.value: TYPE_OBJECT,
    }


# Build the mapping at module import time
SQL_TO_NATIVE_TYPE: Dict[str, str] = _build_native_type_map()


def get_native_type(sql_type_or_value: str) -> str:
    """Get the native type identifier for an LogicalCategory.

    Returns a string identifier compatible with numpy dtype strings
    and other type systems, without requiring numpy to be imported.

    Args:
        sql_type_or_value: Either an LogicalCategory enum or its string value

    Returns:
        String identifier (e.g., "int32", "float64", "datetime64[us]")

    Examples:
        >>> from opteryx.types.logical_type import LogicalCategory
        >>> get_native_type(LogicalCategory.INTEGER.value)
        "int32"
        >>> get_native_type("TIMESTAMP")
        "datetime64[us]"
    """
    # Handle both LogicalCategory enum and string values
    from enum import Enum
    if isinstance(sql_type_or_value, Enum):
        key = sql_type_or_value.value
    else:
        key = sql_type_or_value
    return SQL_TO_NATIVE_TYPE.get(key, TYPE_OBJECT)
