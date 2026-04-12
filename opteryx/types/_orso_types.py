"""
Internal Opteryx type system - inlined and optimized from orso.types.

This module provides the canonical type system for Opteryx, eliminating the
external orso dependency while specializing types for Opteryx's needs.

Key types:
- NULL, BOOLEAN, INTEGER, DOUBLE, VARCHAR, BLOB (core scalars)
- DATE, TIME, TIMESTAMP, INTERVAL (temporal)
- DECIMAL, ARRAY, STRUCT, VECTOR, JSONB (complex)

Design:
- Enum-based for O(1) lookups and type safety
- Metadata registry for properties and methods
- No external dependencies (stdlib only)
- Optimized for hot paths (type checking, parsing)
"""

import datetime
import decimal
from enum import Enum
from typing import Any, Callable, Dict, Optional, Tuple, Type, Union

__all__ = [
    "OrsoTypes",
    "PYTHON_TO_ORSO_MAP",
    "ORSO_TO_PYTHON_MAP",
    "find_compatible_type",
]


class OrsoTypes(Enum):
    """Canonical type system for Opteryx.

    Replaces orso.types.OrsoTypes with optimized, dependency-free implementation.
    """

    # Sentinel type for missing/unknown types
    _MISSING_TYPE = "_MISSING_TYPE"

    # Core scalar types
    NULL = "NULL"
    BOOLEAN = "BOOLEAN"
    INTEGER = "INTEGER"
    DOUBLE = "DOUBLE"
    VARCHAR = "VARCHAR"
    BLOB = "BLOB"

    # Temporal types
    DATE = "DATE"
    TIME = "TIME"
    TIMESTAMP = "TIMESTAMP"
    INTERVAL = "INTERVAL"

    # Complex types
    DECIMAL = "DECIMAL"
    ARRAY = "ARRAY"
    STRUCT = "STRUCT"
    VECTOR = "VECTOR"
    JSONB = "JSONB"

    @property
    def python_type(self) -> Type:
        """Get the native Python type for this OrsoType.

        Examples:
            OrsoTypes.INTEGER.python_type -> int
            OrsoTypes.VARCHAR.python_type -> str
            OrsoTypes.TIMESTAMP.python_type -> datetime.datetime
        """
        return _TYPE_TO_PYTHON.get(self, object)

    @property
    def native_type(self) -> str:
        """Get the native type identifier for this OrsoType.

        Returns a string identifier that describes the native representation
        of this type. These strings are compatible with numpy dtype names
        and other type systems.

        Used during type system transitions and by the expression evaluator
        for constant value handling.

        Returns:
            String type identifier (e.g., "int32", "float64", "datetime64[us]")

        Examples:
            OrsoTypes.INTEGER.native_type -> "int32"
            OrsoTypes.DOUBLE.native_type -> "float64"
            OrsoTypes.TIMESTAMP.native_type -> "datetime64[us]"
        """
        from opteryx.types._native_types import get_native_type

        return get_native_type(self.value)

    def parse(self, value: Any) -> Any:
        """Parse a value to this OrsoType.

        Converts string or other representations to the canonical Python type.

        Args:
            value: Value to parse

        Returns:
            Parsed value in the canonical Python type for this OrsoType

        Examples:
            OrsoTypes.INTEGER.parse("42") -> 42
            OrsoTypes.DOUBLE.parse("3.14") -> 3.14
            OrsoTypes.DATE.parse("2024-01-15") -> datetime.date(2024, 1, 15)
        """
        if value is None:
            return None

        parser = _PARSERS.get(self)
        if parser is None:
            return value

        try:
            return parser(value)
        except (ValueError, TypeError, AttributeError):
            # If parsing fails, return original value
            return value

    def is_numeric(self) -> bool:
        """Check if this is a numeric type (INTEGER, DOUBLE, DECIMAL)."""
        return self in _NUMERIC_TYPES

    def is_temporal(self) -> bool:
        """Check if this is a temporal type (DATE, TIME, TIMESTAMP, INTERVAL)."""
        return self in _TEMPORAL_TYPES

    def is_complex(self) -> bool:
        """Check if this is a complex type (ARRAY, STRUCT, JSONB, VECTOR)."""
        return self in _COMPLEX_TYPES

    def is_large_object(self) -> bool:
        """Check if this is a large object type (BLOB, JSONB, ARRAY, STRUCT, VECTOR)."""
        return self in _LARGE_OBJECT_TYPES

    @classmethod
    def from_name(cls, name: str) -> Tuple["OrsoTypes", None, None, None, None]:
        """Get OrsoType from string name.

        Returns tuple (OrsoType, None, None, None, None) for compatibility with orso.

        Args:
            name: Type name as string (e.g., "INTEGER", "VARCHAR")

        Returns:
            Tuple of (OrsoType, None, None, None, None)

        Raises:
            ValueError: If type name not recognized

        Examples:
            OrsoTypes.from_name("INTEGER") -> (OrsoTypes.INTEGER, None, None, None, None)
        """
        # Normalize to uppercase for simple lookup
        upper = name.strip().upper()

        # Handle compound types: array<element>, list<element>, struct<...>
        if "<" in upper:
            outer, rest = upper.split("<", 1)
            outer = outer.strip()
            inner = rest.rstrip(">").strip()
            try:
                outer_type = cls[outer] if outer in cls.__members__ else cls["ARRAY"]
            except KeyError:
                outer_type = cls.ARRAY
            # Parse element type - map common aliases
            _alias = {
                "INT": "INTEGER",
                "INT32": "INTEGER",
                "INT64": "INTEGER",
                "BIGINT": "INTEGER",
                "FLOAT": "DOUBLE",
                "FLOAT32": "DOUBLE",
                "FLOAT64": "DOUBLE",
                "STRING": "VARCHAR",
                "TEXT": "VARCHAR",
                "BOOL": "BOOLEAN",
                "BYTES": "BLOB",
            }
            inner_key = _alias.get(inner, inner)
            try:
                element_type = cls[inner_key]
            except KeyError:
                element_type = None
            return (outer_type, None, None, None, element_type)

        # Map common type name aliases to canonical enum keys
        _aliases = {
            "INT": "INTEGER",
            "INT32": "INTEGER",
            "INT64": "INTEGER",
            "BIGINT": "INTEGER",
            "FLOAT": "DOUBLE",
            "FLOAT32": "DOUBLE",
            "FLOAT64": "DOUBLE",
            "STRING": "VARCHAR",
            "TEXT": "VARCHAR",
            "BOOL": "BOOLEAN",
            "BYTES": "BLOB",
        }
        key = _aliases.get(upper, upper)
        try:
            return (cls[key], None, None, None, None)
        except KeyError:
            raise ValueError(f"Unknown OrsoType: {name}")


# Type metadata: OrsoType -> Python native type
_TYPE_TO_PYTHON: Dict[OrsoTypes, Type] = {
    OrsoTypes.NULL: type(None),
    OrsoTypes.BOOLEAN: bool,
    OrsoTypes.INTEGER: int,
    OrsoTypes.DOUBLE: float,
    OrsoTypes.VARCHAR: str,
    OrsoTypes.BLOB: bytes,
    OrsoTypes.DATE: datetime.date,
    OrsoTypes.TIME: datetime.time,
    OrsoTypes.TIMESTAMP: datetime.datetime,
    OrsoTypes.INTERVAL: datetime.timedelta,
    OrsoTypes.DECIMAL: decimal.Decimal,
    OrsoTypes.ARRAY: list,
    OrsoTypes.STRUCT: dict,
    OrsoTypes.VECTOR: list,
    OrsoTypes.JSONB: dict,
}

# Reverse mapping: Python type -> OrsoType
# Built dynamically to ensure consistency
PYTHON_TO_ORSO_MAP: Dict[Type, OrsoTypes] = {
    type(None): OrsoTypes.NULL,
    bool: OrsoTypes.BOOLEAN,
    int: OrsoTypes.INTEGER,
    float: OrsoTypes.DOUBLE,
    str: OrsoTypes.VARCHAR,
    bytes: OrsoTypes.BLOB,
    bytearray: OrsoTypes.BLOB,
    memoryview: OrsoTypes.BLOB,
    datetime.date: OrsoTypes.DATE,
    datetime.time: OrsoTypes.TIME,
    datetime.datetime: OrsoTypes.TIMESTAMP,
    datetime.timedelta: OrsoTypes.INTERVAL,
    decimal.Decimal: OrsoTypes.DECIMAL,
    list: OrsoTypes.ARRAY,
    tuple: OrsoTypes.ARRAY,
    dict: OrsoTypes.STRUCT,
    set: OrsoTypes.ARRAY,
}

# Reverse mapping: OrsoType -> Python type (for completeness)
ORSO_TO_PYTHON_MAP: Dict[OrsoTypes, Type] = _TYPE_TO_PYTHON.copy()


# Type classification sets (for fast membership checks)
_NUMERIC_TYPES = {OrsoTypes.INTEGER, OrsoTypes.DOUBLE, OrsoTypes.DECIMAL}
_TEMPORAL_TYPES = {OrsoTypes.DATE, OrsoTypes.TIME, OrsoTypes.TIMESTAMP, OrsoTypes.INTERVAL}
_COMPLEX_TYPES = {OrsoTypes.ARRAY, OrsoTypes.STRUCT, OrsoTypes.JSONB, OrsoTypes.VECTOR}
_LARGE_OBJECT_TYPES = {
    OrsoTypes.BLOB,
    OrsoTypes.JSONB,
    OrsoTypes.ARRAY,
    OrsoTypes.STRUCT,
    OrsoTypes.VECTOR,
}


# Parser functions: OrsoType -> callable that converts string to native type
def _parse_boolean(value: Any) -> bool:
    """Parse to boolean."""
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.lower() in ("true", "1", "yes", "on")
    return bool(value)


def _parse_integer(value: Any) -> int:
    """Parse to integer."""
    if isinstance(value, int) and not isinstance(value, bool):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str):
        return int(value.strip())
    return int(value)


def _parse_double(value: Any) -> float:
    """Parse to double."""
    if isinstance(value, float):
        return value
    if isinstance(value, int) and not isinstance(value, bool):
        return float(value)
    if isinstance(value, str):
        return float(value.strip())
    return float(value)


def _parse_decimal(value: Any) -> decimal.Decimal:
    """Parse to Decimal."""
    if isinstance(value, decimal.Decimal):
        return value
    if isinstance(value, (int, float)):
        return decimal.Decimal(str(value))
    if isinstance(value, str):
        return decimal.Decimal(value.strip())
    return decimal.Decimal(value)


def _parse_varchar(value: Any) -> str:
    """Parse to string."""
    if isinstance(value, str):
        return value
    if isinstance(value, bytes):
        return value.decode("utf-8")
    return str(value)


def _parse_blob(value: Any) -> bytes:
    """Parse to bytes."""
    if isinstance(value, bytes):
        return value
    if isinstance(value, str):
        return value.encode("utf-8")
    if isinstance(value, bytearray):
        return bytes(value)
    if isinstance(value, memoryview):
        return bytes(value)
    return bytes(value)


def _parse_date(value: Any) -> datetime.date:
    """Parse to date."""
    if isinstance(value, datetime.date) and not isinstance(value, datetime.datetime):
        return value
    if isinstance(value, datetime.datetime):
        return value.date()
    if isinstance(value, str):
        # Simple ISO8601 parsing: YYYY-MM-DD
        parts = value.strip().split("-")
        if len(parts) == 3:
            return datetime.date(int(parts[0]), int(parts[1]), int(parts[2]))
    raise ValueError(f"Cannot parse {value} as date")


def _parse_time(value: Any) -> datetime.time:
    """Parse to time."""
    if isinstance(value, datetime.time):
        return value
    if isinstance(value, str):
        # Simple HH:MM:SS parsing
        parts = value.strip().split(":")
        if len(parts) >= 2:
            return datetime.time(
                int(parts[0]), int(parts[1]), int(parts[2]) if len(parts) > 2 else 0
            )
    raise ValueError(f"Cannot parse {value} as time")


def _parse_timestamp(value: Any) -> datetime.datetime:
    """Parse to timestamp."""
    if isinstance(value, datetime.datetime):
        return value
    if isinstance(value, datetime.date):
        return datetime.datetime.combine(value, datetime.time())
    if isinstance(value, str):
        # Simple ISO8601 parsing: YYYY-MM-DD[T ]HH:MM:SS[.fff]
        value_str = value.strip().replace("T", " ")
        try:
            if "." in value_str:
                return datetime.datetime.strptime(value_str, "%Y-%m-%d %H:%M:%S.%f")
            else:
                return datetime.datetime.strptime(value_str, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            # Try date-only
            return datetime.datetime.strptime(value_str.split(" ")[0], "%Y-%m-%d")
    raise ValueError(f"Cannot parse {value} as timestamp")


def _parse_interval(value: Any) -> datetime.timedelta:
    """Parse to interval (timedelta)."""
    if isinstance(value, datetime.timedelta):
        return value
    if isinstance(value, (int, float)):
        # Assume seconds
        return datetime.timedelta(seconds=value)
    raise ValueError(f"Cannot parse {value} as interval")


_PARSERS: Dict[OrsoTypes, Callable[[Any], Any]] = {
    OrsoTypes.BOOLEAN: _parse_boolean,
    OrsoTypes.INTEGER: _parse_integer,
    OrsoTypes.DOUBLE: _parse_double,
    OrsoTypes.DECIMAL: _parse_decimal,
    OrsoTypes.VARCHAR: _parse_varchar,
    OrsoTypes.BLOB: _parse_blob,
    OrsoTypes.DATE: _parse_date,
    OrsoTypes.TIME: _parse_time,
    OrsoTypes.TIMESTAMP: _parse_timestamp,
    OrsoTypes.INTERVAL: _parse_interval,
}


def find_compatible_type(types: list) -> OrsoTypes:
    """Find a compatible type that can represent all types in the list.

    Implements type promotion/coercion rules:
    - NULL promotes to any type
    - BOOLEAN < INTEGER < DOUBLE
    - DOUBLE < DECIMAL
    - VARCHAR can represent anything
    - Incompatible types fall back to VARCHAR

    Args:
        types: List of OrsoTypes to find common type for

    Returns:
        OrsoType that can represent all input types

    Examples:
        find_compatible_type([OrsoTypes.INTEGER, OrsoTypes.DOUBLE]) -> OrsoTypes.DOUBLE
        find_compatible_type([OrsoTypes.BOOLEAN, OrsoTypes.INTEGER]) -> OrsoTypes.INTEGER
    """
    if not types:
        return OrsoTypes.NULL

    # Filter out NULL types
    non_null_types = [t for t in types if t != OrsoTypes.NULL]
    if not non_null_types:
        return OrsoTypes.NULL

    # All same type
    if len(set(non_null_types)) == 1:
        return non_null_types[0]

    # Numeric promotion: BOOLEAN < INTEGER < DOUBLE < DECIMAL
    if all(t in _NUMERIC_TYPES or t == OrsoTypes.BOOLEAN for t in non_null_types):
        if OrsoTypes.DECIMAL in non_null_types:
            return OrsoTypes.DECIMAL
        if OrsoTypes.DOUBLE in non_null_types:
            return OrsoTypes.DOUBLE
        if OrsoTypes.INTEGER in non_null_types:
            return OrsoTypes.INTEGER
        return OrsoTypes.BOOLEAN

    # Temporal types: mixed temporal is VARCHAR
    if any(t in _TEMPORAL_TYPES for t in non_null_types):
        return OrsoTypes.VARCHAR

    # Complex types: mixed complex is VARCHAR or JSONB
    if any(t in _COMPLEX_TYPES for t in non_null_types):
        return OrsoTypes.JSONB

    # Default fallback: VARCHAR can represent anything
    return OrsoTypes.VARCHAR
