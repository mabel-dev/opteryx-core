"""
Internal Opteryx type system - the Draken-native engine.

This module provides the canonical type system for Opteryx, providing the canonical SQL type vocabulary.

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
from typing import Any, Callable, Dict, Tuple, Type

__all__ = [
    "SqlType",
    "PYTHON_TO_SQL_MAP",
    "SQL_TO_PYTHON_MAP",
    "find_compatible_type",
    "sql_to_column_type",
]


class SqlType(Enum):
    """Canonical type system for Opteryx.

    The canonical SQL type vocabulary.
    """

    # Sentinel type for missing/unknown types
    _MISSING_TYPE = "_MISSING_TYPE"

    # Core scalar types
    NULL = "NULL"
    BOOLEAN = "BOOLEAN"
    INTEGER = "INTEGER"
    DOUBLE = "DOUBLE"
    VARCHAR = "VARCHAR"
    NVARCHAR = "NVARCHAR"
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
    # Polymorphic JSON value (result of `->`); concrete type resolved at runtime.
    # Backed by JSON-text storage; exposes to Python as str. Extraction-only —
    # most other operations raise and require the user to extract/cast.
    VARIANT = "VARIANT"

    @property
    def python_type(self) -> Type:
        """Get the native Python type for this SqlType.

        Examples:
            SqlType.INTEGER.python_type -> int
            SqlType.VARCHAR.python_type -> str
            SqlType.TIMESTAMP.python_type -> datetime.datetime
        """
        return _TYPE_TO_PYTHON.get(self, object)

    @property
    def native_type(self) -> str:
        """Get the native type identifier for this SqlType.

        Returns a string identifier that describes the native representation
        of this type. These strings are compatible with numpy dtype names
        and other type systems.

        Used during type system transitions and by the expression evaluator
        for constant value handling.

        Returns:
            String type identifier (e.g., "int32", "float64", "datetime64[us]")

        Examples:
            SqlType.INTEGER.native_type -> "int32"
            SqlType.DOUBLE.native_type -> "float64"
            SqlType.TIMESTAMP.native_type -> "datetime64[us]"
        """
        from opteryx.types._native_types import get_native_type

        return get_native_type(self.value)

    def parse(self, value: Any) -> Any:
        """Parse a value to this SqlType.

        Converts string or other representations to the canonical Python type.

        Args:
            value: Value to parse

        Returns:
            Parsed value in the canonical Python type for this SqlType

        Examples:
            SqlType.INTEGER.parse("42") -> 42
            SqlType.DOUBLE.parse("3.14") -> 3.14
            SqlType.DATE.parse("2024-01-15") -> datetime.date(2024, 1, 15)
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

    def is_string(self) -> bool:
        """Check if this is a string type (VARCHAR, BLOB)."""
        return self in _STRING_TYPES

    @classmethod
    def from_name(cls, name: str) -> Tuple["SqlType", None, None, None, None]:
        """Get SqlType from string name.

        Returns tuple (SqlType, None, None, None, None) for tuple-shaped compatibility.

        Args:
            name: Type name as string (e.g., "INTEGER", "VARCHAR")

        Returns:
            Tuple of (SqlType, None, None, None, None)

        Raises:
            ValueError: If type name not recognized

        Examples:
            SqlType.from_name("INTEGER") -> (SqlType.INTEGER, None, None, None, None)
        """
        # Normalize to uppercase for simple lookup
        upper = name.strip().upper()

        # Handle parameterized types: DECIMAL(p,s), VARCHAR(n), etc.
        if "(" in upper:
            base = upper[:upper.index("(")].strip()
            params = upper[upper.index("(")+1:upper.rindex(")")].strip()
            parts = [p.strip() for p in params.split(",")]
            precision = int(parts[0]) if parts and parts[0].isdigit() else None
            scale = int(parts[1]) if len(parts) > 1 and parts[1].isdigit() else None
            _aliases_p = {"INT": "INTEGER", "FLOAT": "DOUBLE", "STRING": "VARCHAR",
                          "TEXT": "VARCHAR", "BOOL": "BOOLEAN", "BYTES": "BLOB"}
            base = _aliases_p.get(base, base)
            try:
                return (cls[base], None, precision, scale, None)
            except KeyError:
                raise ValueError(f"Unknown SqlType: {name}")

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
            raise ValueError(f"Unknown SqlType: {name}")


# Type metadata: SqlType -> Python native type
_TYPE_TO_PYTHON: Dict[SqlType, Type] = {
    SqlType.NULL: type(None),
    SqlType.BOOLEAN: bool,
    SqlType.INTEGER: int,
    SqlType.DOUBLE: float,
    SqlType.VARCHAR: str,
    SqlType.NVARCHAR: str,
    SqlType.BLOB: bytes,
    SqlType.DATE: datetime.date,
    SqlType.TIME: datetime.time,
    SqlType.TIMESTAMP: datetime.datetime,
    SqlType.INTERVAL: datetime.timedelta,
    SqlType.DECIMAL: decimal.Decimal,
    SqlType.ARRAY: list,
    SqlType.STRUCT: dict,
    SqlType.VECTOR: list,
    SqlType.JSONB: dict,
    SqlType.VARIANT: str,
}

# Reverse mapping: Python type -> SqlType
# Built dynamically to ensure consistency
PYTHON_TO_SQL_MAP: Dict[Type, SqlType] = {
    type(None): SqlType.NULL,
    bool: SqlType.BOOLEAN,
    int: SqlType.INTEGER,
    float: SqlType.DOUBLE,
    str: SqlType.VARCHAR,
    bytes: SqlType.BLOB,
    bytearray: SqlType.BLOB,
    memoryview: SqlType.BLOB,
    datetime.date: SqlType.DATE,
    datetime.time: SqlType.TIME,
    datetime.datetime: SqlType.TIMESTAMP,
    datetime.timedelta: SqlType.INTERVAL,
    decimal.Decimal: SqlType.DECIMAL,
    list: SqlType.ARRAY,
    tuple: SqlType.ARRAY,
    dict: SqlType.STRUCT,
    set: SqlType.ARRAY,
}

# Reverse mapping: SqlType -> Python type (for completeness)
SQL_TO_PYTHON_MAP: Dict[SqlType, Type] = _TYPE_TO_PYTHON.copy()

# fmt: off
# Type classification sets (for fast membership checks)
_NUMERIC_TYPES = {SqlType.INTEGER, SqlType.DOUBLE, SqlType.DECIMAL}
_TEMPORAL_TYPES = {SqlType.DATE, SqlType.TIME, SqlType.TIMESTAMP, SqlType.INTERVAL}
_COMPLEX_TYPES = {SqlType.ARRAY, SqlType.STRUCT, SqlType.JSONB, SqlType.VECTOR}
_STRING_TYPES = {SqlType.VARCHAR, SqlType.NVARCHAR, SqlType.BLOB}
_LARGE_OBJECT_TYPES = {SqlType.BLOB, SqlType.JSONB, SqlType.ARRAY, SqlType.STRUCT, SqlType.VECTOR}
# fmt: on


# Parser functions: SqlType -> callable that converts string to native type
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


_PARSERS: Dict[SqlType, Callable[[Any], Any]] = {
    SqlType.BOOLEAN: _parse_boolean,
    SqlType.INTEGER: _parse_integer,
    SqlType.DOUBLE: _parse_double,
    SqlType.DECIMAL: _parse_decimal,
    SqlType.VARCHAR: _parse_varchar,
    SqlType.NVARCHAR: _parse_varchar,
    SqlType.VARIANT: _parse_varchar,
    SqlType.BLOB: _parse_blob,
    SqlType.DATE: _parse_date,
    SqlType.TIME: _parse_time,
    SqlType.TIMESTAMP: _parse_timestamp,
    SqlType.INTERVAL: _parse_interval,
}


def find_compatible_type(types: list) -> SqlType:
    """Find a compatible type that can represent all types in the list.

    Implements type promotion/coercion rules:
    - NULL promotes to any type
    - BOOLEAN < INTEGER < DOUBLE
    - DOUBLE < DECIMAL
    - VARCHAR can represent anything
    - Incompatible types fall back to VARCHAR

    Args:
        types: List of SqlType to find common type for

    Returns:
        SqlType that can represent all input types

    Examples:
        find_compatible_type([SqlType.INTEGER, SqlType.DOUBLE]) -> SqlType.DOUBLE
        find_compatible_type([SqlType.BOOLEAN, SqlType.INTEGER]) -> SqlType.INTEGER
    """
    if not types:
        return SqlType.NULL

    # Filter out NULL types
    non_null_types = [t for t in types if t != SqlType.NULL]
    if not non_null_types:
        return SqlType.NULL

    # All same type
    if len(set(non_null_types)) == 1:
        return non_null_types[0]

    # Numeric promotion: BOOLEAN < INTEGER < DOUBLE < DECIMAL
    if all(t in _NUMERIC_TYPES or t == SqlType.BOOLEAN for t in non_null_types):
        if SqlType.DECIMAL in non_null_types:
            return SqlType.DECIMAL
        if SqlType.DOUBLE in non_null_types:
            return SqlType.DOUBLE
        if SqlType.INTEGER in non_null_types:
            return SqlType.INTEGER
        return SqlType.BOOLEAN

    # Temporal types: mixed temporal is VARCHAR
    if any(t in _TEMPORAL_TYPES for t in non_null_types):
        return SqlType.VARCHAR

    # Complex types: mixed complex is VARCHAR or JSONB
    if any(t in _COMPLEX_TYPES for t in non_null_types):
        return SqlType.JSONB

    # Default fallback: VARCHAR can represent anything
    return SqlType.VARCHAR


def sql_to_column_type(
    sql_type: "SqlType",
    precision: int = None,
    scale: int = None,
    element_type: "SqlType" = None,
):
    """MIGRATION BRIDGE (TEMPORARY) — map a legacy SqlType (+ side-car params) to a
    unified ColumnType.

    EXIT PLAN (see plan "Exit Plan for Bridges & Shims"): this function exists ONLY for
    the SqlType → ColumnType migration. It is deleted in Phase 6 together with
    SqlType, once `FlatColumn.type` is itself a `ColumnType` and `grep sql_to_column_type`
    returns zero call sites. It is a pure projection of legacy data — it must NEVER
    fabricate type information to avoid a failure. Where the legacy system genuinely
    lacks the information (e.g. a DECIMAL with no precision), this FAILS LOUDLY so the
    gap is fixed at the source, not hidden.

    Faithful mappings (not guesses — these reflect what the legacy data actually is):
      INTEGER -> INT64   (legacy INTEGER was always 64-bit)
      DOUBLE  -> FLOAT64
      BLOB    -> VARBINARY (BLOB is dropped)
      JSONB   -> NVARCHAR  (alias today)
      STRUCT  -> NVARCHAR  (engine stringifies structs to JSON text; not a real type)
      VARIANT -> VARIANT   (first-class JSON value)
    TIME/TIMESTAMP default to microseconds because that IS the engine's storage unit.
    """
    from opteryx.types import logical_type as _lt

    t = sql_type
    if t == SqlType.BOOLEAN:
        return _lt.BOOLEAN
    if t == SqlType.INTEGER:
        return _lt.INT64
    if t == SqlType.DOUBLE:
        return _lt.FLOAT64
    if t == SqlType.VARCHAR:
        return _lt.VARCHAR
    if t == SqlType.NVARCHAR:
        return _lt.NVARCHAR
    if t == SqlType.BLOB:
        return _lt.VARBINARY
    if t == SqlType.DATE:
        return _lt.DATE
    if t == SqlType.TIME:
        return _lt.TIME()
    if t == SqlType.TIMESTAMP:
        return _lt.TIMESTAMP()
    if t == SqlType.INTERVAL:
        return _lt.INTERVAL
    if t == SqlType.DECIMAL:
        # No fabrication: a DECIMAL with no precision/scale is an UNKNOWN type, not a
        # default. The legacy system loses decimal precision in places (e.g. operator_map
        # returns bare DECIMAL) — that gap must be fixed by compute_result_logical_type /
        # connector inference, not papered over here. Fail loud.
        if precision is None or scale is None:
            raise NotImplementedError(
                "legacy DECIMAL column has no precision/scale — refusing to fabricate a "
                "default. Fix the source (result-type derivation or connector inference) "
                "so the precision/scale is known."
            )
        # I-7 raised the DECIMAL precision cap from 18 to 38 (DECIMAL128 backing).
        # The bridge follows; `lt.DECIMAL` picks the physical tier (DECIMAL int64
        # for p≤18, DECIMAL128 int128 for 19≤p≤38) automatically.
        if not (1 <= precision <= 38) or not (0 <= scale <= precision):
            raise ValueError(f"invalid DECIMAL(precision={precision}, scale={scale})")
        return _lt.DECIMAL(precision, scale)
    if t == SqlType.JSONB:
        return _lt.NVARCHAR
    if t == SqlType.STRUCT:
        return _lt.NVARCHAR
    if t == SqlType.VARIANT:
        return _lt.VARIANT
    if t == SqlType.NULL:
        return _lt.NULL
    if t == SqlType.ARRAY:
        # D-4 Phase 2: ColumnType now carries `element` for ARRAY. Map the legacy
        # element_type (SqlType) → ColumnType element. Defaults to ARRAY<VARCHAR>
        # when the legacy data didn't track an element_type (matches earlier behavior
        # where unspecified array element fell back to string semantics).
        if element_type is None:
            elem_ct = _lt.VARCHAR
        else:
            elem_ct = sql_to_column_type(element_type, precision, scale, None)
        return _lt.ARRAY(elem_ct)
    if t == SqlType.VECTOR:
        raise NotImplementedError(
            "VECTOR requires a dimension; legacy SqlType.VECTOR carries none — needs a "
            "dimension source before it can map to a ColumnType"
        )
    raise NotImplementedError(f"no ColumnType mapping for {t!r}")


def column_type_to_sql(column_type) -> dict:
    """Inverse bridge: derive (type, precision, scale, element_type) from a ColumnType.

    D-4 Phase 2 writer migration: callers passing a `ColumnType` to
    `FlatColumn.from_column_type` get the legacy fields populated via this
    helper. Deleted in Phase 6 together with `sql_to_column_type` and
    SqlType itself, once writers stop needing legacy fields.
    """
    from draken.draken_native import DrakenType
    from opteryx.types.logical_type import LogicalCategory

    if column_type is None:
        return {}

    out = {}
    cat = column_type.category

    if cat == LogicalCategory.BOOLEAN:
        out["type"] = SqlType.BOOLEAN
    elif cat == LogicalCategory.INTEGER:
        out["type"] = SqlType.INTEGER
    elif cat == LogicalCategory.FLOAT:
        out["type"] = SqlType.DOUBLE
    elif cat == LogicalCategory.DECIMAL:
        out["type"] = SqlType.DECIMAL
        out["precision"] = column_type.logical.precision
        out["scale"] = column_type.logical.scale
    elif cat == LogicalCategory.DATE:
        out["type"] = SqlType.DATE
    elif cat == LogicalCategory.TIME:
        out["type"] = SqlType.TIME
    elif cat == LogicalCategory.TIMESTAMP:
        out["type"] = SqlType.TIMESTAMP
    elif cat == LogicalCategory.INTERVAL:
        out["type"] = SqlType.INTERVAL
    elif cat == LogicalCategory.VARCHAR:
        out["type"] = SqlType.VARCHAR
    elif cat == LogicalCategory.NVARCHAR:
        out["type"] = SqlType.NVARCHAR
    elif cat == LogicalCategory.VARBINARY:
        out["type"] = SqlType.BLOB
    elif cat == LogicalCategory.VARIANT:
        out["type"] = SqlType.VARIANT
    elif cat == LogicalCategory.ARRAY:
        out["type"] = SqlType.ARRAY
        # Reverse-bridge the element ColumnType to its SqlType representation.
        if column_type.element is not None:
            elem_legacy = column_type_to_sql(column_type.element)
            out["element_type"] = elem_legacy.get("type")
    elif cat == LogicalCategory.VECTOR:
        out["type"] = SqlType.VECTOR
    elif cat == LogicalCategory.NULL:
        out["type"] = SqlType.NULL
    else:
        raise NotImplementedError(
            f"column_type_to_sql: no legacy mapping for category {cat!r}"
        )
    return out
