"""Opteryx column/value type — references Draken's type vocabulary (no copy).

This is the foundation of the unified type system. It does NOT define a parallel
type system; it references Draken's:

- **Decision C-i** — physical type (`DrakenType`) and logical params (`LogicalType`,
  `LogicalKind`, `TimestampUnit`) are imported from `draken.draken_native`. Opteryx
  does not keep copies of them.
- **Decision D1** — an Opteryx column/value type is a *pair*: a `DrakenType` physical
  tag plus an optional `LogicalType` descriptor. The descriptor is present only for
  parameterized physical types (DECIMAL, TIMESTAMP, TIME, VECTOR), mirroring the vector
  contract ("logical descriptor mandatory only for parameterized types").
- **Decision A** — string types (VARCHAR/NVARCHAR/VARBINARY) are unparameterized.
- **Decision B** — operator dispatch keys on `LogicalCategory`, derived here from the
  physical type.

The only genuinely Opteryx-owned type artifact is `LogicalCategory` — a binder
dispatch projection, not a competing type system.

Still-open items are NOT guessed; they raise `NotImplementedError`:
- ARRAY<element> / STRUCT / JSONB representation (Draken carries array children
  structurally on the vector, and has no STRUCT/JSONB physical type yet).
- VECTOR base-type selection (Draken has `VECTOR_FP16` only today; FP32 default per
  MSSQL convention is a Phase-2 Draken extension).
"""

from __future__ import annotations

import datetime
import decimal
from dataclasses import dataclass
from enum import Enum
from typing import Any, Callable, Dict, Optional, Tuple, Type

from draken.draken_native import DrakenType
from draken.draken_native import LogicalKind
from draken.draken_native import LogicalType
from draken.draken_native import TimestampUnit

__all__ = [
    "LogicalCategory",
    "ColumnType",
    "find_compatible_type",
    "PYTHON_TO_SQL_MAP",
    "SQL_TO_PYTHON_MAP",
    "_CATEGORY_TO_CANONICAL",
    # canonical instances
    "INT8", "INT16", "INT32", "INT64",
    "UINT8", "UINT16", "UINT32", "UINT64",
    "FLOAT32", "FLOAT64",
    "BOOLEAN", "DATE", "INTERVAL",
    "VARCHAR", "NVARCHAR", "VARBINARY", "VARIANT",
    "NULL",
    # constructors
    "DECIMAL", "TIMESTAMP", "TIME", "VECTOR",
    # type-classification sets (import instead of calling .is_numeric() etc.)
    "_NUMERIC_TYPES", "_TEMPORAL_TYPES", "_COMPLEX_TYPES",
    "_LARGE_OBJECT_TYPES", "_STRING_TYPES",
]


class LogicalCategory(Enum):
    """The Opteryx SQL type vocabulary AND the operator-dispatch key (Decision B).

    Pure projection enum — reachable only via `ColumnType.category`. 15 canonical
    members; no aliases, no behaviours. Integer/float widths collapse to INTEGER/FLOAT
    (the actual physical width lives on `ColumnType.physical`).

    Unknown/unresolved types are represented by Python `None`, not by a sentinel
    enum member. Check `x is None` rather than comparing against a sentinel.
    """

    NULL = "NULL"
    BOOLEAN = "BOOLEAN"
    INTEGER = "INTEGER"
    FLOAT = "FLOAT"
    DECIMAL = "DECIMAL"
    DATE = "DATE"
    TIME = "TIME"
    TIMESTAMP = "TIMESTAMP"
    INTERVAL = "INTERVAL"
    VARCHAR = "VARCHAR"
    NVARCHAR = "NVARCHAR"
    VARBINARY = "VARBINARY"
    VARIANT = "VARIANT"
    ARRAY = "ARRAY"
    VECTOR = "VECTOR"

# Physical type -> dispatch category. Integer/float widths collapse here.
_CATEGORY_OF: dict = {
    DrakenType.INT8: LogicalCategory.INTEGER,
    DrakenType.INT16: LogicalCategory.INTEGER,
    DrakenType.INT32: LogicalCategory.INTEGER,
    DrakenType.INT64: LogicalCategory.INTEGER,
    DrakenType.UINT8: LogicalCategory.INTEGER,
    DrakenType.UINT16: LogicalCategory.INTEGER,
    DrakenType.UINT32: LogicalCategory.INTEGER,
    DrakenType.UINT64: LogicalCategory.INTEGER,
    DrakenType.DECIMAL: LogicalCategory.DECIMAL,
    DrakenType.DECIMAL128: LogicalCategory.DECIMAL,
    DrakenType.FLOAT32: LogicalCategory.FLOAT,
    DrakenType.FLOAT64: LogicalCategory.FLOAT,
    DrakenType.DATE32: LogicalCategory.DATE,
    DrakenType.TIMESTAMP64: LogicalCategory.TIMESTAMP,
    DrakenType.TIME32: LogicalCategory.TIME,
    DrakenType.TIME64: LogicalCategory.TIME,
    DrakenType.INTERVAL: LogicalCategory.INTERVAL,
    DrakenType.BOOL: LogicalCategory.BOOLEAN,
    DrakenType.VARCHAR: LogicalCategory.VARCHAR,
    DrakenType.NVARCHAR: LogicalCategory.NVARCHAR,
    DrakenType.VARBINARY: LogicalCategory.VARBINARY,
    DrakenType.VARIANT: LogicalCategory.VARIANT,
    DrakenType.ARRAY: LogicalCategory.ARRAY,
    DrakenType.VECTOR_FP16: LogicalCategory.VECTOR,
    DrakenType.NULL: LogicalCategory.NULL,
}

# Physical types that REQUIRE a LogicalType descriptor (LogicalKind != NONE).
# Mirrors logical_type.h: DECIMAL, TIMESTAMP, TIME, VECTOR carry params; everything
# else (including DATE32 and INTERVAL) does not.
_PARAMETERIZED_PHYSICAL = frozenset(
    {
        DrakenType.DECIMAL,
        DrakenType.DECIMAL128,  # int128-backed; same (precision, scale) descriptor
        DrakenType.TIMESTAMP64,
        DrakenType.TIME32,
        DrakenType.TIME64,
        DrakenType.VECTOR_FP16,
    }
)

# Physical type -> SQL display name for the unparameterized cases.
_NAME_OF: dict = {
    DrakenType.INT8: "INT8",
    DrakenType.INT16: "INT16",
    DrakenType.INT32: "INT32",
    DrakenType.INT64: "INT64",
    DrakenType.UINT8: "UINT8",
    DrakenType.UINT16: "UINT16",
    DrakenType.UINT32: "UINT32",
    DrakenType.UINT64: "UINT64",
    DrakenType.FLOAT32: "FLOAT32",
    DrakenType.FLOAT64: "FLOAT64",
    DrakenType.BOOL: "BOOLEAN",
    DrakenType.DATE32: "DATE",
    DrakenType.INTERVAL: "INTERVAL",
    DrakenType.VARCHAR: "VARCHAR",
    DrakenType.NVARCHAR: "NVARCHAR",
    DrakenType.VARBINARY: "VARBINARY",
    DrakenType.VARIANT: "VARIANT",
    DrakenType.NULL: "NULL",
}


@dataclass(frozen=True)
class ColumnType:
    """An Opteryx column/value type: a physical tag + optional logical descriptor (D1).

    `logical` is a Draken `LogicalType` for parameterized physical types
    (DECIMAL, TIMESTAMP, TIME, VECTOR_FP16); `None` otherwise.

    `element` is a child `ColumnType` for ARRAY (the array's element type);
    `None` otherwise. ARRAY isn't carried in Draken's `LogicalType` — the
    array child is held structurally in the vector itself, but at plan time
    we need to know `ARRAY<element>` for type-checking, and `element` carries it.

    Frozen + hashable (all fields are hashable), so it is usable directly as a
    schema column type and in dict/set membership.
    """

    physical: DrakenType
    logical: Optional[LogicalType] = None
    element: Optional["ColumnType"] = None

    def __post_init__(self) -> None:
        # DECIMAL/TIMESTAMP/TIME/VECTOR require a LogicalType descriptor; element None.
        needs_logical = self.physical in _PARAMETERIZED_PHYSICAL
        if needs_logical:
            if self.logical is None:
                raise ValueError(
                    f"{self.physical!r} is a parameterized physical type and requires a "
                    f"LogicalType descriptor"
                )
            if self.element is not None:
                raise ValueError(
                    f"{self.physical!r} must not carry an `element` (that is ARRAY-only)"
                )
            return
        # ARRAY requires an element ColumnType; logical None.
        if self.physical == DrakenType.ARRAY:
            if self.element is None:
                raise ValueError(
                    "ARRAY physical type requires an `element` ColumnType descriptor"
                )
            if self.logical is not None:
                raise ValueError(
                    "ARRAY must not carry a LogicalType (the array child lives in `element`)"
                )
            return
        # Unparameterized physical types: both descriptors must be None.
        if self.logical is not None:
            raise ValueError(
                f"{self.physical!r} is unparameterized and must not carry a LogicalType "
                f"descriptor"
            )
        if self.element is not None:
            raise ValueError(
                f"{self.physical!r} is unparameterized and must not carry an `element`"
            )

    @property
    def category(self) -> LogicalCategory:
        """Operator-dispatch category (Decision B)."""
        try:
            return _CATEGORY_OF[self.physical]
        except KeyError:
            raise NotImplementedError(
                f"no dispatch category for physical type {self.physical!r} "
                f"(unsupported)"
            )

    def __str__(self) -> str:
        if self.physical == DrakenType.DECIMAL or self.physical == DrakenType.DECIMAL128:
            return f"DECIMAL({self.logical.precision}, {self.logical.scale})"
        if self.physical == DrakenType.VECTOR_FP16:
            return f"VECTOR({self.logical.dimension})"
        if self.physical == DrakenType.TIMESTAMP64:
            return "TIMESTAMP"
        if self.physical in (DrakenType.TIME32, DrakenType.TIME64):
            return "TIME"
        if self.physical == DrakenType.ARRAY:
            return f"ARRAY<{self.element}>"
        name = _NAME_OF.get(self.physical)
        if name is None:
            raise NotImplementedError(f"no display name for {self.physical!r}")
        return name


# ---------------------------------------------------------------------------
# Canonical instances (unparameterized — logical is None)
# ---------------------------------------------------------------------------
INT8 = ColumnType(DrakenType.INT8)
INT16 = ColumnType(DrakenType.INT16)
INT32 = ColumnType(DrakenType.INT32)
INT64 = ColumnType(DrakenType.INT64)
UINT8 = ColumnType(DrakenType.UINT8)
UINT16 = ColumnType(DrakenType.UINT16)
UINT32 = ColumnType(DrakenType.UINT32)
UINT64 = ColumnType(DrakenType.UINT64)
FLOAT32 = ColumnType(DrakenType.FLOAT32)
FLOAT64 = ColumnType(DrakenType.FLOAT64)
BOOLEAN = ColumnType(DrakenType.BOOL)
DATE = ColumnType(DrakenType.DATE32)
INTERVAL = ColumnType(DrakenType.INTERVAL)
VARCHAR = ColumnType(DrakenType.VARCHAR)
NVARCHAR = ColumnType(DrakenType.NVARCHAR)
VARBINARY = ColumnType(DrakenType.VARBINARY)
VARIANT = ColumnType(DrakenType.VARIANT)
NULL = ColumnType(DrakenType.NULL)


# ---------------------------------------------------------------------------
# Constructors (parameterized — build a Draken LogicalType descriptor)
# ---------------------------------------------------------------------------
def DECIMAL(precision: int, scale: int) -> ColumnType:
    """Build a DECIMAL ColumnType. p ≤ 18 → int64-backed; 19 ≤ p ≤ 38 → int128-backed
    (DECIMAL128 physical tier). p > 38 raises (genuine overflow — no wider tier)."""
    if not (1 <= precision <= 38):
        raise ValueError(
            f"DECIMAL precision must be 1..38; got {precision}"
        )
    if not (0 <= scale <= precision):
        raise ValueError(f"DECIMAL scale must be 0..{precision}; got {scale}")
    physical = DrakenType.DECIMAL128 if precision > 18 else DrakenType.DECIMAL
    return ColumnType(
        physical,
        LogicalType(kind=LogicalKind.DECIMAL, precision=precision, scale=scale),
    )


def TIMESTAMP(
    unit: TimestampUnit = TimestampUnit.MICROSECONDS, offset_minutes: int = 0
) -> ColumnType:
    return ColumnType(
        DrakenType.TIMESTAMP64,
        LogicalType(
            kind=LogicalKind.TIMESTAMP, unit=unit, offset_minutes=offset_minutes
        ),
    )


def TIME(unit: TimestampUnit = TimestampUnit.MICROSECONDS) -> ColumnType:
    # TIME32 holds second/millisecond resolution; TIME64 holds micro/nanosecond.
    physical = (
        DrakenType.TIME32
        if unit in (TimestampUnit.SECONDS, TimestampUnit.MILLISECONDS)
        else DrakenType.TIME64
    )
    return ColumnType(physical, LogicalType(kind=LogicalKind.TIME, unit=unit))


def VECTOR(dimensions: int, base_type: ColumnType = FLOAT32) -> ColumnType:
    if not isinstance(dimensions, int) or dimensions <= 0:
        raise ValueError(f"VECTOR dimensions must be a positive integer; got {dimensions}")
    # Draken ships only DRAKEN_VECTOR_FP16 today. FP32-default (MSSQL convention) and
    # other base types need a Draken extension (Phase 2) — do not fake it here.
    if base_type != FLOAT32:
        raise NotImplementedError(
            "VECTOR base type other than FLOAT32-placeholder is not yet supported; "
            "Draken has VECTOR_FP16 only (base-type selection is a Phase-2 extension)"
        )
    return ColumnType(
        DrakenType.VECTOR_FP16, LogicalType(kind=LogicalKind.VECTOR, dimension=dimensions)
    )


def ARRAY(element_type: ColumnType) -> ColumnType:
    """Build an ARRAY ColumnType with the given element ColumnType.

    Draken's `LogicalType` has no element/child field — the array child is
    carried structurally in the vector at execution time. At plan time we
    carry it via `ColumnType.element` so the binder can type-check
    `ARRAY<element>` expressions (UNNEST result, ARRAY indexing, etc.).
    """
    if not isinstance(element_type, ColumnType):
        raise TypeError(
            f"ARRAY element_type must be a ColumnType; got {type(element_type).__name__}"
        )
    return ColumnType(DrakenType.ARRAY, logical=None, element=element_type)


# ---------------------------------------------------------------------------
# Canonical (de)serialization — the authoritative wire/persistence form of a
# ColumnType. Used by SchemaColumn.to_dict()/from_dict() (D-4 Phase 2 "full break":
# the schema JSON now carries a single `column_type` string instead of the
# legacy type/precision/scale/element_type quartet).
#
# The string IS `str(ColumnType)` — DECIMAL(p, s) / ARRAY<elem> / VECTOR(n) /
# TIMESTAMP / TIME / the _NAME_OF plain names. `parse_column_type` is the exact
# inverse; it is keyed on those display names directly (NOT on LogicalCategory.from_name,
# which lacks VARBINARY and would lose the physical tier). DECIMAL(p, s) with
# p>18 round-trips to the DECIMAL128 tier because `DECIMAL()` chooses the tier.
# ---------------------------------------------------------------------------

# Inverse of _NAME_OF (display name -> physical DrakenType) for the
# unparameterized types. TIMESTAMP/TIME/DECIMAL/VECTOR/ARRAY are handled by
# dedicated parse branches (they carry parameters).
_NAME_TO_PHYSICAL: dict = {name: phys for phys, name in _NAME_OF.items()}

# SQL-spelling aliases -> canonical ColumnType. The single place SQL type names
# (incl. legacy/alias spellings) resolve to a type — this is what `from_name` did.
_SQL_NAME_ALIASES: dict = {
    "INTEGER": INT64, "INT": INT64, "BIGINT": INT64,
    "DOUBLE": FLOAT64, "FLOAT": FLOAT64,
    "STRING": VARCHAR, "TEXT": VARCHAR,
    "BOOL": BOOLEAN,
    "BYTES": VARBINARY, "BLOB": VARBINARY,
    "STRUCT": NVARCHAR, "JSONB": NVARCHAR,
}


def serialize_column_type(ct) -> Optional[str]:
    """Canonical string for a ColumnType (None -> None)."""
    if ct is None:
        return None
    return str(ct)


def parse_column_type(s: Optional[str]) -> Optional[ColumnType]:
    """Inverse of `serialize_column_type` / `str(ColumnType)`.

    Raises ValueError on an unrecognized form (fail-loud — a malformed persisted
    type is a bug, not something to silently coerce).
    """
    if s is None:
        return None
    s = s.strip()
    upper = s.upper()

    # ARRAY<element>
    if upper.startswith("ARRAY<") and s.endswith(">"):
        inner = s[s.index("<") + 1 : -1]
        return ARRAY(parse_column_type(inner))

    # parameterized: NAME(params)
    if "(" in s:
        base = s[: s.index("(")].strip().upper()
        params = s[s.index("(") + 1 : s.rindex(")")]
        parts = [p.strip() for p in params.split(",")]
        if base == "DECIMAL":
            return DECIMAL(int(parts[0]), int(parts[1]))
        if base == "VECTOR":
            return VECTOR(int(parts[0]))
        raise ValueError(f"parse_column_type: unknown parameterized type {s!r}")

    if upper == "TIMESTAMP":
        return TIMESTAMP()
    if upper == "TIME":
        return TIME()

    alias = _SQL_NAME_ALIASES.get(upper)
    if alias is not None:
        return alias

    phys = _NAME_TO_PHYSICAL.get(upper)
    if phys is not None:
        return ColumnType(phys)
    raise ValueError(f"parse_column_type: unknown type {s!r}")



# ---------------------------------------------------------------------------
# SQL-type behaviours (absorbed from the former flat SQL type enum).
# These back the LogicalCategory methods above and the find_compatible_type /
# python-map module API. There is no separate SQL type enum any more.
# ---------------------------------------------------------------------------

_TYPE_TO_PYTHON: Dict[LogicalCategory, Type] = {
    LogicalCategory.NULL: type(None),
    LogicalCategory.BOOLEAN: bool,
    LogicalCategory.INTEGER: int,
    LogicalCategory.FLOAT: float,
    LogicalCategory.VARCHAR: str,
    LogicalCategory.NVARCHAR: str,
    LogicalCategory.VARBINARY: bytes,
    LogicalCategory.DATE: datetime.date,
    LogicalCategory.TIME: datetime.time,
    LogicalCategory.TIMESTAMP: datetime.datetime,
    LogicalCategory.INTERVAL: datetime.timedelta,
    LogicalCategory.DECIMAL: decimal.Decimal,
    LogicalCategory.ARRAY: list,
    LogicalCategory.VECTOR: list,
    LogicalCategory.VARIANT: str,
}

PYTHON_TO_SQL_MAP: Dict[Type, LogicalCategory] = {
    type(None): LogicalCategory.NULL,
    bool: LogicalCategory.BOOLEAN,
    int: LogicalCategory.INTEGER,
    float: LogicalCategory.FLOAT,
    str: LogicalCategory.VARCHAR,
    bytes: LogicalCategory.VARBINARY,
    bytearray: LogicalCategory.VARBINARY,
    memoryview: LogicalCategory.VARBINARY,
    datetime.date: LogicalCategory.DATE,
    datetime.time: LogicalCategory.TIME,
    datetime.datetime: LogicalCategory.TIMESTAMP,
    datetime.timedelta: LogicalCategory.INTERVAL,
    decimal.Decimal: LogicalCategory.DECIMAL,
    list: LogicalCategory.ARRAY,
    tuple: LogicalCategory.ARRAY,
    dict: LogicalCategory.NVARCHAR,
    set: LogicalCategory.ARRAY,
}
SQL_TO_PYTHON_MAP: Dict[LogicalCategory, Type] = dict(_TYPE_TO_PYTHON)

_NUMERIC_TYPES = {LogicalCategory.INTEGER, LogicalCategory.FLOAT, LogicalCategory.DECIMAL}
_TEMPORAL_TYPES = {LogicalCategory.DATE, LogicalCategory.TIME, LogicalCategory.TIMESTAMP, LogicalCategory.INTERVAL}
_COMPLEX_TYPES = {LogicalCategory.ARRAY, LogicalCategory.VECTOR, LogicalCategory.NVARCHAR}
_STRING_TYPES = {LogicalCategory.VARCHAR, LogicalCategory.NVARCHAR, LogicalCategory.VARBINARY}
_LARGE_OBJECT_TYPES = {LogicalCategory.VARBINARY, LogicalCategory.ARRAY, LogicalCategory.NVARCHAR, LogicalCategory.VECTOR}


def find_compatible_type(types: list) -> Optional["ColumnType"]:
    """Find a ColumnType that can represent all types in the list (promotion rules).

    Accepts: list of ColumnType | LogicalCategory | None values.
    Returns: ColumnType | None (None when types is empty or all-null/unknown).
    """
    # Normalize each item to a LogicalCategory for dispatch logic.
    def _to_lc(t):
        if t is None:
            return None
        if isinstance(t, LogicalCategory):
            return t
        try:
            return t.category  # ColumnType
        except Exception:
            return None

    lc_types = [_to_lc(t) for t in types]
    non_null = [t for t in lc_types if t is not None and t != LogicalCategory.NULL]

    if not non_null:
        return None

    if len(set(non_null)) == 1:
        result_lc = non_null[0]
    elif all(t in _NUMERIC_TYPES or t == LogicalCategory.BOOLEAN for t in non_null):
        if LogicalCategory.DECIMAL in non_null:
            result_lc = LogicalCategory.DECIMAL
        elif LogicalCategory.FLOAT in non_null:
            result_lc = LogicalCategory.FLOAT
        elif LogicalCategory.INTEGER in non_null:
            result_lc = LogicalCategory.INTEGER
        else:
            result_lc = LogicalCategory.BOOLEAN
    elif any(t in _TEMPORAL_TYPES for t in non_null):
        result_lc = LogicalCategory.VARCHAR
    elif any(t in _COMPLEX_TYPES for t in non_null):
        result_lc = LogicalCategory.NVARCHAR
    else:
        result_lc = LogicalCategory.VARCHAR

    # Convert LogicalCategory result to canonical ColumnType.
    _LC_TO_CT = {
        LogicalCategory.BOOLEAN: BOOLEAN,
        LogicalCategory.INTEGER: INT64,
        LogicalCategory.FLOAT: FLOAT64,
        LogicalCategory.DECIMAL: None,  # handled below
        LogicalCategory.DATE: DATE,
        LogicalCategory.INTERVAL: INTERVAL,
        LogicalCategory.VARCHAR: VARCHAR,
        LogicalCategory.NVARCHAR: NVARCHAR,
        LogicalCategory.VARBINARY: VARBINARY,
        LogicalCategory.VARIANT: VARIANT,
        LogicalCategory.NULL: NULL,
    }
    ct = _LC_TO_CT.get(result_lc)
    if ct is not None:
        return ct
    if result_lc == LogicalCategory.DECIMAL:
        # Try to extract actual p/s from ColumnType inputs; fall back to wide default.
        from opteryx.types.type_unification import compute_result_logical_type
        ct_inputs = [t for t in types if isinstance(t, ColumnType) and t.category in (LogicalCategory.DECIMAL, LogicalCategory.INTEGER)]
        if len(ct_inputs) >= 2:
            try:
                return compute_result_logical_type(ct_inputs[0], ct_inputs[1], "Plus", LogicalCategory.DECIMAL)
            except Exception:
                pass
        return DECIMAL(38, 18)
    if result_lc == LogicalCategory.TIMESTAMP:
        return TIMESTAMP()
    if result_lc == LogicalCategory.TIME:
        return TIME()
    return None


# Flat lookup: non-parameterized LogicalCategory → canonical ColumnType instance.
# Parameterized types (DECIMAL, ARRAY, VECTOR, TIMESTAMP, TIME) need explicit
# construction — callers that need those use DECIMAL(p,s), ARRAY(elem), etc. directly.
# Alias members (DOUBLE, BLOB, STRUCT, JSONB) removed in Phase 4; no entries needed.
_CATEGORY_TO_CANONICAL = {
    LogicalCategory.BOOLEAN: BOOLEAN,
    LogicalCategory.INTEGER: INT64,
    LogicalCategory.FLOAT: FLOAT64,
    LogicalCategory.VARCHAR: VARCHAR,
    LogicalCategory.NVARCHAR: NVARCHAR,
    LogicalCategory.VARBINARY: VARBINARY,
    LogicalCategory.DATE: DATE,
    LogicalCategory.TIMESTAMP: TIMESTAMP(),
    LogicalCategory.TIME: TIME(),
    LogicalCategory.INTERVAL: INTERVAL,
    LogicalCategory.VARIANT: VARIANT,
    LogicalCategory.NULL: NULL,
}


