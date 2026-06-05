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

from dataclasses import dataclass
from enum import IntEnum
from typing import Optional

from draken.draken_native import DrakenType
from draken.draken_native import LogicalKind
from draken.draken_native import LogicalType
from draken.draken_native import TimestampUnit

__all__ = [
    "LogicalCategory",
    "ColumnType",
    # canonical instances
    "INT8", "INT16", "INT32", "INT64",
    "FLOAT32", "FLOAT64",
    "BOOLEAN", "DATE", "INTERVAL",
    "VARCHAR", "NVARCHAR", "VARBINARY", "VARIANT",
    "NULL",
    # constructors
    "DECIMAL", "TIMESTAMP", "TIME", "VECTOR",
]


class LogicalCategory(IntEnum):
    """Operator-dispatch key (Decision B). A coarsening of the column type used by the
    binder's operator map and result-type derivation — integer widths collapse to
    INTEGER, float widths to FLOAT. Derived from the physical type via `_CATEGORY_OF`.

    NOT a type system: a projection of the (physical, logical) pair for binder-time
    dispatch. Kernel-time dispatch uses the physical `DrakenType` directly.
    """

    NULL = 0
    BOOLEAN = 1
    INTEGER = 2
    FLOAT = 3
    DECIMAL = 4
    DATE = 5
    TIME = 6
    TIMESTAMP = 7
    INTERVAL = 8
    VARCHAR = 9
    NVARCHAR = 10
    VARBINARY = 11
    VARIANT = 12     # first-class JSON value (DRAKEN_VARIANT), navigable via -> / ->>
    ARRAY = 13
    VECTOR = 14
    # No JSONB / STRUCT categories:
    #   JSONB  is an alias for NVARCHAR today (intentions to do more later; some legacy
    #          spots still treat it as VARBINARY/BLOB — clean up on contact).
    #   STRUCT is not supported — the engine stringifies structs to JSON text (NVARCHAR).


# Physical type -> dispatch category. Integer/float widths collapse here.
_CATEGORY_OF: dict = {
    DrakenType.INT8: LogicalCategory.INTEGER,
    DrakenType.INT16: LogicalCategory.INTEGER,
    DrakenType.INT32: LogicalCategory.INTEGER,
    DrakenType.INT64: LogicalCategory.INTEGER,
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

    `logical` is `None` for unparameterized physical types and a Draken `LogicalType`
    for parameterized ones. Frozen + hashable (both fields are hashable), so it is
    usable directly as a schema column type and in dict/set membership.
    """

    physical: DrakenType
    logical: Optional[LogicalType] = None

    def __post_init__(self) -> None:
        needs_logical = self.physical in _PARAMETERIZED_PHYSICAL
        if needs_logical and self.logical is None:
            raise ValueError(
                f"{self.physical!r} is a parameterized physical type and requires a "
                f"LogicalType descriptor"
            )
        if not needs_logical and self.logical is not None:
            raise ValueError(
                f"{self.physical!r} is unparameterized and must not carry a LogicalType "
                f"descriptor"
            )

    @property
    def category(self) -> LogicalCategory:
        """Operator-dispatch category (Decision B)."""
        try:
            return _CATEGORY_OF[self.physical]
        except KeyError:
            raise NotImplementedError(
                f"no dispatch category for physical type {self.physical!r} "
                f"(NON_NATIVE / unsupported)"
            )

    def __str__(self) -> str:
        if self.physical == DrakenType.DECIMAL:
            return f"DECIMAL({self.logical.precision}, {self.logical.scale})"
        if self.physical == DrakenType.VECTOR_FP16:
            return f"VECTOR({self.logical.dimension})"
        if self.physical == DrakenType.TIMESTAMP64:
            return "TIMESTAMP"
        if self.physical in (DrakenType.TIME32, DrakenType.TIME64):
            return "TIME"
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
    # Draken's LogicalType has no element/child field; the array child is carried
    # structurally on the vector (DrakenArrayBuffer), not in the logical descriptor.
    # Representing ARRAY<element> at plan time is an open Phase-2 item — do not guess.
    raise NotImplementedError(
        "ARRAY<element> plan-time representation is not yet decided (Draken carries the "
        "array child structurally; no logical element field). Deferred to Phase 2."
    )
