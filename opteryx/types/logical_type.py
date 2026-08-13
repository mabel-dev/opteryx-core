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
# Draken owns the physical+descriptor -> SQL name mapping; this is the one
# entry point onto it. Never reimplement the table here (see __str__).
from draken.vectors.vector import type_display_name as _draken_type_display_name
from draken.draken_native import LogicalType
from draken.draken_native import TimestampUnit

__all__ = [
    "LogicalCategory",
    "ColumnType",
    "column_type_from_vector",
    "morsel_column_types",
    "find_compatible_type",
    "is_legal_widen",
    "PYTHON_TO_SQL_MAP",
    "SQL_TO_PYTHON_MAP",
    "_CATEGORY_TO_CANONICAL",
    # canonical instances
    "INT8", "INT16", "INT32", "INT64",
    "UINT8", "UINT16", "UINT32", "UINT64",
    "FLOAT32", "FLOAT64",
    "BOOLEAN", "DATE", "INTERVAL",
    "VARCHAR", "NVARCHAR", "VARBINARY", "VARIANT",
    "IPV4",
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

# Physical types that PERMIT a LogicalType descriptor without requiring one.
#
# Until IPv4 the rule was a biconditional: a descriptor was present if and only
# if the physical type was parameterized. IPv4 breaks that in one direction and
# one direction only — it REFINES an otherwise-complete physical type rather
# than completing an incomplete one. A UINT32 with no descriptor is a valid
# unsigned integer column; the same UINT32 carrying LogicalKind.IPV4 is the
# same 32 bits with a narrower meaning (see draken/logical_type.h).
#
# The two sets must stay disjoint: a physical type is either incomplete without
# a descriptor (_PARAMETERIZED_PHYSICAL, absence is an error) or complete
# without one (_REFINABLE_PHYSICAL, absence is just the unrefined type). A type
# in both would have no defined meaning for a missing descriptor.
#
# Kept deliberately tight. This is not an invitation to hang arbitrary logical
# meanings off physical types — each entry needs the architect's agreement, and
# each one costs a second dispatch axis at the render and cast edges.
_REFINABLE_PHYSICAL: dict = {
    DrakenType.UINT32: frozenset({LogicalKind.IPV4}),
}

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
    # BOOL, not BOOLEAN — the canonical name matches the physical tag, the way
    # INT64 and FLOAT64 do. BOOLEAN is an implied alias the dialect still accepts
    # in CREATE and CAST, and `_SQL_NAME_ALIASES` keeps it readable so schemas
    # stored before this (when `str(ColumnType)` was the source of the stored
    # name) still parse. Backfill of those lives in the catalog repo.
    DrakenType.BOOL: "BOOL",
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
        # Refinable physical types (UINT32/IPv4): a descriptor is optional, but
        # when present it must be one this physical type actually permits — a
        # UINT32 carrying LogicalKind.DECIMAL is nonsense, and accepting it here
        # would surface as a wrong rendering much further downstream.
        permitted = _REFINABLE_PHYSICAL.get(self.physical)
        if permitted is not None and self.logical is not None:
            if self.logical.kind not in permitted:
                raise ValueError(
                    f"{self.physical!r} permits only "
                    f"{sorted(k.name for k in permitted)} as a LogicalType kind; "
                    f"got {self.logical.kind!r}"
                )
            if self.element is not None:
                raise ValueError(
                    f"{self.physical!r} must not carry an `element` (that is ARRAY-only)"
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

    def ordinalize(self, value) -> int:
        """Scalar ordinal key for `value`, in the same int64 space
        `Vector.ordinalize()` produces for a column of this physical type
        (see draken/ops/ordinalize.h). Lets plan-time code — file pruning
        against ordinalize()-encoded manifest min/max bounds — compare a
        predicate literal against those bounds without materialising a
        Vector.

        Mostly a passthrough to `DrakenType.ordinalize`, with two cases that
        physical-only entry point deliberately refuses because it cannot see
        the `LogicalType` descriptor this class carries:

        DATE32/TIMESTAMP64/TIME32/TIME64 — the physical entry point wants a
        `datetime.date`/`datetime`/`time` OBJECT (and refuses TIMESTAMP/TIME
        outright, since their unit lives on `LogicalType` and cannot be
        guessed from the physical tag). That is not the situation here: by
        the time a literal reaches file pruning the binder has already
        normalised it to the column's own raw physical integer — a DATE
        literal binds to `-7305`, days since epoch, NOT a `datetime.date`;
        a TIMESTAMP literal binds to raw micros. For all four types
        `ordinalize` is an identity widen from INT32/INT64, so that
        already-raw integer IS the ordinal key and no conversion is wanted.
        Passing it to the physical entry point would raise, and pruning would
        silently stop happening on exactly the columns most often filtered
        (dates and timestamps on log tables). A non-integer reaching here
        means the bind-time normalisation assumption no longer holds, so it
        raises rather than guessing a unit — the caller then skips pruning,
        which costs speed, never correctness.

        DECIMAL/DECIMAL128 — raises. A stored DECIMAL bound is the unscaled
        mantissa at the COLUMN's scale, while `DrakenType.DECIMAL.ordinalize`
        returns the mantissa at the LITERAL's own natural scale
        (`Decimal("1.5")` -> 15, never 15000 for a scale-4 column), so the
        two are only comparable when the scales happen to coincide. Aligning
        them needs rescaling semantics (rounding direction on truncation)
        that are not pinned down anywhere, so this refuses instead of
        inventing them. Pruning is skipped for DECIMAL, exactly as it is
        today — `_comparable_literal` already declines a `Decimal` literal
        against an integer bound.
        """
        physical = self.physical

        if physical in (
            DrakenType.DATE32,
            DrakenType.TIMESTAMP64,
            DrakenType.TIME32,
            DrakenType.TIME64,
        ):
            if isinstance(value, int) and not isinstance(value, bool):
                return value
            raise ValueError(
                f"ordinalize: {physical!r} expects a bind-normalised integer literal "
                f"(the raw physical value at the column's unit); got "
                f"{type(value).__name__}"
            )

        if physical in (DrakenType.DECIMAL, DrakenType.DECIMAL128):
            raise ValueError(
                f"ordinalize: {physical!r} is not supported — a stored DECIMAL bound is "
                "the mantissa at the column's scale, which cannot be compared against a "
                "literal's own-scale mantissa without rescaling"
            )

        return physical.ordinalize(value)

    def __str__(self) -> str:
        """The SQL type name — DELEGATED to draken, which owns that mapping.

        Draken is the single source (architect's ruling, 2026-08-08): the
        descriptor is what decides the name, and draken owns LogicalType. Keeping
        a second table here is how one surface renders a column `UINT32` while
        another renders the same column `IPV4` — which is exactly the defect this
        replaced, in draken's own Morsel renderer.

        This string is PERSISTED into stored schemas, so it is a format, not a
        display choice: a TIMESTAMP stored at ms and read back as the us default
        reads every value 1000x off, silently. Delegation was gated on a parity
        check over every constructible type — see
        tests/unit/types/test_type_name_parity.py, which also pins `_NAME_OF`
        (still the source of the PARSE direction, `_NAME_TO_PHYSICAL`) against
        draken, so the two directions cannot drift apart.

        ARRAY stays here: its element is a nested ColumnType, which draken has no
        concept of, so draken names the tag and this composes the rest.
        """
        if self.physical == DrakenType.ARRAY:
            return f"ARRAY<{self.element}>"
        logical = self.logical
        name = _draken_type_display_name(
            self.physical,
            kind=(logical.kind if logical is not None else None),
            unit=(_UNIT_TO_SQL.get(logical.unit) if logical is not None else None),
            precision=(logical.precision if logical is not None else 0),
            scale=(logical.scale if logical is not None else 0),
            dimension=(logical.dimension if logical is not None else 0),
        )
        if not name:
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

# IPv4 — UINT32 physical refined by LogicalKind.IPV4. Unparameterized (the
# prefix length is never carried on the value; it is always an operand of the
# operation that needs it), so this is a canonical instance, not a constructor.
#
# `.category` is deliberately INTEGER: ordering, grouping, joins, hashing and
# comparison all operate on the underlying uint32, which is exactly correct for
# IPv4 — dotted-decimal order and unsigned integer order are the same order.
# Only rendering and casting read the descriptor.
IPV4 = ColumnType(DrakenType.UINT32, LogicalType(kind=LogicalKind.IPV4))


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
#
# READ-SIDE ONLY. This table is reached through `parse_column_type`, which resolves
# PERSISTED and external schema type names. It is NOT the CAST surface: cast targets
# go through `_extract_data_type`'s own `type_mappings` in
# planner/logical_planner/logical_planner_builders.py, which never consults this
# table (it rejects even `INT8`/`FLOAT32`). Adding a spelling here therefore widens
# what a stored schema may say, not what the dialect accepts.
#
# The width-bearing spellings exist so a catalog storing exact widths can use the
# natural SQL name and still land on the exact type. Without them the name does not
# parse at all and the reader falls back to its VARCHAR default — a narrow int column
# silently becoming a STRING, which is worse than the INT64 widening they replace.
_SQL_NAME_ALIASES: dict = {
    "INTEGER": INT64, "INT": INT64, "BIGINT": INT64,
    "TINYINT": INT8, "SMALLINT": INT16,
    # REAL is single-precision per the SQL standard. FLOAT's mapping to FLOAT64 is
    # pre-existing and deliberately NOT changed to match — several engines treat
    # bare FLOAT as double, and re-pointing it would narrow every stored `FLOAT`
    # column (which is what this catalog actually persists for the FLOAT category).
    "DOUBLE": FLOAT64, "FLOAT": FLOAT64, "REAL": FLOAT32,
    "STRING": VARCHAR, "TEXT": VARCHAR,
    # BOOLEAN, not BOOL — BOOL is the CANONICAL name now and resolves through
    # `_NAME_TO_PHYSICAL`, so the alias that still has to be readable is the older
    # spelling, which is what every schema stored before the rename says.
    "BOOLEAN": BOOLEAN,
    "BYTES": VARBINARY, "BLOB": VARBINARY,
    "STRUCT": NVARCHAR, "JSONB": NVARCHAR,
}


# The canonical spelling of a TimestampUnit, both directions. It matches the SQL
# surface (`TIMESTAMP[ms]`), so a serialized type string is also a valid declared
# type — the property DECIMAL(p, s), ARRAY<T> and VECTOR(n) already have.
_UNIT_TO_SQL = {
    TimestampUnit.SECONDS: "s",
    TimestampUnit.MILLISECONDS: "ms",
    TimestampUnit.MICROSECONDS: "us",
    TimestampUnit.NANOSECONDS: "ns",
}
_SQL_TO_UNIT = {v: k for k, v in _UNIT_TO_SQL.items()}


def serialize_column_type(ct) -> Optional[str]:
    """Canonical string for a ColumnType (None -> None)."""
    if ct is None:
        return None
    return str(ct)


def try_parse_column_type(s: str) -> Optional[ColumnType]:
    """`parse_column_type`'s body, returning None instead of raising when `s`
    is not a recognized exact type string.

    Exists so a caller with a LEGITIMATE fallback can probe without wrapping
    `parse_column_type` in try/except (§9 — try/except is not flow control).
    The one such caller is the catalog schema reader: the catalog stores bare
    `DECIMAL` and `ARRAY` with their parameters in SEPARATE columns
    (precision/scale, element-type), so those two names correctly do not parse
    on their own and must fall through to a parameter-aware branch.

    "Not recognized" is the ONLY thing None means here. A malformed
    parameterized form (`DECIMAL(x, y)`) still raises from int() — that is a
    corrupt persisted type, not a fallback case.
    """
    s = s.strip()
    upper = s.upper()

    # ARRAY<element>
    if upper.startswith("ARRAY<") and s.endswith(">"):
        inner = try_parse_column_type(s[s.index("<") + 1 : -1])
        return None if inner is None else ARRAY(inner)

    # parameterized: NAME(params)
    if "(" in s:
        base = s[: s.index("(")].strip().upper()
        params = s[s.index("(") + 1 : s.rindex(")")]
        parts = [p.strip() for p in params.split(",")]
        if base == "DECIMAL":
            return DECIMAL(int(parts[0]), int(parts[1]))
        if base == "VECTOR":
            return VECTOR(int(parts[0]))
        return None

    # TIMESTAMP[unit] / TIME[unit] — the form `str(ColumnType)` writes and the
    # form SQL declares. The BARE names remain valid and mean the canonical
    # microseconds: that is what every schema persisted before the unit was
    # serialized says, and re-reading those must not change their meaning.
    if upper.startswith("TIMESTAMP[") and upper.endswith("]"):
        _u = _SQL_TO_UNIT.get(upper[len("TIMESTAMP[") : -1].strip().lower())
        return None if _u is None else TIMESTAMP(_u)
    if upper.startswith("TIME[") and upper.endswith("]"):
        _u = _SQL_TO_UNIT.get(upper[len("TIME[") : -1].strip().lower())
        return None if _u is None else TIME(_u)
    if upper == "TIMESTAMP":
        return TIMESTAMP()
    if upper == "TIME":
        return TIME()
    # Canonical, not an alias — IPV4 cannot go through _NAME_TO_PHYSICAL because
    # that maps UINT32 to the name "UINT32", which would drop the descriptor.
    if upper == "IPV4":
        return IPV4

    alias = _SQL_NAME_ALIASES.get(upper)
    if alias is not None:
        return alias

    phys = _NAME_TO_PHYSICAL.get(upper)
    if phys is not None:
        return ColumnType(phys)
    return None


def parse_column_type(s: Optional[str]) -> Optional[ColumnType]:
    """Inverse of `serialize_column_type` / `str(ColumnType)`.

    Raises ValueError on an unrecognized form (fail-loud — a malformed persisted
    type is a bug, not something to silently coerce). Callers that genuinely
    have somewhere else to look use `try_parse_column_type` instead.
    """
    if s is None:
        return None
    parsed = try_parse_column_type(s)
    if parsed is None:
        raise ValueError(f"parse_column_type: unknown type {s!r}")
    return parsed


# ---------------------------------------------------------------------------
# Reconstruction from a live column — the ONE place a runtime vector's
# (physical tag, logical descriptor) pair becomes a ColumnType.
#
# A vector's `type` is only HALF of its type. DrakenType.UINT32 is both a plain
# unsigned column and an IPv4 address column; DrakenType.TIMESTAMP64 is every
# unit at once; DrakenType.DECIMAL is every (precision, scale) at once. Anything
# that reports a result column's type from the tag alone therefore reports a
# type that is not the column's type — and the loss is unrecoverable once it has
# been serialized. That defect has now been fixed four times independently (the
# catalog schema reader, the CTAS write path, the text writers, and the result
# schema), so the reconstruction lives here ONCE and every reporter calls it.
#
# The pairing with `str(ColumnType)` / `parse_column_type` is the point: this
# produces a ColumnType, those two are an exact round-trip through a string, so
# a consumer that serializes a result schema can recover the real type.
# ---------------------------------------------------------------------------
def column_type_from_vector(vector) -> ColumnType:
    """The ColumnType a draken Vector actually carries — tag AND descriptor.

    Raises for a physical type with no ColumnType spelling, rather than
    substituting a plausible one: a wrong type reported as fact is worse than a
    loud failure, and the caller cannot tell a real VARCHAR from a defaulted one.
    """
    physical = vector.type
    nb = vector._nb  # E.24 — the sanctioned handle for descriptor introspection
    kind = nb.logical_type_kind

    if kind is not None and kind != LogicalKind.NONE:
        if kind == LogicalKind.IPV4:
            return IPV4
        if kind == LogicalKind.DECIMAL:
            precision = nb.logical_type_precision
            scale = nb.logical_type_scale
            if precision is None or scale is None:
                raise ValueError(
                    f"{physical!r} column carries a DECIMAL descriptor with no "
                    "precision/scale"
                )
            return DECIMAL(precision, scale)
        if kind == LogicalKind.VECTOR:
            dimension = nb.logical_type_dimension
            if dimension is None:
                raise ValueError(
                    f"{physical!r} column carries a VECTOR descriptor with no dimension"
                )
            return VECTOR(dimension)
        if kind == LogicalKind.TIMESTAMP or kind == LogicalKind.TIME:
            # The unit IS part of the type — a TIMESTAMP reported without one and
            # read back at the microsecond default is every value 1000x off when the
            # column was milliseconds. (`offset_minutes` is deliberately not carried:
            # `str(ColumnType)` has no spelling for it, so returning it here would
            # produce a type that cannot survive the round-trip this exists to serve.)
            unit = _SQL_TO_UNIT.get(nb.logical_type_unit)
            if unit is None:
                raise ValueError(
                    f"{physical!r} column carries a {kind!r} descriptor with an "
                    f"unrecognized unit {nb.logical_type_unit!r}"
                )
            return TIMESTAMP(unit) if kind == LogicalKind.TIMESTAMP else TIME(unit)
        raise NotImplementedError(f"no ColumnType for logical kind {kind!r}")

    if physical == DrakenType.ARRAY:
        # Draken carries the array child structurally on the vector, so the ELEMENT's
        # own physical TAG is recoverable — but only the tag. The child's descriptor
        # and its own child are not exposed at this layer (the open ARRAY
        # element-descriptor gap), so an element that is incomplete without one is
        # reported as VARIANT — this system's spelling for "element type unknown",
        # and what this path said for EVERY array before. An ARRAY of IPv4 reads back
        # as ARRAY<UINT32> for the same reason: the tag survives, the refinement does
        # not. Do not paper either over with a guessed unit/scale/element.
        child = nb.array_child_type
        if (
            child is None
            or child in _PARAMETERIZED_PHYSICAL
            or child == DrakenType.ARRAY
        ):
            return ARRAY(VARIANT)
        return ARRAY(ColumnType(child))

    # No descriptor: the tag is the whole type. ColumnType.__post_init__ rejects a
    # parameterized physical arriving here, which is exactly right — a DECIMAL or
    # TIMESTAMP vector with no descriptor is a broken vector, not a defaultable one.
    return ColumnType(physical)


def morsel_column_types(morsel) -> list:
    """A morsel's column types as `ColumnType` — the descriptor-aware counterpart
    of `Morsel.column_types`, which reports the bare `DrakenType` tag.

    Lives here rather than on `Morsel` because `ColumnType` is Opteryx's and
    `Morsel` is Draken's: Draken cannot import it, and duplicating the type
    vocabulary on the Draken side is the drift this helper exists to prevent.
    """
    return [column_type_from_vector(morsel.column(name)) for name in morsel.column_names]



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

# The unsigned tier, widest-wins. Kept here rather than derived from itemsize so
# find_compatible_type answers exactly what nc_promote_fixed
# (draken/ops/kernels/function_null_conditional.cpp) produces — the declared type
# must follow the kernel, never lead it.
_UNSIGNED_WIDTH = {
    DrakenType.UINT8: 1,
    DrakenType.UINT16: 2,
    DrakenType.UINT32: 4,
    DrakenType.UINT64: 8,
}
_UNSIGNED_OF = {
    DrakenType.UINT8: UINT8,
    DrakenType.UINT16: UINT16,
    DrakenType.UINT32: UINT32,
    DrakenType.UINT64: UINT64,
}


# Inclusive value range of each integer width. These are the SAME bounds the
# draken kernels range-check against (they are fixed by the width, not tunable),
# and they exist here so the BIND-TIME literal path can reach the same verdict as
# the runtime kernel: `CAST(300 AS INT8)` must fail at plan time with a readable
# error rather than at vector-construction time with a bare OverflowError, and
# `TRY_CAST(300 AS INT8)` must fold to NULL exactly as the kernel would null the
# row.
_INTEGER_BOUNDS = {
    DrakenType.INT8: (-128, 127),
    DrakenType.INT16: (-32768, 32767),
    DrakenType.INT32: (-2147483648, 2147483647),
    DrakenType.INT64: (-9223372036854775808, 9223372036854775807),
    DrakenType.UINT8: (0, 255),
    DrakenType.UINT16: (0, 65535),
    DrakenType.UINT32: (0, 4294967295),
    DrakenType.UINT64: (0, 18446744073709551615),
}


def integer_bounds(column_type) -> Optional[tuple]:
    """(low, high) for an integer-width ColumnType, else None.

    None means "not an integer width" — a float, a decimal, a string; NOT "no
    limits". Callers must treat None as "this check does not apply".
    """
    if column_type is None:
        return None
    return _INTEGER_BOUNDS.get(getattr(column_type, "physical", None))


_SIGNED_INT_LADDER = (DrakenType.INT8, DrakenType.INT16, DrakenType.INT32, DrakenType.INT64)
_UNSIGNED_INT_LADDER = (DrakenType.UINT8, DrakenType.UINT16, DrakenType.UINT32, DrakenType.UINT64)
_FLOAT_LADDER = (DrakenType.FLOAT32, DrakenType.FLOAT64)


def is_legal_widen(old: "ColumnType", new: "ColumnType") -> bool:
    """Whether ALTER COLUMN ... TYPE from `old` to `new` is a safe, lossless widening.

    Directional — unlike `find_compatible_type`, which blends N values to a common
    supertype and deliberately falls back to VARCHAR for anything it doesn't
    recognise. This never falls back: an unrecognised pair is illegal, not "coerce
    to string". Legal only within one ladder — signed int, unsigned int, or float —
    strictly widening. Everything else is rejected: integer->float (not exact
    across the full range at the top of the ladder), any type carrying a `logical`
    descriptor (DECIMAL/TIMESTAMP/TIME/VECTOR are a separate, not-yet-designed
    lattice; IPV4 is a UINT32 descriptor, not a plain integer — widening it away
    would silently drop the descriptor), VARCHAR/temporal involvement, and
    `old == new` (a no-op ALTER has no reason to mint a new schema generation).
    """
    if old is None or new is None or old == new:
        return False
    if old.logical is not None or new.logical is not None:
        return False
    for ladder in (_SIGNED_INT_LADDER, _UNSIGNED_INT_LADDER, _FLOAT_LADDER):
        if old.physical in ladder and new.physical in ladder:
            return ladder.index(new.physical) > ladder.index(old.physical)
    return False


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

    # Every non-null input the SAME ColumnType: pass it through unchanged instead
    # of collapsing to the category's canonical instance. The collapse is lossy in
    # two ways at once. IPV4 is UINT32 refined by a LogicalKind.IPV4 descriptor
    # and its category is deliberately INTEGER, so COALESCE(ipv4, ipv4) resolved
    # to INT64: the descriptor was gone (results read back as raw integers like
    # 3232235777 instead of '192.168.1.1') AND the declared physical type no
    # longer matched the UINT32 the blend kernel actually produces — the declared
    # vs actual divergence that drives downstream cast-kernel selection off a
    # type the data never had. Plain UINT32/UINT64/FLOAT32 were mislabelled the
    # same way.
    #
    # Narrow SIGNED ints are the deliberate exception: they still widen to INT64
    # because that is exactly what the kernel does (nc_canon_fixed in
    # function_null_conditional.cpp widens INT8/16/32 and passes everything else
    # through), and the declared type has to follow the data, not lead it.
    _WIDENED_TO_INT64 = (DrakenType.INT8, DrakenType.INT16, DrakenType.INT32)
    _column_types = [
        t
        for t in types
        if isinstance(t, ColumnType) and t.category != LogicalCategory.NULL
    ]
    _all_column_types = len(_column_types) == len(non_null)
    if (
        _all_column_types
        and len(set(_column_types)) == 1
        and _column_types[0].physical not in _WIDENED_TO_INT64
    ):
        return _column_types[0]

    # Mixed unsigned widths resolve to the WIDEST, matching nc_promote_fixed.
    # The category path below would answer INT64 (every unsigned's category is
    # INTEGER), which cannot hold the top half of UINT64 — and the blend kernel
    # would refuse the pair anyway, so COALESCE(uint32_col, uint64_col) failed
    # outright rather than returning a wrong number. Descriptor-bearing types are
    # excluded: an IPV4 is UINT32, but blending an address with a plain integer
    # is not a widening, it is a category error, and it stays refused.
    if _all_column_types and len(_column_types) > 1:
        _physicals = {t.physical for t in _column_types}
        if _physicals <= _UNSIGNED_WIDTH.keys() and all(
            t.logical is None for t in _column_types
        ):
            return _UNSIGNED_OF[max(_physicals, key=_UNSIGNED_WIDTH.__getitem__)]

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


