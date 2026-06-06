"""Result-type derivation for binary operations (sharp edge #1).

The operator map (keyed on `LogicalCategory`) decides the result *category* — e.g.
`(DECIMAL, DECIMAL, "Multiply") -> DECIMAL`, `(DATE, DATE, "Minus") -> INTERVAL`. It
CANNOT carry the result *parameters* (a static table can't compute `s1 + s2`). This
module is the authority for those parameters:

- DECIMAL (p, s)  — SQL Server rules (Decision E), capped at our int64 precision of 18,
  overflow → raise (never silently truncate; int128 backing is the deferred fix).
- INTEGER width   — the wider of the integer operands, matching Draken's kernel
  (`promote_narrow_int` computes at `wider_int_type` = the wider operand).
- FLOAT width     — FLOAT64 if any operand is FLOAT64, else FLOAT32 (integer/integer
  division yields FLOAT64).

Non-parameterized result categories return their canonical instance. Genuinely-open
cases (VECTOR results of arithmetic, unknown ops) raise rather than guess.
"""

from __future__ import annotations

from draken.draken_native import DrakenType

from opteryx.types import logical_type as lt
from opteryx.types.logical_type import ColumnType
from opteryx.types.logical_type import LogicalCategory

__all__ = ["compute_result_logical_type"]


# Integer width ordering (matches Draken's enum-ordinal width logic for signed ints).
_INT_RANK = {
    DrakenType.INT8: 0,
    DrakenType.INT16: 1,
    DrakenType.INT32: 2,
    DrakenType.INT64: 3,
}
_INT_BY_RANK = {0: lt.INT8, 1: lt.INT16, 2: lt.INT32, 3: lt.INT64}

# Decimal digits needed to hold each signed integer width (for int-as-decimal in mixed
# DECIMAL/INTEGER arithmetic). INT64 needs 19 digits — already over our cap of 18, so
# any DECIMAL op INT64 that grows will hit the overflow policy and raise (honest).
_INT_DIGITS = {
    DrakenType.INT8: 3,
    DrakenType.INT16: 5,
    DrakenType.INT32: 10,
    DrakenType.INT64: 19,
}

# Non-parameterized result categories -> canonical instance.
_CANONICAL_BY_CATEGORY = {
    LogicalCategory.BOOLEAN: lt.BOOLEAN,
    LogicalCategory.DATE: lt.DATE,
    LogicalCategory.INTERVAL: lt.INTERVAL,
    LogicalCategory.VARCHAR: lt.VARCHAR,
    LogicalCategory.NVARCHAR: lt.NVARCHAR,
    LogicalCategory.VARBINARY: lt.VARBINARY,
    LogicalCategory.VARIANT: lt.VARIANT,
    LogicalCategory.NULL: lt.NULL,
}

_DECIMAL_MAX_PRECISION = 38  # int128 (DECIMAL128) now available; raise only past 38
_DECIMAL_INT64_PRECISION = 18  # int64-backed tier limit
_DECIMAL_MIN_SCALE = 6       # SQL Server's floor when reducing scale on overflow


def _decimal_ps(ct: ColumnType):
    """(precision, scale) for a numeric operand: DECIMAL as-is, INTEGER as (digits, 0)."""
    if ct.category == LogicalCategory.DECIMAL:
        return ct.logical.precision, ct.logical.scale
    if ct.category == LogicalCategory.INTEGER:
        return _INT_DIGITS[ct.physical], 0
    raise NotImplementedError(
        f"cannot treat {ct.category.name} as DECIMAL in result derivation"
    )


def _make_decimal(precision: int, scale: int) -> ColumnType:
    """Build a DECIMAL result, choosing physical tier and clamping precision at 38.

    Matches the runtime's posture (`draken_native.cpp` `decimal_*_dispatch`):
    cap precision at 38, cap scale at min(scale, 38). Actual int128 value overflow
    raises per-value at execute time (the runtime checks); the binder doesn't
    pre-raise on a *declared* precision exceeding 38, because real values rarely
    fill the declared headroom.

    Tier selection: p ≤ 18 → DECIMAL (int64); 19 ≤ p ≤ 38 → DECIMAL128 (int128).
    """
    if precision > _DECIMAL_MAX_PRECISION:
        precision = _DECIMAL_MAX_PRECISION
    if scale > precision:
        scale = precision
    if scale < 0:
        scale = 0
    # lt.DECIMAL chooses the physical tier: DECIMAL (int64) or DECIMAL128 (int128)
    return lt.DECIMAL(precision, scale)


def _decimal_result(left: ColumnType, right: ColumnType, op: str) -> ColumnType:
    """Derive DECIMAL result (precision, scale) for a binary op.

    MATCHES THE RUNTIME (draken_native.cpp `decimal_*_dispatch`):
      Plus/Minus: scale = max(s1,s2);  precision = max(s1,s2) + max(p1-s1, p2-s2) + 1
      Multiply:   scale = s1+s2;       precision = p1 + p2          (NB: no `+1`)
      Divide:     scale = max(s1+6,6); precision = p1 + 6           (NB: pa+6 cap)

    The binder MUST agree with what the runtime actually computes. Decision E's
    earlier formulas (mul p1+p2+1, divide scale s1+p2+1) and "reduce-scale-then-raise"
    policy were written before the runtime shipped; reconciling here keeps a single
    source of truth and avoids regressing TPC-H q09 (nested decimal arithmetic whose
    runtime-derived precision the binder would have raised on).
    """
    p1, s1 = _decimal_ps(left)
    p2, s2 = _decimal_ps(right)
    if op in ("Plus", "Minus"):
        scale = max(s1, s2)
        precision = max(s1, s2) + max(p1 - s1, p2 - s2) + 1
    elif op == "Multiply":
        precision = p1 + p2
        scale = s1 + s2
    elif op == "Divide":
        scale = max(s1 + _DECIMAL_MIN_SCALE, _DECIMAL_MIN_SCALE)
        precision = p1 + _DECIMAL_MIN_SCALE
    else:
        raise NotImplementedError(f"no DECIMAL result rule for operator {op!r}")
    return _make_decimal(precision, scale)


def compute_result_logical_type(
    left: ColumnType, right: ColumnType, op: str, result_category: LogicalCategory
) -> ColumnType:
    """Derive the full result `ColumnType` for a binary op.

    Args:
        left, right: operand column types.
        op: operator string ("Plus", "Minus", "Multiply", "Divide", "Eq", ...).
        result_category: the result category from the operator map.
    """
    if result_category == LogicalCategory.DECIMAL:
        return _decimal_result(left, right, op)

    if result_category == LogicalCategory.INTEGER:
        ranks = [
            _INT_RANK[o.physical]
            for o in (left, right)
            if o.category == LogicalCategory.INTEGER
        ]
        if not ranks:
            raise NotImplementedError(
                "INTEGER result requires at least one INTEGER operand to size the width"
            )
        return _INT_BY_RANK[max(ranks)]

    if result_category == LogicalCategory.FLOAT:
        if DrakenType.FLOAT64 in (left.physical, right.physical):
            return lt.FLOAT64
        if DrakenType.FLOAT32 in (left.physical, right.physical):
            return lt.FLOAT32
        # no float operand (e.g. integer/integer division -> double)
        return lt.FLOAT64

    canonical = _CANONICAL_BY_CATEGORY.get(result_category)
    if canonical is not None:
        return canonical

    # Temporal results of arithmetic (e.g. DATE + INTERVAL -> TIMESTAMP). Microseconds is
    # the engine's storage unit; offset 0 (no tz arithmetic in v1).
    if result_category == LogicalCategory.TIMESTAMP:
        return lt.TIMESTAMP()
    if result_category == LogicalCategory.TIME:
        return lt.TIME()

    raise NotImplementedError(
        f"no result-type derivation for category {result_category.name} "
        f"(op {op!r}, {left} {right})"
    )
