# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Catalog-backed datasets store min/max as `Vector.ordinalize()` keys, and
`prune_files` must ordinalize predicate literals to match.

For most types the ordinal key IS the value the binder produces, which is why
pruning appeared healthy: an identity widen for signed integers, and the raw
physical integer for DATE/TIMESTAMP/TIME (a DATE literal binds to `-7305`,
days since epoch, not a `datetime.date`).

FLOAT was the exception and was silently WRONG. Its ordinal key is an
order-preserving BIT transform, so a file holding 0.1..0.9 stored
4591870180066957722..4606281698874543309, and `WHERE x = 0.5` compared 0.5
against those, concluded 0.5 was below the minimum, and pruned away the file
that actually contained the matching rows — rows silently missing from the
result, not merely a slower plan.
"""

import os
import sys
from opteryx.compiled.structures.expressions import Comparison
from opteryx.compiled.structures.expressions import Literal

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

import pytest

import opteryx.planner.optimizer  # noqa: F401  (resolves the optimizer import cycle)
from draken.draken_native import DrakenType
from opteryx.expression import NodeType
from opteryx.types.logical_type import DATE, DECIMAL, FLOAT64, INT64, TIMESTAMP, VARCHAR
from opteryx.types.schema import RelationSchema
from opteryx.compiled.structures.expressions import LogicalColumn
from opteryx.planner.plan_context import PlanContext
from tests.manifests import FileSpec
from tests.manifests import build_manifest


def _manifest(plan_context, column_type, ordinal_min, ordinal_max, *, bounds_are_ordinal=True):
    schema = RelationSchema(
        name="t",
        columns=[plan_context.columns.relation_column("t", "c", column_type=column_type)],
    )
    entry = FileSpec(
        file_path="f1",
        record_count=10,
        lower_bounds={0: ordinal_min},
        upper_bounds={0: ordinal_max},
    )
    return build_manifest(schema, [entry], bounds_are_ordinal=bounds_are_ordinal)


def _predicate(plan_context, op, value, column_type):
    identifier = LogicalColumn(
        node_type=NodeType.IDENTIFIER, source_column="c", arena=plan_context.expressions
    )
    literal = Literal(type=column_type, value=value, arena=plan_context.expressions)
    return Comparison(value=op, left=identifier, right=literal, arena=plan_context.expressions)


def _survives(column_type, ordinal_min, ordinal_max, op, value, literal_type=None, *, bounds_are_ordinal=True):
    """Whether the one file bounded by (ordinal_min, ordinal_max) survives
    `c <op> value`. The literal is typed as the column unless `literal_type`
    says otherwise, and holds that type's native value."""
    plan_context = PlanContext()
    manifest = _manifest(
        plan_context, column_type, ordinal_min, ordinal_max, bounds_are_ordinal=bounds_are_ordinal
    )
    predicate = _predicate(plan_context, op, value, literal_type or column_type)
    manifest = manifest.prune_files([predicate], plan_context=plan_context)
    return manifest.get_file_count() == 1


# ── FLOAT: the bug this exists for ──────────────────────────────────────────


def test_float_in_range_equality_keeps_the_file():
    lo = DrakenType.FLOAT64.ordinalize(0.1)
    hi = DrakenType.FLOAT64.ordinalize(0.9)
    assert _survives(FLOAT64, lo, hi, "Eq", 0.5)


def test_float_out_of_range_equality_prunes_the_file():
    lo = DrakenType.FLOAT64.ordinalize(0.1)
    hi = DrakenType.FLOAT64.ordinalize(0.9)
    assert not _survives(FLOAT64, lo, hi, "Eq", 5.0)


def test_float_range_predicates():
    lo = DrakenType.FLOAT64.ordinalize(0.1)
    hi = DrakenType.FLOAT64.ordinalize(0.9)
    assert _survives(FLOAT64, lo, hi, "Gt", 0.5)
    assert not _survives(FLOAT64, lo, hi, "Gt", 100.0)
    assert _survives(FLOAT64, lo, hi, "Lt", 0.5)
    assert not _survives(FLOAT64, lo, hi, "Lt", -100.0)


def test_negative_floats_order_correctly():
    # ordinalize flips the bits of negatives so signed int64 order matches
    # value order; a naive raw-bit key would sort these backwards.
    lo = DrakenType.FLOAT64.ordinalize(-5.0)
    hi = DrakenType.FLOAT64.ordinalize(-1.0)
    assert _survives(FLOAT64, lo, hi, "Eq", -3.0)
    assert not _survives(FLOAT64, lo, hi, "Eq", -100.0)
    assert not _survives(FLOAT64, lo, hi, "Eq", 3.0)


def test_float_without_the_flag_is_the_old_broken_behaviour():
    # Guards the diagnosis itself: the same in-range value IS wrongly pruned
    # when the manifest doesn't declare its bounds as ordinal.
    lo = DrakenType.FLOAT64.ordinalize(0.1)
    hi = DrakenType.FLOAT64.ordinalize(0.9)
    assert not _survives(FLOAT64, lo, hi, "Eq", 0.5, bounds_are_ordinal=False)


# ── types that already worked must keep working ─────────────────────────────


def test_integer_bounds_unaffected():
    assert _survives(INT64, 1, 177, "Eq", 50)
    assert not _survives(INT64, 1, 177, "Eq", 9999)
    assert not _survives(INT64, 1, 177, "Gt", 1000)


def test_date_bounds_unaffected():
    # Binder normalises a DATE literal to days-since-epoch (an int).
    assert _survives(DATE, -7305, 9131, "Eq", 0)
    assert not _survives(DATE, -7305, 9131, "Eq", 99999)


def test_timestamp_bounds_still_prune():
    # The regression this fix had to avoid: TIMESTAMP64 is refused by the
    # physical-only draken entry point, so a naive wiring would skip pruning
    # for the most common filter on a log table.
    lo, hi = 1784534400432637, 1785477522500643
    assert _survives(TIMESTAMP(), lo, hi, "Eq", 1785000000000000)
    assert not _survives(TIMESTAMP(), lo, hi, "Eq", 1)
    assert not _survives(TIMESTAMP(), lo, hi, "Gt", 1785477522500644)


def test_string_bounds_prune_on_the_prefix_key():
    lo = DrakenType.VARCHAR.ordinalize("apple")
    hi = DrakenType.VARCHAR.ordinalize("pear")
    assert _survives(VARCHAR, lo, hi, "Eq", b"banana")
    assert not _survives(VARCHAR, lo, hi, "Eq", b"zebra")


def test_decimal_prunes_on_the_rescaled_mantissa():
    """A DECIMAL bound is the UNSCALED MANTISSA at the COLUMN's scale, so the
    literal is rescaled onto that gridline before it is compared.

    Bounds 1000..90000 on DECIMAL(10,4) mean the file holds 0.1000..9.0000. The
    scale is the whole risk: `0.5` compared as `0.5` (or as `5`, its own-scale
    mantissa) reads as BELOW 1000 and prunes away the file that holds the matching
    rows — the same silent-wrong-answer shape this file was written about for
    FLOAT. As 5000 it lands inside the bounds and the file is kept.

    Until 2026-08-21 `ColumnType.ordinalize` refused DECIMAL outright and this test
    asserted the abstention. Pruning is now real, so what is asserted is the
    ANSWER: kept when the value could be in range, dropped when it provably cannot.
    """
    import decimal

    dec = DECIMAL(10, 4)
    # 0.5 -> mantissa 5000, inside [1000, 90000].
    assert _survives(dec, 1000, 90000, "Eq", decimal.Decimal("0.5"))
    # 999999 -> mantissa 9999990000, far above the maximum: provably absent.
    assert not _survives(dec, 1000, 90000, "Eq", decimal.Decimal("999999"))
    # Ranges prune from the correct side.
    assert not _survives(dec, 1000, 90000, "Gt", decimal.Decimal("9"))
    assert _survives(dec, 1000, 90000, "Gt", decimal.Decimal("8.9999"))
    assert not _survives(dec, 1000, 90000, "Lt", decimal.Decimal("0.1"))
    assert _survives(dec, 1000, 90000, "Lt", decimal.Decimal("0.1001"))
    # An integer literal is a decimal at scale 0 and must rescale too: 5 is 50000.
    assert _survives(dec, 1000, 90000, "Eq", 5, INT64)
    assert not _survives(dec, 1000, 90000, "Eq", 50, INT64)


def test_decimal_off_gridline_equality_is_still_skipped():
    """A literal that cannot sit on the column's scale gridline has no mantissa,
    and no rounding of an EQUALITY target preserves what it matches — so pruning
    abstains and the file is kept.

    `rescale_decimal_literal` can round an off-gridline ORDERING bound
    direction-aware (exactly), but that needs the operator, which `ordinalize` is
    not given. Both halves of that are asserted here: an unroundable equality, and
    an off-gridline ordering bound that is also skipped from this entry point.
    """
    import decimal

    dec = DECIMAL(10, 2)          # gridline is 0.01
    # 0.005 is not representable at scale 2 -> no term -> file kept.
    assert _survives(dec, 1000, 90000, "Eq", decimal.Decimal("0.005"))
    assert _survives(dec, 1000, 90000, "Gt", decimal.Decimal("999.995"))


def test_decimal128_still_refuses():
    """Not a rescaling question: draken produces no ordinal key for DECIMAL128 at
    all, so there is no stored bound in this space to compare against. It must stay
    a refusal rather than quietly borrowing the int64 tier's answer."""
    import decimal

    # precision > 18 selects the int128-backed tier (see the DECIMAL factory).
    d128 = DECIMAL(38, 4)
    assert d128.physical == DrakenType.DECIMAL128
    with pytest.raises(ValueError):
        d128.ordinalize(decimal.Decimal("0.5"))
    # And the file is KEPT rather than compared in a space that does not exist —
    # the native pruner has no ordinal key for the literal, so it skips the term.
    assert _survives(d128, 1000, 90000, "Eq", decimal.Decimal("999999"))


if __name__ == "__main__":  # pragma: no cover
    pytest.main([__file__, "-v"])
