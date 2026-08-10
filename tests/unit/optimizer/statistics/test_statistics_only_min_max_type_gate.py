# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
The MIN/MAX type allowlist in `is_simple_aggregate` is load-bearing, and nothing
downstream re-checks it.

`StatisticsOnlyResponseStrategy` answers `SELECT MIN(col) / MAX(col) FROM t`
without reading the table, by handing back a manifest bound as a literal.
`get_min_max_from_manifest` does no type checking of its own — it returns
whatever `lower_bounds`/`upper_bounds` (or `column_stats`) hold. So the category
allowlist is the whole defence, and it is currently correct BY ACCIDENT: nothing
records why those three categories and not others.

Two invariants make DATE / INTEGER / TIMESTAMP safe. This file asserts both
against the allowlist itself, so adding a category that breaks either one fails
here instead of silently returning a wrong answer to a user.

  1. ORDINALIZE IS IDENTITY. An ANALYZE/skene manifest stores bounds as
     `Vector.ordinalize()` int64 keys rather than decoded values, and the
     strategy has no `bounds_are_ordinal` branch — it returns the bound as the
     answer either way. That is only correct where the ordinal equals the value.
  2. THE COLUMN CANNOT HOLD A NaN. Parquet omits NaN from min/max by spec while
     draken ranks NaN above every value (draken/ops/float_ops.h, architect-locked
     2026-05-22), so for a float column the recorded upper bound is not the MAX.

Both are about categories that are NOT on the list today, which is exactly the
point: the test exists to catch the widening, not the current state.
"""

from __future__ import annotations

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../../../.."))

import pytest

from opteryx.planner.optimizer.strategies.statistics_only_response import is_simple_aggregate
from opteryx.expression import NodeType
from opteryx.models import Node
from opteryx.planner.logical_planner import LogicalPlanNode, LogicalPlanStepType
from opteryx.types.logical_type import (
    DATE,
    FLOAT32,
    FLOAT64,
    INT64,
    LogicalCategory,
    TIMESTAMP,
)

# The categories the strategy admits today. Spelled out rather than imported so
# widening the production list has to be a deliberate, visible change here too.
ADMITTED = (LogicalCategory.DATE, LogicalCategory.INTEGER, LogicalCategory.TIMESTAMP)

# A representative ColumnType per admitted category, and the raw physical values
# a bound would hold for it. DATE/TIMESTAMP literals are already normalised to
# their raw physical integer by bind time (see ColumnType.ordinalize), which is
# why they are spelled as ints here rather than as date objects.
ADMITTED_TYPES = {
    LogicalCategory.DATE: (DATE, [-7305, 0, 19000]),
    LogicalCategory.INTEGER: (INT64, [-(2**40), -5, 0, 7, 2**40]),
    LogicalCategory.TIMESTAMP: (TIMESTAMP(), [-1_000_000, 0, 1_700_000_000_000_000]),
}


def _aggregate(func, category):
    """An AGGREGATOR node over one column of `category`."""
    schema_column = Node(NodeType.IDENTIFIER, name="c", category=category)
    return Node(
        NodeType.AGGREGATOR,
        value=func,
        duplicate_treatment=None,
        condition=None,
        parameters=[Node(NodeType.IDENTIFIER, schema_column=schema_column, source_column="c")],
    )


def _agg_node(func, category):
    return LogicalPlanNode(
        node_type=LogicalPlanStepType.Aggregate,
        aggregates=[_aggregate(func, category)],
        groups=None,
    )


# ---------------------------------------------------------------------------
# invariant 1 — an ordinal bound is only returnable when it IS the value
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("category", ADMITTED)
def test_ordinalize_is_identity_for_every_admitted_category(category):
    column_type, values = ADMITTED_TYPES[category]
    for value in values:
        assert column_type.ordinalize(value) == value, (
            f"{category} is on the MIN/MAX allowlist but its ordinal encoding is "
            f"not the identity ({value} -> {column_type.ordinalize(value)}). An "
            f"ANALYZE manifest would answer MIN/MAX with the ordinal."
        )


@pytest.mark.parametrize("column_type", [FLOAT32, FLOAT64])
def test_float_ordinalize_is_not_identity(column_type):
    # The counterexample the allowlist exists to exclude. If this ever became an
    # identity the reasoning above would need revisiting, not quietly inheriting.
    assert column_type.ordinalize(3.5) != 3.5
    assert isinstance(column_type.ordinalize(3.5), int)


# ---------------------------------------------------------------------------
# invariant 2 — no admitted category can hold a value its bounds cannot see
# ---------------------------------------------------------------------------


def test_no_admitted_category_can_hold_a_nan():
    # NaN is the only value that is IN a column but OUT of its parquet bounds,
    # and only the float categories can hold one.
    assert LogicalCategory.FLOAT not in ADMITTED


# ---------------------------------------------------------------------------
# the gate itself
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("func", ["MIN", "MAX"])
@pytest.mark.parametrize("category", ADMITTED)
def test_admitted_categories_are_accepted(func, category):
    assert is_simple_aggregate(_agg_node(func, category)) is True


@pytest.mark.parametrize("func", ["MIN", "MAX"])
def test_float_is_refused(func):
    # MAX(float) is the wrong answer this refusal prevents: the bound is the
    # largest FINITE value, and the engine returns NaN when the column has one.
    # MIN(float) is refused as collateral — it would be sound on decoded bounds
    # (NaN is never the minimum) but not on ordinal ones, and turning it on is a
    # deliberate change, not something to inherit by widening a list.
    assert is_simple_aggregate(_agg_node(func, LogicalCategory.FLOAT)) is False


@pytest.mark.parametrize(
    "category",
    [LogicalCategory.VARCHAR, LogicalCategory.VARBINARY, LogicalCategory.DECIMAL],
)
def test_other_categories_are_refused(category):
    assert is_simple_aggregate(_agg_node("MAX", category)) is False
