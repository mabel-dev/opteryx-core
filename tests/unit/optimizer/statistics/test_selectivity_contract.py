# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
`estimate_selectivity` promises a value in [0.0, 1.0]. NaN broke that promise.

Its docstring says "Returns a value in [0.0, 1.0]. Never raises on missing
stats", and every caller relies on it — `int(row_count * selectivity)` in
statistics_refresh, the ordering comparisons in PredicateOrderingStrategy, the
manifest's `estimate_selectivity`, and two sites in the native compiler. None of
them re-checks the range, which is correct: the contract is the estimator's job.

The clamp enforced it with a bare `< 0.0` / `> 1.0` pair. NaN compares False
against both, so it fell straight through the clamp. A NaN literal is enough to
produce one — `col >= SQRT(-390664.0)` makes the interval-arithmetic tiers
evaluate to NaN — and it then survived every multiplication in the callers and
reached `int()`, which raised `ValueError: cannot convert float NaN to integer`
from inside the PLANNER, killing a query the engine executes perfectly well.

Fixed in the clamp, not in the callers: three call sites in statistics_refresh
alone had the same exposure, two were guarded first and the third was missed,
which is the argument for the contract being enforced once where it is stated.

NaN clamps to 1.0 rather than 0.0. It means "the estimator could not compute a
fraction", and the module's posture for absent information is "assume no
reduction". 0.0 would assert that nothing matches — a confident wrong number
feeding row counts and join ordering.

The clamp is now native (`clamp01` in src/cpp/planner/selectivity.hpp) and not
exposed on its own: it is driven here through `IS NULL`, whose estimate is the
column's recorded null fraction passed through the clamp, so any double —
NaN and the infinities included — reaches it verbatim.
"""

from __future__ import annotations

import math
import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../../../.."))

import pytest

# Importing opteryx.planner.optimizer (the package) first resolves the
# pre-existing import cycle a compiled planner module hits when imported first.
import opteryx.planner.optimizer  # noqa: F401
from opteryx.compiled.planner.statistics import StatisticsInput
from opteryx.compiled.planner.statistics import estimate_selectivity
from opteryx.compiled.structures.expressions import LogicalColumn
from opteryx.compiled.structures.expressions import UnaryOperator
from opteryx.expression import NodeType
from opteryx.planner.plan_context import PlanContext

import opteryx

# One query context for the column AND the expression: the native estimator
# resolves a predicate's columns through its arena's bound ColumnTable.
_PLAN_CONTEXT = PlanContext()
_COLUMN = _PLAN_CONTEXT.columns.relation_column("t", "col")


def _clamp01(value):
    """The estimator's clamp applied to `value`: `col IS NULL` against a column
    whose recorded null fraction is `value`."""
    identifier = LogicalColumn(
        node_type=NodeType.IDENTIFIER,
        source_column="col",
        schema_column=_COLUMN,
        arena=_PLAN_CONTEXT.expressions,
    )
    predicate = UnaryOperator(value="IsNull", centre=identifier, arena=_PLAN_CONTEXT.expressions)
    stats = StatisticsInput(
        _PLAN_CONTEXT.columns,
        row_count_estimate=1000,
        column_stats={_COLUMN.identity: {"null_fraction": value}},
    )
    return estimate_selectivity(predicate, stats)


@pytest.mark.parametrize(
    "value,expected",
    [
        (float("nan"), 1.0),   # the case that escaped — no information, no reduction
        (float("inf"), 1.0),
        (float("-inf"), 0.0),
        (-0.5, 0.0),
        (0.0, 0.0),
        (0.5, 0.5),
        (1.0, 1.0),
        (2.0, 1.0),
    ],
)
def test_clamp_never_returns_a_value_outside_the_unit_interval(value, expected):
    result = _clamp01(value)
    assert not math.isnan(result), f"_clamp01({value!r}) returned NaN — the contract is violated"
    assert 0.0 <= result <= 1.0
    assert result == expected


def _scalar(sql):
    session = opteryx.session()
    rows = []
    for morsel in session.execute_to_morsels(sql):
        for i in range(morsel.num_rows):
            rows.append(tuple(morsel[i]))
    assert len(rows) == 1 and len(rows[0]) == 1, f"expected one scalar from {sql!r}, got {rows!r}"
    return rows[0][0]


@pytest.mark.parametrize(
    "sql,expected",
    [
        # Nothing is >= NaN except a NaN, and no planet has one.
        ("SELECT COUNT(*) AS n FROM testdata.planets WHERE orbital_period >= SQRT(-390664.0)", 0),
        # ...but f_special DOES have NaNs, and `NaN >= NaN` is TRUE under the
        # total order, so this is the NaN count, not zero. Pins that the planner
        # fix did not turn into "NaN predicates match nothing".
        ("SELECT COUNT(*) AS n FROM testdata.fuzzing.mixed WHERE f_special >= SQRT(-1.0)", 24),
        # Nothing is > NaN at all.
        ("SELECT COUNT(*) AS n FROM testdata.fuzzing.mixed WHERE f_special > SQRT(-1.0)", 0),
        # A NaN conjunct alongside an ordinary one — the selectivities multiply,
        # which is where a NaN used to contaminate an otherwise fine estimate.
        (
            "SELECT COUNT(*) AS n FROM testdata.fuzzing.mixed "
            "WHERE f_value > SQRT(-2.0) AND i_value < 10",
            0,
        ),
    ],
)
def test_a_nan_literal_predicate_plans_and_runs(sql, expected):
    assert _scalar(sql) == expected
