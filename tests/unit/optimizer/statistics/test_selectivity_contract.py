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

`_clamp01` enforced it with a bare `< 0.0` / `> 1.0` pair. NaN compares False
against both, so it fell straight through the clamp. A NaN literal is enough to
produce one — `col >= SQRT(-390664.0)` makes the interval-arithmetic tiers
evaluate to NaN — and it then survived every multiplication in the callers and
reached `int()`, which raised `ValueError: cannot convert float NaN to integer`
from inside the PLANNER, killing a query the engine executes perfectly well.

Fixed in `_clamp01`, not in the callers: three call sites in statistics_refresh
alone had the same exposure, two were guarded first and the third was missed,
which is the argument for the contract being enforced once where it is stated.

NaN clamps to 1.0 rather than 0.0. It means "the estimator could not compute a
fraction", and the module's posture for absent information is "assume no
reduction". 0.0 would assert that nothing matches — a confident wrong number
feeding row counts and join ordering.
"""

from __future__ import annotations

import math
import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../../../.."))

import pytest

# Import the package before the module: `selectivity` participates in a cycle
# with `planner.optimizer`, so reaching for it first fails to initialise.
import opteryx.planner.optimizer  # noqa: F401
from opteryx.planner.cost_estimation.selectivity import _clamp01

import opteryx


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
