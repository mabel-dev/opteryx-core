# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""`sql_select_limit` enforcement — reject, never truncate.

Two gates, because neither alone is sufficient:

  PLAN time  — refuses before any IO, from the optimizer's estimate, but ONLY when
               every input relation reports a real row count. An estimate resting on
               statistics_refresh._UNKNOWN_ROW_COUNT (1,000,000 for a relation that
               cannot report its size) multiplies through joins: before virtual
               datasets declared their counts, a 2-way self cross join of the 9-row
               $planets estimated 10**12 rows against an actual 81. Gating on that
               would refuse trivial queries.

  RUN time   — counts rows actually delivered, catching results the estimate was too
               low (or too unavailable) to predict.

Both raise rather than truncating: returning the first N rows of a larger result is
a wrong answer the caller cannot detect.
"""

import os
import sys

import pytest

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

import opteryx
from opteryx.exceptions import ResultTooLargeError
from opteryx.managers.virtual_datasets import planet_data
from opteryx.planner.result_size_guard import _declared_row_count, every_input_has_row_counts


def _session_with_limit(limit):
    session = opteryx.session(user="bastian")
    current = session.context.variables._variables["sql_select_limit"]
    session.context.variables._variables["sql_select_limit"] = (
        current[0], limit, current[2], current[3],
    )
    return session


def _run(sql, limit):
    session = _session_with_limit(limit)
    rows = 0
    for morsel in session.execute_to_morsels(sql):
        rows += morsel.num_rows
    return rows


# ── the virtual-dataset statistics this all rests on ────────────────────────────

def test_virtual_datasets_declare_row_counts():
    # Without these, every estimate touching virtual data is fabricated.
    assert planet_data.schema().row_count_metric == 9
    from opteryx.managers.virtual_datasets import no_table_data, user, variables_data

    assert no_table_data.schema().row_count_metric == 1
    assert user.schema().row_count_estimate > 0
    assert variables_data.schema().row_count_estimate > 0


def test_planets_estimate_is_exact_not_the_million_fallback():
    # The regression that motivated this: $planets estimated 1,000,000 (the unknown
    # fallback) rather than its true 9, which cubed to 10**18 across two cross joins.
    assert _declared_row_count is not None
    assert _run("SELECT * FROM $planets", limit=100) == 9
    # Would have been rejected on a 10**12 estimate before the counts were declared.
    assert _run("SELECT p1.id FROM $planets p1 CROSS JOIN $planets p2", limit=1000) == 81


# ── plan-time gate ──────────────────────────────────────────────────────────────

def test_plan_time_rejects_when_estimate_exceeds_limit():
    with pytest.raises(ResultTooLargeError) as exc:
        _run("SELECT p1.id FROM $planets p1 CROSS JOIN $planets p2", limit=50)
    assert exc.value.estimated is True, "should have been caught before execution"
    assert exc.value.rows == 81
    assert exc.value.limit == 50


def test_error_message_tells_the_user_to_add_a_limit():
    with pytest.raises(ResultTooLargeError) as exc:
        _run("SELECT p1.id FROM $planets p1 CROSS JOIN $planets p2", limit=50)
    message = str(exc.value)
    assert "LIMIT" in message, message
    assert "81" in message and "50" in message, message


def test_an_explicit_limit_rescues_the_query():
    # The estimate must account for LIMIT, or every large table would be unqueryable.
    assert _run("SELECT p1.id FROM $planets p1 CROSS JOIN $planets p2 LIMIT 10", limit=50) == 10


def test_order_by_limit_rescues_the_query():
    # Regression: OperatorFusionStrategy fuses an adjacent Order+Limit into a single
    # HeapSort node carrying `.limit`, but statistics_refresh treated HeapSort as a
    # bare pass-through and never applied that limit, so the plan-time estimate was
    # the full (unlimited) row count -- rejecting queries an ordinary LIMIT would
    # have rescued.
    assert (
        _run(
            "SELECT p1.id FROM $planets p1 CROSS JOIN $planets p2 ORDER BY p1.id DESC LIMIT 10",
            limit=50,
        )
        == 10
    )


def test_explain_does_not_apply_the_inner_querys_estimate():
    # Regression reported 2026-07-30: `EXPLAIN SELECT DISTINCT project FROM
    # opteryx.test.pypi WHERE project LIKE '%opteryx%'` was rejected as
    # "estimated to return" billions of rows. EXPLAIN wraps the inner query's
    # plan (`... -> Exit -> Explain`); the guard scanned for any node of type
    # Exit and found the INNER query's Exit node, enforcing the limit against a
    # row count EXPLAIN never streams -- its own output is a plan description,
    # not the inner query's result set. A limit far below the inner query's
    # real estimate must not raise.
    rows = _run(
        "EXPLAIN SELECT p1.id FROM $planets p1 CROSS JOIN $planets p2",
        limit=50,
    )
    assert rows > 0


def test_under_the_limit_is_untouched():
    assert _run("SELECT * FROM $planets", limit=1_000_000) == 9


def test_limit_of_zero_disables_enforcement():
    # 0 / unset means "no limit", not "reject everything".
    assert _run("SELECT * FROM $planets", limit=0) == 9


# ── the conditional: unknown statistics must not produce a false rejection ──────

def test_gate_is_disabled_when_an_input_has_no_row_count():
    from opteryx.planner.logical_planner.logical_planner import (
        LogicalPlanStepType,
        LogicalPlanNode,
    )
    from opteryx.third_party.travers import Graph

    plan = Graph()
    scan = LogicalPlanNode(node_type=LogicalPlanStepType.Scan)
    scan.relation = "unknowable"
    plan.add_node("s", scan)
    # No manifest and no schema -> no declared count -> the plan-time gate must
    # decline to act rather than reject on a fabricated number.
    assert _declared_row_count(scan) is None
    assert every_input_has_row_counts(plan) is False


def test_runtime_catches_what_the_estimate_cannot():
    # UNNEST is a function dataset: no manifest, no declared row count, so the
    # plan-time gate stands down. The runtime counter must still enforce.
    with pytest.raises(ResultTooLargeError) as exc:
        _run("SELECT * FROM UNNEST((1,2,3,4,5,6,7,8,9,10)) AS u", limit=3)
    assert exc.value.estimated is False, "should have been caught during delivery"
    assert exc.value.limit == 3


def test_default_limit_does_not_disturb_ordinary_queries():
    session = opteryx.session(user="bastian")
    rows = 0
    for morsel in session.execute_to_morsels("SELECT * FROM $planets"):
        rows += morsel.num_rows
    assert rows == 9


# ── pushed-down predicates must not blind the plan-time estimate ────────────────
#
# Regression for the bug reported 2026-07-29: `WHERE project = 'x'` against a
# billion-row table was rejected as "estimated to return" ~the full table, even
# though only a handful of rows actually matched. Root cause: PredicatePushdown
# moves a selective single-table predicate off the Filter node and onto
# Scan.predicates, and the plan-time estimator failed to apply selectivity to
# that path -- see test_scan_filter_pushdown.py for the statistics-level proof.
# These tests prove the fix at the level the bug was reported at: does the
# query actually run.

@pytest.mark.skipif(
    not os.path.isdir("testdata/tpch_001"),
    reason="testdata/tpch_001 not populated",
)
def test_selective_pushed_down_filter_is_not_rejected():
    # nation has 25 rows; exactly one is named BRAZIL. n_name = 'BRAZIL' is a
    # VARCHAR equality the filesystem connector pushes onto the Scan, deleting
    # the Filter node before the estimate runs. A limit of 5 sits strictly
    # between the correct estimate (~1-2) and the old bug's estimate (the full
    # 25) -- this only passes if selectivity was actually applied.
    assert (
        _run(
            "SELECT n_name FROM testdata.tpch_001.nation WHERE n_name = 'BRAZIL'",
            limit=5,
        )
        == 1
    )


@pytest.mark.skipif(
    not os.path.isdir("testdata/tpch_001"),
    reason="testdata/tpch_001 not populated",
)
def test_distinct_over_selective_pushed_down_filter_is_not_rejected():
    assert (
        _run(
            "SELECT DISTINCT n_name FROM testdata.tpch_001.nation WHERE n_name = 'BRAZIL'",
            limit=5,
        )
        == 1
    )


@pytest.mark.skipif(
    not os.path.isdir("testdata/tpch_001"),
    reason="testdata/tpch_001 not populated",
)
def test_unfiltered_scan_over_the_limit_is_still_rejected():
    # Guard against the fix over-correcting: with no predicate to push, the
    # estimate must still be the real (too-large) manifest count.
    with pytest.raises(ResultTooLargeError) as exc:
        _run("SELECT * FROM testdata.tpch_001.nation", limit=5)
    assert exc.value.estimated is True
    assert exc.value.rows == 25
    assert exc.value.limit == 5


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
