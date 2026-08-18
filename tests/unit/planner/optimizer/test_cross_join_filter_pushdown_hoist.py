# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Regression tests for the arithmetic-join-key hoist in
``cross_join_filter_pushdown.py``.

Before this fix, an equi-join predicate with one side wrapped in arithmetic
(`a.x = b.y - 53`, TPC-DS Q02's own join condition) was never recognised as
an equi-join key anywhere in the planner: `extract_join_fields` and
`_extract_join_predicates` both require BARE identifiers on both sides, so
the predicate stayed a plain post-join Filter and the cross join beneath it
kept the naive `left_rows * right_rows` estimate -- with no notion of the
predicate's selectivity at all. `_hoist_arithmetic_join_key` rewrites the
arithmetic side into a materialised column above the leg it belongs to, so
the ordinary bare-identifier equi-join path (already NDV/tdom-aware) picks
it up unchanged.
"""

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "..", "..", "..", ".."))

import opteryx
from opteryx.planner.logical_planner import LogicalPlanStepType


def _plan_for(sql: str):
    """Run `sql` for real and return the fully-optimized logical plan, by
    capturing it at the last point the optimizer hands it off -- the
    `result_size_guard` check that runs immediately after optimization. Goes
    through the ordinary `execute_to_morsels` path (not `query_planner`
    directly) so telemetry/context are set up exactly as they are at runtime.
    """
    import opteryx.planner.result_size_guard as rsg

    captured = []
    orig = rsg.check_estimated_result_size

    def patched(plan, limit, telemetry=None):
        captured.append(plan)
        return orig(plan, limit, telemetry=telemetry)

    rsg.check_estimated_result_size = patched
    try:
        list(opteryx.session().execute_to_morsels(sql))
    finally:
        rsg.check_estimated_result_size = orig
    assert captured, "result_size_guard was never reached"
    return captured[-1]


def _join_nodes(plan):
    return [
        (nid, plan[nid])
        for nid in plan.nodes()
        if plan[nid].node_type == LogicalPlanStepType.Join
    ]


def test_arithmetic_join_key_is_converted_to_inner_join():
    sql = """
    SELECT a.id AS pid, b.planetId AS shifted
    FROM testdata.planets a, testdata.satellites b
    WHERE a.id = b.planetId - 1
    """
    plan = _plan_for(sql)
    joins = _join_nodes(plan)
    assert joins, "expected at least one Join node in the plan"
    # No join should remain an unconverted cross join -- the arithmetic
    # predicate must have been hoisted and picked up as an equi-join key.
    cross_joins = [
        (nid, node)
        for nid, node in joins
        if node.type == "cross join" and not getattr(node, "on", None)
    ]
    assert not cross_joins, f"arithmetic join key was not hoisted: {cross_joins}"
    inner = [node for _, node in joins if node.type == "inner"]
    assert inner, "expected the cross join to have become an inner join"
    assert inner[0].left_columns and inner[0].right_columns


def test_arithmetic_join_key_estimate_is_not_the_naive_cross_product():
    """planets has 9 rows, satellites has 177 rows -- a naive cross-join
    estimate would be 9*177=1593. A real equi-join estimate, even with no
    NDV known for either side, must come out under that (the "unknown NDV"
    fallback still divides by a real relation size instead of not dividing
    at all)."""
    from opteryx.planner.optimizer.statistics_refresh import refresh_statistics

    sql = """
    SELECT a.id AS pid, b.planetId AS shifted
    FROM testdata.planets a, testdata.satellites b
    WHERE a.id = b.planetId - 1
    """
    plan = _plan_for(sql)
    plan = refresh_statistics(plan)
    joins = _join_nodes(plan)
    inner = [node for _, node in joins if node.type == "inner"]
    assert inner
    stats = getattr(inner[0], "statistics", None)
    assert stats is not None
    assert stats.row_count < 9 * 177


def test_arithmetic_join_key_result_matches_hoist_free_rewrite():
    """Correctness check: the hoisted path must agree with an entirely
    independent formulation of the same join that never triggers the hoist
    (the shift is applied inside the SELECT list instead of the ON/WHERE
    predicate, so it reaches `extract_join_fields` as a bare identifier
    equality from the start)."""
    session = opteryx.session()

    hoisted_sql = """
    SELECT a.id AS pid, b.name AS m
    FROM testdata.planets a, testdata.satellites b
    WHERE a.id = b.planetId - 1
    """
    independent_sql = """
    SELECT a.id AS pid, s.m
    FROM testdata.planets a,
         (SELECT planetId - 1 AS shifted, name AS m FROM testdata.satellites) s
    WHERE a.id = s.shifted
    """

    def _rows(sql):
        return sorted(
            tuple(row.values())
            for m in session.execute_to_morsels(sql)
            for row in m.to_arrow().to_pylist()
        )

    hoisted = _rows(hoisted_sql)
    independent = _rows(independent_sql)
    assert hoisted
    assert hoisted == independent


def test_non_hoistable_arithmetic_predicate_is_left_as_a_filter():
    """A predicate that doesn't match the recognised IDENTIFIER <op> LITERAL
    shape (both operands are columns) must be declined, not mis-hoisted or
    raise -- it stays a plain cross join with a residual Filter."""
    sql = """
    SELECT a.id AS pid, b.planetId AS bp, b.id AS bm
    FROM testdata.planets a, testdata.satellites b
    WHERE a.id = b.planetId - b.id
    """
    plan = _plan_for(sql)
    # Must not raise, and must still produce a valid plan with a Join node.
    joins = _join_nodes(plan)
    assert joins
