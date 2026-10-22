# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""The cheap `should_i_run` gates of VectorSearch, DecorrelateSubquery, TimestampCastSink
and LengthOnlyColumn.

Both gates used to walk the expressions of every node to learn that a plan has nothing
for the strategy. They now ask a narrower question that is still a NECESSARY condition
for the strategy to change anything:

* VectorSearch acts only on a HeapSort carrying `drops_unsearchable`.
* DecorrelateSubquery acts only on a plan with a SUBQUERY node in some step's
  expressions (it then runs its original precise search, unchanged).
* TimestampCastSink acts only if a pure-retag CAST is reachable from the roots it
  classifies; LengthOnlyColumn only if an IsEmpty/IsNotEmpty/LENGTH-family node is
  reachable from the roots it classifies. Each gate reads the same roots its `complete`
  reads (one shared helper), so it cannot see less than `complete` does.

What must not change is every plan the optimizer produces. Each test below captures the
plan a gate is asked about and checks the gate against the definition it replaced, then
checks the optimized plan equals the one the replaced gate produced.

Run as a script (CLAUDE.md §10) or under pytest.
"""

import os
import re
import sys

sys.path.insert(1, os.path.join(os.path.dirname(__file__), "..", "..", ".."))

import pytest

import opteryx
from opteryx.models import ExecutionContext, QueryTelemetry
from opteryx.planner.ast_rewriter import do_ast_rewriter
from opteryx.planner.binder import do_bind_phase
from opteryx.planner.logical_planner import do_logical_planning_phase
from opteryx.planner.optimizer import do_optimizer
from opteryx.planner.plan_context import PlanContext
from opteryx.planner.plan_rewriter import do_plan_rewrite
from opteryx.planner.relation_resolver import do_resolve_relations
from opteryx.planner.sql_rewriter import do_sql_rewrite
from opteryx.third_party import sqloxide
import uuid
from opteryx.expression import NodeType
from opteryx.expression import get_all_nodes_of_type
from opteryx.planner.logical_planner import LogicalPlanStepType
from opteryx.planner.optimizer.strategies import DecorrelateSubqueryStrategy
from opteryx.planner.optimizer.strategies import LengthOnlyColumnStrategy
from opteryx.planner.optimizer.strategies import TimestampCastSinkStrategy
from opteryx.planner.optimizer.strategies import VectorSearchStrategy
from opteryx.planner.optimizer.strategies import decorrelate_subquery as decorrelate_module

MOONS = "testdata.satellites"

# Plans with no subquery, every kind of subquery the strategy handles, subqueries it
# leaves alone or refuses, and plans that mention COSINE_DISTANCE in places a vector
# search cannot use.
NO_SUBQUERY = [
    "SELECT * FROM $planets",
    "SELECT name, id FROM $planets WHERE id > 3 ORDER BY name LIMIT 4",
    f"SELECT p.name FROM $planets AS p INNER JOIN {MOONS} AS s ON s.planetId = p.id",
    "SELECT COUNT(*), gravity FROM $planets GROUP BY gravity HAVING COUNT(*) > 1",
    "SELECT name FROM $planets WHERE COSINE_DISTANCE(name, 'earth') < 1",
    "SELECT name, COSINE_DISTANCE(name, 'earth') AS d FROM $planets",
]
SUBQUERIES = [
    f"SELECT name FROM $planets WHERE id IN (SELECT planetId FROM {MOONS})",
    f"SELECT name FROM $planets WHERE id NOT IN (SELECT planetId FROM {MOONS})",
    f"SELECT name FROM $planets AS p WHERE EXISTS (SELECT 1 FROM {MOONS} AS s WHERE s.planetId = p.id)",
    f"SELECT name FROM $planets AS p WHERE NOT EXISTS (SELECT 1 FROM {MOONS} AS s WHERE s.planetId = p.id)",
    f"SELECT name FROM $planets WHERE id > (SELECT AVG(planetId) FROM {MOONS})",
    f"SELECT name, (SELECT MAX(id) FROM {MOONS}) AS m FROM $planets",
    f"SELECT gravity, COUNT(*) FROM $planets GROUP BY gravity HAVING COUNT(*) > (SELECT COUNT(*) FROM {MOONS}) / 100",
    f"WITH c AS (SELECT planetId FROM {MOONS}) SELECT name FROM $planets WHERE id IN (SELECT planetId FROM c)",
    f"SELECT SUM(CASE WHEN id > (SELECT AVG(id) FROM {MOONS}) THEN 1 ELSE 0 END) FROM $planets",
]
VECTOR = [
    "SELECT name FROM $planets ORDER BY COSINE_DISTANCE(name, 'earth') LIMIT 3",
    "SELECT name FROM $planets ORDER BY COSINE_DISTANCE(name, 'earth')",
    "SELECT name FROM $planets WHERE id > 2 ORDER BY COSINE_DISTANCE(name, 'earth') LIMIT 3",
    "SELECT name FROM $planets ORDER BY name LIMIT 3",
]
ALL_SQL = NO_SUBQUERY + SUBQUERIES + VECTOR


def _decorrelate_gate_before(plan):
    """should_i_run as it was before the pre-check."""
    return any(
        (node.node_type == LogicalPlanStepType.Filter and decorrelate_module._has_work(node.condition))
        or decorrelate_module._node_has_subquery(node)
        or decorrelate_module._node_has_existence(node)
        for _, node in plan.nodes(True)
    )


def _vector_gate_before(plan):
    """should_i_run as it was before: a COSINE_DISTANCE call anywhere in the plan."""
    for _, node in plan.nodes(True):
        for call in get_all_nodes_of_type(list(node.expressions()), (NodeType.FUNCTION,)):
            if str(call.value).upper() == "COSINE_DISTANCE":
                return True
    return False


def _explain(sql):
    """The optimized plan as text, without the OPTIMIZATIONS block (which lists the
    strategies that ran) and with the per-query random relation suffixes masked."""
    session = opteryx.session()
    lines = []
    for morsel in session.execute_to_morsels("EXPLAIN " + sql):
        morsel.materialize()
        for name in morsel.column_names:
            lines.extend(str(value) for value in morsel.column(name).to_pylist())
    session.close()
    text = "\n".join(lines)
    text = text.split("OPTIMIZATIONS")[0]
    return re.sub(r"(\$[A-Za-z_]+[-_])[A-Za-z0-9]{3,8}", r"\1X", text)


def _capture(monkeypatch, strategy, reference):
    """Record, for every plan `strategy.should_i_run` is asked about, the gate's answer
    and the answer of `reference` on that same plan."""
    seen = []
    gate = strategy.should_i_run

    def wrapped(self, plan):
        answer = gate(self, plan)
        seen.append((answer, reference(plan)))
        return answer

    monkeypatch.setattr(strategy, "should_i_run", wrapped)
    return seen


def _explain_with_gate(monkeypatch, strategy, reference, sql):
    """The plan text for `sql` when the strategy is gated by the replaced definition."""
    monkeypatch.setattr(strategy, "should_i_run", lambda self, plan: reference(plan))
    return _explain(sql)


@pytest.mark.parametrize("sql", ALL_SQL)
def test_decorrelate_gate_matches_the_gate_it_replaced(monkeypatch, sql):
    seen = _capture(monkeypatch, DecorrelateSubqueryStrategy, _decorrelate_gate_before)
    _explain(sql)
    assert seen, "the strategy was never asked"
    for answer, before in seen:
        assert answer == before, sql


def test_decorrelate_gate_is_true_exactly_for_plans_with_work(monkeypatch):
    seen = _capture(monkeypatch, DecorrelateSubqueryStrategy, _decorrelate_gate_before)
    for sql in NO_SUBQUERY:
        seen.clear()
        _explain(sql)
        assert [answer for answer, _ in seen] == [False] * len(seen), sql
    for sql in SUBQUERIES:
        seen.clear()
        _explain(sql)
        assert any(answer for answer, _ in seen), sql


@pytest.mark.parametrize("sql", ALL_SQL)
def test_decorrelate_gate_leaves_the_plan_unchanged(monkeypatch, sql):
    now = _explain(sql)
    before = _explain_with_gate(monkeypatch, DecorrelateSubqueryStrategy, _decorrelate_gate_before, sql)
    assert now == before


def test_decorrelate_gate_still_lets_an_unsupported_subquery_be_refused():
    # `= ANY (SELECT ...)` is refused by the strategy's search; the pre-check must not
    # let a plan holding a subquery slip past that.
    with pytest.raises(Exception) as raised:
        _explain(f"SELECT name FROM $planets WHERE id = ANY (SELECT planetId FROM {MOONS})")
    assert "ANY" in str(raised.value)


@pytest.mark.parametrize("sql", ALL_SQL)
def test_vector_gate_never_runs_where_the_old_gate_did_not(monkeypatch, sql):
    seen = _capture(monkeypatch, VectorSearchStrategy, _vector_gate_before)
    _explain(sql)
    assert seen, "the strategy was never asked"
    for answer, before in seen:
        assert not answer or before, sql


def test_vector_gate_is_true_only_for_a_marked_heapsort(monkeypatch):
    seen = _capture(monkeypatch, VectorSearchStrategy, _vector_gate_before)

    def answers(sql):
        seen.clear()
        _explain(sql)
        return [answer for answer, _ in seen]

    # A COSINE_DISTANCE that is not a sort key gives the strategy nothing to do.
    assert answers("SELECT name FROM $planets WHERE COSINE_DISTANCE(name, 'earth') < 1") == [False]
    assert answers("SELECT name, COSINE_DISTANCE(name, 'earth') AS d FROM $planets") == [False]
    assert answers("SELECT name FROM $planets ORDER BY name LIMIT 3") == [False]
    # ... and a COSINE_DISTANCE-led ORDER BY ... LIMIT becomes the marked HeapSort.
    assert answers("SELECT name FROM $planets ORDER BY COSINE_DISTANCE(name, 'earth') LIMIT 3") == [True]


@pytest.mark.parametrize("sql", ALL_SQL)
def test_vector_gate_leaves_the_plan_unchanged(monkeypatch, sql):
    now = _explain(sql)
    before = _explain_with_gate(monkeypatch, VectorSearchStrategy, _vector_gate_before, sql)
    assert now == before


LINEITEM = "testdata.tpch_001.lineitem"

TIMESTAMP_SQL = [
    f"SELECT l_suppkey::TIMESTAMP[s] AS m FROM {LINEITEM}",
    f"SELECT l_suppkey::TIMESTAMP[ms] AS m FROM {LINEITEM}",
    f"SELECT l_suppkey, l_suppkey::TIMESTAMP[s] AS m FROM {LINEITEM}",
    f"SELECT l_suppkey::TIMESTAMP[s] AS m FROM {LINEITEM} WHERE l_suppkey > 5",
    f"SELECT l_suppkey::TIMESTAMP[s], l_suppkey::TIMESTAMP[ms] FROM {LINEITEM}",
    f"SELECT l_suppkey::TIMESTAMP[s] AS a, SUM(l_quantity) FROM {LINEITEM} GROUP BY l_suppkey::TIMESTAMP[s]",
    f"SELECT l_orderkey FROM {LINEITEM}",
    f"SELECT l_shipdate::TIMESTAMP FROM {LINEITEM}",
    "SELECT * FROM $planets",
]
LENGTH_SQL = [
    f"SELECT AVG(LENGTH(l_comment)) FROM {LINEITEM} WHERE l_comment <> ''",
    f"SELECT COUNT(*) FROM {LINEITEM} WHERE l_comment <> ''",
    f"SELECT LENGTH(l_comment), l_comment FROM {LINEITEM}",
    f"SELECT SUM(LENGTH(l_comment)) FROM {LINEITEM}",
    f"SELECT OCTET_LENGTH(l_comment) FROM {LINEITEM} WHERE l_shipmode = 'AIR'",
    f"SELECT l_comment FROM {LINEITEM}",
    f"SELECT SUM(LENGTH(t)) FROM (SELECT DISTINCT l_comment AS t FROM {LINEITEM})",
    f"SELECT l_orderkey FROM {LINEITEM}",
    "SELECT * FROM $planets",
]


def _optimized(sql):
    plan_context = PlanContext()
    telemetry = QueryTelemetry.detached()
    context = ExecutionContext(access_policies=[{"pattern": "testdata.*", "role": "reader"}])
    plan, _, ctes = do_logical_planning_phase(
        do_ast_rewriter(sqloxide.parse_sql(do_sql_rewrite(sql), _dialect="opteryx"), parameters=[])[0],
        plan_context=plan_context,
    )
    plan = do_resolve_relations(plan, ctes, telemetry, plan_context=plan_context)
    plan = do_plan_rewrite(plan, telemetry, plan_context=plan_context)
    bound = do_bind_phase(
        plan, execution_context=context, query_id=str(uuid.uuid4()), telemetry=telemetry, plan_context=plan_context
    )
    return do_optimizer(bound, telemetry, plan_context)


def _scan_outcome(sql):
    """What the two strategies did to each Scan: its columns' physical types and the
    names of the columns annotated length-only."""
    outcome = []
    for _, node in _optimized(sql).nodes(True):
        if node.node_type != LogicalPlanStepType.Scan:
            continue
        columns = [(c.name, str(c.column_type.physical)) for c in node.schema.columns]
        length_only = sorted(c.name for c in node.schema.columns if c.identity in (node.length_only_columns or ()))
        outcome.append((columns, length_only))
    return sorted(outcome, key=str)


@pytest.mark.parametrize("sql", TIMESTAMP_SQL)
def test_timestamp_cast_sink_gate_leaves_the_outcome_unchanged(monkeypatch, sql):
    now = _scan_outcome(sql)
    monkeypatch.setattr(TimestampCastSinkStrategy, "should_i_run", lambda self, plan: True)
    always_run = _scan_outcome(sql)
    assert now == always_run


@pytest.mark.parametrize("sql", LENGTH_SQL)
def test_length_only_gate_leaves_the_outcome_unchanged(monkeypatch, sql):
    now = _scan_outcome(sql)
    monkeypatch.setattr(LengthOnlyColumnStrategy, "should_i_run", lambda self, plan: True)
    always_run = _scan_outcome(sql)
    assert now == always_run


def test_the_two_gates_still_let_the_strategies_run_when_they_have_work(monkeypatch):
    # The guard against a gate that is simply always False: the unchanged outcomes above
    # would also hold if neither strategy ever did anything.
    retyped = _scan_outcome(f"SELECT l_suppkey::TIMESTAMP[s] AS m FROM {LINEITEM}")
    assert any("TIMESTAMP64" in physical for columns, _ in retyped for name, physical in columns if name == "l_suppkey")
    annotated = _scan_outcome(f"SELECT AVG(LENGTH(l_comment)) FROM {LINEITEM} WHERE l_comment <> ''")
    assert any("l_comment" in length_only for _, length_only in annotated)


def test_the_two_gates_decline_plans_with_nothing_to_do(monkeypatch):
    seen = {"timestamp": [], "length": []}
    timestamp_gate, length_gate = TimestampCastSinkStrategy.should_i_run, LengthOnlyColumnStrategy.should_i_run
    monkeypatch.setattr(
        TimestampCastSinkStrategy, "should_i_run", lambda self, plan: seen["timestamp"].append(timestamp_gate(self, plan)) or seen["timestamp"][-1]
    )
    monkeypatch.setattr(
        LengthOnlyColumnStrategy, "should_i_run", lambda self, plan: seen["length"].append(length_gate(self, plan)) or seen["length"][-1]
    )
    _optimized(f"SELECT l_orderkey FROM {LINEITEM}")
    assert seen == {"timestamp": [False], "length": [False]}
    seen["timestamp"].clear(); seen["length"].clear()
    _optimized(f"SELECT l_suppkey::TIMESTAMP[s] FROM {LINEITEM}")
    assert seen["timestamp"] == [True] and seen["length"] == [False]
    seen["timestamp"].clear(); seen["length"].clear()
    _optimized(f"SELECT COUNT(*) FROM {LINEITEM} WHERE l_comment <> ''")
    assert seen["timestamp"] == [False] and seen["length"] == [True]


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
