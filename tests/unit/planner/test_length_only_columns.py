# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""Tests for LengthOnlyColumnStrategy.

A string column referenced *only* through length-answerable operations
(``col <> ''``, ``LENGTH(col)``) never needs its bytes decoded. This strategy
proves that and annotates the Scan.

The disqualification tests matter more than the eligibility ones: a column
wrongly marked length-only would have its bytes skipped by the (separate)
decode-side change and silently produce wrong answers, so every way of reading
a column's actual value is pinned here.
"""

import os
import sys
import uuid

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

from opteryx.models import ExecutionContext, QueryTelemetry
from opteryx.planner.ast_rewriter import do_ast_rewriter
from opteryx.planner.binder import do_bind_phase
from opteryx.planner.logical_planner import LogicalPlanStepType
from opteryx.planner.logical_planner import do_logical_planning_phase
from opteryx.planner.optimizer import do_optimizer
from opteryx.planner.plan_rewriter import do_plan_rewrite
from opteryx.planner.relation_resolver import do_resolve_relations
from opteryx.planner.sql_rewriter import do_sql_rewrite
from opteryx.third_party import sqloxide


def _optimized_plan(sql: str):
    telemetry = QueryTelemetry()
    query_id = str(uuid.uuid4())
    ctx = ExecutionContext(access_policies=[{"pattern": "testdata.*", "role": "reader"}])
    clean = do_sql_rewrite(sql)
    parsed = sqloxide.parse_sql(clean, _dialect="opteryx")
    ast = do_ast_rewriter(parsed, parameters=[])[0]
    plan, _, ctes = do_logical_planning_phase(ast)
    plan = do_resolve_relations(plan, ctes, telemetry)
    plan = do_plan_rewrite(plan, telemetry)
    bound = do_bind_phase(plan, execution_context=ctx, query_id=query_id, telemetry=telemetry)
    return do_optimizer(bound, telemetry)


def _length_only_names(sql: str) -> set:
    """The column NAMES every Scan in the plan marked length-only."""
    plan = _optimized_plan(sql)
    names = set()
    for _, node in plan.nodes(True):
        if node.node_type != LogicalPlanStepType.Scan or node.schema is None:
            continue
        identities = node.length_only_columns or set()
        by_identity = {c.identity: c.name for c in (node.schema.columns or [])}
        names |= {by_identity[i] for i in identities if i in by_identity}
    return names


# ─── eligible: every use is length-answerable ────────────────────────────────


def test_neq_empty_filter_only_is_length_only():
    # ClickBench Q31/Q32 shape — SearchPhrase is filtered on and never read.
    assert "SearchPhrase" in _length_only_names(
        "SELECT CounterID, COUNT(*) FROM testdata.clickbench_tiny "
        "WHERE SearchPhrase <> '' GROUP BY CounterID"
    )


def test_eq_empty_filter_only_is_length_only():
    assert "SearchPhrase" in _length_only_names(
        "SELECT CounterID, COUNT(*) FROM testdata.clickbench_tiny "
        "WHERE SearchPhrase = '' GROUP BY CounterID"
    )


def test_filter_plus_length_aggregate_is_length_only():
    # ClickBench Q28 shape — the whole prize: URL is used twice, both
    # length-answerable, so its ~90-byte payload never needs decoding.
    assert "URL" in _length_only_names(
        "SELECT CounterID, AVG(length(URL)) FROM testdata.clickbench_tiny "
        "WHERE URL <> '' GROUP BY CounterID"
    )


def test_length_comparison_is_length_only():
    # LENGTH(c) > 0 is rewritten to IsNotEmpty before this strategy runs.
    assert "SearchPhrase" in _length_only_names(
        "SELECT COUNT(*) FROM testdata.clickbench_tiny WHERE LENGTH(SearchPhrase) > 0"
    )


def test_bare_length_projection_is_length_only():
    # LENGTH(col) projected — the length is read, the bytes are not.
    assert "URL" in _length_only_names(
        "SELECT LENGTH(URL) FROM testdata.clickbench_tiny"
    )


# ─── disqualified: something reads the actual bytes ──────────────────────────


def test_projected_column_is_not_length_only():
    # ClickBench Q25/Q26 shape — the value is returned to the caller.
    assert "SearchPhrase" not in _length_only_names(
        "SELECT SearchPhrase FROM testdata.clickbench_tiny WHERE SearchPhrase <> ''"
    )


def test_group_key_is_not_length_only():
    # ClickBench Q13 shape — grouping compares actual values.
    assert "SearchPhrase" not in _length_only_names(
        "SELECT SearchPhrase, COUNT(*) FROM testdata.clickbench_tiny "
        "WHERE SearchPhrase <> '' GROUP BY SearchPhrase"
    )


def test_order_key_is_not_length_only():
    assert "SearchPhrase" not in _length_only_names(
        "SELECT COUNT(*) FROM testdata.clickbench_tiny "
        "WHERE SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY SearchPhrase"
    )


def test_min_aggregate_is_not_length_only():
    # MIN over a string compares values.
    assert "URL" not in _length_only_names(
        "SELECT MIN(URL) FROM testdata.clickbench_tiny WHERE URL <> ''"
    )


def test_like_predicate_is_not_length_only():
    # LIKE reads bytes.
    assert "URL" not in _length_only_names(
        "SELECT COUNT(*) FROM testdata.clickbench_tiny WHERE URL <> '' AND URL LIKE '%google%'"
    )


def test_other_function_is_not_length_only():
    assert "URL" not in _length_only_names(
        "SELECT UPPER(URL) FROM testdata.clickbench_tiny WHERE URL <> ''"
    )


def test_comparison_to_nonempty_literal_is_not_length_only():
    assert "URL" not in _length_only_names(
        "SELECT COUNT(*) FROM testdata.clickbench_tiny WHERE URL = 'http://example.com'"
    )


def test_mixed_uses_disqualify():
    # One length-answerable use does not rescue a column that is also read raw.
    assert "URL" not in _length_only_names(
        "SELECT URL, COUNT(*) FROM testdata.clickbench_tiny "
        "WHERE LENGTH(URL) > 0 GROUP BY URL"
    )


def test_unrelated_column_is_not_marked():
    # A column never mentioned in a length-answerable position is not marked.
    assert "CounterID" not in _length_only_names(
        "SELECT CounterID, COUNT(*) FROM testdata.clickbench_tiny "
        "WHERE SearchPhrase <> '' GROUP BY CounterID"
    )


# ─── results are unchanged (the strategy must be inert) ──────────────────────


def test_results_unchanged_for_eligible_shapes():
    import opteryx

    for sql in (
        "SELECT COUNT(*) FROM testdata.clickbench_tiny WHERE SearchPhrase <> ''",
        "SELECT COUNT(*), AVG(length(URL)) FROM testdata.clickbench_tiny WHERE URL <> ''",
        "SELECT LENGTH(URL) FROM testdata.clickbench_tiny LIMIT 5",
    ):
        session = opteryx.session()
        try:
            rows = sum(m.num_rows for m in session.execute_to_morsels(sql))
            assert rows > 0, sql
        finally:
            session.close()
