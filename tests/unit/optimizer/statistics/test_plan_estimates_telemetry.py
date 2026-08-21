# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""Planner estimates surfaced as telemetry, for diagnosing performance issues.

``refresh_statistics`` already computes a row-count estimate per plan node,
plus per-conjunct selectivity while folding predicates and per-key inputs
while sizing a join -- all of it was discarded once it had served its
in-the-moment purpose. This records it onto ``telemetry._reading`` instead:

  * ``estimated_row_counts`` -- one entry per node: nid, node_type, relation
    (Scan only), row_count. Lets estimate be compared against actual delivered
    rows to catch a bad estimate before it causes a false ResultTooLargeError
    rejection (or a bad join order, or...).
  * ``predicate_estimates`` -- one entry per predicate actually estimated
    (both pushed-to-Scan and surviving-Filter-node shapes): condition text,
    selectivity, and relative per-row cost (from cost_estimation.predicate_cost,
    the same model PredicateOrderingStrategy uses, not a duplicate).
  * ``join_estimates`` -- one entry per Join node: join_type, left/right row
    counts, key_count, output row_count.

Diagnostic only -- never consulted by planning itself, and omitting the new
``telemetry`` parameter (the default) reproduces the exact prior behaviour.
"""

import os
import sys
import uuid

import pytest

sys.path.insert(1, os.path.join(sys.path[0], "../../../.."))

import opteryx


def _build_optimized_and_refreshed_plan_with_telemetry(sql):
    from opteryx.models import ExecutionContext, QueryTelemetry
    from opteryx.planner.ast_rewriter import do_ast_rewriter
    from opteryx.planner.binder import do_bind_phase
    from opteryx.planner.logical_planner import do_logical_planning_phase
    from opteryx.planner.optimizer import do_optimizer
    from opteryx.planner.optimizer.statistics_refresh import refresh_statistics
    from opteryx.planner.plan_rewriter import do_plan_rewrite
    from opteryx.planner.relation_resolver import do_resolve_relations
    from opteryx.planner.sql_rewriter import do_sql_rewrite
    from opteryx.third_party import sqloxide

    telemetry = QueryTelemetry.detached()
    query_id = str(uuid.uuid4())
    ctx = ExecutionContext(access_policies=[{"pattern": "testdata.*", "role": "reader"}])

    clean = do_sql_rewrite(sql)
    parsed = sqloxide.parse_sql(clean, _dialect="opteryx")
    ast = do_ast_rewriter(parsed, parameters=[])[0]
    plan, _, ctes = do_logical_planning_phase(ast)
    plan = do_resolve_relations(plan, ctes, telemetry)
    plan = do_plan_rewrite(plan, telemetry)
    bound = do_bind_phase(plan, execution_context=ctx, query_id=query_id, telemetry=telemetry)
    optimized = do_optimizer(bound, telemetry)
    refresh_statistics(optimized, telemetry=telemetry)
    return telemetry


# ── omitting telemetry reproduces prior behaviour exactly ───────────────────────


@pytest.mark.skipif(
    not os.path.isdir("testdata/tpch_001"),
    reason="testdata/tpch_001 not populated",
)
def test_omitting_telemetry_does_not_change_the_computed_statistics():
    """The default (telemetry=None) path must be untouched by this feature."""
    from opteryx.planner.ast_rewriter import do_ast_rewriter
    from opteryx.planner.binder import do_bind_phase
    from opteryx.planner.logical_planner import do_logical_planning_phase
    from opteryx.planner.logical_planner import LogicalPlanStepType
    from opteryx.planner.optimizer import do_optimizer
    from opteryx.planner.optimizer.statistics_refresh import refresh_statistics
    from opteryx.planner.plan_rewriter import do_plan_rewrite
    from opteryx.planner.relation_resolver import do_resolve_relations
    from opteryx.planner.sql_rewriter import do_sql_rewrite
    from opteryx.models import ExecutionContext, QueryTelemetry
    from opteryx.third_party import sqloxide

    sql = "SELECT n_name FROM testdata.tpch_001.nation WHERE n_name = 'BRAZIL'"
    telemetry = QueryTelemetry.detached()
    ctx = ExecutionContext(access_policies=[{"pattern": "testdata.*", "role": "reader"}])
    clean = do_sql_rewrite(sql)
    parsed = sqloxide.parse_sql(clean, _dialect="opteryx")
    ast = do_ast_rewriter(parsed, parameters=[])[0]
    plan, _, ctes = do_logical_planning_phase(ast)
    plan = do_resolve_relations(plan, ctes, telemetry)
    plan = do_plan_rewrite(plan, telemetry)
    bound = do_bind_phase(plan, execution_context=ctx, query_id=str(uuid.uuid4()), telemetry=telemetry)
    optimized = do_optimizer(bound, telemetry)
    refresh_statistics(optimized)  # no telemetry argument at all

    for _nid, node in optimized.nodes(True):
        if node.node_type == LogicalPlanStepType.Scan:
            assert node.statistics.row_count == 2
    assert telemetry._reading.get("estimated_row_counts", 0) == 0, (
        "estimated_row_counts must not appear when telemetry isn't passed to refresh_statistics"
    )


# ── row-count estimates ──────────────────────────────────────────────────────────


@pytest.mark.skipif(
    not os.path.isdir("testdata/tpch_001"),
    reason="testdata/tpch_001 not populated",
)
def test_estimated_row_counts_has_one_entry_per_node():
    telemetry = _build_optimized_and_refreshed_plan_with_telemetry(
        "SELECT n_name FROM testdata.tpch_001.nation WHERE n_name = 'BRAZIL'"
    )
    entries = telemetry._reading["estimated_row_counts"]
    node_types = {e["node_type"] for e in entries}
    assert "Scan" in node_types
    assert "Exit" in node_types
    scan_entry = next(e for e in entries if e["node_type"] == "Scan")
    assert scan_entry["relation"] == "testdata.tpch_001.nation"
    assert scan_entry["row_count"] < 25  # the selective predicate must have reduced it


# ── predicate selectivity + cost estimates ───────────────────────────────────────


@pytest.mark.skipif(
    not os.path.isdir("testdata/tpch_001"),
    reason="testdata/tpch_001 not populated",
)
def test_predicate_estimates_estimator_tag_is_none_for_non_like_predicates():
    telemetry = _build_optimized_and_refreshed_plan_with_telemetry(
        "SELECT n_name FROM testdata.tpch_001.nation WHERE n_name = 'BRAZIL'"
    )
    entries = telemetry._reading["predicate_estimates"]
    assert len(entries) == 1
    assert entries[0]["estimator"] is None  # Eq, not an infix LIKE — no tier to report


@pytest.mark.skipif(
    not os.path.isdir("testdata/tpch_001"),
    reason="testdata/tpch_001 not populated",
)
def test_predicate_estimates_estimator_tag_is_flat_fallback_without_analyze():
    # No ANALYZE has run against tpch_001.nation -> the column has no
    # char-class stats -> _selectivity_instr falls through to the flat
    # constant, and the telemetry tag must say so explicitly.
    telemetry = _build_optimized_and_refreshed_plan_with_telemetry(
        "SELECT * FROM testdata.tpch_001.nation WHERE n_comment LIKE '%slyly%'"
    )
    entries = telemetry._reading["predicate_estimates"]
    assert len(entries) == 1
    assert entries[0]["estimator"] == "flat_fallback"
    assert entries[0]["selectivity"] == pytest.approx(0.1)


@pytest.mark.skipif(
    not os.path.isdir("testdata/tpch_001"),
    reason="testdata/tpch_001 not populated",
)
def test_predicate_estimates_estimator_tag_is_char_class_decay_after_analyze():
    import glob

    from opteryx.models.manifest_io import DATASET_MANIFEST_NAME

    manifest_glob = f"testdata/tpch_001/nation/{DATASET_MANIFEST_NAME}"
    for p in glob.glob(manifest_glob):
        os.remove(p)
    try:
        for _ in opteryx.session().execute_to_morsels(
            "ANALYZE TABLE testdata.tpch_001.nation FOR COLUMNS n_comment"
        ):
            pass

        telemetry = _build_optimized_and_refreshed_plan_with_telemetry(
            "SELECT * FROM testdata.tpch_001.nation WHERE n_comment LIKE '%slyly%'"
        )
        entries = telemetry._reading["predicate_estimates"]
        assert len(entries) == 1
        assert entries[0]["estimator"] == "char_class_decay"
        # A 6-character needle against real char-class stats must differ
        # from the flat 10% baseline -- otherwise the estimator isn't
        # actually contributing anything over the constant it replaces.
        assert entries[0]["selectivity"] != pytest.approx(0.1)
    finally:
        for p in glob.glob(manifest_glob):
            os.remove(p)


@pytest.mark.skipif(
    not os.path.isdir("testdata/tpch_001"),
    reason="testdata/tpch_001 not populated",
)
def test_predicate_estimates_records_selectivity_and_cost():
    telemetry = _build_optimized_and_refreshed_plan_with_telemetry(
        "SELECT n_name FROM testdata.tpch_001.nation WHERE n_name = 'BRAZIL'"
    )
    entries = telemetry._reading["predicate_estimates"]
    assert len(entries) == 1
    entry = entries[0]
    assert "BRAZIL" in entry["condition"]
    assert 0.0 < entry["selectivity"] <= 1.0
    assert entry["cost"] > 0.0  # VARCHAR comparison cost from predicate_cost.py


@pytest.mark.skipif(
    not os.path.isdir("testdata/tpch_001"),
    reason="testdata/tpch_001 not populated",
)
def test_predicate_estimates_covers_both_pushed_and_filter_node_shapes():
    # A single-relation predicate reaches Scan.predicates (pushed); confirm
    # the telemetry capture fires on that path, not only surviving Filter nodes.
    telemetry = _build_optimized_and_refreshed_plan_with_telemetry(
        "SELECT * FROM testdata.tpch_001.lineitem WHERE l_shipdate < l_commitdate"
    )
    entries = telemetry._reading["predicate_estimates"]
    assert len(entries) == 1
    assert entries[0]["node_type"] == "Scan"
    assert entries[0]["selectivity"] == pytest.approx(1.0 / 3.0, rel=0.02)


# ── join estimates ────────────────────────────────────────────────────────────────


@pytest.mark.skipif(
    not os.path.isdir("testdata/tpch_001"),
    reason="testdata/tpch_001 not populated",
)
def test_join_estimates_records_composite_key_inputs():
    telemetry = _build_optimized_and_refreshed_plan_with_telemetry(
        """
        SELECT a.n_nationkey FROM testdata.tpch_001.nation a
        JOIN testdata.tpch_001.nation b
          ON a.n_nationkey = b.n_nationkey AND a.n_regionkey = b.n_regionkey
        """
    )
    entries = telemetry._reading["join_estimates"]
    assert len(entries) == 1
    entry = entries[0]
    assert entry["join_type"] == "inner"
    assert entry["left_row_count"] == 25
    assert entry["right_row_count"] == 25
    assert entry["key_count"] == 2  # both key columns, not just the first (see the composite-key fix)
    assert entry["row_count"] >= 1


# ── reachable through the public API, not just the internal helper ──────────────


@pytest.mark.skipif(
    not os.path.isdir("testdata/tpch_001"),
    reason="testdata/tpch_001 not populated",
)
def test_reachable_via_session_telemetry_property():
    session = opteryx.session()
    for _m in session.execute_to_morsels(
        "SELECT n_name FROM testdata.tpch_001.nation WHERE n_name = 'BRAZIL'"
    ):
        pass
    t = session.telemetry
    assert "estimated_row_counts" in t
    assert any(e["node_type"] == "Scan" for e in t["estimated_row_counts"])
    assert "predicate_estimates" in t
    assert len(t["predicate_estimates"]) == 1


if __name__ == "__main__":  # pragma: no cover
    pytest.main([__file__, "-v"])
