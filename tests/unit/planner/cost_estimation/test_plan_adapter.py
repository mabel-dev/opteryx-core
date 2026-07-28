# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Tests for the LogicalPlan -> JoinGraph adapter.

The adapter consumes a chain shape produced by ``cross_join_chain_reorder``'s
helpers. We capture its inputs at runtime by intercepting the call inside the
strategy, then assert on the resulting graph. End-to-end driving keeps the
test honest — hand-built mock plans drift from binder output.

Adapter requires manifests on Scan nodes, so tests use real parquet datasets
in ``testdata/``. ``READ_JSONL(...)`` relations are ``FunctionDataset`` nodes,
not ``Scan`` nodes, so the adapter never finds a manifest for them — that's
the one reachable "no stats" case (every virtual dataset like ``$planets``
now carries an explicit ``row_count_metric``/``row_count_estimate``, so none
of them exercise this branch any more).
"""

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "..", "..", "..", ".."))

import opteryx
from opteryx.config import features
from opteryx.expression import NodeType
from opteryx.planner.cost_estimation import plan_adapter
from opteryx.planner.cost_estimation.join_graph import JoinGraph


class _Flag:
    def __init__(self, value):
        self.value = value
        self.prev = None

    def __enter__(self):
        self.prev = features.enable_dpccp_join_planning
        features.enable_dpccp_join_planning = self.value

    def __exit__(self, *a):
        features.enable_dpccp_join_planning = self.prev


def _capture_graphs(sql):
    """Run SQL with the strategy on; return every JoinGraph the adapter built."""
    captured = []
    real_build = plan_adapter.build_join_graph

    def wrapped(plan, leaves, predicates):
        graph = real_build(plan, leaves, predicates)
        captured.append(graph)
        return graph

    from opteryx.planner.optimizer.strategies import join_planning

    plan_adapter.build_join_graph = wrapped
    join_planning.build_join_graph = wrapped
    try:
        with _Flag(True):
            list(opteryx.session().execute_to_morsels(sql))
    finally:
        plan_adapter.build_join_graph = real_build
        join_planning.build_join_graph = real_build
    return captured


def test_three_relation_chain_produces_connected_graph():
    sql = """
    SELECT a.id AS ai, b.id AS bi, c.id AS ci
    FROM testdata.satellites a, testdata.satellites b, testdata.satellites c
    WHERE a.id = b.id AND b.id = c.id LIMIT 1
    """
    graphs = [g for g in _capture_graphs(sql) if g is not None]
    assert graphs, "adapter should have built at least one graph"
    g = graphs[0]
    assert isinstance(g, JoinGraph)
    assert g.n == 3
    assert g.is_connected(g.full_mask)
    assert len(g.edges) == 2


def test_disconnected_predicates_yield_none():
    """Two pairs with no bridging predicate cannot be planned by DPccp."""
    sql = """
    SELECT a.id AS ai, b.id AS bi, c.id AS ci, d.id AS di
    FROM testdata.satellites a, testdata.satellites b,
         testdata.satellites c, testdata.satellites d
    WHERE a.id = b.id AND c.id = d.id LIMIT 1
    """
    graphs = _capture_graphs(sql)
    assert graphs, "adapter should have been called"
    assert any(g is None for g in graphs)


def test_edge_payload_preserves_predicate():
    sql = """
    SELECT a.id AS ai, b.id AS bi, c.id AS ci
    FROM testdata.satellites a, testdata.satellites b, testdata.satellites c
    WHERE a.id = b.id AND b.id = c.id LIMIT 1
    """
    graphs = [g for g in _capture_graphs(sql) if g is not None]
    assert graphs
    for edge in graphs[0].edges:
        assert edge.payload is not None
        assert edge.payload.node_type == NodeType.COMPARISON_OPERATOR
        assert edge.payload.value == "Eq"


def test_vertex_row_count_uses_manifest():
    """Row counts should come from the scan manifest, not be defaulted."""
    sql = """
    SELECT a.id AS ai, b.id AS bi, c.id AS ci
    FROM testdata.satellites a, testdata.satellites b, testdata.satellites c
    WHERE a.id = b.id AND b.id = c.id LIMIT 1
    """
    graphs = [g for g in _capture_graphs(sql) if g is not None]
    assert graphs
    # satellites has 177 rows; every leaf reflects that.
    for v in graphs[0].vertices:
        assert v.row_count == 177


def test_no_manifest_returns_none():
    """READ_JSONL relations are FunctionDataset nodes, not Scan nodes; the
    adapter can't find a manifest for them, so it bails."""
    sql = """
    SELECT a.id AS ai, b.id AS bi, c.id AS ci
    FROM READ_JSONL('testdata/jsonl_perf/data.jsonl') AS a,
         READ_JSONL('testdata/jsonl_perf/data.jsonl') AS b,
         READ_JSONL('testdata/jsonl_perf/data.jsonl') AS c
    WHERE a.id = b.id AND b.id = c.id LIMIT 1
    """
    graphs = _capture_graphs(sql)
    assert graphs, "adapter should have been called"
    assert all(g is None for g in graphs)
