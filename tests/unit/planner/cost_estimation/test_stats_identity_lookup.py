# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Statistics reach the join enumerators through identity-keyed lookups.

``RelationStatistics.columns`` is keyed by ``SchemaColumn.identity`` — opaque
``bytes`` — by contract (see that class's docstring). ``plan_adapter`` resolves
that identity from each predicate identifier's bound ``schema_column``
(``_identifier_identity``) and uses it for BOTH statistics lookups
(``_key_stats``, ``_build_equiv_tdoms``) and the equivalence-class key space,
so NDV and null_fraction flow to the DPccp/greedy enumerators, and a
self-join's two sides — same name, distinct identities — keep separate stats.

This file began life as the Phase 0 xfail record of the str-name lookup defect
(lookups by name always missed, so the enumerators only ever saw the tdom
domain-size fallback); Phase 1 fixed it and flipped these to plain tests.
"""

import os
import sys
from types import SimpleNamespace

sys.path.insert(1, os.path.join(sys.path[0], "..", "..", "..", ".."))

import pytest

from opteryx.expression import NodeType
from opteryx.models import Node
from opteryx.planner.cost_estimation import plan_adapter
from opteryx.planner.cost_estimation.join_graph import JoinVertex
from opteryx.planner.logical_planner.logical_planner import LogicalPlan
from opteryx.planner.logical_planner.logical_planner import LogicalPlanNode
from opteryx.planner.logical_planner.logical_planner import LogicalPlanStepType
from opteryx.planner.optimizer.statistics import ColumnStatistics
from opteryx.planner.optimizer.statistics import RelationStatistics
from opteryx.types.schema import RelationSchema
from opteryx.types.schema import SchemaColumn
from opteryx.types.schema import mint_column_identity


def _make_scan(relation: str, row_count: int, column_specs: dict, alias: str = None):
    """A Scan LogicalPlanNode with schema + identity-keyed statistics attached.

    ``column_specs``: {name: (distinct_count, null_fraction)}. Returns
    (scan_node, {name: SchemaColumn}) so tests can build identifiers bound to
    the same columns the statistics are keyed by. ``alias`` lets a self-join
    scan the same relation twice under different names with freshly minted
    identities, exactly as the binder does.
    """
    alias = alias or relation
    columns = {
        name: SchemaColumn(name=name, identity=mint_column_identity(alias, name))
        for name in column_specs
    }
    schema = RelationSchema(
        name=alias,
        columns=list(columns.values()),
        row_count_metric=row_count,
    )
    stats = RelationStatistics(
        columns={
            columns[name].identity: ColumnStatistics(
                column_name=name,
                data_type="BIGINT",
                distinct_count=ndv,
                null_fraction=null_fraction,
            )
            for name, (ndv, null_fraction) in column_specs.items()
        },
        row_count_metric=row_count,
    )
    scan = LogicalPlanNode(
        node_type=LogicalPlanStepType.Scan,
        relation=relation,
        alias=alias,
        schema=schema,
        connector=None,
    )
    scan.statistics = stats
    return scan, columns


def _identifier(source: str, schema_column: SchemaColumn) -> Node:
    """A bound identifier — carries ``schema_column`` as the binder leaves it."""
    return Node(
        NodeType.IDENTIFIER,
        source=source,
        source_column=schema_column.name,
        value=schema_column.name,
        schema_column=schema_column,
    )


def _eq_predicate(left: Node, right: Node) -> Node:
    return Node(NodeType.COMPARISON_OPERATOR, value="Eq", left=left, right=right)


def test_stats_columns_are_keyed_by_bytes_identity_not_name():
    """The mechanism this file guards, stated plainly: the statistics dict keys
    are opaque bytes identities, so a lookup by the str column name can never
    hit — every lookup path must resolve the identity first."""
    scan, columns = _make_scan("orders", 1_500_000, {"o_orderkey": (1_500_000, 0.0)})
    for key in scan.statistics.columns:
        assert isinstance(key, bytes)
    assert scan.statistics.columns.get("o_orderkey") is None
    assert scan.statistics.columns.get(columns["o_orderkey"].identity) is not None


def test_identifier_identity_resolves_bound_column_and_refuses_names():
    scan, columns = _make_scan("orders", 1_500_000, {"o_custkey": (100_000, 0.02)})
    bound = _identifier("orders", columns["o_custkey"])
    assert plan_adapter._identifier_identity(bound) == columns["o_custkey"].identity
    # An unbound identifier resolves to None — never to its name.
    unbound = Node(
        NodeType.IDENTIFIER, source="orders", source_column="o_custkey", value="o_custkey"
    )
    assert plan_adapter._identifier_identity(unbound) is None


def test_key_stats_receives_ndv_and_null_fraction():
    scan, columns = _make_scan("orders", 1_500_000, {"o_custkey": (100_000, 0.02)})
    key_stats = plan_adapter._key_stats(scan, columns["o_custkey"].identity)
    assert key_stats.ndv == 100_000
    assert key_stats.null_fraction == 0.02


def test_build_equiv_tdoms_prefers_stats_ndv_over_domain_fallback():
    # Two relations joined on a key whose true NDV (100k) is far below the
    # domain fallback min(1.5M, 6M) = 1.5M. With the stats visible the tdom
    # must be the NDV; the fallback is only for when no NDV exists.
    orders, o_cols = _make_scan("orders", 1_500_000, {"o_custkey": (100_000, 0.0)})
    lineitem, l_cols = _make_scan("lineitem", 6_000_000, {"l_custkey": (100_000, 0.0)})

    o_key = (0, o_cols["o_custkey"].identity)
    l_key = (1, l_cols["l_custkey"].identity)
    equivalence_classes = [[o_key, l_key]]
    per_leaf_scans = [{"orders": orders}, {"lineitem": lineitem}]
    vertices = [
        JoinVertex(id=0, name="orders", row_count=1_500_000, payload=None),
        JoinVertex(id=1, name="lineitem", row_count=6_000_000, payload=None),
    ]

    tdoms = plan_adapter._build_equiv_tdoms(equivalence_classes, per_leaf_scans, vertices)
    assert tdoms[o_key] == 100_000
    assert tdoms[l_key] == 100_000


def test_join_graph_edges_receive_stats_ndv_and_null_fraction():
    """End to end through build_join_graph: the KeyStats on the edges the
    enumerators consume must carry the scans' real NDV and null_fraction."""
    plan = LogicalPlan()
    orders, o_cols = _make_scan("orders", 1_500_000, {"o_custkey": (100_000, 0.01)})
    customer, c_cols = _make_scan("customer", 150_000, {"c_custkey": (150_000, 0.0)})
    plan.add_node("scan-orders", orders)
    plan.add_node("scan-customer", customer)

    leaves = [
        SimpleNamespace(subplan_id="scan-orders", rel_names=["orders"]),
        SimpleNamespace(subplan_id="scan-customer", rel_names=["customer"]),
    ]
    predicate = _eq_predicate(
        _identifier("orders", o_cols["o_custkey"]),
        _identifier("customer", c_cols["c_custkey"]),
    )

    graph = plan_adapter.build_join_graph(plan, leaves, [predicate])
    assert graph is not None, "graph construction itself must succeed"
    assert len(graph.edges) == 1
    left_key, right_key = graph.edges[0].equi_keys[0]
    assert left_key.ndv == 100_000
    assert left_key.null_fraction == 0.01
    assert right_key.ndv == 150_000
    assert right_key.null_fraction == 0.0


def test_self_join_sides_keep_their_own_statistics():
    """A self-join scans the same relation twice under different aliases; the
    binder mints DISTINCT identities for the same column NAME on each side.
    Each edge endpoint must read its own side's statistics — keying by name
    collapsed both sides onto whichever dict entry survived."""
    plan = LogicalPlan()
    e1, e1_cols = _make_scan("employees", 1_000_000, {"manager_id": (50_000, 0.10)}, alias="e1")
    e2, e2_cols = _make_scan("employees", 1_000_000, {"manager_id": (60_000, 0.0)}, alias="e2")
    assert e1_cols["manager_id"].identity != e2_cols["manager_id"].identity
    plan.add_node("scan-e1", e1)
    plan.add_node("scan-e2", e2)

    leaves = [
        SimpleNamespace(subplan_id="scan-e1", rel_names=["e1"]),
        SimpleNamespace(subplan_id="scan-e2", rel_names=["e2"]),
    ]
    predicate = _eq_predicate(
        _identifier("e1", e1_cols["manager_id"]),
        _identifier("e2", e2_cols["manager_id"]),
    )

    graph = plan_adapter.build_join_graph(plan, leaves, [predicate])
    assert graph is not None
    left_key, right_key = graph.edges[0].equi_keys[0]
    # Each side's OWN stats — not a name-collapsed merge of the two.
    assert left_key.ndv == 50_000
    assert left_key.null_fraction == 0.10
    assert right_key.ndv == 60_000
    assert right_key.null_fraction == 0.0


if __name__ == "__main__":  # pragma: no cover
    pytest.main([__file__, "-v"])
