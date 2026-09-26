"""
ORDER BY ... LIMIT l OFFSET o fuses into HeapSort(l + o) -> Limit(l, offset o).

Before this, OperatorFusionStrategy refused any LIMIT carrying an OFFSET, so the
query fully sorted its input (every group of a GROUP BY). Now the Order becomes a
HeapSort keeping the top l + o rows and the Limit stays above it to skip the
offset. That also hands k = l + o to everything that reads a HeapSort's limit —
the GROUP BY top-k fusion and the scan top-N pushdown.

The contract: same answer as the unfused plan. Where the ORDER BY is a total order
the rows match exactly; where it is not, the ordering values match and every
returned row is a real row of the input.
"""

import os
import sys

import pytest

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

import opteryx
from opteryx.planner.logical_planner import LogicalPlanStepType
from opteryx.planner.optimizer.strategies import operator_fusion

SERIES = "generate_series(1, 1000003) AS g"


def _run(sql, fused):
    """(rows, plan node kinds) for `sql`, with OFFSET fusion enabled or disabled."""
    original = operator_fusion.OperatorFusionStrategy.visit
    if not fused:

        def visit(self, node, context):
            if node.node_type == LogicalPlanStepType.Order:
                edges = context.optimized_plan.outgoing_edges(context.node_id)
                if len(edges) == 1:
                    following = context.optimized_plan[edges[0][1]]
                    if following.node_type == LogicalPlanStepType.Limit and following.offset:
                        return context
            return original(self, node, context)

        operator_fusion.OperatorFusionStrategy.visit = visit
    try:
        session = opteryx.session()
        try:
            rows = []
            for morsel in session.execute_to_morsels(sql):
                columns = [morsel.column(name).to_pylist() for name in morsel.column_names]
                rows.extend(zip(*columns))
            kinds = [session._plan[nid].kind for nid in session._plan.nodes()]
            return rows, kinds
        finally:
            session.close()
    finally:
        operator_fusion.OperatorFusionStrategy.visit = original


# (sql, total order?) — every one of these must fuse.
# fmt:off
FUSED = [
    # GROUP BY below: also arms the GROUP BY top-k fusion with k = l + o.
    (f"SELECT g % 100003 AS k, SUM(g) AS s FROM {SERIES} GROUP BY g % 100003 ORDER BY s DESC LIMIT 10 OFFSET 1000", True),
    (f"SELECT g % 100003 AS k, SUM(g) AS s FROM {SERIES} GROUP BY g % 100003 ORDER BY s ASC LIMIT 7 OFFSET 99990", True),
    (f"SELECT g % 100003 AS k, COUNT(*) AS c FROM {SERIES} GROUP BY g % 100003 ORDER BY c DESC, k LIMIT 10 OFFSET 500", True),
    # Plain sort over a stream.
    (f"SELECT g FROM {SERIES} ORDER BY g DESC LIMIT 5 OFFSET 17", True),
    ("SELECT name FROM $planets ORDER BY name LIMIT 3 OFFSET 2", True),
    # Offset past the end: empty; offset reaching into the tail: short.
    ("SELECT name FROM $planets ORDER BY name LIMIT 3 OFFSET 20", True),
    ("SELECT name FROM $planets ORDER BY name LIMIT 5 OFFSET 7", True),
    # Scan top-N pushdown receives l + o.
    ("SELECT EventTime, WatchID FROM testdata.clickbench_tiny ORDER BY EventTime, WatchID LIMIT 5 OFFSET 20", True),
    # Ties: ordering values must match, rows must be real.
    (f"SELECT g % 100003 AS k, COUNT(*) AS c FROM {SERIES} GROUP BY g % 100003 ORDER BY c DESC LIMIT 10 OFFSET 500", False),
]
# fmt:on


@pytest.mark.parametrize("sql, total_order", FUSED)
def test_offset_fusion_matches_unfused(sql, total_order):
    fused_rows, fused_kinds = _run(sql, fused=True)
    plain_rows, plain_kinds = _run(sql, fused=False)
    assert "HeapSortNode" in fused_kinds and "LimitNode" in fused_kinds, fused_kinds
    assert "HeapSortNode" not in plain_kinds, plain_kinds
    assert len(fused_rows) == len(plain_rows)
    if total_order:
        assert fused_rows == plain_rows, sql
    else:
        assert [r[-1] for r in fused_rows] == [r[-1] for r in plain_rows], sql
        everything = set(_run(sql.rsplit(" ORDER BY ", 1)[0], fused=True)[0])
        assert all(row in everything for row in fused_rows), sql


def test_offset_without_limit_is_not_fused():
    """OFFSET alone has no bound on the rows to keep — stays a full sort."""
    rows, kinds = _run("SELECT name FROM $planets ORDER BY name OFFSET 6", fused=True)
    assert "HeapSortNode" not in kinds
    assert [r[0] for r in rows] == ["Saturn", "Uranus", "Venus"]


def test_limit_without_offset_still_fuses_to_one_node():
    """No OFFSET: unchanged — the Limit is absorbed into the HeapSort."""
    rows, kinds = _run("SELECT name FROM $planets ORDER BY name LIMIT 2", fused=True)
    assert "HeapSortNode" in kinds and "LimitNode" not in kinds
    assert [r[0] for r in rows] == ["Earth", "Jupiter"]


if __name__ == "__main__":  # pragma: no cover
    sys.exit(pytest.main([__file__, "-q"]))
