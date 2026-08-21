"""Regression locks for the two estimator fixes ported into the DPccp path.

`statistics_refresh` has carried both of these since 2026-08-05; the enumerator
did not, and TPC-H Q09 paid 3.3x for it. The two paths must agree, or the
tree-picker and the build-side chooser cost the same join differently.

One test per property, sized so neither can mask a regression in the other:
the occupancy test uses a single key class (bound cannot bind), and the domain
test uses row counts where the bound is slack.
"""

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../../../.."))

from opteryx.planner.cost_estimation import JoinEdge
from opteryx.planner.cost_estimation import JoinGraph
from opteryx.planner.cost_estimation import JoinTreeLeaf
from opteryx.planner.cost_estimation import JoinVertex
from opteryx.planner.cost_estimation import KeyStats
from opteryx.planner.cost_estimation import NdvProvenance
from opteryx.planner.cost_estimation import dpccp
from opteryx.planner.cost_estimation.dpccp import _combine
from opteryx.planner.cost_estimation.plan_adapter import _build_equiv_tdoms


def _ks(ndv):
    """A KeyStats with a MEASURED NDV -- these tests exercise the arithmetic,
    not the domain-size stand-in path."""
    if ndv is None:
        return KeyStats(ndv=None, null_fraction=0.0)
    return KeyStats(ndv=ndv, null_fraction=0.0, ndv_provenance=NdvProvenance.MEASURED)


def _leaf(vertex_id, rows, domain_rows=None):
    return JoinTreeLeaf(vertex_id=vertex_id, estimated_rows=rows, domain_rows=domain_rows)


def _edge(left, right, ndv, class_id):
    return JoinEdge(
        left=left,
        right=right,
        equi_keys=((_ks(ndv), _ks(ndv)),),
        class_id=class_id,
    )


def test_composite_key_domain_is_bounded_by_the_rows_holding_it():
    """TPC-H Q09's ``partsupp ⋈ lineitem`` on (ps_partkey, ps_suppkey).

    Two DISTINCT key classes, so both selectivities multiply: 2,000,000 x
    100,000 = 2e11 possible key tuples against 8,000,000 partsupp rows to hold
    them. Unbounded that estimates ~30 rows; the true answer is every lineitem
    row. A relation cannot hold more distinct key tuples than it has rows.
    """
    left = _leaf(0, 8_000_000)  # partsupp
    right = _leaf(1, 59_986_052)  # lineitem
    edges = (_edge(0, 1, 2_000_000, class_id=0), _edge(0, 1, 100_000, class_id=1))

    node = _combine(left, right, edges)

    # Bound is min(domain rows) = 8,000,000, so |L|x|R|/bound == |R|.
    assert node.estimated_rows == 59_986_052


def test_occupancy_bound_leaves_a_slack_composite_key_alone():
    """The bound raises estimates only where the product exceeds the rows.

    Collapsing to one pair must be exactly equivalent when the product is
    already under the bound — one divisor of P against N divisors multiplying
    to P — so a composite key with room to spare is untouched.
    """
    left = _leaf(0, 1_000_000)
    right = _leaf(1, 1_000_000)
    edges = (_edge(0, 1, 100, class_id=0), _edge(0, 1, 200, class_id=1))

    node = _combine(left, right, edges)

    # 100 x 200 = 20,000 <= 1,000,000, so both selectivities still apply.
    assert node.estimated_rows == 1_000_000 * 1_000_000 // (100 * 200)


def test_unknown_ndv_disables_the_bound_rather_than_inventing_one():
    """``_key_selectivity`` falls back to a flat constant for an unknown NDV.

    There is no product to bound in that case, and substituting the occupancy
    bound would silently overwrite the fallback with a made-up domain.
    """
    left = _leaf(0, 1_000)
    right = _leaf(1, 1_000)
    known = _edge(0, 1, 10, class_id=0)
    unknown = JoinEdge(
        left=0,
        right=1,
        equi_keys=((_ks(None), _ks(None)),),
        class_id=1,
    )

    node = _combine(left, right, (known, unknown))

    # 1000 x 1000 / 10 x the 0.1 equality fallback — bound never applied.
    assert node.estimated_rows == 10_000


def test_tdom_fallback_uses_pre_filter_rows_so_a_filter_stays_selective():
    """A filter removes ROWS, not the values a key column could hold.

    TPC-H Q09's shape: ``p_name LIKE '%plum%'`` takes part 2,000,000 → 200,000,
    but p_partkey still ranges over 2,000,000 values, so only ~1/10th of
    lineitem survives the join. Taking the fallback from the post-filter count
    makes ``|L| x |R| / tdom`` collapse to ``max(rows)`` — the filter buys
    nothing, and the one join that must happen first stops looking cheap.
    """

    class _NoStatsScan:
        """A scan whose manifest carried no distinct_count — the common
        Parquet case, and the only one that reaches the fallback."""

        statistics = None

    per_leaf_scans = [{"part": _NoStatsScan()}, {"lineitem": _NoStatsScan()}]
    vertices = [
        JoinVertex(id=0, name="part", row_count=200_000, base_row_count=2_000_000),
        JoinVertex(id=1, name="lineitem", row_count=59_986_052),
    ]
    classes = [[(0, "p_partkey"), (1, "l_partkey")]]

    tdoms = _build_equiv_tdoms(classes, per_leaf_scans, vertices)

    # Pre-filter part, NOT the 200,000 rows that survived the LIKE.
    assert tdoms[(0, "p_partkey")] == 2_000_000
    assert tdoms[(1, "l_partkey")] == 2_000_000


def test_join_subtree_domain_composes_as_max_of_its_sides():
    """Matches ``statistics_refresh._join_stats``, which sets a join's
    base_row_count to ``max(left.domain_row_count, right.domain_row_count)``."""
    node = _combine(_leaf(0, 200_000, domain_rows=2_000_000), _leaf(1, 59_986_052),
                    (_edge(0, 1, 2_000_000, class_id=0),))

    assert node.domain_rows == 59_986_052


def test_enumerator_seeds_leaf_domain_rows_from_the_vertex():
    """``dpccp`` must carry ``JoinVertex.domain_row_count`` onto its leaves —
    the occupancy bound is inert if the pre-filter size stops at the graph
    boundary. Composite key, and the bound binds only via part's PRE-filter
    2,000,000; read from the post-filter 200,000 it would bind 10x tighter."""
    graph = JoinGraph(
        vertices=[
            JoinVertex(id=0, name="part", row_count=200_000, base_row_count=2_000_000),
            JoinVertex(id=1, name="lineitem", row_count=59_986_052),
        ],
        edges=[
            _edge(0, 1, 2_000_000, class_id=0),
            _edge(0, 1, 100_000, class_id=1),
        ],
    )

    tree = dpccp(graph)

    assert tree.estimated_rows == 200_000 * 59_986_052 // 2_000_000
