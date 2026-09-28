"""An unstamped MaterializedCteRef must estimate as UNKNOWN, never zero.

A reference to a shared CTE normally carries the body's output estimate,
stamped by do_optimizer before any refresh runs (shared_cte.py's
`stamp_reference_estimates`). When the stamp is absent — DISABLE_OPTIMIZER
skips stamping entirely, and result_size_guard's refresh still runs — the
branch used to return an empty statistics object, i.e. row_count_estimate=0. But 0 is
not unknown: it is a claim of provable emptiness that propagates
multiplicatively — any join against a 0-row side computes max(1, 0*n) = 1,
collapsing the whole subtree's estimate to ~1 row and poisoning every cost
decision above it. The unstamped posture is now kUnknownRowCount
(src/cpp/planner/statistics_refresh.hpp), the same stand-in a scan with no
manifest counts gets.
"""

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../../../.."))

# Importing opteryx.planner.optimizer (the package) first resolves the
# pre-existing import cycle a compiled planner module hits when imported first.
import opteryx.planner.optimizer  # noqa: F401
from opteryx.compiled.planner.plan_graph import EdgeRole
from opteryx.compiled.planner.statistics import StatisticsInput
from opteryx.compiled.structures.plan_steps import ExitStep
from opteryx.compiled.structures.plan_steps import JoinStep
from opteryx.compiled.structures.plan_steps import MaterializedCteRefStep
from opteryx.compiled.structures.plan_steps import ScanStep
from opteryx.planner.logical_planner import LogicalPlan
from opteryx.planner.optimizer.statistics_refresh import refresh_statistics
from opteryx.planner.plan_context import PlanContext

# kUnknownRowCount (src/cpp/planner/statistics_refresh.hpp): the native refresh's
# stand-in for a row count nothing knows. It is not exported to Python.
_UNKNOWN_ROW_COUNT = 1_000_000

_BIG_ROW_COUNT = 5_000_000


def _plan_with_unstamped_ref_joined_to_big_relation():
    """(plan, plan_context, ids): (unstamped MaterializedCteRef) JOIN (5M-row relation) -> Exit.

    Both leaves are MaterializedCteRef so the plan needs no manifest
    resolution; the "big relation" is simply a stamped ref. The join carries
    no equi keys, so its estimate is the cross-product bound
    max(1, left * right) — exactly the shape a 0-row side collapses to 1.
    """
    plan_context = PlanContext()
    plan = LogicalPlan(plan_context)

    unstamped = MaterializedCteRefStep()
    unstamped.cte_key = "unstamped"

    big = MaterializedCteRefStep()
    big.cte_key = "big"
    # the "big" CTE's stamp: its body's statistics, recorded under its key (the
    # body is a node of its own plan, holding the seeded statistics)
    bodies = LogicalPlan(plan_context)
    body_nid = bodies.add_node(ScanStep())
    plan_context.statistics.seed(
        body_nid, StatisticsInput(plan_context.columns, row_count_metric=_BIG_ROW_COUNT)
    )
    plan_context.statistics.set_cte("big", body_nid)

    join = JoinStep()
    join.type = "inner"

    exit_node = ExitStep()

    unstamped_ref_nid = plan.add_node(unstamped)
    big_relation_nid = plan.add_node(big)
    join_nid = plan.add_node(join)
    exit_nid = plan.add_node(exit_node)
    plan.add_edge(unstamped_ref_nid, join_nid, EdgeRole.LEFT)
    plan.add_edge(big_relation_nid, join_nid, EdgeRole.RIGHT)
    plan.add_edge(join_nid, exit_nid)
    ids = {"unstamped_ref": unstamped_ref_nid, "big_relation": big_relation_nid, "join": join_nid}
    return plan, plan_context, ids


def _refreshed():
    plan, plan_context, ids = _plan_with_unstamped_ref_joined_to_big_relation()
    return refresh_statistics(plan, plan_context), plan_context, ids


def test_unstamped_cte_ref_estimates_as_unknown_not_zero():
    plan, plan_context, ids = _refreshed()
    store = plan_context.statistics
    assert store.row_count(ids["unstamped_ref"]) == _UNKNOWN_ROW_COUNT
    # a stand-in is never exact knowledge
    assert store.row_count_metric(ids["unstamped_ref"]) is None


def test_join_against_unstamped_cte_ref_is_not_collapsed_to_one_row():
    """The regression: 0 * 5_000_000 -> max(1, 0) -> 1-row join estimate."""
    plan, plan_context, ids = _refreshed()
    join_rows = plan_context.statistics.row_count(ids["join"])
    assert join_rows >= _BIG_ROW_COUNT, (
        f"join estimate collapsed to {join_rows} rows — the "
        "unstamped CTE ref is propagating as a multiplicative zero"
    )


def test_stamped_cte_ref_still_returns_the_stamp_verbatim():
    plan, plan_context, ids = _refreshed()
    store = plan_context.statistics
    assert store.row_count(ids["big_relation"]) == _BIG_ROW_COUNT
    assert store.row_count_metric(ids["big_relation"]) == _BIG_ROW_COUNT


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
