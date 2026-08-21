"""An unstamped MaterializedCteRef must estimate as UNKNOWN, never zero.

A reference to a shared CTE normally carries the body's output estimate,
stamped by do_optimizer before any refresh runs (shared_cte.py's
`stamp_reference_estimates`). When the stamp is absent — DISABLE_OPTIMIZER
skips stamping entirely, and result_size_guard's refresh still runs — the
branch used to return `_empty_stats()`, i.e. row_count_estimate=0. But 0 is
not unknown: it is a claim of provable emptiness that propagates
multiplicatively — any join against a 0-row side computes max(1, 0*n) = 1,
collapsing the whole subtree's estimate to ~1 row and poisoning every cost
decision above it. The unstamped posture is now _UNKNOWN_ROW_COUNT, the same
stand-in a scan with no manifest counts gets.
"""

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../../../.."))

from opteryx.planner.logical_planner import LogicalPlan
from opteryx.planner.logical_planner import LogicalPlanNode
from opteryx.planner.logical_planner import LogicalPlanStepType
from opteryx.planner.optimizer.statistics import RelationStatistics
from opteryx.planner.optimizer.statistics_refresh import _UNKNOWN_ROW_COUNT
from opteryx.planner.optimizer.statistics_refresh import refresh_statistics

_BIG_ROW_COUNT = 5_000_000


def _plan_with_unstamped_ref_joined_to_big_relation() -> LogicalPlan:
    """(unstamped MaterializedCteRef) JOIN (5M-row relation) -> Exit.

    Both leaves are MaterializedCteRef so the plan needs no manifest
    resolution; the "big relation" is simply a stamped ref. The join carries
    no equi keys, so its estimate is the cross-product bound
    max(1, left * right) — exactly the shape a 0-row side collapses to 1.
    """
    plan = LogicalPlan()

    unstamped = LogicalPlanNode(node_type=LogicalPlanStepType.MaterializedCteRef)

    big = LogicalPlanNode(node_type=LogicalPlanStepType.MaterializedCteRef)
    big.cte_statistics = RelationStatistics(columns={}, row_count_metric=_BIG_ROW_COUNT)

    join = LogicalPlanNode(node_type=LogicalPlanStepType.Join)
    join.type = "inner"

    exit_node = LogicalPlanNode(node_type=LogicalPlanStepType.Exit)

    plan.add_node("unstamped_ref", unstamped)
    plan.add_node("big_relation", big)
    plan.add_node("join", join)
    plan.add_node("exit", exit_node)
    plan.add_edge("unstamped_ref", "join", "left")
    plan.add_edge("big_relation", "join", "right")
    plan.add_edge("join", "exit")
    return plan


def test_unstamped_cte_ref_estimates_as_unknown_not_zero():
    plan = refresh_statistics(_plan_with_unstamped_ref_joined_to_big_relation())
    stats = plan["unstamped_ref"].statistics
    assert stats.row_count == _UNKNOWN_ROW_COUNT
    assert not stats.row_count_is_metric  # a stand-in is never exact knowledge


def test_join_against_unstamped_cte_ref_is_not_collapsed_to_one_row():
    """The regression: 0 * 5_000_000 -> max(1, 0) -> 1-row join estimate."""
    plan = refresh_statistics(_plan_with_unstamped_ref_joined_to_big_relation())
    join_stats = plan["join"].statistics
    assert join_stats.row_count >= _BIG_ROW_COUNT, (
        f"join estimate collapsed to {join_stats.row_count} rows — the "
        "unstamped CTE ref is propagating as a multiplicative zero"
    )


def test_stamped_cte_ref_still_returns_the_stamp_verbatim():
    plan = refresh_statistics(_plan_with_unstamped_ref_joined_to_big_relation())
    stats = plan["big_relation"].statistics
    assert stats.row_count == _BIG_ROW_COUNT
    assert stats.row_count_is_metric


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
