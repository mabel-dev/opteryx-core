# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""WP-7: JoinOrderingStrategy consumes node.statistics (post-filter row counts).

Two layers:
  * ``_decide_swap`` — the pure side-selection logic, including the case the old
    pre-filter size heuristic got wrong (a heavily-filtered large table).
  * ``JoinOrderingStrategy.visit`` end-to-end — proves the strategy reads the
    children's post-filter ``statistics.row_count`` (by 'left'/'right' edge
    label) instead of the binder's pre-filter ``left_size``/``right_size``.
"""

import os
import sys
from types import SimpleNamespace

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

import pytest

from opteryx.models import QueryTelemetry
from opteryx.planner.logical_planner.logical_planner import LogicalPlan
from opteryx.planner.logical_planner.logical_planner import LogicalPlanNode
from opteryx.planner.logical_planner.logical_planner import LogicalPlanStepType
from opteryx.planner.optimizer.statistics import ColumnStatistics
from opteryx.planner.optimizer.statistics import RelationStatistics
from opteryx.planner.optimizer.strategies.join_ordering import JoinOrderingStrategy
from opteryx.planner.optimizer.strategies.join_ordering import _decide_swap
from opteryx.planner.optimizer.strategies.optimization_strategy import OptimizerContext


# --- pure decision logic ----------------------------------------------------


def test_decide_swap_smaller_side_goes_left_no_stats():
    # No NDV/null: smaller side belongs on the left (build). Equivalent to the
    # prior size-only behaviour.
    assert _decide_swap(1000, 100, None, None, None, None) is True  # left bigger -> swap
    assert _decide_swap(100, 1000, None, None, None, None) is False  # right bigger -> keep


def test_decide_swap_uses_post_filter_rows_not_pre_filter():
    # This is the WP-7 win: a large table that a pushed filter shrinks to 50 rows
    # must be treated as the small side. Old code compared pre-filter sizes
    # (1_000_000 vs 1000) and would have put the *truly small* table on the right.
    pre_filter_would_swap = _decide_swap(1_000_000, 1000, None, None, None, None)
    post_filter_decision = _decide_swap(50, 1000, None, None, None, None)
    assert pre_filter_would_swap is True
    assert post_filter_decision is False  # filtered-small left side stays as build


def test_decide_swap_three_x_memory_rule():
    assert _decide_swap(301, 100, None, None, None, None) is True  # >3x -> swap
    assert _decide_swap(100, 301, None, None, None, None) is False  # other side >3x


def test_decide_swap_prefers_smaller_ndv():
    # Within the 3x band; distinct counts decide. Larger NDV on the left -> swap.
    assert _decide_swap(200, 200, 150, 50, None, None) is True
    assert _decide_swap(200, 200, 50, 150, None, None) is False


def test_decide_swap_null_fraction_breaks_cardinality_tie():
    # Equal NDV (within 1%): smaller effective (null-discounted) rows on the left.
    # Left has a heavy null key -> fewer effective rows -> should NOT swap.
    assert _decide_swap(200, 200, 100, 100, 0.9, 0.0) is False
    assert _decide_swap(200, 200, 100, 100, 0.0, 0.9) is True


# --- end-to-end through visit ------------------------------------------------


# RelationStatistics.columns is keyed by column identity, never by name.
_K = b"tes_k_000000001"


def _scan_with_stats(relation, row_count):
    n = LogicalPlanNode(node_type=LogicalPlanStepType.Scan)
    n.relation = relation
    n.all_relations = {relation}
    n.columns = []
    n.statistics = RelationStatistics(
        row_count=row_count,
        columns={
            _K: ColumnStatistics(column_name="k", data_type="INTEGER")
        },
    )
    return n


def _inner_join_node(left_size, right_size):
    n = LogicalPlanNode(node_type=LogicalPlanStepType.Join)
    n.type = "inner"
    n.on = SimpleNamespace(value="Eq")
    # pre-filter sizes (the binder estimate) — deliberately the OPPOSITE order to
    # the post-filter statistics, so a passing test proves statistics won.
    n.left_size = left_size
    n.right_size = right_size
    # Join keys are raw column identities, matching how RelationStatistics is keyed.
    n.left_columns = [_K]
    n.right_columns = [_K]
    n.left_column = _K
    n.right_column = _K
    n.left_relation_names = ["big"]
    n.right_relation_names = ["small"]
    # left_readers/right_readers are attached by _build_join_plan, from the scans
    # actually wired to each leg — `visit` gates the swap on both being present.
    return n


def _build_join_plan(join_node, left_scan, right_scan):
    plan = LogicalPlan()
    # The swap is gated on both legs carrying reader UUIDs, as the binder's
    # join_leg_preprocess attaches for any join over real scans. Without them
    # the strategy declines to reorder and no swap can ever be observed.
    join_node.left_readers = [left_scan.uuid]
    join_node.right_readers = [right_scan.uuid]
    plan.add_node("j", join_node)
    plan.add_node("l", left_scan)
    plan.add_node("r", right_scan)
    plan.add_edge("l", "j", "left")
    plan.add_edge("r", "j", "right")
    exit_node = LogicalPlanNode(node_type=LogicalPlanStepType.Exit)
    exit_node.columns = []
    plan.add_node("e", exit_node)
    plan.add_edge("j", "e")
    return plan


def _leg_labels(plan):
    """The 'left'/'right' label on each edge feeding the join, keyed by source."""
    return {source: relation for source, _target, relation in plan.ingoing_edges("j")}


def test_visit_swaps_on_post_filter_statistics_not_pre_filter_size():
    # Pre-filter sizes say left(big)=1_000_000 huge, right(small)=1000 -> old code
    # swaps (big to the right). But post-filter statistics say the LEFT side is
    # only 50 rows (a selective filter) and the right is 1000. With statistics the
    # left is already the smaller side, so NO swap should happen.
    join_node = _inner_join_node(left_size=1_000_000, right_size=1000)
    left_scan = _scan_with_stats("big", row_count=50)  # post-filter: tiny
    right_scan = _scan_with_stats("small", row_count=1000)
    plan = _build_join_plan(join_node, left_scan, right_scan)

    strategy = JoinOrderingStrategy(telemetry=QueryTelemetry())
    context = OptimizerContext(plan)
    context.node_id = "j"

    before = strategy.telemetry.optimization_inner_join_smallest_table_left
    strategy.visit(plan["j"], context)
    after = strategy.telemetry.optimization_inner_join_smallest_table_left

    # No swap: statistics show left already smallest. (Pre-filter sizes alone
    # would have forced a swap via the 3x rule.)
    assert after == before, "should not swap when post-filter stats show left is smaller"
    assert _leg_labels(context.optimized_plan) == {"l": "left", "r": "right"}


def test_visit_swaps_when_statistics_show_left_is_larger():
    # Mirror: post-filter statistics show the left side is the big one.
    join_node = _inner_join_node(left_size=1000, right_size=1000)
    left_scan = _scan_with_stats("big", row_count=100_000)
    right_scan = _scan_with_stats("small", row_count=100)
    plan = _build_join_plan(join_node, left_scan, right_scan)

    strategy = JoinOrderingStrategy(telemetry=QueryTelemetry())
    context = OptimizerContext(plan)
    context.node_id = "j"

    before = strategy.telemetry.optimization_inner_join_smallest_table_left
    strategy.visit(plan["j"], context)
    after = strategy.telemetry.optimization_inner_join_smallest_table_left

    assert after == before + 1, "should swap when post-filter stats show left is larger"
    # The swap must reach the edges: they are what the physical plan reads to
    # choose the build side. Swapping only the node attributes loses the decision.
    assert _leg_labels(context.optimized_plan) == {"l": "right", "r": "left"}


def test_visit_falls_back_to_pre_filter_size_without_statistics():
    # No statistics on children -> fall back to node.left_size / right_size.
    join_node = _inner_join_node(left_size=100_000, right_size=100)
    left_scan = LogicalPlanNode(node_type=LogicalPlanStepType.Scan)
    left_scan.relation = "big"
    left_scan.columns = []
    right_scan = LogicalPlanNode(node_type=LogicalPlanStepType.Scan)
    right_scan.relation = "small"
    right_scan.columns = []
    plan = _build_join_plan(join_node, left_scan, right_scan)

    strategy = JoinOrderingStrategy(telemetry=QueryTelemetry())
    context = OptimizerContext(plan)
    context.node_id = "j"

    before = strategy.telemetry.optimization_inner_join_smallest_table_left
    strategy.visit(plan["j"], context)
    after = strategy.telemetry.optimization_inner_join_smallest_table_left

    # left_size (100_000) > 3 * right_size (100) -> swap.
    assert after == before + 1
    assert _leg_labels(context.optimized_plan) == {"l": "right", "r": "left"}


if __name__ == "__main__":  # pragma: no cover
    pytest.main([__file__, "-v"])
