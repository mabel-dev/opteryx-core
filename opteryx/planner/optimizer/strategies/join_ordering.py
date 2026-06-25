# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Optimization Rule - Join Ordering

Type: Cost-Based
Goal: Faster Joins

Build a left-deep join tree, where the left relation of any pair is the smaller relation.

We also decide if we should use a nested loop join or a hash join based on the size of the left relation.

Join Ordering Rules (from COST-BASED-OPTIMIZER.md):
1. If one table is more than 3x the bytes of the other, larger table goes right (memory pressure heuristic)
2. If cardinalities are within 1%, larger table goes right
3. Otherwise, use cardinality estimation of join column(s) to decide left/right tables
4. If table sizes and cardinalities are the same (e.g. self join), don't change order
"""

from opteryx.config import features
from opteryx.planner.logical_planner import LogicalPlan
from opteryx.planner.logical_planner import LogicalPlanNode
from opteryx.planner.logical_planner import LogicalPlanStepType

from .optimization_strategy import OptimizationStrategy
from .optimization_strategy import OptimizerContext
from .optimization_strategy import get_nodes_of_type_from_logical_plan

DISABLE_NESTED_LOOP_JOIN: bool = features.disable_nested_loop_join
FORCE_NESTED_LOOP_JOIN: bool = features.force_nested_loop_join


def _col_value(col):
    """Return the underlying column identifier regardless of object shape."""
    return getattr(col, "value", col)


def _join_key_name(col):
    """Best-effort physical name of a join-key column, matching how
    RelationStatistics.columns is keyed (see statistics_refresh._column_name).
    Returns None when no name can be resolved (NDV/null then go unknown)."""
    schema_column = getattr(col, "schema_column", None)
    if schema_column is not None:
        name = getattr(schema_column, "name", None)
        if isinstance(name, str):
            return name
    name = getattr(col, "source_column", None) or getattr(col, "value", None)
    if isinstance(name, str):
        return name
    name = getattr(col, "name", None)
    return name if isinstance(name, str) else None


def _decide_swap(left_rows, right_rows, left_ndv, right_ndv, left_null, right_null):
    """Decide whether to swap the join's sides so the smaller/cheaper relation
    ends up on the left (build) side.

    Pure function of per-side row counts, join-key NDVs and join-key null
    fractions. Row counts are *post-filter* ``statistics.row_count`` when
    available (so a heavily-filtered large table is correctly seen as small),
    falling back to the binder's pre-filter row estimate otherwise. The 3x and
    1% thresholds are unchanged from the previous size-only implementation, and
    with unknown NDV/null this reduces to "smaller side on the left" exactly as
    before.
    """
    # Rule 1: memory pressure — one side dominates the other in rows.
    if left_rows > 3 * right_rows:
        return True
    if right_rows > 3 * left_rows:
        return False

    # Effective rows discount join keys that are partly NULL (worst-case side).
    left_eff = left_rows * (1.0 - left_null) if left_null else left_rows
    right_eff = right_rows * (1.0 - right_null) if right_null else right_rows

    # Rules 2 & 3: cardinality-aware when both join-key NDVs are known.
    if left_ndv is not None and right_ndv is not None:
        denom = max(left_ndv, right_ndv)
        card_diff_pct = (abs(left_ndv - right_ndv) / denom * 100.0) if denom else 0.0
        if card_diff_pct <= 1.0:
            # Rule 2: near-equal cardinality — smaller effective rows on the left.
            return left_eff > right_eff
        # Rule 3: prefer smaller cardinality left; tie-break on effective rows.
        return left_ndv > right_ndv or (left_ndv == right_ndv and left_eff > right_eff)

    # Fallback: no cardinality data — smaller effective rows on the left.
    return right_eff < left_eff


class JoinOrderingStrategy(OptimizationStrategy):
    optimization_technique = "cost"
    requires = ("joins-planned",)

    def visit(self, node: LogicalPlanNode, context: OptimizerContext) -> OptimizerContext:
        if not context.optimized_plan:
            context.optimized_plan = context.pre_optimized_tree.copy()  # type: ignore

        if node.node_type == LogicalPlanStepType.Join and node.type == "cross join":
            # 1438
            pass

        if node.node_type == LogicalPlanStepType.Join and node.type == "inner":
            # Only reorder joins whose legs carry reader UUIDs. Joins without
            # them (window-function partitions, set-op / IN-subquery rewrites)
            # are leg-resolved by relation-name matching in
            # physical_plan._label_join_legs_by_relation, which a swap would
            # break — and one side is a synthetic relation ($win-*, derived)
            # whose statistics are meaningless for build-side selection anyway.
            # The non-equi / nested-loop classification below still runs for
            # these joins (it's a correctness concern), only the swap is gated.
            can_reorder = bool(node.left_readers) and bool(node.right_readers)

            should_swap = False
            if can_reorder:
                # Apply join ordering rules from COST-BASED-OPTIMIZER.md, fed from the
                # refreshed per-node statistics (post-filter row counts and join-key
                # NDV/null fractions) rather than the binder's pre-filter size estimate.
                left_stats, right_stats = self._side_statistics(
                    context.pre_optimized_tree, context.node_id
                )
                left_rows = self._side_rows(left_stats, node.left_size)
                right_rows = self._side_rows(right_stats, node.right_size)
                left_ndv = self._key_ndv(left_stats, node.left_columns)
                right_ndv = self._key_ndv(right_stats, node.right_columns)
                left_null = self._key_null_fraction(left_stats, node.left_columns)
                right_null = self._key_null_fraction(right_stats, node.right_columns)

                should_swap = _decide_swap(
                    left_rows, right_rows, left_ndv, right_ndv, left_null, right_null
                )

            # Perform the swap if needed
            if should_swap:
                # fmt:off
                node.left_size, node.right_size = node.right_size, node.left_size
                node.left_columns, node.right_columns = node.right_columns, node.left_columns
                node.left_column, node.right_column = node.right_column, node.left_column
                node.left_readers, node.right_readers = node.right_readers, node.left_readers
                node.left_relation_names, node.right_relation_names = node.right_relation_names, node.left_relation_names
                # fmt:on
                self.telemetry.optimization_inner_join_smallest_table_left += 1
                context.optimized_plan[context.node_id] = node

            # if any of the comparisons are other than "equal", we cannot use a hash join
            comparator = _col_value(node.on)
            if comparator in ("NotEq", "Lt", "Gt", "LtEq", "GtEq"):
                node.type = "non equi"
                context.optimized_plan[context.node_id] = node
            # Nested-loop join wins when the smaller side is tiny enough that
            # building a hash table doesn't amortize, AND the larger side is
            # big enough that the bloom prefilter pays off. The upper bound
            # is a safety bound against extrapolation outside the calibrated
            # range; see scratch/_sweep_join_crossover.py for the empirical
            # crossover sweep that produced these thresholds.
            elif (
                not DISABLE_NESTED_LOOP_JOIN
                and min(node.left_size, node.right_size) <= 500
                and 1_000 <= max(node.left_size, node.right_size) <= 1_000_000
            ) or FORCE_NESTED_LOOP_JOIN:
                node.type = "nested loop"
                context.optimized_plan[context.node_id] = node

        return context

    def complete(self, plan: LogicalPlan, context: OptimizerContext) -> LogicalPlan:
        # No finalization needed for this strategy
        return plan

    def should_i_run(self, plan):
        # only run if there are LIMIT clauses in the plan
        candidates = get_nodes_of_type_from_logical_plan(plan, (LogicalPlanStepType.Join,))
        return len(candidates) > 0

    @staticmethod
    def _side_statistics(plan, join_nid):
        """Return (left_stats, right_stats) RelationStatistics for the join's two
        inputs, identified by the 'left'/'right' edge labels. Either may be None
        when statistics are absent or a side is unlabelled.

        Cross-join→inner-converted joins carry unlabelled ingoing edges
        (label ``None``); without a fallback every such join would read the
        binder's pre-filter size estimate instead of the refreshed post-filter
        statistics. When labels are missing we fall back to ingoing-edge
        insertion order (left, then right) — mirroring
        ``statistics_refresh._split_join_children``.
        """
        left = right = None
        ordered = []
        for child_nid, _, label in plan.ingoing_edges(join_nid):
            stats = getattr(plan[child_nid], "statistics", None)
            ordered.append(stats)
            if label == "left":
                left = stats
            elif label == "right":
                right = stats
        if left is None and ordered:
            left = ordered[0]
        if right is None and len(ordered) > 1:
            right = ordered[1]
        return left, right

    @staticmethod
    def _side_rows(stats, fallback):
        """Post-filter row count for a side, falling back to the binder estimate."""
        if stats is not None and getattr(stats, "row_count", None) is not None:
            return stats.row_count
        return fallback

    @staticmethod
    def _key_ndv(stats, key_columns):
        """Smallest known join-key NDV for a side, or None when unavailable."""
        if stats is None:
            return None
        ndvs = []
        for col in key_columns or []:
            name = _join_key_name(col)
            col_stats = stats.get_column(name) if isinstance(name, str) else None
            if col_stats is not None and col_stats.distinct_count is not None:
                ndvs.append(col_stats.distinct_count)
        return min(ndvs) if ndvs else None

    @staticmethod
    def _key_null_fraction(stats, key_columns):
        """Worst-case (highest) join-key null fraction for a side, or None."""
        if stats is None:
            return None
        fractions = []
        for col in key_columns or []:
            name = _join_key_name(col)
            col_stats = stats.get_column(name) if isinstance(name, str) else None
            if col_stats is not None and col_stats.null_fraction is not None:
                fractions.append(col_stats.null_fraction)
        return max(fractions) if fractions else None
