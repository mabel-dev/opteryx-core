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


def get_column_cardinality_estimates(plan, relation_names, column_names):
    """
    Get cardinality estimates for join columns from Scan node manifests.

    Args:
        plan: Logical plan to search for Scan nodes
        relation_names: List of relation names to find
        column_names: List of column names to estimate cardinality for

    Returns:
        List of estimated cardinalities, or None if not available
    """
    scan_nodes = get_nodes_of_type_from_logical_plan(plan, (LogicalPlanStepType.Scan,))

    estimates = []
    for relation_name in relation_names:
        # Find the Scan node for this relation
        scan_node = None
        for node in scan_nodes:
            if hasattr(node, "relation") and node.relation == relation_name:
                scan_node = node
                break

        if not scan_node or not hasattr(scan_node, "manifest") or not scan_node.manifest:
            return None

        # Get cardinality estimates from manifest
        manifest = scan_node.manifest
        column_estimates = []

        for col_name in column_names:
            cardinality = manifest.estimate_cardinality(col_name)
            if cardinality is not None:
                column_estimates.append(cardinality)

        if column_estimates:
            estimates.append(min(column_estimates))
        else:
            return None

    return estimates if len(estimates) == len(relation_names) else None


def get_column_null_fractions(plan, relation_names, column_names):
    """
    Get null fractions for join columns from Scan node manifests.

    Returns a list of max null fractions per relation (one per relation_name), or None if unavailable.
    """
    scan_nodes = get_nodes_of_type_from_logical_plan(plan, (LogicalPlanStepType.Scan,))

    null_fracs = []
    for relation_name in relation_names:
        scan_node = None
        for node in scan_nodes:
            if hasattr(node, "relation") and node.relation == relation_name:
                scan_node = node
                break

        if not scan_node or not hasattr(scan_node, "manifest") or not scan_node.manifest:
            return None

        manifest = scan_node.manifest
        col_nulls = []
        for col_name in column_names:
            frac = manifest.estimate_null_fraction(col_name)
            if frac is not None:
                col_nulls.append(frac)

        if col_nulls:
            # Use the worst-case (highest) null fraction across join cols
            null_fracs.append(max(col_nulls))
        else:
            return None

    return null_fracs if len(null_fracs) == len(relation_names) else None


def _col_value(col):
    """Return the underlying column identifier regardless of object shape."""
    return getattr(col, "value", col)


class JoinOrderingStrategy(OptimizationStrategy):
    def visit(self, node: LogicalPlanNode, context: OptimizerContext) -> OptimizerContext:
        if not context.optimized_plan:
            context.optimized_plan = context.pre_optimized_tree.copy()  # type: ignore

        if node.node_type == LogicalPlanStepType.Join and node.type == "cross join":
            # 1438
            pass

        if node.node_type == LogicalPlanStepType.Join and node.type == "inner":
            # Apply join ordering rules from COST-BASED-OPTIMIZER.md
            should_swap = False

            # Rule 1: Memory pressure heuristic - if one table is >3x bytes of the other
            if node.left_size > 3 * node.right_size:
                should_swap = True
            elif node.right_size > 3 * node.left_size:
                should_swap = False
            else:
                # Rule 2 & 3: Use cardinality and null-aware effective rows if available
                left_cards = get_column_cardinality_estimates(
                    context.pre_optimized_tree,
                    node.left_relation_names,
                    [_col_value(col) for col in node.left_columns],
                )
                right_cards = get_column_cardinality_estimates(
                    context.pre_optimized_tree,
                    node.right_relation_names,
                    [_col_value(col) for col in node.right_columns],
                )

                left_nulls = get_column_null_fractions(
                    context.pre_optimized_tree,
                    node.left_relation_names,
                    [_col_value(col) for col in node.left_columns],
                )
                right_nulls = get_column_null_fractions(
                    context.pre_optimized_tree,
                    node.right_relation_names,
                    [_col_value(col) for col in node.right_columns],
                )

                # Effective rows discounting null join keys (worst-case per side)
                left_eff_rows = node.left_size
                right_eff_rows = node.right_size
                if left_nulls and len(left_nulls) > 0:
                    left_eff_rows = node.left_size * (1 - max(left_nulls))
                if right_nulls and len(right_nulls) > 0:
                    right_eff_rows = node.right_size * (1 - max(right_nulls))

                if left_cards and right_cards:
                    left_card = min(left_cards)
                    right_card = min(right_cards)

                    # Rule 2: If cardinalities within 1%, fall back to effective rows
                    card_diff_pct = abs(left_card - right_card) / max(left_card, right_card) * 100
                    if card_diff_pct <= 1.0:
                        if left_eff_rows > right_eff_rows:
                            should_swap = True
                    # Rule 3: Otherwise, prefer smaller cardinality (and if tied, smaller effective rows)
                    elif left_card > right_card or (
                        left_card == right_card and left_eff_rows > right_eff_rows
                    ):
                        should_swap = True
                else:
                    # Fallback: No cardinality data, use effective rows
                    if right_eff_rows < left_eff_rows:
                        should_swap = True

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
            # Small datasets benefit from nested loop joins (avoids building a hash table)
            elif (
                not DISABLE_NESTED_LOOP_JOIN
                and min(node.left_size, node.right_size) < 1_000
                and max(node.left_size, node.right_size) < 100_000
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
