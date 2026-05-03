# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Optimization Strategy - Nullability Inference

Type: Cost-based heuristic
Goal: Reduce data materialization by filtering null rows at the IO boundary

INNER JOIN keys are guaranteed to be non-NULL by join semantics. When a join key
column lacks an explicit WHERE IS NOT NULL filter, the reader materializes all rows
including NULLs, then the join filters them out at execution time.

This strategy:
1. Walks the logical plan for INNER JOIN nodes
2. Extracts join key columns from ON expressions
3. Synthesizes implicit NOT IS NULL filters for keys without explicit null checks
4. Lets existing predicate pushdown logic push them to the reader

Example:
    SELECT * FROM users INNER JOIN orders ON users.id = orders.user_id

Without optimization: Reader materializes all rows (including NULLs in users.id and
orders.user_id), then the join filters out NULL rows at execution time.

With optimization: Reader sees implicit "users.id IS NOT NULL AND orders.user_id IS NOT NULL"
filters and can skip null rows without materializing them.
"""

from typing import Set, Optional, Tuple

from opteryx.expression import NodeType, get_all_nodes_of_type
from opteryx.models import Node
from opteryx.planner.logical_planner import LogicalPlan, LogicalPlanNode, LogicalPlanStepType

from .optimization_strategy import (
    OptimizationStrategy,
    OptimizerContext,
    get_nodes_of_type_from_logical_plan,
)


class NullabilityInferenceStrategy(OptimizationStrategy):
    """Synthesize implicit NOT IS NULL filters for INNER JOIN keys."""

    def __init__(self, telemetry=None):
        super().__init__(telemetry)

    def should_i_run(self, plan: LogicalPlan) -> bool:
        """Only run if there are JOIN clauses in the plan."""
        candidates = get_nodes_of_type_from_logical_plan(plan, (LogicalPlanStepType.Join,))
        return len(candidates) > 0

    def visit(self, node: LogicalPlanNode, context: OptimizerContext) -> OptimizerContext:
        """No per-node state collection needed; we'll analyze in complete()."""
        return context

    def complete(self, plan: LogicalPlan, context: OptimizerContext) -> LogicalPlan:
        """Synthesize NOT IS NULL filters for INNER JOIN keys."""
        optimized_plan = context.optimized_plan
        if len(optimized_plan) == 0:
            optimized_plan = context.pre_optimized_tree.copy()
            context.optimized_plan = optimized_plan

        # Find INNER JOINs
        join_nodes = get_nodes_of_type_from_logical_plan(optimized_plan, (LogicalPlanStepType.Join,))
        if not join_nodes:
            return optimized_plan

        print(f"[NULLABILITY_INFERENCE] Found {len(join_nodes)} join node(s)")

        first_join_nid = join_nodes[0]
        join_node = optimized_plan[first_join_nid]
        print(f"[NULLABILITY_INFERENCE] Join node type: {type(join_node)}, node_type: {join_node.node_type if hasattr(join_node, 'node_type') else 'N/A'}")
        print(f"[NULLABILITY_INFERENCE] Join node str: {str(join_node)}")
        print(f"[NULLABILITY_INFERENCE] Join node.__dict__: {join_node.__dict__ if hasattr(join_node, '__dict__') else 'N/A'}")

        # Only process INNER JOINs
        if not (hasattr(join_node, "type") and join_node.type == "inner"):
            print(f"[NULLABILITY_INFERENCE] Not an inner join, returning")
            return optimized_plan

        if not (hasattr(join_node, "on") and join_node.on is not None):
            print(f"[NULLABILITY_INFERENCE] No ON clause, returning")
            return optimized_plan

        # Collect left and right join key columns
        left_cols, right_cols = self._collect_join_key_columns_by_side(join_node.on)
        print(f"[NULLABILITY_INFERENCE] Collected left_cols={left_cols}, right_cols={right_cols}")

        if not left_cols and not right_cols:
            print(f"[NULLABILITY_INFERENCE] No join columns found, returning")
            return optimized_plan

        # Get existing null filters to avoid duplication
        existing_filters = self._get_existing_null_filters(optimized_plan)
        print(f"[NULLABILITY_INFERENCE] Existing null filters: {existing_filters}")

        # Get incoming edges to the join to identify left vs right inputs
        ingoing = list(optimized_plan.ingoing_edges(first_join_nid))
        print(f"[NULLABILITY_INFERENCE] Ingoing edges: {ingoing}")
        if len(ingoing) < 2:
            return optimized_plan

        # ingoing is a list of (source_nid, target_nid, relationship) tuples
        # Find which edge is left and which is right based on relationship
        left_input_source = None
        right_input_source = None

        for source, target, relationship in ingoing:
            if relationship == "left":
                left_input_source = source
            elif relationship == "right":
                right_input_source = source

        # Create left filter if needed
        if left_cols and left_input_source:
            left_cols_to_filter = left_cols - existing_filters
            print(f"[NULLABILITY_INFERENCE] Left columns to filter: {left_cols_to_filter}")
            if left_cols_to_filter:
                left_filters = self._synthesize_not_is_null_filters(left_cols_to_filter, set())
                left_chain = self._build_filter_chain(left_filters)
                if left_chain:
                    left_filter_nid = self._generate_node_id()
                    left_filter_node = LogicalPlanNode(
                        step_type=LogicalPlanStepType.Filter,
                        condition=left_chain,
                    )
                    print(f"[NULLABILITY_INFERENCE] Inserting left filter {left_filter_nid} before {left_input_source}")
                    optimized_plan.insert_node_before(left_filter_nid, left_filter_node, left_input_source)

        # Create right filter if needed
        if right_cols and right_input_source:
            right_cols_to_filter = right_cols - existing_filters
            print(f"[NULLABILITY_INFERENCE] Right columns to filter: {right_cols_to_filter}")
            if right_cols_to_filter:
                right_filters = self._synthesize_not_is_null_filters(right_cols_to_filter, set())
                right_chain = self._build_filter_chain(right_filters)
                if right_chain:
                    right_filter_nid = self._generate_node_id()
                    right_filter_node = LogicalPlanNode(
                        step_type=LogicalPlanStepType.Filter,
                        condition=right_chain,
                    )
                    print(f"[NULLABILITY_INFERENCE] Inserting right filter {right_filter_nid} before {right_input_source}")
                    optimized_plan.insert_node_before(right_filter_nid, right_filter_node, right_input_source)

        if self.telemetry:
            self.telemetry.optimization_nullability_inference += 1

        return optimized_plan

    def _collect_join_key_columns_by_side(self, on_expression) -> Tuple[Set[bytes], Set[bytes]]:
        """Extract join key columns by which side of equality they appear on.

        Returns (left_columns, right_columns) tuples of column identity bytes.
        """
        left_cols = set()
        right_cols = set()

        for node in get_all_nodes_of_type(on_expression, (NodeType.COMPARISON_OPERATOR,)):
            if node.value != "Eq":
                continue

            left = getattr(node, "left", None)
            right = getattr(node, "right", None)

            if left is None or right is None:
                continue

            # Left side column
            if left.node_type == NodeType.IDENTIFIER:
                col_id = getattr(left, "schema_column", None)
                if col_id is not None:
                    left_cols.add(col_id.identity)

            # Right side column
            if right.node_type == NodeType.IDENTIFIER:
                col_id = getattr(right, "schema_column", None)
                if col_id is not None:
                    right_cols.add(col_id.identity)

        return left_cols, right_cols

    def _collect_inner_join_key_columns(self, plan: LogicalPlan) -> Set[bytes]:
        """Collect all column identities that are INNER JOIN keys.

        Returns a set of column identity bytes (e.g., {b'table.column', ...}).
        """
        join_columns = set()

        join_nodes = get_nodes_of_type_from_logical_plan(plan, (LogicalPlanStepType.Join,))
        for join_nid in join_nodes:
            join_node = plan[join_nid]

            # Only process INNER JOINs
            if not hasattr(join_node, "type") or join_node.type != "inner":
                continue

            if not hasattr(join_node, "on") or join_node.on is None:
                continue

            # Extract equality comparisons from ON expression
            columns = self._extract_eq_comparison_columns(join_node.on)
            join_columns.update(columns)

        return join_columns

    def _extract_eq_comparison_columns(self, on_expression) -> Set[bytes]:
        """Extract column identities from equality comparisons in the ON expression.

        Finds all nodes matching: col1 = col2 (both IDENTIFIER nodes).
        """
        columns = set()

        for node in get_all_nodes_of_type(on_expression, (NodeType.COMPARISON_OPERATOR,)):
            if node.value != "Eq":
                continue

            left = getattr(node, "left", None)
            right = getattr(node, "right", None)

            if left is None or right is None:
                continue

            # Both sides should be identifiers for join keys
            if left.node_type == NodeType.IDENTIFIER:
                col_id = getattr(left, "schema_column", None)
                if col_id is not None:
                    columns.add(col_id.identity)

            if right.node_type == NodeType.IDENTIFIER:
                col_id = getattr(right, "schema_column", None)
                if col_id is not None:
                    columns.add(col_id.identity)

        return columns

    def _get_existing_null_filters(self, plan: LogicalPlan) -> Set[bytes]:
        """Collect column identities that already have explicit IS NOT NULL filters.

        Returns a set of column identity bytes.
        """
        null_filtered_columns = set()

        filter_nodes = get_nodes_of_type_from_logical_plan(plan, (LogicalPlanStepType.Filter,))
        for filter_nid in filter_nodes:
            filter_node = plan[filter_nid]

            if not hasattr(filter_node, "condition") or filter_node.condition is None:
                continue

            # Look for IS NOT NULL nodes in the filter condition
            for node in get_all_nodes_of_type(filter_node.condition, (NodeType.UNARY_OPERATOR,)):
                if node.value != "IsNotNull":
                    continue

                centre = getattr(node, "centre", None)
                if centre is not None and centre.node_type == NodeType.IDENTIFIER:
                    col_id = getattr(centre, "schema_column", None)
                    if col_id is not None:
                        null_filtered_columns.add(col_id.identity)

        return null_filtered_columns

    def _synthesize_not_is_null_filters(
        self, join_key_columns: Set[bytes], existing_filters: Set[bytes]
    ) -> list:
        """Create NOT IS NULL filter nodes for join keys without existing filters.

        Returns a list of Node objects (UNARY_OPERATOR with IsNotNull).
        """
        filters = []

        for col_id in join_key_columns:
            if col_id in existing_filters:
                continue  # Already has explicit IS NOT NULL

            # Create Identifier node (stub - will be filled in by binder if needed)
            identifier = Node(NodeType.IDENTIFIER, value="unknown")
            identifier.schema_column = type('obj', (object,), {'identity': col_id})()

            # Create IS NOT NULL unary operator
            is_not_null = Node(NodeType.UNARY_OPERATOR, value="IsNotNull", centre=identifier)

            filters.append(is_not_null)

        return filters

    def _build_filter_chain(self, filter_nodes: list) -> Optional[Node]:
        """Build an AND chain of filter nodes.

        If filters is empty or has one element, return as-is or None.
        Otherwise, chain them with AND operators.
        """
        if not filter_nodes:
            return None

        if len(filter_nodes) == 1:
            return filter_nodes[0]

        # Build AND chain: (a AND b) AND c
        result = filter_nodes[0]
        for filter_node in filter_nodes[1:]:
            result = Node(NodeType.AND, left=result, right=filter_node)

        return result

    @staticmethod
    def _generate_node_id() -> str:
        """Generate a unique node ID."""
        import uuid
        return str(uuid.uuid4())
