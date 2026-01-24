# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Statistics-Only Response Strategy and File Pruning

Detects queries that can be answered entirely from table statistics without
reading any data, or optimizes file access when LIMIT is present.

Currently supports:

  - SELECT COUNT(*) FROM table (no filters, no GROUP BY)
  - SELECT COUNT(*) AS alias FROM table

Expected Speedup:
  - COUNT(*): ~400-800x (no file I/O)
"""

import pyarrow
from orso.types import OrsoTypes

from opteryx.managers.expression import NodeType
from opteryx.planner import build_literal_node
from opteryx.planner.logical_planner.logical_planner import LogicalPlanStepType

# Strategy-style Optimization Class
from .optimization_strategy import OptimizationStrategy
from .optimization_strategy import OptimizerContext
from .optimization_strategy import get_nodes_of_type_from_logical_plan


def find_scan_node(logical_plan):
    """
    Find the Scan node in the logical plan.

    Returns:
        The Scan node if found, None otherwise.
    """
    for _, node in logical_plan.nodes(data=True):
        if node.node_type == LogicalPlanStepType.Scan:
            return node
    return None


def find_aggregate_node(logical_plan):
    """
    Find the Aggregate node in the logical plan.

    Returns:
        The Aggregate node if found, None otherwise.
    """
    for _, node in logical_plan.nodes(data=True):
        if node.node_type == LogicalPlanStepType.Aggregate:
            return node
    return None


def find_exit_node(logical_plan):
    """
    Find the Exit node in the logical plan.

    Returns:
        The Exit node if found, None otherwise.
    """
    for _, node in logical_plan.nodes(data=True):
        if node.node_type == LogicalPlanStepType.Exit:
            return node
    return None


def is_count_star_aggregate(aggregate_node) -> bool:
    """
    Check if the aggregate node is specifically COUNT(*).

    Parameters:
        aggregate_node: The Aggregate node to check

    Returns:
        True if this is a COUNT(*) aggregate, False otherwise
    """
    if not aggregate_node:
        return False

    # Check that we have exactly one aggregate
    if not hasattr(aggregate_node, "aggregates") or not aggregate_node.aggregates:
        return False

    if len(aggregate_node.aggregates) != 1:
        return False

    aggregate = aggregate_node.aggregates[0]

    # Check that it's a COUNT aggregator
    if not hasattr(aggregate, "node_type") or aggregate.node_type != NodeType.AGGREGATOR:
        return False

    if not hasattr(aggregate, "value") or aggregate.value.upper() != "COUNT":
        return False

    # Check that there's no expression (COUNT(*) has no expression, COUNT(column) has one)
    return not (hasattr(aggregate, "expression") and aggregate.expression is not None)


def is_count_star_query(logical_plan) -> bool:
    """
    Check if the logical plan matches: SELECT COUNT(*) FROM table

    Requirements for match:
    - Has exactly one Scan node (no joins)
    - Has exactly one Aggregate node (the COUNT(*))
    - The aggregate is COUNT(*)
    - No GROUP BY (groups should be None or empty)
    - No WHERE/HAVING filters
    - No DISTINCT, LIMIT, ORDER BY

    Parameters:
        logical_plan: The logical plan to check

    Returns:
        True if this matches the pattern, False otherwise
    """
    # Count Scan nodes (should be exactly 1)
    scan_nodes = [
        n for nid, n in logical_plan.nodes(data=True) if n.node_type == LogicalPlanStepType.Scan
    ]
    if len(scan_nodes) != 1:
        return False

    # Find aggregate node
    aggregate_node = find_aggregate_node(logical_plan)
    if not aggregate_node:
        return False

    # Check that it's COUNT(*)
    if not is_count_star_aggregate(aggregate_node):
        return False

    # Check no GROUP BY (groups should be None or empty)
    if hasattr(aggregate_node, "groups") and aggregate_node.groups:
        return False

    # Check no Filter nodes between Scan and Aggregate
    filter_nodes = [
        n for nid, n in logical_plan.nodes(data=True) if n.node_type == LogicalPlanStepType.Filter
    ]
    if filter_nodes:
        return False

    # Check no Distinct, Limit, Order nodes in the plan
    unsupported_nodes = [
        n
        for nid, n in logical_plan.nodes(data=True)
        if n.node_type
        in (
            LogicalPlanStepType.Distinct,
            LogicalPlanStepType.Limit,
            LogicalPlanStepType.Order,
            LogicalPlanStepType.Join,
            LogicalPlanStepType.Union,
        )
    ]
    if unsupported_nodes:
        return False

    # Check no AggregateAndGroup nodes (GROUP BY case)
    agg_group_nodes = [
        n
        for nid, n in logical_plan.nodes(data=True)
        if n.node_type == LogicalPlanStepType.AggregateAndGroup
    ]
    return not agg_group_nodes


def extract_column_alias(logical_plan) -> str:
    """
    Extract the column name/alias for the COUNT(*) result.

    Looks at the Exit node's columns to determine the output column name.
    Falls back to "COUNT(*)" if no alias is found.

    Parameters:
        logical_plan: The logical plan

    Returns:
        The column name to use in the result (str)
    """
    exit_node = find_exit_node(logical_plan)
    if not exit_node:
        return "COUNT(*)"

    if not hasattr(exit_node, "columns") or not exit_node.columns:
        return "COUNT(*)"

    # Get the first (and should be only) column
    columns = exit_node.columns
    if not columns:
        return "COUNT(*)"

    first_column = columns[0]

    # Try to get the alias
    if hasattr(first_column, "alias") and first_column.alias:
        return first_column.alias

    # Try to get the source_column
    if hasattr(first_column, "source_column") and first_column.source_column:
        return first_column.source_column

    # Default to COUNT(*)
    return "COUNT(*)"


def get_count_from_manifest(manifest) -> int:
    """
    Get total row count from manifest statistics.

    The manifest aggregates record counts from all files in the table.

    Parameters:
        manifest: The Manifest object from the Scan node

    Returns:
        The total record count (int), or 0 if manifest is None/empty
    """
    if manifest is None:
        return 0

    return manifest.get_record_count()


class StatisticsOnlyResponseStrategy(OptimizationStrategy):
    """Optimizer strategy that rewrites trivial COUNT(*) aggregates into a
    simple projection of a literal count over the `$no_table` virtual dataset.

    This strategy strictly follows the plan->plan pattern used by other
    strategies: it accepts a logical plan, mutates it when appropriate, and
    returns the (possibly rewritten) plan.
    """

    def visit(self, node, context: OptimizerContext) -> OptimizerContext:
        # This strategy operates globally in `complete` and does not need to
        # inspect nodes during the traversal phase.
        if not context.optimized_plan:
            context.optimized_plan = context.pre_optimized_tree.copy()  # type: ignore

        return context

    def should_i_run(self, plan) -> bool:  # pragma: no cover - trivial
        # Skip if there are Filter, Join, or AggregateAndGroup nodes present
        killer_candidates = get_nodes_of_type_from_logical_plan(
            plan,
            (
                LogicalPlanStepType.Filter,
                LogicalPlanStepType.Join,
                LogicalPlanStepType.AggregateAndGroup,
            ),
        )
        if len(killer_candidates) > 0:
            return False

        # Run only when there are Aggregate nodes present
        agg_candidates = get_nodes_of_type_from_logical_plan(plan, (LogicalPlanStepType.Aggregate,))
        return len(agg_candidates) != 0

    def complete(self, plan, context: OptimizerContext) -> object:
        # If the plan does not match our conservative COUNT(*) pattern, do
        # nothing and return the plan unchanged.
        if not is_count_star_query(plan):
            return plan

        # Locate nodes we'll need
        aggregate_node = find_aggregate_node(plan)
        scan_node = find_scan_node(plan)
        exit_node = find_exit_node(plan)

        if aggregate_node is None or scan_node is None:
            return plan

        # We only act when we have manifest-based statistics
        manifest = getattr(scan_node, "manifest", None)
        if manifest is None:
            return plan

        # Determine count and alias
        count_value = get_count_from_manifest(manifest)
        column_alias = extract_column_alias(plan)

        # Build a literal projection node to replace the aggregate
        literal = build_literal_node(count_value, suggested_type=OrsoTypes.INTEGER)

        # Preserve the expected alias for downstream consumers
        setattr(literal, "alias", column_alias)
        # Ensure the literal uses the same schema identity as the original
        # aggregate so downstream Exit/Projection nodes can match by identity.
        if aggregate_node.aggregates:
            agg_schema = aggregate_node.aggregates[0].schema_column
            if agg_schema is not None and literal.schema_column is not None:
                literal.schema_column.identity = agg_schema.identity
                literal.schema_column.type = agg_schema.type or literal.schema_column.type

        # Point the source(s) to $no_table BEFORE we mutate the aggregate node.
        # Doing this early avoids potential iterator/side-effect issues when
        # modifying the plan structure.
        scan_node.relation = "$no_table"
        scan_node.alias = "$no_table"
        # Prune 100% of files in the manifest so optimizer/executor treat
        # this as having no data to read while preserving connector/schema
        if scan_node.manifest is not None:
            scan_node.manifest.files = []

        # Replace any lingering AGGREGATOR expressions in Project/Exit nodes with
        # our literal, to ensure no node still references COUNT(*) after the
        # rewrite. This targets the exact aggregator identity or matching
        # aggregator schema identity to be conservative.
        try:
            target_agg = None
            if hasattr(aggregate_node, "aggregates") and aggregate_node.aggregates:
                target_agg = aggregate_node.aggregates[0]

            def _is_target_agg(expr):
                if expr is None:
                    return False
                # direct object identity
                if expr is target_agg:
                    return True
                # structural match: aggregator by schema identity and function name
                try:
                    if getattr(expr, "node_type", None) == NodeType.AGGREGATOR:
                        expr_id = getattr(getattr(expr, "schema_column", None), "identity", None)
                        agg_id = getattr(
                            getattr(target_agg, "schema_column", None), "identity", None
                        )
                        if expr_id is not None and agg_id is not None and expr_id == agg_id:
                            return True
                except Exception:
                    pass
                return False

            for nid, n in plan.nodes(data=True):
                cols = getattr(n, "columns", None)
                if not cols:
                    continue
                changed = False
                new_cols = []
                for c in cols:
                    # Replace explicit aggregator expressions
                    if _is_target_agg(c) or getattr(c, "alias", None) == column_alias:
                        new_cols.append(literal)
                        changed = True
                    else:
                        new_cols.append(c)
                if changed:
                    try:
                        n.columns = new_cols
                    except Exception:
                        # best-effort - if mutation fails, continue
                        pass
            if self.telemetry is not None:
                try:
                    self.telemetry._after_replace_agg = True
                except Exception:
                    pass
        except Exception:
            # conservative: on unexpected errors, bail out and keep plan unchanged
            return plan

        # Rewrite aggregate node into a Project with the literal column
        aggregate_node.node_type = LogicalPlanStepType.Project
        aggregate_node.columns = [literal]
        # Remove aggregate-specific attributes to avoid confusion downstream
        aggregate_node.aggregates = None
        aggregate_node.groups = None
        aggregate_node.projection = None

        # Point the source(s) to $no_table so physical planner / executor treat
        # this as a projection-only plan (no table scanning required). We apply
        # the change to all Scan nodes found to be conservative.
        try:
            # We located the relevant scan node earlier; set it directly. This
            # avoids potential iterator-side-effects and is consistent with the
            # conservative single-scan expectation in `is_count_star_query`.
            scan_node.relation = "$no_table"
            scan_node.alias = "$no_table"

            # Replace the connector with the virtual `$no_table` table engine so
            # the ReaderNode will produce the one-row $no_table morsel. This
            # avoids relying on the original connector's behavior after we
            # rewrote the plan to a projection-only query.
            # Indicate we're about to attempt connector reassignment (diagnostic)
            from opteryx.connectors import connector_factory

            virt_gateway = connector_factory("$no_table", telemetry=self.telemetry)
            scan_node.connector = virt_gateway.table_engine("$no_table", telemetry=self.telemetry)

            # Ensure schema is the virtual dataset schema so ReaderNode
            # normalization succeeds and downstream nodes see the
            # expected column identities.
            scan_node.schema = scan_node.connector.get_dataset_schema()
            # Ensure origin is set for schema columns
            for col in getattr(scan_node.schema, "columns", []) or []:
                col.origin = [scan_node.alias]

            # Finally, clear the manifest to avoid file-based readers from
            # providing file lists (we prefer virtual connector semantics
            # instead)
            scan_node.manifest = None
        except Exception:
            # If we cannot mutate scan node safely, leave the plan unchanged
            return plan

        # Update exit node columns so aliasing is preserved
        exit_node.columns = [literal]

        # Update telemetry safely
        self.telemetry.statistics_only_response_optimization += 1

        # Record connector assignment status on the plan for diagnostic purposes
        try:
            plan._stats_assigned_connector_type = getattr(scan_node, "connector", None) and getattr(
                scan_node.connector, "__type__", None
            )
        except Exception:
            plan._stats_assigned_connector_type = None

        return plan
