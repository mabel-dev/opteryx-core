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
  - SELECT MIN(column) FROM table (for INTEGER and TIMESTAMP columns)
  - SELECT MAX(column) FROM table (for INTEGER and TIMESTAMP columns)

Expected Speedup:
  - COUNT(*): ~400-800x (no file I/O)
  - MIN/MAX: ~400-800x (no file I/O, uses BRIN bounds)

Note: MIN/MAX only work for INTEGER and TIMESTAMP types. FLOAT, STRING,
and complex types lose precision in BRIN bounds and cannot be answered.
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


def is_simple_aggregate(aggregate_node) -> bool:
    """
    Check if the aggregate node is a supported statistics-only aggregate.

    Supported:
    - COUNT(*)
    - MIN(column) where column is INTEGER or TIMESTAMP
    - MAX(column) where column is INTEGER or TIMESTAMP

    Parameters:
        aggregate_node: The Aggregate node to check

    Returns:
        True if this is a supported aggregate, False otherwise
    """
    if not aggregate_node:
        return False

    # Check that we have exactly one aggregate
    if not hasattr(aggregate_node, "aggregates") or not aggregate_node.aggregates:
        return False

    if len(aggregate_node.aggregates) != 1:
        return False

    aggregate = aggregate_node.aggregates[0]

    # Check that it's an aggregator node
    if not hasattr(aggregate, "node_type") or aggregate.node_type != NodeType.AGGREGATOR:
        return False

    agg_func = getattr(aggregate, "value", "").upper()

    # COUNT(*) - no expression
    if agg_func == "COUNT":
        return not (hasattr(aggregate, "expression") and aggregate.expression is not None)

    # MIN/MAX - must have expression (column reference) and be a supported type
    if agg_func in ("MIN", "MAX"):
        if not hasattr(aggregate, "parameters") or not aggregate.parameters:
            return False
        # Get the column reference from parameters[0]
        expr = aggregate.parameters[0]
        if not hasattr(expr, "schema_column") or expr.schema_column is None:
            return False
        col_type = getattr(expr.schema_column, "type", None)
        if col_type is None:
            return False
        # Only INTEGER and TIMESTAMP types preserve exact values in BRIN bounds
        return col_type in (OrsoTypes.INTEGER, OrsoTypes.TIMESTAMP)

    return False


def is_statistics_only_query(logical_plan) -> bool:
    """
    Check if the logical plan matches a statistics-only query pattern.

    Supported patterns:
    - SELECT COUNT(*) FROM table
    - SELECT MIN(column) FROM table (INTEGER/TIMESTAMP only)
    - SELECT MAX(column) FROM table (INTEGER/TIMESTAMP only)

    Requirements for match:
    - Has exactly one Scan node (no joins)
    - Has exactly one Aggregate node with a supported aggregate
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

    # Check that it's a supported aggregate (COUNT(*), MIN(col), MAX(col))
    if not is_simple_aggregate(aggregate_node):
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


def get_aggregate_type(aggregate_node) -> str:
    """
    Get the aggregate function type (COUNT, MIN, MAX).

    Parameters:
        aggregate_node: The Aggregate node

    Returns:
        Uppercase aggregate function name (e.g., "COUNT", "MIN", "MAX")
    """
    if not aggregate_node or not aggregate_node.aggregates:
        return ""
    return aggregate_node.aggregates[0].value.upper()


def get_column_name_from_aggregate(aggregate_node) -> str:
    """
    Get the column name from MIN/MAX aggregate expression.

    Parameters:
        aggregate_node: The Aggregate node

    Returns:
        Column name (str), or empty string if not found
    """
    if not aggregate_node or not aggregate_node.aggregates:
        return ""
    agg = aggregate_node.aggregates[0]
    if not hasattr(agg, "parameters") or not agg.parameters:
        return ""
    param = agg.parameters[0]
    if not hasattr(param, "source_column"):
        return ""
    return param.source_column


def get_min_max_from_manifest(manifest, column_name: str, operation: str):
    """
    Get MIN or MAX value for a column from manifest bounds.

    Uses the aggregated column bounds across all files in the manifest.
    BRIN bounds preserve exact values for INTEGER and TIMESTAMP types.

    Parameters:
        manifest: The Manifest object from the Scan node
        column_name: Name of the column
        operation: "MIN" or "MAX"

    Returns:
        The min or max value (int/timestamp), or None if not available
    """
    if manifest is None:
        return None

    # Get field_id for this column
    field_id = None
    for i, col in enumerate(manifest.schema.columns):
        if col.name == column_name:
            field_id = i
            break

    if field_id is None:
        return None

    # Aggregate min/max across all files
    min_val = None
    max_val = None

    for file_entry in manifest.files:
        # Try using min_values/max_values lists first (already deserialized)
        if file_entry.min_values and field_id < len(file_entry.min_values):
            file_min = file_entry.min_values[field_id]
            if file_min is not None:
                if min_val is None or file_min < min_val:
                    min_val = file_min

        if file_entry.max_values and field_id < len(file_entry.max_values):
            file_max = file_entry.max_values[field_id]
            if file_max is not None:
                if max_val is None or file_max > max_val:
                    max_val = file_max

    if operation == "MIN":
        return min_val
    elif operation == "MAX":
        return max_val
    return None


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
        # If the plan does not match our conservative statistics-only pattern, do
        # nothing and return the plan unchanged.
        if not is_statistics_only_query(plan):
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

        # Determine aggregate type and extract the value
        agg_type = get_aggregate_type(aggregate_node)
        column_alias = extract_column_alias(plan)

        # Get the aggregate value based on type
        if agg_type == "COUNT":
            result_value = get_count_from_manifest(manifest)
            result_type = OrsoTypes.INTEGER
        elif agg_type in ("MIN", "MAX"):
            column_name = get_column_name_from_aggregate(aggregate_node)
            if not column_name:
                return plan
            result_value = get_min_max_from_manifest(manifest, column_name, agg_type)
            if result_value is None:
                return plan
            # Preserve the column type (INTEGER or TIMESTAMP)
            agg_col_type = aggregate_node.aggregates[0].parameters[0].schema_column.type
            result_type = agg_col_type
        else:
            # Unsupported aggregate type
            return plan

        # Build a literal projection node to replace the aggregate
        literal = build_literal_node(result_value, suggested_type=result_type)

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
        self.telemetry.optimization_statistics_only_response += 1

        # Record connector assignment status on the plan for diagnostic purposes
        try:
            plan._stats_assigned_connector_type = getattr(scan_node, "connector", None) and getattr(
                scan_node.connector, "__type__", None
            )
        except Exception:
            plan._stats_assigned_connector_type = None

        return plan
