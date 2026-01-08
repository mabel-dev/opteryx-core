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

from typing import Optional

import pyarrow

from opteryx.managers.expression import NodeType
from opteryx.planner.logical_planner.logical_planner import LogicalPlanStepType


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


def create_count_result_table(count_value: int, column_alias: str) -> pyarrow.Table:
    """
    Create a PyArrow table with the COUNT(*) result.

    Creates a single-row, single-column table with:
    - Column name: column_alias (e.g., "COUNT(*)" or "total_rows")
    - Value: count_value (int64)
    - Schema metadata: "disposition" = "statistics"

    Parameters:
        count_value: The count from manifest
        column_alias: The output column name

    Returns:
        PyArrow Table with the result
    """
    # Create the table with int64 type to match COUNT(*) behavior
    table = pyarrow.table({column_alias: pyarrow.array([count_value], type=pyarrow.int64())})

    # Add metadata to indicate this came from statistics
    metadata = table.schema.metadata or {}
    metadata[b"disposition"] = b"statistics"

    table = table.replace_schema_metadata(metadata)

    return table


def try_statistics_only_response(logical_plan) -> Optional[pyarrow.Table]:
    """
    Try to answer query using statistics only, without reading any data.

    This function attempts to detect a query that can be answered entirely from
    table statistics (e.g., COUNT(*)) and return the result directly without
    executing the full query plan.

    Currently supported:
      - SELECT COUNT(*) FROM table

    Future support:
      - SELECT MIN/MAX without filters
      - File pruning with LIMIT pushdown

    Parameters:
        logical_plan: The logical plan to optimize

    Returns:
        PyArrow Table with the result if optimization succeeds, None otherwise
    """
    # Check if this query matches our pattern
    if not is_count_star_query(logical_plan):
        return None

    # Get the Scan node to access the manifest
    scan_node = find_scan_node(logical_plan)
    if scan_node is None:
        return None

    # Check that manifest is available
    if not hasattr(scan_node, "manifest") or scan_node.manifest is None:
        return None

    # Get count from manifest
    count_value = get_count_from_manifest(scan_node.manifest)

    # Get the output column alias
    column_alias = extract_column_alias(logical_plan)

    # Create and return the result table
    result_table = create_count_result_table(count_value, column_alias)

    return result_table
