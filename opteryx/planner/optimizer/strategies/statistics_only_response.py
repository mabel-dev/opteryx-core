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
  - SELECT MIN(column) FROM table (for DATE, INTEGER and TIMESTAMP columns)
  - SELECT MAX(column) FROM table (for DATE, INTEGER and TIMESTAMP columns)

Expected Speedup:
  - COUNT(*): ~400-800x (no file I/O)
  - MIN/MAX: ~400-800x (no file I/O, uses BRIN bounds)

Note: MIN/MAX work for DATE, INTEGER and TIMESTAMP types. FLOAT, STRING,
and complex types lose precision in BRIN bounds and cannot be answered.
"""

from typing import Optional

from opteryx.expression import NodeType
from opteryx.expression import get_all_nodes_of_type
from opteryx.models import LogicalColumn
from opteryx.planner import build_literal_node
from opteryx.planner.logical_planner.logical_planner import LogicalPlanStepType
from opteryx.types.logical_type import LogicalCategory, INT64 as _CT_INT64

# Strategy-style Optimization Class
from .optimization_strategy import (
    OptimizationStrategy,
    OptimizerContext,
    get_nodes_of_type_from_logical_plan,
)


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
    Check if the aggregate node contains supported statistics-only aggregates.

    Supported:
    - COUNT(*)
    - COUNT(column) — answered as total_rows - null_count when manifest has nulls
    - MIN(column) where column is INTEGER or TIMESTAMP
    - MAX(column) where column is INTEGER or TIMESTAMP

    Supports one or more aggregates, as long as each individually is supported.

    Parameters:
        aggregate_node: The Aggregate node to check

    Returns:
        True if all aggregates are supported, False if any are unsupported
    """
    if not aggregate_node:
        return False

    # Check that we have at least one aggregate
    if not aggregate_node.aggregates:
        return False

    # Validate each aggregate in the list
    for aggregate in aggregate_node.aggregates:
        # Check that it's an aggregator node
        if aggregate.node_type != NodeType.AGGREGATOR:
            return False

        agg_func = getattr(aggregate, "value", "").upper()

        # COUNT(*) or COUNT(col) - must not be DISTINCT/FILTER.
        # COUNT(*) reads from manifest record count.
        # COUNT(col) reads from manifest record count minus null_count;
        # the null_count availability check happens in `complete()`.
        if agg_func == "COUNT":
            if aggregate.duplicate_treatment == "Distinct":
                return False

            if aggregate.condition is not None:
                return False

            parameters = getattr(aggregate, "parameters", None)
            if not parameters or len(parameters) != 1:
                return False

            param = parameters[0]
            param_kind = getattr(param, "node_type", None)
            if param_kind == NodeType.WILDCARD:
                continue
            # Column reference: must carry a resolvable schema column with a name
            if getattr(param, "schema_column", None) is None:
                return False
            if not getattr(param, "source_column", None):
                return False
            continue

        # MIN/MAX - must have expression (column reference) and be a supported type
        if agg_func in ("MIN", "MAX"):
            if not aggregate.parameters:
                return False
            # Get the column reference from parameters[0]
            expr = aggregate.parameters[0]
            if expr.schema_column is None:
                return False
            col_type = getattr(expr.schema_column, "category", None)
            if col_type is None:
                return False
            # This allowlist is the ONLY thing standing between `MIN`/`MAX` and a
            # wrong answer, because `get_min_max_from_manifest` returns whatever
            # the bounds hold without re-checking anything. Two invariants make
            # these three categories safe, and BOTH must hold for anything added:
            #
            # 1. ORDINAL BOUNDS ARE THE VALUE. An ANALYZE/skene manifest carries
            #    `Vector.ordinalize()` int64 keys, not decoded values, and this
            #    strategy hands the bound straight back as the answer. For DATE,
            #    INTEGER and TIMESTAMP ordinalize is an identity widen from
            #    INT32/INT64 (ColumnType.ordinalize says so; `INT64.ordinalize(-5)`
            #    is `-5`), so the ordinal IS the value. FLOAT's is not — it is a
            #    monotonic bit-twiddle, and `FLOAT64.ordinalize(3.5)` is
            #    4615063718147915776. Admitting FLOAT here would answer
            #    `MIN(density)` with that integer.
            # 2. THE BOUNDS COVER EVERY VALUE. Only a float can hold a NaN, and
            #    Parquet excludes NaN from min/max by spec while draken ranks NaN
            #    ABOVE everything (float_ops.h, architect-locked 2026-05-22). So a
            #    float `MAX` read off parquet-sourced bounds is the largest FINITE
            #    value where the engine says NaN. (`MIN` does not have this
            #    problem — NaN is never the minimum — so FLOAT `MIN` is the one
            #    piece of this that could be turned on, deliberately and alone.)
            #
            # `tests/unit/optimizer/statistics/test_statistics_only_min_max_type_gate.py`
            # asserts both invariants against the list, so widening it without
            # satisfying them fails rather than silently answering wrongly.
            if col_type not in (LogicalCategory.DATE, LogicalCategory.INTEGER, LogicalCategory.TIMESTAMP):
                return False
            continue

        # Unsupported aggregate type
        return False

    return True


def is_statistics_only_query(logical_plan) -> bool:
    """
    Check if the logical plan matches a statistics-only query pattern.

    Supported patterns:
    - SELECT COUNT(*) FROM table
    - SELECT COUNT(column) FROM table
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
    if aggregate_node.groups:
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
            # A CROSS JOIN UNNEST is LogicalPlanStepType.Unnest, NOT .Join, so it
            # slipped past the Join guard above. It must be listed in its own right:
            # the answer to COUNT(*) over an unnest is the SUM OF THE ARRAY LENGTHS,
            # which no manifest statistic records — the scan's row count is a
            # different number entirely. The rewrite replaced the scan with a
            # $no_table manifest count, which left the unnest with no source column
            # and the query died as "a CROSS JOIN UNNEST source array the engine
            # could not resolve here". The refusal was luck: the same rewrite over a
            # plan that still resolved would have returned the PARENT row count as
            # if it were the unnested one.
            LogicalPlanStepType.Unnest,
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
    Extract the column name/alias for a single aggregate result (deprecated for multi-aggregate).

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

    if not exit_node.columns:
        return "COUNT(*)"

    # Get the first (and should be only) column
    columns = exit_node.columns
    if not columns:
        return "COUNT(*)"

    first_column = columns[0]

    # Try to get the alias
    if first_column.alias:
        return first_column.alias

    # Try to get the source_column
    if first_column.source_column:
        return first_column.source_column

    # Default to COUNT(*)
    return "COUNT(*)"


def extract_all_column_aliases(logical_plan) -> list:
    """
    Extract all column name/aliases from the Exit node.

    Returns aliases in the same order as the aggregates.
    Falls back to default names if aliases not found.

    Parameters:
        logical_plan: The logical plan

    Returns:
        List of column names/aliases (one per aggregate)
    """
    exit_node = find_exit_node(logical_plan)
    if not exit_node:
        return []

    if not exit_node.columns:
        return []

    aliases = []
    for column in exit_node.columns:
        # Try to get the alias
        if column.alias:
            aliases.append(column.alias)
        # Try to get the source_column
        elif column.source_column:
            aliases.append(column.source_column)
        else:
            # Default fallback
            aliases.append(f"col_{len(aliases)}")

    return aliases


def extract_alias_by_identity(logical_plan) -> dict:
    """Map each Exit column's schema identity → its output alias.

    Used to align aliases to aggregates by identity rather than by position:
    the Exit node's column order need not match aggregate_node.aggregates order.
    """
    exit_node = find_exit_node(logical_plan)
    if not exit_node or not getattr(exit_node, "columns", None):
        return {}

    mapping = {}
    for column in exit_node.columns:
        identity = getattr(getattr(column, "schema_column", None), "identity", None)
        if identity is None:
            continue
        if getattr(column, "alias", None):
            mapping[identity] = column.alias
        elif getattr(column, "source_column", None):
            mapping[identity] = column.source_column
    return mapping


def _replace_nested_aggregators(node, agg_identity_to_literal: dict):
    """Recursively replace AGGREGATOR nodes within an expression tree with their
    literal replacement, in place.

    The aggregate is not always the column itself — it can be embedded inside a
    wrapping expression (e.g. HUMANIZE(COUNT(*)), COUNT(*) + 1). A caller that only
    matches the top-level column's own identity/alias leaves a nested AGGREGATOR
    dangling, still referencing an identity no node in the rewritten plan carries.
    """
    if node is None:
        return node

    if node.node_type == NodeType.AGGREGATOR:
        agg_id = getattr(getattr(node, "schema_column", None), "identity", None)
        return agg_identity_to_literal.get(agg_id, node)

    if node.parameters:
        if isinstance(node.parameters, tuple):
            node.parameters = list(node.parameters)
        node.parameters = [
            _replace_nested_aggregators(p, agg_identity_to_literal) for p in node.parameters
        ]

    # NodeType.CASE uses conditions/results/else_result instead of parameters
    if node.node_type == NodeType.CASE:
        if node.conditions:
            node.conditions = [
                _replace_nested_aggregators(c, agg_identity_to_literal) for c in node.conditions
            ]
        if node.results:
            node.results = [
                _replace_nested_aggregators(r, agg_identity_to_literal) for r in node.results
            ]
        if node.else_result is not None:
            node.else_result = _replace_nested_aggregators(node.else_result, agg_identity_to_literal)

    if node.right is not None:
        node.right = _replace_nested_aggregators(node.right, agg_identity_to_literal)
    if node.centre is not None:
        node.centre = _replace_nested_aggregators(node.centre, agg_identity_to_literal)
    if node.left is not None:
        node.left = _replace_nested_aggregators(node.left, agg_identity_to_literal)

    return node


def get_count_from_manifest(manifest) -> Optional[int]:
    """
    Get total row count from manifest statistics.

    The manifest aggregates record counts from all files in the table.

    Parameters:
        manifest: The Manifest object from the Scan node

    Returns:
        The total record count (int), or None when the count is UNKNOWN - no
        manifest, or a manifest holding a file whose row count nobody computed.
        None is NOT 0: this number is handed straight back as the answer to
        COUNT(*) with the scan removed, so an unknown reported as 0 is a silent
        wrong answer. The caller must abandon the rewrite on None.
    """
    if manifest is None:
        return None

    return manifest.get_record_count()


def get_aggregate_type(aggregate_node) -> str:
    """
    Get the aggregate function type (COUNT, MIN, MAX) for single aggregate (deprecated).

    Parameters:
        aggregate_node: The Aggregate node

    Returns:
        Uppercase aggregate function name (e.g., "COUNT", "MIN", "MAX")
    """
    if not aggregate_node or not aggregate_node.aggregates:
        return ""
    return aggregate_node.aggregates[0].value.upper()


def get_all_aggregate_metadata(aggregate_node) -> list:
    """
    Extract metadata for all aggregates in the node.

    Returns list of (agg_func, column_name, aggregate) tuples:
    - agg_func: "COUNT", "MIN", or "MAX"
    - column_name: column name (empty string for COUNT(*))
    - aggregate: the aggregate node itself

    Parameters:
        aggregate_node: The Aggregate node

    Returns:
        List of tuples, empty list if no aggregates
    """
    if not aggregate_node or not aggregate_node.aggregates:
        return []

    metadata = []
    for agg in aggregate_node.aggregates:
        agg_func = getattr(agg, "value", "").upper()

        if agg_func == "COUNT":
            param = agg.parameters[0] if agg.parameters else None
            if param is not None and getattr(param, "node_type", None) != NodeType.WILDCARD:
                column_name = getattr(param, "source_column", "") or ""
            else:
                column_name = ""
        elif agg_func in ("MIN", "MAX"):
            param = agg.parameters[0] if agg.parameters else None
            column_name = getattr(param, "source_column", "") if param else ""
        else:
            column_name = ""

        metadata.append((agg_func, column_name, agg))

    return metadata


def get_column_name_from_aggregate(aggregate_node) -> str:
    """
    Get the column name from MIN/MAX aggregate expression (deprecated for single agg).

    Parameters:
        aggregate_node: The Aggregate node

    Returns:
        Column name (str), or empty string if not found
    """
    if not aggregate_node or not aggregate_node.aggregates:
        return ""
    agg = aggregate_node.aggregates[0]
    if not agg.parameters:
        return ""
    param = agg.parameters[0]
    return param.source_column or ""


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

    # The manifest owns this mapping: per-file stats are keyed by the column's
    # LOAD-TIME position, and by now projection pushdown has pruned
    # manifest.schema to just the referenced columns. Resolving the position here
    # against that pruned schema silently read a different column's bounds —
    # MAX(followers) answered with MAX(tweet_id) once followers was the only
    # column left (index 0, the file's tweet_id slot).
    field_id = manifest._resolve_field_id(column_name)
    if field_id is None:
        return None

    # Aggregate min/max across all files
    min_val = None
    max_val = None

    for file_entry in manifest.files:
        if file_entry.column_stats is not None:
            file_min = file_entry.column_stats.get_min(field_id)
            file_max = file_entry.column_stats.get_max(field_id)
        elif file_entry.lower_bounds is not None or file_entry.upper_bounds is not None:
            # lower_bounds/upper_bounds are keyed by field_id (not raw list
            # position) — see FileEntry.from_datafile. Indexing the positional
            # min_values/max_values lists by field_id here would be wrong
            # whenever field_id isn't a small schema-start-relative position,
            # which is exactly the bug this field-id scheme fixes.
            file_min = (file_entry.lower_bounds or {}).get(field_id)
            file_max = (file_entry.upper_bounds or {}).get(field_id)
        else:
            continue

        if file_min is not None and (min_val is None or file_min < min_val):
            min_val = file_min
        if file_max is not None and (max_val is None or file_max > max_val):
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
        return context

    def should_i_run(self, plan) -> bool:  # pragma: no cover - trivial
        # Skip if there are Filter, Join, Unnest, or AggregateAndGroup nodes present.
        # Unnest for the reason given on the `unsupported_nodes` list above: it
        # changes the row count to something no manifest statistic knows.
        killer_candidates = get_nodes_of_type_from_logical_plan(
            plan,
            (
                LogicalPlanStepType.Filter,
                LogicalPlanStepType.Join,
                LogicalPlanStepType.Unnest,
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

        # Extract metadata for all aggregates
        agg_metadata = get_all_aggregate_metadata(aggregate_node)
        if not agg_metadata:
            return plan

        # Extract aliases for all aggregates, ALIGNED to agg_metadata by schema
        # identity. The Exit node's column order is NOT guaranteed to match
        # aggregate_node.aggregates order, so positional pairing silently mislabels
        # results when more than one aggregate is present (e.g. MIN(a), MAX(b)).
        alias_by_identity = extract_alias_by_identity(plan)
        column_aliases = []
        for idx, (agg_func, column_name, agg_node) in enumerate(agg_metadata):
            agg_id = getattr(getattr(agg_node, "schema_column", None), "identity", None)
            column_aliases.append(alias_by_identity.get(agg_id, f"agg_{idx}"))

        # Build literal nodes for each aggregate, collecting values
        literals = []
        for idx, (agg_func, column_name, agg_node) in enumerate(agg_metadata):
            # Get the aggregate value based on type
            if agg_func == "COUNT":
                total_rows = get_count_from_manifest(manifest)
                if total_rows is None:
                    # Row count unknown - the manifest cannot answer this, so
                    # leave the plan alone and let the scan count the rows.
                    return plan
                if column_name:
                    # COUNT(col) = total_rows - nulls(col); requires every file
                    # in the manifest to carry null counts for the column.
                    null_count = manifest.get_total_null_count(column_name)
                    if null_count is None:
                        return plan
                    result_value = total_rows - null_count
                else:
                    result_value = total_rows
                result_type = _CT_INT64
            elif agg_func in ("MIN", "MAX"):
                if not column_name:
                    return plan
                result_value = get_min_max_from_manifest(manifest, column_name, agg_func)
                if result_value is None:
                    return plan
                # Preserve the column type (INTEGER or TIMESTAMP)
                result_type = agg_node.parameters[0].schema_column.column_type or _CT_INT64
            else:
                # Unsupported aggregate type
                return plan

            # Build a literal projection node to replace the aggregate
            literal = build_literal_node(result_value, suggested_type=result_type)

            # Preserve the expected alias for this column
            setattr(literal, "alias", column_aliases[idx])

            # Ensure the literal uses the same schema identity as the original
            # aggregate so downstream Exit/Projection nodes can match by identity.
            agg_schema = agg_node.schema_column
            if agg_schema is not None and literal.schema_column is not None:
                literal.schema_column.identity = agg_schema.identity
                if agg_schema.column_type is not None:
                    literal.schema_column.column_type = agg_schema.column_type

            literals.append(literal)

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
        # the corresponding literal, to ensure no node still references aggregators
        # after the rewrite. Match by schema identity or alias.
        # Build a mapping from aggregator schema identity to replacement literal
        agg_identity_to_literal = {}
        for agg_node, literal in zip(aggregate_node.aggregates, literals):
            agg_id = getattr(getattr(agg_node, "schema_column", None), "identity", None)
            if agg_id is not None:
                agg_identity_to_literal[agg_id] = literal

        # Also build a mapping from alias to literal
        alias_to_literal = {}
        for alias, literal in zip(column_aliases, literals):
            alias_to_literal[alias] = literal

        for nid, n in plan.nodes(data=True):
            cols = getattr(n, "columns", None)
            if not cols:
                continue
            changed = False
            new_cols = []
            for c in cols:
                # Try to match by schema identity first
                expr_id = getattr(getattr(c, "schema_column", None), "identity", None)
                replacement = None

                if expr_id in agg_identity_to_literal:
                    replacement = agg_identity_to_literal[expr_id]
                elif getattr(c, "alias", None) in alias_to_literal:
                    replacement = alias_to_literal[getattr(c, "alias", None)]

                if replacement is not None:
                    new_cols.append(replacement)
                    changed = True
                else:
                    # The aggregate may be embedded inside a wrapping expression
                    # (e.g. HUMANIZE(COUNT(*))) rather than being the column
                    # itself. Walk the tree and splice the literal in wherever a
                    # nested AGGREGATOR still references a replaced identity.
                    nested_aggs = get_all_nodes_of_type(c, (NodeType.AGGREGATOR,))
                    if any(
                        getattr(getattr(a, "schema_column", None), "identity", None)
                        in agg_identity_to_literal
                        for a in nested_aggs
                    ):
                        new_cols.append(_replace_nested_aggregators(c, agg_identity_to_literal))
                        changed = True
                    else:
                        new_cols.append(c)

            if changed:
                n.columns = new_cols
        if self.telemetry is not None:
            self.telemetry._after_replace_agg = True

        # Order the literal columns to match the Exit node's column order. The
        # executor takes the rewritten Project's column order as the output order,
        # but aggregate_node.aggregates is not guaranteed to be in projection
        # order — without this, multi-aggregate results come out permuted
        # (e.g. MIN(a), MAX(b) returned swapped).
        literal_by_identity = {
            getattr(getattr(lit, "schema_column", None), "identity", None): lit
            for lit in literals
        }
        ordered_literals = []
        matched_ids = set()
        for col in getattr(exit_node, "columns", None) or []:
            ident = getattr(getattr(col, "schema_column", None), "identity", None)
            lit = literal_by_identity.get(ident)
            if lit is not None and ident not in matched_ids:
                ordered_literals.append(lit)
                matched_ids.add(ident)
        # Append any literals not matched to an Exit column (defensive; should be
        # none for a statistics-only query whose Exit columns are the aggregates).
        for lit in literals:
            ident = getattr(getattr(lit, "schema_column", None), "identity", None)
            if ident not in matched_ids:
                ordered_literals.append(lit)
        if len(ordered_literals) == len(literals):
            literals = ordered_literals

        # Rewrite aggregate node into a Project with the literal columns
        aggregate_node.node_type = LogicalPlanStepType.Project
        aggregate_node.columns = literals
        # Remove aggregate-specific attributes to avoid confusion downstream
        aggregate_node.aggregates = None
        aggregate_node.groups = None
        aggregate_node.projection = None

        # Point the source(s) to $no_table so physical planner / executor treat
        # this as a projection-only plan (no table scanning required). We apply
        # the change to all Scan nodes found to be conservative.
        # We located the relevant scan node earlier; set it directly. This
        # avoids potential iterator-side-effects and is consistent with the
        # conservative single-scan expectation in `is_statistics_only_query`.
        scan_node.relation = "$no_table"
        scan_node.alias = "$no_table"

        # Replace the connector with the virtual `$no_table` table engine so
        # the ReaderNode will produce the one-row $no_table morsel. This
        # avoids relying on the original connector's behavior after we
        # rewrote the plan to a projection-only query.
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

        # The scan's `.columns` describes the scan's OWN schema (the binder seeds it
        # that way -- see binder/dataset.py::visit_scan), so re-pointing the scan at
        # `$no_table` has to re-seed them too. Left stale, they still name the real
        # table's columns while the reader now emits `$no_table`'s single column, and
        # the native compiler rejects the plan with "a virtual dataset missing plan
        # columns". Projection pushdown hid this by overwriting `.columns` from the
        # NEW schema afterwards -- but only when it runs; with its kill-switch set the
        # stale list survived to compile time.
        scan_node.columns = [
            LogicalColumn(
                node_type=NodeType.IDENTIFIER,
                source_column=col.name,
                source=(col.origin[0] if col.origin else None),
                schema_column=col,
            )
            for col in getattr(scan_node.schema, "columns", []) or []
        ]

        # Finally, clear the manifest to avoid file-based readers from
        # providing file lists (we prefer virtual connector semantics
        # instead)
        scan_node.manifest = None

        # NOTE: exit_node.columns is deliberately NOT overwritten here. The
        # substitution loop above already rewrote it correctly, in place, for
        # both cases: a plain `SELECT COUNT(*)` Exit column IS the aggregate
        # (matched and replaced by identity), while a wrapping expression like
        # `SELECT HUMANIZE(COUNT(*))` has its Exit column reference the
        # HUMANIZE result identity — which must NOT be replaced with the raw
        # count literal. Blindly overwriting exit_node.columns with `literals`
        # here (as this used to do) discarded that distinction and pointed the
        # Exit at the wrong (bare aggregate) identity whenever the aggregate
        # was embedded in a larger expression.

        # Update telemetry safely
        if self.telemetry is not None:
            self.telemetry.optimization_statistics_only_response += 1

        # Write the rewritten nodes back through the plan. Every edit above was
        # in place; the write-back is what tells the optimizer this pass changed
        # the plan (marking its statistics stale — they describe the scan this
        # rewrite just deleted) and, under copy-on-write, what materializes the
        # working copy. See OptimizationStrategy's mutation contract.
        for nid, n in list(plan.nodes(data=True)):
            if n is scan_node or n is aggregate_node or n is exit_node:
                plan[nid] = n

        # Record connector assignment status on the plan for diagnostic purposes
        plan._stats_assigned_connector_type = getattr(scan_node, "connector", None) and getattr(
            scan_node.connector, "__type__", None
        )

        return plan
