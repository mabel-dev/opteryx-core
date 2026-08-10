# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.


from platform import node
from typing import Callable

from opteryx.expression import format_expression
from opteryx.planner.logical_planner import LogicalPlanNode, LogicalPlanStepType

_render_registry: dict[LogicalPlanStepType, Callable[["LogicalPlanNode"], str]] = {}


def register_render(step_type: LogicalPlanStepType):
    """
    Decorator to register a rendering function for a given LogicalPlanStepType
    """

    def wrapper(func: Callable[["LogicalPlanNode"], str]):
        _render_registry[step_type] = func
        return func

    return wrapper


@register_render(LogicalPlanStepType.Filter)
def render_filter(node: LogicalPlanNode) -> str:
    return f"FILTER ({format_expression(node.condition)})"


@register_render(LogicalPlanStepType.Aggregate)
def render_aggregate(node: LogicalPlanNode) -> str:
    response = "UNGROUPED AGGREGATE ["
    for col in node.aggregates:
        if col.condition:
            response += (
                f"{format_expression(col)} FILTER (WHERE {format_expression(col.condition)})"
            )
        else:
            response += format_expression(col)
        response += ", "
    response = response.rstrip(", ") + "]"
    return response


@register_render(LogicalPlanStepType.AggregateAndGroup)
def render_aggregate_group(node: LogicalPlanNode) -> str:
    aggregates = ", ".join(format_expression(col) for col in node.aggregates)
    groups = ", ".join(format_expression(col) for col in node.groups)
    return f"HASHED AGGREGATE [{aggregates}] GROUP BY [{groups}]"


@register_render(LogicalPlanStepType.Distinct)
def render_distinct(node: LogicalPlanNode) -> str:
    if node.on:
        cols = ",".join(format_expression(col) for col in node.on)
        return f"DISTINCT ON [{cols}]"
    return "DISTINCT"


@register_render(LogicalPlanStepType.Project)
def render_project(node: LogicalPlanNode) -> str:
    cols = ", ".join(format_expression(col) for col in node.columns)
    order_by = (
        f" + ({', '.join(format_expression(col) for col in node.passthrough_columns)})"
        if node.passthrough_columns
        else ""
    )
    except_cols = (
        f" EXCEPT ({', '.join(format_expression(col) for col in node.except_columns)})"
        if node.except_columns
        else ""
    )
    hoisted = (
        f" (hoisted: {', '.join(format_expression(col) for col in node.hoisted_columns)})"
        if getattr(node, "hoisted_columns", None)
        else ""
    )
    return f"PROJECT [{cols}]{except_cols}{order_by}{hoisted}"


@register_render(LogicalPlanStepType.Union)
def render_union(node: LogicalPlanNode) -> str:
    modifier = f" {node.modifier.upper()}" if node.modifier else ""
    columns = (
        " [" + ", ".join(format_expression(c) for c in node.columns) + "]"
        if node.columns
        else ""
    )
    return f"UNION{modifier}{columns}"


@register_render(LogicalPlanStepType.Explain)
def render_explain(node: LogicalPlanNode) -> str:
    fmt = f" (FORMAT {node.format})" if node.format else ""
    return f"EXPLAIN{' ANALYZE' if node.analyze else ''}{fmt}"


@register_render(LogicalPlanStepType.Difference)
def render_difference(_: LogicalPlanNode) -> str:
    return "DIFFERENCE"


@register_render(LogicalPlanStepType.Join)
def render_join(node: LogicalPlanNode) -> str:
    join_type = node.type.upper()
    cols = ""
    if node.columns:
        cols = ", ".join(format_expression(col) for col in node.columns)
        cols = f" [{cols}]"
    if node.on:
        return f"{join_type} JOIN ({format_expression(node.on, True)}){cols}"
    if node.using:
        using = ",".join(map(format_expression, node.using))
        return f"{join_type} JOIN (USING {using}){cols}"
    return f"{join_type} JOIN{cols}"


@register_render(LogicalPlanStepType.Unnest)
def render_unnest(node: LogicalPlanNode) -> str:
    distinct = "DISTINCT " if node.distinct else ""
    filters = f" FILTER ({', '.join(node.filters)})" if node.filters else ""
    return f"CROSS JOIN UNNEST ({distinct}{node.unnest_column.current_name}) AS {node.unnest_alias}{filters}"


@register_render(LogicalPlanStepType.AggregateAndGroup)
def render_aggregate_and_group(node: LogicalPlanNode) -> str:
    result = f"HASHED AGGREGATE [{', '.join(format_expression(col) for col in node.aggregates)}] GROUP BY [{', '.join(format_expression(col) for col in node.groups)}]"
    if node.having_condition is not None:
        result += f" ({format_expression(node.having_condition)})"
    return result


@register_render(LogicalPlanStepType.FunctionDataset)
def render_function_dataset(node: LogicalPlanNode) -> str:
    alias = f" AS {node.alias}" if node.alias else ""
    if node.function == "GENERATE_SERIES":
        return f"GENERATE SERIES ({', '.join(format_expression(arg) for arg in node.args)}){alias}"
    if node.function == "VALUES":
        # Pre-bind, node.columns are plain name strings; post-bind they are
        # LogicalColumn objects exposing the name via `.value`.
        column_names = ", ".join(c if isinstance(c, str) else c.value for c in node.columns)
        return f"VALUES (({column_names}) x {len(node.values)} AS {node.alias})"
    if node.function == "UNNEST":
        return f"UNNEST ({', '.join(format_expression(arg) for arg in node.args)}{alias})"
    if node.function == "READ_JSONL":
        return _render_bare_reader(node, "READ_JSONL", "$read_jsonl-")
    if node.function == "READ_PARQUET":
        return _render_bare_reader(node, "READ_PARQUET", "$read_parquet-")
    if node.function == "READ_CSV":
        return _render_bare_reader(node, "READ_CSV", "$read_csv-")
    return node.function


def _render_bare_reader(node: LogicalPlanNode, label: str, auto_alias_prefix: str) -> str:
    """READ_JSONL, READ_PARQUET, and READ_CSV are bare dataset functions with a
    real backing reader (rugo's JSONL/CSV decoders / the native ParquetReadNode),
    so their plan line carries the same detail a Scan's does -- file path (or
    glob), columns actually read (projected, plus any filter-only columns not
    otherwise projected, marked with ~), and any pushed-down predicate -- rather
    than just the bare function name every other FunctionDataset case renders as.
    """
    from opteryx.expression import NodeType, get_all_nodes_of_type

    dataset = getattr(node, "dataset", None)
    path = f" ('{dataset}')" if dataset else ""

    # node.alias is never None by render time (opteryx.planner.binder.dataset always
    # sets it, minting an auto_alias_prefix-prefixed name when the user gave none) --
    # unlike render_scan's `relation != alias` check, so an unstable internal name
    # isn't shown as if the user had written it. AS alias(col1, col2, ...) is not
    # supported (rejected at bind time), so alias is always a plain relation name,
    # never a column-rename list.
    node_alias = getattr(node, "alias", None)
    alias = f" AS {node_alias}" if node_alias and not node_alias.startswith(auto_alias_prefix) else ""

    proj_names = [c.source_column for c in node.columns] if node.columns else []
    proj_set = set(proj_names)

    # Columns referenced only in a pushed-down predicate are not in node.columns
    # (ProjectionPushdown removed them because they're not output columns), but
    # the reader still has to decode them from the file. Marked with ~, same
    # convention render_scan uses, so the plan makes clear what is actually read
    # vs projected.
    filter_only_names = []
    if node.predicates:
        for pred in node.predicates:
            for ident in get_all_nodes_of_type(pred, (NodeType.IDENTIFIER,)):
                name = getattr(ident, "source_column", None) or getattr(ident, "value", None)
                if name and name not in proj_set and name not in filter_only_names:
                    filter_only_names.append(name)

    all_col_parts = proj_names + [f"~{n}" for n in filter_only_names]
    columns = " [" + ", ".join(all_col_parts) + "]" if all_col_parts else ""

    predicates = (
        " (" + " AND ".join(map(format_expression, node.predicates)) + ")"
        if node.predicates
        else ""
    )
    return f"{label}{path}{alias}{columns}{predicates}"


@register_render(LogicalPlanStepType.HeapSort)
def render_heapsort(node: LogicalPlanNode) -> str:
    order = ", ".join(
        format_expression(expr) + ("" if ascending else " DESC")
        for expr, ascending in node.order_by
    )
    qualifier = " VECTOR TOPK" if getattr(node, "vector_topk_candidate", False) else ""
    return f"HEAP SORT{qualifier} (LIMIT {node.limit}, ORDER BY [{order}])"


@register_render(LogicalPlanStepType.Limit)
def render_limit(node: LogicalPlanNode) -> str:
    limit_str = f"LIMIT ({node.limit})" if node.limit is not None else ""
    offset_str = f" OFFSET ({node.offset})" if node.offset is not None else ""
    return (limit_str + offset_str).strip()


@register_render(LogicalPlanStepType.Order)
def render_order(node: LogicalPlanNode) -> str:
    order = ", ".join(
        format_expression(expr) + ("" if ascending else " DESC")
        for expr, ascending in node.order_by
    )
    return f"ORDER BY [{order}]"


@register_render(LogicalPlanStepType.Scan)
def render_scan(node: LogicalPlanNode) -> str:
    from opteryx.expression import NodeType, get_all_nodes_of_type

    io_async = "ASYNC " if getattr(node.connector, "async_read_blob", None) is not None else ""
    connector = (
        " " if getattr(node.connector, "__type__", None) is None else f" [{node.connector.__type__}] "
    )
    date_range = ""
    if node.at_date is not None:
        date_range = f" AT ('{node.at_date.isoformat()}')"
    alias = f" AS {node.alias}" if node.relation != node.alias else ""

    proj_names = [c.source_column for c in node.columns] if node.columns else []
    proj_set = set(proj_names)

    # Columns referenced only in pushed-down predicates are not in node.columns
    # (they were removed by ProjectionPushdown because they're not output columns),
    # but will still be fetched from storage by the executor.  Mark them with ~ so
    # the plan makes clear what is actually read vs what is projected.
    filter_only_names = []
    if node.predicates:
        for pred in node.predicates:
            for ident in get_all_nodes_of_type(pred, (NodeType.IDENTIFIER,)):
                name = getattr(ident, "source_column", None) or getattr(ident, "value", None)
                if name and name not in proj_set and name not in filter_only_names:
                    filter_only_names.append(name)

    all_col_parts = proj_names + [f"~{n}" for n in filter_only_names]
    columns = " [" + ", ".join(all_col_parts) + "]" if all_col_parts else ""

    predicates = (
        " (" + " AND ".join(map(format_expression, node.predicates)) + ")"
        if node.predicates
        else ""
    )
    hints = f" WITH({','.join(node.hints)})" if node.hints else ""
    limit = f" LIMIT {node.limit}" if node.limit else ""
    return f"{io_async}SCAN{connector}({node.relation}{alias}{date_range}{hints}){columns}{predicates}{limit}"


@register_render(LogicalPlanStepType.Set)
def render_set(node: LogicalPlanNode) -> str:
    return f"SET ({node.variable} TO {node.value.value})"


@register_render(LogicalPlanStepType.Show)
def render_show(node: LogicalPlanNode) -> str:
    if node.object_type == "VARIABLE":
        return f"SHOW ({' '.join(node.items)})"
    if node.object_type == "VIEW":
        return f"SHOW (CREATE VIEW {node.object_name})"
    return "SHOW"


@register_render(LogicalPlanStepType.ShowColumns)
def render_show_columns(node: LogicalPlanNode) -> str:
    full = " FULL" if node.full else ""
    extended = " EXTENDED" if node.extended else ""
    return f"SHOW{full}{extended} COLUMNS ({node.relation})"


@register_render(LogicalPlanStepType.ShowManifest)
def render_show_manifest(node: LogicalPlanNode) -> str:
    return f"SHOW MANIFEST FOR ({node.relation})"


@register_render(LogicalPlanStepType.Subquery)
def render_subquery(node: LogicalPlanNode) -> str:
    return f"SUBQUERY{' AS ' + node.alias if node.alias else ''}"


@register_render(LogicalPlanStepType.Exit)
def render_exit(_: LogicalPlanNode) -> str:
    return "EXIT"


@register_render(LogicalPlanStepType.CreateView)
def render_create_view(node: LogicalPlanNode) -> str:
    or_replace = "OR REPLACE " if node.or_replace else ""
    columns = f" ({', '.join(node.columns)})" if node.columns else ""
    return f"CREATE {or_replace}VIEW ({node.view_name}{columns})"


@register_render(LogicalPlanStepType.AlterView)
def render_alter_view(node: LogicalPlanNode) -> str:
    columns = f" ({', '.join(node.columns)})" if node.columns else ""
    return f"ALTER VIEW ({node.view_name}{columns})"


@register_render(LogicalPlanStepType.DropView)
def render_drop_view(node: LogicalPlanNode) -> str:
    if_exists = "IF EXISTS " if node.if_exists else ""
    view_list = ", ".join(node.view_names)
    return f"DROP VIEW {if_exists}({view_list})"


@register_render(LogicalPlanStepType.RenameRelation)
def render_rename_relation(node: LogicalPlanNode) -> str:
    if_exists = "IF EXISTS " if node.if_exists else ""
    return f"ALTER TABLE {if_exists}({node.relation_name}) RENAME TO ({node.new_relation_name})"


@register_render(LogicalPlanStepType.AlterWorkspace)
def render_alter_workspace(node: LogicalPlanNode) -> str:
    return f"ALTER WORKSPACE ({node.workspace_name}) SET {node.property_name} = {node.property_value}"


@register_render(LogicalPlanStepType.Analyze)
def render_analyze(node: LogicalPlanNode) -> str:
    return f"ANALYZE TABLE ({node.table_name})"


@register_render(LogicalPlanStepType.DropTrigger)
def render_drop_trigger(node: LogicalPlanNode) -> str:
    if_exists = "IF EXISTS " if node.if_exists else ""
    return f"DROP TRIGGER {if_exists}({node.trigger_name}) ON ({node.table_name})"


@register_render(LogicalPlanStepType.AlterMaterializedViewOwner)
def render_alter_materialized_view_owner(node: LogicalPlanNode) -> str:
    return f"ALTER MATERIALIZED VIEW ({node.relation_name}) OWNER TO ({node.new_owner})"


@register_render(LogicalPlanStepType.Window)
def render_window(node: LogicalPlanNode) -> str:
    aggs = ", ".join(format_expression(a) for a in (node.aggregates or []))
    parts = ", ".join(format_expression(p) for p in (node.partition_by or []))
    return f"WINDOW [{aggs}] OVER (PARTITION BY [{parts}])"
