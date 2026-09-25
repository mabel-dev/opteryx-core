# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.


from typing import Callable

from opteryx.expression import format_expression
from opteryx.planner.logical_planner import PlanStep, LogicalPlanStepType
from opteryx.models import current_name_of

_render_registry: dict[LogicalPlanStepType, Callable[["PlanStep"], str]] = {}


def register_render(step_type: LogicalPlanStepType):
    """
    Decorator to register a rendering function for a given LogicalPlanStepType
    """

    def wrapper(func: Callable[["PlanStep"], str]):
        # A plain assignment let a SECOND registration for one step type overwrite
        # the first without a word - AggregateAndGroup carried two, and the loser
        # was the one that did not render HAVING, so which of them ran came down
        # to definition order. A renderer that never runs is dead code that still
        # reads as live; this refuses the collision at IMPORT time instead.
        if step_type in _render_registry:
            raise AssertionError(
                f"{step_type} already has a renderer "
                f"({_render_registry[step_type].__name__}); {func.__name__} would replace it."
            )
        _render_registry[step_type] = func
        return func

    return wrapper


@register_render(LogicalPlanStepType.Filter)
def render_filter(node: PlanStep) -> str:
    return f"FILTER ({format_expression(node.condition)})"


@register_render(LogicalPlanStepType.Aggregate)
def render_aggregate(node: PlanStep) -> str:
    # An aggregate's filter is NOT rendered here, because it is not here to render:
    # `AGG(x WHERE p)` is lowered to `AGG(IIF(p, x, NULL))` in the builder, so by
    # the time a plan exists the condition is part of the argument expression and
    # `format_expression` prints it. This used to carry a second arm that printed
    # `... FILTER (WHERE <col.condition>)`; the lowering clears `condition` before
    # the node is built, so that arm rendered nothing across the whole shape
    # battery - and it printed a spelling the engine now refuses.
    aggregates = ", ".join(format_expression(col) for col in node.aggregates)
    return f"UNGROUPED AGGREGATE [{aggregates}]"


@register_render(LogicalPlanStepType.Distinct)
def render_distinct(node: PlanStep) -> str:
    if node.on:
        cols = ",".join(format_expression(col) for col in node.on)
        return f"DISTINCT ON [{cols}]"
    return "DISTINCT"


@register_render(LogicalPlanStepType.Project)
def render_project(node: PlanStep) -> str:
    cols = ", ".join(format_expression(col) for col in node.columns or ())
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
        if node.hoisted_columns
        else ""
    )
    return f"PROJECT [{cols}]{except_cols}{order_by}{hoisted}"


@register_render(LogicalPlanStepType.Union)
def render_union(node: PlanStep) -> str:
    modifier = f" {node.modifier.upper()}" if node.modifier else ""
    columns = (
        " [" + ", ".join(format_expression(c) for c in node.columns) + "]"
        if node.columns
        else ""
    )
    return f"UNION{modifier}{columns}"


@register_render(LogicalPlanStepType.Explain)
def render_explain(node: PlanStep) -> str:
    fmt = f" (FORMAT {node.format})" if node.format else ""
    return f"EXPLAIN{' ANALYZE' if node.analyze else ''}{fmt}"


@register_render(LogicalPlanStepType.Difference)
def render_difference(_: PlanStep) -> str:
    return "DIFFERENCE"


@register_render(LogicalPlanStepType.Join)
def render_join(node: PlanStep) -> str:
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
def render_unnest(node: PlanStep) -> str:
    distinct = "DISTINCT " if node.distinct_target else ""
    filters = (
        f" FILTER ({', '.join(format_expression(f) for f in node.filter_conditions)})"
        if node.filter_conditions
        else ""
    )
    return f"CROSS JOIN UNNEST ({distinct}{current_name_of(node.unnest_column)}) AS {node.unnest_alias}{filters}"


@register_render(LogicalPlanStepType.AggregateAndGroup)
def render_aggregate_and_group(node: PlanStep) -> str:
    result = f"HASHED AGGREGATE [{', '.join(format_expression(col) for col in node.aggregates)}] GROUP BY [{', '.join(format_expression(col) for col in node.groups)}]"
    if node.having_condition is not None:
        result += f" ({format_expression(node.having_condition)})"
    return result


@register_render(LogicalPlanStepType.FunctionDataset)
def render_function_dataset(node: PlanStep) -> str:
    alias = f" AS {node.alias}" if node.alias else ""
    if node.function == "GENERATE_SERIES":
        return f"GENERATE SERIES ({', '.join(format_expression(arg) for arg in node.args)}){alias}"
    if node.function == "VALUES":
        # Pre-bind the names are the alias's column list; post-bind they are the
        # bound column references.
        names = node.column_aliases if node.columns is None else [c.value for c in node.columns]
        column_names = ", ".join(names or ())
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


def _render_bare_reader(node: PlanStep, label: str, auto_alias_prefix: str) -> str:
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
def render_heapsort(node: PlanStep) -> str:
    order = ", ".join(
        format_expression(expr) + ("" if ascending else " DESC")
        for expr, ascending in node.order_by
    )
    qualifier = " VECTOR TOPK" if getattr(node, "vector_topk_candidate", False) else ""
    return f"HEAP SORT{qualifier} (LIMIT {node.limit}, ORDER BY [{order}])"


@register_render(LogicalPlanStepType.ScalarSubqueryGuard)
def render_scalar_subquery_guard(node: PlanStep) -> str:
    return "SCALAR SUBQUERY GUARD (one row or NULL)"


@register_render(LogicalPlanStepType.Limit)
def render_limit(node: PlanStep) -> str:
    limit_str = f"LIMIT ({node.limit})" if node.limit is not None else ""
    offset_str = f" OFFSET ({node.offset})" if node.offset is not None else ""
    return (limit_str + offset_str).strip()


@register_render(LogicalPlanStepType.Order)
def render_order(node: PlanStep) -> str:
    order = ", ".join(
        format_expression(expr) + ("" if ascending else " DESC")
        for expr, ascending in node.order_by
    )
    return f"ORDER BY [{order}]"


@register_render(LogicalPlanStepType.Scan)
def render_scan(node: PlanStep) -> str:
    from opteryx.expression import NodeType, get_all_nodes_of_type

    connector = (
        " " if getattr(node.connector, "__type__", None) is None else f" [{node.connector.__type__}] "
    )
    date_range = ""
    if node.at_date is not None:
        date_range = f" AT ('{node.at_date.isoformat()}')"
    elif node.version is not None:
        date_range = " VERSION AS OF PREVIOUS" if node.version == 0 else f" VERSION AS OF {node.version}"
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
    # Bare legacy hints and per-scan `name = value` settings render in one
    # WITH(...), because that is how they were written. Omitting the settings
    # would make the plan understate what this scan actually runs with.
    _hint_parts = list(node.hints or [])
    _hint_parts.extend(
        f"{name}={literal.value}"
        for name, literal in sorted((node.hint_settings or {}).items())
    )
    hints = f" WITH({','.join(_hint_parts)})" if _hint_parts else ""
    limit = f" LIMIT {node.limit}" if node.limit else ""
    # Shapes absorbed into the scan by the remote-pushdown strategies. Rendered
    # here so a plain EXPLAIN shows what the reader was asked to do, not only
    # EXPLAIN ANALYZE's `remote_sql`.
    pushed = ""
    if node.pushed_aggregates is not None:
        aggs = ", ".join(format_expression(a) for a in node.pushed_aggregates)
        groups = ", ".join(format_expression(g) for g in (node.pushed_groups or []))
        pushed += f" AGGREGATE [{aggs}]" + (f" GROUP BY [{groups}]" if groups else "")
    if node.pushed_distinct:
        pushed += " DISTINCT"
    if node.topn_order_by and node.topn_limit:
        order = ", ".join(
            f"{sc.name}{'' if ascending else ' DESC'}" for sc, ascending in node.topn_order_by
        )
        pushed += f" ORDER BY [{order}] LIMIT {node.topn_limit}"
    return f"SCAN{connector}({node.relation}{alias}{date_range}{hints}){columns}{predicates}{pushed}{limit}"


@register_render(LogicalPlanStepType.Set)
def render_set(node: PlanStep) -> str:
    return f"SET ({node.variable} TO {node.value.value})"


@register_render(LogicalPlanStepType.Show)
def render_show(node: PlanStep) -> str:
    if node.object_type == "VARIABLE":
        return f"SHOW ({' '.join(node.items)})"
    if node.object_type == "VIEW":
        return f"SHOW (CREATE VIEW {node.object_name})"
    return "SHOW"


@register_render(LogicalPlanStepType.ShowColumns)
def render_show_columns(node: PlanStep) -> str:
    full = " FULL" if node.full else ""
    extended = " EXTENDED" if node.extended else ""
    return f"SHOW{full}{extended} COLUMNS ({node.relation})"


@register_render(LogicalPlanStepType.ShowManifest)
def render_show_manifest(node: PlanStep) -> str:
    return f"SHOW MANIFEST FOR ({node.relation})"


@register_render(LogicalPlanStepType.ShowSnapshots)
def render_show_snapshots(node: PlanStep) -> str:
    # The ALL form reads a wider history under a stricter gate, so an EXPLAIN
    # that called it plain SHOW SNAPSHOTS would name a statement that was not run.
    if getattr(node, "history_view", None) == "snapshots_all":
        return f"SHOW ALL SNAPSHOTS FOR ({node.relation})"
    return f"SHOW SNAPSHOTS FOR ({node.relation})"


@register_render(LogicalPlanStepType.ShowLineage)
def render_show_lineage(node: PlanStep) -> str:
    return f"SHOW LINEAGE FOR ({node.relation})"


@register_render(LogicalPlanStepType.ShowSources)
def render_show_sources(node: PlanStep) -> str:
    return f"SHOW SOURCES FOR ({node.relation})"


@register_render(LogicalPlanStepType.Subquery)
def render_subquery(node: PlanStep) -> str:
    return f"SUBQUERY{' AS ' + node.alias if node.alias else ''}"


@register_render(LogicalPlanStepType.Exit)
def render_exit(_: PlanStep) -> str:
    return "EXIT"


@register_render(LogicalPlanStepType.CreateView)
def render_create_view(node: PlanStep) -> str:
    or_replace = "OR REPLACE " if node.or_replace else ""
    columns = f" ({', '.join(node.columns)})" if node.columns else ""
    return f"CREATE {or_replace}VIEW ({node.view_name}{columns})"


@register_render(LogicalPlanStepType.AlterView)
def render_alter_view(node: PlanStep) -> str:
    columns = f" ({', '.join(node.columns)})" if node.columns else ""
    return f"ALTER VIEW ({node.view_name}{columns})"


@register_render(LogicalPlanStepType.DropView)
def render_drop_view(node: PlanStep) -> str:
    if_exists = "IF EXISTS " if node.if_exists else ""
    view_list = ", ".join(node.view_names)
    return f"DROP VIEW {if_exists}({view_list})"


@register_render(LogicalPlanStepType.RenameRelation)
def render_rename_relation(node: PlanStep) -> str:
    if_exists = "IF EXISTS " if node.if_exists else ""
    return f"ALTER TABLE {if_exists}({node.relation_name}) RENAME TO ({node.new_relation_name})"


@register_render(LogicalPlanStepType.AlterWorkspace)
def render_alter_workspace(node: PlanStep) -> str:
    return f"ALTER WORKSPACE ({node.workspace_name}) SET {node.property_name} = {node.property_value}"


@register_render(LogicalPlanStepType.AlterWorkspaceSecure)
def render_alter_workspace_secure(node: PlanStep) -> str:
    if node.secure_destinations is None:
        return f"ALTER WORKSPACE ({node.workspace_name}) DROP SECURE {node.secure_object}"
    destinations = ", ".join(node.secure_destinations)
    return f"ALTER WORKSPACE ({node.workspace_name}) SET SECURE {node.secure_object} TO {destinations}"


@register_render(LogicalPlanStepType.DropWorkspace)
def render_drop_workspace(node: PlanStep) -> str:
    if_exists = "IF EXISTS " if node.if_exists else ""
    return f"DROP WORKSPACE {if_exists}({node.workspace_name})"


@register_render(LogicalPlanStepType.CloneRelation)
def render_clone_relation(node: PlanStep) -> str:
    return f"CLONE ({node.source_relation}) INTO ({node.relation_name})"


@register_render(LogicalPlanStepType.CloneCollection)
def render_clone_collection(node: PlanStep) -> str:
    return f"CLONE COLLECTION ({node.source_collection}) INTO ({node.collection_name})"


@register_render(LogicalPlanStepType.ResyncRelation)
def render_resync_relation(node: PlanStep) -> str:
    force = " FORCE" if getattr(node, "force", False) else ""
    return f"RESYNC ({node.relation_name}){force}"


@register_render(LogicalPlanStepType.DetachRelation)
def render_detach_relation(node: PlanStep) -> str:
    return f"DETACH ({node.relation_name})"


@register_render(LogicalPlanStepType.Analyze)
def render_analyze(node: PlanStep) -> str:
    return f"ANALYZE TABLE ({node.table_name})"


@register_render(LogicalPlanStepType.CreateTrigger)
def render_create_trigger(node: PlanStep) -> str:
    or_replace = "OR REPLACE " if node.or_replace else ""
    event_kind = getattr(node, "event_kind", None) or "commit"
    if event_kind == "schedule":
        event = f"ON SCHEDULE ('{node.schedule}')"
        if getattr(node, "time_zone", None):
            event += f" AT TIME ZONE ('{node.time_zone}')"
    elif event_kind == "signal":
        event = "ON SIGNAL"
    else:
        event = f"ON ({node.table_name})"
    if getattr(node, "window_source", None):
        event += f" OVER ({node.window_source})"
    return f"CREATE {or_replace}TRIGGER ({node.trigger_name}) {event} EXECUTE ({node.task_name})"


@register_render(LogicalPlanStepType.AlterTriggerSuspended)
def render_alter_trigger_suspended(node: PlanStep) -> str:
    state = "SUSPEND" if node.suspended else "RESUME"
    return f"ALTER TRIGGER ({node.trigger_name}) ON ({node.table_name}) {state}"


@register_render(LogicalPlanStepType.AlterTriggerMinimumInterval)
def render_alter_trigger_minimum_interval(node: PlanStep) -> str:
    return (
        f"ALTER TRIGGER ({node.trigger_name}) ON ({node.table_name}) "
        f"SET MINIMUM INTERVAL TO ({node.minimum_interval_seconds} SECONDS)"
    )


@register_render(LogicalPlanStepType.CreateTask)
def render_create_task(node: PlanStep) -> str:
    or_replace = "OR REPLACE " if node.or_replace else ""
    return f"CREATE {or_replace}TASK ({node.task_name})"


@register_render(LogicalPlanStepType.AlterTriggerOwner)
def render_alter_trigger_owner(node: PlanStep) -> str:
    owner = "CURRENT_USER" if node.owner_is_current_user else node.new_owner
    return f"ALTER TRIGGER ({node.trigger_name}) ON ({node.table_name}) OWNER TO ({owner})"


@register_render(LogicalPlanStepType.DropTask)
def render_drop_task(node: PlanStep) -> str:
    if_exists = "IF EXISTS " if node.if_exists else ""
    return f"DROP TASK {if_exists}({node.task_name})"


@register_render(LogicalPlanStepType.Listen)
def render_listen(node: PlanStep) -> str:
    return f"LISTEN TO ({node.task_name}) FOR {node.outcome}"


@register_render(LogicalPlanStepType.Unlisten)
def render_unlisten(node: PlanStep) -> str:
    return f"UNLISTEN ({node.task_name})"


@register_render(LogicalPlanStepType.DropTrigger)
def render_drop_trigger(node: PlanStep) -> str:
    if_exists = "IF EXISTS " if node.if_exists else ""
    return f"DROP TRIGGER {if_exists}({node.trigger_name}) ON ({node.table_name})"


@register_render(LogicalPlanStepType.AlterMaterializedViewSuspended)
def render_alter_materialized_view_suspended(node: PlanStep) -> str:
    return f"ALTER MATERIALIZED VIEW ({node.relation_name}) {'SUSPEND' if node.suspended else 'RESUME'}"


@register_render(LogicalPlanStepType.AlterMaterializedViewOwner)
def render_alter_materialized_view_owner(node: PlanStep) -> str:
    return f"ALTER MATERIALIZED VIEW ({node.relation_name}) OWNER TO ({node.new_owner})"


@register_render(LogicalPlanStepType.Window)
def render_window(node: PlanStep) -> str:
    aggs = ", ".join(format_expression(a) for a in (node.aggregates or []))
    parts = ", ".join(format_expression(p) for p in (node.partition_by or []))
    return f"WINDOW [{aggs}] OVER (PARTITION BY [{parts}])"


@register_render(LogicalPlanStepType.FramedWindow)
def render_framed_window(node: PlanStep) -> str:
    fns = ", ".join(kind for kind, *_rest in (node.outputs or []))
    parts = ", ".join(format_expression(p) for p in (node.partition_by or []))
    order = ", ".join(format_expression(c) for c, _asc in (node.order_by or []))
    return f"FRAMED WINDOW [{fns}] OVER (PARTITION BY [{parts}] ORDER BY [{order}])"
