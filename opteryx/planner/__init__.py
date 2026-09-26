# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
                      ┌───────────┐
                      │   USER    │
       ┌──────────────┤           ◄──────────────┐
       │              └───────────┘              │
───────┼─────────────────────────────────────────┼──────
       │ SQL                                     │ Results
 ┌─────▼─────┐                             ┌─────┴─────┐
 │ SQL       │                             │           │
 │ Rewriter  │                             │ Executor  │
 └─────┬─────┘                             └─────▲─────┘
       │ SQL                                     │ Plan
 ┌─────▼─────┐                             ┌─────┴─────┐
 │           │                             │ Physical  │
 │ Parser    │                             │ Planner   │
 └─────┬─────┘                             └─────▲─────┘
       │ AST                                     │ Plan
 ┌─────▼─────┐                             ┌─────┴─────┐
 │ AST       │                             │           │
 │ Rewriter  │                             │ Optimizer │
 └─────┬─────┘                             └─────▲─────┘
       │ AST                                     │ Plan
 ┌─────▼─────┐        ┌───────────┐        ┌─────┴─────┐
 │ Logical   │ Plan   │ Plan      │ Plan   │           │
 │   Planner ├────────► Rewriter  ├────────► Binder    │
 └───────────┘        └───────────┘        └─────▲─────┘
                                                 │ Stats & Schemas
                                           ┌─────┴─────┐
                                           │           │
                                           │ Catalogue │
                                           └───────────┘
"""

import datetime
import decimal
import time
from typing import Any, Dict, Generator, Iterable, Optional, Union

from opteryx.compiled.structures.expressions import Expression
from opteryx.exceptions import SqlError
from opteryx.exceptions import UnsupportedSyntaxError
from opteryx.expression import NodeType
from opteryx.expression.intervals import normalize_interval_value
from opteryx.types.logical_type import (
    ARRAY,
    BOOLEAN,
    DATE,
    DECIMAL,
    FLOAT32,
    FLOAT64,
    INT64,
    INTERVAL,
    NULL,
    NVARCHAR,
    TIME,
    TIMESTAMP,
    VARBINARY,
    VARCHAR,
    VARIANT,
    ColumnType,
    LogicalCategory,
)
from opteryx.planner.plan_context import PlanContext
from opteryx.compiled.structures.expressions import Literal

# Mirrors `GRAMMAR_ERROR_PREFIX` in src/aside/cursor.rs. The aside parser has one
# error channel to Python - a `ValueError` carrying sqlparser's message - so a
# refusal that quotes an Opteryx grammar back marks itself, and is re-typed here
# as the `UnsupportedSyntaxError` the regex layer raised before it.
ASIDE_GRAMMAR_ERROR_PREFIX = "OPTERYX-SYNTAX: "


def _infer_collection_literal(value: Any, *, plan_context):
    """Return (ColumnType_for_collection, None) for a list/tuple literal.

    The element type is embedded in the ARRAY ColumnType; no separate sidecar.
    Returns (ARRAY(VARIANT), None) for heterogeneous or unknown-element lists.
    """
    if not isinstance(value, (list, tuple)) or not value:
        return ARRAY(VARIANT), None

    element_types = {build_literal_node(item, plan_context=plan_context).type for item in value if item is not None}
    if len(element_types) != 1:
        return ARRAY(VARIANT), None

    element_ct = element_types.pop()
    if element_ct is None:
        return ARRAY(VARIANT), None
    # Numeric homogeneous array → treat as ARRAY<FLOAT64> at binder time
    if element_ct.category in (
        LogicalCategory.INTEGER,
        LogicalCategory.FLOAT,
        LogicalCategory.DECIMAL,
    ):
        return ARRAY(FLOAT64), None
    return ARRAY(element_ct), None


def build_literal_node(value: Any, identity_of: Optional[Expression] = None, suggested_type=None, *, plan_context):
    """
    Build a NEW literal node with the appropriate type based on the value.

    `identity_of`: an expression whose identity the literal takes over — its uuid,
    alias, query_column, schema_column (retyped to the literal's type) and
    relations — so references to that expression's bound column resolve to the
    literal (constant folding). Nothing else of it is carried, and it is not
    modified (its schema_column object is, being shared).
    """
    # Normalise scalar wrappers to native Python types.
    _PYTHON_NATIVE = (
        bool,
        int,
        float,
        str,
        bytes,
        datetime.datetime,
        datetime.date,
        datetime.time,
        decimal.Decimal,
        list,
        tuple,
    )
    if (
        not isinstance(value, _PYTHON_NATIVE)
        and value is not None
        and getattr(value, "item", None) is not None
    ):
        value = value.item()

    if identity_of is None:
        root = Literal(
            schema_column=plan_context.columns.constant(str(value)),
        )
    else:
        root = Literal(
            uuid=identity_of.uuid,
            alias=identity_of.alias,
            query_column=identity_of.query_column,
            schema_column=identity_of.schema_column,
            relations=identity_of.relations,
            do_not_create_column=identity_of.do_not_create_column is True,
        )

    if value is None:
        # A None value is a NULL literal. When a concrete type was requested
        # (e.g. folding CAST(NULL AS VARCHAR)) preserve it — an untyped NULL
        # loses the physical tag string-family kernels dispatch on, so a typed
        # NULL string operand would otherwise be read as a garbage arena. With
        # no suggestion the literal stays untyped NULL.
        root.value = None
        root.type = suggested_type if suggested_type is not None else NULL
        if root.schema_column is not None:
            root.schema_column.column_type = root.type
        return root

    collection_ct = None
    if suggested_type is None:
        collection_ct, _ = _infer_collection_literal(value, plan_context=plan_context)

    # Define a mapping of Python types to canonical ColumnType instances.
    type_mapping = {
        bool: BOOLEAN,
        str: VARCHAR,
        bytes: VARBINARY,
        int: INT64,
        float: FLOAT64,
        datetime.datetime: TIMESTAMP(),
        datetime.time: TIME(),
        datetime.date: DATE,
        decimal.Decimal: DECIMAL(18, 6),
        list: collection_ct or ARRAY(VARIANT),
        tuple: collection_ct or ARRAY(VARIANT),
    }

    value_type = type(value)
    # Determine the type from the value using the mapping
    if value_type in type_mapping or suggested_type is not None:
        if suggested_type is not None and suggested_type == INTERVAL:
            value = normalize_interval_value(value)
        if isinstance(value, datetime.datetime):
            from opteryx.types.timestamps._datetime_conversion import timestamp_to_int64_us

            value = timestamp_to_int64_us(value)
        elif isinstance(value, datetime.date):
            from opteryx.types.timestamps._datetime_conversion import date_to_int64_days

            value = date_to_int64_days(value)
        root.value = value
        root.type = suggested_type if suggested_type is not None else type_mapping[value_type]
        if root.schema_column is not None:
            root.schema_column.column_type = root.type
        return root

    # No literal type for this value: nothing is built. The expression it would
    # have replaced stands.
    return identity_of if identity_of is not None else root


def attach_source_position(error, statement) -> None:
    """Give `error` a `SourcePosition` over the SQL its `span` came from.

    The two halves of a positioned error are known in different places: a raise site
    deep in the planner knows WHICH node went wrong but not what the statement said,
    and only here do we have both. So raise sites set `SqlError.span` and this maps it
    onto the text the reader submitted.

    The result is a RANGE. The span already had an end - the parser gives one for every
    identifier - and it is what lets the editor underline the whole name rather than
    put a mark under its first character and leave the reader to work out how far it
    goes. Both endpoints are mapped independently, because a rewrite between them (a
    `b'..'` inside the offending expression) moves them by different amounts.

    The error is untouched otherwise - same type, same identity, same message. Silent
    when there is no span, no statement, or the position does not resolve; an error
    with no position is the normal case for anything the reader did not write down.
    """
    from opteryx.exceptions import SourcePosition
    from opteryx.planner.sql_rewriter import RewrittenStatement

    span = error.span
    if span is None or statement is None or error.position is not None:
        return

    if not isinstance(statement, RewrittenStatement):
        # A statement nothing rewrote is its own source, and the identity mapping is
        # the general one with an empty edit list - no second code path needed.
        statement = RewrittenStatement(str(statement), source=str(statement))

    start = statement.to_source_point(span[0], span[1])
    end = statement.to_source_point(span[2], span[3])
    if start is None:
        return
    if end is None or end[2] < start[2]:
        # An unmappable or backwards end degrades to an empty range at the start: the
        # editor draws a caret there. Better a narrow truth than a wrong underline.
        end = start
    error.position = SourcePosition(start[0], start[1], end[0], end[1], start[2], end[2])


def parse_statement(
    operation: str,
    source: Optional[str] = None,
    source_offset: int = 0,
    telemetry=None,
):
    """
    Rewrite and parse `operation`. No catalog, no binding - syntax only.

    Split out from `bind_statement` because two callers need the parsed statement
    WITHOUT paying for a bind, and both must see exactly what the planner sees:
    `analyze_query`, which reports what a statement is and touches before anything
    runs it, and `Session.check`, which reports the same alongside its diagnostics
    and must not parse the statement a second time to do so.

    Returns:
        (clean_sql, parsed_statements) - `clean_sql` is the RewrittenStatement the
        parser was given, which also maps a reported position back onto the text the
        reader wrote. `parsed_statements` is PRE-rewrite: the AST rewriter substitutes
        placeholders, so this is the only form that still knows a `:name` was written.

    Raises:
        QueryParseError, positioned against the submitted text.
    """
    from opteryx.planner.sql_rewriter import do_sql_rewrite
    from opteryx.third_party import sqloxide

    # SQL Rewriter
    start = time.monotonic_ns()
    clean_sql = do_sql_rewrite(operation, source=source, source_offset=source_offset)
    if telemetry is not None:
        telemetry.time_planning_sql_rewriter += time.monotonic_ns() - start

    # Parser converts the SQL command into an AST.
    #
    # ONE front door. Statements sqlparser has no grammar for - task and trigger
    # DDL, REFRESH, SAVE, the subscriptions, the grant surface, and the rest -
    # are recognized by the ASIDE PARSER, in Rust, on the same token stream
    # (`src/aside/`). There is no Python pre-parse layer any more: it was a
    # regex front door that had to re-implement, per statement, what the
    # tokenizer provides once. See `docs/ASIDE_PARSER_DESIGN.md`.
    try:
        parsed_statements = sqloxide.parse_sql(clean_sql, _dialect="opteryx")
    except ValueError as parser_error:
        from opteryx.planner.parse_error import raise_parse_error

        # A GRAMMAR refusal from the aside parser, not a parse failure: the
        # statement was recognized as one of ours and did not fit the form, so
        # the reader needs the form quoted back rather than a caret at the token
        # sqlparser gave up on. The prefix is the only way the two are
        # distinguishable by the time they arrive as one ValueError - see
        # `GRAMMAR_ERROR_PREFIX` in src/aside/cursor.rs.
        message = str(parser_error)
        marker = message.find(ASIDE_GRAMMAR_ERROR_PREFIX)
        if marker >= 0:
            raise UnsupportedSyntaxError(
                message[marker + len(ASIDE_GRAMMAR_ERROR_PREFIX) :].strip()
            ) from None

        # `clean_sql` carries both texts: the one the parser was given, which the
        # reported line/column index, and the one the reader wrote, which is what
        # the caret gets printed against. It maps between them.
        raise_parse_error(clean_sql, parser_error)

    return clean_sql, parsed_statements


def bind_statement(
    operation: str,
    parameters: Union[Iterable, Dict, None],
    visibility_filters: Optional[Dict[str, Any]],
    execution_context,
    query_id: str,
    telemetry,
    source: Optional[str] = None,
    source_offset: int = 0,
    catalog_cache=None,
    schema_only: bool = False,
    *,
    plan_context,
):
    """
    Plan `operation` as far as the end of binding, and return the bound plan.

    This is the whole front half of `query_planner`, factored out rather than copied,
    because the edit-time check (`Session.check`) stops here and the two MUST agree:
    a statement that binds clean when checked has to be the same statement that binds
    clean when run, resolved through the same rewriter, the same parser and the same
    binder. A second implementation of "parse and bind" would drift, and it would
    drift in the direction of telling the reader their query is fine when it is not.

    Everything past this point - optimizer, result-size guard, physical planner - is
    cost and shape, and needs statistics the check deliberately does not read.

    Returns:
        (bound_plan, clean_sql, ast) - see `parse_statement` for `clean_sql`; `ast` is
        the rewritten statement.

    Parameters:
        catalog_cache: opt-in, check-path only. See `opteryx.CatalogCache`.
        schema_only: bind without reading each relation's Manifest. Check-path only -
            the resulting plan cannot be optimized or executed.
        plan_context: the query's PlanContext — every column the plan gets is
            minted in it.
    """
    from opteryx.connectors import resolution_scope

    clean_sql, parsed_statements = parse_statement(
        operation, source=source, source_offset=source_offset, telemetry=telemetry
    )
    # The check path reaches binding through here rather than through
    # `query_planner`, so the scope is opened here too. Nested inside
    # `query_planner`'s it is a no-op - see `resolution_scope`.
    with resolution_scope():
        return bind_parsed_statement(
            parsed_statements=parsed_statements,
            clean_sql=clean_sql,
            parameters=parameters,
            visibility_filters=visibility_filters,
            execution_context=execution_context,
            query_id=query_id,
            telemetry=telemetry,
            catalog_cache=catalog_cache,
            schema_only=schema_only,
            plan_context=plan_context,
        )


def build_logical_plan(
    parsed_statements,
    clean_sql,
    parameters: Union[Iterable, Dict, None],
    telemetry,
    catalog_cache=None,
    variables=None,
    *,
    plan_context,
):
    """
    Rewrite the AST, plan it, expand its relations and rewrite the plan - everything
    between parsing and binding. Returns `(logical_plan, ast)`, unbound.

    Split from the bind so a caller can hold the plan object ACROSS a failed bind.
    `do_bind_phase` binds bottom-up and mutates the plan in place, so a statement that
    fails in its SELECT list has already resolved its FROM, and the relations it
    resolved are still readable on this object. That is what lets `Session.check`
    offer completions for a query that is, right now, wrong - which is every query
    while it is being typed.
    """
    from opteryx.planner.ast_rewriter import do_ast_rewriter
    from opteryx.planner.logical_planner import do_logical_planning_phase
    from opteryx.planner.plan_rewriter import do_plan_rewrite
    from opteryx.planner.relation_resolver import do_resolve_relations

    params: Union[list, dict, None] = None
    if parameters is None:
        params = []
    elif isinstance(parameters, dict):
        params = parameters.copy()
    else:
        params = [p for p in parameters or []]

    # AST Rewriter adds temporal filters and parameters to the AST, and resolves
    # the `$me` pronoun inside relation names - which is why it takes `variables`:
    # the session's own identity, read once here so no later phase has to.
    start = time.monotonic_ns()
    parsed_statement = do_ast_rewriter(parsed_statements, parameters=params, variables=variables)[0]
    telemetry.time_planning_ast_rewriter += time.monotonic_ns() - start

    # Logical Planner converts ASTs to logical plans.
    #
    # From here to the end of binding, an error that named a node - an unknown column,
    # an unknown function - gets a caret pointing at where that node was written. This
    # is the only place that holds both halves: the raise sites know the node, and
    # `clean_sql` knows both the text the parser saw and the text the reader submitted.
    # The error is re-raised unchanged; only its presentation is filled in.
    try:
        logical_plan, ast, ctes = do_logical_planning_phase(parsed_statement, plan_context=plan_context)  # type: ignore
    except SqlError as error:
        attach_source_position(error, clean_sql)
        raise

    # Relation Resolver: expand CTE and view references into the plan. Runs BEFORE the
    # rewriter so the rewriter sees one fully-expanded plan — a subquery inside a view or
    # CTE body is eliminated by the same pass that handles the main query.
    start = time.monotonic_ns()
    logical_plan = do_resolve_relations(
        logical_plan, ctes, telemetry, catalog_cache, plan_context=plan_context
    )
    telemetry.time_planning_relation_resolver += time.monotonic_ns() - start

    # Plan Rewriter: structural rewrites on the unbound, fully-expanded logical plan
    start = time.monotonic_ns()
    logical_plan = do_plan_rewrite(logical_plan, telemetry, plan_context=plan_context)
    telemetry.time_planning_plan_rewriter += time.monotonic_ns() - start

    # check user has permission for this query type
    query_type = next(iter(ast))
    # Special-case DROP VIEW -> treat as DropView permission
    # ast["Drop"]["object_type"] is the object type (e.g., "View") when Drop is a mapping
    if query_type == "Drop" and isinstance(ast["Drop"], dict):
        if ast["Drop"].get("object_type") == "View":
            query_type = "DropView"

    return logical_plan, ast


def bind_logical_plan(
    logical_plan,
    clean_sql,
    visibility_filters: Optional[Dict[str, Any]],
    execution_context,
    query_id: str,
    telemetry,
    schema_only: bool = False,
    *,
    plan_context,
):
    """
    The Binder adds schema information to the logical plan.

    Mutates `logical_plan` in place and returns it, so a caller that kept a reference
    can read how far binding got even when this raises - see `build_logical_plan`.
    """
    from opteryx.planner.binder import do_bind_phase

    start = time.monotonic_ns()
    try:
        bound_plan = do_bind_phase(
            logical_plan,
            execution_context=execution_context,
            query_id=query_id,
            visibility_filters=visibility_filters,
            telemetry=telemetry,
            schema_only=schema_only,
            plan_context=plan_context,
        )
    except SqlError as error:
        attach_source_position(error, clean_sql)
        raise
    finally:
        telemetry.time_planning_binder += time.monotonic_ns() - start

    return bound_plan


def bind_parsed_statement(
    parsed_statements,
    clean_sql,
    parameters: Union[Iterable, Dict, None],
    visibility_filters: Optional[Dict[str, Any]],
    execution_context,
    query_id: str,
    telemetry,
    catalog_cache=None,
    schema_only: bool = False,
    *,
    plan_context,
):
    """
    Everything from the AST rewriter to the end of binding, on an already-parsed
    statement. Callers that have not parsed yet want `bind_statement`.
    """
    logical_plan, ast = build_logical_plan(
        parsed_statements=parsed_statements,
        clean_sql=clean_sql,
        parameters=parameters,
        telemetry=telemetry,
        catalog_cache=catalog_cache,
        variables=execution_context.variables,
        plan_context=plan_context,
    )
    bound_plan = bind_logical_plan(
        logical_plan=logical_plan,
        clean_sql=clean_sql,
        visibility_filters=visibility_filters,
        execution_context=execution_context,
        query_id=query_id,
        telemetry=telemetry,
        schema_only=schema_only,
        plan_context=plan_context,
    )
    return bound_plan, clean_sql, ast


def query_planner(
    operation: str,
    parameters: Union[Iterable, Dict, None],
    visibility_filters: Optional[Dict[str, Any]],
    execution_context,
    query_id: str,
    telemetry,
    output_format: str = "physical",
    source: Optional[str] = None,
    source_offset: int = 0,
) -> Union[Generator[Any, Any, Any], Dict[str, Any]]:
    """
    Plan `operation`.

    `source` and `source_offset` say where `operation` came from - the whole text the
    caller submitted, and where this statement starts inside it. They exist so that a
    position reported by the parser, or a span carried on an AST node, can be quoted
    back against the text the reader wrote rather than the text the rewriter produced.
    A caller that has only one statement and no rewriting to account for can leave them
    alone; `operation` is then its own source.

    Takes no catalog cache, on purpose: a plan that reads rows is built against the
    catalog as it is now, not as it was up to a minute ago.
    """
    from opteryx.connectors import resolution_scope
    from opteryx.models import QueryProperties
    from opteryx.planner.optimizer import do_optimizer
    from opteryx.planner.physical_planner import create_physical_plan

    # One statement, one answer per workspace from the slot-2 resolver. Opened
    # around the WHOLE of planning, not just the bind: the optimizer and the
    # physical planner build connectors too, and each of those is another call
    # into the resolver. See `opteryx.connectors.resolution_scope`.
    with resolution_scope():
        # The query's planning context: its columns (minted by every phase, from the
        # AST builders on), its estimates and the scan base-statistics memo — shared
        # by the optimizer, the result-size guard and the billing meter, never across
        # queries, and never stored on plan nodes.
        plan_context = PlanContext()
        # Parse, resolve, rewrite and bind - the same path `Session.check` stops at the
        # end of.
        bound_plan, _clean_sql, _ast = bind_statement(
            operation=operation,
            parameters=parameters,
            visibility_filters=visibility_filters,
            execution_context=execution_context,
            query_id=query_id,
            telemetry=telemetry,
            source=source,
            source_offset=source_offset,
            plan_context=plan_context,
        )

        start = time.monotonic_ns()
        # Threaded explicitly from here on: Graph copies do not carry instance
        # attributes, so `shared_ctes` on the plan object would not survive an
        # optimizer strategy handing back a copy.
        shared_ctes = getattr(bound_plan, "shared_ctes", None) or {}
        # Recursive-CTE metadata rides the same way: the legs are shared_ctes
        # entries, this maps each rcte_key to them (docs/RECURSIVE_CTE_DESIGN.md).
        recursive_ctes = getattr(bound_plan, "recursive_ctes", None) or {}
        optimized_plan = do_optimizer(
            bound_plan, telemetry, plan_context, shared_ctes=shared_ctes
        )
        shared_ctes = getattr(optimized_plan, "shared_ctes", None) or shared_ctes
        telemetry.time_planning_optimizer += time.monotonic_ns() - start
        # Read BEFORE the guard / EXPLAIN ANALYZE refreshes below, which clear it:
        # together with the flag after them it says whether either of those
        # refreshed (and so recorded telemetry for) the final plan.
        fresh_from_optimizer = not optimized_plan.statistics_are_stale

        # Refuse a query whose result is already known to blow the row limit, BEFORE any
        # data is read — an accidental cross join should cost nothing, not an hour of IO.
        # Only fires when every input has real row counts; see result_size_guard.
        from opteryx.planner.result_size_guard import check_estimated_result_size
        from opteryx.variables import resolve as _resolve_var

        optimized_plan = check_estimated_result_size(
            optimized_plan,
            _resolve_var("sql_select_limit", execution_context.variables, 0),
            plan_context,
            telemetry=telemetry,
        )

        # EXPLAIN ANALYZE: force the estimate refresh. `refresh_statistics` otherwise
        # runs opportunistically (only when an optimizer strategy asks for it, plus
        # result_size_guard above), so nodes it never reached carry no estimate and
        # EXPLAIN's `est_rows`/`est_bytes` render NULL. On ANALYZE specifically that
        # is worth paying plan time to avoid: the entire value of the statement is
        # putting the planner's estimate beside the row count the query actually
        # produced, which doubles as a cardinality-estimator audit on every real
        # query — and a column that is blank half the time cannot do that job.
        # Architect ruling D3, 2026-08-25. Plain EXPLAIN is deliberately NOT forced:
        # it has no actuals to compare against, so it does not earn the plan time.
        from opteryx.planner.logical_planner import LogicalPlanStepType as _LPST

        if getattr(optimized_plan, "statistics_are_stale", True) and any(
            node.node_type == _LPST.Explain and node.analyze
            for _, node in optimized_plan.nodes(True)
        ):
            from opteryx.planner.optimizer.statistics_refresh import refresh_statistics

            optimized_plan = refresh_statistics(optimized_plan, plan_context, telemetry=telemetry)

        # Estimate telemetry (estimated_row_counts / predicate_estimates /
        # join_estimates) describes the FINAL plan — architect ruling 2026-09-24.
        # The optimizer's own refreshes record none: each was overwritten by the
        # next, and the survivor could describe an intermediate plan whose nodes
        # no longer exist. When the optimizer estimated at all and neither refresh
        # above ran on the final plan (it was fresh already, or is still stale),
        # this is the one refresh that records it.
        if optimized_plan.statistics_estimated_by_optimizer and (
            fresh_from_optimizer or optimized_plan.statistics_are_stale
        ):
            from opteryx.planner.optimizer.statistics_refresh import refresh_statistics

            optimized_plan = refresh_statistics(optimized_plan, plan_context, telemetry=telemetry)

        # The physical planner reads each shared CTE body's estimates too (the
        # execution estimates the native sinks size themselves from). A body gets
        # no guard or telemetry refresh, so refresh any whose plan changed since
        # its last one — without telemetry, which describes the main plan.
        from opteryx.planner.optimizer.statistics_refresh import refresh_statistics

        for body in shared_ctes.values():
            if body.statistics_are_stale:
                refresh_statistics(body, plan_context)

        # The `data_processed` billing meter, measured on the FINAL logical plan —
        # after manifest pruning, projection pushdown and predicate pushdown, all of
        # which change the answer. Plan-time by ruling (2026-08-24): jobs.opteryx
        # enforces usage limits at submit time and has to quote the same number this
        # bills, which a runtime counter cannot be. See planner/data_processed.py for
        # the definition and for what that choice costs.
        #
        # `increase`, not assign: a semicolon-separated batch plans each statement
        # through here and bills the sum, matching the one DATA_PROCESSED_BYTES event
        # per execute() call that the session emits.
        from opteryx.planner.data_processed import data_processed_by_scan
        from opteryx.planner.data_processed import measure_data_processed
        from opteryx.planner.data_processed import plan_relations

        telemetry.increase(
            "billing_bytes",
            measure_data_processed(optimized_plan, plan_context.scan_stats_cache, shared_ctes),
        )
        # Per-scan breakdown of that same figure, keyed by the `uuid` the physical
        # planner carries from the logical node onto the compiled scan node —
        # EXPLAIN (mermaid.py) reads this so a TABLE SCAN's displayed bytes are the
        # SAME number the bill was computed from, not a second, disagreeing
        # estimate. One entry per Scan node, so a self-UNION's two legs are two
        # entries, matching the two nodes EXPLAIN draws and the two `billing_bytes`
        # above counts.
        telemetry._reading["billing_bytes_by_scan"] = data_processed_by_scan(
            optimized_plan, plan_context.scan_stats_cache, shared_ctes
        )
        # The relations that figure was measured over, recorded from the SAME plan
        # and the same scan walk. Downstream this is what attributes a query to the
        # things it read; nothing else records it, and re-deriving it from the SQL
        # text later would need the binder and could disagree with the number
        # billed here. Unioned, not assigned, for the same reason `billing_bytes`
        # is increased: a semicolon-separated batch plans each statement through
        # here and the session emits one event for the batch.
        telemetry.add_relations(plan_relations(optimized_plan, shared_ctes))

        # Default: build traditional physical plan
        # before we write the new optimizer and execution engine, convert to a V1 plan
        start = time.monotonic_ns()
        query_properties = QueryProperties(query_id=query_id, variables=execution_context.variables)
        physical_plan = create_physical_plan(
            optimized_plan, query_properties, plan_context, shared_ctes=shared_ctes
        )
        physical_plan.recursive_ctes = recursive_ctes
        telemetry.time_planning_physical_planner += time.monotonic_ns() - start

        return physical_plan


def execute_logical_plan(
    logical_plan,
    connection=None,
    query_id: Optional[str] = None,
    telemetry=None,
    visibility_filters: Optional[Dict[str, Any]] = None,
    output_format: str = "physical",
):
    """
    Execute an already-constructed logical plan through bind, optimizer and
    physical planning so it can be executed by the executor or returned as
    a Substrait plan. Intended for use by external services that generate
    logical plans (eg. OData service).
    """
    from opteryx.constants import ResultType
    from opteryx.exceptions import SqlError
    from opteryx.managers.execution import execute as execute_plan
    from opteryx.models import ExecutionContext, QueryProperties, QueryTelemetry
    from opteryx.planner.binder import do_bind_phase
    from opteryx.planner.optimizer import do_optimizer
    from opteryx.planner.physical_planner import create_physical_plan
    from opteryx.planner.plan_rewriter import do_plan_rewrite
    from opteryx.planner.relation_resolver import do_resolve_relations
    from opteryx.utils import random_string

    # Prepare query_id and telemetry defaults
    if query_id is None:
        query_id = random_string(32)
    if telemetry is None:
        telemetry = QueryTelemetry(query_id)

    # Determine execution context for binder
    if connection is None:
        conn_context = ExecutionContext(memberships=[])
    elif "context" in dir(connection):
        conn_context = connection.context
    else:
        conn_context = connection

    # The query's planning context — see query_planner. The caller built the logical
    # plan outside any query, so the columns it mints from here on are this query's.
    plan_context = PlanContext()

    # Externally-supplied logical plans still reference relations by name, so they go
    # through the same resolver. They carry no CTEs — a CTE only exists in SQL text.
    start = time.monotonic_ns()
    logical_plan = do_resolve_relations(logical_plan, None, telemetry, plan_context=plan_context)
    telemetry.time_planning_relation_resolver += time.monotonic_ns() - start

    # Must run AFTER relation resolution and BEFORE the binder, exactly as query_planner
    # orders it. An externally-supplied plan carries no such shapes of its own -- but once
    # the resolver splices a VIEW body into it, it carries whatever SQL that view was
    # written in. INTERSECT ALL / EXCEPT ALL are *lowered* here (to ROW_NUMBER plus a
    # semi/anti join) and aggregate Window nodes are turned into joins; nothing downstream
    # can execute either un-lowered, so omitting this stage does not merely forfeit an
    # optimisation, it makes any view containing one fail at execution.
    #
    # NOT here, though both used to be: IN (<subquery>) and EXISTS lower in the OPTIMIZER
    # (post-bind -- which side of `WHERE a = b` is the outer reference is undecidable from
    # query text and needs schema to settle), and the DISTINCT forms of INTERSECT/EXCEPT
    # lower in the BINDER. See plan_rewriter/strategies/__init__.py, which is the list.
    start = time.monotonic_ns()
    logical_plan = do_plan_rewrite(logical_plan, telemetry, plan_context=plan_context)
    telemetry.time_planning_plan_rewriter += time.monotonic_ns() - start

    # The Binder adds schema information to the logical plan
    start = time.monotonic_ns()
    bound_plan = do_bind_phase(
        logical_plan,
        execution_context=conn_context,
        query_id=query_id,
        visibility_filters=visibility_filters,
        telemetry=telemetry,
        plan_context=plan_context,
    )
    telemetry.time_planning_binder += time.monotonic_ns() - start

    start = time.monotonic_ns()
    optimized_plan = do_optimizer(bound_plan, telemetry, plan_context)
    telemetry.time_planning_optimizer += time.monotonic_ns() - start

    # Estimate telemetry describes the FINAL plan, as in query_planner. This path
    # has no result-size guard to have refreshed it, so the optimizer having
    # estimated at all is the whole condition.
    if optimized_plan.statistics_estimated_by_optimizer:
        from opteryx.planner.optimizer.statistics_refresh import refresh_statistics

        optimized_plan = refresh_statistics(optimized_plan, plan_context, telemetry=telemetry)

    # The `billing_bytes` meter, same figure `plan_query` records: a query is a
    # query whether it arrived as SQL or as a logical plan, and this path
    # answering without setting the meter is exactly how "what is reported
    # differs based on what is answering". Externally-supplied plans carry no
    # shared CTEs (a CTE only exists in SQL text).
    from opteryx.planner.data_processed import data_processed_by_scan
    from opteryx.planner.data_processed import measure_data_processed
    from opteryx.planner.data_processed import plan_relations

    telemetry.increase("billing_bytes", measure_data_processed(optimized_plan))
    telemetry._reading["billing_bytes_by_scan"] = data_processed_by_scan(optimized_plan)
    telemetry.add_relations(plan_relations(optimized_plan))

    # Default: build physical plan
    start = time.monotonic_ns()
    variables = {}
    try:
        variables = conn_context.variables  # type: ignore
    except (AttributeError, TypeError):
        variables = {}

    query_properties = QueryProperties(query_id=query_id, variables=variables)
    physical_plan = create_physical_plan(optimized_plan, query_properties, plan_context)
    telemetry.time_planning_physical_planner += time.monotonic_ns() - start

    # Execute the physical plan and return the executor's generator and ResultType.
    results_generator, result_type = execute_plan(physical_plan, telemetry=telemetry)

    return results_generator, result_type
