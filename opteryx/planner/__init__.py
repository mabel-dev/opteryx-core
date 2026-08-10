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

from opteryx.exceptions import SqlError
from opteryx.expression import NodeType
from opteryx.expression.intervals import normalize_interval_value
from opteryx.models import Node
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
from opteryx.types.schema import ConstantColumn


def _infer_collection_literal(value: Any):
    """Return (ColumnType_for_collection, None) for a list/tuple literal.

    The element type is embedded in the ARRAY ColumnType; no separate sidecar.
    Returns (ARRAY(VARIANT), None) for heterogeneous or unknown-element lists.
    """
    if not isinstance(value, (list, tuple)) or not value:
        return ARRAY(VARIANT), None

    element_types = {build_literal_node(item).type for item in value if item is not None}
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


def build_literal_node(value: Any, root: Optional[Node] = None, suggested_type=None):
    """
    Build a literal node with the appropriate type based on the value.
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

    if root is None:
        root = Node(
            NodeType.LITERAL,
            schema_column=ConstantColumn(name=str(value)),
        )

    if value is None:
        # A None value is a NULL literal. When a concrete type was requested
        # (e.g. folding CAST(NULL AS VARCHAR)) preserve it — an untyped NULL
        # loses the physical tag string-family kernels dispatch on, so a typed
        # NULL string operand would otherwise be read as a garbage arena. With
        # no suggestion the literal stays untyped NULL.
        root.value = None
        root.node_type = NodeType.LITERAL
        root.type = suggested_type if suggested_type is not None else NULL
        root.left = None
        root.right = None
        if root.schema_column is not None:
            root.schema_column.column_type = root.type
        return root

    collection_ct = None
    if suggested_type is None:
        collection_ct, _ = _infer_collection_literal(value)

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
        root.node_type = NodeType.LITERAL
        root.type = suggested_type if suggested_type is not None else type_mapping[value_type]
        root.left = None
        root.right = None
        if root.schema_column is not None:
            root.schema_column.column_type = root.type

    # DEBUG:log (f"Unable to create literal node for {value}, of type {value_type}")
    return root


import re as _re

# DROP STATISTICS ON <table> [FOR COLUMNS <c1>, <c2>, ...]
_DROP_STATS_RE = _re.compile(
    r"^\s*DROP\s+STATISTICS\s+ON\s+(?P<table>[A-Za-z_][\w.$]*)"
    r"(?:\s+FOR\s+COLUMNS\s+(?P<cols>.+?))?\s*;?\s*$",
    _re.IGNORECASE | _re.DOTALL,
)
_DROP_STATS_LEAD = _re.compile(r"^\s*DROP\s+STATISTICS\b", _re.IGNORECASE)


def _intercept_drop_statistics(clean_sql: str):
    """Recognize `DROP STATISTICS ON t [FOR COLUMNS …]` before the SQL parser.

    Returns a synthesized single-statement AST list, or None if the statement is
    not a DROP STATISTICS. A statement that begins with DROP STATISTICS but does
    not match the full grammar fails loudly rather than falling through to the
    parser (which would emit a confusing error or, worse, mis-parse it)."""
    if not _DROP_STATS_LEAD.match(clean_sql):
        return None
    match = _DROP_STATS_RE.match(clean_sql)
    if match is None:
        from opteryx.exceptions import UnsupportedSyntaxError

        raise UnsupportedSyntaxError(
            "Expected: DROP STATISTICS ON <table> [FOR COLUMNS <col>, ...]"
        )
    cols_raw = match.group("cols")
    columns = []
    if cols_raw:
        for part in cols_raw.split(","):
            name = part.strip().strip('"').strip("`")
            if name:
                columns.append(name)
    return [{"DropStatistics": {"table_name": match.group("table"), "columns": columns}}]


# DROP TRIGGER [IF EXISTS] <name> ON <table>
# The table is REQUIRED: trigger names are only unique per dataset, and naming
# the table makes the permission target (WRITE on that table) explicit.
_DROP_TRIGGER_RE = _re.compile(
    r"^\s*DROP\s+TRIGGER\s+(?P<if_exists>IF\s+EXISTS\s+)?"
    r"(?P<name>[A-Za-z_][\w$]*)\s+ON\s+(?P<table>[A-Za-z_][\w.$]*)\s*;?\s*$",
    _re.IGNORECASE | _re.DOTALL,
)
_DROP_TRIGGER_LEAD = _re.compile(r"^\s*DROP\s+TRIGGER\b", _re.IGNORECASE)
_CREATE_TRIGGER_LEAD = _re.compile(r"^\s*CREATE\s+(OR\s+REPLACE\s+)?TRIGGER\b", _re.IGNORECASE)

# REFRESH MATERIALIZED VIEW <name>. sqlparser has no REFRESH statement in the
# Opteryx dialect, so it takes the same pre-parse route DROP TRIGGER does.
_REFRESH_MV_RE = _re.compile(
    r"^\s*REFRESH\s+MATERIALIZED\s+VIEW\s+(?P<name>[A-Za-z_][\w.$]*)\s*;?\s*$",
    _re.IGNORECASE | _re.DOTALL,
)
_REFRESH_LEAD = _re.compile(r"^\s*REFRESH\b", _re.IGNORECASE)


def _intercept_refresh_statements(clean_sql: str):
    """Recognize `REFRESH MATERIALIZED VIEW <name>` before the SQL parser.

    Returns a synthesized single-statement AST list, or None when the statement
    does not begin with REFRESH.

    Anything else beginning with REFRESH is rejected here by name rather than
    left to the parser, which would report it as a generic syntax error several
    layers away from the word that caused it.
    """
    from opteryx.exceptions import UnsupportedSyntaxError

    if not _REFRESH_LEAD.match(clean_sql):
        return None
    match = _REFRESH_MV_RE.match(clean_sql)
    if match is None:
        raise UnsupportedSyntaxError(
            "Expected: **REFRESH MATERIALIZED VIEW** <name>. It is the only "
            "**REFRESH** statement, and it takes no options."
        )
    return [{"RefreshMaterializedView": {"name": match.group("name")}}]


# ALTER MATERIALIZED VIEW <name> OWNER TO <principal>. Same pre-parse route as
# REFRESH, but narrower: ALTER has other legitimate forms (ALTER TABLE, ALTER
# WORKSPACE), so anything not aimed at a materialized view falls through to the
# parser untouched.
_ALTER_MV_LEAD = _re.compile(r"^\s*ALTER\s+MATERIALIZED\s+VIEW\b", _re.IGNORECASE)
_ALTER_MV_OWNER_RE = _re.compile(
    r"^\s*ALTER\s+MATERIALIZED\s+VIEW\s+(?P<name>[A-Za-z_][\w.$]*)\s+"
    r"OWNER\s+TO\s+(?P<owner>'[^']+'|\"[^\"]+\"|[\w.@:+-]+)\s*;?\s*$",
    _re.IGNORECASE | _re.DOTALL,
)


def _intercept_alter_materialized_view(clean_sql: str):
    """Recognize `ALTER MATERIALIZED VIEW <name> OWNER TO <principal>`.

    Returns a synthesized single-statement AST list, or None when the statement
    is not aimed at a materialized view - every other ALTER goes to the parser.

    A statement that IS aimed at one but does not match is rejected here by
    name: ownership is the only alterable property of a view, because
    everything else about it follows from its defining SELECT and changes by
    redefining that.
    """
    from opteryx.exceptions import UnsupportedSyntaxError

    if not _ALTER_MV_LEAD.match(clean_sql):
        return None
    match = _ALTER_MV_OWNER_RE.match(clean_sql)
    if match is None:
        raise UnsupportedSyntaxError(
            "Expected: **ALTER MATERIALIZED VIEW** <name> **OWNER TO** <principal>. "
            "Ownership is the only alterable property of a materialized view - "
            "everything else follows from its defining SELECT, so change it with "
            "**CREATE OR REPLACE MATERIALIZED VIEW**."
        )
    owner = match.group("owner")
    if owner[0] in "'\"":
        owner = owner[1:-1]
    return [{"AlterMaterializedViewOwner": {"name": match.group("name"), "owner": owner}}]


def _intercept_trigger_statements(clean_sql: str):
    """Recognize `DROP TRIGGER [IF EXISTS] <name> ON <table>` before the SQL
    parser (OpteryxDialect is not in sqlparser's allowlist for trigger
    statements, so they would otherwise fail to parse with an unhelpful error).

    Returns a synthesized single-statement AST list, or None if the statement
    is not a trigger statement. `CREATE TRIGGER` is rejected here by name -
    triggers exist only as the automatic artifact of CREATE MATERIALIZED VIEW.
    """
    from opteryx.exceptions import UnsupportedSyntaxError

    if _CREATE_TRIGGER_LEAD.match(clean_sql):
        raise UnsupportedSyntaxError(
            "CREATE TRIGGER is not supported; triggers are created automatically "
            "by **CREATE MATERIALIZED VIEW**. A materialized view gets its trigger when it is created."
        )
    if not _DROP_TRIGGER_LEAD.match(clean_sql):
        return None
    match = _DROP_TRIGGER_RE.match(clean_sql)
    if match is None:
        # CASCADE/RESTRICT (or any other trailing modifier) lands here: the
        # grammar above accepts nothing after the table name.
        raise UnsupportedSyntaxError(
            "Expected: DROP TRIGGER [IF **EXISTS**] <name> ON <table> "
            "(no CASCADE/RESTRICT; the table name is required)"
        )
    return [
        {
            "DropTrigger": {
                "trigger_name": match.group("name"),
                "table_name": match.group("table"),
                "if_exists": match.group("if_exists") is not None,
            }
        }
    ]


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
    """
    from opteryx.models import QueryProperties
    from opteryx.planner.ast_rewriter import do_ast_rewriter
    from opteryx.planner.binder import do_bind_phase
    from opteryx.planner.logical_planner import do_logical_planning_phase
    from opteryx.planner.optimizer import do_optimizer
    from opteryx.planner.physical_planner import create_physical_plan
    from opteryx.planner.plan_rewriter import do_plan_rewrite
    from opteryx.planner.relation_resolver import do_resolve_relations
    from opteryx.planner.sql_rewriter import do_sql_rewrite
    from opteryx.third_party import sqloxide

    # SQL Rewriter
    start = time.monotonic_ns()
    clean_sql = do_sql_rewrite(operation, source=source, source_offset=source_offset)
    telemetry.time_planning_sql_rewriter += time.monotonic_ns() - start

    params: Union[list, dict, None] = None
    if parameters is None:
        params = []
    elif isinstance(parameters, dict):
        params = parameters.copy()
    else:
        params = [p for p in parameters or []]

    # Parser converts the SQL command into an AST.
    # DROP STATISTICS has no native sqlparser grammar (and `ALTER TABLE … DROP
    # STATISTICS` mis-parses STATISTICS as a column name), so it is recognized
    # here in the pre-parse layer and synthesized into an AST directly.
    parsed_statements = _intercept_drop_statistics(clean_sql)
    if parsed_statements is None:
        parsed_statements = _intercept_trigger_statements(clean_sql)
    if parsed_statements is None:
        parsed_statements = _intercept_refresh_statements(clean_sql)
    if parsed_statements is None:
        parsed_statements = _intercept_alter_materialized_view(clean_sql)
    if parsed_statements is None:
        try:
            parsed_statements = sqloxide.parse_sql(clean_sql, _dialect="opteryx")
        except ValueError as parser_error:
            from opteryx.planner.parse_error import raise_parse_error

            # `clean_sql` carries both texts: the one the parser was given, which the
            # reported line/column index, and the one the reader wrote, which is what
            # the caret gets printed against. It maps between them.
            raise_parse_error(clean_sql, parser_error)
    # AST Rewriter adds temporal filters and parameters to the AST
    start = time.monotonic_ns()
    parsed_statement = do_ast_rewriter(parsed_statements, parameters=params)[0]
    telemetry.time_planning_ast_rewriter += time.monotonic_ns() - start

    # Logical Planner converts ASTs to logical plans.
    #
    # From here to the end of binding, an error that named a node - an unknown column,
    # an unknown function - gets a caret pointing at where that node was written. This
    # is the only place that holds both halves: the raise sites know the node, and
    # `clean_sql` knows both the text the parser saw and the text the reader submitted.
    # The error is re-raised unchanged; only its presentation is filled in.
    try:
        logical_plan, ast, ctes = do_logical_planning_phase(parsed_statement)  # type: ignore
    except SqlError as error:
        attach_source_position(error, clean_sql)
        raise

    # Relation Resolver: expand CTE and view references into the plan. Runs BEFORE the
    # rewriter so the rewriter sees one fully-expanded plan — a subquery inside a view or
    # CTE body is eliminated by the same pass that handles the main query.
    start = time.monotonic_ns()
    logical_plan = do_resolve_relations(logical_plan, ctes, telemetry)
    telemetry.time_planning_relation_resolver += time.monotonic_ns() - start

    # Plan Rewriter: structural rewrites on the unbound, fully-expanded logical plan
    start = time.monotonic_ns()
    logical_plan = do_plan_rewrite(logical_plan, telemetry)
    telemetry.time_planning_plan_rewriter += time.monotonic_ns() - start

    # check user has permission for this query type
    query_type = next(iter(ast))
    # Special-case DROP VIEW -> treat as DropView permission
    # ast["Drop"]["object_type"] is the object type (e.g., "View") when Drop is a mapping
    if query_type == "Drop" and isinstance(ast["Drop"], dict):
        if ast["Drop"].get("object_type") == "View":
            query_type = "DropView"

    # The Binder adds schema information to the logical plan
    start = time.monotonic_ns()
    try:
        bound_plan = do_bind_phase(
            logical_plan,
            execution_context=execution_context,
            query_id=query_id,
            visibility_filters=visibility_filters,
            telemetry=telemetry,
        )
    except SqlError as error:
        attach_source_position(error, clean_sql)
        raise
    telemetry.time_planning_binder += time.monotonic_ns() - start

    start = time.monotonic_ns()
    optimized_plan = do_optimizer(bound_plan, telemetry)
    telemetry.time_planning_optimizer += time.monotonic_ns() - start

    # Refuse a query whose result is already known to blow the row limit, BEFORE any
    # data is read — an accidental cross join should cost nothing, not an hour of IO.
    # Only fires when every input has real row counts; see result_size_guard.
    from opteryx.planner.result_size_guard import check_estimated_result_size
    from opteryx.variables import resolve as _resolve_var

    optimized_plan = check_estimated_result_size(
        optimized_plan,
        _resolve_var("sql_select_limit", execution_context.variables, 0),
        telemetry=telemetry,
    )

    # Default: build traditional physical plan
    # before we write the new optimizer and execution engine, convert to a V1 plan
    start = time.monotonic_ns()
    query_properties = QueryProperties(query_id=query_id, variables=execution_context.variables)
    physical_plan = create_physical_plan(optimized_plan, query_properties)
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

    # Externally-supplied logical plans still reference relations by name, so they go
    # through the same resolver. They carry no CTEs — a CTE only exists in SQL text.
    start = time.monotonic_ns()
    logical_plan = do_resolve_relations(logical_plan, None, telemetry)
    telemetry.time_planning_relation_resolver += time.monotonic_ns() - start

    # Must run AFTER relation resolution and BEFORE the binder, exactly as query_planner
    # orders it. An externally-supplied plan carries no subqueries of its own -- but once
    # the resolver splices a VIEW body into it, it carries whatever SQL that view was
    # written in. Constructs like IN (<subquery>) and INTERSECT/EXCEPT are *lowered* here
    # (to semi/anti joins); nothing downstream can execute them un-lowered -- there is no
    # physical operator for an InSubQuery -- so omitting this stage does not merely
    # forfeit an optimisation, it makes any view containing one fail at execution.
    start = time.monotonic_ns()
    logical_plan = do_plan_rewrite(logical_plan, telemetry)
    telemetry.time_planning_plan_rewriter += time.monotonic_ns() - start

    # The Binder adds schema information to the logical plan
    start = time.monotonic_ns()
    bound_plan = do_bind_phase(
        logical_plan,
        execution_context=conn_context,
        query_id=query_id,
        visibility_filters=visibility_filters,
        telemetry=telemetry,
    )
    telemetry.time_planning_binder += time.monotonic_ns() - start

    start = time.monotonic_ns()
    optimized_plan = do_optimizer(bound_plan, telemetry)
    telemetry.time_planning_optimizer += time.monotonic_ns() - start

    # Default: build physical plan
    start = time.monotonic_ns()
    variables = {}
    try:
        variables = conn_context.variables  # type: ignore
    except (AttributeError, TypeError):
        variables = {}

    query_properties = QueryProperties(query_id=query_id, variables=variables)
    physical_plan = create_physical_plan(optimized_plan, query_properties)
    telemetry.time_planning_physical_planner += time.monotonic_ns() - start

    # Execute the physical plan and return the executor's generator and ResultType.
    results_generator, result_type = execute_plan(physical_plan, telemetry=telemetry)

    return results_generator, result_type
