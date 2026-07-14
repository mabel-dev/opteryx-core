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


def query_planner(
    operation: str,
    parameters: Union[Iterable, Dict, None],
    visibility_filters: Optional[Dict[str, Any]],
    execution_context,
    query_id: str,
    telemetry,
    output_format: str = "physical",
) -> Union[Generator[Any, Any, Any], Dict[str, Any]]:
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
    clean_sql = do_sql_rewrite(operation)
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
        try:
            parsed_statements = sqloxide.parse_sql(clean_sql, _dialect="opteryx")
        except ValueError as parser_error:
            from opteryx.exceptions import SqlError

            raise SqlError(parser_error) from parser_error
    # AST Rewriter adds temporal filters and parameters to the AST
    start = time.monotonic_ns()
    parsed_statement = do_ast_rewriter(parsed_statements, parameters=params)[0]
    telemetry.time_planning_ast_rewriter += time.monotonic_ns() - start

    # Logical Planner converts ASTs to logical plans

    logical_plan, ast, ctes = do_logical_planning_phase(parsed_statement)  # type: ignore

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
    bound_plan = do_bind_phase(
        logical_plan,
        execution_context=execution_context,
        query_id=query_id,
        visibility_filters=visibility_filters,
        telemetry=telemetry,
    )
    telemetry.time_planning_binder += time.monotonic_ns() - start

    start = time.monotonic_ns()
    optimized_plan = do_optimizer(bound_plan, telemetry)
    telemetry.time_planning_optimizer += time.monotonic_ns() - start

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
