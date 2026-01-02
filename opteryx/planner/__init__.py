# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
~~~
                      ┌───────────┐
                      │   USER    │
         ┌────────────┤           ◄────────────┐
         │SQL         └───────────┘            │
  ───────┼─────────────────────────────────────┼──────
         │                                     │
   ┌─────▼─────┐                               │
   │ SQL       │                               │
   │ Rewriter  │                               │
   └─────┬─────┘                               │
         │SQL                                  │Results
   ┌─────▼─────┐                         ┌─────┴─────┐
   │           │                         │           │
   │ Parser    │                         │ Executor  │
   └─────┬─────┘                         └─────▲─────┘
         │AST                                  │Plan
   ┌─────▼─────┐      ┌───────────┐      ┌─────┴─────┐
   │ AST       │      │           │      │ Physical  │
   │ Rewriter  │      │ Catalogue │      │ Planner   │
   └─────┬─────┘      └───────────┘      └─────▲─────┘
         │AST               │Schemas           │Plan
   ┌─────▼─────┐      ┌─────▼─────┐      ┌─────┴─────┐
   │ Logical   │ Plan │           │ Plan │           │
   │   Planner ├──────► Binder    ├──────► Optimizer │
   └───────────┘      └───────────┘      └───────────┘

~~~
"""

import datetime
import decimal
import time
from typing import Any
from typing import Dict
from typing import Generator
from typing import Iterable
from typing import Optional
from typing import Union

import numpy
from orso.schema import ConstantColumn
from orso.types import OrsoTypes

from opteryx import config
from opteryx.datatypes.intervals import normalize_interval_value
from opteryx.managers.expression import NodeType
from opteryx.models import Node
from opteryx.models import PhysicalPlan


def build_literal_node(
    value: Any, root: Optional[Node] = None, suggested_type: Optional[OrsoTypes] = None
):
    """
    Build a literal node with the appropriate type based on the value.
    """
    # Convert value if it has `as_py` method (e.g., from PyArrow)
    if hasattr(value, "as_py"):
        value = value.as_py()

    if root is None:
        root = Node(NodeType.LITERAL, schema_column=ConstantColumn(name=str(value)))

    if value is None:
        # Matching None has complications
        root.value = None
        root.node_type = NodeType.LITERAL
        root.type = OrsoTypes.NULL
        root.left = None
        root.right = None
        return root

    # Define a mapping of types to OrsoTypes
    type_mapping = {
        bool: OrsoTypes.BOOLEAN,
        numpy.bool_: OrsoTypes.BOOLEAN,
        str: OrsoTypes.VARCHAR,
        numpy.str_: OrsoTypes.VARCHAR,
        bytes: OrsoTypes.BLOB,
        numpy.bytes_: OrsoTypes.BLOB,
        int: OrsoTypes.INTEGER,
        numpy.int64: OrsoTypes.INTEGER,
        float: OrsoTypes.DOUBLE,
        numpy.float64: OrsoTypes.DOUBLE,
        numpy.datetime64: OrsoTypes.TIMESTAMP,
        datetime.datetime: OrsoTypes.TIMESTAMP,
        datetime.time: OrsoTypes.TIME,
        datetime.date: OrsoTypes.DATE,
        decimal.Decimal: OrsoTypes.DECIMAL,
        list: OrsoTypes.ARRAY,
        tuple: OrsoTypes.ARRAY,
    }

    value_type = type(value)
    # Determine the type from the value using the mapping
    if value_type in type_mapping or suggested_type not in (OrsoTypes._MISSING_TYPE, 0, None):
        if suggested_type == OrsoTypes.INTERVAL:
            value = normalize_interval_value(value)
        root.value = value
        root.node_type = NodeType.LITERAL
        root.type = (
            suggested_type
            if suggested_type not in (OrsoTypes._MISSING_TYPE, 0, None)
            else type_mapping[value_type]
        )
        root.left = None
        root.right = None
        root.schema_column.type = root.type

    # DEBUG:log (f"Unable to create literal node for {value}, of type {value_type}")
    return root


def query_planner(
    operation: str,
    parameters: Union[Iterable, Dict, None],
    visibility_filters: Optional[Dict[str, Any]],
    connection,
    qid: str,
    telemetry,
    output_format: str = "physical",
) -> Union[Generator[Any, Any, Any], Dict[str, Any]]:
    from opteryx.exceptions import SqlError
    from opteryx.models import QueryProperties
    from opteryx.planner.ast_rewriter import do_ast_rewriter
    from opteryx.planner.binder import do_bind_phase
    from opteryx.planner.logical_planner import do_logical_planning_phase
    from opteryx.planner.optimizer import do_optimizer
    from opteryx.planner.physical_planner import create_physical_plan
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

    # Parser converts the SQL command into an AST
    try:
        parsed_statements = sqloxide.parse_sql(clean_sql, _dialect="opteryx")
    except ValueError as parser_error:
        raise SqlError(parser_error) from parser_error
    # AST Rewriter adds temporal filters and parameters to the AST
    start = time.monotonic_ns()
    parsed_statement = do_ast_rewriter(
        parsed_statements,
        parameters=params,
        connection=connection,
    )[0]
    telemetry.time_planning_ast_rewriter += time.monotonic_ns() - start

    # Logical Planner converts ASTs to logical plans

    logical_plan, ast, ctes = do_logical_planning_phase(parsed_statement)  # type: ignore
    # check user has permission for this query type
    query_type = next(iter(ast))
    # Special-case DROP VIEW -> treat as DropView permission
    if query_type == "Drop":
        try:
            # ast["Drop"]["object_type"] is expected to be the object type (e.g., "View")
            if ast["Drop"].get("object_type") == "View":
                query_type = "DropView"
        except Exception:
            pass

    if query_type not in connection.permissions:
        from opteryx.exceptions import PermissionsError

        raise PermissionsError(f"User does not have permission to execute '{query_type}' queries.")

    # The Binder adds schema information to the logical plan
    start = time.monotonic_ns()
    bound_plan = do_bind_phase(
        logical_plan,
        connection=connection.context,
        qid=qid,
        common_table_expressions=ctes,
        visibility_filters=visibility_filters,
        telemetry=telemetry,
    )
    telemetry.time_planning_binder += time.monotonic_ns() - start

    start = time.monotonic_ns()
    optimized_plan = do_optimizer(bound_plan, telemetry)
    telemetry.time_planning_optimizer += time.monotonic_ns() - start

    # Choose output format
    if output_format == "substrait":
        # Build Substrait representation directly from optimized logical plan
        from opteryx.planner.substrait_builder import build_substrait_plan

        start = time.monotonic_ns()
        query_properties = QueryProperties(qid=qid, variables=connection.context.variables)
        substrait_plan = build_substrait_plan(optimized_plan, query_properties)
        telemetry.time_planning_physical_planner += time.monotonic_ns() - start

        return substrait_plan

    # Default: build traditional physical plan
    # before we write the new optimizer and execution engine, convert to a V1 plan
    start = time.monotonic_ns()
    query_properties = QueryProperties(qid=qid, variables=connection.context.variables)
    physical_plan = create_physical_plan(optimized_plan, query_properties)
    telemetry.time_planning_physical_planner += time.monotonic_ns() - start

    return physical_plan
