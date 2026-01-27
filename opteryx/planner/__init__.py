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

from opteryx.datatypes.intervals import normalize_interval_value
from opteryx.managers.expression import NodeType
from opteryx.models import Node


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
        from opteryx.exceptions import SqlError

        raise SqlError(parser_error) from parser_error
    # AST Rewriter adds temporal filters and parameters to the AST
    start = time.monotonic_ns()
    parsed_statement = do_ast_rewriter(parsed_statements, parameters=params)[0]
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

    # The Binder adds schema information to the logical plan
    start = time.monotonic_ns()
    bound_plan = do_bind_phase(
        logical_plan,
        execution_context=execution_context,
        query_id=query_id,
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
        try:
            from opteryx.planner.substrait_builder import build_substrait_plan

            start = time.monotonic_ns()
            query_properties = QueryProperties(
                query_id=query_id, variables=execution_context.variables
            )
            substrait_plan = build_substrait_plan(optimized_plan, query_properties)
            telemetry.time_planning_physical_planner += time.monotonic_ns() - start

            return substrait_plan
        except ImportError:
            # Fallback to physical planner if substrait builder not available
            pass

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
    common_table_expressions=None,
    visibility_filters: Optional[Dict[str, Any]] = None,
    output_format: str = "physical",
):
    """
    Execute an already-constructed logical plan through bind, optimizer and
    physical planning so it can be executed by the executor or returned as
    a Substrait plan. Intended for use by external services that generate
    logical plans (eg. OData service).
    """
    import uuid

    import pyarrow

    from opteryx.constants import ResultType
    from opteryx.exceptions import SqlError
    from opteryx.managers.execution import execute as execute_plan
    from opteryx.models import ExecutionContext
    from opteryx.models import QueryProperties
    from opteryx.models import QueryTelemetry
    from opteryx.planner.binder import do_bind_phase
    from opteryx.planner.optimizer import do_optimizer
    from opteryx.planner.physical_planner import create_physical_plan

    # Prepare query_id and telemetry defaults
    if query_id is None:
        query_id = str(uuid.uuid4())
    if telemetry is None:
        telemetry = QueryTelemetry(query_id)

    # Determine execution context for binder
    if connection is None:
        conn_context = ExecutionContext(memberships=[])
    elif hasattr(connection, "context"):
        conn_context = connection.context
    else:
        conn_context = connection

    # The Binder adds schema information to the logical plan
    start = time.monotonic_ns()
    bound_plan = do_bind_phase(
        logical_plan,
        connection=conn_context,
        query_id=query_id,
        common_table_expressions=None,  # executing logical plans: no CTEs
        visibility_filters=visibility_filters,
        telemetry=telemetry,
    )
    telemetry.time_planning_binder += time.monotonic_ns() - start

    start = time.monotonic_ns()
    optimized_plan = do_optimizer(bound_plan, telemetry)
    telemetry.time_planning_optimizer += time.monotonic_ns() - start

    # Choose output format
    if output_format == "substrait":
        try:
            from opteryx.planner.substrait_builder import build_substrait_plan

            start = time.monotonic_ns()
            query_properties = QueryProperties(
                query_id=query_id, variables=connection.context.variables
            )
            substrait_plan = build_substrait_plan(optimized_plan, query_properties)
            telemetry.time_planning_physical_planner += time.monotonic_ns() - start

            return substrait_plan
        except ImportError:
            # fallback to traditional physical planner
            pass

    # Default: build physical plan
    start = time.monotonic_ns()
    variables = {}
    try:
        variables = connection.context.variables  # type: ignore
    except (AttributeError, TypeError):
        variables = {}

    query_properties = QueryProperties(query_id=query_id, variables=variables)
    physical_plan = create_physical_plan(optimized_plan, query_properties)
    telemetry.time_planning_physical_planner += time.monotonic_ns() - start

    # Execute the physical plan and return a single pyarrow.Table
    results_generator, result_type = execute_plan(physical_plan, telemetry=telemetry)

    # Handle statistics-only (execute_plan may have returned a simple generator)
    if result_type == ResultType.NON_TABULAR:
        import orso
        from orso.schema import FlatColumn
        from orso.schema import RelationSchema

        # Consume generator to get the first non-empty result (if any)
        data = next(results_generator, None)
        if data is None:
            # return an empty meta table
            meta_dataframe = orso.DataFrame(
                rows=[(0,)],
                schema=RelationSchema(
                    name="table",
                    columns=[FlatColumn(name="rows_affected", type=OrsoTypes.INTEGER)],
                ),
            )
            return meta_dataframe.arrow()
        # If data is already an Arrow table, return it
        if isinstance(data, pyarrow.Table):
            return data
        # else assume it's orso-like and convert
        return data.arrow()

    # For tabular results, the generator yields pyarrow tables (or EOS)
    try:
        first_table = next(results_generator, None)
        if first_table is None:
            # No rows; return empty table with schema from physical plan Exit node
            from orso.schema import RelationSchema
            from orso.schema import convert_orso_schema_to_arrow_schema

            exit_node = physical_plan.get_exit_points()[0]
            exit_instance = physical_plan[exit_node]
            orso_schema = RelationSchema(
                name="Relation", columns=[c.schema_column for c in exit_instance.columns]
            )
            arrow_schema = convert_orso_schema_to_arrow_schema(orso_schema, use_identities=True)
            return pyarrow.Table.from_arrays(
                [pyarrow.array([]) for _ in exit_instance.columns], schema=arrow_schema
            )

        # If result is a single table, return it directly
        if (
            isinstance(first_table, pyarrow.Table)
            and getattr(first_table, "num_rows", None) is not None
            and len(first_table.column_names) == len(set(first_table.column_names))
        ):
            # attempt to concatenate remaining tables if generator returns more
            from itertools import chain

            rest = results_generator
            if rest is not None:
                try:
                    combined = pyarrow.concat_tables(
                        chain([first_table], rest), promote_options="permissive"
                    )
                    return combined
                except (pyarrow.ArrowInvalid, pyarrow.ArrowTypeError):
                    return first_table
            return first_table

        # Otherwise, concatenate streaming tables handling duplicate names similarly to Cursor.execute_to_arrow
        from itertools import chain

        if first_table is not None:
            column_names = first_table.column_names
            if len(column_names) != len(set(column_names)):
                temporary_names = [f"col_{i}" for i in range(len(column_names))]
                first_table = first_table.rename_columns(temporary_names)
                return_table = pyarrow.concat_tables(
                    chain(
                        [first_table],
                        (t.rename_columns(temporary_names) for t in results_generator),
                    ),
                    promote_options="permissive",
                )
                return return_table.rename_columns(column_names)

        table = pyarrow.concat_tables(
            chain([first_table], results_generator), promote_options="permissive"
        )
        return table
    except StopIteration:
        # no results
        return pyarrow.Table.from_batches([])
