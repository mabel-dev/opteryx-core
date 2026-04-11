# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""Session object that *is* the cursor.

This implementation replaces the legacy `Cursor` by inheriting from it and
making the session object the primary execution surface. The class keeps
the `ExecutionContext` previously owned by `Connection` and preserves the
cursor execution behavior by reusing the existing `Cursor` implementation.

Design goals:
- Session *replaces* Cursor (no internal delegation/wrapping)
- Minimize code duplication by subclassing `Cursor`
- Provide a minimalist `cursor()` compatibility that returns `self`
- Keep `close()`, `__enter__/__exit__`, and execution methods unchanged
  (they are inherited from `Cursor`)

Note: This approach keeps the tested `Cursor` execution semantics and
lets us collapse Connection+Cursor into a single object with minimal
code churn.
"""

import re
import time
from typing import Any, Dict, Iterable, Iterator, List, Optional, Tuple, Union
from uuid import uuid4

import pyarrow

from opteryx import EOS, config, utils
from opteryx.constants import QueryStatus, ResultType
from opteryx.exceptions import (
    InconsistentSchemaError,
    InvalidCursorStateError,
    MissingSqlStatement,
    ProgrammingError,
    SqlError,
    UnsupportedSyntaxError,
)
from opteryx.managers.billing import BillingEventType, write_billing_event
from opteryx.models import ExecutionContext, QueryTelemetry
from opteryx.models.dataframe import DataFrame
from opteryx.tracing import record_event
from opteryx.types import OrsoTypes
from opteryx.types.schema import FlatColumn, RelationSchema
from opteryx.utils import arrow_interop as converters
from opteryx.utils import sql

_CAMEL_SPLIT_RE = re.compile(r"[A-Z][a-z]*|[0-9]+")


class Session(DataFrame):
    """Session acts as the canonical execution object and replaces Cursor.

    It subclasses `Cursor` to reuse the DataFrame and execution logic and
    sets up the `ExecutionContext` that planners expect on `connection.context`.
    """

    def __init__(
        self,
        *,
        user: Optional[str] = None,
        memberships: Optional[Iterable[str]] = None,
        schema: Optional[str] = None,
        access_policies: Optional[Iterable[dict]] = None,
        query_id: Optional[str] = None,
        **kwargs,
    ):
        # reject removed parameters explicitly
        if "io_trace_file" in kwargs:
            raise TypeError(
                "io_trace_file argument has been removed; events are recorded in "
                "memory and exposed via session.trace()"
            )
        # input validation consistent with the old Connection
        if memberships and not all(isinstance(v, str) for v in memberships):
            raise ProgrammingError("Invalid memberships provided to Session")
        if user and not isinstance(user, str):
            raise ProgrammingError("Invalid user provided to Session")
        if access_policies and not all(isinstance(v, dict) for v in access_policies):
            raise ProgrammingError("Invalid access_policies provided to Session")
        if memberships is None:
            memberships = ["opteryx"]
        if access_policies is None:
            access_policies = [{"pattern": "*", "role": "owner"}]

        # Provide execution context expected by planner & execution code
        self.context = ExecutionContext(
            query_id=query_id,
            user=user,
            access_policies=access_policies,
            schema=schema,
            memberships=memberships,
        )

        # Initialize cursor-like state (merged from previous Cursor implementation)
        self.arraysize = 1
        self._query_planner = None
        self._collected_stats = None
        self._plan = None
        self._query_id = query_id if query_id is not None else str(uuid4())
        self._telemetry = QueryTelemetry(self._query_id)
        self._query_status = QueryStatus._UNDEFINED
        self._result_type = ResultType._UNDEFINED
        self._rowcount = None
        self._description: Optional[Tuple[Tuple[Any, ...], ...]] = None
        self._owns_connection = False
        self._closed = False
        self._executed = False

        DataFrame.__init__(self, rows=[], schema=[])

        # Initialize IO tracing state.  Tracing is governed solely by the
        # global ``config.OPTERYX_TRACE`` flag and events are kept in memory.
        # The engine no longer supports writing to a file, and there is no
        # per-session path.  ``_tracing_enabled`` controls whether the session
        # will emit start/end markers and allow ``session.trace()`` to run.
        self._tracing_enabled = bool(config.OPTERYX_TRACE)

        # if tracing is active, register our session id so the recorder can
        # tag subsequent events automatically.  We push the id globally and
        # clear it on close.
        if self._tracing_enabled:
            from opteryx.tracing import event_recorder

            event_recorder._current_session_id = self._query_id

    @property
    def query_id(self) -> str:
        return self._query_id

    def _inner_execute(
        self,
        operation: str,
        params: Union[Iterable, Dict, None] = None,
        visibility_filters: Optional[Dict[str, Any]] = None,
    ) -> Any:
        from opteryx.managers.execution import execute
        from opteryx.planner import query_planner

        if not operation:  # pragma: no cover
            raise MissingSqlStatement("SQL provided was empty.")

        start = time.time_ns()
        processing_bytes_estimate = 0
        try:
            self._plan = query_planner(
                operation=operation,
                parameters=params,
                visibility_filters=visibility_filters,
                execution_context=self.context,
                query_id=self.query_id,
                telemetry=self._telemetry,
            )

            # Extract bytes estimate from scan nodes in the plan
            for nid, node in self._plan.nodes(data=True):
                if getattr(node, "is_scan", False):
                    try:
                        # Get structured config directly without full dict conversion
                        node_config = (
                            node.plan_config()
                            if hasattr(node, "plan_config")
                            else getattr(node, "config", None)
                        )
                        if isinstance(node_config, dict) and "projection" in node_config:
                            for proj_col in node_config.get("projection", []):
                                col_bytes = proj_col.get("total-bytes") or 0
                                processing_bytes_estimate += col_bytes
                    except Exception:
                        # If extraction fails, continue without bytes for this node
                        pass

        except RuntimeError as err:  # pragma: no cover
            raise SqlError(f"Error Executing SQL Statement ({err}) (QID:{self.query_id})") from err
        finally:
            self._telemetry.time_planning += time.time_ns() - start

        results = execute(self._plan, telemetry=self._telemetry)

        write_billing_event(
            billing_event=BillingEventType.QUERY_EXECUTION,
            billing_account="opteryx",
            event_details={
                "user": self.context.user,
                "query_id": self.query_id,
                "query": operation,
            },
        )
        write_billing_event(
            billing_event=BillingEventType.DATA_PROCESSED_BYTES,
            billing_account="opteryx",
            event_details={
                "user": self.context.user,
                "query_id": self.query_id,
                "query": operation,
                "bytes_processed": processing_bytes_estimate,
            },
        )

        return results

    def _execute_statements(
        self,
        operation: str,
        params: Optional[Iterable] = None,
        visibility_filters: Optional[Dict[str, Any]] = None,
    ):
        self._telemetry.start_time = time.time_ns()

        if hasattr(operation, "decode"):
            operation = operation.decode()

        operation = sql.remove_comments(operation)
        operation = sql.clean_statement(operation)
        statements = sql.split_sql_statements(operation)

        if len(statements) == 0:
            raise MissingSqlStatement("No statement found")

        if len(statements) > 1 and params is not None and not isinstance(params, dict) and params:
            raise UnsupportedSyntaxError(
                "Batched queries cannot be parameterized with parameter lists, use named parameters."
            )

        results = None
        for index, statement in enumerate(statements):
            results = self._inner_execute(statement, params, visibility_filters)
            if index < len(statements) - 1:
                for _ in results:
                    pass

        # we only return the last result set
        return results

    def execute(
        self,
        operation: str,
        params: Optional[Iterable] = None,
        visibility_filters: Optional[Dict[str, Any]] = None,
    ):
        self._ensure_open()
        if self._tracing_enabled:
            try:
                record_event("trace_session_start", session_id=self._query_id, query=operation)
            except Exception:
                pass
        start = time.time_ns()
        results = self._execute_statements(operation, params, visibility_filters)
        if results is not None:
            result_data, self._result_type = results
            if self._result_type == ResultType.NON_TABULAR:
                meta_dataframe = DataFrame(
                    rows=[(result_data.record_count,)],  # type: ignore
                    schema=RelationSchema(
                        name="table",
                        columns=[FlatColumn(name="rows_affected", type=OrsoTypes.INTEGER)],
                    ),
                )  # type: ignore
                self._rows = meta_dataframe._rows
                self._schema = meta_dataframe._schema

                self._rowcount = result_data.record_count  # type: ignore
                self._query_status = result_data.status  # type: ignore
            elif self._result_type == ResultType.TABULAR:
                # Ensure each item in result_data is an Arrow Table before passing to
                # converters.from_arrow, which expects pyarrow.Table items.
                def _to_arrow_gen(items):
                    for item in items:
                        if hasattr(item, "to_arrow"):
                            yield item.to_arrow()
                        else:
                            yield item

                self._rows, self._schema = converters.from_arrow(_to_arrow_gen(result_data))
                self._cursor = iter(self._rows)
                self._query_status = QueryStatus.SQL_SUCCESS
            else:  # pragma: no cover
                self._query_status = QueryStatus.SQL_FAILURE
            self._description = self._schema_to_description(self._schema)
        else:
            self._description = None
        # time_executing includes planning time, so subtract it to get just execution time
        elapsed = time.time_ns() - start
        self._telemetry.time_executing += elapsed - self._telemetry.time_planning
        self._executed = True

    def execute_logical_plan(self, logical_plan, **kwargs):
        """
        Execute a logical plan by delegating to the planner module. qid, telemetry
        and connection are optional to support external callers that only have a
        logical plan (eg. OData service).
        """
        from opteryx import planner

        return planner.execute_logical_plan(logical_plan, **kwargs)

    def plan(
        self,
        operation: str,
        params: Optional[Iterable] = None,
        visibility_filters: Optional[Dict[str, Any]] = None,
    ) -> dict:
        self._ensure_open()

        from opteryx.planner import query_planner

        start = time.time_ns()
        physical_plan = query_planner(
            operation=operation,
            parameters=params,
            visibility_filters=visibility_filters,
            execution_context=self.context,
            query_id=self.query_id,
            telemetry=self._telemetry,
        )
        self._telemetry.time_planning += time.time_ns() - start

        # Temporarily set the plan so we can use _get_plan_dict
        old_plan = self._plan
        self._plan = physical_plan
        plan_dict = self._get_plan_dict()
        self._plan = old_plan

        return plan_dict

    @property
    def result_type(self) -> ResultType:
        return self._result_type

    @property
    def query_status(self) -> QueryStatus:
        return self._query_status

    @property
    def rowcount(self) -> int:
        if self._result_type == ResultType.TABULAR:
            return super().rowcount
        if self._result_type == ResultType.NON_TABULAR:
            return self._rowcount
        raise InvalidCursorStateError("Session not in valid state to return a row count.")

    @property
    def description(self) -> Optional[Tuple[Tuple[Any, ...], ...]]:
        """DBAPI-compatible column description metadata."""
        return self._description

    def execute_to_arrow(
        self,
        operation: str,
        params: Optional[Iterable] = None,
        limit: Optional[int] = None,
        visibility_filters: Optional[Dict[str, Any]] = None,
    ) -> pyarrow.Table:
        """
        Executes the SQL operation and returns results in Arrow format.

        The query engine emits Draken morsels. This method converts them to PyArrow.
        Vectors handle their own type conversions (e.g., IntervalVector → month_day_nano_interval).
        """
        from opteryx.compiled.draken.morsels.morsel import Morsel

        self._ensure_open()
        if self._tracing_enabled:
            try:
                record_event("trace_session_start", session_id=self._query_id, query=operation)
            except Exception:
                pass
        start = time.time_ns()
        results = self._execute_statements(operation, params, visibility_filters)
        if results is not None:
            result_data, self._result_type = results

            if self._result_type == ResultType.NON_TABULAR:
                meta_dataframe = DataFrame(
                    rows=[(result_data.record_count,)],  # type: ignore
                    schema=RelationSchema(
                        name="table",
                        columns=[FlatColumn(name="rows_affected", type=OrsoTypes.INTEGER)],
                    ),
                )  # type: ignore
                self._executed = True
                return meta_dataframe.arrow()

            if limit is not None:
                # Handle both Draken morsels and Arrow tables
                if isinstance(result_data, Morsel) or (
                    hasattr(result_data, "__iter__") and not isinstance(result_data, pyarrow.Table)
                ):
                    # Convert morsels to Arrow first, then limit
                    tables = []
                    count = 0
                    for morsel in result_data:
                        if count >= limit:
                            break
                        table = morsel.to_arrow() if isinstance(morsel, Morsel) else morsel
                        if table.num_rows + count > limit:
                            table = table.slice(0, limit - count)
                        tables.append(table)
                        count += table.num_rows
                    result_data = tables if tables else [pyarrow.Table.from_batches([])]
                else:
                    result_data = utils.arrow.limit_records(result_data, limit)  # type: ignore

        # Handle single Arrow table (direct result or from fallback path)
        if isinstance(result_data, pyarrow.Table):
            self._executed = True
            return result_data

        # Handle Draken morsels or iterables of morsels/tables
        try:
            # Convert morsels to Arrow tables
            arrow_tables = []
            for item in result_data:
                if isinstance(item, Morsel):
                    arrow_tables.append(item.to_arrow())
                elif isinstance(item, pyarrow.Table):
                    arrow_tables.append(item)
                elif item is not None:
                    # Skip EOS and other sentinel values
                    continue

            if not arrow_tables:
                # Return empty table
                self._executed = True
                return pyarrow.Table.from_batches([])

            # Handle duplicate column names in concatenation
            first_table = arrow_tables[0]
            column_names = first_table.column_names
            if len(column_names) != len(set(column_names)):
                temporary_names = [f"col_{i}" for i in range(len(column_names))]
                arrow_tables = [t.rename_columns(temporary_names) for t in arrow_tables]
                result_table = pyarrow.concat_tables(arrow_tables, promote_options="permissive")
                result_table = result_table.rename_columns(column_names)
            else:
                result_table = pyarrow.concat_tables(arrow_tables, promote_options="permissive")

            self._executed = True
            elapsed = time.time_ns() - start
            self._telemetry.time_executing += elapsed - self._telemetry.time_planning
            return result_table

        except (
            pyarrow.ArrowInvalid,
            pyarrow.ArrowTypeError,
        ) as err:  # pragma: no cover
            if "struct" in str(err):
                raise InconsistentSchemaError(
                    f"Unable to resolve different schemas, most likely related to a STRUCT column. (QID:{self.query_id})"
                ) from err

            from opteryx.exceptions import DataError

            raise DataError(
                f"Unable to build result dataset ({err}) (QID:{self.query_id})"
            ) from err

    def _get_plan_dict(self) -> Optional[dict]:
        """
        Generate the plan dictionary representation.

        Returns:
            A dictionary with nodes and edges representing the query plan, or None if no plan exists.
        """

        # build a JSON representation
        def _humanize_physical_type(class_name: str) -> str:
            # Remove common suffix
            if class_name.endswith("Node"):
                class_name = class_name[: -len("Node")]
            # Split CamelCase into words
            parts = _CAMEL_SPLIT_RE.findall(class_name)
            # Normalize last token 'Read' -> 'reader'
            if parts and parts[-1].lower() == "read":
                parts[-1] = "reader"
            return " ".join(p.lower() for p in parts)

        nodes = []
        for nid, node in self._plan.nodes(data=True):
            # friendly/logical type: prefer Substrait-like names for common kinds
            def _logical_rel_name(node):
                try:
                    if getattr(node, "is_scan", False):
                        return "ReadRel"
                    if getattr(node, "is_join", False):
                        return "JoinRel"
                    # fall back to name-based heuristics
                    candidate = getattr(node, "name", None) or getattr(node, "node_type", None)
                    if candidate is None:
                        return None
                    s = str(candidate).lower()
                    if "aggregate" in s or "group" in s or "distinct" in s:
                        return "AggregateRel"
                    if "project" in s or "projection" in s:
                        return "ProjectRel"
                    if "filter" in s or "where" in s:
                        return "FilterRel"
                    if "limit" in s:
                        return "LimitRel"
                    if "sort" in s or "order" in s:
                        return "SortRel"
                    if "union" in s:
                        return "UnionRel"
                    if "exit" in s:
                        return "ExitRel"
                    # default: title-case the candidate and append Rel
                    token = str(candidate)
                    token = token.replace(" ", "_").replace("-", "_")
                    token = token[0].upper() + token[1:] if token else token
                    return f"{token}Rel"
                except Exception:
                    return None

            logical_type = _logical_rel_name(node)

            # physical implementation type (class name -> human readable)
            try:
                class_name = node.__class__.__name__
                physical_type = _humanize_physical_type(class_name)
            except Exception:
                physical_type = str(getattr(node, "__class__", type(node)))

            # config / plan_config
            try:
                config_val = (
                    node.plan_config()
                    if hasattr(node, "plan_config")
                    else getattr(node, "config", None)
                )
            except Exception as err:
                # Don't silently drop errors from plan_config — include them in the output
                try:
                    cfg_str = getattr(node, "config", None)
                except Exception:
                    cfg_str = None
                config_val = {"_plan_error": str(err), "config": cfg_str}

            node_entry = {
                "rel_id": nid,
                "type": logical_type,
                "physical_type": physical_type,
                "config": config_val,
            }
            nodes.append(node_entry)

        edges = [{"source": s, "target": t, "relation": r} for s, t, r in self._plan.edges()]

        return {
            "nodes": nodes,
            "edges": edges,
            "exit_points": list(self._plan.get_exit_points()),
        }

    @property
    def telemetry(self) -> Dict[str, Any]:
        """Gets the execution telemetry as a dictionary."""
        if self._telemetry.end_time == 0:  # pragma: no cover
            self._telemetry.end_time = time.time_ns()

        # Include mermaid diagram of the plan if available
        if self._plan is not None:
            self._telemetry.plan = self.mermaid()

        return self._telemetry.as_dict()

    def mermaid(self) -> str:
        """Render the current plan as a mermaid diagram string."""
        from opteryx.utils import mermaid

        return mermaid.plan_to_mermaid(self._plan)

    def __repr__(self):  # pragma: no cover - helpful for debugging
        return f"<opteryx.Session (QID:{self.query_id})>"

    def __bool__(self):
        """
        Truthy if executed, Falsy if not executed or error
        """
        return self._executed and not self._closed

    def _ensure_open(self):
        if self._closed:
            raise InvalidCursorStateError("Session is closed.")

    @staticmethod
    def _schema_to_description(schema: Optional[RelationSchema]):
        if schema is None or not schema.columns:
            return None
        description: List[Tuple[Any, ...]] = []
        for column in schema.columns:
            description.append(
                (
                    column.name,
                    column.type,
                    None,
                    None,
                    None,
                    None,
                    getattr(column, "nullable", None),
                )
            )
        return tuple(description)

    def execute_to_arrow_batches(
        self,
        operation: str,
        params: Optional[Iterable] = None,
        batch_size: int = 1024,
        limit: Optional[int] = None,
        visibility_filters: Optional[Dict[str, Any]] = None,
    ):
        """Execute a SQL operation and stream pyarrow.RecordBatch objects.

        Yields RecordBatch objects; keeps the session alive for the iterator lifetime.
        """
        self._ensure_open()
        start = time.time_ns()
        results = self._execute_statements(operation, params, visibility_filters)
        if results is None:
            self._telemetry.time_executing += time.time_ns() - start
            return
        result_data, self._result_type = results

        # Handle non-tabular results
        if self._result_type == ResultType.NON_TABULAR:
            meta_dataframe = DataFrame(
                rows=[(result_data.record_count,)],  # type: ignore
                schema=RelationSchema(
                    name="table",
                    columns=[FlatColumn(name="rows_affected", type=OrsoTypes.INTEGER)],
                ),
            )  # type: ignore
            table = meta_dataframe.arrow()
            self._executed = True
            self._schema = meta_dataframe._schema
            self._description = self._schema_to_description(self._schema)
            self._query_status = QueryStatus.SQL_SUCCESS
            for batch in table.to_batches(max_chunksize=batch_size):
                yield batch
            elapsed = time.time_ns() - start
            self._telemetry.time_executing += elapsed - self._telemetry.time_planning
            return

        # Single table case
        if isinstance(result_data, pyarrow.Table):
            table = result_data
            if limit is not None:
                table = table.slice(offset=0, length=limit)
            self._executed = True
            schema = table.schema
            self._schema = RelationSchema(
                name="table",
                columns=[FlatColumn.from_arrow(field) for field in schema],
            )
            self._description = self._schema_to_description(self._schema)
            self._query_status = QueryStatus.SQL_SUCCESS
            for batch in table.to_batches(max_chunksize=batch_size):
                yield batch
            elapsed = time.time_ns() - start
            self._telemetry.time_executing += elapsed - self._telemetry.time_planning
            return

        # Handle Draken morsels or iterables of morsels/tables
        from opteryx.compiled.draken.morsels.morsel import Morsel

        items = result_data
        last_item = None
        buffer_batches = []
        buffered_rows = 0

        def _consume_buffered_rows(target_rows: int):
            nonlocal buffer_batches
            nonlocal buffered_rows
            rows_to_consume = target_rows
            slices = []
            while rows_to_consume > 0 and buffer_batches:
                b = buffer_batches[0]
                if b.num_rows <= rows_to_consume:
                    slices.append(b)
                    rows_to_consume -= b.num_rows
                    buffer_batches.pop(0)
                else:
                    slices.append(b.slice(offset=0, length=rows_to_consume))
                    buffer_batches[0] = b.slice(
                        offset=rows_to_consume, length=b.num_rows - rows_to_consume
                    )
                    rows_to_consume = 0

            if not slices:
                return None

            column_names = slices[0].schema.names
            if len(column_names) != len(set(column_names)):
                temporary_names = [f"col_{i}" for i in range(len(column_names))]
                from itertools import chain

                first_table = slices[0].to_table().rename_columns(temporary_names)
                combined = pyarrow.concat_tables(
                    chain(
                        [first_table],
                        (b.to_table().rename_columns(temporary_names) for b in slices[1:]),
                    ),
                    promote_options="permissive",
                )
                combined = combined.rename_columns(column_names)
            else:
                combined = pyarrow.Table.from_batches(slices).combine_chunks()
            batches = combined.to_batches(max_chunksize=target_rows)
            batch = batches[0] if batches else None
            buffered_rows = sum(b.num_rows for b in buffer_batches)
            return batch

        row_count = 0
        for item in items:
            if item is None or item is EOS:
                continue

            last_item = item

            # Convert Draken morsel to Arrow table
            if isinstance(item, Morsel):
                arrow_table = item.to_arrow()
            else:
                # Already an Arrow table or batch
                arrow_table = item if isinstance(item, pyarrow.Table) else item.to_table()

            # Initialize schema from first morsel/table
            if not getattr(self._schema, "columns", None):
                self._schema = RelationSchema(
                    name="table",
                    columns=[FlatColumn.from_arrow(field) for field in arrow_table.schema],
                )
                self._description = self._schema_to_description(self._schema)
                self._query_status = QueryStatus.SQL_SUCCESS

            # Convert Arrow table to batches and buffer
            for batch in arrow_table.to_batches(max_chunksize=batch_size):
                buffer_batches.append(batch)
                buffered_rows += batch.num_rows
                row_count += batch.num_rows

                if limit is not None and row_count >= limit:
                    # Yield remaining buffered data up to limit
                    batch = _consume_buffered_rows(limit - (row_count - batch.num_rows))
                    if batch is not None:
                        self._executed = True
                        yield batch
                    elapsed = time.time_ns() - start
                    self._telemetry.time_executing += elapsed - self._telemetry.time_planning
                    return

                while buffered_rows >= batch_size:
                    batch = _consume_buffered_rows(batch_size)
                    if batch is not None:
                        self._executed = True
                        yield batch
                    else:
                        break

        if buffered_rows > 0:
            combined = pyarrow.Table.from_batches(buffer_batches).combine_chunks()
            for batch in combined.to_batches(max_chunksize=batch_size):
                self._executed = True
                yield batch
        else:
            if last_item is not None and not self._executed:
                if isinstance(last_item, Morsel):
                    schema = last_item.to_arrow().schema
                else:
                    schema = last_item.schema
                self._schema = RelationSchema(
                    name="table",
                    columns=[FlatColumn.from_arrow(field) for field in schema],
                )
                self._description = self._schema_to_description(self._schema)
                self._query_status = QueryStatus.SQL_SUCCESS

        if last_item is not None:
            self._executed = True

        elapsed = time.time_ns() - start
        self._telemetry.time_executing += elapsed - self._telemetry.time_planning
        return

    def execute_to_morsels(
        self,
        operation: str,
        params: Optional[Iterable] = None,
        max_size: int = 10_000,
        visibility_filters: Optional[Dict[str, Any]] = None,
    ):
        """Execute a SQL operation and stream Draken Morsels.

        This method merges adjacent morsels and splits large morsels such that each
        yielded morsel contains at most ``max_size`` rows.

        This is a *Draken-native* API: it avoids converting morsels to Arrow (or
        any other intermediate format) except when absolutely required.
        """
        from opteryx.compiled.draken.morsels.morsel import Morsel

        _DRAKEN_TO_ORSO = {
            1: OrsoTypes.INTEGER,  # INT8
            2: OrsoTypes.INTEGER,  # INT16
            3: OrsoTypes.INTEGER,  # INT32
            4: OrsoTypes.INTEGER,  # INT64
            20: OrsoTypes.DOUBLE,  # FLOAT32
            21: OrsoTypes.DOUBLE,  # FLOAT64
            30: OrsoTypes.DATE,  # DATE32
            40: OrsoTypes.TIMESTAMP,  # TIMESTAMP64
            43: OrsoTypes.INTERVAL,  # INTERVAL
            50: OrsoTypes.BOOLEAN,  # BOOL
            60: OrsoTypes.VARCHAR,  # STRING
            61: OrsoTypes.VARCHAR,  # DICTIONARY
            80: OrsoTypes.ARRAY,  # ARRAY
        }

        def _schema_from_morsel(morsel: Morsel):
            columns = []
            for name, dtype in zip(morsel.column_names, morsel.column_types):
                dtype_int = int(dtype)
                orso_type = _DRAKEN_TO_ORSO.get(dtype_int, OrsoTypes.VARCHAR)
                col_name = name.decode("utf-8") if isinstance(name, bytes) else name
                columns.append(FlatColumn(name=col_name, type=orso_type))
            return RelationSchema(name="table", columns=columns)

        self._ensure_open()
        start = time.time_ns()
        results = self._execute_statements(operation, params, visibility_filters)
        if results is None:
            self._telemetry.time_executing += time.time_ns() - start
            return
        result_data, self._result_type = results

        def _yield_morsel(morsel: Morsel):
            if not getattr(self._schema, "columns", None):
                self._schema = _schema_from_morsel(morsel)
                self._description = self._schema_to_description(self._schema)
                self._query_status = QueryStatus.SQL_SUCCESS
            yield morsel

        def _flush_buffer(buffered):
            if not buffered:
                return
            if len(buffered) == 1:
                yield from _yield_morsel(buffered[0])
            else:
                yield from _yield_morsel(Morsel.combine(buffered))

        def _split_morsel(morsel: Morsel):
            # Split a large morsel into <= max_size pieces using slice().
            offset = 0
            total = morsel.num_rows
            while offset < total:
                chunk = morsel.slice(offset, min(max_size, total - offset))
                yield chunk
                offset += chunk.num_rows

        pending = []
        pending_rows = 0

        for item in result_data if hasattr(result_data, "__iter__") else [result_data]:
            if item is None or item is EOS:
                continue

            if isinstance(item, Morsel):
                morsels = [item]
            elif isinstance(item, pyarrow.Table):
                # Fallback for non-Draken sources: convert to morsels
                morsels = list(Morsel.from_arrow(item).slice(0, item.num_rows) for _ in [None])
            else:
                # Assume Arrow batch-like
                morsels = [Morsel.from_arrow(item.to_table())]

            for morsel in morsels:
                for chunk in _split_morsel(morsel):
                    if chunk.num_rows == 0:
                        continue
                    if pending_rows + chunk.num_rows <= max_size:
                        pending.append(chunk)
                        pending_rows += chunk.num_rows
                    else:
                        # Fill up the current buffer, flush, then start new buffer
                        if pending:
                            yield from _flush_buffer(pending)
                            pending = []
                            pending_rows = 0
                        # If chunk itself is larger than max_size, split it further
                        if chunk.num_rows > max_size:
                            for sub in _split_morsel(chunk):
                                yield from _flush_buffer([sub])
                        else:
                            pending.append(chunk)
                            pending_rows = chunk.num_rows

        if pending:
            yield from _flush_buffer(pending)

        self._executed = True
        elapsed = time.time_ns() - start
        self._telemetry.time_executing += elapsed - self._telemetry.time_planning

    @property
    def messages(self) -> List[str]:
        return self._telemetry.messages

    # ------------------------------------------------------------------
    def trace(self) -> Iterator[dict]:
        """Yield trace events for this session.

        The method will flush any pending events to disk before reading the
        trace file.  It filters on ``session_id`` so you only see events
        emitted by this session.  If tracing was never enabled for the
        session a ``RuntimeError`` is raised.
        """
        if not self._tracing_enabled:
            raise RuntimeError("IO tracing not enabled for this session")

        from opteryx.tracing import event_recorder, flush_all

        # flush any pending in‑memory events so they appear in the result
        _ = flush_all()

        # simply iterate the global buffer; file support has been removed.
        with event_recorder._global_lock:
            for ev in event_recorder._global_events:
                if ev.get("session_id") == self._query_id:
                    yield ev

    def close(self):
        if self._closed:
            return
        self._cursor = iter(())
        self._description = None
        # best effort close of child cursors
        try:
            self._close_all_cursors()
        except Exception:
            pass

        # Flush any pending trace events and emit end marker
        if self._tracing_enabled:
            try:
                from opteryx.tracing import event_recorder, record_event

                record_event("trace_session_end", session_id=self._query_id)
                event_recorder.flush_all()
            except Exception:
                pass  # Don't let tracing errors affect query close
            finally:
                # clear the global session id if it's still pointing at us
                from opteryx.tracing import event_recorder as _er

                if _er._current_session_id == self._query_id:
                    _er._current_session_id = None

        # _old_trace_state is no longer used; there is nothing to restore.

        self._closed = True
