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

import time
from typing import Any
from typing import Dict
from typing import Iterable
from typing import List
from typing import Optional
from typing import Tuple
from typing import Union
from uuid import uuid4

import pyarrow
from orso import DataFrame
from orso import converters
from orso.schema import FlatColumn
from orso.schema import RelationSchema
from orso.types import OrsoTypes

from opteryx import config
from opteryx import utils
from opteryx.constants import QueryStatus
from opteryx.constants import ResultType
from opteryx.exceptions import InconsistentSchemaError
from opteryx.exceptions import InvalidCursorStateError
from opteryx.exceptions import MissingSqlStatement
from opteryx.exceptions import ProgrammingError
from opteryx.exceptions import SqlError
from opteryx.exceptions import UnsupportedSyntaxError
from opteryx.managers.billing import BillingEventType
from opteryx.managers.billing import write_billing_event
from opteryx.models import ExecutionContext
from opteryx.models import QueryTelemetry
from opteryx.utils import sql


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
        try:
            self._plan = query_planner(
                operation=operation,
                parameters=params,
                visibility_filters=visibility_filters,
                execution_context=self.context,
                query_id=self.query_id,
                telemetry=self._telemetry,
            )
        except RuntimeError as err:  # pragma: no cover
            raise SqlError(f"Error Executing SQL Statement ({err}) (QID:{self.id})") from err
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
                "bytes_processed": self._telemetry.bytes_processed,
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
        start = time.time_ns()
        results = self._execute_statements(operation, params, visibility_filters)
        if results is not None:
            result_data, self._result_type = results
            if self._result_type == ResultType.NON_TABULAR:
                import orso

                meta_dataframe = orso.DataFrame(
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
                self._rows, self._schema = converters.from_arrow(result_data)
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
        Executes the SQL operation, bypassing conversion to Orso and returning directly in Arrow format.
        """
        self._ensure_open()
        results = self._execute_statements(operation, params, visibility_filters)
        if results is not None:
            result_data, self._result_type = results

            if self._result_type == ResultType.NON_TABULAR:
                import orso

                meta_dataframe = orso.DataFrame(
                    rows=[(result_data.record_count,)],  # type: ignore
                    schema=RelationSchema(
                        name="table",
                        columns=[FlatColumn(name="rows_affected", type=OrsoTypes.INTEGER)],
                    ),
                )  # type: ignore
                self._executed = True
                return meta_dataframe.arrow()

            if limit is not None:
                result_data = utils.arrow.limit_records(result_data, limit)  # type: ignore

        if isinstance(result_data, pyarrow.Table):
            self._executed = True
            return result_data
        try:
            # arrow allows duplicate column names, but not when concatting
            from itertools import chain

            first_table = next(result_data, None)
            if first_table is not None:
                column_names = first_table.column_names
                if len(column_names) != len(set(column_names)):
                    temporary_names = [f"col_{i}" for i in range(len(column_names))]
                    first_table = first_table.rename_columns(temporary_names)
                    return_table = pyarrow.concat_tables(
                        chain(
                            [first_table], (t.rename_columns(temporary_names) for t in result_data)
                        ),
                        promote_options="permissive",
                    )
                    return return_table.rename_columns(column_names)
            table = pyarrow.concat_tables(
                chain([first_table], result_data), promote_options="permissive"
            )
            self._executed = True
            return table
        except (
            pyarrow.ArrowInvalid,
            pyarrow.ArrowTypeError,
        ) as err:  # pragma: no cover
            # DEBUG: print(err)
            if "struct" in str(err):
                raise InconsistentSchemaError(
                    f"Unable to resolve different schemas, most likely related to a STRUCT column. (QID:{self.id})"
                ) from err

            from opteryx.exceptions import DataError

            raise DataError(f"Unable to build result dataset ({err}) (QID:{self.id})") from err

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
            import re

            parts = re.findall(r"[A-Z][a-z]*|[0-9]+", class_name)
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
            import orso

            meta_dataframe = orso.DataFrame(
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

        # Iterator/generator of tables
        morsels = result_data
        if limit is not None:
            morsels = utils.arrow.limit_records(morsels, limit)

        last_morsel = None
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
                combined = combined.combine_chunks()
            else:
                combined = pyarrow.Table.from_batches(slices).combine_chunks()
            batches = combined.to_batches(max_chunksize=target_rows)
            batch = batches[0] if batches else None
            buffered_rows = sum(b.num_rows for b in buffer_batches)
            return batch

        for morsel in morsels:
            last_morsel = morsel
            if morsel is None:
                continue
            if not getattr(self._schema, "columns", None):
                self._schema = RelationSchema(
                    name="table",
                    columns=[FlatColumn.from_arrow(field) for field in morsel.schema],
                )
                self._description = self._schema_to_description(self._schema)
                self._query_status = QueryStatus.SQL_SUCCESS

            for morsel_batch in morsel.to_batches(max_chunksize=batch_size):
                buffer_batches.append(morsel_batch)
                buffered_rows += morsel_batch.num_rows
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
            if last_morsel is not None and not self._executed:
                self._schema = RelationSchema(
                    name="table",
                    columns=[FlatColumn.from_arrow(field) for field in last_morsel.schema],
                )
                self._description = self._schema_to_description(self._schema)
                self._query_status = QueryStatus.SQL_SUCCESS

        if last_morsel is not None:
            self._executed = True

        elapsed = time.time_ns() - start
        self._telemetry.time_executing += elapsed - self._telemetry.time_planning

    @property
    def messages(self) -> List[str]:
        return self._telemetry.messages

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
        self._closed = True
