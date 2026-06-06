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

import logging
import re
import time
from typing import Any, Dict, Iterable, Iterator, List, Optional, Tuple, Union
from uuid import uuid4

logger = logging.getLogger(__name__)

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
from opteryx.types.logical_type import LogicalCategory
from opteryx.types.schema import SchemaColumn, RelationSchema
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
                except (AttributeError, TypeError, KeyError) as err:
                    logger.debug(f"Could not determine logical relation type: {err}")
                    return None
                except Exception as err:
                    logger.warning(f"Unexpected error determining logical relation type: {err}")
                    return None

            logical_type = _logical_rel_name(node)

            # physical implementation type (class name -> human readable)
            try:
                class_name = node.__class__.__name__
                physical_type = _humanize_physical_type(class_name)
            except (AttributeError, TypeError) as err:
                logger.debug(f"Could not determine physical type, falling back to __class__: {err}")
                physical_type = str(getattr(node, "__class__", type(node)))
            except Exception as err:
                logger.warning(f"Unexpected error determining physical type: {err}")
                physical_type = str(getattr(node, "__class__", type(node)))

            # config / plan_config
            # Try primary config source first, fallback to direct config attribute
            try:
                config_val = (
                    node.plan_config()
                    if hasattr(node, "plan_config")
                    else getattr(node, "config", None)
                )
            except (AttributeError, TypeError, ValueError) as err:
                logger.debug(f"plan_config() failed, attempting fallback: {err}")
                # Fallback: try direct config attribute
                try:
                    cfg_str = getattr(node, "config", None)
                except Exception as fallback_err:
                    logger.debug(f"Fallback config extraction also failed: {fallback_err}")
                    cfg_str = None
                config_val = {"_plan_error": str(err), "config": cfg_str}
            except Exception as err:
                logger.warning(f"Unexpected error extracting config: {err}")
                try:
                    cfg_str = getattr(node, "config", None)
                except Exception as fallback_err:
                    logger.debug(f"Fallback config extraction failed: {fallback_err}")
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
                    column.category,
                    None,
                    None,
                    None,
                    None,
                    getattr(column, "nullable", None),
                )
            )
        return tuple(description)

    def execute_to_morsels(
        self,
        operation: str,
        params: Optional[Iterable] = None,
        max_size: int = 65_536,
        visibility_filters: Optional[Dict[str, Any]] = None,
    ):
        """Execute a SQL operation and stream Draken Morsels.

        This method merges adjacent morsels and splits large morsels such that each
        yielded morsel contains at most ``max_size`` rows.

        This is a *Draken-native* API: it avoids converting morsels to Arrow (or
        any other intermediate format) except when absolutely required.
        """
        from draken.morsels.morsel import Morsel

        from opteryx.types import logical_type as _lt
        from draken.draken_native import DrakenType as _DT
        _DRAKEN_TO_LT = {
            _DT.INT8: _lt.INT8, _DT.INT16: _lt.INT16,
            _DT.INT32: _lt.INT32, _DT.INT64: _lt.INT64,
            _DT.FLOAT32: _lt.FLOAT32, _DT.FLOAT64: _lt.FLOAT64,
            _DT.DATE32: _lt.DATE, _DT.TIMESTAMP64: _lt.TIMESTAMP(),
            _DT.INTERVAL: _lt.INTERVAL, _DT.BOOL: _lt.BOOLEAN,
            _DT.VARCHAR: _lt.VARCHAR, _DT.NVARCHAR: _lt.NVARCHAR,
            _DT.VARIANT: _lt.VARIANT, _DT.ARRAY: _lt.ARRAY(_lt.VARIANT),
            _DT.VARBINARY: _lt.VARBINARY, _DT.NULL: _lt.NULL,
        }

        def _schema_from_morsel(morsel: Morsel):
            columns = []
            for name, dtype in zip(morsel.column_names, morsel.column_types):
                col_name = name.decode("utf-8") if isinstance(name, bytes) else name
                ct = _DRAKEN_TO_LT.get(dtype, _lt.VARCHAR)
                columns.append(SchemaColumn.from_column_type(name=col_name, column_type=ct))
            return RelationSchema(name="table", columns=columns)

        self._ensure_open()
        start = time.time_ns()
        results = self._execute_statements(operation, params, visibility_filters)
        if results is None:
            self._telemetry.time_executing += time.time_ns() - start
            return
        result_data, self._result_type = results

        # Handle non-tabular results (DDL statements)
        if self._result_type == ResultType.NON_TABULAR:
            from opteryx.models import NonTabularResult
            if isinstance(result_data, NonTabularResult):
                self._rowcount = result_data.record_count
                self._query_status = result_data.status
            self._executed = True
            elapsed = time.time_ns() - start
            self._telemetry.time_executing += elapsed - self._telemetry.time_planning
            return

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
