# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""The Session - the object a query is executed on.

Session replaced the DBAPI Connection and Cursor pair, which no longer exist:
it owns the `ExecutionContext` the Connection used to hold (planners read it as
`connection.context`), and it is what a statement is executed on. Build one with
`opteryx.session()` rather than by importing this module.

`execute_to_morsels()` is the execution surface. It streams Draken morsels as the
engine produces them - there is no materialized result object, and no fetch*()
methods, so a caller which needs the whole result assembles it (see
`tests/helpers.py`, or `opteryx/__main__.py` for the command line's version).

What the session reports about a result - `rowcount`, `column_names`,
`description`, `query_status` - describes the last statement executed on it, and
`rowcount` is only knowable once that statement's stream has been read to the end.
"""

import logging
import re
import time
from typing import TYPE_CHECKING, Any, Dict, Iterable, Iterator, List, Optional, Tuple, Union

if TYPE_CHECKING:  # pragma: no cover - the planner is imported lazily at call time
    from opteryx.planner.query_check import QueryCheck

from opteryx.utils import random_string

logger = logging.getLogger(__name__)

from opteryx import EOS, config, utils
from opteryx.constants import QueryStatus, ResultType
from opteryx.exceptions import (
    ExecutionError,
    InconsistentSchemaError,
    InvalidCursorStateError,
    MissingSqlStatement,
    ProgrammingError,
    ResultTooLargeError,
    SqlError,
    UnsupportedSyntaxError,
    compose,
    md_cause,
    md_code,
)
from opteryx.managers.billing import DEFAULT_BILLING_ACCOUNT, BillingEventType, write_billing_event
from opteryx.variables import resolve as _resolve_var
from opteryx.models import ExecutionContext, QueryTelemetry, TraceBundle
from opteryx.models.dataframe import DataFrame
from opteryx.types.logical_type import LogicalCategory
from opteryx.types.schema import SchemaColumn, RelationSchema
from opteryx.utils import sql

_CAMEL_SPLIT_RE = re.compile(r"[A-Z][a-z]*|[0-9]+")


class Session(DataFrame):
    """Session is the canonical execution object, and replaces Connection+Cursor.

    It subclasses `DataFrame` for the result metadata (schema, description,
    column names) and sets up the `ExecutionContext` that planners expect on
    `connection.context`.
    """

    def __init__(
        self,
        *,
        user: Optional[str] = None,
        memberships: Optional[Iterable[str]] = None,
        entitlements: Optional[Iterable[str]] = None,
        schema: Optional[str] = None,
        access_policies: Optional[Iterable[dict]] = None,
        billing_account: Optional[str] = None,
        workspace: Optional[str] = None,
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
            raise ProgrammingError("Session memberships must all be strings.")
        if entitlements and not all(isinstance(v, str) for v in entitlements):
            raise ProgrammingError("Session entitlements must all be strings.")
        if user and not isinstance(user, str):
            raise ProgrammingError("A Session user must be a string.")
        if access_policies and not all(isinstance(v, dict) for v in access_policies):
            raise ProgrammingError("Session access_policies must all be dictionaries.")
        if billing_account and not isinstance(billing_account, str):
            raise ProgrammingError("A Session billing_account must be a string.")
        if workspace and not isinstance(workspace, str):
            raise ProgrammingError("A Session workspace must be a string.")
        if memberships is None:
            # `public` — the group every caller is in by virtue of being a caller.
            # NOT a product/tenant name: a caller who supplied no memberships holds
            # only the universal one, and the default must not read as membership of
            # anything more specific than that.
            memberships = ["public"]
        if access_policies is None:
            access_policies = [{"pattern": "*", "role": "owner"}]
        if not billing_account:
            billing_account = DEFAULT_BILLING_ACCOUNT

        # Provide execution context expected by planner & execution code
        self.context = ExecutionContext(
            query_id=query_id,
            user=user,
            access_policies=access_policies,
            schema=schema,
            memberships=memberships,
            # NOT defaulted like `memberships` above: an unsupplied entitlement list
            # means "holds none", never a house default.
            entitlements=entitlements,
            billing_account=billing_account,
            # NOT defaulted: an unsupplied workspace means "this execution is not
            # on behalf of one workspace", which is the normal case for caller
            # SQL. Defaulting it would assert a single target where there is none.
            workspace=workspace,
        )

        # Initialize cursor-like state (merged from previous Cursor implementation)
        self.arraysize = 1
        self._query_planner = None
        self._collected_stats = None
        self._plan = None
        self._query_id = query_id if query_id is not None else random_string(32)
        self._telemetry = QueryTelemetry(self._query_id)
        self._trace = TraceBundle()
        # Set fresh per STATEMENT in _inner_execute (not once per _execute_statements()
        # batch) — the `trace` session variable (opteryx/variables.py) can be changed
        # by a `SET trace TO ...` earlier in the same semicolon-separated batch, so
        # this must not be a __init__-time or once-per-batch snapshot. False here is
        # just the pre-first-statement default.
        self._trace_armed = False
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
        source: Optional[str] = None,
        source_offset: int = 0,
    ) -> Any:
        from opteryx.managers.execution import execute
        from opteryx.planner import query_planner

        if not operation:  # pragma: no cover
            raise MissingSqlStatement("SQL provided was empty. Provide a statement to run.")

        start = time.time_ns()
        try:
            self._plan = query_planner(
                operation=operation,
                parameters=params,
                visibility_filters=visibility_filters,
                execution_context=self.context,
                query_id=self.query_id,
                telemetry=self._telemetry,
                source=source,
                source_offset=source_offset,
            )
        except RuntimeError as err:  # pragma: no cover
            # ExecutionError, not SqlError: the planner failed, which is not the same
            # as the statement being wrong, and SqlError told the reader their SQL was
            # at fault. The query id goes in its own clause rather than glued into the
            # sentence in brackets - it is the thing to quote when reporting this.
            raise ExecutionError(
                compose(
                    "The query could not be planned",
                    md_cause(err),
                    f"Quote {md_code(self.query_id)} if you report this",
                )
            ) from err
        finally:
            self._telemetry.time_planning += time.time_ns() - start

        # Read fresh per statement (not once per _execute_statements() batch): a
        # `SET trace TO true` earlier in the same semicolon-separated batch must
        # be visible to this statement's execute() call, not just to the next
        # batch — see docs/EXECUTION_TRACING_DESIGN.md.
        self._trace.reset()
        self._trace_armed = bool(self.context.variables["trace"])
        results = execute(
            self._plan,
            telemetry=self._telemetry,
            trace_sink=self._trace if self._trace_armed else None,
        )

        write_billing_event(
            billing_event=BillingEventType.QUERY_EXECUTION,
            billing_account=self.context.billing_account,
            # The identity this session runs as. Not the same question as
            # `billing_account`, which is who pays for what it does - a
            # materialized view refresh acts as its owner and bills the target
            # workspace's account.
            actor=self.context.user,
            workspace=self.context.workspace,
            event_details={
                "user": self.context.user,
                "query_id": self.query_id,
                "query": operation,
            },
        )

        return results

    def _emit_processed_bytes_billing(self, operation: str) -> None:
        """Emit the DATA_PROCESSED_BYTES billing event.

        Must be called *after* execution has completed and the results have
        been fully consumed: ``bytes_processed`` is accumulated by the scan
        operators as morsels flow, so it is only complete once the result
        stream has been drained. Emitting earlier (e.g. right after the lazy
        ``execute()`` call returns) always reports zero bytes.
        """
        write_billing_event(
            billing_event=BillingEventType.DATA_PROCESSED_BYTES,
            billing_account=self.context.billing_account,
            actor=self.context.user,
            workspace=self.context.workspace,
            event_details={
                "user": self.context.user,
                "query_id": self.query_id,
                "query": operation,
                "bytes_processed": self._telemetry.bytes_processed,
            },
        )

    def _execute_statements(
        self,
        operation: str,
        params: Optional[Iterable] = None,
        visibility_filters: Optional[Dict[str, Any]] = None,
    ):
        self._telemetry.reset()
        self._telemetry.start_time = time.time_ns()

        if getattr(operation, "decode", None) is not None:
            operation = operation.decode()

        # The statement is handed on exactly as it was written - comments, line breaks
        # and all. Everything downstream that reports a position (the parse-error
        # formatter, the binder pointing at a column) quotes it back at the reader, so
        # normalising it here would be normalising away the thing being pointed at.
        statements = sql.split_sql_statements(operation)

        if len(statements) == 0:
            raise MissingSqlStatement("No statement found. Provide a statement to run.")

        if len(statements) > 1 and params is not None and not isinstance(params, dict) and params:
            raise UnsupportedSyntaxError(
                "Batched queries cannot be parameterized with parameter lists, use named parameters."
            )

        results = None
        for index, statement in enumerate(statements):
            results = self._inner_execute(
                statement.text,
                params,
                visibility_filters,
                source=operation,
                source_offset=statement.offset,
            )
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

    def check(
        self,
        operation: str,
        params: Optional[Iterable] = None,
        visibility_filters: Optional[Dict[str, Any]] = None,
        catalog_cache=None,
    ) -> "QueryCheck":
        """Check a statement without running it, for an editor to draw on.

        Plans as far as the end of binding - so every name is resolved and every type
        settled - and returns what that found: a positioned error to underline, the
        result shape, and the relations and columns in scope for completion. Reads the
        catalog and nothing else. Cheap enough to call as the statement is typed.

        It is a superset of `opteryx.analyze_query`: everything that reports about the
        statement itself - type, relations named, parameters needed, permission tier -
        comes back too, from the same single parse, and survives a failed bind.

        The check runs AS THIS SESSION'S USER. Permission gates are evaluated live on
        every call, so completion offers only relations this user may read, and a
        statement they may not run reports that rather than checking out clean.

        Errors are returned, not raised: a statement being wrong is the expected case
        while it is being written. Only errors ABOUT THE STATEMENT come back that way
        (a `SqlError`, or the permission refusal); a catalog that is unreachable still
        raises, because that is not something the reader can fix by typing.

        Parameters:
            operation: one SQL statement. Not a batch - a batch's later statements can
                depend on what its earlier ones create, and this runs none of them.
            params: parameters, if the statement is parameterized. Binding needs them:
                an unsupplied `:name` is itself a reportable error.
            visibility_filters: row-level filters, as `execute` takes them.
            catalog_cache: an optional `opteryx.CatalogCache`. Holds each relation's
                catalog lookup for its TTL so a burst of keystrokes costs one round
                trip per relation rather than one per call. Entries can be up to that
                stale; see CatalogCache for why that is fine here and nowhere else.

        Returns:
            QueryCheck

        Example:
            session = opteryx.session(user="alice", memberships=["finance"])
            cache = opteryx.CatalogCache(ttl=60)
            result = session.check("SELECT nam FROM $planets", catalog_cache=cache)
            result.ok             # False
            result.position       # SourcePosition(1, 8, 1, 11, 7, 10)
            result.query_type     # 'Query' - known even though it did not bind
        """
        self._ensure_open()

        from opteryx.planner.query_check import check_statement

        if not operation:
            raise MissingSqlStatement("SQL provided was empty. Provide a statement to check.")

        # Its own timing window, for the same reason plan() takes one: the readings
        # are per-operation, and a check must not report the previous statement's.
        self._telemetry.reset()
        start = time.time_ns()
        self._telemetry.start_time = start
        try:
            return check_statement(
                operation=operation,
                execution_context=self.context,
                query_id=self.query_id,
                telemetry=self._telemetry,
                parameters=params,
                visibility_filters=visibility_filters,
                catalog_cache=catalog_cache,
            )
        finally:
            self._telemetry.time_planning += time.time_ns() - start
            self._telemetry.end_time = time.time_ns()

    def plan(
        self,
        operation: str,
        params: Optional[Iterable] = None,
        visibility_filters: Optional[Dict[str, Any]] = None,
    ) -> dict:
        self._ensure_open()

        from opteryx.planner import query_planner

        # Planning a statement is a whole operation, so it opens and closes its own
        # timing window exactly as _execute_statements() does, and resets first for
        # the same reason: the readings are per-operation and a plan() following an
        # execute() on this session would otherwise report that execute's timings as
        # part of its own. The window closes below rather than here, so `time_total`
        # covers the whole call and `time_planning` remains the planner's slice of it.
        self._telemetry.reset()
        start = time.time_ns()
        self._telemetry.start_time = start
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

        self._telemetry.end_time = time.time_ns()
        return plan_dict

    @property
    def result_type(self) -> ResultType:
        return self._result_type

    @property
    def query_status(self) -> QueryStatus:
        return self._query_status

    @property
    def rowcount(self) -> int:
        """
        The number of rows the last statement produced.

        A tabular result only has a row count once it has been read to the end -
        the engine streams, so until the last morsel has been handed over the
        number does not exist to report. Asking early is an error rather than a
        count of what has been delivered so far, which reads like a total and
        isn't one.
        """
        if self._result_type == ResultType.TABULAR:
            if not self._executed:
                raise InvalidCursorStateError(
                    "Row count is not known until the result has been read - "
                    "consume execute_to_morsels() before asking for it."
                )
            return self._rowcount
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
                    if getattr(node, "plan_config", None) is not None
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
        # Closing the timing window is this property's job, and it is what makes it
        # the only sanctioned way to read telemetry: `as_dict()` refuses to report a
        # `time_total` for a window still open. The engine streams, so there is no
        # earlier point that knows the query is over - it is over when someone asks
        # for the readings.
        if self._telemetry.end_time == 0:
            self._telemetry.end_time = time.time_ns()

        # Populate per-node/edge telemetry from the plan (operations + edges).
        # This is the definitive structured record; no mermaid string is built
        # or stored here — EXPLAIN renders the diagram separately, on demand.
        if self._plan is not None:
            from opteryx.utils import mermaid

            mermaid.collect_plan_telemetry(self._plan)

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
        yielded morsel contains at most ``max_size`` rows. The 65,536 default is the
        empirical sweet spot on ARM: join build cost plateaus there and hash /
        group-by are flat across chunk sizes. (This is the *output*-boundary row
        target; execution-internal morsels are still row-group sized.)

        This is a *Draken-native* API: it avoids converting morsels to Arrow (or
        any other intermediate format) except when absolutely required.

        Planning and execution are lazy - nothing runs until the returned generator
        is iterated. What this call does DO immediately is check the session is open
        and clear what the last statement left behind, so a session holding a
        generator it has not started cannot answer questions about a result which
        does not exist yet with the previous statement's numbers.
        """
        self._ensure_open()
        # A statement supersedes the one before it the moment it is submitted, not
        # when its first morsel arrives: `rowcount`, the schema behind
        # `column_names`/`description`, and `query_status` all describe the LAST
        # statement, and until this one produces its own they must describe nothing.
        # The schema in particular is only rebuilt when there isn't one, so without
        # clearing it a reused session reports its FIRST query's columns forever.
        self._executed = False
        self._rowcount = None
        self._schema = RelationSchema(name="table", columns=[])
        self._description = None
        self._query_status = QueryStatus._UNDEFINED
        self._result_type = ResultType._UNDEFINED
        return self._stream_morsels(operation, params, max_size, visibility_filters)

    def _stream_morsels(
        self,
        operation: str,
        params: Optional[Iterable],
        max_size: int,
        visibility_filters: Optional[Dict[str, Any]],
    ):
        """The generator behind `execute_to_morsels` - see it for the contract."""
        from draken.morsels.morsel import Morsel

        from opteryx.types.logical_type import column_type_from_vector

        def _schema_from_morsel(morsel: Morsel):
            from opteryx.types.schema import mint_column_identity
            columns = []
            for name in morsel.column_names:
                col_name = name.decode("utf-8") if isinstance(name, bytes) else name
                # A vector's DrakenType tag is only half of its type — the unit of a
                # TIMESTAMP, the (precision, scale) of a DECIMAL and the IPv4-ness of a
                # UINT32 all live on the descriptor beside it. `column_type_from_vector`
                # is the single reconstructor for that pair; keeping a local
                # tag->type map here is what let the result schema report a bare
                # UINT32 for an address column (and, via its VARCHAR default, a
                # STRING for every unsigned integer column).
                ct = column_type_from_vector(morsel.column(name))
                columns.append(SchemaColumn(name=col_name, column_type=ct, identity=mint_column_identity("table", col_name)))
            return RelationSchema(name="table", columns=columns)

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
            self._emit_processed_bytes_billing(operation)
            return

        # Runtime backstop for `sql_select_limit`. The plan-time guard only fires when
        # every input has real statistics AND the estimate is high enough; this counts
        # what is actually DELIVERED, so it also catches a result the estimate was too
        # low to predict. One add and one compare PER MORSEL (not per row), at the
        # boundary where morsels already cross into Python — no per-row cost.
        row_budget = _resolve_var("sql_select_limit", self.context.variables, 0)
        delivered_rows = 0

        def _yield_morsel(morsel: Morsel):
            nonlocal delivered_rows
            if not getattr(self._schema, "columns", None):
                self._schema = _schema_from_morsel(morsel)
                self._description = self._schema_to_description(self._schema)
                self._query_status = QueryStatus.SQL_SUCCESS
            delivered_rows += morsel.num_rows
            if row_budget and delivered_rows > row_budget:
                # Raise rather than truncate: handing back the first N rows of a
                # larger result is a wrong answer the caller cannot detect.
                raise ResultTooLargeError(rows=delivered_rows, limit=row_budget)
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
        last_empty_morsel = None
        saw_nonzero_rows = False

        for item in result_data if getattr(result_data, "__iter__", None) is not None else [result_data]:
            if item is None or item is EOS:
                continue

            if isinstance(item, Morsel):
                # Cursor is the sole shim: a Cxx-backed morsel from the engine
                # becomes PyObject here, once, at the user boundary.
                item.materialize()
                morsels = [item]

            for morsel in morsels:
                if morsel.num_rows == 0:
                    # The engine's courtesy empty-result morsel (engine.hpp's
                    # `run()`): a query that legitimately returns zero rows still
                    # carries its output schema on one such morsel. Hold onto it
                    # in case the whole result turns out empty -- don't yield it
                    # if real data shows up later in the stream.
                    last_empty_morsel = morsel
                    continue
                saw_nonzero_rows = True
                for chunk in _split_morsel(morsel):
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
        elif not saw_nonzero_rows and last_empty_morsel is not None:
            yield from _yield_morsel(last_empty_morsel)

        # The result has been delivered in full, so the rows counted for the
        # `sql_select_limit` backstop above are also the query's row count. It is
        # only knowable here: the engine streams, so nothing before the last
        # morsel knows how many rows there were.
        self._rowcount = delivered_rows
        self._executed = True
        elapsed = time.time_ns() - start
        self._telemetry.time_executing += elapsed - self._telemetry.time_planning
        self._emit_processed_bytes_billing(operation)

    @property
    def messages(self) -> List[str]:
        return self._telemetry.messages

    # ------------------------------------------------------------------
    @property
    def trace_armed(self) -> bool:
        """Whether tracing was armed for the last statement executed on this
        session — i.e. whether :meth:`trace` will return a bundle rather than
        raise.

        Arming is a per-statement fact read from the `trace` session variable
        (see ``_inner_execute``), which is USER-owned and settable with `SET
        trace TO ...`. ``OPTERYX_TRACE`` only supplies its default, so a caller
        that wants to persist a trace must branch on this, not on the
        environment: the two disagree whenever a query sets the variable, and a
        caller keyed on the env var either calls :meth:`trace` on an unarmed
        query (RuntimeError) or discards a bundle the engine already paid to
        record.
        """
        return self._trace_armed

    def trace(self) -> Tuple[bytes, Dict[int, str], Dict[int, str], str, bool]:
        """Return this query's raw native execution trace: ``(blob,
        node_symbols, file_symbols, host_info, truncated)``.

        ``blob`` is a packed array of fixed-layout span records (see
        ``opteryx.tracing`` / docs/EXECUTION_TRACING_DESIGN.md); ``node_symbols``
        resolves a span's ``node_id`` to its plan-node identity, ``file_symbols``
        resolves a span's ``file_id`` to a file path. ``host_info`` is an
        ``"arch=...;host=..."`` identity of the process that captured this
        trace, so two trace bundles can be compared honestly (e.g. telling a
        genuine perf difference apart from an ARM-vs-x86 difference) without
        out-of-band knowledge of where each one ran. ``truncated`` is True when
        some worker's span arena (``OPTERYX_TRACE_ARENA_SPANS``) filled up
        mid-query — every downstream number (file/row/byte counts, concurrency,
        throughput, …) is then a floor, not a true total, and WHICH spans got
        dropped is a scheduling race, so the undercount varies run to run even
        for an identical query. A caller that ignores this flag will see
        numbers that look plausible but silently aren't the whole picture.
        Deliberately returned raw, not interpreted — a caller that only needs
        to persist the trace (e.g. alongside a query's results) pays no
        per-span Python object cost, and a caller that wants to look at it
        calls ``opteryx.tracing.interpret_trace`` (or ``opteryx.tracing.parse_spans``
        for the unresolved fields).

        Raises ``RuntimeError`` if tracing was not armed for this query — check
        :attr:`trace_armed` first rather than inferring arming from
        ``OPTERYX_TRACE``, which is only the default for a per-query variable.
        """
        if not self._trace_armed:
            raise RuntimeError(
                "Execution tracing not enabled for this query "
                "(SET trace TO true, or set OPTERYX_TRACE=1)"
            )
        return (
            self._trace.blob,
            self._trace.node_symbols,
            self._trace.file_symbols,
            self._trace.host_info,
            self._trace.truncated,
        )

    def close(self):
        if self._closed:
            return
        self._cursor = iter(())
        self._description = None
        self._closed = True
