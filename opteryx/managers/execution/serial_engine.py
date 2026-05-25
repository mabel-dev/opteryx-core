# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Serial execution engine — streaming push pipeline.

Drives the compiled pipeline by iterating each scan, pushing morsels into
the chain head, and draining the terminal ExitNode's `_pending` queue to
the caller as a single generator. The per-morsel hot path is a typed
Cython vtable call (`chain_head.push(morsel)`) — no generator protocol, no
graph traversal, no Python wrapper.

LIMIT short-circuit: the LIMIT operator calls `ctx.terminate()` when its
quota is reached; the scan loop checks `ctx.is_terminated()` between morsels
and breaks promptly, dropping the scan iterator (which closes the underlying
I/O).

Special operators (Explain, SetVariable, ShowValue, ShowCreate, Insert,
ViewManagement, TableManagement, RelationManagement) do not enter the push
pipeline — they're invoked directly via their `__call__` interface.
"""

from typing import Any, Generator, Tuple

from opteryx import EOS
from draken.morsels.morsel import Morsel
from draken.interop.vector_sequence import vector_from_sequence
from opteryx.constants import ResultType
from opteryx.exceptions import InvalidInternalStateError
from opteryx.managers.execution.pipeline_compiler import compile_pipeline
from opteryx.models import PhysicalPlan, QueryTelemetry
from opteryx.operators._operators import drive_scan
from opteryx.types import OrsoTypes


def execute(
    plan: PhysicalPlan, head_node: str = None, telemetry: QueryTelemetry = None
) -> Tuple[Generator[Morsel, Any, Any], ResultType]:
    from opteryx.operators.explain import ExplainNode
    from opteryx.operators.set_variable import SetVariableNode
    from opteryx.operators.show_create import ShowCreateNode
    from opteryx.operators.show_value import ShowValueNode
    from opteryx.operators.table_management import TableManagementNode
    from opteryx.operators.view_management import ViewManagementNode
    from opteryx.operators.relation_management import RelationManagementNode
    from opteryx.operators.insert import InsertNode

    head_nodes = list(set(plan.get_exit_points()))
    if len(head_nodes) != 1:
        raise InvalidInternalStateError(
            f"Query plan has {len(head_nodes)} heads, expected exactly 1."
        )

    if head_node is None:
        head_node = plan[head_nodes[0]]

    # ── Non-pipeline special cases ───────────────────────────────────────────
    if isinstance(head_node, ExplainNode):
        return (
            explain(plan, analyze=head_node.analyze, _format=head_node.format),
            ResultType.TABULAR,
        )
    if isinstance(head_node, SetVariableNode):
        return head_node(None), ResultType.NON_TABULAR
    if isinstance(head_node, (ViewManagementNode, TableManagementNode, RelationManagementNode)):
        return head_node(None), ResultType.NON_TABULAR
    if isinstance(head_node, InsertNode):
        # Insert IS on the push pipeline (as a sink), but produces a
        # non-tabular result via its `result` attribute. Drive the pipeline
        # to completion, then return the result.
        _drain_pipeline(plan, collect=False)
        if head_node.result is None:
            raise InvalidInternalStateError("InsertNode did not produce a result")
        return head_node.result, ResultType.NON_TABULAR
    if isinstance(head_node, (ShowValueNode, ShowCreateNode)):
        return head_node(None), ResultType.TABULAR

    # ── Push pipeline: streaming generator ───────────────────────────────────
    chains, exit_node, ctx = compile_pipeline(plan)

    def stream():
        if exit_node is None:
            return
        # drive_scan runs the per-morsel hot path inside Cython — typed
        # chain_head.push / exit_node.has_pending / exit_node.pop_pending
        # calls, no Python -> cpdef boundary per morsel. We only re-yield
        # the morsels it produces (one yield per emitted result).
        for scan, chain_head in chains:
            yield from drive_scan(scan, chain_head, exit_node, ctx)
            if ctx.is_terminated():
                return

    return stream(), ResultType.TABULAR


def _drain_pipeline(plan: PhysicalPlan, enable_tracing: bool = False):
    """Drive the pipeline to completion without yielding to the caller.
    Used for INSERT (side effects) and EXPLAIN ANALYZE (telemetry) where
    we discard the result rows."""
    chains, exit_node, ctx = compile_pipeline(plan)
    if enable_tracing:
        # EXPLAIN ANALYZE wants per-operator wall-time. Flip the trace flag
        # on every node so push() emits clock_gettime + execution_time.
        for nid, node in plan.depth_first_search_flat():
            if hasattr(node, "enable_tracing"):
                node.enable_tracing(True)
    for scan, chain_head in chains:
        # Consume drive_scan but discard all yielded morsels.
        for _ in drive_scan(scan, chain_head, exit_node, ctx):
            pass
        if ctx.is_terminated():
            break


def explain(plan: PhysicalPlan, analyze: bool, _format: str) -> Generator[Morsel, None, None]:
    from opteryx.operators import BasePlanNode
    from opteryx.operators.exit import ExitNode
    from opteryx.operators.explain import ExplainNode

    def _inner_explain(node, depth):
        incoming_operators = plan.ingoing_edges(node)
        for operator_name in incoming_operators:
            operator = plan[operator_name[0]]
            if isinstance(operator, (ExitNode, ExplainNode)):
                yield from _inner_explain(operator_name[0], depth)
                continue
            elif isinstance(operator, BasePlanNode):
                record = {
                    "identity": operator.identity,
                    "tree": depth,
                    "operator": operator.name,
                    "config": operator.config,
                }
                if analyze:
                    record["time_ms"] = operator.execution_time / 1e6
                    record["records_in"] = operator.records_in
                    record["records_out"] = operator.records_out
                    record["bytes_in"] = operator.bytes_in
                    record["bytes_out"] = operator.bytes_out
                    record["calls"] = operator.calls
                yield record
                yield from _inner_explain(operator_name[0], depth + 1)

    head = list(dict.fromkeys(plan.get_exit_points()))
    if len(head) != 1:
        raise InvalidInternalStateError(f"Problem with the plan - it has {len(head)} heads.")

    if analyze:
        # Drive the underlying query for telemetry but discard the result rows.
        head_node = plan.get_exit_points()[0]
        query_head_edges = plan.ingoing_edges(head_node)
        if query_head_edges:
            _drain_pipeline(plan, enable_tracing=True)

    explained = list(_inner_explain(head[0], 1))

    if _format == "TEXT":
        table = Morsel.from_vectors(
            ["identity", "bytes_in", "bytes_out"],
            [
                vector_from_sequence(
                    [row["identity"] for row in explained], dtype=OrsoTypes.VARCHAR
                ),
                vector_from_sequence(
                    [row.get("bytes_in", 0) for row in explained], dtype=OrsoTypes.INTEGER
                ),
                vector_from_sequence(
                    [row.get("bytes_out", 0) for row in explained], dtype=OrsoTypes.INTEGER
                ),
            ],
        )
    else:
        from opteryx.utils import mermaid

        mermaid_plan = mermaid.plan_to_mermaid(plan, explained)
        table = Morsel.from_vectors(
            ["plan"], [vector_from_sequence([mermaid_plan], dtype=OrsoTypes.VARCHAR)]
        )

    yield table
