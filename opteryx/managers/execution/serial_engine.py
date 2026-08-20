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

Special operators (Explain, SetVariable, ShowValue, ShowCreate, ShowColumns,
Insert, ViewManagement, TableManagement, RelationManagement) do not enter the
push pipeline — they're invoked directly via their `__call__` interface.
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
from draken.draken_native import DrakenType


def _special_op_types():
    from opteryx.operators.explain import ExplainNode
    from opteryx.operators.set_variable import SetVariableNode
    from opteryx.operators.show_columns import ShowColumnsNode
    from opteryx.operators.show_manifest import ShowManifestNode
    from opteryx.operators.show_snapshots import ShowSnapshotsNode
    from opteryx.operators.show_create import ShowCreateNode
    from opteryx.operators.show_value import ShowValueNode
    from opteryx.operators.table_management import TableManagementNode
    from opteryx.operators.view_management import ViewManagementNode
    from opteryx.operators.relation_management import RelationManagementNode
    from opteryx.operators.insert import InsertNode

    return (
        ExplainNode,
        SetVariableNode,
        ShowColumnsNode,
        ShowManifestNode,
        ShowSnapshotsNode,
        ShowCreateNode,
        ShowValueNode,
        TableManagementNode,
        ViewManagementNode,
        RelationManagementNode,
        InsertNode,
    )


def is_special_op(node) -> bool:
    """True for a non-pipeline special operation (EXPLAIN / INSERT / SET / SHOW /
    DDL). These run on ``serial_engine.execute``; every data pipeline (SELECT and
    friends) runs on the native engine (``compiler.execute_native``). The dispatcher uses
    this to triage — serial_engine is NOT a fallback for data pipelines."""
    return isinstance(node, _special_op_types())


def execute(
    plan: PhysicalPlan, head_node: str = None, telemetry: QueryTelemetry = None
) -> Tuple[Generator[Morsel, Any, Any], ResultType]:
    from opteryx.operators.explain import ExplainNode
    from opteryx.operators.set_variable import SetVariableNode
    from opteryx.operators.show_columns import ShowColumnsNode
    from opteryx.operators.show_manifest import ShowManifestNode
    from opteryx.operators.show_snapshots import ShowSnapshotsNode
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
            explain(plan, analyze=head_node.analyze, _format=head_node.format, telemetry=telemetry),
            ResultType.TABULAR,
        )
    if isinstance(head_node, SetVariableNode):
        return head_node(None), ResultType.NON_TABULAR
    if isinstance(head_node, (ViewManagementNode, TableManagementNode, RelationManagementNode)):
        return head_node(None), ResultType.NON_TABULAR
    if isinstance(head_node, InsertNode):
        # INSERT ... SELECT: plan_insert keeps the sub-query's ExitNode instead
        # of stripping it (see logical_planner.py), so the SELECT subplan is
        # genuinely Exit-headed — run it on the native engine like any other
        # SELECT. INSERT ... VALUES has no SELECT subplan at all (its source is
        # a FunctionDatasetNode, never Exit-headed) and stays on the legacy
        # push-pipeline. Strip InsertNode from a copy of the plan (heal=True
        # re-exposes whatever sits below it as the sole exit point) and check
        # which shape we actually have before picking an engine.
        subplan = plan.copy()
        subplan.remove_node(head_nodes[0], heal=True)
        new_head = subplan[subplan.get_exit_points()[0]]
        if type(new_head).__name__ == "ExitNode":
            # Drive InsertNode's existing, tested write/commit logic directly
            # from the native generator's morsels. This keeps a Python-driven
            # per-morsel loop calling into `_push_impl` — interim debt per
            # CLAUDE.md §2, accepted here rather than building a native write
            # sink.
            from opteryx.managers.execution.compiler import execute_native

            generator, _ = execute_native(subplan, telemetry=telemetry)
            for morsel in generator:
                head_node._push_impl(morsel)
            head_node._push_impl(EOS)
        else:
            _drain_pipeline(plan)
        if head_node.result is None:
            raise InvalidInternalStateError("InsertNode did not produce a result")
        return head_node.result, ResultType.NON_TABULAR
    # SHOW COLUMNS/MANIFEST/SNAPSHOTS are answered entirely from what the binder
    # already attached (binder/view.py's visit_show_columns/visit_show_manifest/
    # visit_show_snapshots) — the Scan below any of them in the plan is never
    # read. No pipeline, no native engine.
    if isinstance(
        head_node,
        (ShowValueNode, ShowCreateNode, ShowColumnsNode, ShowManifestNode, ShowSnapshotsNode),
    ):
        return head_node(None), ResultType.TABULAR

    # serial_engine handles ONLY the special, non-pipeline operations above. A
    # data-pipeline head (Exit — i.e. SELECT and friends) reaching here means the
    # data executor punted to a hidden serial fallback. We refuse to paper over
    # that (CLAUDE.md §1/§9: no fallbacks, no hidden behaviour, fail fast). The
    # native engine (compiler.execute_native) owns ALL data-pipeline execution.
    raise InvalidInternalStateError(
        f"serial_engine received a data-pipeline head ({type(head_node).__name__}); "
        "SELECT/data plans must run on the native engine, not here."
    )


def _drain_pipeline(plan: PhysicalPlan, enable_tracing: bool = False):
    """Drive the pipeline to completion without yielding to the caller.
    Used for INSERT (side effects) and EXPLAIN ANALYZE (telemetry) where
    we discard the result rows."""
    chains, exit_node, ctx = compile_pipeline(plan)
    if enable_tracing:
        # EXPLAIN ANALYZE wants per-operator wall-time. Flip the trace flag
        # on every node so push() emits clock_gettime + execution_time.
        for nid, node in plan.depth_first_search_flat():
            if getattr(node, "enable_tracing", None) is not None:
                node.enable_tracing(True)
    try:
        for scan, chain_head in chains:
            # Consume drive_scan but discard all yielded morsels.
            for _ in drive_scan(scan, chain_head, exit_node, ctx):
                pass
            if ctx.is_terminated():
                break
    finally:
        # On exception, stop any further scan and let drive_scan's own finally
        # (entered as the generator is collected) close source-side resources.
        ctx.terminate()


def explain(
    plan: PhysicalPlan,
    analyze: bool,
    _format: str,
    telemetry: QueryTelemetry = None,
) -> Generator[Morsel, None, None]:
    from opteryx.operators import BasePlanNode
    from opteryx.operators.exit import ExitNode
    from opteryx.operators.explain import ExplainNode

    # Record stream consumed by the MERMAID renderer (one dict per operator).
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
                    sensors = operator.sensors()
                    record["self_time_ms"] = sensors.get("self_time", operator.execution_time) / 1e6
                    record["records_in"] = operator.records_in
                    record["records_out"] = operator.records_out
                    record["bytes_in"] = operator.bytes_in
                    record["bytes_out"] = operator.bytes_out
                    record["calls"] = operator.calls
                yield record
                yield from _inner_explain(operator_name[0], depth + 1)

    # Real operator children of a node, transparently skipping the Exit/Explain
    # wrappers so the rendered tree starts at the first data operator.
    def _real_children(node_id):
        # Ordered by edge label so a join's legs render left-then-right. The
        # label is what the physical plan reads to pick the build side, and
        # ingoing_edges yields in storage order, which for a swapped join is
        # the pre-swap order -- rendering that order makes a correct
        # smallest-table-left swap read as inverted. Unlabelled edges keep
        # their relative order.
        _leg_rank = {"left": 0, "right": 1}
        kids = []
        edges = sorted(
            plan.ingoing_edges(node_id), key=lambda edge: _leg_rank.get(edge[2], 2)
        )
        for edge in edges:
            child_id = edge[0]
            child = plan[child_id]
            if isinstance(child, (ExitNode, ExplainNode)):
                kids.extend(_real_children(child_id))
            elif isinstance(child, BasePlanNode):
                kids.append(child_id)
        return kids

    # Build the indented operator tree (label, details, operator) bottom-up.
    def _tree_rows(node_id, prefix, is_last, is_root, out):
        operator = plan[node_id]
        name = operator.name or type(operator).__name__
        if is_root:
            label = name
            child_prefix = ""
        else:
            label = prefix + ("└─ " if is_last else "├─ ") + name
            child_prefix = prefix + ("   " if is_last else "│  ")
        out.append((label, str(operator.config) if operator.config else "", operator, node_id))
        children = _real_children(node_id)
        for index, child_id in enumerate(children):
            _tree_rows(child_id, child_prefix, index == len(children) - 1, False, out)

    head = list(dict.fromkeys(plan.get_exit_points()))
    if len(head) != 1:
        raise InvalidInternalStateError(f"Problem with the plan - it has {len(head)} heads.")

    if analyze:
        # Drive the underlying query for telemetry but discard the result rows.
        # The wrapped query is SELECT-shaped (an ExitNode still sits one edge
        # below ExplainNode — see plan_explain) and must run on the native
        # engine like any other SELECT, not the legacy push-pipeline. Strip
        # ExplainNode from a copy of the plan (heal=True re-exposes the
        # ExitNode as the sole exit point) and hand that to execute_native;
        # native per-operator stats land in telemetry._reading["native_op_stats"],
        # keyed by node identity, which _row_count/_self_ms below read via the
        # same overlay mermaid.py already uses for the MERMAID format.
        query_head_edges = plan.ingoing_edges(head[0])
        if query_head_edges:
            from opteryx.managers.execution.compiler import execute_native

            subplan = plan.copy()
            subplan.remove_node(head[0], heal=True)
            generator, _ = execute_native(subplan, telemetry=telemetry)
            for _ in generator:
                pass

    if _format != "TEXT":
        explained = list(_inner_explain(head[0], 1))
        from opteryx.utils import mermaid

        mermaid_plan = mermaid.plan_to_mermaid(plan, explained)
        yield Morsel.from_vectors(
            ["plan"], [vector_from_sequence([mermaid_plan], dtype=DrakenType.VARCHAR)]
        )
        return

    # ── TEXT: tabular operator tree ──────────────────────────────────────────
    op_rows: list = []
    tops = _real_children(head[0])
    for index, top in enumerate(tops):
        _tree_rows(top, "", index == len(tops) - 1, True, op_rows)

    # The query now always runs on the native engine (see the `analyze` branch
    # above) — per-operator stats live in telemetry._reading["native_op_stats"],
    # keyed by node identity. Reuse mermaid.py's overlay (the same one the
    # MERMAID format and the general `.telemetry` property already read)
    # instead of re-deriving the lookup here.
    node_stats_by_nid: dict = {}
    if analyze:
        from opteryx.utils import mermaid as _mermaid

        node_stats_by_nid, _, _ = _mermaid._collect_node_stats(plan)

    def _row_count(node_id):
        stat = node_stats_by_nid.get(node_id)
        return int(stat.get("records_out", 0) or 0) if stat else 0

    def _time_ms(node_id):
        stat = node_stats_by_nid.get(node_id)
        return round((stat.get("execution_time", 0) or 0) / 1e6, 3) if stat else 0.0

    def _self_ms(node_id):
        # self_ms == time_ms on the native path — the executor times each
        # operator's own call only, so there is no separate downstream
        # component to subtract (see mermaid.py's get_node_stats).
        stat = node_stats_by_nid.get(node_id)
        return round((stat.get("self_time", 0) or 0) / 1e6, 3) if stat else 0.0

    # est_rows is planning-time (no execution needed), so it's available for
    # plain EXPLAIN too, not just ANALYZE -- the physical plan graph is keyed
    # by the same nid as the logical plan (see create_physical_plan), so this
    # correlates directly against statistics_refresh's estimate, no separate
    # identity mapping needed. Empty/0 when refresh_statistics never ran for
    # this node (e.g. an input lacked real row counts -- see result_size_guard).
    est_rows_by_nid = {
        entry["nid"]: entry["row_count"]
        for entry in (getattr(telemetry, "estimated_row_counts", None) or [])
    }

    def _est_row_count(node_id):
        return int(est_rows_by_nid.get(node_id, 0) or 0)

    # est_bytes mirrors est_rows above -- same planning-time availability, same
    # nid correlation. `total_bytes` is None (not 0) for a node where not one
    # column carried a byte-size estimate (see StatisticsRefreshVisitor.
    # _record_telemetry); 0 here means exactly that "genuinely unknown" case,
    # same convention _est_row_count already uses for a node refresh_statistics
    # never reached.
    est_bytes_by_nid = {
        entry["nid"]: entry["total_bytes"]
        for entry in (getattr(telemetry, "estimated_total_bytes", None) or [])
    }

    def _est_bytes(node_id):
        value = est_bytes_by_nid.get(node_id)
        return int(value) if value is not None else 0

    tree_col = [row[0] for row in op_rows]
    details_col = [row[1] for row in op_rows]
    est_rows_col = [_est_row_count(row[3]) for row in op_rows]
    est_bytes_col = [_est_bytes(row[3]) for row in op_rows]
    rows_col = [_row_count(row[3]) for row in op_rows]
    time_col = [_time_ms(row[3]) for row in op_rows]
    self_col = [_self_ms(row[3]) for row in op_rows]

    # ── OPTIMIZATIONS: which optimizer rules fired, from telemetry counters ───
    readings = getattr(telemetry, "_reading", None) or {}
    opt_items = []
    for key in sorted(readings):
        value = readings[key]
        if not isinstance(value, int) or value <= 0:
            continue
        if key.startswith("optimization_"):
            opt_items.append((key[len("optimization_") :].replace("_", " "), value))
        elif key == "files_pruned":
            opt_items.append(("files pruned", value))

    # Costed plan choices (QueryTelemetry.add_decision): rendered with the numbers
    # the choice was made on, so a wrong pick is diagnosable from this text alone.
    opt_rows = [(label, f"applied {count}×" if count > 1 else "applied") for label, count in opt_items]
    for decision in readings.get("optimizer_decisions") or []:
        opt_rows.append((decision["label"], decision["detail"]))

    if opt_rows:
        tree_col.append("OPTIMIZATIONS")
        details_col.append("")
        est_rows_col.append(0)
        est_bytes_col.append(0)
        rows_col.append(0)
        time_col.append(0.0)
        self_col.append(0.0)
        for index, (label, detail) in enumerate(opt_rows):
            connector = "└─ " if index == len(opt_rows) - 1 else "├─ "
            tree_col.append(connector + label)
            details_col.append(detail)
            est_rows_col.append(0)
            est_bytes_col.append(0)
            rows_col.append(0)
            time_col.append(0.0)
            self_col.append(0.0)

    # ── REWRITE TRACE: ordered strategies that changed plan structure ────────
    # Grade-A structural trace (see QueryTelemetry.add_plan_rewrite): the
    # sequence of plan-rewriter/optimizer strategies whose node or edge counts
    # moved, in application order. Expression-only rewrites appear under
    # OPTIMIZATIONS above, not here.
    trace = getattr(telemetry, "optimizer_trace", None) or []
    shape_changes = [entry for entry in trace if entry.get("changed")]
    if shape_changes:
        tree_col.append("REWRITE TRACE")
        details_col.append("")
        est_rows_col.append(0)
        est_bytes_col.append(0)
        rows_col.append(0)
        time_col.append(0.0)
        self_col.append(0.0)
        for index, entry in enumerate(shape_changes):
            connector = "└─ " if index == len(shape_changes) - 1 else "├─ "
            node_before, node_after = entry["nodes"]
            edge_before, edge_after = entry["edges"]
            tree_col.append(connector + entry["strategy"])
            details_col.append(
                f"nodes {node_before}→{node_after}, edges {edge_before}→{edge_after}"
            )
            est_rows_col.append(0)
            est_bytes_col.append(0)
            rows_col.append(0)
            time_col.append(0.0)
            self_col.append(0.0)

    columns = ["tree", "details", "est_rows", "est_bytes"]
    vectors = [
        vector_from_sequence([row.encode("utf-8") for row in tree_col], dtype=DrakenType.VARBINARY),
        vector_from_sequence(details_col, dtype=DrakenType.VARCHAR),
        vector_from_sequence(est_rows_col, dtype=DrakenType.INT64),
        vector_from_sequence(est_bytes_col, dtype=DrakenType.INT64),
    ]
    if analyze:
        # time_ms is INCLUSIVE (own + downstream); self_ms is this operator's own
        # work only — the column to read when deciding what to parallelise.
        columns += ["rows", "time_ms", "self_ms"]
        vectors += [
            vector_from_sequence(rows_col, dtype=DrakenType.INT64),
            vector_from_sequence(time_col, dtype=DrakenType.FLOAT64),
            vector_from_sequence(self_col, dtype=DrakenType.FLOAT64),
        ]

    yield Morsel.from_vectors(columns, vectors)
