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
    from opteryx.operators.show_grants import ShowGrantsNode
    from opteryx.operators.show_manifest import ShowManifestNode
    from opteryx.operators.show_snapshots import ShowSnapshotsNode
    from opteryx.operators.show_create import ShowCreateNode
    from opteryx.operators.show_value import ShowValueNode
    from opteryx.operators.table_management import TableManagementNode
    from opteryx.operators.view_management import ViewManagementNode
    from opteryx.operators.relation_management import RelationManagementNode
    from opteryx.operators.insert import InsertNode
    from opteryx.operators.merge import MergeNode

    return (
        ExplainNode,
        SetVariableNode,
        ShowColumnsNode,
        ShowGrantsNode,
        ShowManifestNode,
        ShowSnapshotsNode,
        ShowCreateNode,
        ShowValueNode,
        TableManagementNode,
        ViewManagementNode,
        RelationManagementNode,
        InsertNode,
        MergeNode,
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
    from opteryx.operators.show_grants import ShowGrantsNode
    from opteryx.operators.show_manifest import ShowManifestNode
    from opteryx.operators.show_snapshots import ShowSnapshotsNode
    from opteryx.operators.show_create import ShowCreateNode
    from opteryx.operators.show_value import ShowValueNode
    from opteryx.operators.table_management import TableManagementNode
    from opteryx.operators.view_management import ViewManagementNode
    from opteryx.operators.relation_management import RelationManagementNode
    from opteryx.operators.insert import InsertNode
    from opteryx.operators.merge import MergeNode

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
    if isinstance(head_node, MergeNode):
        # MERGE's whole body — the join, the action chain and the blended
        # columns — is an ordinary Exit-headed SELECT built by plan_merge, so it
        # runs natively like any other. Python drives only the sink, exactly as
        # the INSERT path below does: interim debt per CLAUDE.md §2, shared with
        # that path rather than duplicated so there stays ONE loop to move.
        subplan = plan.copy()
        subplan.remove_node(head_nodes[0], heal=True)
        new_head = subplan[subplan.get_exit_points()[0]]
        if type(new_head).__name__ != "ExitNode":
            raise InvalidInternalStateError(
                "MERGE sub-plan is not Exit-headed; it cannot run on the native engine"
            )
        from opteryx.managers.execution.compiler import execute_native

        generator, _ = execute_native(subplan, telemetry=telemetry)
        for morsel in generator:
            head_node._push_impl(morsel)
        head_node._push_impl(EOS)
        if head_node.result is None:
            raise InvalidInternalStateError("MergeNode did not produce a result")
        return head_node.result, ResultType.NON_TABULAR
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
    # SHOW GRANTS ON is answered from the permissions capability (stashed
    # execution context), not from a Scan — same no-pipeline shape.
    if isinstance(
        head_node,
        (
            ShowValueNode,
            ShowCreateNode,
            ShowColumnsNode,
            ShowGrantsNode,
            ShowManifestNode,
            ShowSnapshotsNode,
        ),
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
            # Graph.copy() drops instance attributes; without these the compiler
            # refuses every CTE reference ("shared body was not compiled").
            subplan.shared_ctes = getattr(plan, "shared_ctes", None) or {}
            subplan.recursive_ctes = getattr(plan, "recursive_ctes", None) or {}
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
        # Shared/recursive CTE bodies ran in the same engine and their operators
        # carry readings under their own identities; fold them in so the
        # RECURSIVE CTE section below renders real numbers, not zeros.
        for _body in (getattr(plan, "shared_ctes", None) or {}).values():
            _body_stats, _, _ = _mermaid._collect_node_stats(_body)
            node_stats_by_nid.update(_body_stats)

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

    def _cpu_ms(node_id):
        # CPU this node actually burned (CLOCK_THREAD_CPUTIME_ID, summed across
        # every dop worker), against time_ms's WALL time. The gap between them is
        # time the node was not running: blocked on IO, on downstream
        # backpressure, or descheduled. That distinction — "expensive" vs
        # "starved" — is the first question anyone asks of a slow operator, and
        # the engine has recorded it all along (OpStats::cpu_ns, always-on); it
        # was simply never rendered. Architect ruling D1, 2026-08-25: publish the
        # two honest columns side by side and let the reader subtract, rather than
        # a derived `wait_ms` that hides which of the two causes it was.
        #
        # An Operator cannot block (pure in-memory compute over a morsel), so a
        # wide time_ms/cpu_ms gap is expected only at sources and sinks.
        #
        # KNOWN BOUND. The CPU clock is read on the OUTSIDE of the wall bracket
        # (see telem_cpu_now_ns in executor.hpp), which keeps time_ms honest at
        # the cost of charging cpu_ms two wall-clock reads (~52ns) per call.
        # Measured consequence: for nodes averaging >=45us per call the ratio is
        # 0.95-1.07 (trustworthy); for nodes averaging ~1us per call it reads
        # 1.5-1.9 and cpu_ms can exceed time_ms, which is instrument overhead,
        # not a measurement. Those nodes total single-digit microseconds, so the
        # distortion is confined to rows nobody is optimising. Not clamped:
        # min()-ing cpu_ms to time_ms would hide the artifact rather than bound it.
        stat = node_stats_by_nid.get(node_id)
        return round((stat.get("cpu_time_ms", 0) or 0), 3) if stat else 0.0

    def _merge_ms(node_id):
        # Breaker cost: combine() + finalize(), the two Sink calls that run AFTER
        # the morsels stop flowing. Zero on sources and operators, which have
        # neither. Until 2026-08-25 both were timed by nothing at all, so a hash
        # aggregate's cross-worker merge and its result construction were real work
        # charged to no plan node — they showed up in the pipeline's wall clock and
        # nowhere else. Deliberately NOT folded into time_ms (architect ruling D4):
        # that would move a published number.
        #
        # One column, not two, because they answer one question — what the breaker
        # cost once the stream ended. The split stays in
        # telemetry._reading["native_op_stats"] as combine_time/finalize_time for
        # anyone who needs to know which half.
        stat = node_stats_by_nid.get(node_id)
        return round((stat.get("merge_time", 0) or 0) / 1e6, 3) if stat else 0.0

    def _dop(node_id):
        # Degree of parallelism the node's pipeline ran at. A width, not a
        # duration: time_ms and cpu_ms are both summed across dop workers, so
        # neither is readable without knowing how many there were.
        stat = node_stats_by_nid.get(node_id)
        return int(stat.get("dop", 0) or 0) if stat else 0

    # est_rows is planning-time (no execution needed), so it's available for
    # plain EXPLAIN too, not just ANALYZE -- the physical plan graph is keyed
    # by the same nid as the logical plan (see create_physical_plan), so this
    # correlates directly against statistics_refresh's estimate, no separate
    # identity mapping needed. NULL (not 0) when refresh_statistics never reached
    # this node -- see _est_row_count; EXPLAIN ANALYZE forces the refresh so this
    # is rare on the surface where est-vs-actual is the whole point.
    est_rows_by_nid = {
        entry["nid"]: entry["row_count"]
        for entry in (getattr(telemetry, "estimated_row_counts", None) or [])
    }

    def _est_row_count(node_id):
        # None, not 0. `refresh_statistics` runs opportunistically (only when a
        # strategy asks for it, plus result_size_guard), so a node it never reached
        # has NO estimate — which is a different fact from "estimated zero rows",
        # and 0 cannot say which one happened. Architect ruling D3, 2026-08-25:
        # render the unknown distinctly. EXPLAIN ANALYZE additionally forces the
        # refresh (planner/__init__.py) so this is rare on the surface where the
        # est-vs-actual comparison is the point.
        value = est_rows_by_nid.get(node_id)
        return int(value) if value is not None else None

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
        # None, not 0 — same D3 reasoning as _est_row_count. `total_bytes` is
        # already None (not 0) for a node where no column carried a byte-size
        # estimate, so this now preserves that distinction instead of flattening it.
        value = est_bytes_by_nid.get(node_id)
        return int(value) if value is not None else None

    tree_col = [row[0] for row in op_rows]
    details_col = [row[1] for row in op_rows]
    est_rows_col = [_est_row_count(row[3]) for row in op_rows]
    est_bytes_col = [_est_bytes(row[3]) for row in op_rows]
    rows_col = [_row_count(row[3]) for row in op_rows]
    time_col = [_time_ms(row[3]) for row in op_rows]
    self_col = [_self_ms(row[3]) for row in op_rows]
    cpu_col = [_cpu_ms(row[3]) for row in op_rows]
    merge_col = [_merge_ms(row[3]) for row in op_rows]
    dop_col = [_dop(row[3]) for row in op_rows]

    def _append_no_reading():
        """Append one row that is NOT a plan node — an OPTIMIZATIONS/REWRITE TRACE
        section heading or rule entry. Such a row has no estimate, ran nothing and
        produced nothing, so every numeric column is NULL rather than 0. Same D3
        reasoning as _est_row_count, applied across the row: 0 is a legitimate
        reading (a node CAN emit zero rows in zero measurable time) and therefore
        cannot also mean "there is no reading here"."""
        est_rows_col.append(None)
        est_bytes_col.append(None)
        rows_col.append(None)
        time_col.append(None)
        self_col.append(None)
        cpu_col.append(None)
        merge_col.append(None)
        dop_col.append(None)

    # ── RECURSIVE CTEs: anchor + term bodies and the fixpoint readings ───────
    # Each WITH RECURSIVE renders as its own section: the header carries the
    # UNION flavour and — under ANALYZE — the passes the fixpoint actually ran
    # and (UNION) the visited-set size, from the engine's LoopSpan readings.
    # The legs are physical plans of their own (plan.shared_ctes); their
    # operator rows read the same per-identity stats overlay as the main tree.
    recursive_meta = getattr(plan, "recursive_ctes", None) or {}
    if recursive_meta:
        shared_bodies = getattr(plan, "shared_ctes", None) or {}
        loop_by_name = {
            entry["name"]: entry
            for entry in (
                (getattr(telemetry, "_reading", None) or {}).get("recursive_loop_stats")
                or []
            )
        }

        def _graph_tree_rows(graph, node_id, prefix, is_last, out):
            operator = graph[node_id]
            name = operator.name or type(operator).__name__
            label = prefix + ("└─ " if is_last else "├─ ") + name
            child_prefix = prefix + ("   " if is_last else "│  ")
            out.append((label, str(operator.config) if operator.config else "", node_id))
            _leg_rank = {"left": 0, "right": 1}
            children = sorted(
                graph.ingoing_edges(node_id), key=lambda edge: _leg_rank.get(edge[2], 2)
            )
            for index, edge in enumerate(children):
                _graph_tree_rows(
                    graph, edge[0], child_prefix, index == len(children) - 1, out
                )

        for _rkey, meta in recursive_meta.items():
            reading = loop_by_name.get(meta["name"])
            header_detail = "UNION" if meta.get("distinct") else "UNION ALL"
            if reading is not None:
                header_detail += f", {reading['iterations']} iterations"
                if reading["distinct"]:
                    header_detail += f", {reading['visited_rows']} distinct rows"
                header_detail += f", ceiling {reading['max_iterations']}"
            tree_col.append(f"RECURSIVE CTE {meta['name']}")
            details_col.append(header_detail)
            _append_no_reading()
            legs = (
                ("ANCHOR", meta["anchor_key"], False),
                ("RECURSIVE TERM", meta["term_key"], True),
            )
            for role, leg_key, leg_is_last in legs:
                tree_col.append(("└─ " if leg_is_last else "├─ ") + role)
                details_col.append("")
                _append_no_reading()
                body = shared_bodies.get(leg_key)
                if body is None:
                    continue
                body_rows: list = []
                body_heads = list(dict.fromkeys(body.get_exit_points()))
                for index, body_head in enumerate(body_heads):
                    _graph_tree_rows(
                        body,
                        body_head,
                        "   " if leg_is_last else "│  ",
                        index == len(body_heads) - 1,
                        body_rows,
                    )
                for label, config, body_nid in body_rows:
                    tree_col.append(label)
                    details_col.append(config)
                    est_rows_col.append(_est_row_count(body_nid))
                    est_bytes_col.append(_est_bytes(body_nid))
                    rows_col.append(_row_count(body_nid))
                    time_col.append(_time_ms(body_nid))
                    self_col.append(_self_ms(body_nid))
                    cpu_col.append(_cpu_ms(body_nid))
                    merge_col.append(_merge_ms(body_nid))
                    dop_col.append(_dop(body_nid))

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
        _append_no_reading()
        for index, (label, detail) in enumerate(opt_rows):
            connector = "└─ " if index == len(opt_rows) - 1 else "├─ "
            tree_col.append(connector + label)
            details_col.append(detail)
            _append_no_reading()

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
        _append_no_reading()
        for index, entry in enumerate(shape_changes):
            connector = "└─ " if index == len(shape_changes) - 1 else "├─ "
            node_before, node_after = entry["nodes"]
            edge_before, edge_after = entry["edges"]
            tree_col.append(connector + entry["strategy"])
            details_col.append(
                f"nodes {node_before}→{node_after}, edges {edge_before}→{edge_after}"
            )
            _append_no_reading()

    columns = ["tree", "details", "est_rows", "est_bytes"]
    vectors = [
        vector_from_sequence([row.encode("utf-8") for row in tree_col], dtype=DrakenType.VARBINARY),
        vector_from_sequence(details_col, dtype=DrakenType.VARCHAR),
        vector_from_sequence(est_rows_col, dtype=DrakenType.INT64),
        vector_from_sequence(est_bytes_col, dtype=DrakenType.INT64),
    ]
    if analyze:
        # time_ms is WALL time; cpu_ms is the CPU actually burned. time_ms - cpu_ms
        # is time the node was NOT running (blocked on IO/backpressure, or
        # descheduled) — read together, that gap separates an expensive operator
        # from a starved one. Both are summed across `dop` workers, which is why
        # dop is here too. self_ms is this operator's own work only, and equals
        # time_ms on the native path.
        columns += ["rows", "time_ms", "cpu_ms", "merge_ms", "self_ms", "dop"]
        vectors += [
            vector_from_sequence(rows_col, dtype=DrakenType.INT64),
            vector_from_sequence(time_col, dtype=DrakenType.FLOAT64),
            vector_from_sequence(cpu_col, dtype=DrakenType.FLOAT64),
            vector_from_sequence(merge_col, dtype=DrakenType.FLOAT64),
            vector_from_sequence(self_col, dtype=DrakenType.FLOAT64),
            vector_from_sequence(dop_col, dtype=DrakenType.INT64),
        ]

    yield Morsel.from_vectors(columns, vectors)
