# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Pipeline compiler — translates a PhysicalPlan into a wired push pipeline.

Runs once per query. Walks the plan graph and:
  - Sets `_downstream` on every operator (typed Cython pointer)
  - Allocates and attaches a shared `PipelineContext` (for LIMIT short-circuit)
  - For each join node, inserts `JoinLeftAdapter` / `JoinRightAdapter` at the
    terminal of the join's left / right input chains
  - Returns the scan-to-chain-head map plus the terminal exit node

After compilation the engine drives scans directly:
    for scan, chain_head in chains:
        for morsel in scan.read_morsels():
            chain_head.push(morsel)
        chain_head.push(EOS)

Per-morsel: typed Cython vtable dispatch only. No Python graph traversal,
no `next()` calls, no per-morsel generator frames.
"""

from typing import List, Optional, Tuple

from opteryx.models import PhysicalPlan
from opteryx.operators import (
    BasePlanNode,
    JoinLeftAdapter,
    JoinNode,
    JoinRightAdapter,
    PipelineContext,
)


def _outgoing_node(plan: PhysicalPlan, nid: str) -> Optional[str]:
    """Return the single downstream node id for `nid`, or None if `nid` is a
    sink. The push pipeline is a single-output topology; if a node has more
    than one outgoing edge that's a planner bug."""
    edges = list(plan.outgoing_edges(nid))
    if not edges:
        return None
    if len(edges) > 1:
        from opteryx.exceptions import InvalidInternalStateError
        raise InvalidInternalStateError(
            f"Operator {nid} has {len(edges)} downstream consumers; the push "
            "pipeline supports single-output topology only."
        )
    _, child, _ = edges[0]
    return child


def compile_pipeline(plan: PhysicalPlan):
    """Wire the operator graph into a push pipeline.

    Returns:
        (chains, exit_node, ctx) where
          chains    : list[(scan_node, chain_head)] in scan execution order
          exit_node : the terminal ExitNode (None if the query has no Exit)
          ctx       : shared PipelineContext used for backpressure
    """
    ctx = PipelineContext()

    # Build a per-join edge-label map. `plan.label_join_legs()` covers joins
    # that have populated left_readers/right_readers UUIDs (most cases), but
    # joins synthesised from INTERSECT/EXCEPT/IN-subquery may have those
    # fields unset — for those we fall back to incoming-edge insertion order
    # (first edge = left, second edge = right). This mirrors the fallback
    # already present at the tail of PhysicalPlan.label_join_legs.
    join_edge_labels: dict = {}
    for nid in plan.nodes():
        node = plan[nid]
        if not getattr(node, "is_join", False):
            continue
        for idx, (provider, _target, label) in enumerate(plan.ingoing_edges(nid)):
            if not label:
                label = "left" if idx == 0 else "right"
            join_edge_labels[(provider, nid)] = label

    # Walk the plan in DFS left-before-right order — same ordering the legacy
    # engine used for sequential scan driving and join build-before-probe.
    flat = plan.depth_first_search_flat()

    # Find the exit node (sink). The traversal starts from the exit, so head
    # of `flat` is the sink.
    exit_node = None
    if flat:
        candidate = flat[0][1]
        # ExitNode is the terminal formatter; identify by class name to avoid
        # a hard import dependency here.
        if candidate.__class__.__name__ == "ExitNode":
            exit_node = candidate

    # Lower and bind each ParquetReadNode's pushed-down predicate, exactly as
    # compiler.py._compile_scan does for the native engine's StreamingScanSource
    # path (same rewrite chain: CASE->IF_THEN_ELSE, BETWEEN->compares, decimal
    # rescale). ParquetReadNode fails loud at execute() time if predicates are
    # present with no compiled_predicate bound — that used to only happen on
    # the native path, so this push pipeline (EXPLAIN ANALYZE / INSERT ... SELECT)
    # crashed on any pushed-down scan predicate. One lowering, one rewrite chain,
    # reused here rather than re-implemented.
    for _nid, node in flat:
        if not getattr(node, "is_scan", False):
            continue
        predicates = getattr(node, "predicates", None)
        if not predicates or getattr(node, "compiled_predicate", None) is not None:
            continue
        from opteryx.managers.execution.compiler import _Compiler

        _compiler = _Compiler(None, None)
        node.compiled_predicate = _compiler._lower_bytecode(
            _compiler._compose_predicate_nodes(predicates)
        )

    # Attach the shared context to every operator. Wire _downstream pointers.
    # Stamp each operator with the number of upstream input chains feeding it
    # (incoming-edge count) so multi-input operators (e.g. Union) gate their
    # downstream EOS on all legs closing instead of hardcoding the leg count.
    # Joins route their two inputs through adapters and handle EOS in
    # push_left/push_right, so the stamped count is inert for them.
    for nid, node in flat:
        if isinstance(node, BasePlanNode):
            node.set_context(ctx)
            incoming = len(list(plan.ingoing_edges(nid)))
            if incoming > 1:
                node.set_expected_input_closes(incoming)

    # Wire downstream pointers — for each non-join operator, _downstream is
    # the (single) outgoing edge's target. For joins we keep the join's
    # downstream as the operator that follows it; the join's two inputs are
    # adapted below.
    for nid, node in flat:
        if not isinstance(node, BasePlanNode):
            continue
        child_id = _outgoing_node(plan, nid)
        if child_id is None:
            # Sink (exit node, or insert / management nodes off-pipeline)
            continue
        child = plan[child_id]
        if isinstance(child, JoinNode):
            # Insert an adapter so this operator pushes into the correct
            # side of the join. Determine which leg by looking at the edge
            # label between `nid` and `child_id`.
            label = _edge_label(plan, nid, child_id, join_edge_labels)
            adapter = _make_join_adapter(child, label)
            adapter.set_context(ctx)
            node.set_downstream(adapter)
            # Adapters themselves have no downstream — the join routes its
            # own output via its _downstream pointer.
        else:
            node.set_downstream(child)

    # Identify the scan-to-chain-head mapping. A "chain head" is the operator
    # immediately downstream of the scan (or, if the scan feeds directly
    # into a join leg, the adapter for that leg).
    chains: List[Tuple[BasePlanNode, BasePlanNode]] = []
    for nid, node in flat:
        if not getattr(node, "is_scan", False):
            continue
        child_id = _outgoing_node(plan, nid)
        if child_id is None:
            # Scan with no downstream — degenerate plan, skip.
            continue
        child = plan[child_id]
        if isinstance(child, JoinNode):
            label = _edge_label(plan, nid, child_id, join_edge_labels)
            head = _make_join_adapter(child, label)
            head.set_context(ctx)
        else:
            head = child
        chains.append((node, head))

    return chains, exit_node, ctx


def _edge_label(plan: PhysicalPlan, source: str, target: str, fallback_map: dict = None) -> str:
    """Return the edge label between `source` and `target` (e.g. 'left',
    'right', or ''). Falls back to the precomputed positional map (built
    above) when the graph edge itself has no label."""
    for s, t, label in plan.outgoing_edges(source):
        if t == target:
            if label:
                return label
            break
    if fallback_map is not None:
        return fallback_map.get((source, target), "")
    return ""


def _make_join_adapter(join: JoinNode, label: str):
    """Build the appropriate adapter for a join input leg. Falls back to
    'left' if the label is missing — joins that don't use traditional
    left/right routing (e.g. UnnestJoinNode) are excluded by the caller."""
    if label == "right":
        return JoinRightAdapter(join)
    return JoinLeftAdapter(join)
