# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Pipeline compiler — translates a PhysicalPlan into a wired push pipeline.

Runs once per query. Walks the plan graph and:
  - Sets `_downstream` on every operator (typed Cython pointer)
  - Allocates and attaches a shared `PipelineContext` (for LIMIT short-circuit)
  - Returns the scan-to-chain-head map plus the terminal exit node

Only INSERT ... VALUES still runs here (a function dataset pushed into the insert
sink) — interim debt per CLAUDE.md §2. Every data pipeline runs on the native
engine, and no operator on this path has two inputs.

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

    # Walk the plan in DFS left-before-right order — same ordering the legacy
    # engine used for sequential scan driving and join build-before-probe.
    flat = plan.depth_first_search_flat()

    # Find the exit node (sink). The traversal starts from the exit, so head
    # of `flat` is the sink.
    exit_node = None
    if flat:
        candidate = flat[0][1]
        # ExitNode is the terminal formatter.
        if candidate.kind == "ExitNode":
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
    for nid, node in flat:
        if isinstance(node, BasePlanNode):
            node.set_context(ctx)
            incoming = len(list(plan.ingoing_edges(nid)))
            if incoming > 1:
                node.set_expected_input_closes(incoming)

    # Wire downstream pointers — each operator's _downstream is the (single)
    # outgoing edge's target.
    for nid, node in flat:
        if not isinstance(node, BasePlanNode):
            continue
        child_id = _outgoing_node(plan, nid)
        if child_id is None:
            # Sink (exit node, or insert / management nodes off-pipeline)
            continue
        node.set_downstream(plan[child_id])

    # Identify the scan-to-chain-head mapping. A "chain head" is the operator
    # immediately downstream of the scan.
    chains: List[Tuple[BasePlanNode, BasePlanNode]] = []
    for nid, node in flat:
        if not getattr(node, "is_scan", False):
            continue
        child_id = _outgoing_node(plan, nid)
        if child_id is None:
            # Scan with no downstream — degenerate plan, skip.
            continue
        chains.append((node, plan[child_id]))

    return chains, exit_node, ctx
