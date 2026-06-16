# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Parallel execution engine — central scheduler (M4).

The engine parallelises a *pipeline segment* (scan → stateless ops → pipeline
breaker) over data partitions, recombining at the breaker. Reusable
infrastructure:

  * ``resolve_worker_count`` — query-scoped worker sizing (cap 8, the measured
    regression boundary; leaves cores for IO/decode).
  * ``identify_segments`` — cuts the compiled operator DAG into *pipeline
    segments* (the unit of parallelism). A segment is a maximal linear run of
    operators ending at the next pipeline breaker (inclusive) or at the sink.
    Because the push pipeline is single-output topology, every segment is a
    linear chain; multi-input breakers (joins, union) are the shared tail of
    their two input segments.

Stage 1 (current scope): the **grouped-aggregate pipeline** —
``scan → {Filter,Projection}* → Grouped Aggregate``. The whole segment runs on
W workers, each over a disjoint partition of the scan's morsels into its OWN
cloned chain (one worker thread per clone — no shared operator state); on EOS
the W partial engines are combined via the proven WP-7 ``merge()`` into the
original breaker, which then finalises and drives the serial tail of the plan.

Scope guards (everything else runs serially — strategy selection, NOT silent
degradation; surfaced via the ``parallel_engaged`` telemetry reading):
  * exactly one scan (no joins/unions yet — Stage 2);
  * the scan's segment ends in a *grouped* aggregate whose engine
    ``is_mergeable()`` (ungrouped agg carries literal state outside the engine
    and is deferred; non-mergeable aggregates — COUNT DISTINCT / median /
    decimal / string MIN-MAX — stay serial);
  * the segment's middle operators are all STATELESS (Filter / Projection);
  * a row-floor: tiny inputs run serially (clone + thread setup would dominate).

The §6 enforcement-checklist items (PipelineContext atomicity, Union/Exit
close-counting, _FOOTER_CACHE) are intentionally NOT needed by this drive: the
scan is pulled single-threaded, each worker owns a private clone, and the merge
+ EOS happen on the calling thread AFTER the workers join — so there is no
concurrent writer to the termination flag, no concurrent singleton push, and no
concurrent scan. They land with the stage that introduces those (parallel scan /
joins — Stage 2/3), per the contract's "don't harden until needed".

See docs/M4_CENTRAL_SCHEDULER_DESIGN.md.
"""

import os
import queue as _queue
from dataclasses import dataclass
from typing import List
from typing import Optional
from typing import Tuple

from opteryx import EOS as _EOS_SENTINEL
from opteryx import config
from opteryx.constants import ResultType
from opteryx.operators.catalog import OperatorParallelism
from opteryx.operators.catalog import get_registry

# Hard cap on parallel width: past 8 workers every prototype regressed (the
# merge/recombination tail and DRAM bandwidth dominate). See
# docs/M4_PARALLEL_AGG_PROTOTYPE.md.
_MAX_WORKER_CAP = 8

# Per-worker bounded-queue depth — backpressure. A full queue blocks the main
# thread's morsel hand-off, throttling the scan pull to the workers' rate.
_QUEUE_DEPTH = 4

# Per-worker queue sentinel: tells a worker loop its partition is complete.
_WORKER_DONE = object()


def resolve_worker_count(requested: int) -> int:
    """Resolve the effective worker count for a query.

    ``requested`` is ``config.MAX_EXECUTION_WORKERS``. The result is clamped to
    ``[1, min(requested, cpu-2, 8)]`` — leaving headroom for the C++ IO/decode
    pool and never exceeding the measured regression boundary.
    """
    if requested <= 1:
        return 1
    cpu = os.cpu_count() or 1
    # Leave 2 cores for IO/decode + the main orchestration thread.
    headroom = max(1, cpu - 2)
    return max(1, min(requested, headroom, _MAX_WORKER_CAP))


@dataclass(frozen=True)
class Segment:
    """A pipeline segment — the scheduler's unit of parallelism.

    ``nodes`` are the plan node ids in dataflow order (source first, tail last).
    ``tail`` is the last node: a pipeline breaker, or the pipeline sink.
    ``tail_is_breaker`` distinguishes the two. ``parallelism`` is the tail's
    recombination class (how W partials become one) — ``None`` for a sink tail.
    """

    nodes: Tuple[str, ...]
    tail: str
    tail_is_breaker: bool
    parallelism: Optional[OperatorParallelism]


def _is_breaker(plan, nid: str, registry) -> bool:
    meta = registry.get(type(plan[nid]))
    return bool(meta and meta.is_pipeline_breaking)


def identify_segments(plan) -> List[Segment]:
    """Cut the physical plan into pipeline segments.

    A segment starts at a scan or at the operator immediately downstream of a
    breaker, and runs to the next breaker (inclusive) or the sink. The push
    pipeline is single-output topology, so each walk is unambiguous: every
    non-breaker node belongs to exactly one segment, and each breaker is the
    tail of its input segment(s).
    """
    registry = get_registry()

    # Segment sources: scans, plus any node whose provider is a breaker (a
    # breaker's output begins a fresh segment). A breaker is never a source —
    # it is the tail of the segment(s) feeding it.
    sources: List[str] = []
    for nid in plan.nodes():
        node = plan[nid]
        if getattr(node, "is_scan", False):
            sources.append(nid)
            continue
        if _is_breaker(plan, nid, registry):
            continue
        providers = list(plan.ingoing_edges(nid))
        if providers and any(_is_breaker(plan, p[0], registry) for p in providers):
            sources.append(nid)

    segments: List[Segment] = []
    for start in sources:
        nodes: List[str] = []
        cur = start
        while True:
            nodes.append(cur)
            if _is_breaker(plan, cur, registry):
                break  # breaker is this segment's tail
            outs = list(plan.outgoing_edges(cur))
            if not outs:
                break  # sink (e.g. Exit) is this segment's tail
            nxt = outs[0][1]
            if _is_breaker(plan, nxt, registry):
                nodes.append(nxt)  # breaker tail belongs to this segment
                break
            cur = nxt
        tail = nodes[-1]
        tail_is_breaker = _is_breaker(plan, tail, registry)
        meta = registry.get(type(plan[tail]))
        parallelism = meta.parallelism if (meta and tail_is_breaker) else None
        segments.append(
            Segment(
                nodes=tuple(nodes),
                tail=tail,
                tail_is_breaker=tail_is_breaker,
                parallelism=parallelism,
            )
        )
    return segments


# ── Stage 1: grouped-aggregate pipeline ──────────────────────────────────────


def _find_parallel_grouped_agg(plan):
    """Return ``(scan_id, middle_ids, breaker_id)`` if the plan is a single-scan
    grouped-aggregate pipeline this stage can parallelise, else ``None``.

    Pure plan inspection — no compilation, no side effects.
    """
    registry = get_registry()
    scans = [nid for nid in plan.nodes() if getattr(plan[nid], "is_scan", False)]
    if len(scans) != 1:
        return None
    scan_id = scans[0]

    # The scan's segment must end in a mergeable breaker.
    segment = next((s for s in identify_segments(plan) if s.nodes[0] == scan_id), None)
    if segment is None or not segment.tail_is_breaker:
        return None
    if segment.parallelism != OperatorParallelism.STATEFUL_MERGEABLE:
        return None

    breaker = plan[segment.tail]
    # Stage 1 = GROUPED aggregate only. The grouped node keeps ALL cross-morsel
    # state in `_engine`, so WP-7 merge() is a complete recombination. (Ungrouped
    # agg carries literal state outside the engine — deferred.)
    if breaker.__class__.__name__ != "GroupedAggregateHashedNode":
        return None
    engine = getattr(breaker, "_engine", None)
    if engine is None or not engine.is_mergeable():
        return None

    # Middle operators must be stateless (Filter / Projection) — safe to clone
    # and run per-worker.
    middle_ids = list(segment.nodes[1:-1])
    for nid in middle_ids:
        meta = registry.get(type(plan[nid]))
        if meta is None or meta.parallelism != OperatorParallelism.STATELESS:
            return None

    return scan_id, middle_ids, segment.tail


def _clone_op(op):
    """A fresh, independent instance of a push operator — re-running __init__
    rebuilds its private state (compiled bytecode, a clean aggregate engine).
    BasePlanNode stores the original construction args, so this reproduces it."""
    return type(op)(properties=op.properties, **op.parameters)


def _build_clone_chain(plan, middle_ids, breaker, ctx):
    """Clone the [middle ops … breaker] chain, wired head→…→breaker_clone. The
    breaker clone has no downstream — workers only ingest into it (never EOS),
    so it never emits. Returns ``(chain_head, breaker_clone)``."""
    chain = [_clone_op(plan[nid]) for nid in middle_ids]
    breaker_clone = _clone_op(breaker)
    chain.append(breaker_clone)
    for index, op in enumerate(chain):
        op.set_context(ctx)
        if index + 1 < len(chain):
            op.set_downstream(chain[index + 1])
    return chain[0], breaker_clone


def execute(plan, head_node=None, telemetry=None):
    """Parallel engine entry point.

    Parallelises a single-scan grouped-aggregate pipeline; every other plan
    shape runs on the serial engine (strategy selection, recorded via the
    ``parallel_engaged`` telemetry reading — not a hidden fallback).
    """
    from opteryx.managers.execution import serial_engine

    target = _find_parallel_grouped_agg(plan)
    if target is None:
        if telemetry is not None:
            telemetry._reading["parallel_engaged"] = 0
        return serial_engine.execute(plan, head_node=head_node, telemetry=telemetry)

    if telemetry is not None:
        telemetry._reading["parallel_engaged"] = 1

    workers = resolve_worker_count(config.MAX_EXECUTION_WORKERS)
    if workers <= 1:
        if telemetry is not None:
            telemetry._reading["parallel_engaged"] = 0
        return serial_engine.execute(plan, head_node=head_node, telemetry=telemetry)

    scan_id, middle_ids, breaker_id = target

    # Strategy: shuffle (no merge, high-card) vs round-robin (+merge, low/med card).
    # Shuffle needs the group key materialised in the scan output (plain identifier
    # keys) so it can partition pre-aggregate. 'auto' (NDV-selected) lands in
    # Stage 2; until then the default is round-robin.
    strategy = config.PARALLEL_AGG_STRATEGY
    breaker = plan[breaker_id]
    if strategy == "shuffle" and _shuffle_eligible(breaker):
        return (
            _shuffle_agg_stream(plan, scan_id, middle_ids, breaker_id, workers),
            ResultType.TABULAR,
        )
    return (
        _grouped_agg_stream(plan, scan_id, middle_ids, breaker_id, workers),
        ResultType.TABULAR,
    )


def _pow2_floor(n: int) -> int:
    """Largest power of two <= n (>= 1)."""
    p = 1
    while (p << 1) <= n:
        p <<= 1
    return p


def _shuffle_eligible(breaker) -> bool:
    """Shuffle partitions rows pre-aggregate, so the group key must already exist
    in the scan output — i.e. plain identifier group keys, no computed keys and no
    per-row aggregate expression eval (`_needs_expression_eval`). Computed-key
    grouped aggs fall back to round-robin."""
    return bool(getattr(breaker, "group_by_columns", None)) and not breaker._needs_expression_eval


def _grouped_agg_stream(plan, scan_id, middle_ids, breaker_id, workers):
    """Drive the grouped-aggregate pipeline in parallel and yield result morsels.

    Mirrors serial_engine.stream()'s lifecycle: a try/finally that terminates the
    context and closes the scan source on every exit path.
    """
    from opteryx.compiled.thread_pool import CppThreadPool
    from opteryx.managers.execution.pipeline_compiler import compile_pipeline

    chains, exit_node, ctx = compile_pipeline(plan)
    scan = plan[scan_id]
    breaker = plan[breaker_id]

    def _drain():
        if exit_node is None:
            return
        while exit_node.has_pending():
            yield exit_node.pop_pending()

    pool = None
    try:
        # ── Phase 1: buffer up to the row-floor to decide serial vs parallel ──
        buffer = []
        buffered_rows = 0
        exhausted = False
        while buffered_rows < config.PARALLEL_MIN_ROWS:
            morsel = scan._next_morsel_py()
            if morsel is None:
                exhausted = True
                break
            buffer.append(morsel)
            buffered_rows += morsel.num_rows

        # ── Small input: run the buffer serially through the ORIGINAL chain ──
        if exhausted and buffered_rows < config.PARALLEL_MIN_ROWS:
            head = plan[middle_ids[0]] if middle_ids else breaker
            for morsel in buffer:
                if ctx.is_terminated():
                    break
                head.push(morsel)
                yield from _drain()
            if not ctx.is_terminated():
                head.push(_EOS_SENTINEL)
                yield from _drain()
            return

        # ── Large input: W cloned chains, one worker thread each ──
        clones = [_build_clone_chain(plan, middle_ids, breaker, ctx) for _ in range(workers)]
        queues = [_queue.Queue(maxsize=_QUEUE_DEPTH) for _ in range(workers)]
        errors: list = [None] * workers

        def worker(index):
            chain_head = clones[index][0]
            work_queue = queues[index]
            try:
                while True:
                    morsel = work_queue.get()
                    if morsel is _WORKER_DONE:
                        return
                    chain_head.push(morsel)
            except BaseException as exc:  # noqa: BLE001 — surface on the main thread
                errors[index] = exc
                # Keep draining so the main thread's put() can never block on a
                # dead worker; discard the rest of this partition.
                while work_queue.get() is not _WORKER_DONE:
                    pass

        pool = CppThreadPool(workers, "m4-grouped-agg")
        futures = [pool.submit(worker, k) for k in range(workers)]

        # Exclusive morsel ownership: each morsel goes to exactly one worker
        # (contract rule 1). A full queue blocks here → backpressure to the scan.
        round_robin = 0
        received = [0] * workers

        def dispatch(morsel):
            nonlocal round_robin
            target = round_robin % workers
            queues[target].put(morsel)
            received[target] += 1
            round_robin += 1

        for morsel in buffer:
            dispatch(morsel)
        if not exhausted:
            while True:
                if ctx.is_terminated():
                    break
                morsel = scan._next_morsel_py()
                if morsel is None:
                    break
                dispatch(morsel)

        for work_queue in queues:
            work_queue.put(_WORKER_DONE)
        for future in futures:
            future.result()  # join + propagate any worker exception
        for index, exc in enumerate(errors):
            if exc is not None:
                raise exc

        if ctx.is_terminated():
            return

        # ── Recombine: merge only the engines that actually ingested. A worker
        # that received no morsels has an EMPTY engine; merging an empty engine
        # corrupts the AVG finalizer (a latent merge() edge — see report), and
        # is pointless anyway. We merge into the first populated engine (never the
        # original's empty one — merge() is validated transplanting into a
        # non-empty target) and have the original breaker finalise it.
        populated = [k for k in range(workers) if received[k] > 0]
        if populated:
            base_engine = clones[populated[0]][1]._engine
            for k in populated[1:]:
                base_engine.merge(clones[k][1]._engine)
            breaker._engine = base_engine
        # else: no rows reached any worker → the original empty engine finalises
        # to zero groups, exactly as serial would.

        # Finalise via the original breaker → emits to exit and drives the
        # serial tail of the plan (any sort/limit downstream of the aggregate).
        breaker.push(_EOS_SENTINEL)
        yield from _drain()
    finally:
        if pool is not None:
            pool.shutdown(wait=True)
        ctx.terminate()
        scan.close_source()


def _shuffle_agg_stream(plan, scan_id, middle_ids, breaker_id, workers):
    """High-cardinality strategy: hash-partition rows by group key into B
    row-disjoint bins (one worker per bin), aggregate each bin independently, and
    concatenate — **NO merge**. Removes the round-robin merge() Amdahl term.

    B = the largest power of two <= workers (one bin per worker, no work-stealing
    yet — that is Stage-3). Each clone chain is identical to the round-robin path
    (filter → agg); only the dispatch (partition vs whole morsel) and the
    recombine (finalize-each vs merge) differ.
    """
    from opteryx.compiled.thread_pool import CppThreadPool
    from opteryx.managers.execution.pipeline_compiler import compile_pipeline
    from opteryx.operators.exchange import ExchangeNode

    chains, exit_node, ctx = compile_pipeline(plan)
    scan = plan[scan_id]
    breaker = plan[breaker_id]
    key_columns = list(breaker.group_by_columns)
    # The shuffle is an operator: it owns the partition logic, the scheduler owns
    # the threads (see docs/M4_CENTRAL_SCHEDULER_DESIGN.md §11).
    exchange = ExchangeNode(properties=breaker.properties, partition_columns=key_columns)

    # The operator after the aggregate (sort / projection / exit) — we push each
    # bin's finalized output into it, then a SINGLE EOS drives the serial tail.
    post_edges = list(plan.outgoing_edges(breaker_id))
    post = plan[post_edges[0][1]] if post_edges else None

    bins = _pow2_floor(workers)

    def _drain():
        if exit_node is None:
            return
        while exit_node.has_pending():
            yield exit_node.pop_pending()

    pool = None
    try:
        # ── Phase 1: buffer to the row-floor (same gate as round-robin) ──
        buffer = []
        buffered_rows = 0
        exhausted = False
        while buffered_rows < config.PARALLEL_MIN_ROWS:
            morsel = scan._next_morsel_py()
            if morsel is None:
                exhausted = True
                break
            buffer.append(morsel)
            buffered_rows += morsel.num_rows

        if (exhausted and buffered_rows < config.PARALLEL_MIN_ROWS) or bins < 2:
            head = plan[middle_ids[0]] if middle_ids else breaker
            for morsel in buffer:
                if ctx.is_terminated():
                    break
                head.push(morsel)
                yield from _drain()
            if not ctx.is_terminated():
                head.push(_EOS_SENTINEL)
                yield from _drain()
            return

        # ── B cloned chains (filter → agg), one worker thread per bin ──
        clones = [_build_clone_chain(plan, middle_ids, breaker, ctx) for _ in range(bins)]
        queues = [_queue.Queue(maxsize=_QUEUE_DEPTH) for _ in range(bins)]
        errors: list = [None] * bins
        received = [0] * bins

        def worker(index):
            chain_head = clones[index][0]
            work_queue = queues[index]
            try:
                while True:
                    sub = work_queue.get()
                    if sub is _WORKER_DONE:
                        return
                    chain_head.push(sub)
            except BaseException as exc:  # noqa: BLE001
                errors[index] = exc
                while work_queue.get() is not _WORKER_DONE:
                    pass

        pool = CppThreadPool(bins, "m4-shuffle-agg")
        futures = [pool.submit(worker, b) for b in range(bins)]

        def dispatch(morsel):
            # Hash-partition the raw morsel by key into `bins` disjoint sub-morsels
            # via the Exchange operator; each sub-morsel is exclusive to one worker
            # (contract rule 1).
            subs = exchange.partition(morsel, bins)
            for b in range(bins):
                sub = subs[b]
                if sub.num_rows > 0:
                    queues[b].put(sub)
                    received[b] += 1

        for morsel in buffer:
            dispatch(morsel)
        if not exhausted:
            while True:
                if ctx.is_terminated():
                    break
                morsel = scan._next_morsel_py()
                if morsel is None:
                    break
                dispatch(morsel)

        for work_queue in queues:
            work_queue.put(_WORKER_DONE)
        for future in futures:
            future.result()
        for index, exc in enumerate(errors):
            if exc is not None:
                raise exc

        if ctx.is_terminated():
            return

        # ── Recombine = NO merge. Each bin owns disjoint keys; finalize each
        # populated clone and push its groups into the post-aggregate operator,
        # then a SINGLE EOS drives the serial tail (sort/limit/exit). ──
        if post is None:
            return
        for index in range(bins):
            if received[index] == 0:
                continue
            for chunk in clones[index][1]._finalize():
                post.push(chunk)
                yield from _drain()
        post.push(_EOS_SENTINEL)
        yield from _drain()
    finally:
        if pool is not None:
            pool.shutdown(wait=True)
        ctx.terminate()
        scan.close_source()
