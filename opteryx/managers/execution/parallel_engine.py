# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Parallel execution engine — central scheduler (M4).

The engine owns parallelism; the OPERATORS stay parallel-unaware (the founding
principle). It parallelises only the shapes where extra cores actually pay —
those with NO cross-worker recombination tax:

  * **Standalone selection / projection** (``scan → {filter,projection}* → exit``,
    no breaker): each morsel is an independent transform, so workers self-pull and
    STREAM their finished morsels out — no merge, no barrier. ``_stateless_stream``.
  * **Ungrouped aggregate** (``scan → stateless* → UngroupedAggregate``): no group
    key, so recombination is a trivial SCALAR merge. ``_ungrouped_agg_stream``.
  * **Grouped aggregate** (``scan → stateless* → GroupedAggregate``): recombination
    is a per-group merge (key probe + state transplant). Previously this REGRESSED
    because the merge was GIL-bound (serial-merge Amdahl wall). Under free-threaded
    CPython the merge runs GIL-free (``GroupHashEngine.merge`` over per-worker
    clones, measured 6.4×–7.2× vs serial), so GROUP BY is parallelised again.
    ``_grouped_agg_stream``. The grouped engine resolves collectors lazily, so
    mergeability is decided at RUNTIME on the first morsel (not at plan time): a
    non-mergeable aggregate (COUNT DISTINCT / MEDIAN / decimal / string / bool /
    interval MIN-MAX) finishes serially through the breaker (capability routing,
    not a perf gate).

Shared infrastructure: ``resolve_worker_count`` (cap 8), ``identify_segments``
(cut the DAG into pipeline segments), ``pull_one`` (the concurrent-safe scan
entry, in _operators.pyx), and a row-floor (tiny inputs run serial). The
remaining ceiling on the parallel shapes is the still-GIL scan pull (wide-column
decode) and string-expression eval; both are real levers, not gates.
"""

import os
from dataclasses import dataclass
from typing import List
from typing import Optional
from typing import Tuple

from opteryx import EOS as _EOS_SENTINEL
from opteryx import config
from opteryx.constants import ResultType
from opteryx.operators._operators import push_one
from opteryx.operators.catalog import OperatorParallelism
from opteryx.operators.catalog import get_registry

# Hard cap on parallel width: past 8 workers every prototype regressed (the
# merge/recombination tail and DRAM bandwidth dominate). See
# docs/M4_PARALLEL_AGG_PROTOTYPE.md.
_MAX_WORKER_CAP = 8


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


# NOTE: GROUP BY parallelism was previously removed because the cross-worker
# recombination (a per-group merge) was GIL-bound — serial merge was an Amdahl
# wall that exceeded the keying gain. Under free-threaded CPython that wall is
# gone: the per-group merge runs GIL-free (GroupHashEngine.merge over per-worker
# clones), measured 6.4×–7.2× vs serial. So GROUP BY is parallelised again — same
# clone-per-worker / self-pull / recombine shape as the ungrouped path, just with
# the per-group merge() instead of a scalar merge.


def _find_parallel_grouped_agg(plan):
    """Return ``(scan_id, middle_ids, breaker_id)`` for a single-scan
    ``scan → stateless* → grouped-aggregate`` pipeline (a candidate by SHAPE),
    else ``None``.

    Unlike the ungrouped case the recombination is a per-group merge (key probe +
    state transplant), not a scalar merge — but under free-threading that merge is
    GIL-free, so it no longer dominates.

    This finder gates on SHAPE only. It does NOT consult ``engine.is_mergeable()``:
    the grouped engine resolves its collectors lazily (deferred until the first
    morsel reveals the column type), so mergeability is unknowable at plan time and
    is always False here. The mergeable-vs-serial decision is made at runtime in
    ``_grouped_agg_stream`` once the first morsel resolves the collectors —
    correctness routing by capability, NOT a perf gate.
    """
    registry = get_registry()
    scans = [nid for nid in plan.nodes() if getattr(plan[nid], "is_scan", False)]
    if len(scans) != 1:
        return None
    scan_id = scans[0]
    segment = next((s for s in identify_segments(plan) if s.nodes[0] == scan_id), None)
    if segment is None or not segment.tail_is_breaker:
        return None
    breaker = plan[segment.tail]
    if breaker.__class__.__name__ != "GroupedAggregateHashedNode":
        return None
    if getattr(breaker, "_engine", None) is None:
        return None
    middle_ids = list(segment.nodes[1:-1])
    for nid in middle_ids:
        meta = registry.get(type(plan[nid]))
        if meta is None or meta.parallelism != OperatorParallelism.STATELESS:
            return None
    return scan_id, middle_ids, segment.tail


def _find_parallel_ungrouped_agg(plan):
    """Return ``(scan_id, middle_ids, breaker_id)`` for a single-scan
    ``scan → stateless* → ungrouped-aggregate`` pipeline, else ``None``.

    Ungrouped agg has NO group key, so recombination is a trivial SCALAR merge
    (sum/min/max the W partials) — no key exchange, no row copy, no keying tax.
    This is the embarrassingly-parallel case: the parallel work is the (often
    non-pushable, CPU-bound) filter/projection + scalar accumulate.
    """
    registry = get_registry()
    scans = [nid for nid in plan.nodes() if getattr(plan[nid], "is_scan", False)]
    if len(scans) != 1:
        return None
    scan_id = scans[0]
    segment = next((s for s in identify_segments(plan) if s.nodes[0] == scan_id), None)
    if segment is None or not segment.tail_is_breaker:
        return None
    breaker = plan[segment.tail]
    if breaker.__class__.__name__ != "UngroupedAggregateNode":
        return None
    # Require a mergeable engine and NO literal-only aggregates: literal state
    # lives outside `_engine`, so merging only the engine would drop it (a
    # capability boundary — literal-agg parallelism is unbuilt, not gated-off).
    engine = getattr(breaker, "_engine", None)
    if engine is None or not engine.is_mergeable() or breaker._has_literals:
        return None
    middle_ids = list(segment.nodes[1:-1])
    for nid in middle_ids:
        meta = registry.get(type(plan[nid]))
        if meta is None or meta.parallelism != OperatorParallelism.STATELESS:
            return None
    return scan_id, middle_ids, segment.tail


def _find_parallel_stateless(plan):
    """Return ``(scan_id, op_ids, exit_id)`` for a single-scan
    ``scan → {filter,projection}* → exit`` pipeline with NO breaker (no aggregate,
    sort, limit, join, union), else ``None``.

    The embarrassingly-parallel case: W workers each filter/project their OWN
    morsels; the outputs are simply CONCATENATED — no merge, no key, no copy, no
    recombination tax. There must be at least one stateless op (otherwise it's a
    bare scan → exit with no compute worth parallelising).
    """
    registry = get_registry()
    scans = [nid for nid in plan.nodes() if getattr(plan[nid], "is_scan", False)]
    if len(scans) != 1:
        return None
    scan_id = scans[0]
    segment = next((s for s in identify_segments(plan) if s.nodes[0] == scan_id), None)
    # Tail must be the SINK, not a breaker — i.e. no aggregate/sort/etc. The whole
    # plan is the single linear scan→stateless*→exit chain.
    if segment is None or segment.tail_is_breaker:
        return None
    op_ids = list(segment.nodes[1:-1])
    if not op_ids:
        return None
    for nid in op_ids:
        meta = registry.get(type(plan[nid]))
        if meta is None or meta.parallelism != OperatorParallelism.STATELESS:
            return None  # a Limit/Window (stateful) middle → not eligible
    return scan_id, op_ids, segment.tail


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

    Parallelises the shapes where the cores actually pay — standalone
    selection/projection and ungrouped aggregates (no cross-worker recombination
    tax). GROUP BY runs serial (its recombination tax outweighs the gain — see the
    note above ``_find_parallel_ungrouped_agg``). Everything else runs serial too;
    this is strategy selection, recorded via ``parallel_engaged`` telemetry.
    """
    from opteryx.managers.execution import serial_engine

    ungrouped = _find_parallel_ungrouped_agg(plan)
    grouped = None if ungrouped is not None else _find_parallel_grouped_agg(plan)
    stateless = (
        None if (ungrouped is not None or grouped is not None) else _find_parallel_stateless(plan)
    )
    if ungrouped is None and grouped is None and stateless is None:
        if telemetry is not None:
            telemetry._reading["parallel_engaged"] = 0
        return serial_engine.execute(plan, head_node=head_node, telemetry=telemetry)

    workers = resolve_worker_count(config.MAX_EXECUTION_WORKERS)
    if workers <= 1:
        if telemetry is not None:
            telemetry._reading["parallel_engaged"] = 0
        return serial_engine.execute(plan, head_node=head_node, telemetry=telemetry)

    if telemetry is not None:
        telemetry._reading["parallel_engaged"] = 1

    if ungrouped is not None:
        scan_id, middle_ids, breaker_id = ungrouped
        return (
            _ungrouped_agg_stream(plan, scan_id, middle_ids, breaker_id, workers, telemetry),
            ResultType.TABULAR,
        )
    if grouped is not None:
        scan_id, middle_ids, breaker_id = grouped
        return (
            _grouped_agg_stream(plan, scan_id, middle_ids, breaker_id, workers, telemetry),
            ResultType.TABULAR,
        )
    scan_id, op_ids, exit_id = stateless
    return (
        _stateless_stream(plan, scan_id, op_ids, exit_id, workers, telemetry),
        ResultType.TABULAR,
    )


def _ungrouped_agg_stream(plan, scan_id, middle_ids, breaker_id, workers, telemetry=None):
    """Parallel UNGROUPED aggregate (no GROUP BY). W workers SELF-PULL and
    aggregate their OWN morsels into private plain engines; recombination is a
    trivial SCALAR merge (sum/min/max the W partials) — no key, no exchange, no
    row copy. The operator stays parallel-unaware. The parallel work is the
    (often non-pushable, CPU-bound) filter/projection + scalar accumulate; this is
    the embarrassingly-parallel case, the opposite of grouped-agg's keying tax.
    """
    import threading

    from opteryx.compiled.thread_pool import CppThreadPool
    from opteryx.managers.execution.pipeline_compiler import compile_pipeline
    from opteryx.operators._operators import pull_one

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
        # Row-floor: tiny inputs run serially (clone + thread setup dominate).
        buffer = []
        buffered_rows = 0
        exhausted = False
        while buffered_rows < config.PARALLEL_MIN_ROWS:
            morsel = pull_one(scan)
            if morsel is None:
                exhausted = True
                break
            buffer.append(morsel)
            buffered_rows += morsel.num_rows

        if exhausted and buffered_rows < config.PARALLEL_MIN_ROWS:
            head = plan[middle_ids[0]] if middle_ids else breaker
            for morsel in buffer:
                if ctx.is_terminated():
                    break
                push_one(head, morsel)
                yield from _drain()
            if not ctx.is_terminated():
                push_one(head, _EOS_SENTINEL)
                yield from _drain()
            return

        clones = [_build_clone_chain(plan, middle_ids, breaker, ctx) for _ in range(workers)]
        errors = [None] * workers
        ingested = [0] * workers
        buf_iter = iter(buffer)
        pull_lock = threading.Lock()

        def next_input():
            with pull_lock:
                if ctx.is_terminated():
                    return None
                buffered = next(buf_iter, None)
                if buffered is not None:
                    return buffered
                return pull_one(scan)

        def worker(index):
            chain_head = clones[index][0]
            count = 0
            try:
                while True:
                    morsel = next_input()
                    if morsel is None:
                        break
                    push_one(chain_head, morsel)
                    count += 1
            except BaseException as exc:  # noqa: BLE001 — surface on the main thread
                errors[index] = exc
            ingested[index] = count

        pool = CppThreadPool(workers, "m4-ungrouped-agg")
        futures = [pool.submit(worker, k) for k in range(workers)]
        for future in futures:
            future.result()
        for exc in errors:
            if exc is not None:
                raise exc
        if ctx.is_terminated():
            return

        # Scalar merge of the W partial engines into the first populated one, then
        # finalise via the original breaker (drives the serial tail + EOS).
        populated = [k for k in range(workers) if ingested[k] > 0]
        if populated:
            base_engine = clones[populated[0]][1]._engine
            for k in populated[1:]:
                base_engine.merge(clones[k][1]._engine)
            breaker._engine = base_engine
        push_one(breaker, _EOS_SENTINEL)
        yield from _drain()
    finally:
        if pool is not None:
            pool.shutdown(wait=True)
        ctx.terminate()
        scan.close_source()


def _grouped_agg_stream(plan, scan_id, middle_ids, breaker_id, workers, telemetry=None):
    """Parallel GROUPED aggregate (GROUP BY). W workers SELF-PULL morsels and
    ingest each into their OWN private cloned engine (keying + accumulate is
    GIL-free per-engine), then the W partials are combined into one by the
    existing per-group merge (key probe + state transplant). Under free-threading
    that merge is GIL-free (GroupHashEngine.merge over disjoint partitions), so the
    recombination tax that previously sank this shape no longer dominates.

    The operator stays parallel-unaware: the ENGINE clones it per worker, routes
    morsels, and recombines. Result is identical to the serial path for every
    group-key shape merge() supports (single-, multi- and zero-column; null and
    bool keys) and every mergeable aggregate.

    **Mergeability is decided at RUNTIME, not at plan time.** The grouped engine
    uses *deferred* collectors that only resolve to a concrete type (int / float /
    decimal / string / …) on the first morsel, so ``engine.is_mergeable()`` is
    unknowable before any data flows — the finder can only gate on *shape*. So we
    ingest the first buffered morsel into the ORIGINAL breaker (single-threaded,
    through the real middle-op chain) to RESOLVE its collectors, then check
    ``is_mergeable()``:

      * not mergeable (COUNT DISTINCT / MEDIAN / decimal / string / bool / interval
        MIN-MAX) → finish the whole query SERIALLY through the breaker (correctness
        routing by capability — the breaker already holds morsel 0);
      * mergeable → fan W workers over the REMAINING morsels into per-worker engine
        clones, then per-group ``merge()`` each worker engine INTO the breaker's
        engine (which already holds morsel 0's groups) — no work discarded.
    """
    import threading

    from opteryx.compiled.thread_pool import CppThreadPool
    from opteryx.managers.execution.pipeline_compiler import compile_pipeline
    from opteryx.operators._operators import pull_one

    chains, exit_node, ctx = compile_pipeline(plan)
    scan = plan[scan_id]
    breaker = plan[breaker_id]
    head = plan[middle_ids[0]] if middle_ids else breaker

    def _drain():
        if exit_node is None:
            return
        while exit_node.has_pending():
            yield exit_node.pop_pending()

    def _finish_serial(remaining):
        """Drive `remaining` morsels through the original breaker, then EOS."""
        for m in remaining:
            if ctx.is_terminated():
                return
            push_one(head, m)
            yield from _drain()
        while True:
            if ctx.is_terminated():
                return
            m = pull_one(scan)
            if m is None:
                break
            push_one(head, m)
            yield from _drain()
        if not ctx.is_terminated():
            push_one(head, _EOS_SENTINEL)
            yield from _drain()

    pool = None
    try:
        # Row-floor: tiny inputs run serially (clone + thread setup dominate).
        buffer = []
        buffered_rows = 0
        exhausted = False
        while buffered_rows < config.PARALLEL_MIN_ROWS:
            morsel = pull_one(scan)
            if morsel is None:
                exhausted = True
                break
            buffer.append(morsel)
            buffered_rows += morsel.num_rows

        if exhausted and buffered_rows < config.PARALLEL_MIN_ROWS:
            yield from _finish_serial(buffer)
            return

        # ---- Resolve mergeability on the first morsel (single-threaded) -------
        # Push morsel 0 through the real middle-op chain into the ORIGINAL breaker
        # so its deferred collectors resolve to concrete typed ones. NO EOS yet, so
        # the breaker does not finalize/emit. This makes is_mergeable() answerable.
        if buffer:
            push_one(head, buffer[0])
        if not breaker._engine.is_mergeable():
            # Capability routing: a non-mergeable aggregate cannot be partitioned.
            # The breaker already holds morsel 0; finish the rest serially.
            yield from _finish_serial(buffer[1:])
            return

        # ---- Mergeable → parallel fan-out over the REMAINING morsels ----------
        clones = [_build_clone_chain(plan, middle_ids, breaker, ctx) for _ in range(workers)]
        errors = [None] * workers
        ingested = [0] * workers
        buf_iter = iter(buffer[1:])
        pull_lock = threading.Lock()

        def next_input():
            with pull_lock:
                if ctx.is_terminated():
                    return None
                buffered = next(buf_iter, None)
                if buffered is not None:
                    return buffered
                return pull_one(scan)

        def worker(index):
            chain_head = clones[index][0]
            count = 0
            try:
                while True:
                    morsel = next_input()
                    if morsel is None:
                        break
                    push_one(chain_head, morsel)
                    count += 1
            except BaseException as exc:  # noqa: BLE001 — surface on the main thread
                errors[index] = exc
            ingested[index] = count

        pool = CppThreadPool(workers, "m4-grouped-agg")
        futures = [pool.submit(worker, k) for k in range(workers)]
        for future in futures:
            future.result()
        for exc in errors:
            if exc is not None:
                raise exc
        if ctx.is_terminated():
            return

        # Per-group merge of the W worker engines INTO the breaker's engine (which
        # already holds morsel 0's groups), then finalise via the breaker (drives
        # the serial tail + EOS). The breaker keeps its node-level finalize state
        # (implicit COUNT(*), HAVING, aggregation specs).
        base_engine = breaker._engine
        for k in range(workers):
            if ingested[k] > 0:
                base_engine.merge(clones[k][1]._engine)
        push_one(breaker, _EOS_SENTINEL)
        yield from _drain()
    finally:
        if pool is not None:
            pool.shutdown(wait=True)
        ctx.terminate()
        scan.close_source()


def _stateless_stream(plan, scan_id, op_ids, exit_id, workers, telemetry=None):
    """Parallel SELECTION/PROJECTION (no aggregate). Filter and projection are
    per-morsel INDEPENDENT transforms — there is NO recombination/merge and no
    barrier. Each worker SELF-PULLS, runs its morsel through its OWN cloned chain
    (stateless ops + a cloned Exit doing the final select/rename), and STREAMS the
    finished morsel straight to a shared queue. The main generator yields morsels
    as they arrive — workers produce while the caller consumes (true streaming,
    bounded by nothing but the caller's pace). Operators stay parallel-unaware.
    """
    import queue as _queue
    import threading

    from opteryx.compiled.thread_pool import CppThreadPool
    from opteryx.managers.execution.pipeline_compiler import compile_pipeline
    from opteryx.operators._operators import pull_one

    chains, exit_node, ctx = compile_pipeline(plan)
    scan = plan[scan_id]

    pool = None
    try:
        # Row-floor: tiny inputs run serially through the original chain.
        buffer = []
        buffered_rows = 0
        exhausted = False
        while buffered_rows < config.PARALLEL_MIN_ROWS:
            morsel = pull_one(scan)
            if morsel is None:
                exhausted = True
                break
            buffer.append(morsel)
            buffered_rows += morsel.num_rows

        if exhausted and buffered_rows < config.PARALLEL_MIN_ROWS:
            head = plan[op_ids[0]]
            for morsel in buffer:
                if ctx.is_terminated():
                    break
                push_one(head, morsel)
                while exit_node.has_pending():
                    yield exit_node.pop_pending()
            if not ctx.is_terminated():
                push_one(head, _EOS_SENTINEL)
                while exit_node.has_pending():
                    yield exit_node.pop_pending()
            return

        # Each worker: clone the stateless ops + a clone of the Exit (which applies
        # the final select/rename into its own _pending). After each pushed morsel
        # the worker drains its Exit-clone's pending straight to the shared queue —
        # no collect, no barrier. (Unbounded queue: the caller drives consumption;
        # workers never block on put, so abandonment can't deadlock the join.)
        out_q = _queue.Queue()
        DONE = object()
        errors = [None] * workers
        buf_iter = iter(buffer)
        pull_lock = threading.Lock()

        def next_input():
            with pull_lock:
                if ctx.is_terminated():
                    return None
                buffered = next(buf_iter, None)
                if buffered is not None:
                    return buffered
                return pull_one(scan)

        def worker(index):
            ops = [_clone_op(plan[nid]) for nid in op_ids]
            exit_clone = _clone_op(plan[exit_id])
            chain = ops + [exit_clone]
            for i, op in enumerate(chain):
                op.set_context(ctx)
                if i + 1 < len(chain):
                    op.set_downstream(chain[i + 1])
            head = chain[0]
            try:
                while True:
                    morsel = next_input()
                    if morsel is None:
                        break
                    push_one(head, morsel)
                    while exit_clone.has_pending():
                        out_q.put(exit_clone.pop_pending())
            except BaseException as exc:  # noqa: BLE001 — surface on the main thread
                errors[index] = exc
            finally:
                out_q.put(DONE)

        pool = CppThreadPool(workers, "m4-stateless")
        futures = [pool.submit(worker, k) for k in range(workers)]

        # Stream: yield morsels as workers produce them, until all W signal DONE.
        done = 0
        yielded = False
        while done < workers:
            item = out_q.get()
            if item is DONE:
                done += 1
                continue
            yielded = True
            yield item

        for future in futures:
            future.result()
        for exc in errors:
            if exc is not None:
                raise exc

        # Empty result still needs the schema morsel the serial Exit emits on EOS.
        if not yielded and not ctx.is_terminated():
            push_one(exit_node, _EOS_SENTINEL)
            while exit_node.has_pending():
                yield exit_node.pop_pending()
    finally:
        if pool is not None:
            pool.shutdown(wait=True)
        ctx.terminate()
        scan.close_source()
