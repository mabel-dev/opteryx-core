# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Parallel execution engine — central scheduler (M4).

The engine owns parallelism; the OPERATORS stay parallel-unaware (the founding
principle). It parallelises the shapes where extra cores actually pay, each with
NO cross-worker recombination tax:

  * **Grouped aggregate** (``scan → stateless* → GroupedAggregateHashed``): a serial
    producer row-routes each prepared morsel into ``W`` bins by ``hash(key) % W`` so
    every occurrence of a key lands on ONE worker; each worker keys its own DISJOINT
    bin and finalize is a CONCAT, never a merge. Because each group is seen whole by
    one worker this parallelises HOLISTIC aggregates too (MEDIAN / COUNT(DISTINCT)).
    ``_grouped_agg_stream``.
  * **DISTINCT** (``scan → stateless* → Distinct``): the SAME row-routing, run as a
    two-phase parallel shuffle (scatter, then per-partition dedup). Engages only at
    ``W >= 2`` (serial dedup is fine at W=1). ``_distinct_stream``.
  * **Ungrouped aggregate** (``scan → stateless* → UngroupedAggregate``): no group
    key, so recombination is a trivial SCALAR merge. ``_ungrouped_agg_stream``.
  * **Standalone selection / projection** (``scan → {filter,projection}* → exit``,
    no breaker): each morsel is an independent transform, so workers self-pull and
    STREAM their finished morsels out — no merge, no barrier. ``_stateless_stream``.

Any plan no strategy matches (joins, set ops, window, sort, limit-only) is driven
SERIAL-INLINE here (``_serial_stream``) — the data executor owns it end-to-end; it
never punts to a hidden fallback in another engine.

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


def resolve_worker_count(requested) -> int:
    """Resolve the effective worker count for a query.

    The worker count is SOFTCODED — derived by the software from the core count,
    ``max(1, min(cpu - 2, _MAX_WORKER_CAP))``: 2 cores reserved for the C++
    IO/decode pool + the main orchestration thread, never above the measured
    regression boundary. The softcoded value is used when ``requested``
    (``config.MAX_EXECUTION_WORKERS``) is unset / ``"auto"`` / an impossible value
    (``None`` or ``<= 0``). An explicit positive request is honoured, still clamped
    to the softcoded cap.

    Worker count is degree-of-parallelism ONLY — it never selects a code path; an
    explicit ``1`` is a single worker, not "the serial engine".
    """
    cpu = os.cpu_count() or 1
    # Leave 2 cores for IO/decode + the main orchestration thread.
    softcoded = max(1, min(cpu - 2, _MAX_WORKER_CAP))
    if requested is None or requested <= 0:
        return softcoded
    return max(1, min(requested, cpu - 2, _MAX_WORKER_CAP))


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


def _find_parallel_grouped_agg(plan):
    """Return ``(scan_id, middle_ids, breaker_id)`` for a single-scan
    ``scan → stateless* → GroupedAggregateHashedNode`` pipeline, else ``None``.

    Row-routing parallelism (M4): the producer routes each (prepared) morsel by
    ``hash(group-key) % W`` so every occurrence of a key lands on ONE worker —
    each worker then owns a DISJOINT key slice and keys it independently, and
    finalize is a CONCAT, never a merge. Because each group is seen whole by one
    worker, this parallelises HOLISTIC aggregates too (MEDIAN / COUNT(DISTINCT) /
    PERCENTILE) — there is no ``is_mergeable`` gate, unlike the ungrouped path.
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
    # A grouped breaker with NO group columns is degenerate — there is no key to
    # route on, so row-routing is inapplicable; leave it to the serial engine.
    # (A binder quirk can produce this, e.g. GROUP BY a quoted reserved word
    # collapsing to zero groups — a separate, pre-existing correctness bug.)
    if not breaker.group_by_columns:
        return None
    middle_ids = list(segment.nodes[1:-1])
    for nid in middle_ids:
        meta = registry.get(type(plan[nid]))
        if meta is None or meta.parallelism != OperatorParallelism.STATELESS:
            return None
    return scan_id, middle_ids, segment.tail


def _find_parallel_distinct(plan):
    """Return ``(scan_id, middle_ids, breaker_id, exit_id)`` for a single-scan
    ``scan → stateless* → DistinctNode`` pipeline, else ``None``.

    DISTINCT parallelises by the SAME row-routing as grouped-agg: the producer
    routes each (already projected-to-key) morsel by ``hash(dedup-key) % W`` so
    every copy of a value lands on ONE worker. Each worker then dedups its OWN
    disjoint slice into a private set — no cross-worker merge, finalize is a
    CONCAT. The dedup key is the FULL set of the distinct input's columns
    (``_distinct_on`` is None for plain ``DISTINCT`` — upstream projection has
    already narrowed the morsel to the selected columns), so routing on all
    columns matches the dedup exactly.
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
    if breaker.__class__.__name__ != "DistinctNode":
        return None
    middle_ids = list(segment.nodes[1:-1])
    for nid in middle_ids:
        meta = registry.get(type(plan[nid]))
        if meta is None or meta.parallelism != OperatorParallelism.STATELESS:
            return None
    # The deduped output is driven through the DistinctNode's real downstream
    # (the next operator after it — Exit, or a sort/projection/limit on top),
    # so any post-DISTINCT operators still run.
    outs = list(plan.outgoing_edges(segment.tail))
    if not outs:
        return None
    return scan_id, middle_ids, segment.tail, outs[0][1]


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


def _serial_stream(plan):
    """Serial-inline drive of a data pipeline that none of the parallel strategies
    cover. Compiles the full push pipeline and drives each scan's chain through
    ``drive_scan`` (the canonical full-pipeline driver — GIL released inside the
    chain, EOS pushed per scan, exit-pending drained, source closed in its own
    finally). Multi-scan plans (joins, set ops) are driven in DFS scan order
    (build legs before probe), matching the push topology compile_pipeline wires."""
    from opteryx.managers.execution.pipeline_compiler import compile_pipeline
    from opteryx.operators._operators import drive_scan

    chains, exit_node, ctx = compile_pipeline(plan)
    try:
        for scan, chain_head in chains:
            yield from drive_scan(scan, chain_head, exit_node, ctx)
            if ctx.is_terminated():
                break
    finally:
        ctx.terminate()


def execute(plan, head_node=None, telemetry=None):
    """The data executor (M4) — parallel where a strategy fits, serial-inline where
    not.

    Parallelises the shapes where extra cores pay (row-routing GROUP BY, ungrouped
    aggregate, standalone selection/projection). A data pipeline with NO parallel
    strategy is driven SERIAL-INLINE here (``_serial_stream``) — the data executor
    owns it end-to-end; it does not punt to a hidden fallback in another engine.
    Non-pipeline special ops (EXPLAIN/INSERT/DDL) never reach here — the dispatcher
    routes them to serial_engine. ``parallel_engaged`` records the path (1 when a
    parallel strategy ran, 0 when the serial-inline path drove the plan).
    """
    # GROUP BY parallelises by ROW-ROUTING (disjoint key bins, no merge) — the only
    # grouped strategy (round-robin + per-group merge was eliminated). Engages at
    # any W >= 1 (W=1 = one worker keying one bin, the same path as W=8).
    grouped = _find_parallel_grouped_agg(plan)
    ungrouped = None if grouped is not None else _find_parallel_ungrouped_agg(plan)
    distinct = (
        None
        if (grouped is not None or ungrouped is not None)
        else _find_parallel_distinct(plan)
    )
    stateless = (
        None
        if (grouped is not None or ungrouped is not None or distinct is not None)
        else _find_parallel_stateless(plan)
    )

    workers = resolve_worker_count(config.MAX_EXECUTION_WORKERS)

    # DISTINCT row-routing only pays above one worker — the serial dedup is fine,
    # and scatter+thread setup is pure overhead at W=1. Below that, drop DISTINCT
    # back to the serial-inline path (treat it as no parallel strategy).
    if distinct is not None and workers < 2:
        distinct = None

    if grouped is None and ungrouped is None and distinct is None and stateless is None:
        # No PARALLEL strategy for this shape (joins, set ops, window, sort,
        # limit-only, subqueries, bare/projection-only scans, DISTINCT at W=1) —
        # drive it SERIAL-INLINE. This is the "serial-driven inline where it cannot
        # [parallelise]" half of the data executor's contract: the data executor
        # still owns execution end-to-end (this is not a punt to a hidden fallback
        # in another engine — serial_engine handles ONLY non-pipeline special ops).
        if telemetry is not None:
            telemetry._reading["parallel_engaged"] = 0
        return _serial_stream(plan), ResultType.TABULAR

    # Grouped row-routing engages at any W >= 1 (darkness is the strategy flag).
    if grouped is not None:
        if telemetry is not None:
            telemetry._reading["parallel_engaged"] = 1
        scan_id, middle_ids, breaker_id = grouped
        # M4 Stage 2: route-on-abandon (parallel sink) when enabled; else row-routing.
        agg_stream = _grouped_agg_route if config.M4_ROUTE_AGG else _grouped_agg_stream
        return (
            agg_stream(plan, scan_id, middle_ids, breaker_id, workers, telemetry),
            ResultType.TABULAR,
        )

    if telemetry is not None:
        telemetry._reading["parallel_engaged"] = 1

    if distinct is not None:
        scan_id, middle_ids, breaker_id, exit_id = distinct
        return (
            _distinct_stream(plan, scan_id, middle_ids, breaker_id, exit_id, workers, telemetry),
            ResultType.TABULAR,
        )

    if ungrouped is not None:
        scan_id, middle_ids, breaker_id = ungrouped
        return (
            _ungrouped_agg_stream(plan, scan_id, middle_ids, breaker_id, workers, telemetry),
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


class _ScatterCollectEngine:
    """Stand-in for the breaker's ``GroupHashEngine`` during the SERIAL producer
    pass of row-routing GROUP BY. The breaker's ``_push_impl`` does
    ``prepare → select → engine.ingest`` exactly as always — but with this object
    in the ``_engine`` seam, ``ingest`` does NOT key: it row-routes each prepared
    morsel into ``W`` per-worker bins by ``hash(group-key) % W``. Keying happens
    later, in parallel, when the real per-worker engines ingest these bins. The
    operator stays parallel-unaware (it just calls ``engine.ingest``); this mirrors
    the ungrouped path swapping ``breaker._engine`` for a recombined engine.

    Serial scatter + parallel keying is the v1 (2a) shape. Parallelising the
    scatter across producers is a later optimisation (the materialisation tail).
    """

    __slots__ = ("_workers", "_key_resolver", "bins", "_positions")

    def __init__(self, workers, key_resolver):
        self._workers = workers
        # A real per-worker GroupHashEngine, used only for its pure
        # `group_col_positions` resolver (the SAME key columns the workers key on).
        self._key_resolver = key_resolver
        self.bins = [[] for _ in range(workers)]  # bins[k] = list[Morsel]
        self._positions = None

    def ingest(self, morsel):
        if morsel.num_rows == 0:
            return
        if self._positions is None:
            self._positions = self._key_resolver.group_col_positions(morsel)
            if self._positions is None:
                # Fail loud, not silently serial: a grouped plan that engaged
                # row-routing must have resolvable key columns on its morsels. None
                # here means the key columns are absent — an internal invariant break.
                raise RuntimeError(
                    "row-routing scatter: group-key columns not resolvable on a "
                    "Cxx-backed morsel"
                )
        from draken.morsels.morsel import Morsel

        sub = morsel._get_cxx().scatter(self._positions, self._workers)
        bins = self.bins
        for k in range(self._workers):
            bins[k].append(Morsel.from_cxx(sub[k]))


class _DistinctCollector:
    """Producer-side seam swapped into ``DistinctNode._scatter_engine``: instead of
    deduping, it just GATHERS the post-middle-op morsels (after projection/filter)
    into a flat list. The expensive work — scatter and dedup — is then done in two
    parallel phases by ``_distinct_stream`` (a parallel shuffle), so no full pass
    over the data runs serially."""

    __slots__ = ("morsels",)

    def __init__(self):
        self.morsels = []

    def ingest(self, morsel):
        if morsel.num_rows:
            self.morsels.append(morsel)


def _resolve_distinct_positions(morsel, distinct_on):
    """Resolve the DISTINCT dedup-key columns to scatter positions on `morsel`.
    `distinct_on` is None for plain DISTINCT (route on ALL columns — upstream
    projection already narrowed the morsel to the selected columns), else the
    explicit DISTINCT ON column identities. Returns a list[int], or None if a key
    column is absent. Pure Python via the CxxMorsel `names()` / `num_columns`
    accessors — no operator change beyond the one-line seam."""
    cxx = morsel._get_cxx()
    if distinct_on is None:
        return list(range(int(cxx.num_columns)))
    names = cxx.names()  # list[bytes], one per column
    positions = []
    for ident in distinct_on:
        key = ident.encode("utf-8") if isinstance(ident, str) else ident
        if key not in names:
            return None
        positions.append(names.index(key))
    return positions


def _distinct_stream(plan, scan_id, middle_ids, breaker_id, exit_id, workers, telemetry=None):
    """Parallel DISTINCT by a PARALLEL SHUFFLE.

    The naive row-routing shape (serial scatter → parallel dedup) loses: the
    serial scatter is a full hash+copy pass that costs ~as much as the dedup it
    parallelises. Here BOTH halves run in parallel:

      * Collect: a serial pass pulls the scan and runs the (cheap, often
        scan-pushed) stateless middle ops, gathering post-middle morsels.
      * Phase A (parallel shuffle): each worker scatters its OWN stripe of the
        collected morsels into ``W`` dest bins by ``hash(dedup-key) % W`` — every
        copy of a value lands in the same dest bin across all workers.
      * Phase B (parallel dedup): dest worker ``d`` dedups every worker's bin-``d``
        into a private carchar set — disjoint key slices, so NO cross-worker merge.

    The concatenated deduped output is driven through the DistinctNode's real
    downstream, so any post-DISTINCT sort / projection / limit / Exit still runs.
    (``exit_id`` is the node after DISTINCT; single-scan guarantees it is the real
    downstream instance, never a join leg/adapter.)"""
    from opteryx.compiled.morsel_ops.distinct import distinct as _distinct_op
    from opteryx.compiled.structures.carchar_set import CarcharSetWrapper
    from opteryx.compiled.thread_pool import CppThreadPool
    from opteryx.managers.execution.pipeline_compiler import compile_pipeline
    from opteryx.operators._operators import pull_one
    from draken.morsels.morsel import Morsel

    chains, exit_node, ctx = compile_pipeline(plan)
    scan = plan[scan_id]
    breaker = plan[breaker_id]
    downstream = plan[exit_id]
    head = plan[middle_ids[0]] if middle_ids else breaker
    distinct_on = breaker._distinct_on

    def _drain():
        if exit_node is None:
            return
        while exit_node.has_pending():
            yield exit_node.pop_pending()

    pool = None
    try:
        # Row-floor: tiny inputs run serially through the ORIGINAL DistinctNode (its
        # real serial dedup — the seam is left unset).
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
            for morsel in buffer:
                if ctx.is_terminated():
                    break
                push_one(head, morsel)
                yield from _drain()
            if not ctx.is_terminated():
                push_one(head, _EOS_SENTINEL)
                yield from _drain()
            return

        import queue
        import threading

        pool = CppThreadPool(workers, "m4-distinct")

        # ---- Pipeline 1: scan → (middle ops) → shuffle, FUSED, no internal barrier
        # W workers self-pull the scan (reentrant: the native source hands each
        # caller a distinct decoded row group) and scatter each morsel inline into W
        # dest bins → shuffle[w][d]. Scan-decode (the source's own worker pool) and
        # the scatter overlap; this is one streaming pipeline, not scan-then-shuffle.
        # Each worker owns a cloned middle chain (the "pipeline to a worker" shape) so
        # filter/projection also run in parallel here.
        buf_iter = iter(buffer)
        input_lock = threading.Lock()
        pos_holder = [None]
        pos_lock = threading.Lock()
        shuffle = [None] * workers
        err_a = [None] * workers

        # Concurrent self-pull is correct ONLY if the source's next_morsel is
        # reentrant (native single-pass parquet). For any other source (two-pass
        # latmat, empty-manifest fallback, non-parquet generators) the live pull
        # MUST be serialised, or N workers re-enter one non-reentrant generator
        # and crash. This is a correctness gate, not a perf toggle.
        concurrent_safe = scan.is_concurrent_pull_safe()

        def _next_input():
            if concurrent_safe:
                # Buffered floor sample first (locked; tiny), then reentrant self-pull.
                with input_lock:
                    m = next(buf_iter, None)
                if m is not None:
                    return m
                return pull_one(scan)
            # Non-reentrant source: serialise the whole pull under the lock.
            with input_lock:
                m = next(buf_iter, None)
                if m is not None:
                    return m
                return pull_one(scan)

        def pipe1_worker(w):
            dest = [[] for _ in range(workers)]
            chain_head = None
            collector = None
            if middle_ids:
                clones = [_clone_op(plan[nid]) for nid in middle_ids]
                dclone = _clone_op(breaker)
                collector = _DistinctCollector()
                dclone._scatter_engine = collector  # gather post-middle morsels
                chain = clones + [dclone]
                for i, op in enumerate(chain):
                    op.set_context(ctx)
                    if i + 1 < len(chain):
                        op.set_downstream(chain[i + 1])
                chain_head = chain[0]
            try:
                while True:
                    if ctx.is_terminated():
                        break
                    m = _next_input()
                    if m is None:
                        break
                    if middle_ids:
                        collector.morsels = []
                        push_one(chain_head, m)  # scan-slice → middle → seam-gather
                        produced = collector.morsels
                    else:
                        produced = (m,) if m.num_rows else ()
                    for pm in produced:
                        if pm.num_rows == 0:
                            continue
                        if pos_holder[0] is None:
                            with pos_lock:
                                if pos_holder[0] is None:
                                    p = _resolve_distinct_positions(pm, distinct_on)
                                    if p is None:
                                        # Fail loud — the finder only engages a shape
                                        # whose key columns are present on the morsel.
                                        raise RuntimeError(
                                            "distinct shuffle: dedup-key columns not "
                                            "resolvable on a Cxx-backed morsel"
                                        )
                                    pos_holder[0] = p
                        sub = pm._get_cxx().scatter(pos_holder[0], workers)
                        for d in range(workers):
                            dest[d].append(Morsel.from_cxx(sub[d]))
            except BaseException as exc:  # noqa: BLE001 — surface on the main thread
                err_a[w] = exc
            shuffle[w] = dest

        futures = [pool.submit(pipe1_worker, w) for w in range(workers)]
        for future in futures:
            future.result()
        for exc in err_a:
            if exc is not None:
                raise exc
        if ctx.is_terminated():
            return
        del buffer  # input consumed; the shuffle holds the only copy now

        if telemetry is not None:
            telemetry._reading["rowrouting_workers"] = workers
            telemetry._reading["distinct_rowrouting"] = 1

        # ---- Pipeline 2: [DISTINCT → exit], cloned and run in parallel ----------
        # The dedup pipeline runs per-partition through the SAME machinery the
        # stateless strategy uses: each worker drives a cloned operator chain
        # [DistinctNode → Exit] over its partition. The exit is a STAGE of the
        # parallel pipeline (select/rename runs on each worker), not bolted onto a
        # bespoke dedup loop — the DistinctNode operator dedups its disjoint slice
        # and streams survivors into its own Exit clone, whose output goes to the
        # caller via a shared queue. When the node after DISTINCT is a stateful
        # breaker (ORDER BY / LIMIT) the chain can't be cloned per worker, so those
        # dedup in parallel and drive the single downstream serially.
        fuse_exit = downstream is exit_node
        err_b = [None] * workers

        if fuse_exit:
            out_q = queue.Queue()
            DONE = object()

            def pipeline2_worker(d):
                # Cloned [DISTINCT → Exit] chain over this worker's disjoint
                # partition. No EOS is pushed to the per-worker chain — DISTINCT
                # streams survivors per morsel, and the one-time empty-result schema
                # morsel is emitted once via the original exit below.
                chain_head = _clone_op(breaker)
                exit_clone = _clone_op(exit_node)
                chain_head.set_context(ctx)
                exit_clone.set_context(ctx)
                chain_head.set_downstream(exit_clone)
                try:
                    for w in range(workers):
                        for chunk in shuffle[w][d]:
                            if ctx.is_terminated():
                                break
                            push_one(chain_head, chunk)
                            while exit_clone.has_pending():
                                out_q.put(exit_clone.pop_pending())
                except BaseException as exc:  # noqa: BLE001 — surface on main thread
                    err_b[d] = exc
                finally:
                    out_q.put(DONE)

            futures = [pool.submit(pipeline2_worker, d) for d in range(workers)]
            # Stream morsels to the caller as workers produce them.
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
            for exc in err_b:
                if exc is not None:
                    raise exc
            if ctx.is_terminated():
                return
            # Empty result still needs the schema morsel the Exit emits on EOS.
            if not yielded:
                push_one(exit_node, _EOS_SENTINEL)
                yield from _drain()
        else:
            # Stateful downstream (ORDER BY / LIMIT): dedup in parallel, then drive
            # the deduped output through the single downstream serially.
            worker_out = [None] * workers

            def dedup_worker(d):
                hash_set = CarcharSetWrapper()
                out = []
                try:
                    for w in range(workers):
                        for chunk in shuffle[w][d]:
                            if ctx.is_terminated():
                                break
                            _distinct_op(chunk, hash_set, columns=distinct_on)
                            if chunk.num_rows > 0:
                                out.append(chunk)
                except BaseException as exc:  # noqa: BLE001 — surface on main thread
                    err_b[d] = exc
                worker_out[d] = out

            futures = [pool.submit(dedup_worker, d) for d in range(workers)]
            for future in futures:
                future.result()
            for exc in err_b:
                if exc is not None:
                    raise exc
            if ctx.is_terminated():
                return
            for d in range(workers):
                for chunk in worker_out[d] or ():
                    if ctx.is_terminated():
                        break
                    push_one(downstream, chunk)
                    yield from _drain()
            if not ctx.is_terminated():
                push_one(downstream, _EOS_SENTINEL)
                yield from _drain()
    finally:
        if pool is not None:
            pool.shutdown(wait=True)
        ctx.terminate()
        scan.close_source()


def _grouped_agg_stream(plan, scan_id, middle_ids, breaker_id, workers, telemetry=None):
    """Parallel GROUP BY by ROW-ROUTING (M4, strategy="rowrouting").

    A serial producer runs ``scan → stateless* → breaker`` through the normal push
    chain; the breaker's engine is swapped for a ``_ScatterCollectEngine`` that
    row-routes each prepared morsel into ``W`` per-worker bins (the middle ops run
    upstream for free — no bespoke collector). Then ``W`` workers key their OWN
    bin into a private ``GroupHashEngine`` — disjoint key slices, so no merge.
    Finalize injects the ``W`` engines into the original breaker and drives EOS:
    the breaker concatenates their outputs downstream through Exit.
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

    pool = None
    try:
        # Row-floor: tiny inputs run serially through the ORIGINAL breaker (its real
        # engine, untouched). Above the floor, the row-routing path engages.
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
            for morsel in buffer:
                if ctx.is_terminated():
                    break
                push_one(head, morsel)
                yield from _drain()
            if not ctx.is_terminated():
                push_one(head, _EOS_SENTINEL)
                yield from _drain()
            return

        # ---- Serial scatter: route every (prepared) morsel into W bins --------
        worker_engines = [_clone_op(breaker)._engine for _ in range(workers)]
        scatter_engine = _ScatterCollectEngine(workers, worker_engines[0])
        breaker._engine = scatter_engine

        # Scatter the floor SAMPLE (the buffered >=PARALLEL_MIN_ROWS rows) first
        # and snapshot per-bin row counts — the skew estimate a future cost-model
        # cutover (§7) would read PRE-engagement. Telemetry only: we do NOT act on
        # it, so a skewed query still engages and its cost stays observable.
        for morsel in buffer:
            if ctx.is_terminated():
                return
            push_one(head, morsel)  # → breaker prepare/select → scatter into bins
        sample_bin_rows = [sum(m.num_rows for m in b) for b in scatter_engine.bins]

        while True:
            morsel = pull_one(scan)
            if morsel is None:
                break
            if ctx.is_terminated():
                return
            push_one(head, morsel)

        # ---- Parallel keying: each worker keys its OWN disjoint bin -----------
        bins = scatter_engine.bins
        errors = [None] * workers
        worker_rows = [0] * workers

        def worker(index):
            engine = worker_engines[index]
            count = 0
            try:
                for chunk in bins[index]:
                    if ctx.is_terminated():
                        break
                    engine.ingest(chunk)
                    count += chunk.num_rows
            except BaseException as exc:  # noqa: BLE001 — surface on the main thread
                errors[index] = exc
            worker_rows[index] = count

        pool = CppThreadPool(workers, "m4-grouped-rowrouting")
        futures = [pool.submit(worker, k) for k in range(workers)]
        for future in futures:
            future.result()
        for exc in errors:
            if exc is not None:
                raise exc
        if ctx.is_terminated():
            return

        # ---- Skew / NDV telemetry — MEASURED, not acted on (§7 calibration) ----
        # Pre-engagement sample bin balance vs post-keying bin balance, plus exact
        # NDV (sum over disjoint workers). A future cutover correlates these with
        # measured speed-up to set thresholds. Computed single-threaded after the
        # keying barrier — no shared-counter FT race (each worker wrote only its
        # own worker_rows[k] / its own engine).
        if telemetry is not None:
            reading = telemetry._reading
            reading["rowrouting_workers"] = workers
            reading["rowrouting_sample_rows"] = sum(sample_bin_rows)
            reading["rowrouting_sample_maxbin_rows"] = max(sample_bin_rows) if sample_bin_rows else 0
            reading["rowrouting_total_rows"] = sum(worker_rows)
            reading["rowrouting_maxbin_rows"] = max(worker_rows) if worker_rows else 0
            reading["rowrouting_ndv"] = sum(e.num_groups() for e in worker_engines)

        # ---- Finalize: concat the W disjoint engines (no merge) through Exit ---
        populated = [k for k in range(workers) if worker_rows[k] > 0]
        breaker._parallel_engines = (
            [worker_engines[k] for k in populated] if populated else [worker_engines[0]]
        )
        push_one(breaker, _EOS_SENTINEL)
        yield from _drain()
    finally:
        if pool is not None:
            pool.shutdown(wait=True)
        ctx.terminate()
        scan.close_source()


def _grouped_agg_route(plan, scan_id, middle_ids, breaker_id, workers, telemetry=None):
    """Parallel GROUP BY by ROUTE-RAW (M4 Stage 2, route-on-abandon SINK).

    Unlike ``_grouped_agg_stream`` (one SERIAL producer scatters every morsel, then W
    workers key their bin), here each worker SELF-PULLS its own morsels, runs them
    through its OWN cloned ``scan→middle→breaker-prepare`` chain, and routes the
    prepared morsels raw into its OWN thread-local ``radix`` partition bins. There is
    NO serial scatter — that serial pass is the Amdahl ceiling the row-routing path
    hits (~2.73× on real int-key data). A short Combine hands the thread-local bins off
    to global per-partition lists; then a parallel per-partition read-out aggregates
    each partition exactly once (``hash(key) % radix`` co-locates a group in ONE
    partition, so the partition engines concat with NO merge — the identical
    ``breaker._parallel_engines`` finalize the row-routing path uses).

    Increment 1: ALWAYS route (no bounded-adaptive pre-aggregate yet — that switch is
    prototype-validated in scratch/ddb_proto/demo_agg_adaptive.py and lands next). The
    result is identical to serial; gated behind ``config.M4_ROUTE_AGG``, DOP-sweep-gated.
    """
    import threading

    from opteryx.compiled.thread_pool import CppThreadPool
    from opteryx.managers.execution.pipeline_compiler import compile_pipeline
    from opteryx.operators._operators import pull_one

    chains, exit_node, ctx = compile_pipeline(plan)
    scan = plan[scan_id]
    breaker = plan[breaker_id]
    head = plan[middle_ids[0]] if middle_ids else breaker

    # radix = next power of two >= workers (>= so the read-out has >= DOP parallelism).
    radix = 1
    while radix < workers:
        radix <<= 1

    def _drain():
        if exit_node is None:
            return
        while exit_node.has_pending():
            yield exit_node.pop_pending()

    pool = None
    rpool = None
    try:
        # Row-floor: tiny inputs run serially through the ORIGINAL breaker (real engine,
        # untouched) — identical to _grouped_agg_stream's floor.
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
            for morsel in buffer:
                if ctx.is_terminated():
                    break
                push_one(head, morsel)
                yield from _drain()
            if not ctx.is_terminated():
                push_one(head, _EOS_SENTINEL)
                yield from _drain()
            return

        if telemetry is not None:
            telemetry._reading["parallel_engaged"] = 1
            telemetry._reading["route_agg_workers"] = workers
            telemetry._reading["route_agg_radix"] = radix

        # ---- Parallel route-raw SINK: each worker self-pulls, prepares, routes -----
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

        sink_bins = [None] * workers  # sink_bins[w][p] -> list[Morsel]
        errors = [None] * workers

        def sink_worker(index):
            try:
                clones = [_clone_op(plan[nid]) for nid in middle_ids]
                cloned_breaker = _clone_op(breaker)
                chain = clones + [cloned_breaker]
                for i, op in enumerate(chain):
                    op.set_context(ctx)
                    if i + 1 < len(chain):
                        op.set_downstream(chain[i + 1])
                # Swap the cloned breaker's GroupHashEngine for a thread-local scatter
                # into `radix` bins (its own real engine is the key-position resolver).
                route_engine = _ScatterCollectEngine(radix, cloned_breaker._engine)
                cloned_breaker._engine = route_engine
                h = chain[0]
                while True:
                    morsel = next_input()
                    if morsel is None:
                        break
                    push_one(h, morsel)  # → cloned prepare/select → scatter to bins
                sink_bins[index] = route_engine.bins
            except BaseException as exc:  # noqa: BLE001 — surface on the main thread
                errors[index] = exc

        pool = CppThreadPool(workers, "m4-route-sink")
        futures = [pool.submit(sink_worker, k) for k in range(workers)]
        for future in futures:
            future.result()
        for exc in errors:
            if exc is not None:
                raise exc
        if ctx.is_terminated():
            return

        # ---- Combine: O(partitions) hand-off of thread-local bins → global lists ---
        global_bins = [[] for _ in range(radix)]
        for w in range(workers):
            wb = sink_bins[w]
            if wb is None:
                continue
            for p in range(radix):
                if wb[p]:
                    global_bins[p].extend(wb[p])

        # ---- Parallel per-partition READ-OUT: aggregate each partition ONCE --------
        part_engines = [None] * radix
        part_rows = [0] * radix
        rerr = [None] * radix

        def readout_worker(p):
            try:
                engine = _clone_op(breaker)._engine
                count = 0
                for chunk in global_bins[p]:
                    if ctx.is_terminated():
                        break
                    engine.ingest(chunk)
                    count += chunk.num_rows
                part_engines[p] = engine
                part_rows[p] = count
            except BaseException as exc:  # noqa: BLE001
                rerr[p] = exc

        rpool = CppThreadPool(max(1, min(radix, workers)), "m4-route-readout")
        rfutures = [rpool.submit(readout_worker, p) for p in range(radix)]
        for future in rfutures:
            future.result()
        for exc in rerr:
            if exc is not None:
                raise exc
        if ctx.is_terminated():
            return

        if telemetry is not None:
            telemetry._reading["route_agg_total_rows"] = sum(part_rows)
            telemetry._reading["route_agg_ndv"] = sum(
                e.num_groups() for e in part_engines if e is not None
            )

        # ---- Finalize: concat the populated partition engines (no merge) via Exit --
        populated = [p for p in range(radix) if part_rows[p] > 0]
        breaker._parallel_engines = (
            [part_engines[p] for p in populated] if populated else [_clone_op(breaker)._engine]
        )
        push_one(breaker, _EOS_SENTINEL)
        yield from _drain()
    finally:
        if rpool is not None:
            rpool.shutdown(wait=True)
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
