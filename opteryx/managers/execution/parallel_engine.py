# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Parallel execution engine — central scheduler (M4).

The engine owns parallelism; the OPERATORS stay parallel-unaware (the founding
principle). There is ONE data-pipeline executor, ``_pipeline_stream``: every data
pipeline runs through it, and it routes the plan to the handler for its shape. No
dual path, no flag, no bespoke per-shape cascade:

  * **Grouped aggregate** (``scan → stateless* → GroupedAggregateHashed``): the
    HASH_REPARTITION ``PipelineSink`` contract — each worker self-pulls + prepares its
    own morsels and routes them raw into ``radix`` thread-local bins by
    ``hash(key) % radix`` (so every occurrence of a key lands in ONE partition), then a
    parallel per-partition read-out aggregates each partition once; finalize is a
    CONCAT, never a merge. Because each group is seen whole by one partition this
    parallelises HOLISTIC aggregates too (MEDIAN / COUNT(DISTINCT)).
  * **DISTINCT** (``scan → stateless* → Distinct``): the SAME HASH_REPARTITION
    scatter, with a per-partition DEDUP read-out. Engages only at ``W >= 2`` (serial
    dedup is fine at W=1).
  * **Ungrouped aggregate** (``scan → stateless* → UngroupedAggregate``): no group
    key, so recombination is a trivial SCALAR merge.
  * **join → agg** (``[scan, scan] → INNER JOIN → stateless* → AGG → exit``): the
    SHARED_SOURCE join composed with the agg sink through the SAME skeleton via a
    ``source_prep`` — build once serially, W workers probe disjoint slices.
  * **Bare inner-equi join** (``scan → join → stateless* → exit``, no agg): build once
    serially, then W workers probe disjoint slices with private engines
    (``_join_probe_stream``).
  * **Standalone selection / projection** (``scan → {filter,projection}* → exit``,
    no breaker): each morsel is an independent transform, so workers self-pull and
    STREAM their finished morsels out — no merge, no barrier (``_stateless_stream``).

Any plan no strategy matches (multi-join, set ops, window, sort, limit-only) is driven
SERIAL-INLINE here (``_serial_stream``) — the data executor owns it end-to-end; it
never punts to a hidden fallback in another engine.

The grouped-agg / ungrouped-agg / distinct / join→agg shapes share ONE breaker-segment
skeleton (``_run_breaker_segment``) + a ``PipelineSink`` adapter (in ``pipeline_sink.py``)
for the three points that vary per shape (make-local-state / combine / finalize).

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
from opteryx.managers.execution.pipeline_sink import RecombClass
from opteryx.managers.execution.pipeline_sink import make_sink
from opteryx.managers.execution.pipeline_sink import recombination_class_for
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


def _walk_leg_to_scan(plan, provider_id, join_id, registry):
    """Walk a join leg UP from the join's input node (`provider_id`) to its scan,
    collecting the (stateless) middle ops between scan and join.

    The push topology is single-output, so each leg is a linear chain
    scan → middle* → join. Returns ``(scan_id, middle_ids)`` where ``middle_ids``
    is in dataflow order (scan-first), or ``None`` if the leg is not a clean
    linear chain of a scan + STATELESS middle ops (any stateful middle op — a
    Limit/Window/aggregate/sort — disqualifies the whole parallel shape)."""
    chain = []  # provider_id first as we walk up, reversed to dataflow order later
    cur = provider_id
    while True:
        node = plan[cur]
        if getattr(node, "is_scan", False):
            scan_id = cur
            break
        # A middle op must be STATELESS and feed exactly this leg (one outgoing
        # edge into the chain). Anything else (a breaker, a fan-in) rejects.
        meta = registry.get(type(node))
        if meta is None or meta.parallelism != OperatorParallelism.STATELESS:
            return None
        ins = list(plan.ingoing_edges(cur))
        if len(ins) != 1:
            return None
        chain.append(cur)
        cur = ins[0][0]
    chain.reverse()  # now scan-side-first (dataflow order), excludes scan & join
    return scan_id, chain


# Stateful ops that may sit in the SERIAL TAIL above a parallelised AGG breaker. The
# agg is the parallelised breaker; once it recombines, a Sort / HeapSort / Limit (+ any
# stateless ops) runs SERIALLY on the (small) grouped output, driven by the ORIGINAL
# breaker's EOS push (the breaker's _downstream is the tail → Exit). These are the
# ORDER/LIMIT operators a `GROUP BY ... ORDER BY ... LIMIT` plan puts above the agg —
# order-dependent ops that are CORRECT serially on the already-recombined result.
# (HeapSortNode fuses ORDER BY + LIMIT; a standalone LimitNode appears when there is a
# LIMIT with no ORDER BY.) They are NOT STATELESS in the catalog (Sort/HeapSort are
# breakers; Limit is STATEFUL_SERIAL), so the stateless predicate would reject them —
# the explicit allow-list is what lets the tail through.
_SERIAL_TAIL_OPS = frozenset({"SortNode", "HeapSortNode", "LimitNode"})


def _agg_tail_reaches_exit(plan, breaker_id, registry):
    """Walk the chain ABOVE an AGG breaker and confirm it is a SERIAL TAIL ending at
    the Exit — ``(stateless | Sort | HeapSort | Limit)* → Exit`` — returning the Exit
    id, else ``None``.

    The AGG breaker is the PARALLELISED breaker; the skeleton's finalize pushes EOS
    through the ORIGINAL breaker, whose ``_downstream`` is this tail, so the tail runs
    SERIALLY on the recombined grouped output (the small side). A Sort/HeapSort here is
    fine: ORDER BY ordering is preserved because the serial sort runs AFTER the parallel
    agg fully recombines. Anything that is NOT stateless and NOT one of the sanctioned
    serial-tail breakers (an Aggregate / Distinct / Join / Union — a SECOND parallelisable
    breaker that would need its own segment) rejects the shape (→ serial)."""
    cur = breaker_id
    while True:
        outs = list(plan.outgoing_edges(cur))
        if not outs:
            return None  # no Exit — degenerate, reject
        nxt = outs[0][1]
        node = plan[nxt]
        cls = node.__class__.__name__
        if cls == "ExitNode":
            return nxt
        if cls in _SERIAL_TAIL_OPS:
            cur = nxt
            continue
        meta = registry.get(type(node))
        if meta is None or meta.parallelism != OperatorParallelism.STATELESS:
            # A non-stateless, non-sanctioned-tail op (a second agg/distinct/join, or a
            # Window) → not a clean serial tail. Reject (→ serial).
            return None
        cur = nxt


def _find_parallel_join(plan):
    """Return a struct describing a parallel INNER-EQUI-JOIN shape, else ``None``.

    THREAD-LOCAL-FULL parallel probe: the build side is prepared ONCE serially
    (the join accumulates left + builds ``left_hash`` at left-EOS); then W workers
    each rebuild their OWN private join engine from the shared read-only
    ``left_morsel`` and probe a DISJOINT slice of the probe scan. No shared mutable
    engine → no data race → no draken/C++ change.

    Detect ONLY the provably-safe shape; anything else returns ``None`` (serial):
      * exactly 2 scans, exactly 1 join, and the join is ``DrakenInnerJoinNode``
        (inner-equi only — Outer/Semi/Anti/Cross/NonEqui/Asof/Filter rejected by
        class);
      * the join's ``_compiled_right_evals`` is empty — a non-empty right ON-eval
        mutates the shared ``self.right_columns`` during probe (a data race);
      * both legs are a clean linear ``scan → stateless* → join`` chain;
      * every node between the join and the Exit is STATELESS — no aggregate /
        sort / distinct / limit may depend on probe emission order (workers emit
        out of order).

    Returns a struct with build/probe scan + middle ids, join id, the downstream
    ids (join→exit, exclusive), and the exit id."""
    registry = get_registry()
    scans = [nid for nid in plan.nodes() if getattr(plan[nid], "is_scan", False)]
    if len(scans) != 2:
        return None
    joins = [nid for nid in plan.nodes() if getattr(plan[nid], "is_join", False)]
    if len(joins) != 1:
        return None
    join_id = joins[0]
    join = plan[join_id]
    if join.__class__.__name__ != "DrakenInnerJoinNode":
        return None
    # A non-empty right ON-eval appends to the shared `right_columns` list during
    # probe — a data race across workers sharing the original column list. v1
    # requires it empty. (Left evals are fine: they run once in the serial build.)
    if getattr(join, "_compiled_right_evals", None):
        return None

    # Identify the two legs by edge label ('left' = build, 'right' = probe). The
    # label may be empty on synthesised joins → fall back to ingoing-edge order
    # (first = left, second = right), mirroring pipeline_compiler.
    legs = {}
    for idx, (provider, _target, label) in enumerate(plan.ingoing_edges(join_id)):
        if not label:
            label = "left" if idx == 0 else "right"
        legs[label] = provider
    if "left" not in legs or "right" not in legs:
        return None

    build_leg = _walk_leg_to_scan(plan, legs["left"], join_id, registry)
    probe_leg = _walk_leg_to_scan(plan, legs["right"], join_id, registry)
    if build_leg is None or probe_leg is None:
        return None
    build_scan_id, build_middle_ids = build_leg
    probe_scan_id, probe_middle_ids = probe_leg

    # Downstream chain join → exit (exclusive of both): every node must be
    # STATELESS so out-of-order probe emission is safe (no sort/agg/distinct/limit
    # may sit here). The single-output topology makes the walk unambiguous.
    downstream_ids = []
    cur = join_id
    exit_id = None
    while True:
        outs = list(plan.outgoing_edges(cur))
        if not outs:
            # `cur` is a sink with no Exit — degenerate; reject.
            return None
        nxt = outs[0][1]
        node = plan[nxt]
        if node.__class__.__name__ == "ExitNode":
            exit_id = nxt
            break
        meta = registry.get(type(node))
        if meta is None or meta.parallelism != OperatorParallelism.STATELESS:
            return None
        downstream_ids.append(nxt)
        cur = nxt
    if exit_id is None:
        return None

    return _ParallelJoinShape(
        build_scan_id=build_scan_id,
        build_middle_ids=build_middle_ids,
        probe_scan_id=probe_scan_id,
        probe_middle_ids=probe_middle_ids,
        join_id=join_id,
        downstream_ids=downstream_ids,
        exit_id=exit_id,
    )


@dataclass(frozen=True)
class _ParallelJoinShape:
    """The detected parallel inner-equi-join shape (see ``_find_parallel_join``)."""

    build_scan_id: str
    build_middle_ids: list
    probe_scan_id: str
    probe_middle_ids: list
    join_id: str
    downstream_ids: list
    exit_id: str


@dataclass(frozen=True)
class _ParallelJoinAggShape:
    """The detected parallel ``join → stateless* → AGG-breaker → exit`` shape (Step 5).

    A SHARED_SOURCE inner-equi join feeding a generic agg sink (grouped or
    ungrouped). The join's BUILD leg is prepared once serially (the SHARED_SOURCE
    contribution); then W workers each own a private join engine and probe a disjoint
    slice of the probe scan, with the join output flowing through the (stateless)
    ``agg_middle_ids`` into the AGG breaker's per-worker sink — all driven through the
    ONE ``_run_breaker_segment`` skeleton via a ``source_prep`` (a ``_SharedSourceJoin``).

    The fields mirror ``_ParallelJoinShape`` for the join legs, plus the agg tail:
    ``agg_middle_ids`` are the STATELESS ops between the join and the AGG breaker;
    ``breaker_id`` is the AGG breaker (the sink); ``exit_id`` is the Exit after it.

    ``build_scan_ids`` generalises the build leg to a MULTI-JOIN subtree (Step 6): when
    the TOP join's BUILD (left) leg is itself a join subtree, this carries ALL the scan
    ids on the build side (every scan EXCEPT the PROBE fact scan). The build prelude then
    drives every one of those scan chains serially (in compiler DFS order, build legs
    before probe), so the TOP join ends with its prepared ``left_morsel``/``left_hash``
    exactly as the single-build-leg case. For the single-join shape it is ``None`` and the
    prelude drives just ``build_scan_id``'s chain. ``build_scan_id``/``build_middle_ids``
    are still set (to the TOP join's left-leg head, used only for the single-join drive)."""

    build_scan_id: str
    build_middle_ids: list
    probe_scan_id: str
    probe_middle_ids: list
    join_id: str
    agg_middle_ids: list
    breaker_id: str
    exit_id: str
    build_scan_ids: tuple = None


def _join_legs(plan, join_id, registry):
    """Resolve the build (left) + probe (right) legs of ``join_id`` to
    ``((build_scan, build_middle), (probe_scan, probe_middle))`` linear chains, or
    ``None`` if either leg is not a clean ``scan → stateless* → join`` chain. Shared by
    ``_find_parallel_join`` and ``_find_parallel_join_agg`` (the leg topology is the
    same; only the downstream tail differs)."""
    legs = {}
    for idx, (provider, _target, label) in enumerate(plan.ingoing_edges(join_id)):
        if not label:
            label = "left" if idx == 0 else "right"
        legs[label] = provider
    if "left" not in legs or "right" not in legs:
        return None
    build_leg = _walk_leg_to_scan(plan, legs["left"], join_id, registry)
    probe_leg = _walk_leg_to_scan(plan, legs["right"], join_id, registry)
    if build_leg is None or probe_leg is None:
        return None
    return build_leg, probe_leg


def _safe_parallel_join(plan, registry):
    """Return the single ``DrakenInnerJoinNode`` id if the plan has the
    provably-safe parallel inner-equi-join PREFIX (2 scans, 1 inner join, empty
    ``_compiled_right_evals``), else ``None``. The DOWNSTREAM tail (stateless→exit
    vs stateless→agg→exit) is checked by the caller — this gates only the join
    itself + its build/probe legs being clonable."""
    scans = [nid for nid in plan.nodes() if getattr(plan[nid], "is_scan", False)]
    if len(scans) != 2:
        return None
    joins = [nid for nid in plan.nodes() if getattr(plan[nid], "is_join", False)]
    if len(joins) != 1:
        return None
    join_id = joins[0]
    join = plan[join_id]
    if join.__class__.__name__ != "DrakenInnerJoinNode":
        return None
    # A non-empty right ON-eval appends to the shared `right_columns` during probe —
    # a data race across workers sharing the original column list (the Phase-1 gate).
    if getattr(join, "_compiled_right_evals", None):
        return None
    return join_id


def _is_safe_probe_join(plan, join_id, registry):
    """Return True if ``join_id`` is an inner-equi join whose PROBE (right) side can be
    parallelised: a ``DrakenInnerJoinNode`` with empty ``_compiled_right_evals`` (the
    Phase-1 race gate — a non-empty right ON-eval mutates the shared ``right_columns``
    during probe). The BUILD side is unconstrained here (it is driven serially)."""
    join = plan[join_id]
    if join.__class__.__name__ != "DrakenInnerJoinNode":
        return False
    if getattr(join, "_compiled_right_evals", None):
        return False
    return True


def _top_join_for_agg(plan, breaker_id, registry):
    """Walk UP from the AGG breaker through the (stateless) agg-middle ops to the FIRST
    join — the TOP join feeding the aggregate — returning ``(join_id, agg_middle_ids)``
    where ``agg_middle_ids`` are the stateless ops between the join and the breaker (in
    dataflow order), or ``None`` if the agg's immediate upstream is not a join reachable
    through a clean stateless chain. The push topology is single-input for these
    operators, so the walk is unambiguous."""
    agg_middle_rev = []
    cur = breaker_id
    while True:
        ins = list(plan.ingoing_edges(cur))
        if len(ins) != 1:
            return None
        prov = ins[0][0]
        node = plan[prov]
        if getattr(node, "is_join", False):
            agg_middle_rev.reverse()
            return prov, agg_middle_rev
        meta = registry.get(type(node))
        if meta is None or meta.parallelism != OperatorParallelism.STATELESS:
            return None  # a breaker / stateful op between agg and join → reject
        agg_middle_rev.append(prov)
        cur = prov


def _build_subtree_scans(plan, top_join_id, probe_scan_id):
    """Every scan id reachable on the BUILD side of ``top_join_id`` — i.e. all scans in
    the plan EXCEPT the PROBE fact scan. The build subtree is driven serially by the
    prelude (all its scan chains, in compiler DFS order), so it does not matter how the
    inner joins nest: every non-probe scan contributes to building the TOP join's left.
    Returns the tuple of build scan ids (excludes ``probe_scan_id``)."""
    return tuple(
        nid
        for nid in plan.nodes()
        if getattr(plan[nid], "is_scan", False) and nid != probe_scan_id
    )


def _find_parallel_join_agg(plan):
    """Return a ``_ParallelJoinAggShape`` for a single-inner-join feeding a generic
    AGG breaker — ``[build-scan, probe-scan] → INNER JOIN → stateless* → AGG → exit`` —
    else ``None``.

    This is the Step-5 ``join → agg`` shape: the SHARED_SOURCE join composes with the
    grouped/ungrouped agg sink THROUGH the contract (no monolithic ``_join_agg_stream``).
    It reuses ``_find_parallel_join``'s exact leg topology (``_join_legs``) and the same
    safety gate (``_safe_parallel_join``); the ONLY difference is the downstream walk —
    where ``_find_parallel_join`` requires ``stateless* → exit``, this requires
    ``stateless* → AGG-breaker → exit`` (a grouped or ungrouped aggregate, the breakers
    the generic sink factory builds). The intervening agg-middle ops MUST be STATELESS
    (out-of-order probe emission feeds the agg, which is order-independent)."""
    registry = get_registry()
    join_id = _safe_parallel_join(plan, registry)
    if join_id is None:
        return None

    legs = _join_legs(plan, join_id, registry)
    if legs is None:
        return None
    (build_scan_id, build_middle_ids), (probe_scan_id, probe_middle_ids) = legs

    # Downstream walk: join → stateless* → AGG breaker → exit. Collect the stateless
    # middle ops, then require the NEXT node to be a generic AGG breaker whose sink the
    # factory builds (grouped/ungrouped), then the Exit.
    agg_middle_ids = []
    cur = join_id
    breaker_id = None
    while True:
        outs = list(plan.outgoing_edges(cur))
        if not outs:
            return None
        nxt = outs[0][1]
        node = plan[nxt]
        cls = node.__class__.__name__
        if cls in ("GroupedAggregateHashedNode", "UngroupedAggregateNode"):
            breaker_id = nxt
            break
        # A non-agg breaker (sort/distinct/limit) in the tail → not this shape.
        meta = registry.get(type(node))
        if meta is None or meta.parallelism != OperatorParallelism.STATELESS:
            return None
        agg_middle_ids.append(nxt)
        cur = nxt

    breaker = plan[breaker_id]
    bcls = breaker.__class__.__name__
    # Apply the SAME eligibility gates the single-scan agg finders enforce, so the
    # generic sink the composition reuses has the state it needs.
    if bcls == "GroupedAggregateHashedNode":
        if not breaker.group_by_columns:
            return None  # degenerate grouped breaker — no key to route on
    else:  # UngroupedAggregateNode
        engine = getattr(breaker, "_engine", None)
        if engine is None or not engine.is_mergeable() or breaker._has_literals:
            return None

    # The AGG breaker may be followed by a SERIAL TAIL — (stateless | Sort | HeapSort |
    # Limit)* → Exit — NOT only the Exit directly. The agg is the PARALLELISED breaker;
    # the skeleton's finalize pushes EOS through the ORIGINAL breaker, whose _downstream
    # is this tail, so the tail (an ORDER BY / LIMIT on the grouped output) runs SERIALLY
    # on the recombined result — ORDER BY ordering is preserved (the sort runs AFTER the
    # parallel agg fully recombines). A second parallelisable breaker (agg/distinct/join/
    # window) in the tail rejects the shape (it would need its own segment) → serial.
    exit_id = _agg_tail_reaches_exit(plan, breaker_id, registry)
    if exit_id is None:
        return None

    return _ParallelJoinAggShape(
        build_scan_id=build_scan_id,
        build_middle_ids=build_middle_ids,
        probe_scan_id=probe_scan_id,
        probe_middle_ids=probe_middle_ids,
        join_id=join_id,
        agg_middle_ids=agg_middle_ids,
        breaker_id=breaker_id,
        exit_id=exit_id,
    )


def _find_parallel_multi_join_agg(plan):
    """Return a ``_ParallelJoinAggShape`` (with ``build_scan_ids`` set) for a MULTI-JOIN
    pipeline whose TOP join feeds a generic AGG breaker — ``... → INNER JOIN [build =
    join subtree, probe = fact scan] → stateless* → AGG → (serial tail) → exit`` — else
    ``None`` (Step 6).

    This generalises ``_find_parallel_join_agg``'s single-join shape: the TOP join (the
    one feeding the agg) may have a BUILD (left) leg that is itself a JOIN subtree, while
    its PROBE (right) leg must still be a raw fact scan (``scan → stateless* → join``).
    The build subtree is built ONCE serially by the prelude (driving every scan chain
    EXCEPT the fact/probe scan, in compiler DFS order); only the TOP join's probe — the
    big fact table — is parallelised, mirroring the design's "build is the small side".

    The shape it returns is identical to the single-join one PLUS ``build_scan_ids`` (all
    non-probe scans), which the prelude drives serially. The single-join detector runs
    FIRST, so this only engages when there is more than one join."""
    registry = get_registry()
    joins = [nid for nid in plan.nodes() if getattr(plan[nid], "is_join", False)]
    if len(joins) < 2:
        return None  # single-join (or no-join) handled by _find_parallel_join_agg

    # Locate the AGG breaker fed (through stateless ops) by the TOP join, and the Exit
    # tail above it (serial-tail-tolerant, Step A).
    breakers = [
        nid
        for nid in plan.nodes()
        if plan[nid].__class__.__name__
        in ("GroupedAggregateHashedNode", "UngroupedAggregateNode")
    ]
    if len(breakers) != 1:
        return None
    breaker_id = breakers[0]

    top = _top_join_for_agg(plan, breaker_id, registry)
    if top is None:
        return None
    join_id, agg_middle_ids = top

    # The TOP join's PROBE (right) leg must be a clean fact-scan chain; its BUILD (left)
    # leg may be a join subtree (driven serially by the prelude). Only the TOP join is
    # probed in parallel, so only IT must pass the probe-race gate.
    if not _is_safe_probe_join(plan, join_id, registry):
        return None
    legs = {}
    for idx, (provider, _t, label) in enumerate(plan.ingoing_edges(join_id)):
        if not label:
            label = "left" if idx == 0 else "right"
        legs[label] = provider
    if "left" not in legs or "right" not in legs:
        return None
    probe_leg = _walk_leg_to_scan(plan, legs["right"], join_id, registry)
    if probe_leg is None:
        # The probe (right) leg is NOT a raw fact scan (it is a join subtree). This
        # engine parallelises only a fact-scan probe, so this plan stays serial.
        return None
    probe_scan_id, probe_middle_ids = probe_leg

    # The BUILD side = every scan except the probe fact scan, driven serially by the
    # prelude. (The build-side joins run serially, so their right-eval races and join
    # types are immaterial — only the TOP join's probe is parallel.)
    build_scan_ids = _build_subtree_scans(plan, join_id, probe_scan_id)
    if not build_scan_ids:
        return None  # no build side — degenerate

    breaker = plan[breaker_id]
    bcls = breaker.__class__.__name__
    if bcls == "GroupedAggregateHashedNode":
        if not breaker.group_by_columns:
            return None
    else:  # UngroupedAggregateNode
        engine = getattr(breaker, "_engine", None)
        if engine is None or not engine.is_mergeable() or breaker._has_literals:
            return None

    exit_id = _agg_tail_reaches_exit(plan, breaker_id, registry)
    if exit_id is None:
        return None

    return _ParallelJoinAggShape(
        # build_scan_id/build_middle_ids are the TOP join's LEFT-leg head only as a
        # nominal anchor; the real (multi-scan) build is driven via build_scan_ids.
        build_scan_id=legs["left"],
        build_middle_ids=[],
        probe_scan_id=probe_scan_id,
        probe_middle_ids=probe_middle_ids,
        join_id=join_id,
        agg_middle_ids=agg_middle_ids,
        breaker_id=breaker_id,
        exit_id=exit_id,
        build_scan_ids=build_scan_ids,
    )


class _SharedSourceJoin:
    """The SHARED_SOURCE contribution of an inner-equi join, lifted out of
    ``_join_probe_stream`` so the join can compose with the generic agg sink through
    the ONE ``_run_breaker_segment`` skeleton (design §1.3, Step 5).

    It captures the read-only build state ONCE (``build``) — the build-before-probe
    dependency, run before the worker fan-out — and hands each worker a private join
    engine built over the shared ``left_morsel`` (``prepare_worker_clone``). No worker
    mutates shared probe state: each owns its own ``left_hash`` (thread-local-full).

    The skeleton uses it as a ``source_prep``: the self-pulled scan is the PROBE scan,
    and each worker's chain prefix is ``[probe-middle* → JoinRightAdapter(cloned_join) →
    cloned_join]`` ahead of the agg-middle ops + the agg sink clone."""

    __slots__ = (
        "_plan",
        "_shape",
        "_ctx",
        "left_morsel",
        "left_columns",
        "left_is_empty",
        "columns",
        "load_factor",
    )

    def __init__(self, plan, shape, ctx):
        self._plan = plan
        self._shape = shape
        self._ctx = ctx
        self.left_morsel = None
        self.left_columns = None
        self.left_is_empty = False
        self.columns = None
        self.load_factor = None

    def build(self, build_chains, exit_node):
        """Drive the BUILD side to completion serially, then capture the shared read-only
        build state. Run ONCE, before the worker fan-out (the build-before-probe edge).

        ``build_chains`` is a list of ``(scan, chain_head)`` pairs in compiler DFS order
        (build legs before probe). For a SINGLE-build-leg join it is one pair; for a
        MULTI-JOIN subtree (Step 6) it is EVERY scan chain except the probe fact scan —
        driving them in order builds each inner join, and the inner joins' outputs flow
        through the TOP join's left adapter, so the TOP join ends with its prepared
        ``left_morsel``/``left_hash`` exactly as the single-leg case. The TOP join (and
        every inner join) accumulates + builds at its left-EOS, emitting NOTHING past the
        TOP join (build EOS is absorbed there), so this yields nothing — it just builds."""
        from opteryx.operators._operators import drive_scan

        j = self._plan[self._shape.join_id]
        for build_scan, build_head in build_chains:
            for _ in drive_scan(build_scan, build_head, exit_node, self._ctx):
                pass
            if self._ctx.is_terminated():
                return
        self.left_morsel = j.left_morsel
        self.left_columns = j.left_columns
        self.left_is_empty = j.left_is_empty
        self.columns = j.columns
        self.load_factor = j.carchar_probe_load_factor

    def prepare_worker_clone(self, cloned_join):
        """Build THIS worker's private join engine over the shared READ-ONLY
        ``left_morsel`` (no shared mutable probe state — thread-local-full). Mirrors
        ``_join_probe_stream``'s per-worker setup exactly."""
        from opteryx.operators._operators import build_side_carchar_morsel_map

        cloned_join.left_morsel = self.left_morsel
        cloned_join.left_columns = self.left_columns
        cloned_join.columns = self.columns
        cloned_join.left_is_empty = self.left_is_empty
        cloned_join.carchar_probe_load_factor = self.load_factor
        cloned_join.left_hash = build_side_carchar_morsel_map(
            self.left_morsel,
            self.left_columns,
            self.load_factor,
            cloned_join.kernel_metrics,
        )
        cloned_join._build_complete = True
        cloned_join.set_context(self._ctx)


@dataclass(frozen=True)
class _JoinSourcePrep:
    """The ``source_prep`` describing a join's per-worker chain PREFIX for
    ``_run_breaker_segment`` (Step 5). ``shared`` is the ``_SharedSourceJoin`` carrying
    the build state + ``prepare_worker_clone``; ``join_id`` + ``probe_middle_ids`` let
    the skeleton clone the ``[probe-middle* → JoinRightAdapter → cloned_join]`` prefix
    that sits ahead of the agg-middle ops + the agg sink clone. ``probe_scan_id`` is the
    scan the skeleton self-pulls (the PROBE scan, NOT the build scan)."""

    shared: "_SharedSourceJoin"
    join_id: str
    probe_middle_ids: list
    probe_scan_id: str


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


def _pipeline_stream(plan, workers, telemetry=None):
    """The SOLE data-pipeline executor.

    Every data pipeline runs through here. It detects the plan's shape and routes to
    the engine's handler for it:

      * grouped / ungrouped / distinct aggregate → the generic ``PipelineSink``
        contract via the ONE shared breaker-segment skeleton (``_run_breaker_segment``):
        SCALAR_MERGE for ungrouped agg, HASH_REPARTITION for grouped agg + distinct.
      * ``join → agg`` (single inner-equi join feeding an agg breaker) → the SAME
        skeleton, with the SHARED_SOURCE join composed in via a ``source_prep``.
      * bare inner-equi join (``scan → join → stateless* → exit``, no agg) →
        ``_join_probe_stream`` (THREAD-LOCAL-FULL probe).
      * stateless (``scan → {filter,projection}* → exit``, no breaker) →
        ``_stateless_stream``.
      * everything else (multi-join, sort, window, set-ops, limit-only, subqueries,
        bare/projection-only scans) → ``_serial_stream``, the engine's serial drive.

    Detection PRECEDENCE matters: a ``join → agg`` plan also "looks like" a grouped
    agg (the scan-walk from one leg hits the join) and like a bare join, so the
    grouped/ungrouped/distinct finders run FIRST (they reject the two-scan join), then
    join→agg, then bare join, then stateless. ``_serial_stream`` is the fall-through —
    not the "old engine", but this engine's serial path, required for correctness on
    un-parallelisable shapes. DOP=1 / below-floor still drives the ORIGINAL un-cloned
    chain inside each handler (the prime constraint).
    """
    # Reuse the existing finders to delimit the scan→stateless*→breaker segment and
    # classify the breaker — they already enforce the single-scan / all-stateless /
    # mergeable-engine invariants the generic sinks require.
    grouped = _find_parallel_grouped_agg(plan)
    if grouped is not None:
        scan_id, middle_ids, breaker_id = grouped
        breaker = plan[breaker_id]
        recomb = recombination_class_for(breaker)
        if recomb is RecombClass.HASH_REPARTITION:
            if telemetry is not None:
                telemetry._reading["parallel_engaged"] = 1
                telemetry._reading["generic_pipeline"] = 1
            return _run_breaker_segment(
                plan, scan_id, middle_ids, breaker_id, recomb, workers, telemetry
            )

    ungrouped = _find_parallel_ungrouped_agg(plan)
    if ungrouped is not None:
        scan_id, middle_ids, breaker_id = ungrouped
        breaker = plan[breaker_id]
        recomb = recombination_class_for(breaker)
        if recomb is RecombClass.SCALAR_MERGE:
            if telemetry is not None:
                telemetry._reading["parallel_engaged"] = 1
                telemetry._reading["generic_pipeline"] = 1
            return _run_breaker_segment(
                plan, scan_id, middle_ids, breaker_id, recomb, workers, telemetry
            )

    # DISTINCT is also HASH_REPARTITION (same scatter+combine as grouped agg; the
    # finalize DEDUPES instead of aggregating — the per-class adapter map in make_sink
    # picks `_DistinctSink`). It threads `exit_id` (the breaker's real downstream node)
    # so the sink can push its deduped survivors there.
    # DISTINCT row-routing only pays above one worker — the serial dedup is fine, and
    # scatter+thread setup is pure overhead at W=1. At W<2 the sink is skipped and the
    # plan falls to _serial_stream (the byte-identical serial dedup — the prime constraint).
    distinct = _find_parallel_distinct(plan) if workers >= 2 else None
    if distinct is not None:
        scan_id, middle_ids, breaker_id, exit_id = distinct
        breaker = plan[breaker_id]
        recomb = recombination_class_for(breaker)
        if recomb is RecombClass.HASH_REPARTITION:
            if telemetry is not None:
                telemetry._reading["parallel_engaged"] = 1
                telemetry._reading["generic_pipeline"] = 1
            return _run_breaker_segment(
                plan, scan_id, middle_ids, breaker_id, recomb, workers, telemetry,
                exit_id=exit_id,
            )

    # join → agg (Step 5): a SHARED_SOURCE inner-equi join feeding a generic AGG sink.
    # The join's BUILD leg is prepared once serially; W workers each probe a disjoint
    # slice with their OWN private engine, the join output flowing through the agg-middle
    # ops into the SAME agg sink (grouped HASH_REPARTITION / ungrouped SCALAR_MERGE) the
    # plain-agg path uses — composed through the ONE skeleton via a `source_prep`, NO
    # monolithic `_join_agg_stream`.
    #
    # Step 6: try the SINGLE-join shape first, then the MULTI-JOIN shape (TOP join's
    # BUILD leg is a join subtree, PROBE leg a fact scan). Both feed the SAME skeleton
    # via a `source_prep`; the only difference is the build prelude drives one leg vs the
    # whole build subtree (carried on the shape's `build_scan_ids`).
    join_agg = _find_parallel_join_agg(plan) or _find_parallel_multi_join_agg(plan)
    if join_agg is not None:
        breaker = plan[join_agg.breaker_id]
        recomb = recombination_class_for(breaker)
        if recomb in (RecombClass.HASH_REPARTITION, RecombClass.SCALAR_MERGE):
            if telemetry is not None:
                telemetry._reading["parallel_engaged"] = 1
                telemetry._reading["generic_pipeline"] = 1
                telemetry._reading["generic_join_agg"] = 1
            # The shared-source helper needs a ctx to drive the build leg + set the
            # cloned-join context. `_run_breaker_segment` owns the real ctx (it
            # re-compiles), so we build the `_SharedSourceJoin` against THAT ctx by
            # threading it lazily: the helper captures plan+shape now and is bound to the
            # skeleton's ctx inside `build` / `prepare_worker_clone` (it reads
            # `self._ctx`). We pass a placeholder ctx that the skeleton overwrites.
            shared = _SharedSourceJoin(plan, join_agg, ctx=None)
            source_prep = _JoinSourcePrep(
                shared=shared,
                join_id=join_agg.join_id,
                probe_middle_ids=join_agg.probe_middle_ids,
                probe_scan_id=join_agg.probe_scan_id,
            )
            return _run_breaker_segment(
                plan,
                join_agg.probe_scan_id,
                join_agg.agg_middle_ids,
                join_agg.breaker_id,
                recomb,
                workers,
                telemetry,
                source_prep=source_prep,
            )

    # Bare inner-equi join (no agg): scan → join → stateless* → exit. Build once
    # serially, then W workers probe disjoint slices with their own private engines.
    # Checked AFTER join→agg so a join feeding an agg never lands here.
    join = _find_parallel_join(plan)
    if join is not None:
        if telemetry is not None:
            telemetry._reading["parallel_engaged"] = 1
        return _join_probe_stream(plan, join, workers, telemetry)

    # Standalone selection/projection: scan → {filter,projection}* → exit, no breaker.
    # Each worker self-pulls + transforms its own morsels; outputs are concatenated.
    stateless = _find_parallel_stateless(plan)
    if stateless is not None:
        scan_id, op_ids, exit_id = stateless
        if telemetry is not None:
            telemetry._reading["parallel_engaged"] = 1
        return _stateless_stream(plan, scan_id, op_ids, exit_id, workers, telemetry)

    # No parallel strategy handles this shape (multi-join, sort, window, set-ops,
    # limit-only, subqueries, bare/projection-only scans) — serial-inline (the data
    # executor still owns it end-to-end; this is not a punt to another engine).
    if telemetry is not None:
        telemetry._reading["parallel_engaged"] = 0
    return _serial_stream(plan)


def _run_breaker_segment(
    plan,
    scan_id,
    middle_ids,
    breaker_id,
    recomb_class,
    workers,
    telemetry=None,
    exit_id=None,
    source_prep=None,
):
    """The ONE generic breaker-segment skeleton (design §1.2).

    Drives a ``source → stateless* → breaker`` segment in parallel via a
    ``PipelineSink`` adapter. The 7-part skeleton — verified identical across the
    bespoke functions — lives here ONCE; the adapter supplies only
    ``make_local_sink_state`` / ``combine`` / ``finalize_source``. This single
    function serves SCALAR_MERGE (trivial combine + finalize), HASH_REPARTITION agg
    (bin-handoff combine + parallel-readout AGGREGATE finalize), HASH_REPARTITION
    distinct (same bin-handoff combine + parallel-readout DEDUP finalize) AND
    ``join → agg`` (the SHARED_SOURCE join composed with the agg sink — Step 5). The
    distinct-specific thread is ``exit_id`` — the breaker's real downstream node, which
    the distinct sink pushes its deduped survivors into (the operator's ``_downstream``
    pointer is a private ``cdef`` not readable from the sink); ``None`` for the agg
    sinks, which inject their result into the breaker itself and rely on the EOS push.

    ``source_prep`` (a ``_JoinSourcePrep``) is the Step-5 composition seam: when set,
    the self-pulled ``scan`` is the PROBE scan, each worker's chain is prefixed with the
    join's ``[probe-middle* → JoinRightAdapter → cloned_join]`` (its private engine built
    by ``source_prep.shared.prepare_worker_clone``), and ``middle_ids`` are the
    STATELESS agg-middle ops between the join and the AGG breaker. The SAME skeleton
    serves plain agg (``source_prep=None``) and join→agg (``source_prep`` set) — NO
    forked skeleton, NO ``_join_agg_stream``. The build leg is driven serially ONCE
    before the worker fan-out (the build-before-probe dependency).

    Steps:
      1. ``compile_pipeline``.
      1b. (join→agg only) drive the BUILD leg serially → capture shared build state.
      2. ROW-FLOOR serial fallback: below ``PARALLEL_MIN_ROWS`` (or empty build side)
         drive the ORIGINAL un-cloned chain through the ORIGINAL breaker (byte-identical
         to serial — the DOP=1 guarantee, §3). For join→agg the original join (built by
         step 1b) + original agg are driven from the PROBE chain.
      3. W self-pull workers: each ``sink.make_local_sink_state(k)`` → a cloned
         ``[ (probe-prefix) → middle → clone-breaker]`` chain; self-pull disjoint
         morsels (gated on ``is_concurrent_pull_safe``, buffered-floor-first).
      4. errors barrier; ``sink.combine(locals)``.
      5. ``finalize_source()`` read-out, then push EOS through the ORIGINAL breaker
         and drain the exit (the read-out flows downstream → exit).
      6. ``finally``: pool shutdown, ctx terminate, close source(s).
    """
    import threading

    from opteryx.compiled.thread_pool import CppThreadPool
    from opteryx.managers.execution.pipeline_compiler import compile_pipeline
    from opteryx.operators._operators import pull_one

    # ---- 1. compile ---------------------------------------------------------------
    chains, exit_node, ctx = compile_pipeline(plan)
    scan = plan[scan_id]
    breaker = plan[breaker_id]
    head = plan[middle_ids[0]] if middle_ids else breaker

    # join→agg: the probe chain HEAD is the join's probe-leg head (probe-middle* or the
    # join itself), resolved from the compiler's wired chains. The ORIGINAL probe chain
    # already runs join → agg-middle* → breaker → exit, so driving it serially (floor /
    # empty build) is byte-identical to serial.
    probe_head = None
    build_chains = None
    build_scans = None
    if source_prep is not None:
        # Bind the skeleton's ctx into the shared-source helper (it drives the build side
        # + sets the cloned-join context against THIS ctx, the one compile_pipeline made).
        source_prep.shared._ctx = ctx
        scan = plan[source_prep.probe_scan_id]
        shape = source_prep.shared._shape
        # The BUILD chains = the single build leg (single-join), or EVERY scan chain
        # except the probe fact scan (MULTI-JOIN subtree, Step 6). `chains` is in compiler
        # DFS order (build legs before probe), so the build subtree is driven in
        # dependency order by iterating it. The probe chain is resolved separately.
        if shape.build_scan_ids is not None:
            build_ids = set(shape.build_scan_ids)
        else:
            build_ids = {shape.build_scan_id}
        # Map each scan NODE (by object identity) in the compiler's chains back to its
        # plan id, so the build-set membership test is keyed on the plan id without
        # depending on node __eq__/__hash__ (some plan nodes override equality).
        scan_obj_to_id = {
            id(plan[nid]): nid
            for nid in plan.nodes()
            if getattr(plan[nid], "is_scan", False)
        }
        build_chains = []
        build_scans = []
        for sc, hd in chains:
            if sc is scan:
                probe_head = hd
            elif scan_obj_to_id.get(id(sc)) in build_ids:
                build_chains.append((sc, hd))
                build_scans.append(sc)
        head = probe_head  # the floor/empty path drives the ORIGINAL probe chain
        if probe_head is None or len(build_chains) != len(build_ids):
            # Compiler did not produce the expected legs — refuse to guess, run serial.
            ctx.terminate()
            yield from _serial_stream(plan)
            return

    def _drain():
        if exit_node is None:
            return
        while exit_node.has_pending():
            yield exit_node.pop_pending()

    # radix for HASH_REPARTITION: next power of two >= workers (>= DOP read-out width).
    radix = 1
    while radix < workers:
        radix <<= 1

    pool = None
    try:
        # ---- 1b. join→agg: drive the BUILD side ONCE, serially (build-before-probe) --
        # build_chains is the single build leg (single-join) or the WHOLE build subtree
        # (multi-join, Step 6), driven in compiler DFS order.
        if source_prep is not None:
            source_prep.shared.build(build_chains, exit_node)
            if ctx.is_terminated():
                return
            if source_prep.shared.left_is_empty:
                # Empty build side: an inner join yields nothing. Push EOS through the
                # ORIGINAL wired probe chain once so the join's EOS path (→ agg → exit)
                # runs exactly once, emitting the empty grouped/ungrouped result
                # identically to serial.
                push_one(head, _EOS_SENTINEL)
                yield from _drain()
                return

        # ---- 2. row-floor: tiny inputs run serially through the ORIGINAL breaker --
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
            # Drive the ORIGINAL un-cloned chain through the ORIGINAL breaker — its
            # real engine, untouched. Byte-identical to serial (the prime constraint).
            # For join→agg this is the original probe chain (join built by step 1b →
            # agg-middle* → original breaker → exit).
            for morsel in buffer:
                if ctx.is_terminated():
                    break
                push_one(head, morsel)
                yield from _drain()
            if not ctx.is_terminated():
                push_one(head, _EOS_SENTINEL)
                yield from _drain()
            return

        # Build the sink adapter. HASH_REPARTITION needs the radix + a read-out pool
        # factory (the skeleton owns pool lifecycle; finalize spawns the read-out).
        def _readout_pool_factory():
            return CppThreadPool(max(1, min(radix, workers)), "m4-generic-readout")

        # The distinct sink pushes its deduped survivors into the breaker's real
        # downstream node (its `_downstream` cdef pointer isn't readable from Python),
        # so resolve it from `exit_id` when the caller threaded one through. The agg
        # sinks ignore `downstream` (they inject into the breaker + rely on the EOS push).
        downstream = plan[exit_id] if exit_id is not None else None
        sink = make_sink(
            breaker,
            recomb_class,
            radix=radix,
            pool_factory=_readout_pool_factory,
            ctx=ctx,
            downstream=downstream,
            telemetry=telemetry,
        )
        if sink is None:
            # Unhandled recombination class — fail loud (the dispatcher must only
            # reach here for a class the sink factory builds).
            raise RuntimeError(
                f"_run_breaker_segment: no sink adapter for {recomb_class!r}"
            )

        if telemetry is not None:
            telemetry._reading["generic_pipeline_workers"] = workers
            if recomb_class is RecombClass.HASH_REPARTITION:
                telemetry._reading["generic_pipeline_radix"] = radix

        # ---- 3. W self-pull workers, each driving its local sink ------------------
        buf_iter = iter(buffer)
        pull_lock = threading.Lock()
        concurrent_safe = scan.is_concurrent_pull_safe()

        def next_input():
            # Buffered floor sample first, then self-pull the scan. The pull is
            # lockless only when the source is reentrant; otherwise serialise the
            # whole pull (a correctness gate, not a perf toggle).
            if concurrent_safe:
                with pull_lock:
                    m = next(buf_iter, None)
                if m is not None:
                    return m
                if ctx.is_terminated():
                    return None
                return pull_one(scan)
            with pull_lock:
                if ctx.is_terminated():
                    return None
                m = next(buf_iter, None)
                if m is not None:
                    return m
                return pull_one(scan)

        local_states = [None] * workers
        local_rows = [0] * workers
        errors = [None] * workers

        def worker(index):
            try:
                clone = sink.make_local_sink_state(index)
                # Wire [middle clones → clone-breaker]; the clone is the local sink
                # (its seam IS the accumulator). The breaker clone has no downstream —
                # the worker only ingests (never EOS), so it never emits.
                chain = [_clone_op(plan[nid]) for nid in middle_ids]
                chain.append(clone)
                for i, op in enumerate(chain):
                    op.set_context(ctx)
                    if i + 1 < len(chain):
                        op.set_downstream(chain[i + 1])
                chain_head = chain[0]

                # join→agg: prepend the join's per-worker prefix
                # [probe-middle* → JoinRightAdapter → cloned_join] ahead of the
                # agg-middle chain. The cloned join gets its OWN private engine over the
                # shared READ-ONLY left_morsel (zero shared mutable probe state). The
                # join's output flows into `chain_head` (agg-middle* → local sink).
                if source_prep is not None:
                    from opteryx.operators import JoinRightAdapter

                    cloned_join = _clone_op(plan[source_prep.join_id])
                    source_prep.shared.prepare_worker_clone(cloned_join)
                    cloned_join.set_downstream(chain_head)
                    probe_ops = [
                        _clone_op(plan[nid]) for nid in source_prep.probe_middle_ids
                    ]
                    adapter = JoinRightAdapter(cloned_join)
                    adapter.set_context(ctx)
                    probe_chain = probe_ops + [adapter]
                    for i, op in enumerate(probe_chain):
                        op.set_context(ctx)
                        if i + 1 < len(probe_chain):
                            op.set_downstream(probe_chain[i + 1])
                    chain_head = probe_chain[0]

                count = 0
                while True:
                    morsel = next_input()
                    if morsel is None:
                        break
                    push_one(chain_head, morsel)
                    count += morsel.num_rows
                local_states[index] = clone
                local_rows[index] = count
            except BaseException as exc:  # noqa: BLE001 — surface on the main thread
                errors[index] = exc

        pool = CppThreadPool(workers, "m4-generic-sink")
        futures = [pool.submit(worker, k) for k in range(workers)]
        for future in futures:
            future.result()
        # ---- 4. errors barrier; combine ------------------------------------------
        for exc in errors:
            if exc is not None:
                raise exc
        if ctx.is_terminated():
            return

        locals_ = [
            (local_states[k], local_rows[k])
            for k in range(workers)
            if local_states[k] is not None
        ]
        sink.combine(locals_)

        # ---- 5. finalize read-out, then push terminal EOS through the sink's EOS
        #         target. For the agg sinks the target IS the ORIGINAL breaker (whose
        #         _finalize emits the injected result). For distinct the breaker holds
        #         NO state (its survivors went straight to its downstream), so its EOS
        #         target is the real downstream node — pushing EOS through the empty
        #         breaker would re-enter its dedup-init on the EOS sentinel and crash.
        #         Either way the EOS flows to the Exit, which emits the empty-result
        #         schema morsel when nothing survived (byte-identical to serial).
        for m in sink.finalize_source():
            if ctx.is_terminated():
                return
            yield m
        if ctx.is_terminated():
            return
        push_one(sink.eos_target(), _EOS_SENTINEL)
        yield from _drain()
    finally:
        # ---- 6. teardown ----------------------------------------------------------
        if pool is not None:
            pool.shutdown(wait=True)
        ctx.terminate()
        scan.close_source()
        if source_prep is not None and build_scans is not None:
            for bs in build_scans:
                bs.close_source()


def execute(plan, head_node=None, telemetry=None):
    """The data executor (M4).

    There is ONE data-pipeline engine: ``_pipeline_stream``. Every data pipeline runs
    through it — it detects the plan's shape and routes to the right handler (grouped /
    ungrouped / distinct agg + join→agg through the generic ``PipelineSink`` contract;
    bare inner join through ``_join_probe_stream``; stateless through
    ``_stateless_stream``; everything else serial-inline through ``_serial_stream``).
    No dual path, no flag, no bespoke per-shape cascade. The data executor owns
    execution end-to-end — it never punts to a hidden fallback in another engine.
    Non-pipeline special ops (EXPLAIN/INSERT/DDL) never reach here; the dispatcher in
    ``managers/execution/__init__.py`` routes those to serial_engine.
    """
    workers = resolve_worker_count(config.MAX_EXECUTION_WORKERS)
    return _pipeline_stream(plan, workers, telemetry), ResultType.TABULAR


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


def _join_probe_stream(plan, shape, workers, telemetry=None):
    """Parallel INNER-EQUI-JOIN by THREAD-LOCAL-FULL probe (M4).

    The build side is prepared ONCE serially through the normal push chain — the
    join accumulates its left input and builds ``left_hash`` at left-EOS, emitting
    NOTHING downstream during the build. Then W workers each rebuild their OWN
    private join engine (a fresh ``build_side_carchar_morsel_map`` over the shared
    READ-ONLY ``left_morsel``) and probe a DISJOINT slice of the probe scan,
    streaming matches through a cloned ``[probe-middle* → JoinRightAdapter →
    join → downstream* → Exit]`` chain into a shared queue. No worker mutates the
    original join, and the shared ``left_morsel`` is immutable column data — so no
    data race, and no draken/C++ change. Probe emission is unordered across
    workers; the finder guarantees no downstream op depends on order."""
    import queue as _queue
    import threading

    from opteryx.compiled.thread_pool import CppThreadPool
    from opteryx.managers.execution.pipeline_compiler import compile_pipeline
    from opteryx.operators._operators import build_side_carchar_morsel_map
    from opteryx.operators._operators import drive_scan
    from opteryx.operators._operators import pull_one
    from opteryx.operators import JoinRightAdapter

    chains, exit_node, ctx = compile_pipeline(plan)
    build_scan = plan[shape.build_scan_id]
    probe_scan = plan[shape.probe_scan_id]
    j = plan[shape.join_id]

    # Resolve the (scan, chain_head) pairs the compiler wired for each leg.
    build_head = None
    probe_head = None
    for scan, head in chains:
        if scan is build_scan:
            build_head = head
        elif scan is probe_scan:
            probe_head = head
    if build_head is None or probe_head is None:
        # Compiler did not produce the expected legs — refuse to guess, run serial.
        ctx.terminate()
        yield from _serial_stream(plan)
        return

    def _drain():
        if exit_node is None:
            return
        while exit_node.has_pending():
            yield exit_node.pop_pending()

    pool = None
    try:
        # ---- SERIAL BUILD PRELUDE ------------------------------------------------
        # Drive the BUILD leg to completion. The join accumulates left + builds
        # left_hash at left-EOS and emits NOTHING downstream (build-side EOS is
        # absorbed), so this yields nothing — it just builds the engine on j.
        for _ in drive_scan(build_scan, build_head, exit_node, ctx):
            pass
        if ctx.is_terminated():
            return

        # Capture the prepared build state (post ON-eval / projection).
        left_morsel = j.left_morsel
        left_columns = j.left_columns
        left_is_empty = j.left_is_empty
        columns = j.columns
        load_factor = j.carchar_probe_load_factor

        # Empty build side: an inner join yields nothing. Push EOS through the
        # original wired probe chain once so the join's EOS path (telemetry flush +
        # downstream EOS → the schema morsel) runs exactly once, then return.
        if left_is_empty:
            push_one(probe_head, _EOS_SENTINEL)
            yield from _drain()
            return

        # ---- Row-floor: tiny probe inputs run serially (clone + thread setup
        # dominates). Buffer the floor sample, and if the probe is below the floor
        # drive the WHOLE join serially through the original probe chain — this is
        # byte-identical to _serial_stream's probe drive (and to DOP=1).
        buffer = []
        buffered_rows = 0
        exhausted = False
        while buffered_rows < config.PARALLEL_MIN_ROWS:
            morsel = pull_one(probe_scan)
            if morsel is None:
                exhausted = True
                break
            buffer.append(morsel)
            buffered_rows += morsel.num_rows

        if exhausted and buffered_rows < config.PARALLEL_MIN_ROWS:
            for morsel in buffer:
                if ctx.is_terminated():
                    break
                push_one(probe_head, morsel)
                yield from _drain()
            if not ctx.is_terminated():
                push_one(probe_head, _EOS_SENTINEL)
                yield from _drain()
            return

        # ---- PARALLEL PROBE ------------------------------------------------------
        out_q = _queue.Queue()
        DONE = object()
        errors = [None] * workers
        buf_iter = iter(buffer)
        pull_lock = threading.Lock()
        concurrent_safe = probe_scan.is_concurrent_pull_safe()

        def _next_input():
            # Buffered floor sample first (locked; tiny), then self-pull the scan.
            # The pull is lockless only when the source is reentrant; otherwise the
            # whole pull is serialised (a correctness gate, not a perf toggle).
            if concurrent_safe:
                with pull_lock:
                    m = next(buf_iter, None)
                if m is not None:
                    return m
                if ctx.is_terminated():
                    return None
                return pull_one(probe_scan)
            with pull_lock:
                if ctx.is_terminated():
                    return None
                m = next(buf_iter, None)
                if m is not None:
                    return m
                return pull_one(probe_scan)

        def worker(index):
            # Each worker owns a fresh, private join clone + its own engine built
            # over the shared READ-ONLY left_morsel. No worker touches `j`.
            clone_join = _clone_op(j)
            clone_join.left_morsel = left_morsel
            clone_join.left_columns = left_columns
            clone_join.columns = columns
            clone_join.left_is_empty = left_is_empty
            clone_join.carchar_probe_load_factor = load_factor
            clone_join.left_hash = build_side_carchar_morsel_map(
                left_morsel, left_columns, load_factor, clone_join.kernel_metrics
            )
            clone_join._build_complete = True
            clone_join.set_context(ctx)

            # Build the worker's downstream chain: clone_join → downstream* → Exit.
            downstream_chain = [_clone_op(plan[nid]) for nid in shape.downstream_ids]
            exit_clone = _clone_op(plan[shape.exit_id])
            tail = downstream_chain + [exit_clone]
            for i, op in enumerate(tail):
                op.set_context(ctx)
                if i + 1 < len(tail):
                    op.set_downstream(tail[i + 1])
            clone_join.set_downstream(tail[0])

            # Build the worker's probe-ops chain → adapter (the adapter routes into
            # clone_join.push_right; adapters have NO downstream of their own).
            probe_ops = [_clone_op(plan[nid]) for nid in shape.probe_middle_ids]
            adapter = JoinRightAdapter(clone_join)
            adapter.set_context(ctx)
            probe_chain = probe_ops + [adapter]
            for i, op in enumerate(probe_chain):
                op.set_context(ctx)
                if i + 1 < len(probe_chain):
                    op.set_downstream(probe_chain[i + 1])
            probe_head_clone = probe_chain[0]

            try:
                while True:
                    morsel = _next_input()
                    if morsel is None:
                        break
                    push_one(probe_head_clone, morsel)
                    while exit_clone.has_pending():
                        out_q.put(exit_clone.pop_pending())
                if not ctx.is_terminated():
                    # Probe-side EOS: clone_join flushes + emits EOS to exit_clone.
                    push_one(probe_head_clone, _EOS_SENTINEL)
                    while exit_clone.has_pending():
                        out_q.put(exit_clone.pop_pending())
            except BaseException as exc:  # noqa: BLE001 — surface on the main thread
                errors[index] = exc
            finally:
                out_q.put(DONE)

        pool = CppThreadPool(workers, "m4-join-probe")
        futures = [pool.submit(worker, k) for k in range(workers)]

        # Stream matches to the caller as workers produce them.
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

        # An all-non-matching join still needs the schema morsel the Exit emits on
        # EOS. The per-worker Exit clones each emit their own EOS schema morsel
        # already when they produced rows; when NO worker produced anything, drive
        # a single EOS through the original chain so the schema morsel surfaces.
        if not yielded and not ctx.is_terminated():
            push_one(probe_head, _EOS_SENTINEL)
            yield from _drain()
    finally:
        if pool is not None:
            pool.shutdown(wait=True)
        ctx.terminate()
        build_scan.close_source()
        probe_scan.close_source()
