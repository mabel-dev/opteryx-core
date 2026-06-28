# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Parallel execution substrate — the M4 data-pipeline machinery.

The engine owns parallelism; the OPERATORS stay parallel-unaware (the founding
principle). This module is the kept SUBSTRATE (design §5 "Kept"): the shape-router
``dispatch_data_pipeline`` and every per-shape handler, plus ``identify_segments`` /
``Segment`` / ``resolve_worker_count`` / ``_clone_op`` / the ``_run_breaker_segment``
skeleton / ``_SharedSourceJoin``. There is NO ``execute`` entry here any more — the
SOLE data executor is ``scheduler_engine.py``, which hosts this substrate under its
Event/Executor DAG (one Event per pipeline segment; build-before-probe and
multi-segment build ordering are ``add_dependency`` edges). There is ONE skeleton and
ONE live executor — no dual path.

``dispatch_data_pipeline`` routes the plan to the handler for its shape:

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
never punts to a hidden fallback in another engine. (Multi-join + agg DOES parallelise
via the build prelude — see ``_find_parallel_multi_join_agg``.)

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
from opteryx.managers.execution.pipeline_sink import RecombClass
from opteryx.managers.execution.pipeline_sink import make_sink
from opteryx.managers.execution.pipeline_sink import parallel_sink_spec_for
from opteryx.managers.execution.pipeline_sink import recombination_class_for
from opteryx.operators._operators import accumulate_worker_drive
from opteryx.operators._operators import native_accumulate_fanout
from opteryx.operators._operators import push_one
from opteryx.operators.catalog import OperatorParallelism
from opteryx.operators.catalog import get_registry

# Hard cap on parallel width: past 8 workers every prototype regressed (the
# merge/recombination tail and DRAM bandwidth dominate). See
# docs/M4_PARALLEL_AGG_PROTOTYPE.md.
_MAX_WORKER_CAP = 8


_oversubscribe_warned = False


def resolve_worker_count(requested) -> int:
    """Resolve the effective worker count for a query.

    Two cases, and ONLY the first is ever capped:

    - **Unset / "auto" / impossible (``None`` or ``<= 0``):** the worker count is
      SOFTCODED — ``max(1, min(cpu - 2, _MAX_WORKER_CAP))`` — 2 cores reserved for the
      C++ IO/decode pool + main thread, never above the measured regression boundary.
    - **Explicit positive request:** HONOURED EXACTLY. Never clamped, never silently
      overridden — not to the cap, not to the physical core count. If the operator asks
      for 128 workers, they get 128. CLAUDE.md is law here: no hidden behaviour, the user
      is the architect, intent is obeyed not assumed. Oversubscription (request > cores)
      is *warned* (once), never quietly reduced.

    Worker count is degree-of-parallelism ONLY — it never selects a code path; an
    explicit ``1`` is a single worker, not "the serial engine".
    """
    cpu = os.cpu_count() or 1
    if requested is None or requested <= 0:
        # Leave 2 cores for IO/decode + the main orchestration thread.
        return max(1, min(cpu - 2, _MAX_WORKER_CAP))
    requested = int(requested)
    if requested > cpu:
        global _oversubscribe_warned
        if not _oversubscribe_warned:
            _oversubscribe_warned = True
            import warnings

            warnings.warn(
                f"MAX_EXECUTION_WORKERS={requested} exceeds {cpu} physical cores "
                f"(oversubscribed). Honouring the explicit request as set.",
                stacklevel=2,
            )
    return max(1, requested)


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


def _find_parallel_breaker_segment(plan, workers):
    """Return ``(scan_id, middle_ids, breaker_id, recomb_class, downstream_id)`` for a
    single-scan ``scan → stateless* → breaker`` pipeline whose breaker DECLARES a
    parallel-sink capability (its catalog ``parallel_sink``), else ``None``.

    This is the capability-resolved, segment-driven path that retires the three
    bespoke single-breaker matchers — ``_find_parallel_grouped_agg`` (HASH_REPARTITION
    agg), ``_find_parallel_ungrouped_agg`` (SCALAR_MERGE) and ``_find_parallel_distinct``
    (HASH_REPARTITION distinct) — with ONE walk (design §1.4 Phase B). The structural
    gates the matchers hand-coded — single scan, every middle op STATELESS — are
    per-operator checks along the segment here; the operator-specific eligibility
    (a mergeable ungrouped engine, a grouped breaker with real group columns) and the
    DOP floor (distinct only pays at ``W >= 2``) are DECLARED on the spec, so adding a
    breaker to this path is a one-file catalog change, not a new matcher.

    ``downstream_id`` is the breaker's immediate downstream node id — the distinct sink
    pushes its deduped survivors there (its ``_downstream`` cdef pointer is not readable
    from Python); the agg sinks ignore it. A breaker with no downstream is degenerate
    → rejected.
    """
    registry = get_registry()
    scans = [nid for nid in plan.nodes() if getattr(plan[nid], "is_scan", False)]
    if len(scans) != 1:
        return None
    scan_id = scans[0]
    segment = next((s for s in identify_segments(plan) if s.nodes[0] == scan_id), None)
    if segment is None or not segment.tail_is_breaker:
        return None
    breaker_id = segment.tail
    breaker = plan[breaker_id]
    # The breaker's DECLARED sink capability — None → no generic sink → serial.
    spec = parallel_sink_spec_for(breaker)
    if spec is None:
        return None
    # DOP floor (distinct's serial dedup is fine at W=1 — scatter+thread setup is pure
    # overhead there; the agg sinks declare 1 and engage at any width).
    if workers < spec.min_workers:
        return None
    # Per-instance eligibility (mergeable ungrouped engine; grouped real key columns).
    if spec.eligible is not None and not spec.eligible(breaker):
        return None
    # Every middle op between scan and breaker must be a ParallelOperator (STATELESS) —
    # the "min over operator hints" along the segment (DuckDB §8).
    middle_ids = list(segment.nodes[1:-1])
    for nid in middle_ids:
        meta = registry.get(type(plan[nid]))
        if meta is None or meta.parallelism != OperatorParallelism.STATELESS:
            return None
    outs = list(plan.outgoing_edges(breaker_id))
    if not outs:
        return None  # a breaker with no downstream is degenerate — reject
    return scan_id, middle_ids, breaker_id, spec.recomb_class, outs[0][1]


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


def _scans_reachable_upstream(plan, start_id):
    """Every scan id transitively reachable by walking the INGOING edges from
    ``start_id`` (inclusive). Used to collect the scans on one join leg's subtree
    without assuming how deeper joins nest."""
    seen = set()
    out = set()
    stack = [start_id]
    while stack:
        nid = stack.pop()
        if nid in seen:
            continue
        seen.add(nid)
        if getattr(plan[nid], "is_scan", False):
            out.add(nid)
        for provider, _t, _label in plan.ingoing_edges(nid):
            stack.append(provider)
    return out


@dataclass(frozen=True)
class _BuildPreludeJoin:
    """The build subtree's OUTERMOST inner-equi join, resolved for PARALLEL probing
    (the build-prelude parallelization, design §4-Step6 follow-on).

    The TOP join's BUILD (left) leg is itself a join (e.g. ``customer ⋈ orders`` feeding
    ``(customer⋈orders) ⋈ lineitem``). That inner join is driven SERIALLY today inside
    ``_SharedSourceJoin.build`` — an Amdahl anchor (Q3: 29% of wall). When the inner join
    is a clean parallelisable inner-equi join (passes ``_is_safe_probe_join``), with its
    OWN build leg a small dimension scan chain and its probe leg a raw fact scan, its
    probe is parallelised exactly like the TOP join's: build the inner build leg serially,
    PARALLEL-probe the inner probe leg, MATERIALISE the joined result, then replay it
    serially into the TOP join's left adapter (so the TOP join builds from the fully
    materialised intermediate — build-before-probe preserved).

    ``inner_join_id`` — the inner join (the TOP join's left-leg join).
    ``inner_build_scan_ids`` — the inner join's BUILD-side scan ids (driven serially).
    ``inner_probe_scan_id`` — the inner join's PROBE fact scan (parallelised).
    ``inner_probe_middle_ids`` — stateless ops between the inner probe scan and the inner
        join (cloned per worker ahead of the inner join clone).
    """

    inner_join_id: str
    inner_build_scan_ids: tuple
    inner_probe_scan_id: str
    inner_probe_middle_ids: list


def _resolve_build_prelude_join(plan, top_join_id, probe_scan_id, registry):
    """Resolve the TOP join's BUILD (left) leg's OUTERMOST join for PARALLEL probing, or
    ``None`` if it cannot be parallelised cleanly (→ the build prelude stays serial).

    Walk from the TOP join's LEFT provider up to the first join — the inner join feeding
    the TOP join's build. It is parallelisable iff:
      * it is a ``DrakenInnerJoinNode`` with empty ``_compiled_right_evals`` (the probe
        race gate — ``_is_safe_probe_join``);
      * its PROBE (right) leg is a clean ``scan → stateless* → join`` chain whose scan is
        NOT the TOP join's fact probe (a distinct fact table — e.g. ``orders``);
      * its BUILD (left) leg resolves to one-or-more scan chains, none of which is the TOP
        join's fact probe.

    One level deep only — the inner join's OWN build leg (if itself a join subtree) is
    driven serially. Left-deep TPC-H plans (Q3/Q5/Q7/Q9) put the anchor at this single
    level, so this captures it; deeper recursion is deliberately not attempted (correct
    one-level beats flaky N-level — the per-worker hash rebuild is the residual cost, a
    separate optimization)."""
    legs = {}
    for idx, (provider, _t, label) in enumerate(plan.ingoing_edges(top_join_id)):
        if not label:
            label = "left" if idx == 0 else "right"
        legs[label] = provider
    if "left" not in legs:
        return None

    # Walk the TOP join's LEFT leg up to the first join — that is the inner join. The
    # left leg may carry stateless ops between the inner join and the TOP join; the build
    # prelude drives those serially as part of the replay (they sit on the inner join's
    # downstream chain), so we only need to find the inner join itself.
    cur = legs["left"]
    inner_join_id = None
    while True:
        node = plan[cur]
        if getattr(node, "is_join", False):
            inner_join_id = cur
            break
        meta = registry.get(type(node))
        if meta is None or meta.parallelism != OperatorParallelism.STATELESS:
            return None  # a breaker between the inner join and the TOP join → serial
        ins = list(plan.ingoing_edges(cur))
        if len(ins) != 1:
            return None
        cur = ins[0][0]

    if not _is_safe_probe_join(plan, inner_join_id, registry):
        return None

    ilegs = {}
    for idx, (provider, _t, label) in enumerate(plan.ingoing_edges(inner_join_id)):
        if not label:
            label = "left" if idx == 0 else "right"
        ilegs[label] = provider
    if "left" not in ilegs or "right" not in ilegs:
        return None

    inner_probe_leg = _walk_leg_to_scan(plan, ilegs["right"], inner_join_id, registry)
    if inner_probe_leg is None:
        # The inner join's probe (right) leg is NOT a raw fact scan — this engine
        # parallelises only a fact-scan probe, so leave the inner join serial.
        return None
    inner_probe_scan_id, inner_probe_middle_ids = inner_probe_leg
    if inner_probe_scan_id == probe_scan_id:
        return None  # the inner probe IS the TOP fact probe — degenerate, reject

    # The inner join's BUILD side = every scan reachable on the inner join's LEFT subtree
    # (driven serially by the prelude before the inner probe fans out). Collected by a
    # transitive walk of the ingoing edges from the inner join's left provider; the TOP
    # probe scan must NOT appear here (it would mean the legs were mis-labelled).
    inner_build_scan_ids = _scans_reachable_upstream(plan, ilegs["left"])
    if not inner_build_scan_ids or probe_scan_id in inner_build_scan_ids:
        return None
    if inner_probe_scan_id in inner_build_scan_ids:
        return None

    return _BuildPreludeJoin(
        inner_join_id=inner_join_id,
        inner_build_scan_ids=inner_build_scan_ids,
        inner_probe_scan_id=inner_probe_scan_id,
        inner_probe_middle_ids=inner_probe_middle_ids,
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
        "_workers",
        "_prelude_join",
        "left_morsel",
        "left_columns",
        "left_is_empty",
        "columns",
        "load_factor",
        "built_hash",
    )

    def __init__(self, plan, shape, ctx, workers=1, prelude_join=None):
        self._plan = plan
        self._shape = shape
        self._ctx = ctx
        # The build prelude's worker count and the resolved inner join to parallelise
        # (``_BuildPreludeJoin`` or ``None``). When ``None`` (or W<2) the prelude drives
        # the ORIGINAL build chains serially, byte-identical to before.
        self._workers = workers
        self._prelude_join = prelude_join
        self.left_morsel = None
        self.left_columns = None
        self.left_is_empty = False
        self.columns = None
        self.load_factor = None
        self.built_hash = None

    def build(self, build_chains, exit_node):
        """Drive the BUILD side to completion, then capture the shared read-only build
        state. Run ONCE, before the worker fan-out (the build-before-probe edge).

        ``build_chains`` is a list of ``(scan, chain_head)`` pairs in compiler DFS order
        (build legs before probe). For a SINGLE-build-leg join it is one pair; for a
        MULTI-JOIN subtree (Step 6) it is EVERY scan chain except the probe fact scan —
        driving them in order builds each inner join, and the inner joins' outputs flow
        through the TOP join's left adapter, so the TOP join ends with its prepared
        ``left_morsel``/``left_hash`` exactly as the single-leg case.

        BUILD-PRELUDE PARALLELISATION: when ``self._prelude_join`` is set (the TOP join's
        BUILD leg is itself a parallelisable inner-equi join) AND W>=2, the inner join's
        big PROBE leg is parallel-probed instead of driven serially — the Amdahl anchor.
        Otherwise the ORIGINAL serial drive runs (byte-identical to before)."""
        from opteryx.operators._operators import drive_scan

        j = self._plan[self._shape.join_id]

        # Fast path / DOP=1 / no parallelisable inner join: drive every build chain
        # serially, byte-identical to the original prelude (the prime constraint).
        if self._prelude_join is None or self._workers < 2:
            for build_scan, build_head in build_chains:
                for _ in drive_scan(build_scan, build_head, exit_node, self._ctx):
                    pass
                if self._ctx.is_terminated():
                    return
            self._capture(j)
            return

        # PARALLEL build prelude: split the build chains into the inner join's BUILD
        # legs, its PROBE leg, and any OTHER build chains (dimensions joined directly to
        # the TOP join). Drive the inner build legs + other chains serially; parallel-probe
        # the inner join; replay its materialised output into the TOP join, then drive any
        # remaining serial build chains. If the split does not match the expectation, fall
        # back to the serial drive (refuse to guess).
        prelude = self._prelude_join
        inner_build_ids = set(prelude.inner_build_scan_ids)
        scan_obj_to_id = {
            id(self._plan[nid]): nid
            for nid in self._plan.nodes()
            if getattr(self._plan[nid], "is_scan", False)
        }
        inner_build_chains = []
        inner_probe_chain = None
        other_chains = []
        for sc, hd in build_chains:
            sid = scan_obj_to_id.get(id(sc))
            if sid == prelude.inner_probe_scan_id:
                inner_probe_chain = (sc, hd)
            elif sid in inner_build_ids:
                inner_build_chains.append((sc, hd))
            else:
                other_chains.append((sc, hd))
        if inner_probe_chain is None or len(inner_build_chains) != len(inner_build_ids):
            # Compiler did not produce the expected legs — drive serially.
            for build_scan, build_head in build_chains:
                for _ in drive_scan(build_scan, build_head, exit_node, self._ctx):
                    pass
                if self._ctx.is_terminated():
                    return
            self._capture(j)
            return

        self._build_parallel_inner(
            j, inner_build_chains, inner_probe_chain, other_chains, exit_node
        )

    def _capture(self, j):
        """Capture the prepared read-only build state from the TOP join ``j`` —
        including the BUILT probe table (``left_hash``) the worker clones SHARE."""
        self.left_morsel = j.left_morsel
        self.left_columns = j.left_columns
        self.left_is_empty = j.left_is_empty
        self.columns = j.columns
        self.load_factor = j.carchar_probe_load_factor
        self.built_hash = j.left_hash

    def _build_parallel_inner(
        self, top_join, inner_build_chains, inner_probe_chain, other_chains, exit_node
    ):
        """Parallel-probe the build prelude's inner join, materialise its output, replay it
        into the TOP join, then build the TOP join.

        1. Drive the inner join's BUILD legs serially → the inner join builds its hash.
        2. PARALLEL-probe the inner join's fact scan: W workers each own a private clone of
           the inner join (built over the shared read-only inner ``left_morsel``) and probe
           a DISJOINT slice, ACCUMULATING the joined output into a per-worker clone of the
           TOP join (via a ``JoinLeftAdapter`` — data morsels only, no EOS, so no build).
        3. Replay every worker's accumulated morsels serially into the REAL TOP join's left
           adapter, then EOS → the TOP join builds from the fully materialised intermediate
           (build-before-probe preserved). Any ``other_chains`` (dimensions joined directly
           to the TOP join) are driven serially first so they accumulate too."""
        import threading

        from opteryx.compiled.thread_pool import CppThreadPool
        from opteryx.operators._operators import build_side_carchar_morsel_map
        from opteryx.operators._operators import drive_scan
        from opteryx.operators._operators import pull_one
        from opteryx.operators._operators import push_one
        from opteryx.operators import JoinLeftAdapter
        from opteryx.operators import JoinRightAdapter

        ctx = self._ctx
        inner = self._plan[self._prelude_join.inner_join_id]

        # ---- Other build chains (e.g. a dimension joined directly to the TOP join) and
        # the inner build legs are driven serially. The inner build legs build the inner
        # join's hash; other chains accumulate into the TOP join's left adapter. Driving
        # other chains FIRST keeps the TOP-join accumulation order independent of the
        # parallel replay (the TOP join combines all left morsels at its left-EOS, so order
        # within the left side does not change the build hash).
        for build_scan, build_head in other_chains:
            for _ in drive_scan(build_scan, build_head, exit_node, ctx):
                pass
            if ctx.is_terminated():
                return
        for build_scan, build_head in inner_build_chains:
            for _ in drive_scan(build_scan, build_head, exit_node, ctx):
                pass
            if ctx.is_terminated():
                return

        # Capture the inner join's read-only build state (post ON-eval / projection).
        inner_left_morsel = inner.left_morsel
        inner_left_columns = inner.left_columns
        inner_left_is_empty = inner.left_is_empty
        inner_columns = inner.columns
        inner_load_factor = inner.carchar_probe_load_factor

        inner_probe_scan = inner_probe_chain[0]

        # Empty inner build side → the inner join yields nothing → the TOP join's left is
        # empty. Push EOS through the TOP join's left adapter so it finalises (empty) build.
        top_left_adapter = JoinLeftAdapter(top_join)
        top_left_adapter.set_context(ctx)
        if inner_left_is_empty:
            push_one(top_left_adapter, _EOS_SENTINEL)
            self._capture(top_join)
            return

        # ROW-FLOOR: buffer the inner probe up to PARALLEL_MIN_ROWS; if it is below the
        # floor, the parallel fan-out (clone + per-worker hash rebuild) is pure overhead —
        # drive the ORIGINAL wired inner probe chain serially instead. The original chain
        # emits the inner join's output straight into the TOP join's left adapter and EOS,
        # building the TOP join byte-identically to the fully serial prelude.
        inner_probe_head = inner_probe_chain[1]
        buffer = []
        buffered_rows = 0
        exhausted = False
        while buffered_rows < config.PARALLEL_MIN_ROWS:
            morsel = pull_one(inner_probe_scan)
            if morsel is None:
                exhausted = True
                break
            buffer.append(morsel)
            buffered_rows += morsel.num_rows

        if exhausted and buffered_rows < config.PARALLEL_MIN_ROWS:
            for morsel in buffer:
                if ctx.is_terminated():
                    return
                push_one(inner_probe_head, morsel)
            if not ctx.is_terminated():
                push_one(inner_probe_head, _EOS_SENTINEL)
            self._capture(top_join)
            return

        workers = self._workers
        errors = [None] * workers
        worker_tops = [None] * workers
        buf_iter = iter(buffer)
        pull_lock = threading.Lock()
        concurrent_safe = inner_probe_scan.is_concurrent_pull_safe()

        def _next_input():
            # Buffered floor sample first, then self-pull the inner probe scan. Lockless
            # only when the source is reentrant; otherwise serialise the whole pull.
            if concurrent_safe:
                with pull_lock:
                    m = next(buf_iter, None)
                if m is not None:
                    return m
                if ctx.is_terminated():
                    return None
                return pull_one(inner_probe_scan)
            with pull_lock:
                if ctx.is_terminated():
                    return None
                m = next(buf_iter, None)
                if m is not None:
                    return m
                return pull_one(inner_probe_scan)

        def worker(index):
            try:
                # Private clone of the inner join over the shared READ-ONLY inner
                # left_morsel (thread-local-full — no shared mutable probe state).
                clone_inner = _clone_op(inner)
                clone_inner.left_morsel = inner_left_morsel
                clone_inner.left_columns = inner_left_columns
                clone_inner.columns = inner_columns
                clone_inner.left_is_empty = inner_left_is_empty
                clone_inner.carchar_probe_load_factor = inner_load_factor
                clone_inner.left_hash = build_side_carchar_morsel_map(
                    inner_left_morsel,
                    inner_left_columns,
                    inner_load_factor,
                    clone_inner.kernel_metrics,
                )
                clone_inner._build_complete = True
                clone_inner.set_context(ctx)

                # The clone's output ACCUMULATES into a per-worker TOP-join clone (data
                # morsels only, no EOS → no build). The per-worker top clone's
                # `left_morsels` is the worker's materialised slice.
                worker_top = _clone_op(top_join)
                worker_top.set_context(ctx)
                top_adapter = JoinLeftAdapter(worker_top)
                top_adapter.set_context(ctx)
                clone_inner.set_downstream(top_adapter)

                # Probe-side chain: [inner-probe-middle* → JoinRightAdapter(clone_inner)].
                probe_ops = [
                    _clone_op(self._plan[nid])
                    for nid in self._prelude_join.inner_probe_middle_ids
                ]
                adapter = JoinRightAdapter(clone_inner)
                adapter.set_context(ctx)
                probe_chain = probe_ops + [adapter]
                for i, op in enumerate(probe_chain):
                    op.set_context(ctx)
                    if i + 1 < len(probe_chain):
                        op.set_downstream(probe_chain[i + 1])
                probe_head = probe_chain[0]

                while True:
                    morsel = _next_input()
                    if morsel is None:
                        break
                    push_one(probe_head, morsel)
                # Probe-side EOS flushes the inner join's downstream emit into the worker
                # TOP clone's left adapter as a data path (the inner join's right-EOS emits
                # EOS downstream → the TOP clone's push_left(EOS) would BUILD it). We must
                # NOT let the worker TOP clone build — we only want its accumulated
                # `left_morsels`. So we do NOT push EOS into the inner clone here; instead
                # the inner clone has buffered nothing post-probe (every probe morsel emits
                # immediately), and the accumulated `left_morsels` is complete.
                worker_tops[index] = worker_top
            except BaseException as exc:  # noqa: BLE001 — surface on the main thread
                errors[index] = exc

        pool = CppThreadPool(workers, "m4-build-prelude")
        try:
            futures = [pool.submit(worker, k) for k in range(workers)]
            for future in futures:
                future.result()
        finally:
            pool.shutdown(wait=True)
            inner_probe_scan.close_source()
        for exc in errors:
            if exc is not None:
                raise exc
        if ctx.is_terminated():
            return

        # ---- Replay the materialised intermediate serially into the REAL TOP join, then
        # EOS → the TOP join builds. Build-before-probe is preserved: the replay completes
        # before the TOP join's probe (the skeleton's worker fan-out) reads its build state.
        for worker_top in worker_tops:
            if worker_top is None:
                continue
            for materialised in worker_top.left_morsels:
                if ctx.is_terminated():
                    return
                push_one(top_left_adapter, materialised)
        push_one(top_left_adapter, _EOS_SENTINEL)
        self._capture(top_join)

    def prepare_worker_clone(self, cloned_join):
        """Set up THIS worker's join clone to SHARE the ONE built probe table read-only
        (Phase D). The Carchar probe is now reentrant (each kernel call owns a
        thread-local ProbeScratch, the table data is const), so workers no longer each
        rebuild a private hash — the build runs once and every worker probes the same
        sealed engine concurrently."""
        cloned_join.left_morsel = self.left_morsel
        cloned_join.left_columns = self.left_columns
        cloned_join.columns = self.columns
        cloned_join.left_is_empty = self.left_is_empty
        cloned_join.carchar_probe_load_factor = self.load_factor
        cloned_join.left_hash = self.built_hash
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
    """A fresh-STATE worker that borrows the operator's SPEC (native scheduler
    rewrite, slice 2a). Routes through the `make_worker` contract on BasePlanNode:
    migrated operators (projection, sort) share their compiled SPEC by reference
    with no recompile; un-migrated operators fall back to the default reflection
    clone (re-running __init__). See docs/NATIVE_SCHEDULER_REWRITE_DESIGN.md §9.3."""
    from opteryx.operators._operators import spawn_worker

    return spawn_worker(op)


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


def _pipeline_is_serial(plan, workers):
    """True iff no parallel strategy covers this pipeline — i.e. the point at which
    ``dispatch_data_pipeline`` falls through to the serial drive (multi-join, sort,
    window, set-ops, limit-only, subqueries…).

    SINGLE source of the serial/breaker split, shared by the scheduler gate (which
    routes serial plans to the native ``_native_serial_execute`` drive instead of
    pumping ``_serial_stream``) and by ``dispatch_data_pipeline`` below — whose
    ``_run_breaker_segment`` returns are *exactly* the not-serial cases. The two MUST
    stay in lockstep: a breaker mis-classified as serial loses its concurrent
    fan-out (the concurrency guard fails loud on that)."""
    if _find_parallel_breaker_segment(plan, workers) is not None:
        return False
    join_agg = _find_parallel_join_agg(plan) or _find_parallel_multi_join_agg(plan)
    if join_agg is not None:
        recomb = recombination_class_for(plan[join_agg.breaker_id])
        if recomb in (RecombClass.HASH_REPARTITION, RecombClass.SCALAR_MERGE):
            return False
    return True


def dispatch_data_pipeline(plan, workers, telemetry=None):
    """Shape-router for a data pipeline — returns the streaming drive generator.

    The shared substrate the M4 scheduler (``scheduler_engine.py``) drives. It detects
    the plan's shape and returns the generator for the handler matching it — the
    scheduler hosts this under its Event/Executor DAG (one Event per pipeline segment;
    build-before-probe and multi-segment build ordering are ``add_dependency`` edges).
    The handlers themselves (``_run_breaker_segment``, ``_join_probe_stream``,
    ``_stateless_stream``, ``_serial_stream``) are the kept substrate — unchanged.

    It detects the plan's shape and routes to the handler for it:

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
    single-scan ``_find_parallel_breaker_segment`` walk runs FIRST (it rejects the
    two-scan join), then join→agg, then bare join, then stateless. ``_serial_stream``
    is the fall-through —
    not the "old engine", but this engine's serial path, required for correctness on
    un-parallelisable shapes. DOP=1 / below-floor still drives the ORIGINAL un-cloned
    chain inside each handler (the prime constraint).
    """
    # Single-scan scan→stateless*→breaker segment whose breaker DECLARES a parallel
    # sink (grouped agg HASH_REPARTITION, ungrouped agg SCALAR_MERGE, distinct
    # HASH_REPARTITION). ONE segment-driven, capability-resolved walk replaces the
    # three bespoke matchers (design §1.4 Phase B): the structural gates and the
    # per-operator eligibility/DOP-floor are resolved off the catalog spec, not
    # re-derived per shape. The distinct sink threads ``downstream_id`` (the breaker's
    # real downstream node) so it can push its deduped survivors there; the agg sinks
    # ignore it and inject into the breaker itself (relying on the EOS push).
    breaker_seg = _find_parallel_breaker_segment(plan, workers)
    if breaker_seg is not None:
        scan_id, middle_ids, breaker_id, recomb, downstream_id = breaker_seg
        if telemetry is not None:
            telemetry._reading["parallel_engaged"] = 1
            telemetry._reading["generic_pipeline"] = 1
        return _run_breaker_segment(
            plan, scan_id, middle_ids, breaker_id, recomb, workers, telemetry,
            exit_id=downstream_id,
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
            # For the MULTI-JOIN shape, resolve whether the build prelude's OUTERMOST
            # inner join can itself be parallel-probed (the Amdahl-anchor fix). For the
            # single-join shape there is no build subtree, so this is None and the build
            # leg drives serially as before.
            prelude_join = None
            if join_agg.build_scan_ids is not None:
                prelude_join = _resolve_build_prelude_join(
                    plan, join_agg.join_id, join_agg.probe_scan_id, get_registry()
                )
            # The shared-source helper needs a ctx to drive the build leg + set the
            # cloned-join context. `_run_breaker_segment` owns the real ctx (it
            # re-compiles), so we build the `_SharedSourceJoin` against THAT ctx by
            # threading it lazily: the helper captures plan+shape now and is bound to the
            # skeleton's ctx inside `build` / `prepare_worker_clone` (it reads
            # `self._ctx`). We pass a placeholder ctx that the skeleton overwrites.
            shared = _SharedSourceJoin(
                plan, join_agg, ctx=None, workers=workers, prelude_join=prelude_join
            )
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
    # NOTE: the parallel INNER-EQUI-JOIN (``_find_parallel_join``) and the parallel
    # STATELESS (``scan → stateless* → exit``) shapes are handled natively in
    # ``scheduler_engine`` (``_native_join_execute`` / ``_native_stateless_execute``),
    # which intercept them before dispatch — so they never reach here. The old
    # ``_join_probe_stream`` / ``_stateless_stream`` handlers were removed.

    # Unreachable in correct operation: the scheduler gate (`_pipeline_is_serial`)
    # routes every serial plan to the native `_native_serial_execute` drive BEFORE
    # `_native_generic_execute` calls dispatch, so dispatch only ever sees a parallel
    # breaker shape. Reaching here means `_pipeline_is_serial` and this router have
    # drifted out of lockstep — fail loud rather than silently serialize, which would
    # mask the gate bug as "green but fake".
    raise RuntimeError(
        "dispatch_data_pipeline reached its serial fallback: a plan the scheduler "
        "gate classified non-serial matched no breaker shape here — "
        "_pipeline_is_serial and dispatch_data_pipeline have drifted out of lockstep."
    )


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
        worker_heads = [None] * workers

        def _build_worker_chain(index):
            # Coarse per-worker SETUP (PyObject clone + wire) — runs serially below;
            # the per-morsel ACCUMULATE drive is then fanned out NATIVELY (no Python
            # worker closure, no Future) via native_accumulate_fanout.
            clone = sink.make_local_sink_state(index)
            # [middle clones → clone-breaker]; the clone is the local sink (its seam IS
            # the accumulator). The breaker clone has no downstream — the worker only
            # ingests (never EOS), so it never emits.
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
            return clone, chain_head

        # Build the W worker chains serially (clone is coarse PyObject setup), then
        # drive them all natively. A clone failure here is a setup bug — raise loud
        # rather than collect it into errors[] (only per-morsel drive faults go there).
        for index in range(workers):
            local_states[index], worker_heads[index] = _build_worker_chain(index)

        pool = CppThreadPool(workers, "m4-generic-sink")
        # ---- 3b. NATIVE W-way ACCUMULATE fan-out — no Python worker closure, no
        #          Future. Each worker drains the shared self-pull `next_input`
        #          disjointly and accumulates into its local sink; counts land in
        #          local_rows[k], faults in errors[k].
        native_accumulate_fanout(pool, worker_heads, next_input, ctx, local_rows, errors)
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


# =================================================================================
# Phase C — per-segment materialising drive for LINEAR MULTI-BREAKER chains.
#
# A single-scan plan whose breakers form a straight chain (e.g. GROUP BY → DISTINCT,
# GROUP BY → GROUP BY) parallelises ONLY its first breaker today: the rest run serial
# in the first breaker's EOS tail. Phase C drives EACH segment independently and
# MATERIALISES its breaker's recombined output into a buffer; the consuming segment is
# re-sourced from that buffer (the cross-segment dependency the scheduler's Event-DAG
# expresses), so every breaker parallelises on its own DOP.
#
# This is gated to single-scan linear chains where EVERY breaker declares a parallel
# sink (so `GROUP BY … ORDER BY`, whose Sort declares none, stays on today's terminal
# drive — agg parallel + serial sort tail — unchanged). `_run_breaker_segment` (the
# join/agg/distinct skeleton) is LEFT UNTOUCHED; `_drive_segment` is a focused
# per-segment driver (no joins — a linear chain is single-scan by construction).
# =================================================================================


def _make_collect_sink(out, properties, ctx):
    """Build a materialisation sink for a PRODUCER segment (Phase C). Wired as a
    breaker's downstream so the breaker's recombined output is CAPTURED into ``out``
    instead of streamed onward; the consuming segment is re-sourced from ``out``.
    Swallows EOS.

    A pure-Python ``BasePlanNode`` subclass — the breaker's typed ``emit`` pushes the
    C++ carrier into it, the base ``_dispatch_push`` re-acquires the GIL and calls this
    overridden ``_push_impl`` (the sanctioned Python-class-subclass seam). Light init
    mirrors the join adapters (a wiring node, not a catalogued operator). Built via a
    factory so the Cython base is imported lazily, not at module top."""
    from collections import defaultdict

    from opteryx.operators._operators import BasePlanNode
    from opteryx.utils import random_string

    class _CollectSinkImpl(BasePlanNode):
        def __init__(self):
            self.identity = random_string()
            self.parameters = {}
            self.columns = []
            self.readings = defaultdict(int)
            self._time_stat_key = "time_collect_sink"
            self.properties = properties
            self.is_scan = False
            self.is_join = False
            self.is_stateless = True
            self.is_not_explained = True
            self._empty_morsel_cache = None
            self._out = out

        @property
        def name(self):
            return "CollectSink"

        def _push_impl(self, morsel):
            if morsel is _EOS_SENTINEL:
                return
            self._out.append(morsel)

    node = _CollectSinkImpl()
    node.set_context(ctx)
    return node


def _drive_segment(
    plan,
    source,
    middle_ids,
    breaker_id,
    recomb,
    workers,
    telemetry,
    *,
    downstream_id=None,
    collect_into=None,
):
    """Drive ONE pipeline segment ``source → stateless* → breaker`` in parallel via the
    ``PipelineSink`` contract — the focused per-segment driver the Phase C Event-DAG
    calls. ``source`` is ``('scan', scan_id)`` or ``('buffer', morsel_list)``.

    Output mode:
      * ``collect_into`` is a list → PRODUCER segment: the breaker's recombined output
        is materialised into it (the consuming segment is re-sourced from it). The
        generator yields nothing.
      * ``collect_into`` is None → TERMINAL segment: the breaker emits to its real
        downstream and the exit's pending morsels are yielded (streaming drive).

    The W=1 / below-floor path drives the ORIGINAL un-cloned breaker (the prime
    constraint — byte-identical to serial). No joins / ``source_prep``: a linear
    multi-breaker chain is single-scan by construction."""
    import threading

    from opteryx.compiled.thread_pool import CppThreadPool
    from opteryx.managers.execution.pipeline_compiler import compile_pipeline
    from opteryx.operators._operators import pull_one

    chains, exit_node, ctx = compile_pipeline(plan)
    breaker = plan[breaker_id]

    # ---- source abstraction: scan pull, or a thread-safe buffer iterator ----------
    src_kind, src_val = source
    if src_kind == "scan":
        scan = plan[src_val]
        concurrent_safe = scan.is_concurrent_pull_safe()

        def _raw_pull():
            return pull_one(scan)

        def _close():
            scan.close_source()

    else:  # buffer — already materialised in memory; a locked list iterator is reentrant
        scan = None
        _buf_lock = threading.Lock()
        _buf_it = iter(src_val)
        concurrent_safe = True

        def _raw_pull():
            with _buf_lock:
                return next(_buf_it, None)

        def _close():
            return None

    # ---- collect mode: redirect the breaker's output into a materialisation sink ----
    collector = None
    if collect_into is not None:
        collector = _make_collect_sink(collect_into, breaker.properties, ctx)
        # Agg breakers emit via their downstream (floor + sink finalize); the distinct
        # breaker emits via the sink's `downstream` param. Redirecting the breaker's
        # downstream covers both the floor path and the agg-sink emit; the distinct sink
        # gets the collector through `make_sink(downstream=...)` below.
        breaker.set_downstream(collector)

    def _drain():
        # Collect mode yields nothing (output goes to the collector); terminal mode
        # yields the exit's pending morsels.
        if collect_into is not None or exit_node is None:
            return
        while exit_node.has_pending():
            yield exit_node.pop_pending()

    radix = 1
    while radix < workers:
        radix <<= 1

    head = plan[middle_ids[0]] if middle_ids else breaker

    pool = None
    try:
        # ---- row-floor: tiny inputs run serially through the ORIGINAL breaker ------
        buffer = []
        buffered_rows = 0
        exhausted = False
        while buffered_rows < config.PARALLEL_MIN_ROWS:
            morsel = _raw_pull()
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

        def _readout_pool_factory():
            return CppThreadPool(max(1, min(radix, workers)), "phasec-readout")

        # In collect mode the distinct sink pushes survivors to `downstream` = the
        # collector; the agg sinks ignore it (they emit via the breaker, already
        # redirected). In stream mode `downstream_id` is the breaker's real downstream
        # (used by the distinct sink), else None.
        if collect_into is not None:
            downstream = collector
        else:
            downstream = plan[downstream_id] if downstream_id is not None else None

        sink = make_sink(
            breaker,
            recomb,
            radix=radix,
            pool_factory=_readout_pool_factory,
            ctx=ctx,
            downstream=downstream,
            telemetry=telemetry,
        )
        if sink is None:
            raise RuntimeError(f"_drive_segment: no sink adapter for {recomb!r}")

        buf_iter = iter(buffer)
        pull_lock = threading.Lock()

        def next_input():
            if concurrent_safe:
                with pull_lock:
                    m = next(buf_iter, None)
                if m is not None:
                    return m
                if ctx.is_terminated():
                    return None
                return _raw_pull()
            with pull_lock:
                if ctx.is_terminated():
                    return None
                m = next(buf_iter, None)
                if m is not None:
                    return m
                return _raw_pull()

        local_states = [None] * workers
        local_rows = [0] * workers
        errors = [None] * workers
        worker_heads = [None] * workers

        # Serial pre-clone (coarse PyObject setup), then native ACCUMULATE fan-out —
        # no Python worker closure, no Future (a linear chain is single-scan, no join
        # prelude). Same native drive as _run_breaker_segment.
        for index in range(workers):
            clone = sink.make_local_sink_state(index)
            chain = [_clone_op(plan[nid]) for nid in middle_ids]
            chain.append(clone)
            for i, op in enumerate(chain):
                op.set_context(ctx)
                if i + 1 < len(chain):
                    op.set_downstream(chain[i + 1])
            local_states[index] = clone
            worker_heads[index] = chain[0]

        pool = CppThreadPool(workers, "phasec-segment")
        native_accumulate_fanout(pool, worker_heads, next_input, ctx, local_rows, errors)
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

        for m in sink.finalize_source():
            if ctx.is_terminated():
                return
            yield m
        if ctx.is_terminated():
            return
        push_one(sink.eos_target(), _EOS_SENTINEL)
        yield from _drain()
    finally:
        if pool is not None:
            pool.shutdown(wait=True)
        ctx.terminate()
        _close()


def _drive_passthrough_segment(plan, source_buffer, head_id):
    """Drive a no-breaker TERMINAL segment (e.g. a bare ``Exit``, or
    ``stateless* → Exit``) from a materialised buffer, serially. The terminal segment
    of a multi-breaker chain operates on already-recombined (small) data, so a serial
    push through its head to the exit is correct and cheap. Yields the exit's morsels."""
    from opteryx.managers.execution.pipeline_compiler import compile_pipeline

    chains, exit_node, ctx = compile_pipeline(plan)
    head = plan[head_id]

    def _drain():
        if exit_node is None:
            return
        while exit_node.has_pending():
            yield exit_node.pop_pending()

    try:
        for morsel in source_buffer:
            if ctx.is_terminated():
                break
            push_one(head, morsel)
            yield from _drain()
        if not ctx.is_terminated():
            push_one(head, _EOS_SENTINEL)
            yield from _drain()
    finally:
        ctx.terminate()


def _find_linear_multibreaker_chain(plan, workers):
    """Return the segments of a single-scan LINEAR MULTI-BREAKER chain in DATAFLOW
    order (source-first), else ``None`` (design §1.4 Phase C).

    Eligible iff: exactly one scan; the plan's segments form a straight chain (each
    segment's source is fed by the previous segment's breaker); at least TWO breaker
    segments; every middle op STATELESS; and EVERY breaker is PARALLEL-DRIVABLE under
    the SAME per-breaker gate ``_find_parallel_breaker_segment`` applies — a declared
    sink, ``workers >= min_workers``, and ``eligible(breaker)``. If ANY breaker is
    ineligible (a constant-key grouped agg, a non-mergeable ungrouped agg, a Sort with
    no sink, distinct below its DOP floor) the WHOLE chain bails to today's terminal
    drive (which still parallelises the first eligible breaker + serial tail — no loss).
    Returns the ordered ``Segment`` list (producers first, terminal last), or ``None``."""
    registry = get_registry()
    scans = [nid for nid in plan.nodes() if getattr(plan[nid], "is_scan", False)]
    if len(scans) != 1:
        return None
    segments = identify_segments(plan)
    breaker_segs = [s for s in segments if s.tail_is_breaker]
    if len(breaker_segs) < 2:
        return None  # single breaker → today's path already parallelises it

    # Order the segments by dataflow: a segment depends on the segment whose breaker
    # feeds its source head. Build provider→segment links from plan edges.
    tail_to_seg = {s.tail: s for s in segments}
    # For each segment, find its predecessor segment (the breaker feeding its head).
    def _predecessor(seg):
        head = seg.nodes[0]
        for provider, _t, _l in plan.ingoing_edges(head):
            if provider in tail_to_seg and _is_breaker(plan, provider, registry):
                return tail_to_seg[provider]
        return None

    # Every breaker must be PARALLEL-DRIVABLE under the SAME gate the single-breaker
    # dispatcher uses (declared sink + DOP floor + per-instance eligibility), AND every
    # middle op STATELESS — else the per-segment scatter would engage an ineligible
    # breaker (e.g. a constant-key agg → `cxx_scatter: no key columns`). Bail the whole
    # chain if any segment fails; today's terminal drive handles it correctly.
    for s in breaker_segs:
        spec = parallel_sink_spec_for(plan[s.tail])
        if spec is None or workers < spec.min_workers:
            return None
        if spec.eligible is not None and not spec.eligible(plan[s.tail]):
            return None
        for nid in s.nodes[1:-1]:
            meta = registry.get(type(plan[nid]))
            if meta is None or meta.parallelism != OperatorParallelism.STATELESS:
                return None

    # Walk from the source segment (the one whose head is the scan) following
    # successors. The chain is linear iff every breaker segment has exactly one
    # successor and the walk visits every segment once.
    source_seg = next((s for s in segments if s.nodes[0] == scans[0]), None)
    if source_seg is None:
        return None
    ordered = []
    seen = set()
    cur = source_seg
    while cur is not None:
        if id(cur) in seen:
            return None  # cycle / non-linear — bail
        seen.add(id(cur))
        ordered.append(cur)
        # The successor segment is the one whose predecessor is `cur` (cur's breaker
        # feeds its head). A linear chain has at most one such successor.
        succs = [s for s in segments if _predecessor(s) is cur]
        if len(succs) > 1:
            return None  # fan-out — not a linear chain
        cur = succs[0] if succs else None

    if len(ordered) != len(segments):
        return None  # some segment not on the linear spine — bail
    return ordered
