# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Generic pipeline parallelism — the ``PipelineSink`` contract and its adapters.

Every parallelisable breaker shape shares the SAME 7-part skeleton; the ONLY thing
that differs per shape is *what the sink does*. This module lifts "what the sink
does" into a thin per-breaker contract — pure-Python **adapter** classes that
operate on the breaker operator's EXISTING public seams (``_engine``,
``_parallel_engines``) without touching the Cython operators.

``_run_breaker_segment`` (in ``parallel_engine.py``) drives the shared skeleton and
calls into a ``PipelineSink`` adapter for the three points that vary:

  * ``make_local_sink_state(task)`` → a per-worker CLONE whose existing seam IS the
    local accumulator (its ``_engine``). The worker drives ``[middle → clone]`` into it.
  * ``combine(locals)``            → fold the W per-worker locals into a single
    shared state (the ONE mutating contact).
  * ``finalize_source()``          → set up the ORIGINAL breaker's state and return
    the read-out iterator; the skeleton then pushes EOS through the ORIGINAL breaker
    so its ``_finalize`` emits downstream → exit (preserving DOP=1 byte-identity).

The ORIGINAL breaker drives EOS in every case — never a clone — which is what makes
the DOP=1 / below-floor path byte-identical to serial (the prime constraint, §3 of
``docs/GENERIC_PIPELINE_PARALLELISM_DESIGN.md``).

This module imports NOTHING from ``parallel_engine`` at module load; the cloning
helpers (``_clone_op``, ``_ScatterCollectEngine``) are imported lazily inside the
adapter methods so ``parallel_engine`` can import this module at the top level.
"""

from dataclasses import dataclass
from enum import Enum
from typing import Optional


class RecombClass(Enum):
    """How a breaker's W per-worker partials recombine into one result.

    This is the per-breaker dispatch key the generic executor reads (replacing the
    too-coarse ``OperatorParallelism`` enum, which carries no class for joins and
    collapses Distinct + GroupedAggregate into one value despite their differing
    recombination). See §1.3 of the design.

      * ``SCALAR_MERGE``     — combine = ``engine.merge()``; finalize = the merged
                               engine. Ungrouped aggregate.
      * ``HASH_REPARTITION`` — combine = O(radix) bin hand-off; finalize = parallel
                               per-partition read-out. Grouped aggregate, distinct.
      * ``SHARED_SOURCE``    — inner-equi join; combine = no-op (built once);
                               finalize = streaming probe. (Step 5, reserved.)
      * ``NONE``             — no generic parallel sink; the segment runs serial
                               (today's ``_finalize`` verbatim). Everything un-migrated.
    """

    SCALAR_MERGE = "scalar_merge"
    HASH_REPARTITION = "hash_repartition"
    SHARED_SOURCE = "shared_source"
    NONE = "none"


@dataclass(frozen=True)
class ParallelSinkSpec:
    """A breaker's declared parallel-sink capability — the catalog field that
    retires the class-name dispatch dicts (design §1.4 Phase A).

    The operator gains ONE piece of self-knowledge — "here is my parallel-sink
    factory, or None" — a declared capability, not parallel logic (Option A). The
    recombination contract still lives OUTSIDE the operator (the adapter); the
    catalog merely carries the pointer to it.

      ``recomb_class`` — how W per-worker partials recombine (the dispatch key the
                         generic executor reads).
      ``adapter``      — the concrete ``PipelineSink`` subclass to instantiate.
      ``min_workers``  — the segment parallelises only at ``>= this DOP``. Distinct's
                         serial dedup is fine at ``W=1`` (scatter + thread setup is
                         pure overhead there), so its sink declares ``2``.
      ``eligible``     — per-instance gate, or ``None`` (always eligible). Some
                         breakers parallelise only for certain configurations — a
                         mergeable ungrouped engine, a grouped breaker with real
                         group columns. ``eligible(node) -> False`` runs the segment
                         serial *by declaration*, not by falling off a matcher.
    """

    recomb_class: RecombClass
    adapter: type
    min_workers: int = 1
    eligible: object = None


class PipelineSink:
    """The contract every parallelisable breaker exposes — a thin SEAM, not a
    rewrite. An adapter wraps a breaker NODE and implements the three points where
    the shared breaker-segment skeleton varies per shape.

    The skeleton owns: ``compile_pipeline``, the row-floor serial fallback, the W
    self-pull workers, the errors barrier, the EOS drive through the ORIGINAL
    breaker, and pool/ctx teardown. The adapter owns ONLY:

    ``recombination_class()``        — the dispatch key (a ``RecombClass``).
    ``make_local_sink_state(task)``  — a per-worker clone whose seam is the local
                                       accumulator; returns the clone (its
                                       ``_engine`` / collector is the local state).
    ``combine(locals)``              — fold the W locals into shared state.
    ``finalize_source()``            — set up the ORIGINAL breaker's state + return
                                       the read-out iterator (driven downstream by
                                       the skeleton's EOS push through ``eos_target``).
    ``eos_target()``                 — the node the skeleton pushes the terminal EOS
                                       into. Default = the ORIGINAL breaker (the agg
                                       sinks inject their result into it and its
                                       ``_finalize`` emits on EOS). Distinct overrides
                                       it to the breaker's real downstream, because the
                                       original distinct breaker holds NO state (its
                                       survivors went straight downstream) — pushing
                                       EOS through it would re-enter its dedup-init on
                                       the sentinel.
    """

    def recombination_class(self) -> RecombClass:  # pragma: no cover - overridden
        raise NotImplementedError

    def make_local_sink_state(self, task: int):  # pragma: no cover - overridden
        raise NotImplementedError

    def combine(self, locals_):  # pragma: no cover - overridden
        raise NotImplementedError

    def finalize_source(self):  # pragma: no cover - overridden
        raise NotImplementedError

    def eos_target(self):
        # Default: the breaker drives EOS (the agg sinks). Adapters whose breaker
        # holds no post-finalize state (distinct) override this.
        return self._breaker


class _ScalarMergeSink(PipelineSink):
    """SCALAR_MERGE adapter for the UNGROUPED aggregate breaker.

    The ungrouped recombination: each worker aggregates its own morsels into a
    private cloned engine; the W engines scalar-merge (sum/min/max the partials) into
    the first populated one and the result becomes the ORIGINAL breaker's engine, so
    its EOS path emits the single merged row downstream.

    Recombination LOGIC is OPERATOR-OWNED: the merge+inject lives in the breaker's
    native ``recombine_scalar_merge`` (``cpdef``, engine types local). This adapter is
    the thin orchestrator — clone per worker, delegate combine, no read-out (the EOS
    push through the original breaker emits the merged row).
    """

    __slots__ = ("_breaker",)

    def __init__(self, breaker):
        self._breaker = breaker

    def recombination_class(self) -> RecombClass:
        return RecombClass.SCALAR_MERGE

    def make_local_sink_state(self, task: int):
        # A fresh clone with a clean private aggregate engine (the seam = _engine).
        from opteryx.managers.execution.parallel_engine import _clone_op

        return _clone_op(self._breaker)

    def combine(self, locals_):
        # Operator-owned: fold the W per-worker partials into THIS breaker's engine
        # (merge + inject), so the EOS-driven _finalize emits the merged global row.
        self._breaker.recombine_scalar_merge(locals_)

    def finalize_source(self):
        # Nothing to read out — the merged engine is already injected (combine), and
        # the skeleton's EOS push through the ORIGINAL breaker emits the merged row.
        return iter(())


class _HashRepartitionSink(PipelineSink):
    """HASH_REPARTITION adapter for the GROUPED aggregate breaker.

    The route-raw scatter / combine / parallel-readout, on the contract:

      * ``make_local_sink_state`` clones the breaker and swaps its ``_engine`` for a
        thread-local ``_ScatterCollectEngine(radix, real_engine)`` — the worker
        drives ``[middle → clone]`` and the scatter routes prepared morsels raw into
        ``radix`` thread-local bins by ``hash(key) % radix``.
      * ``combine`` is an O(radix) bin hand-off — concat each worker's bin ``p`` into
        a global per-partition list (``hash(key) % radix`` co-locates a group in ONE
        partition, so partition engines concat with NO merge).
      * ``finalize_source`` runs the PARALLEL per-partition read-out: ≤radix workers
        each aggregate one global partition into a fresh engine, then the populated
        engines are injected into the ORIGINAL breaker's ``_parallel_engines`` so its
        ``_finalize`` concats them downstream.

    The radix is the next power of two ≥ DOP (so the read-out has ≥ DOP parallelism),
    chosen by the skeleton and passed in.
    """

    __slots__ = ("_breaker", "_radix", "_pool_factory", "_ctx", "_global_bins", "_telemetry")

    def __init__(self, breaker, radix, pool_factory, ctx, telemetry=None):
        self._breaker = breaker
        self._radix = radix
        # A zero-arg callable returning a CppThreadPool for the read-out phase; the
        # skeleton owns pool lifecycle, so it injects a factory the sink uses and the
        # skeleton shuts down (kept in the sink so finalize can spawn the read-out).
        self._pool_factory = pool_factory
        self._ctx = ctx
        self._telemetry = telemetry
        self._global_bins = None

    def recombination_class(self) -> RecombClass:
        return RecombClass.HASH_REPARTITION

    def make_local_sink_state(self, task: int):
        from opteryx.managers.execution.parallel_engine import _clone_op
        from opteryx.managers.execution.parallel_engine import _ScatterCollectEngine

        clone = _clone_op(self._breaker)
        # Swap the cloned breaker's GroupHashEngine for a thread-local scatter into
        # `radix` bins (its own real engine is the key-position resolver). The worker
        # drives [middle → clone]; the cloned breaker's _push_impl does
        # prepare → select → engine.ingest, and ingest now routes raw to bins.
        clone._engine = _ScatterCollectEngine(self._radix, clone._engine)
        return clone

    def combine(self, locals_):
        # O(radix) hand-off of thread-local bins → global per-partition lists.
        # `locals_` is the list of (clone, rows) the skeleton collected; the scatter
        # bins live on the clone's swapped _engine.
        radix = self._radix
        global_bins = [[] for _ in range(radix)]
        for (clone, _rows) in locals_:
            wb = clone._engine.bins  # the _ScatterCollectEngine's bins
            for p in range(radix):
                if wb[p]:
                    global_bins[p].extend(wb[p])
        self._global_bins = global_bins

    def finalize_source(self):
        # NATIVE per-partition read-out fan-out: ≤radix native tasks each key one
        # global partition into a fresh engine via the operator-owned
        # ``readout_partition`` (no Python worker closure, no Future). The populated
        # partition engines are injected into the ORIGINAL breaker's _parallel_engines;
        # the skeleton's EOS push through the original then drives _finalize (concat,
        # no merge) downstream → exit.
        from opteryx.managers.execution.parallel_engine import _clone_op
        from opteryx.operators._operators import native_readout_fanout

        radix = self._radix
        global_bins = self._global_bins
        ctx = self._ctx

        part_engines = [None] * radix
        part_rows = [0] * radix
        rerr = [None] * radix

        rpool = self._pool_factory()
        try:
            native_readout_fanout(
                rpool, self._breaker, global_bins, ctx, part_engines, part_rows, rerr
            )
        finally:
            rpool.shutdown(wait=True)
        for exc in rerr:
            if exc is not None:
                raise exc
        if ctx.is_terminated():
            return iter(())

        if self._telemetry is not None:
            self._telemetry._reading["route_agg_total_rows"] = sum(part_rows)
            self._telemetry._reading["route_agg_ndv"] = sum(
                e.num_groups() for e in part_engines if e is not None
            )

        # Concat the populated partition engines (no merge) — inject into the ORIGINAL
        # breaker. If nothing populated, hand a single fresh empty engine so the
        # finalize emits the (empty) grouped result identically to serial.
        populated = [p for p in range(radix) if part_rows[p] > 0]
        self._breaker._parallel_engines = (
            [part_engines[p] for p in populated]
            if populated
            else [_clone_op(self._breaker)._engine]
        )
        return iter(())


class _DistinctScatterCollectEngine:
    """Producer-side seam swapped into ``DistinctNode._scatter_engine`` for the
    generic HASH_REPARTITION distinct sink — the distinct mirror of
    ``parallel_engine._ScatterCollectEngine``.

    With this object in the ``_scatter_engine`` seam, the cloned ``DistinctNode``'s
    ``_push_impl`` does NOT dedup: it row-routes each (already projected-to-key)
    input morsel into ``radix`` thread-local bins by ``hash(dedup-key) % radix`` —
    every copy of a value lands in the same bin across all workers, so the bins are
    disjoint key slices. Keying / dedup happens later, in parallel, in
    ``_DistinctSink.finalize_source``. ``distinct_on`` is ``None`` for plain
    ``DISTINCT`` (route on ALL columns — upstream projection already narrowed the
    morsel to the selected columns), else the explicit ``DISTINCT ON`` identities.
    """

    __slots__ = ("_radix", "_distinct_on", "bins", "_positions")

    def __init__(self, radix, distinct_on):
        self._radix = radix
        self._distinct_on = distinct_on
        self.bins = [[] for _ in range(radix)]  # bins[p] = list[Morsel]
        self._positions = None

    def ingest(self, morsel):
        if morsel.num_rows == 0:
            return
        from draken.morsels.morsel import Morsel

        if self._positions is None:
            self._positions = _resolve_distinct_positions(morsel, self._distinct_on)
            if self._positions is None:
                # Fail loud, not silently serial: the finder only engages a shape
                # whose dedup-key columns are present on the morsel. None here means
                # the key columns are absent — an internal invariant break.
                raise RuntimeError(
                    "distinct scatter: dedup-key columns not resolvable on a "
                    "Cxx-backed morsel"
                )
        sub = morsel._get_cxx().scatter(self._positions, self._radix)
        bins = self.bins
        for p in range(self._radix):
            bins[p].append(Morsel.from_cxx(sub[p]))


def _resolve_distinct_positions(morsel, distinct_on):
    """Resolve the DISTINCT dedup-key columns to scatter positions on ``morsel``.

    ``distinct_on`` is ``None`` for plain ``DISTINCT`` (route on ALL columns —
    upstream projection already narrowed the morsel to the selected columns), else
    the explicit ``DISTINCT ON`` column identities. Returns a ``list[int]`` of
    column positions, or ``None`` if a key column is absent. Pure Python via the
    ``CxxMorsel`` ``names()`` / ``num_columns`` accessors (matches the bespoke
    ``parallel_engine._resolve_distinct_positions`` exactly — the dedup key is the
    SAME column set the workers later dedup on)."""
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


class _DistinctSink(PipelineSink):
    """HASH_REPARTITION adapter for the DISTINCT breaker.

    Mirrors ``_HashRepartitionSink`` (the grouped-agg sink) exactly in SHAPE — same
    scatter-into-``radix``-bins ``make_local_sink_state``, same O(radix) bin
    hand-off ``combine`` — and differs ONLY in ``finalize_source``: the parallel
    per-partition read-out DEDUPES each partition (instead of AGGREGATES). This is
    the distinct half of the design's "grouped agg + distinct are both
    HASH_REPARTITION yet recombine differently" (§1.3).

      * ``make_local_sink_state`` clones the breaker and swaps its ``_scatter_engine``
        (the distinct producer seam — ``distinct.pyx:48``) for a thread-local
        ``_DistinctScatterCollectEngine(radix, distinct_on)``. The worker drives
        ``[middle → clone]``; the cloned ``DistinctNode``'s ``_push_impl`` routes raw
        into ``radix`` bins instead of deduping.
      * ``combine`` is the SAME O(radix) hand-off as the agg sink — concat each
        worker's bin ``p`` into a global per-partition list (``hash(key) % radix``
        co-locates a value in ONE partition, so partitions dedup with NO cross-worker
        merge).
      * ``finalize_source`` spawns ≤radix read-out workers, each deduping one global
        partition into a private carchar set via the SAME ``distinct`` dedup kernel
        the serial operator uses (NULL handling identical; result set independent of
        phash/parvi ordering, which the consumer sorts away), then pushes the deduped
        survivors into the ORIGINAL breaker's real downstream node (``eos_target``).
        The skeleton's subsequent EOS push through that downstream drives the Exit's
        EOS handling (the empty-result schema morsel when nothing survived). The
        original distinct breaker holds NO post-finalize state, so — unlike the agg
        sinks — its EOS target is the downstream, never the breaker itself (pushing
        EOS through the empty breaker re-enters its dedup-init on the sentinel).
    """

    __slots__ = (
        "_breaker",
        "_radix",
        "_pool_factory",
        "_ctx",
        "_downstream",
        "_distinct_on",
        "_global_bins",
        "_telemetry",
    )

    def __init__(self, breaker, radix, pool_factory, ctx, downstream, telemetry=None):
        self._breaker = breaker
        self._radix = radix
        self._pool_factory = pool_factory
        self._ctx = ctx
        # The breaker's REAL downstream node (the operator after DISTINCT — Exit, or
        # a sort/limit/projection on top). `_downstream` on the operator is a private
        # `cdef` pointer not readable from Python, so the skeleton passes the resolved
        # node in. Deduped survivors are pushed here; EOS comes from the skeleton's
        # push through the original breaker.
        self._downstream = downstream
        self._distinct_on = breaker._distinct_on
        self._telemetry = telemetry
        self._global_bins = None

    def recombination_class(self) -> RecombClass:
        return RecombClass.HASH_REPARTITION

    def eos_target(self):
        # The original distinct breaker holds NO state after finalize (its survivors
        # were pushed straight into the downstream), so the terminal EOS must go to the
        # real downstream node — pushing it through the empty breaker would re-enter the
        # dedup-init on the EOS sentinel and crash.
        return self._downstream

    def make_local_sink_state(self, task: int):
        from opteryx.managers.execution.parallel_engine import _clone_op

        clone = _clone_op(self._breaker)
        # Swap the cloned DistinctNode's producer seam for a thread-local scatter into
        # `radix` bins. The worker drives [middle → clone]; the cloned breaker's
        # _push_impl routes raw to bins instead of deduping (distinct.pyx:152).
        clone._scatter_engine = _DistinctScatterCollectEngine(
            self._radix, self._distinct_on
        )
        return clone

    def combine(self, locals_):
        # O(radix) hand-off of thread-local bins → global per-partition lists —
        # IDENTICAL to _HashRepartitionSink.combine; the scatter bins live on the
        # clone's swapped _scatter_engine.
        radix = self._radix
        global_bins = [[] for _ in range(radix)]
        for (clone, _rows) in locals_:
            wb = clone._scatter_engine.bins
            for p in range(radix):
                if wb[p]:
                    global_bins[p].extend(wb[p])
        self._global_bins = global_bins

    def finalize_source(self):
        # PARALLEL per-partition DEDUP read-out: ≤radix workers each dedup one global
        # partition into a private carchar set, then the deduped survivors are pushed
        # into the ORIGINAL breaker's real downstream. Returns an empty iterator — the
        # skeleton's EOS push through the ORIGINAL breaker drives the downstream/Exit
        # EOS handling (the empty-result schema morsel when nothing survived),
        # preserving DOP=1 byte-identity. The per-partition dedup on the contract:
        # ``hash(key) % radix``
        # co-locates every copy of a value in ONE partition, so the per-partition
        # carchar sets are disjoint key slices — NO cross-worker merge.
        from opteryx.managers.execution.parallel_engine import push_one
        from opteryx.operators._operators import native_readout_fanout

        radix = self._radix
        global_bins = self._global_bins
        ctx = self._ctx

        part_out = [None] * radix
        part_rows = [0] * radix
        rerr = [None] * radix

        # NATIVE per-partition DEDUP read-out fan-out — no Python worker closure, no
        # Future. Each partition dedups into a private carchar set via the operator-owned
        # readout_partition; survivor-chunk lists land in part_out[p], counts in
        # part_rows[p]. native_readout_fanout is generic on result[0]/result[1] — here
        # result[0] is the survivor list (an engine for the agg sink).
        rpool = self._pool_factory()
        try:
            native_readout_fanout(
                rpool, self._breaker, global_bins, ctx, part_out, part_rows, rerr
            )
        finally:
            rpool.shutdown(wait=True)
        for exc in rerr:
            if exc is not None:
                raise exc
        if ctx.is_terminated():
            return iter(())

        if self._telemetry is not None:
            self._telemetry._reading["distinct_total_rows"] = sum(part_rows)

        # Push the deduped survivors into the ORIGINAL breaker's real downstream node
        # (NOT through the breaker — that would re-dedup). They flow downstream → Exit,
        # setting Exit.at_least_one; the skeleton's EOS push through the original
        # breaker then drives the (empty-or-not) Exit EOS handling.
        for p in range(radix):
            for chunk in part_out[p] or ():
                if ctx.is_terminated():
                    return iter(())
                push_one(self._downstream, chunk)
        return iter(())


# ---------------------------------------------------------------------------------
# Declared sink capabilities (design §1.4 Phase A). Each parallelisable breaker
# declares a ``ParallelSinkSpec`` on its catalog metadata (``catalog.py``); the
# specs below are the concrete capabilities the three migrated breakers expose. The
# previous class-name dispatch dicts (``_RECOMB_BY_CLASS`` / ``_ADAPTER_BY_CLASS``)
# are retired — resolution is now off the catalog meta, not ``node.__class__``.
#
# A breaker whose catalog meta carries no spec resolves to ``RecombClass.NONE`` →
# the segment runs serial *by declaration*, not by falling off a matcher.
# ---------------------------------------------------------------------------------


def _ungrouped_eligible(node) -> bool:
    """An ungrouped aggregate parallelises only with a MERGEABLE engine and NO
    literal-only aggregates (literal state lives outside ``_engine``, so a scalar
    merge of only the engine would drop it). Matches the gate the bespoke
    ``_find_parallel_ungrouped_agg`` matcher enforced."""
    engine = getattr(node, "_engine", None)
    return engine is not None and engine.is_mergeable() and not node._has_literals


def _grouped_eligible(node) -> bool:
    """A grouped aggregate with NO group columns is degenerate — there is no key to
    route on. Matches the gate the bespoke ``_find_parallel_grouped_agg`` enforced."""
    return bool(node.group_by_columns)


# Module-level spec instances, referenced from the catalog's ``parallel_sink`` field.
SCALAR_MERGE_SINK_SPEC = ParallelSinkSpec(
    RecombClass.SCALAR_MERGE, _ScalarMergeSink, min_workers=1, eligible=_ungrouped_eligible
)
HASH_REPARTITION_AGG_SINK_SPEC = ParallelSinkSpec(
    RecombClass.HASH_REPARTITION, _HashRepartitionSink, min_workers=1, eligible=_grouped_eligible
)
HASH_REPARTITION_DISTINCT_SINK_SPEC = ParallelSinkSpec(
    RecombClass.HASH_REPARTITION, _DistinctSink, min_workers=2, eligible=None
)


def parallel_sink_spec_for(node) -> Optional[ParallelSinkSpec]:
    """The declared parallel-sink capability for a breaker node (its catalog
    ``parallel_sink``), or ``None`` if it declares no parallel sink."""
    from opteryx.operators.catalog import get_registry

    meta = get_registry().get(type(node))
    return meta.parallel_sink if meta is not None else None


def recombination_class_for(node) -> RecombClass:
    """The recombination class for a breaker node, resolved off its catalog
    ``parallel_sink`` capability, or ``NONE`` if it declares no generic sink
    (→ serial)."""
    spec = parallel_sink_spec_for(node)
    return spec.recomb_class if spec is not None else RecombClass.NONE


def make_sink(
    node,
    recomb_class,
    *,
    radix=None,
    pool_factory=None,
    ctx=None,
    downstream=None,
    telemetry=None,
):
    """Construct the ``PipelineSink`` adapter for ``node`` from its declared catalog
    capability (``parallel_sink``).

    The adapter class is the one the spec names — Distinct and grouped aggregate are
    BOTH HASH_REPARTITION yet name DIFFERENT adapters (``_DistinctSink`` vs
    ``_HashRepartitionSink``). ``recomb_class`` is still validated against the
    declared spec so a mismatch fails loud rather than silently picking the wrong
    sink.

    ``radix`` / ``pool_factory`` / ``ctx`` are required only for HASH_REPARTITION
    (the parallel read-out); ``downstream`` (the breaker's real downstream node) is
    required only for the distinct sink. Returns ``None`` when the node declares no
    sink (→ the segment runs serial)."""
    spec = parallel_sink_spec_for(node)
    if spec is None:
        return None
    if spec.recomb_class is not recomb_class:
        raise RuntimeError(
            f"make_sink: recomb_class {recomb_class!r} does not match the declared "
            f"spec for {node.__class__.__name__} ({spec.recomb_class!r})"
        )
    adapter = spec.adapter
    if adapter is _ScalarMergeSink:
        return _ScalarMergeSink(node)
    if adapter is _HashRepartitionSink:
        return _HashRepartitionSink(node, radix, pool_factory, ctx, telemetry=telemetry)
    if adapter is _DistinctSink:
        return _DistinctSink(
            node, radix, pool_factory, ctx, downstream, telemetry=telemetry
        )
    return None  # pragma: no cover - exhaustive above
