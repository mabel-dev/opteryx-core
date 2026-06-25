# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Generic pipeline parallelism — the ``PipelineSink`` contract and its adapters.

The bespoke per-shape strategies in ``parallel_engine.py`` (``_ungrouped_agg_stream``,
``_grouped_agg_route``, …) all copy-paste the SAME 7-part skeleton; the ONLY thing
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

from enum import Enum


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
      * ``ORDER_MERGE``      — sort; combine = register a sorted run; finalize =
                               k-way merge tail. (Step 8, reserved.)
      * ``NONE``             — no generic parallel sink; the segment runs serial
                               (today's ``_finalize`` verbatim). Everything un-migrated.
    """

    SCALAR_MERGE = "scalar_merge"
    HASH_REPARTITION = "hash_repartition"
    SHARED_SOURCE = "shared_source"
    ORDER_MERGE = "order_merge"
    NONE = "none"


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

    Lifts ``_ungrouped_agg_stream``'s recombination verbatim onto the contract:
    each worker aggregates its own morsels into a private cloned engine; the W
    engines scalar-merge (sum/min/max the partials) into the first populated one;
    the ORIGINAL breaker's ``_engine`` is set to that merged engine and its EOS
    path emits the single merged row downstream.
    """

    __slots__ = ("_breaker", "_base_engine")

    def __init__(self, breaker):
        self._breaker = breaker
        self._base_engine = None

    def recombination_class(self) -> RecombClass:
        return RecombClass.SCALAR_MERGE

    def make_local_sink_state(self, task: int):
        # A fresh clone with a clean private aggregate engine (the seam = _engine).
        from opteryx.managers.execution.parallel_engine import _clone_op

        return _clone_op(self._breaker)

    def combine(self, locals_):
        # Scalar-merge the W clones' engines into the first populated one. `locals_`
        # is the list of (clone, ingested_rows) pairs the skeleton collected; only
        # clones that ingested rows carry state worth merging (mirrors
        # _ungrouped_agg_stream's `populated` filter at ~674).
        populated = [clone for (clone, rows) in locals_ if rows > 0]
        if not populated:
            # No worker ingested anything; the original breaker's own (empty) engine
            # stays in place and finalize emits the empty-aggregate row.
            self._base_engine = None
            return
        base = populated[0]._engine
        for clone in populated[1:]:
            base.merge(clone._engine)
        self._base_engine = base

    def finalize_source(self):
        # Inject the merged engine into the ORIGINAL breaker, then yield nothing —
        # the read-out is the breaker's own EOS-driven _finalize (the skeleton pushes
        # EOS through the original breaker). When no worker ingested rows we leave the
        # original engine untouched so the empty-result row still emits.
        if self._base_engine is not None:
            self._breaker._engine = self._base_engine
        return iter(())


class _HashRepartitionSink(PipelineSink):
    """HASH_REPARTITION adapter for the GROUPED aggregate breaker.

    Lifts ``_grouped_agg_route``'s scatter / combine / parallel-readout verbatim
    onto the contract:

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
        # O(radix) hand-off of thread-local bins → global per-partition lists. Mirror
        # _grouped_agg_route ~1302-1310. `locals_` is the list of (clone, rows) the
        # skeleton collected; the scatter bins live on the clone's swapped _engine.
        radix = self._radix
        global_bins = [[] for _ in range(radix)]
        for (clone, _rows) in locals_:
            wb = clone._engine.bins  # the _ScatterCollectEngine's bins
            for p in range(radix):
                if wb[p]:
                    global_bins[p].extend(wb[p])
        self._global_bins = global_bins

    def finalize_source(self):
        # PARALLEL per-partition read-out: ≤radix workers each aggregate one global
        # partition into a fresh engine. Mirror _grouped_agg_route ~1312-1351. The
        # populated partition engines are injected into the ORIGINAL breaker's
        # _parallel_engines; the skeleton's EOS push through the original then drives
        # _finalize (concat, no merge) downstream → exit.
        from opteryx.managers.execution.parallel_engine import _clone_op

        radix = self._radix
        global_bins = self._global_bins
        ctx = self._ctx

        part_engines = [None] * radix
        part_rows = [0] * radix
        rerr = [None] * radix

        def readout_worker(p):
            try:
                engine = _clone_op(self._breaker)._engine
                count = 0
                for chunk in global_bins[p]:
                    if ctx.is_terminated():
                        break
                    engine.ingest(chunk)
                    count += chunk.num_rows
                part_engines[p] = engine
                part_rows[p] = count
            except BaseException as exc:  # noqa: BLE001 — surface on the main thread
                rerr[p] = exc

        rpool = self._pool_factory()
        try:
            rfutures = [rpool.submit(readout_worker, p) for p in range(radix)]
            for future in rfutures:
                future.result()
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
        # preserving DOP=1 byte-identity. This is the bespoke ``_distinct_stream``
        # non-fuse phase-B dedup lifted onto the contract: ``hash(key) % radix``
        # co-locates every copy of a value in ONE partition, so the per-partition
        # carchar sets are disjoint key slices — NO cross-worker merge.
        from opteryx.managers.execution.parallel_engine import push_one
        from opteryx.compiled.morsel_ops.distinct import distinct as _distinct_op
        from opteryx.compiled.structures.carchar_set import CarcharSetWrapper

        radix = self._radix
        global_bins = self._global_bins
        ctx = self._ctx
        distinct_on = self._distinct_on

        part_out = [None] * radix
        part_rows = [0] * radix
        rerr = [None] * radix

        def dedup_worker(p):
            try:
                hash_set = CarcharSetWrapper()
                out = []
                count = 0
                for chunk in global_bins[p]:
                    if ctx.is_terminated():
                        break
                    # In-place dedup against this partition's private set (mutates the
                    # chunk to its NEW-survivor rows). Identical to serial's dedup
                    # kernel — result set is order-independent of phash/parvi, which
                    # the md5 gate sorts away.
                    _distinct_op(chunk, hash_set, columns=distinct_on)
                    if chunk.num_rows > 0:
                        out.append(chunk)
                    count += chunk.num_rows
                part_out[p] = out
                part_rows[p] = count
            except BaseException as exc:  # noqa: BLE001 — surface on the main thread
                rerr[p] = exc

        rpool = self._pool_factory()
        try:
            rfutures = [rpool.submit(dedup_worker, p) for p in range(radix)]
            for future in rfutures:
                future.result()
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
# Dispatch registries (design §1.3). Two maps, both keyed by operator class NAME (not
# the class object, to avoid importing the Cython operators here):
#
#   _RECOMB_BY_CLASS  — breaker class → RecombClass taxonomy label (the dispatch key
#                       the generic executor reads). Distinct and grouped agg are
#                       BOTH HASH_REPARTITION yet recombine via DIFFERENT adapters.
#   _ADAPTER_BY_CLASS — breaker class → the concrete PipelineSink adapter. Resolves
#                       the Distinct-vs-grouped-agg ambiguity the coarse taxonomy
#                       cannot: same RecombClass, different finalize.
#
# An operator absent from _RECOMB_BY_CLASS is NONE → the segment runs serial.
# ---------------------------------------------------------------------------------
_RECOMB_BY_CLASS = {
    "UngroupedAggregateNode": RecombClass.SCALAR_MERGE,
    "GroupedAggregateHashedNode": RecombClass.HASH_REPARTITION,
    "DistinctNode": RecombClass.HASH_REPARTITION,
}

_ADAPTER_BY_CLASS = {
    "UngroupedAggregateNode": _ScalarMergeSink,
    "GroupedAggregateHashedNode": _HashRepartitionSink,
    "DistinctNode": _DistinctSink,
}


def recombination_class_for(node) -> RecombClass:
    """The recombination class for a breaker node, or ``NONE`` if no generic sink
    handles its shape (→ serial)."""
    return _RECOMB_BY_CLASS.get(node.__class__.__name__, RecombClass.NONE)


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
    """Construct the ``PipelineSink`` adapter for ``node``.

    The adapter is chosen by NODE CLASS (``_ADAPTER_BY_CLASS``), not by the coarse
    ``recomb_class`` taxonomy — Distinct and grouped aggregate are BOTH
    HASH_REPARTITION yet need DIFFERENT adapters (``_DistinctSink`` vs
    ``_HashRepartitionSink``). ``recomb_class`` is still validated against the
    per-class taxonomy so a mismatch fails loud rather than silently picking the
    wrong sink.

    ``radix`` / ``pool_factory`` / ``ctx`` are required only for HASH_REPARTITION
    (the parallel read-out); ``downstream`` (the breaker's real downstream node) is
    required only for the distinct sink. Returns ``None`` when no adapter handles the
    class (→ the segment runs serial)."""
    adapter = _ADAPTER_BY_CLASS.get(node.__class__.__name__)
    if adapter is None:
        return None
    expected = _RECOMB_BY_CLASS.get(node.__class__.__name__)
    if expected is not recomb_class:
        raise RuntimeError(
            f"make_sink: recomb_class {recomb_class!r} does not match the "
            f"registered class for {node.__class__.__name__} ({expected!r})"
        )
    if adapter is _ScalarMergeSink:
        return _ScalarMergeSink(node)
    if adapter is _HashRepartitionSink:
        return _HashRepartitionSink(node, radix, pool_factory, ctx, telemetry=telemetry)
    if adapter is _DistinctSink:
        return _DistinctSink(
            node, radix, pool_factory, ctx, downstream, telemetry=telemetry
        )
    return None  # pragma: no cover - exhaustive above
