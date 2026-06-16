# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""M4 Stage 0 — central parallel scheduler infrastructure.

Validates the reusable machinery the parallel drive (Stage 1) is built on, and
proves the central CppThreadPool is a safe substrate for the clone-per-worker
model under real concurrency. See docs/M4_CENTRAL_SCHEDULER_DESIGN.md.

Three things are exercised so none of the Stage 0 code is dead:

  1. ``resolve_worker_count`` — query-scoped sizing + the cap-8 boundary.
  2. ``identify_segments`` — the pipeline-segment cut on real planned queries
     (linear-to-sink, breaker+sink, and a join as the shared tail of both legs).
  3. A pipeline-level concurrency stress harness: N tasks submitted to ONE
     CppThreadPool, each ingesting a disjoint partition into its OWN engine
     concurrently, then merged and asserted byte-identical to a single-threaded
     reference. This mirrors tests/unit/operators/test_grouped_engine_concurrency
     but drives the work through the central pool the scheduler will own.
"""

import os
import random
import sys

sys.path.insert(1, os.path.join(os.path.dirname(__file__), "../../.."))

import pytest

import opteryx
from opteryx.models import QueryTelemetry
from opteryx.planner import query_planner

from opteryx.managers.execution.parallel_engine import Segment
from opteryx.managers.execution.parallel_engine import identify_segments
from opteryx.managers.execution.parallel_engine import resolve_worker_count
from opteryx.operators.catalog import OperatorParallelism


# ── helpers ──────────────────────────────────────────────────────────────────


def _plan(sql):
    return query_planner(
        operation=sql,
        parameters=None,
        visibility_filters=None,
        execution_context=opteryx.session().context,
        query_id="t",
        telemetry=QueryTelemetry(),
    )


def _names(plan, segment):
    return [plan[nid].name for nid in segment.nodes]


# ── worker sizing ────────────────────────────────────────────────────────────


def test_resolve_worker_count_serial_default():
    # 1 (or below) always means the serial engine.
    assert resolve_worker_count(1) == 1
    assert resolve_worker_count(0) == 1
    assert resolve_worker_count(-4) == 1


def test_resolve_worker_count_caps_at_8():
    # Never exceeds the measured regression boundary, regardless of request or
    # core count.
    assert resolve_worker_count(1000) <= 8
    cpu = os.cpu_count() or 1
    expected = max(1, min(1000, max(1, cpu - 2), 8))
    assert resolve_worker_count(1000) == expected


def test_resolve_worker_count_respects_request_below_cap():
    # A small request is honoured (subject to available cores).
    cpu = os.cpu_count() or 1
    if cpu - 2 >= 2:
        assert resolve_worker_count(2) == 2


# ── segment identification ───────────────────────────────────────────────────


def test_segment_linear_pipeline_to_sink():
    # No pipeline breaker → one segment ending at the Exit sink.
    plan = _plan("SELECT name FROM $planets WHERE diameter > 100")
    segments = identify_segments(plan)
    assert len(segments) == 1
    seg = segments[0]
    assert seg.tail_is_breaker is False
    assert seg.parallelism is None
    names = _names(plan, seg)
    assert names[0] == "Reader"
    assert names[-1] == "Exit"
    assert "Filter" in names


def test_segment_grouped_aggregate_is_a_breaker_tail():
    # GROUP BY cuts the pipeline: [Reader … Grouped Aggregate] + [Exit].
    plan = _plan("SELECT COUNT(*), gravity FROM $planets GROUP BY gravity")
    segments = identify_segments(plan)
    assert len(segments) == 2

    breaker_segs = [s for s in segments if s.tail_is_breaker]
    sink_segs = [s for s in segments if not s.tail_is_breaker]
    assert len(breaker_segs) == 1
    assert len(sink_segs) == 1

    agg_seg = breaker_segs[0]
    assert plan[agg_seg.tail].name == "Grouped Aggregate (Hashed)"
    assert agg_seg.parallelism == OperatorParallelism.STATEFUL_MERGEABLE
    assert _names(plan, agg_seg)[0] == "Reader"

    assert plan[sink_segs[0].tail].name == "Exit"


def test_segment_join_is_shared_tail_of_both_legs():
    # A join is the tail of BOTH its input segments (build leg + probe leg);
    # its output sources a third segment. (Self-join avoids any environment
    # dataset quirks while still producing a two-input join.)
    plan = _plan("SELECT p.name FROM $planets AS p INNER JOIN $planets AS s ON p.id = s.id")
    segments = identify_segments(plan)

    join_segs = [s for s in segments if plan[s.tail].is_join]
    assert len(join_segs) == 2  # one per input leg
    # Both legs name the SAME join node as their tail (the shared recombination
    # point) — build-once / probe-parallel territory for Stage 2.
    assert join_segs[0].tail == join_segs[1].tail
    assert all(s.tail_is_breaker for s in join_segs)


def test_every_non_breaker_node_in_exactly_one_segment():
    # Coverage invariant: the cut partitions the plan — every operator node
    # appears in some segment, and no non-breaker node appears twice.
    plan = _plan("SELECT COUNT(*), gravity FROM $planets WHERE diameter > 1 GROUP BY gravity")
    segments = identify_segments(plan)
    seen = [nid for s in segments for nid in s.nodes]
    # Breakers may be the tail of >1 segment (joins); non-breakers must be unique.
    from collections import Counter

    counts = Counter(seen)
    for nid, c in counts.items():
        if not plan[nid].is_join:
            assert c == 1, f"{plan[nid].name} appeared in {c} segments"


# ── pipeline-level concurrency stress (the central pool as substrate) ─────────

from draken.morsels.morsel import Morsel
from draken.draken_native import vector_from_sequence

from opteryx.compiled.thread_pool import CppThreadPool
from opteryx.operators._operators import (
    AggregationSpec,
    GroupHashEngine,
    create_collectors,
)

_GROUP = "g"
_VALUE = "v"
_SPECS = [
    AggregationSpec(alias="cstar", function="count", column="*"),
    AggregationSpec(alias="sum", function="sum", column=_VALUE),
    AggregationSpec(alias="min", function="min", column=_VALUE),
    AggregationSpec(alias="max", function="max", column=_VALUE),
]
_ALIASES = ["cstar", "sum", "min", "max"]

_WORKERS = 8
_MORSELS_PER_WORKER = 6
_ROWS_PER_MORSEL = 4000
_N_GROUPS = 256


def _engine():
    collectors, _ = create_collectors(_SPECS, [_GROUP])
    return GroupHashEngine([_GROUP], collectors, False, False)


def _morsel(groups, values):
    return Morsel.from_vectors(
        [_GROUP, _VALUE], [vector_from_sequence(groups), vector_from_sequence(values)]
    )


def _finalize(engine):
    out = {}
    for chunk in engine.finalize_morsels():
        gcol = chunk.column(_GROUP).to_pylist()
        cols = {a: chunk.column(a).to_pylist() for a in _ALIASES}
        for i, gv in enumerate(gcol):
            out[gv] = {a: cols[a][i] for a in _ALIASES}
    return out


def _partition(rng):
    return [
        (
            [rng.randrange(_N_GROUPS) for _ in range(_ROWS_PER_MORSEL)],
            [rng.randint(-(10**9), 10**9) for _ in range(_ROWS_PER_MORSEL)],
        )
        for _ in range(_MORSELS_PER_WORKER)
    ]


@pytest.mark.parametrize("round_seed", range(4))
def test_pool_concurrent_ingest_equals_serial(round_seed):
    partitions = [_partition(random.Random(round_seed * 100 + t)) for t in range(_WORKERS)]
    all_morsels = [m for part in partitions for m in part]

    # Single-threaded ground truth, computed before any pool task runs.
    ref_engine = _engine()
    for groups, values in all_morsels:
        ref_engine.ingest(_morsel(groups, values))
    reference = _finalize(ref_engine)

    # Pre-build morsels so the submitted task is pure concurrent ingestion
    # (exclusive morsel ownership: each partition's morsels go to exactly one
    # worker engine — contract rule 1).
    built = [[_morsel(g, v) for g, v in partitions[t]] for t in range(_WORKERS)]

    def ingest_partition(idx):
        eng = _engine()
        for m in built[idx]:
            eng.ingest(m)
        return eng

    pool = CppThreadPool(_WORKERS, "m4-stage0-stress")
    try:
        futures = [pool.submit(ingest_partition, t) for t in range(_WORKERS)]
        engines = [f.result() for f in futures]
    finally:
        pool.shutdown(wait=True)

    # Merge partials + finalize on the calling thread (exactly the recombination
    # the scheduler does at a mergeable breaker).
    base = engines[0]
    for other in engines[1:]:
        base.merge(other)
    merged = _finalize(base)

    assert merged.keys() == reference.keys()
    for g in reference:
        m, r = merged[g], reference[g]
        assert m["cstar"] == r["cstar"]
        assert m["sum"] == r["sum"]
        assert m["min"] == r["min"]
        assert m["max"] == r["max"]


# ── dispatch seam ────────────────────────────────────────────────────────────


def test_find_parallel_grouped_agg_matches_supported_shape():
    # Single-scan grouped aggregate → recognised as parallelisable.
    from opteryx.managers.execution.parallel_engine import _find_parallel_grouped_agg

    plan = _plan("SELECT COUNT(*), gravity FROM $planets WHERE diameter > 1 GROUP BY gravity")
    target = _find_parallel_grouped_agg(plan)
    assert target is not None
    scan_id, middle_ids, breaker_id = target
    assert plan[scan_id].is_scan
    assert plan[breaker_id].__class__.__name__ == "GroupedAggregateHashedNode"
    # Filter is a stateless middle op of the segment.
    assert any(plan[nid].name == "Filter" for nid in middle_ids)


def test_find_parallel_grouped_agg_rejects_unsupported_shapes():
    from opteryx.managers.execution.parallel_engine import _find_parallel_grouped_agg

    # No aggregate breaker → not this stage's target.
    assert _find_parallel_grouped_agg(_plan("SELECT name FROM $planets WHERE diameter > 1")) is None
    # A join → more than one scan → deferred to Stage 2.
    join_sql = "SELECT p.name FROM $planets AS p INNER JOIN $planets AS s ON p.id = s.id GROUP BY p.name"
    assert _find_parallel_grouped_agg(_plan(join_sql)) is None


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT gravity, COUNT(*) c, SUM(diameter) s, AVG(diameter) a, MIN(diameter) mn, MAX(diameter) mx FROM $planets GROUP BY gravity ORDER BY gravity",
        "SELECT status, COUNT(*) c, AVG(year) a FROM testdata.astronauts WHERE year > 1950 GROUP BY status ORDER BY status",
        'SELECT "group" g, COUNT(*) c FROM testdata.astronauts GROUP BY "group" ORDER BY c DESC LIMIT 3',
    ],
)
def test_parallel_grouped_agg_equals_serial(monkeypatch, sql):
    # The real gate: identical answers serial vs parallel, with the floor forced
    # to 0 so the parallel drive engages even on small fixtures.
    import opteryx

    monkeypatch.setattr(opteryx.config, "PARALLEL_MIN_ROWS", 0)

    def _run(workers):
        monkeypatch.setattr(opteryx.config, "MAX_EXECUTION_WORKERS", workers)
        rows = []
        for morsel in opteryx.session().execute_to_morsels(sql):
            if morsel is None:
                continue
            names = list(morsel.column_names)
            cols = [morsel.column(n).to_pylist() for n in names]
            rows.extend(tuple(c[i] for c in cols) for i in range(morsel.num_rows))
        return rows

    assert _run(1) == _run(4)


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-v"]))
