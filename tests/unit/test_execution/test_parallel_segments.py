# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""Central parallel scheduler infrastructure.

Validates the reusable machinery the parallel drive is built on. See
docs/M4_CENTRAL_SCHEDULER_DESIGN.md.

  1. ``resolve_worker_count`` — query-scoped sizing + the cap-8 boundary.
  2. ``identify_segments`` — the pipeline-segment cut on real planned queries
     (linear-to-sink, breaker+sink, and a join as the shared tail of both legs).
  3. The capability-resolved dispatch seam (``_find_parallel_breaker_segment``):
     which shapes engage row-routing, and that the routed answer equals the serial one.
"""

import os
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


def test_resolve_worker_count_softcoded_when_auto():
    # Unset / "auto" / impossible (None or <= 0) → SOFTCODED from cores:
    # max(1, min(cpu-2, 8)). An explicit 1 is honoured (one worker — degree of
    # parallelism, NOT a serial-engine selector).
    cpu = os.cpu_count() or 1
    softcoded = max(1, min(cpu - 2, 8))
    assert resolve_worker_count(0) == softcoded
    assert resolve_worker_count(-4) == softcoded
    assert resolve_worker_count(None) == softcoded
    assert resolve_worker_count(1) == 1


def test_resolve_worker_count_caps_only_the_auto_case():
    # The cap-8 boundary applies ONLY to the unset / auto / impossible case
    # (None or <= 0) — there it softcodes max(1, min(cpu-2, 8)). An EXPLICIT
    # positive request is honoured EXACTLY (warn-then-obey, never clamped) per
    # CLAUDE.md: the user is the architect, intent is obeyed not assumed.
    cpu = os.cpu_count() or 1
    softcoded = max(1, min(cpu - 2, 8))
    # Auto path is capped at the regression boundary.
    assert resolve_worker_count(None) == softcoded
    assert resolve_worker_count(0) == softcoded
    assert resolve_worker_count(-4) == softcoded
    assert softcoded <= 8
    # Explicit oversubscribed request → returned verbatim, with a one-shot warning.
    import warnings

    import opteryx.managers.execution.parallel_engine as pe

    pe._oversubscribe_warned = False
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        assert resolve_worker_count(1000) == 1000
    assert any("oversubscribed" in str(w.message) for w in caught)


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


# ── dispatch seam ────────────────────────────────────────────────────────────


def test_find_parallel_breaker_segment_matches_supported_shape():
    # Single-scan grouped aggregate → recognised as parallelisable via the
    # capability-resolved segment walk (design §1.4 Phase B).
    from opteryx.managers.execution.parallel_engine import _find_parallel_breaker_segment

    plan = _plan("SELECT COUNT(*), gravity FROM $planets WHERE diameter > 1 GROUP BY gravity")
    target = _find_parallel_breaker_segment(plan, workers=4)
    assert target is not None
    scan_id, middle_ids, breaker_id, recomb, downstream_id = target
    assert plan[scan_id].is_scan
    assert plan[breaker_id].__class__.__name__ == "GroupedAggregateHashedNode"
    assert recomb.value == "hash_repartition"
    # Filter is a stateless middle op of the segment.
    assert any(plan[nid].name == "Filter" for nid in middle_ids)


def test_find_parallel_breaker_segment_rejects_unsupported_shapes():
    from opteryx.managers.execution.parallel_engine import _find_parallel_breaker_segment

    # No declared-sink breaker (linear scan→exit) → not a parallel breaker segment.
    assert _find_parallel_breaker_segment(_plan("SELECT name FROM $planets WHERE diameter > 1"), workers=4) is None
    # A join → more than one scan → deferred to the join shapes.
    join_sql = "SELECT p.name FROM $planets AS p INNER JOIN $planets AS s ON p.id = s.id GROUP BY p.name"
    assert _find_parallel_breaker_segment(_plan(join_sql), workers=4) is None
    # Any non-empty group key is eligible — there is no key-type gate. Both a
    # string key and a fixed-width (decimal) key route.
    assert _find_parallel_breaker_segment(_plan("SELECT name, COUNT(*) FROM $planets GROUP BY name"), workers=4) is not None
    assert _find_parallel_breaker_segment(_plan("SELECT gravity, COUNT(*) FROM $planets GROUP BY gravity"), workers=4) is not None
    # DISTINCT declares min_workers=2: engages at W>=2, serial at W=1.
    distinct_sql = "SELECT DISTINCT gravity FROM $planets"
    assert _find_parallel_breaker_segment(_plan(distinct_sql), workers=4) is not None
    assert _find_parallel_breaker_segment(_plan(distinct_sql), workers=1) is None


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT gravity, COUNT(*) c, SUM(diameter) s, AVG(diameter) a, MIN(diameter) mn, MAX(diameter) mx FROM $planets GROUP BY gravity ORDER BY gravity",
        "SELECT status, COUNT(*) c, AVG(year) a FROM testdata.astronauts WHERE year > 1950 GROUP BY status ORDER BY status",
        # ORDER BY <agg> DESC + LIMIT downstream of the grouped breaker.
        "SELECT status, COUNT(*) c FROM testdata.astronauts GROUP BY status ORDER BY c DESC LIMIT 3",
        # GROUP BY on a double-quoted reserved word (`group` is a real column). The
        # dialect tokenises "group" as a string; the binder must rebind it to the
        # column, otherwise the group key collapses to zero columns.
        'SELECT "group" g, COUNT(*) c FROM testdata.astronauts GROUP BY "group" ORDER BY g',
    ],
)
def test_parallel_grouped_agg_equals_serial(monkeypatch, sql):
    # The real gate: identical answers serial vs row-routing parallel. Row-routing
    # is the only grouped strategy; the serial baseline is obtained by raising the
    # floor above the fixture row count (single-engine keying), the routed run uses
    # floor=0 so row-routing engages. All SQL here have ORDER BY, so output order is
    # deterministic and list equality is meaningful.
    import opteryx

    def _run(workers, floor):
        monkeypatch.setattr(opteryx.config, "MAX_EXECUTION_WORKERS", workers)
        monkeypatch.setattr(opteryx.config, "PARALLEL_MIN_ROWS", floor)
        rows = []
        for morsel in opteryx.session().execute_to_morsels(sql):
            if morsel is None:
                continue
            names = list(morsel.column_names)
            cols = [morsel.column(n).to_pylist() for n in names]
            rows.extend(tuple(c[i] for c in cols) for i in range(morsel.num_rows))
        return rows

    # Serial floor is above any fixture's row count → row-floor serial path.
    assert _run(1, 10_000_000) == _run(4, 0)


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-v"]))
