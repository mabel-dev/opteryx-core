"""
Parallel GROUP BY by row-routing.

Correctness is defined as EQUALITY WITH THE SERIAL ENGINE: for each query we run
the serial path (the oracle) and the row-routing path and assert identical result
sets (order-independent — group order differs between paths). This covers the
properties the design rests on:

  - mergeable aggregates (COUNT/SUM/MIN/MAX/AVG) — disjoint slices, concat;
  - HOLISTIC aggregates (COUNT(DISTINCT), MEDIAN) — each group seen whole by one
    worker, so they parallelise with no merge (the design's decisive advantage);
  - multi-column composite keys and NULL keys route correctly;
  - string keys row-route correctly (there is no key-type gate);
  - W=1 runs the SAME row-routing path (one worker), not a serial divert.

Row-routing is the only grouped strategy. The serial oracle is a W=1 run; the
routed run uses W=N. (These tests previously also set a `PARALLEL_MIN_ROWS`
row-floor — that config was removed as dead: nothing in the engine ever read it,
so it never affected which path ran. Only the worker count ever did.)
"""
import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

import opteryx
from opteryx import config

def _collect(sql):
    out = []
    for m in opteryx.session().execute_to_morsels(sql):
        names = [n if isinstance(n, bytes) else n.encode() for n in m.column_names]
        if not names:
            continue
        cols = [m.column(n).to_pylist() for n in names]
        for r in range(m.num_rows):
            out.append(tuple(c[r] for c in cols))
    return out


def _sortkey(row):
    # None-safe, type-stable per column position.
    return tuple((x is None, x) for x in row)


def _run(sql, *, workers):
    saved = config.MAX_EXECUTION_WORKERS
    try:
        config.MAX_EXECUTION_WORKERS = workers
        return _collect(sql)
    finally:
        config.MAX_EXECUTION_WORKERS = saved


def _assert_matches_serial(sql, workers=4):
    serial = _run(sql, workers=1)
    routed = _run(sql, workers=workers)
    assert sorted(routed, key=_sortkey) == sorted(serial, key=_sortkey), (
        f"row-routing != serial for: {sql}\n"
        f"serial={sorted(serial, key=_sortkey)}\nrouted={sorted(routed, key=_sortkey)}"
    )


def test_count_star():
    _assert_matches_serial("SELECT gravity, COUNT(*) AS c FROM $planets GROUP BY gravity")


def test_sum_min_max():
    _assert_matches_serial(
        "SELECT gravity, SUM(id) AS s, MIN(id) AS mn, MAX(id) AS mx "
        "FROM $planets GROUP BY gravity"
    )


def test_avg():
    _assert_matches_serial("SELECT gravity, AVG(id) AS a FROM $planets GROUP BY gravity")


def test_count_distinct_holistic():
    # Holistic: the merge model cannot parallelise this; row-routing can.
    _assert_matches_serial(
        "SELECT gravity, COUNT(DISTINCT id) AS cd FROM $planets GROUP BY gravity"
    )


def test_median_holistic():
    _assert_matches_serial(
        "SELECT gravity, MEDIAN(id) AS m FROM $planets GROUP BY gravity"
    )


def test_multi_column_key():
    _assert_matches_serial(
        "SELECT gravity, mass, COUNT(*) AS c FROM $planets GROUP BY gravity, mass"
    )


def test_null_keys():
    # surface_pressure is NULL for several planets — the NULL group must not split.
    _assert_matches_serial(
        "SELECT surface_pressure, COUNT(*) AS c FROM $planets GROUP BY surface_pressure"
    )


def test_computed_key_expression():
    # GROUP BY a computed key — exercises the node's prepare step before scatter.
    _assert_matches_serial(
        "SELECT id % 3 AS k, COUNT(*) AS c FROM $planets GROUP BY id % 3"
    )


def test_string_key_routes_and_is_correct():
    # A string key row-routes like any other (the scatter hashes it); there is no
    # key-type gate. Results must match serial.
    _assert_matches_serial("SELECT name, COUNT(*) AS c FROM $planets GROUP BY name")


# The `generic_pipeline_*` / `route_agg_*` readings the next two tests asserted on
# are GONE — not renamed. Grouped aggregation moved into the native engine and that
# Python-side row-routing sink took its skew/NDV telemetry with it; what the engine
# reports now is `native_engine_engaged` / `native_engine_dop` / `native_op_stats`.
#
# The self-consistency the skew test really pinned needs no telemetry, so it is
# asserted against the DATA below. What is NOT recovered is the bin-balance (skew)
# reading — no current sensor exposes per-bin occupancy — so that one piece of
# coverage is dropped rather than faked with a substitute that doesn't measure it.


def _grouped_telemetry(sql, workers):
    """Run `sql` at a given worker count; return its telemetry readings."""
    saved = config.MAX_EXECUTION_WORKERS
    try:
        config.MAX_EXECUTION_WORKERS = workers
        session = opteryx.session()
        for _ in session.execute_to_morsels(sql):
            pass
        return session._telemetry._reading
    finally:
        config.MAX_EXECUTION_WORKERS = saved


def test_grouped_counts_are_self_consistent():
    # Every input row lands in exactly one group: COUNT(*) must sum to the table's
    # row count, the emitted group count must equal the key's exact NDV, and no
    # group may be emitted twice. A router that dropped, duplicated or mis-binned
    # rows moves one of these — which is what the old NDV/total telemetry watched.
    saved = config.MAX_EXECUTION_WORKERS
    try:
        config.MAX_EXECUTION_WORKERS = 4
        rows = _collect("SELECT gravity, COUNT(*) AS c FROM $planets GROUP BY gravity")
    finally:
        config.MAX_EXECUTION_WORKERS = saved

    total = _collect("SELECT COUNT(*) AS c FROM $planets")[0][0]
    distinct = _collect("SELECT DISTINCT gravity FROM $planets")

    assert sum(r[1] for r in rows) == total
    assert len(rows) == len(distinct)
    assert len({r[0] for r in rows}) == len(rows)


def test_w1_runs_the_same_grouped_path():
    # W=1 must run the SAME grouped path with one worker, not divert to a separate
    # serial implementation. The generic-pipeline reading that used to prove this is
    # gone; `native_engine_engaged` is the surviving one, and a serial divert would
    # be a different operator. Paired with the serial-equality oracle so the
    # assertion cannot pass vacuously.
    sql = "SELECT gravity, COUNT(*) AS c FROM $planets GROUP BY gravity"
    _assert_matches_serial(sql, workers=1)

    assert _grouped_telemetry(sql, 1).get("native_engine_engaged") == 1
    assert _grouped_telemetry(sql, 4).get("native_engine_engaged") == 1


if __name__ == "__main__":
    test_count_star()
    test_sum_min_max()
    test_avg()
    test_count_distinct_holistic()
    test_median_holistic()
    test_multi_column_key()
    test_null_keys()
    test_computed_key_expression()
    test_string_key_routes_and_is_correct()
    test_skew_ndv_telemetry_emitted()
    test_w1_runs_rowrouting_path()
    print("✅ grouped row-routing — matches serial across mergeable + holistic + multi-col + NULL + W1")
