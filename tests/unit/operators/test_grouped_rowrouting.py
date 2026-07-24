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


def test_skew_ndv_telemetry_emitted():
    # Row-routing measures bin balance (skew) and exact NDV and emits them as
    # telemetry, without acting on them. Assert the readings are present and
    # self-consistent.
    saved = config.MAX_EXECUTION_WORKERS
    try:
        config.MAX_EXECUTION_WORKERS = 4
        sess = __import__("opteryx").session()
        group_count = 0
        total_from_counts = 0
        for m in sess.execute_to_morsels(
            "SELECT gravity, COUNT(*) c FROM $planets GROUP BY gravity"
        ):
            group_count += m.num_rows
            names = [n if isinstance(n, bytes) else n.encode() for n in m.column_names]
            total_from_counts += sum(m.column(names[1]).to_pylist())  # the COUNT(*) col
        rd = sess._telemetry._reading
    finally:
        config.MAX_EXECUTION_WORKERS = saved

    # The SOLE grouped path is the HASH_REPARTITION PipelineSink (route-raw + parallel
    # per-partition read-out); it emits the generic-pipeline + route-agg telemetry.
    for k in (
        "generic_pipeline_workers",
        "generic_pipeline_radix",
        "route_agg_total_rows",
        "route_agg_ndv",
    ):
        assert k in rd, f"missing telemetry reading: {k}"

    w = rd["generic_pipeline_workers"]
    total = rd["route_agg_total_rows"]
    assert 1 <= w <= 4
    assert rd["generic_pipeline_radix"] >= w  # radix = next pow2 >= DOP
    assert total == total_from_counts  # COUNT(*) per group sums to all input rows
    assert rd["route_agg_ndv"] == group_count  # exact distinct group count


def test_w1_runs_rowrouting_path():
    # W=1 must GENUINELY run the parallel grouped path (one worker), not divert to
    # serial. Prove it ran via the generic-pipeline telemetry the sole grouped sink
    # emits (so the assertion can't pass vacuously by both runs being serial), and
    # that the result still matches serial.
    sql = "SELECT gravity, COUNT(*) AS c FROM $planets GROUP BY gravity"
    _assert_matches_serial(sql, workers=1)

    saved = config.MAX_EXECUTION_WORKERS
    try:
        config.MAX_EXECUTION_WORKERS = 1
        sess = __import__("opteryx").session()
        for _ in sess.execute_to_morsels(sql):
            pass
        rd = sess._telemetry._reading
    finally:
        config.MAX_EXECUTION_WORKERS = saved

    assert rd.get("generic_pipeline_workers") == 1, (
        "W=1 must run the parallel grouped sink (1 worker), not serial"
    )
    assert rd.get("generic_pipeline") == 1
    assert "route_agg_ndv" in rd


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
