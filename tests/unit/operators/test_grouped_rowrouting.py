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

Row-routing is the only grouped strategy and engages on any input above the
row-floor. The serial oracle is obtained by raising the floor above the fixture
row count (the input is then buffered and keyed by a single engine); the routed
run uses floor=0 so row-routing engages on the small fixtures.
"""
import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

import opteryx
from opteryx import config

# Above any fixture's row count — forces the row-floor serial path (single engine).
_SERIAL_FLOOR = 10_000_000


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


def _run(sql, *, workers, floor):
    saved = (
        config.MAX_EXECUTION_WORKERS,
        config.PARALLEL_MIN_ROWS,
    )
    try:
        config.MAX_EXECUTION_WORKERS = workers
        config.PARALLEL_MIN_ROWS = floor
        return _collect(sql)
    finally:
        (
            config.MAX_EXECUTION_WORKERS,
            config.PARALLEL_MIN_ROWS,
        ) = saved


def _assert_matches_serial(sql, workers=4):
    serial = _run(sql, workers=1, floor=_SERIAL_FLOOR)
    routed = _run(sql, workers=workers, floor=0)
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
    saved = (
        config.MAX_EXECUTION_WORKERS,
        config.PARALLEL_MIN_ROWS,
    )
    try:
        config.MAX_EXECUTION_WORKERS = 4
        # floor=1 buffers a non-empty floor SAMPLE (the skew snapshot) while still
        # engaging row-routing — floor=0 would route but leave the sample empty.
        config.PARALLEL_MIN_ROWS = 1
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
        (
            config.MAX_EXECUTION_WORKERS,
            config.PARALLEL_MIN_ROWS,
        ) = saved

    for k in (
        "rowrouting_workers",
        "rowrouting_sample_rows",
        "rowrouting_sample_maxbin_rows",
        "rowrouting_total_rows",
        "rowrouting_maxbin_rows",
        "rowrouting_ndv",
    ):
        assert k in rd, f"missing telemetry reading: {k}"

    w = rd["rowrouting_workers"]
    total = rd["rowrouting_total_rows"]
    assert 1 <= w <= 4
    assert total == total_from_counts  # COUNT(*) per group sums to all input rows
    assert rd["rowrouting_ndv"] == group_count  # exact distinct group count
    # pigeonhole: average <= busiest bin <= total
    assert total / w <= rd["rowrouting_maxbin_rows"] <= total
    assert 0 < rd["rowrouting_sample_rows"] <= total
    assert rd["rowrouting_sample_maxbin_rows"] <= rd["rowrouting_sample_rows"]


def test_w1_runs_rowrouting_path():
    # W=1 must GENUINELY run the row-routing path (one worker), not divert to
    # serial. Prove it ran via the telemetry only _grouped_agg_stream emits (so
    # the assertion can't pass vacuously by both runs being serial), and that the
    # result still matches serial.
    sql = "SELECT gravity, COUNT(*) AS c FROM $planets GROUP BY gravity"
    _assert_matches_serial(sql, workers=1)

    saved = (
        config.MAX_EXECUTION_WORKERS,
        config.PARALLEL_MIN_ROWS,
    )
    try:
        config.MAX_EXECUTION_WORKERS = 1
        config.PARALLEL_MIN_ROWS = 0
        sess = __import__("opteryx").session()
        for _ in sess.execute_to_morsels(sql):
            pass
        rd = sess._telemetry._reading
    finally:
        (
            config.MAX_EXECUTION_WORKERS,
            config.PARALLEL_MIN_ROWS,
        ) = saved

    assert rd.get("rowrouting_workers") == 1, "W=1 must run row-routing (1 worker), not serial"
    assert "rowrouting_ndv" in rd


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
