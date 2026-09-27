# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Correctness gate for the TOP-N RUNTIME BOUNDARY.

See docs/TOPN_RUNTIME_BOUNDARY_DESIGN.md. For `... ORDER BY k LIMIT n` over a
parquet scan, whatever sees the rows that reach the Top-N keeps a running n-th best
value of the leading key, and the scan skips row groups whose footer statistics prove
they cannot beat it. Two producers/consumers exist and BOTH are exercised here:

  * single-pass — NativeParquetScanSource feeding a TopNSink (ClickBench Q25/Q27):
    the sink publishes, the scan's submit loop consumes;
  * two-pass    — LatmatScanSource (ClickBench Q24): pass 1 publishes and consumes.

The boundary is PURE SKIPPING: turning it off can only make a query read more, never
answer differently. Held up three ways, as for the runtime join filter:

1. ``test_oracle_*`` — every eligible shape, boundary on vs off. Compared on the
   three things SQL promises for a LIMIT over possible ties (see
   test_wp_r3_latmat_scan.py for the full argument): the row COUNT, the exact
   multiset of SORT KEYS, and that every returned row is a real row of
   `WHERE <pred>`. WHICH tied rows come back is unspecified and is not compared.
2. ``test_refused_*`` — shapes the boundary must not arm for. Asserted on ARMING,
   not on answers: on fixtures this small an unsound boundary might skip nothing.
3. ``test_positive_control_*`` — it really arms and really skips, on both paths.
   Without these, 1 and 2 would pass with the feature doing nothing.

Fixtures are clustered on the sort key (the precondition the design names, §5):
row i has k = i, written 250 rows per row group, so row groups are narrow, ordered
windows of k. The submission window is pinned small (`parquet_io_in_flight_limit`)
through the relation's `WITH(parquet_io_in_flight_limit = ...)` hint (a RESTRICTED
knob, so the sessions hold `platform_admin`) so the positive controls do not depend on how far ahead the
scan happened to run before the first boundary was published.
"""

import datetime
import decimal
import os
import sys

sys.path.insert(1, os.path.join(os.path.dirname(__file__), "../.."))

import pyarrow as pa  # test-only dep (allowed in tests/)
import pyarrow.parquet as pq
import pytest

import opteryx

N = 20_000
ROW_GROUP = 250            # 80 row groups
WINDOW = 2                 # pinned in-flight window for the positive controls
# 1-in-4 rows match: selective enough for the two-pass scan's late-mat gate.
NEEDLE = "%pick%"


def _write(directory, columns, row_group_size=ROW_GROUP, write_statistics=True):
    os.makedirs(directory, exist_ok=True)
    arrays = {name: pa.array(values, type=typ) for name, (typ, values) in columns.items()}
    pq.write_table(pa.table(arrays), os.path.join(directory, "part.parquet"),
                   row_group_size=row_group_size, write_statistics=write_statistics)
    return directory


def _tags():
    return [("pick-%d" % i) if i % 4 == 0 else ("skip-%d" % i) for i in range(N)]


def _columns(keys, key_type=pa.int64()):
    """`tag` (the predicate), `k` (the sort key) and payload columns — the payload is
    what makes the two-pass scan eligible (pass 2 needs something to fetch)."""
    return {
        "tag": (pa.string(), _tags()),
        "k": (key_type, keys),
        "pay_str": (pa.string(), ["payload-%d" % i for i in range(N)]),
        "pay_i64": (pa.int64(), [i * 7 for i in range(N)]),
    }


def _ds(path, window=WINDOW):
    """The relation text for `path`, with the submission window pinned (None = the
    engine's default window)."""
    if window is None:
        return f"'{path}'"
    return f"'{path}' WITH(parquet_io_in_flight_limit = {window})"


def _run(sql, enabled):
    """(rows, names, telemetry). Rows are tuples of repr()s, so NULLs and types
    compare exactly. Flipped through the session variable — the surface a caller
    actually has, and the one the compiler resolves (a patched config attribute
    would be ignored)."""
    # platform_admin: the in-flight window hint runs SET's permission gate, and every
    # per-scan IO knob is RESTRICTED (tests/unit/planner/test_scan_hint_settings.py).
    session = opteryx.session(entitlements=["platform_admin"])
    setting = f"SET disable_topn_runtime_boundary = {'false' if enabled else 'true'}"
    for _ in session.execute_to_morsels(setting):
        pass
    rows, names = [], []
    for morsel in session.execute_to_morsels(sql):
        raw = list(morsel.column_names)
        names = [n.decode("utf-8") if type(n) is bytes else n for n in raw]
        for i in range(morsel.num_rows):
            rows.append(tuple(repr(morsel.column(n)[i]) for n in raw))
    return rows, names, session.telemetry


def _scan_reading(telemetry):
    readings = [r for r in telemetry.get("operations", {}).values()
                if "row_groups_read" in r]
    assert len(readings) == 1, f"expected exactly one scan reading, got {len(readings)}"
    return readings[0]


def _assert_oracle(path, sql_tail, key="k", window=WINDOW):
    """Boundary on vs off: same count, same sort-key multiset, every row real.
    Returns (source, on_telemetry, off_telemetry)."""
    sql = "SELECT " + sql_tail.format(DATASET=_ds(path, window))
    on_rows, on_names, on_tel = _run(sql, True)
    off_rows, off_names, off_tel = _run(sql, False)
    assert on_names == off_names, "output layout differs"
    assert len(on_rows) == len(off_rows), (
        f"row COUNT differs: on {len(on_rows)} vs off {len(off_rows)}\n{sql}")
    k = on_names.index(key)
    on_keys = sorted(r[k] for r in on_rows)
    off_keys = sorted(r[k] for r in off_rows)
    assert on_keys == off_keys, (
        f"sort-key multiset differs — the boundary changed the ANSWER\n{sql}\n"
        f"  on : {on_keys}\n  off: {off_keys}")
    if " WHERE " in sql_tail:
        where = sql_tail.split(" WHERE ", 1)[1].split(" ORDER BY ")[0]
        projection = sql_tail.split(" FROM ", 1)[0]
        universe_sql = f"SELECT {projection} FROM '{path}' WHERE {where}"
        universe = set(_run(universe_sql, False)[0])
        stray = [r for r in on_rows if r not in universe]
        assert not stray, f"{len(stray)} returned row(s) are not rows of the table"
    sources = list(on_tel["scan_sources"].values())
    assert len(sources) == 1
    return sources[0], on_tel, off_tel


def _armed(telemetry):
    return telemetry.get("topn_runtime_boundaries_armed", 0)


# The two query shapes, one per path. `{DATASET}` is filled with the file path.
SINGLE_PASS = "k, tag FROM {DATASET} WHERE tag LIKE '" + NEEDLE + "' ORDER BY k{DIR} LIMIT {N}"
TWO_PASS = "* FROM {DATASET} WHERE tag LIKE '" + NEEDLE + "' ORDER BY k{DIR} LIMIT {N}"
SHAPES = (("single-pass", SINGLE_PASS, "NativeParquetScanSource"),
          ("two-pass", TWO_PASS, "LatmatScanSource"))


def _shape(template, direction="", limit=10):
    return template.replace("{DIR}", direction).replace("{N}", str(limit))


# ---------------------------------------------------------------------------------
# 1. Oracle — on and off must agree, on both paths
# ---------------------------------------------------------------------------------


@pytest.mark.parametrize("label,template,source", SHAPES)
@pytest.mark.parametrize("direction", ["", " ASC", " DESC"])
def test_oracle_unique_keys(tmp_path, label, template, source, direction):
    path = _write(str(tmp_path / "unique"), _columns(list(range(N))))
    got, on, _off = _assert_oracle(path, _shape(template, direction))
    assert got == source, f"{label}: expected {source}, got {got}"
    assert _armed(on) == 1, f"{label}: the boundary did not arm"


@pytest.mark.parametrize("label,template,source", SHAPES)
@pytest.mark.parametrize("direction", ["", " DESC"])
def test_oracle_ties_across_row_groups(tmp_path, label, template, source, direction):
    """Tie blocks of 600 rows straddle row groups of 250: a boundary EQUAL to a row
    group's min must keep it (the skip test is strict)."""
    path = _write(str(tmp_path / "ties"), _columns([i // 600 for i in range(N)]))
    _assert_oracle(path, _shape(template, direction, limit=40))


@pytest.mark.parametrize("label,template,source", SHAPES)
@pytest.mark.parametrize("nulls", [" NULLS FIRST", " NULLS LAST"])
@pytest.mark.parametrize("direction", [" ASC", " DESC"])
def test_oracle_nulls(tmp_path, label, template, source, nulls, direction):
    """NULL keys scattered into LATE row groups. Under NULLS FIRST a NULL beats every
    value, so a row group holding one must never be skipped — these rows belong in
    the answer. Under NULLS LAST they never block a skip."""
    keys = [None if (i % 997 == 0 and i > N // 2) else i for i in range(N)]
    path = _write(str(tmp_path / "nulls"), _columns(keys))
    tail = _shape(template, direction + nulls, limit=12)
    _assert_oracle(path, tail)


@pytest.mark.parametrize("label,template,source", SHAPES)
def test_oracle_all_null_row_groups(tmp_path, label, template, source):
    """Whole row groups with no min/max at all must be kept, not guessed about."""
    keys = [None if 5000 <= i < 7500 else i for i in range(N)]
    path = _write(str(tmp_path / "allnull"), _columns(keys))
    _assert_oracle(path, _shape(template, " DESC NULLS FIRST", limit=10))
    _assert_oracle(path, _shape(template, " ASC NULLS LAST", limit=10))


@pytest.mark.parametrize("label,template,source", SHAPES)
def test_oracle_limit_larger_than_matches(tmp_path, label, template, source):
    path = _write(str(tmp_path / "big_limit"), _columns(list(range(N))))
    _assert_oracle(path, _shape(template, "", limit=N))


@pytest.mark.parametrize("label,template,source", SHAPES)
def test_oracle_predicate_matches_nothing(tmp_path, label, template, source):
    path = _write(str(tmp_path / "nothing"), _columns(list(range(N))))
    sql_tail = _shape(template).replace(NEEDLE, "%no-such-tag%")
    _assert_oracle(path, sql_tail)


@pytest.mark.parametrize("label,template,source", SHAPES)
def test_oracle_row_groups_without_statistics(tmp_path, label, template, source):
    """No footer statistics: armed, skips nothing, answers exactly."""
    path = _write(str(tmp_path / "nostats"), _columns(list(range(N))),
                  write_statistics=False)
    _src, on, _off = _assert_oracle(path, _shape(template))
    assert _scan_reading(on).get("row_groups_pruned_topn", 0) == 0


@pytest.mark.parametrize(
    "key_type,keys",
    [
        (pa.int32(), list(range(N))),
        (pa.int16(), [i // 2 for i in range(N)]),
        (pa.uint32(), list(range(N))),
        # Values above 2**31 exercise the unsigned statistic path (a signed read of
        # these bytes would invert the range and skip real answers).
        (pa.uint32(), [2**31 + i for i in range(N)]),
        (pa.int64(), [-(2**62) + i for i in range(N)]),
        (pa.date32(), [datetime.date(2000, 1, 1) + datetime.timedelta(days=i // 10)
                       for i in range(N)]),
    ],
    ids=["int32", "int16", "uint32", "uint32_high", "int64_negative", "date32"],
)
@pytest.mark.parametrize("direction", ["", " DESC"])
def test_oracle_admitted_types(tmp_path, key_type, keys, direction):
    path = _write(str(tmp_path / "types"), _columns(keys, key_type))
    _src, on, _off = _assert_oracle(path, _shape(SINGLE_PASS, direction))
    assert _armed(on) == 1, f"{key_type} should arm the boundary"


def test_oracle_multi_key_leading_key_only(tmp_path):
    """ClickBench Q27's shape: two sort keys. The boundary uses the leading key only,
    strictly, so a leading-key tie block straddling the boundary must be kept whole
    for the second key to break it. The full rows are deterministic here (the second
    key breaks every tie), so they are compared exactly, in order."""
    path = _write(str(tmp_path / "multikey"), _columns([i // 600 for i in range(N)]))
    sql = f"SELECT k, tag FROM {_ds(path)} WHERE tag LIKE '{NEEDLE}' ORDER BY k, tag LIMIT 40"
    on_rows, _, on = _run(sql, True)
    off_rows, _, _ = _run(sql, False)
    assert on_rows == off_rows
    assert _armed(on) == 1


# ---------------------------------------------------------------------------------
# 2. Refusals — must not arm
# ---------------------------------------------------------------------------------


@pytest.mark.parametrize(
    "key_type,keys",
    [
        (pa.float64(), [float(i) for i in range(N)]),
        (pa.timestamp("ms"), [datetime.datetime(2000, 1, 1) + datetime.timedelta(seconds=i)
                              for i in range(N)]),
        (pa.string(), ["%08d" % i for i in range(N)]),
        (pa.decimal128(12, 2), [decimal.Decimal(i) / 100 for i in range(N)]),
        (pa.uint64(), list(range(N))),
    ],
    ids=["float64", "timestamp", "string", "decimal", "uint64"],
)
def test_refused_types(tmp_path, key_type, keys):
    """Types outside the allow-list (§6): refusing costs a read, never an answer —
    and the answer is still checked."""
    path = _write(str(tmp_path / "refused"), _columns(keys, key_type))
    _src, on, _off = _assert_oracle(path, _shape(SINGLE_PASS))
    assert _armed(on) == 0, f"{key_type} must not arm the boundary"
    assert "row_groups_pruned_topn" not in _scan_reading(on)


def test_refused_when_switched_off(tmp_path):
    path = _write(str(tmp_path / "off"), _columns(list(range(N))))
    _rows, _names, off = _run("SELECT " + _shape(SINGLE_PASS).format(DATASET=_ds(path)),
                              False)
    assert _armed(off) == 0
    assert "row_groups_pruned_topn" not in _scan_reading(off)


def test_oracle_with_offset(tmp_path):
    """LIMIT l OFFSET o plans as HeapSort(l + o) under a Limit that drops the first o,
    so the boundary is built on the HeapSort's own n = l + o and stays sound. Keys are
    unique here, so the rows are compared exactly, in order."""
    path = _write(str(tmp_path / "offset"), _columns(list(range(N))))
    sql = f"SELECT k, tag FROM {_ds(path)} WHERE tag LIKE '{NEEDLE}' ORDER BY k LIMIT 10 OFFSET 5"
    on_rows, _, on = _run(sql, True)
    off_rows, _, _ = _run(sql, False)
    assert on_rows == off_rows
    assert len(on_rows) == 10
    assert _armed(on) == 1


def test_refused_for_a_computed_leading_key(tmp_path):
    """ORDER BY an expression: no footer statistic describes it."""
    path = _write(str(tmp_path / "computed"), _columns(list(range(N))))
    sql = f"SELECT k, tag FROM {_ds(path)} WHERE tag LIKE '{NEEDLE}' ORDER BY k * -1 LIMIT 10"
    on_rows, _, on = _run(sql, True)
    off_rows, _, _ = _run(sql, False)
    assert sorted(on_rows) == sorted(off_rows)
    assert _armed(on) == 0


# ---------------------------------------------------------------------------------
# 3. Positive controls — it arms, it skips, and the telemetry says so
# ---------------------------------------------------------------------------------


@pytest.mark.parametrize("label,template,source", SHAPES)
@pytest.mark.parametrize("direction", ["", " DESC"])
def test_positive_control_skips_row_groups(tmp_path, label, template, source, direction):
    """The file is clustered in the direction of the sort, so its FIRST row group
    holds the whole answer and every later one is skippable once the boundary exists.

    Clustered the other way (descending sort over ascending data) the answer sits in
    the LAST row groups, the boundary only appears once the scan reaches them, and
    nothing is left to skip: row groups are walked in file order, and reordering them
    by their statistics is deferred (design D5). That case is covered, for
    correctness, by the oracle tests above."""
    keys = list(range(N)) if direction == "" else [N - i for i in range(N)]
    path = _write(str(tmp_path / "control"), _columns(keys))
    got, on, off = _assert_oracle(path, _shape(template, direction))
    assert got == source
    on_scan, off_scan = _scan_reading(on), _scan_reading(off)
    skipped = on_scan.get("row_groups_pruned_topn")
    total = N // ROW_GROUP
    assert skipped is not None and skipped > 0, f"{label}: armed but skipped nothing"
    # How much can already be in flight when the boundary is first published: the
    # pinned window, plus — on the single-pass path — one unit per execution worker,
    # because every worker claims a result (advancing the submit frontier) before the
    # first one has come back. The two-pass scan's pass 1 is one thread.
    dop = on.get("native_engine_dop", 1) if label == "single-pass" else 0
    lag = WINDOW + dop + 2
    assert skipped >= total - lag, (
        f"{label}: skipped only {skipped} of {total} row groups (lag allowance {lag})")
    assert on_scan["row_groups_read"] == off_scan["row_groups_read"] - skipped
    # Absent — not zero — when nothing was armed.
    assert "row_groups_pruned_topn" not in off_scan


@pytest.mark.parametrize("label,template,source", SHAPES)
def test_positive_control_default_window(tmp_path, label, template, source):
    """No pinned window: still correct, still skips (the default window is larger than
    WINDOW, so only a lower bound is asserted)."""
    path = _write(str(tmp_path / "default_window"), _columns(list(range(N))))
    _src, on, _off = _assert_oracle(path, _shape(template), window=None)
    assert _scan_reading(on).get("row_groups_pruned_topn", 0) > 0


if __name__ == "__main__":  # pragma: no cover
    sys.exit(pytest.main([__file__, "-q"]))
