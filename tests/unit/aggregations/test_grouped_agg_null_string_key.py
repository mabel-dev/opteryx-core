"""Regression test for null validity in string GROUP BY keys (GsAccum path, WP-2).

The GsAccum validity bitmap grows in lockstep with its slots buffer, and
gs_accum_append's per-row null branch is a single lazy allocation plus a cheap
bit-clear. Previously each null row called _ks_ensure_bitmap_capacity, which
recomputed "current bytes" from the logical row count rather than the allocated
capacity and reallocated roughly every 8 null rows — quadratic on null-heavy,
high-cardinality string keys.

These tests use column-sourced nulls (parquet) and multiple files so the engine
ingests several morsels — exercising the cross-morsel invariant that a non-NULL
bitmap always covers slots_cap as the accumulator crosses growth boundaries,
including nulls that arrive in a later morsel after capacity already grew.
"""
import os
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

import pyarrow as pa
import pyarrow.parquet as pq

import opteryx
from opteryx.connectors import DiskConnector

_WS = [0]


def _ground_truth(files):
    counts = {}
    for rows in files:
        for v in rows:
            counts[v] = counts.get(v, 0) + 1
    return counts


def _run(files):
    """Write each list in `files` as its own parquet (→ its own morsel), then
    GROUP BY the string column and return {key_or_None: count}."""
    _WS[0] += 1
    ws = f"ws_nullstr_{_WS[0]}"
    with tempfile.TemporaryDirectory() as tmp:
        data_dir = os.path.join(tmp, ws, "t")
        os.makedirs(data_dir)
        for i, rows in enumerate(files):
            pq.write_table(
                pa.table({"s": pa.array(rows, type=pa.string())}),
                os.path.join(data_dir, f"part_{i}.parquet"),
            )
        cwd = os.getcwd()
        os.chdir(tmp)
        try:
            opteryx.register_workspace(ws, DiskConnector)
            got = {}
            for m in opteryx.session().execute_to_morsels(
                f"SELECT s, COUNT(*) AS c FROM {ws}.t GROUP BY s"
            ):
                ks = m.column(b"s").to_pylist()
                cs = m.column(b"c").to_pylist()
                for k, c in zip(ks, cs):
                    got[k] = got.get(k, 0) + c
            return got
        finally:
            os.chdir(cwd)


def test_highcard_null_string_key_crosses_growth_boundaries():
    # 500 distinct keys (forces GsAccum past 16→32→…→512), every other row NULL.
    keys = [f"key_{i:04d}" for i in range(500)]
    rows = []
    for k in keys:
        rows.append(k)
        rows.append(None)
    got = _run([rows])
    assert got == _ground_truth([rows])
    assert got.get(None) == 500
    assert all(got[k] == 1 for k in keys)


def test_nulls_arrive_in_later_morsel():
    # Morsel 1: many distinct non-null keys (grows slots_cap, bitmap stays NULL).
    # Morsel 2: introduces the first nulls — the lazy bitmap must size to the
    # already-grown slots_cap, and later growth must keep covering it.
    first = [f"a_{i:03d}" for i in range(300)]
    second = []
    for i in range(300):
        second.append(f"b_{i:03d}")  # new distinct keys → more growth
        second.append(None)          # nulls, first seen in this later morsel
    got = _run([first, second])
    assert got == _ground_truth([first, second])
    assert got.get(None) == 300
    assert got["a_000"] == 1 and got["b_299"] == 1


def test_mostly_null_string_key():
    # 95% NULL with a sprinkling of distinct non-null keys. Keeps at least one
    # non-null row (an all-null string *column* is dropped by the scan — a
    # separate, upstream pre-existing bug, not the GsAccum null path). The null
    # rows all collapse to one NULL group; the heavy null fraction stresses the
    # per-null bit-clear path that WP-2 made realloc-free.
    rows = []
    for i in range(2000):
        rows.append(f"k_{i:04d}" if i % 20 == 0 else None)
    got = _run([rows])
    assert got == _ground_truth([rows])
    assert got.get(None) == sum(1 for r in rows if r is None)


def test_alternating_with_repeats_multi_morsel():
    f1 = [("x" if i % 3 else None) for i in range(50)]
    f2 = [(None if i % 2 else "y") for i in range(50)]
    got = _run([f1, f2])
    assert got == _ground_truth([f1, f2])


if __name__ == "__main__":
    for fn in (
        test_highcard_null_string_key_crosses_growth_boundaries,
        test_nulls_arrive_in_later_morsel,
        test_mostly_null_string_key,
        test_alternating_with_repeats_multi_morsel,
    ):
        fn()
        print("PASS", fn.__name__)
