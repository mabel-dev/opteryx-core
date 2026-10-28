# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""`WHERE … ORDER BY … LIMIT n` over an ORC scan: top-N late materialisation
(docs/ORC_TOPN_LATE_MATERIALISATION_DESIGN.md).

Pass 1 decodes the predicate columns and the ORDER BY keys for every stripe; each decode
worker keeps its best n (plus ties); at the barrier the cut keeps every row not strictly worse
than the n-th best; pass 2 decodes the other columns for only the row groups holding a kept
row. The HeapSort above makes the final cut.

Every returned row is checked against the row pyarrow.orc wrote with the same id (test-only,
CLAUDE.md §4), and the ORDER BY keys of the answer against a Python ordering with the NULL
placement written out. Ties at the boundary make WHICH tied row is returned free, so the keys
are compared, not the ids. `orc_topn_kept_rows` / `orc_pass2_rows` show pass 2 is small."""

import datetime
import functools
import os
import sys
import time

import pytest

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))
os.environ["TZ"] = "UTC"
time.tzset()

import pyarrow as pa
import pyarrow.orc as po

import opteryx

UTC = datetime.timezone.utc
ROWS = 150_007
NAMES = ["id", "grp", "ts", "word", "real", "pad"]


def _table():
    ids = range(ROWS)
    return pa.table(
        {
            "id": pa.array(ids, pa.int64()),
            "grp": pa.array([None if i % 97 == 0 else (i * 7919) % 500 for i in ids], pa.int32()),
            "ts": pa.array(
                [None if i % 13 == 0 else datetime.datetime(2024, 1, 1) + datetime.timedelta(seconds=(i * 104729) % 9_000_000) for i in ids],
                pa.timestamp("us"),
            ),
            "word": pa.array([None if i % 11 == 0 else f"w{(i * 31) % 2000:05d}" for i in ids], pa.string()),
            "real": pa.array([None if i % 17 == 0 else ((i * 613) % 10_000) / 8 for i in ids], pa.float64()),
            "pad": pa.array([f"pad-{i}-{'z' * (i % 30)}" for i in ids], pa.string()),
        }
    )


TABLE = _table()
BY_ID = {}
for _row in TABLE.to_pylist():
    if _row["ts"] is not None:
        _row["ts"] = _row["ts"].replace(tzinfo=UTC)
    BY_ID[_row["id"]] = _row


def _ordering(keys):
    """A comparator for rows under [(column, ascending, nulls_first)]."""

    def cmp(a, b):
        for column, ascending, nulls_first in keys:
            x, y = a[column], b[column]
            if x is None or y is None:
                if x is None and y is None:
                    continue
                return (-1 if x is None else 1) * (1 if nulls_first else -1)
            if x != y:
                return (-1 if x < y else 1) * (1 if ascending else -1)
        return 0

    return functools.cmp_to_key(cmp)


def _sql_order(keys):
    return ", ".join(
        f"{c} {'ASC' if asc else 'DESC'} NULLS {'FIRST' if nf else 'LAST'}" for c, asc, nf in keys
    )


@pytest.fixture(scope="module", params=["zstd", "uncompressed"])
def orc_path(request, tmp_path_factory):
    path = tmp_path_factory.mktemp("orc") / f"t_{request.param}.orc"
    with open(path, "wb") as f:
        po.write_table(TABLE, f, compression=request.param, stripe_size=64 * 1024, row_index_stride=10_000)
    return path


CASES = [
    # (WHERE, the same in Python, ORDER BY keys, LIMIT)
    ("word LIKE 'w01%'", lambda r: r["word"] is not None and r["word"].startswith("w01"), [("ts", True, False)], 10),
    ("word LIKE 'w01%'", lambda r: r["word"] is not None and r["word"].startswith("w01"), [("ts", False, True)], 10),
    ("grp < 40", lambda r: r["grp"] is not None and r["grp"] < 40, [("ts", True, True)], 7),
    # Heavy ties at the boundary: grp has 500 values.
    ("id % 3 = 0", lambda r: r["id"] % 3 == 0, [("grp", True, False)], 25),
    ("id % 3 = 0", lambda r: r["id"] % 3 == 0, [("grp", False, False), ("word", True, True)], 25),
    # A string key, and a key the predicate does not read.
    ("real > 1000", lambda r: r["real"] is not None and r["real"] > 1000, [("word", True, True)], 12),
    # More survivors than... fewer survivors than the LIMIT.
    ("id BETWEEN 1000 AND 1004", lambda r: 1000 <= r["id"] <= 1004, [("ts", True, False)], 50),
    # No survivor.
    ("id < 0", lambda r: False, [("ts", True, False)], 10),
]


@pytest.mark.parametrize("where, test, keys, limit", CASES)
def test_top_n_matches_the_ordering(orc_path, where, test, keys, limit):
    sql = f"SELECT {', '.join(NAMES)} FROM READ_ORC('{orc_path}') WHERE {where} ORDER BY {_sql_order(keys)} LIMIT {limit}"
    session = opteryx.session()
    got = []
    for morsel in session.execute_to_morsels(sql):
        columns = [morsel.column(n).to_pylist() for n in NAMES]
        got.extend(dict(zip(NAMES, values)) for values in zip(*columns))
    ops = [op for op in session.telemetry["operations"].values() if "orc_topn_kept_rows" in op]
    assert len(ops) == 1, "top-N was not armed"

    survivors = sorted((r for r in BY_ID.values() if test(r)), key=_ordering(keys))
    want = survivors[:limit]
    assert len(got) == len(want)
    for row in got:   # every column of every row is the row that was written
        assert row == BY_ID[row["id"]], row["id"]
        assert test(row)
    key_of = lambda r: tuple(r[c] for c, _, _ in keys)
    assert [key_of(r) for r in got] == [key_of(r) for r in want]

    kept, pass2 = ops[0]["orc_topn_kept_rows"], ops[0]["orc_pass2_rows"]
    assert kept >= len(want)
    # Pass 2 reads one row group (10,000 rows, or the 1,024-row stripe) per kept row at most.
    assert pass2 <= kept * 10_000


def test_top_n_reads_few_row_groups_in_pass_2(orc_path):
    sql = f"SELECT * FROM READ_ORC('{orc_path}') WHERE word LIKE 'w01%' ORDER BY ts NULLS LAST LIMIT 5"
    session = opteryx.session()
    rows = sum(m.num_rows for m in session.execute_to_morsels(sql))
    assert rows == 5
    op = [op for op in session.telemetry["operations"].values() if "orc_topn_kept_rows" in op][0]
    # Distinct timestamps, NULLs last: no ties. (By default NULLs sort lowest, and every
    # NULL survivor would tie at the boundary and be kept.)
    assert op["orc_topn_kept_rows"] == 5
    assert op["orc_pass2_rows"] <= 5 * 10_000     # against 150,007 rows without the cut


def test_a_shape_that_is_not_top_n_runs_the_plain_prefilter(orc_path):
    # A computed ORDER BY key: no top-N, the same rows.
    sql = f"SELECT id FROM READ_ORC('{orc_path}') WHERE grp = 7 ORDER BY id + 1 LIMIT 3"
    session = opteryx.session()
    got = [v for m in session.execute_to_morsels(sql) for v in m.column("id").to_pylist()]
    assert got == sorted(r["id"] for r in BY_ID.values() if r["grp"] == 7)[:3]
    assert not [op for op in session.telemetry["operations"].values() if "orc_topn_kept_rows" in op]


def test_a_worker_cuts_its_own_buffer_as_it_grows(tmp_path):
    # One 300,000-row stripe: one decode worker holds every survivor, so it passes
    # max(4n, 65,536) rows and cuts its buffer back mid-scan, more than once. Keys repeat
    # (ties across the cuts) and have NULLs.
    n = 300_000
    table = pa.table(
        {
            "id": pa.array(range(n), pa.int64()),
            "key": pa.array([None if i % 101 == 0 else (i * 7919) % 50_000 for i in range(n)], pa.int64()),
            "pad": pa.array([f"p{i}" for i in range(n)], pa.string()),
        }
    )
    path = tmp_path / "one.orc"
    with open(path, "wb") as f:
        po.write_table(table, f, compression="zstd", stripe_size=512 * 1024 * 1024, row_index_stride=10_000)
    assert po.ORCFile(str(path)).nstripes == 1
    rows = table.to_pylist()
    for order, ascending, nulls_first in [("ASC NULLS LAST", True, False), ("DESC NULLS FIRST", False, True)]:
        session = opteryx.session()
        got = [
            row
            for m in session.execute_to_morsels(
                f"SELECT id, key, pad FROM READ_ORC('{path}') WHERE id % 2 = 0 ORDER BY key {order} LIMIT 40"
            )
            for row in zip(m.column("id").to_pylist(), m.column("key").to_pylist(), m.column("pad").to_pylist())
        ]
        survivors = sorted((r for r in rows if r["id"] % 2 == 0), key=_ordering([("key", ascending, nulls_first)]))
        assert [k for _, k, _ in got] == [r["key"] for r in survivors[:40]]
        for i, k, pad in got:
            assert rows[i]["key"] == k and pad == f"p{i}"
        op = [op for op in session.telemetry["operations"].values() if "orc_topn_kept_rows" in op][0]
        # Each key value repeats ~3 times, so ASC keeps the 40 plus a few ties; NULLS FIRST
        # keeps every NULL survivor (~1,500, all tied at the top).
        nulls = sum(1 for r in survivors if r["key"] is None)
        assert op["orc_topn_kept_rows"] < 200 if ascending else op["orc_topn_kept_rows"] == nulls
