# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""The ORC scan runs the WHERE directly above it itself, two-pass (docs/ORC_READER_DESIGN.md §6.6).

The predicate's columns are decoded for every row; the other columns only for the row groups
from a range's first survivor to its last (with a row index, by seeking there; without one, the
whole range), and the survivors taken. Every answer is checked against what pyarrow.orc wrote
(test-only, CLAUDE.md §4) over every primitive kind with nulls, dictionary strings, and nested
columns: with row groups that are seeked to and ones that are not, compressed and not, RLE v1 and v2, one stripe and many.
`orc_pass2_rows` (the rows the second pass decoded) shows the seek is real. A file without a row
index is not covered here: pyarrow cannot write one."""

import datetime
import decimal
import json
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
from opteryx.exceptions import DataError

UTC = datetime.timezone.utc
ROWS = 150_007
NAMES = ["id", "i16", "flag", "real", "unique", "few", "day", "ts", "price", "tags", "point"]


def _table():
    ids = range(ROWS)
    return pa.table(
        {
            "id": pa.array(ids, pa.int64()),
            "i16": pa.array([None if i % 7 == 0 else (i % 30_000) - 15_000 for i in ids], pa.int16()),
            "flag": pa.array([None if i % 5 == 0 else i % 3 == 0 for i in ids], pa.bool_()),
            "real": pa.array([None if i % 9 == 0 else i / 4 for i in ids], pa.float64()),
            "unique": pa.array([None if i % 3 == 0 else f"value-{i}-{'x' * (i % 20)}" for i in ids], pa.string()),
            "few": pa.array([None if i % 19 == 0 else f"category {i % 40}" for i in ids], pa.string()),
            "day": pa.array([None if i % 29 == 0 else datetime.date(2020, 1, 1) + datetime.timedelta(days=i % 3000) for i in ids], pa.date32()),
            "ts": pa.array(
                [None if i % 31 == 0 else datetime.datetime(2024, 1, 1) + datetime.timedelta(seconds=i * 37, microseconds=i % 1000) for i in ids],
                pa.timestamp("us"),
            ),
            "price": pa.array([None if i % 37 == 0 else decimal.Decimal(i) / 100 for i in ids], pa.decimal128(12, 2)),
            "tags": pa.array([None if i % 41 == 0 else list(range(i % 4)) for i in ids], pa.list_(pa.int64())),
            "point": pa.array(
                [None if i % 43 == 0 else {"x": i, "label": None if i % 2 else f"p{i}"} for i in ids],
                pa.struct([("x", pa.int64()), ("label", pa.string())]),
            ),
        }
    )


TABLE = _table()
ROWS_BY_ID = []
for _row in TABLE.to_pylist():
    if _row["ts"] is not None:
        _row["ts"] = _row["ts"].replace(tzinfo=UTC)
    ROWS_BY_ID.append(_row)

# (WHERE clause, the same test in Python)
PREDICATES = [
    ("id BETWEEN 70000 AND 70100", lambda r: 70000 <= r["id"] <= 70100),
    ("id % 25013 = 7", lambda r: r["id"] % 25013 == 7),
    ("few = 'category 3' AND i16 > 0", lambda r: r["few"] == "category 3" and r["i16"] is not None and r["i16"] > 0),
    ("flag = TRUE AND unique IS NOT NULL AND id < 40000", lambda r: r["flag"] is True and r["unique"] is not None and r["id"] < 40000),
    ("id < 0", lambda r: False),
    ("id >= 0", lambda r: True),
]


def _write(path, **options):
    with open(path, "wb") as f:
        po.write_table(TABLE, f, **options)
    return path


def _query(sql):
    session = opteryx.session()
    got = []
    for morsel in session.execute_to_morsels(sql):
        columns = [morsel.column(n).to_pylist() for n in NAMES]
        for values in zip(*columns):
            row = dict(zip(NAMES, values))
            if row["point"] is not None:
                row["point"] = json.loads(row["point"])
            got.append(row)
    pass2 = [op["orc_pass2_rows"] for op in session.telemetry["operations"].values() if "orc_pass2_rows" in op]
    assert len(pass2) == 1, "the prefilter was not armed"
    return sorted(got, key=lambda r: r["id"]), pass2[0]


@pytest.mark.parametrize("compression", ["zstd", "uncompressed"])
@pytest.mark.parametrize("version", ["0.11", "0.12"])
# A row group larger than a morsel (1,000,000) is read without seeking, as a file without a row
# index is (pyarrow cannot write one).
@pytest.mark.parametrize("stride", [10_000, 1_000_000])
# pyarrow writes 7 zstd stripes at 64 KiB, and 1,024-row stripes whenever uncompressed.
@pytest.mark.parametrize("stripe_size", [512 * 1024 * 1024, 64 * 1024])
def test_where_gives_the_rows_the_filter_would(tmp_path, compression, version, stride, stripe_size):
    path = _write(
        tmp_path / "t.orc",
        compression=compression,
        file_version=version,
        row_index_stride=stride,
        stripe_size=stripe_size,
        dictionary_key_size_threshold=0.5,
    )
    for where, test in PREDICATES:
        got, pass2 = _query(f"SELECT {', '.join(NAMES)} FROM READ_ORC('{path}') WHERE {where}")
        want = [r for r in ROWS_BY_ID if test(r)]
        assert len(got) == len(want), where
        for g, w in zip(got, want):
            assert g == w, (where, w["id"])
        if not want:
            assert pass2 == 0, where
        assert pass2 <= ROWS, where
        if where.startswith("id BETWEEN") and stride == 10_000:
            # The survivors sit in one row group (two, where a stripe boundary splits it): the
            # other columns are decoded for those only.
            assert pass2 <= 2 * stride, pass2


def test_where_whose_columns_are_the_whole_projection(tmp_path):
    # Pass 2 has nothing to decode.
    path = _write(tmp_path / "t.orc", compression="zstd")
    session = opteryx.session()
    got = sorted(
        row
        for morsel in session.execute_to_morsels(f"SELECT id, few FROM READ_ORC('{path}') WHERE few LIKE 'category 1%' AND id > 100")
        for row in zip(morsel.column("id").to_pylist(), morsel.column("few").to_pylist())
    )
    assert got == [(r["id"], r["few"]) for r in ROWS_BY_ID if r["few"] is not None and r["few"].startswith("category 1") and r["id"] > 100]
    assert [op["orc_pass2_rows"] for op in session.telemetry["operations"].values() if "orc_pass2_rows" in op] == [0]


def test_where_on_a_column_that_is_not_selected(tmp_path):
    path = _write(tmp_path / "t.orc", compression="zstd")
    session = opteryx.session()
    got = sorted(
        row
        for morsel in session.execute_to_morsels(f"SELECT unique FROM READ_ORC('{path}') WHERE i16 = 1234")
        for row in morsel.column("unique").to_pylist()
    )
    assert got == sorted(r["unique"] for r in ROWS_BY_ID if r["i16"] == 1234)


def test_a_data_error_in_the_where_is_raised_as_the_filter_raises_it(tmp_path):
    table = pa.table({"s": pa.array(["12", "not a number"]), "n": pa.array([1, 2], pa.int64())})
    path = tmp_path / "t.orc"
    with open(path, "wb") as f:
        po.write_table(table, f)
    with pytest.raises(DataError):
        list(opteryx.session().execute_to_morsels(f"SELECT n FROM READ_ORC('{path}') WHERE CAST(s AS INT64) > 0"))


def _query_rows(sql):
    session = opteryx.session()
    got = sorted(
        row
        for morsel in session.execute_to_morsels(sql)
        for row in zip(*(morsel.column(c).to_pylist() for c in morsel.column_names))
    )
    return got, session


def _pruned(sql):
    got, session = _query_rows(sql)
    ops = [op for op in session.telemetry["operations"].values() if "orc_stripes_pruned" in op]
    assert len(ops) == 1, "no prune term was armed"
    return got, ops[0]["orc_stripes_pruned"], ops[0]["orc_row_groups_pruned"], ops[0]["orc_pass2_rows"]


@pytest.mark.parametrize("compression", ["zstd", "uncompressed"])
def test_statistics_skip_stripes_and_row_groups_that_cannot_match(tmp_path, compression):
    # `id` ascends, so its stripe and row group ranges are disjoint (§6.7).
    path = _write(tmp_path / "t.orc", compression=compression, stripe_size=64 * 1024, row_index_stride=10_000)
    stripes = po.ORCFile(str(path)).nstripes
    assert stripes > 2
    source = f"READ_ORC('{path}')"

    got, stripes_pruned, groups_pruned, pass2 = _pruned(f"SELECT id, unique FROM {source} WHERE id BETWEEN 70000 AND 70100")
    assert got == [(r["id"], r["unique"]) for r in ROWS_BY_ID if 70000 <= r["id"] <= 70100]
    assert stripes_pruned >= stripes - 2
    assert pass2 <= 2 * 10_000

    got, stripes_pruned, groups_pruned, pass2 = _pruned(f"SELECT id, few FROM {source} WHERE id = -5")
    assert got == []
    assert stripes_pruned == stripes and pass2 == 0

    # The optimizer rewrites a short IN list into a form the statistics are not tested by: the
    # same rows, nothing armed.
    got, _ = _query_rows(f"SELECT id FROM {source} WHERE id IN (5, 140000) AND i16 > -20000")
    assert got == [(r["id"],) for r in ROWS_BY_ID if r["id"] in (5, 140000) and r["i16"] is not None]

    # Row groups inside a stripe that is read: one stripe holds every row here (zstd: pyarrow
    # writes 1,024-row stripes whenever uncompressed).
    one = _write(tmp_path / "one.orc", compression="zstd", stripe_size=512 * 1024 * 1024, row_index_stride=10_000)
    got, stripes_pruned, groups_pruned, _ = _pruned(f"SELECT id, real FROM READ_ORC('{one}') WHERE id >= 145000")
    assert got == [(r["id"], r["real"]) for r in ROWS_BY_ID if r["id"] >= 145000]
    assert stripes_pruned == 0 and groups_pruned == 14

    # A date column, and a predicate the statistics cannot decide (no pruning, the same rows).
    got, _, _, _ = _pruned(f"SELECT id FROM {source} WHERE day = CAST('2021-03-04' AS DATE) AND id < 100000")
    want_day = datetime.date(2021, 3, 4)
    assert got == [(r["id"],) for r in ROWS_BY_ID if r["day"] == want_day and r["id"] < 100000]
    got, stripes_pruned, groups_pruned, _ = _pruned(f"SELECT id FROM {source} WHERE i16 <> 7 AND id > 149990")
    assert got == [(r["id"],) for r in ROWS_BY_ID if r["i16"] is not None and r["i16"] != 7 and r["id"] > 149990]
