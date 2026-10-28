# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""A stripe is emitted as morsels of at most 65,536 rows (docs/ORC_READER_DESIGN.md §6.3).

The native ORC scan decodes a stripe a row range at a time, each column continuing where the last
range ended. A single-stripe file of 200,003 rows is read through READ_ORC and checked, row by row,
against what pyarrow.orc wrote (test-only, CLAUDE.md §4): every primitive kind, nulls placed so a
range's non-null count is not a multiple of 8 (BOOLEAN and PRESENT bits carry across ranges), direct
and dictionary strings (a range's dictionary is the entries it uses), RLE v1 and v2, and nested
columns (a LIST of a scalar sliced per range, a STRUCT rendered per range)."""

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

UTC = datetime.timezone.utc
ROWS = 200_003
MORSEL_ROWS = 65_536


def _table():
    ids = range(ROWS)
    return pa.table(
        {
            "id": pa.array(ids, pa.int64()),
            "i16": pa.array([None if i % 7 == 0 else (i % 30_000) - 15_000 for i in ids], pa.int16()),
            "i32": pa.array([None if i % 11 == 0 else i * 3 - 100_000 for i in ids], pa.int32()),
            "byte": pa.array([None if i % 13 == 0 else (i % 200) - 100 for i in ids], pa.int8()),
            "flag": pa.array([None if i % 5 == 0 else i % 3 == 0 for i in ids], pa.bool_()),
            "real": pa.array([None if i % 9 == 0 else i / 4 for i in ids], pa.float64()),
            "small": pa.array([None if i % 17 == 0 else float(i % 1000) / 8 for i in ids], pa.float32()),
            "unique": pa.array([None if i % 3 == 0 else f"value-{i}-{'x' * (i % 20)}" for i in ids], pa.string()),
            "few": pa.array([None if i % 19 == 0 else f"category {i % 40}" for i in ids], pa.string()),
            "raw": pa.array([None if i % 23 == 0 else bytes([i % 256]) * (i % 5) for i in ids], pa.binary()),
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


def _expected(table):
    out = {}
    for row in table.to_pylist():
        if row["ts"] is not None:
            row["ts"] = row["ts"].replace(tzinfo=UTC)
        out[row["id"]] = row
    return out


@pytest.mark.parametrize("version", ["0.11", "0.12"])
@pytest.mark.parametrize("dictionary", [0.0, 1.0])
def test_a_large_stripe_is_split_into_bounded_morsels_with_the_same_answer(tmp_path, version, dictionary):
    table = _table()
    path = tmp_path / "big.orc"
    with open(path, "wb") as f:
        po.write_table(
            table,
            f,
            file_version=version,
            dictionary_key_size_threshold=dictionary,
            compression="zstd",
            stripe_size=512 * 1024 * 1024,
        )
    assert po.ORCFile(str(path)).nstripes == 1

    names = table.column_names
    session = opteryx.session()
    sizes = []
    got = {}
    for morsel in session.execute_to_morsels(f"SELECT {', '.join(names)} FROM READ_ORC('{path}')"):
        columns = [morsel.column(n).to_pylist() for n in names]
        sizes.append(len(columns[0]))
        for values in zip(*columns):
            row = dict(zip(names, values))
            if row["point"] is not None:
                row["point"] = json.loads(row["point"])
            got[row["id"]] = row
    assert "NativeOrcScanSource" in set(session.telemetry["scan_sources"].values())

    assert max(sizes) <= MORSEL_ROWS
    assert len(sizes) >= (ROWS + MORSEL_ROWS - 1) // MORSEL_ROWS
    assert sum(sizes) == ROWS

    want = _expected(table)
    assert got.keys() == want.keys()
    for i in range(ROWS):
        assert got[i] == want[i], i
