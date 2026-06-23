# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Tests for the rugo.parquet read/write facade and the E.28 reader reconstruction.

PyArrow is the read-side oracle only.
"""

import glob
import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import pytest

import opteryx
from rugo import parquet


def _planets_path() -> str:
    return glob.glob("**/planets/planets.parquet", recursive=True)[0]


def test_read_all_columns_match_pyarrow():
    """Every planets column decodes correctly (E.28 reconstruction)."""
    import pyarrow.parquet as pq

    path = _planets_path()
    truth = pq.read_table(path).to_pydict()
    meta = parquet.read_metadata(path)
    names = [c.name for c in meta.schema_columns]

    with parquet.read_parquet(path) as reader:
        morsels = list(reader)
    assert len(morsels) == 1
    m = morsels[0]
    for name in names:
        assert m.column(name.encode()).to_pylist() == truth[name], name


def test_read_from_bytes_and_path_agree():
    path = _planets_path()
    with parquet.read_parquet(path, columns=["name"]) as r:
        by_path = list(r)[0].column(b"name").to_pylist()
    with parquet.read_parquet(open(path, "rb").read(), columns=["name"]) as r:
        by_bytes = list(r)[0].column(b"name").to_pylist()
    assert by_path == by_bytes


def test_filter_keeps_matching_row_group():
    path = _planets_path()
    with parquet.read_parquet(path, columns=["name"], filters=[("id", ">", 4)]) as r:
        assert len(list(r)) == 1  # ids 1..9 — row group survives


def test_filter_prunes_row_group():
    path = _planets_path()
    with parquet.read_parquet(path, columns=["name"], filters=[("id", ">", 10_000)]) as r:
        assert list(r) == []  # pruned, nothing decoded
    with parquet.read_parquet(path, filters=[("name", "=", "Zzz")]) as r:
        assert list(r) == []  # string min/max prune


def test_write_then_read_roundtrip_with_nulls():
    """Facade write -> facade read, across types and interior nulls."""
    sql = """
    SELECT * FROM (VALUES
      (1, 1.5, true, 'alpha'),
      (-2, 2.25, false, 'beta'),
      (NULL, NULL, NULL, NULL),
      (7, 1e30, false, 'delta')
    ) AS t(i, d, b, s)
    """
    morsel = next(iter(opteryx.session().execute_to_morsels(sql)))
    for compression in ("zstd", "none"):
        data = parquet.write_parquet(morsel, compression=compression)
        with parquet.read_parquet(data) as reader:
            out = list(reader)[0]
        assert out.column(b"i").to_pylist() == [1, -2, None, 7]
        assert out.column(b"d").to_pylist() == [1.5, 2.25, None, 1e30]
        assert out.column(b"b").to_pylist() == [True, False, None, False]
        assert out.column(b"s").to_pylist() == ["alpha", "beta", None, "delta"]


def test_write_planets_roundtrip():
    """Narrow ints (planets.id is INT8) widen and round-trip."""
    morsel = next(
        iter(opteryx.session().execute_to_morsels("SELECT id, name FROM $planets"))
    )
    data = parquet.write_parquet(morsel)
    with parquet.read_parquet(data) as reader:
        out = list(reader)[0]
    assert out.column(b"id").to_pylist() == list(range(1, 10))
    assert out.column(b"name").to_pylist()[0] == "Mercury"


if __name__ == "__main__":
    test_read_all_columns_match_pyarrow()
    test_read_from_bytes_and_path_agree()
    test_filter_keeps_matching_row_group()
    test_filter_prunes_row_group()
    test_write_then_read_roundtrip_with_nulls()
    test_write_planets_roundtrip()
    print("✅ okay")
