# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""A folder of .orc files is a dataset: `SELECT ... FROM '<folder>'` (docs/ORC_READER_DESIGN.md).

The relation is the FIRST file's footer schema and every file is read as it, matched by
name, by the native source (NativeOrcScanSource). The files are written by pyarrow.orc,
an independent ORC writer (test-only; CLAUDE.md §4). Every expected value is the value
the test wrote.
"""

import datetime
import decimal
import io
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
from opteryx.exceptions import DatasetReadError
from opteryx.exceptions import NotSupportedError
from opteryx.models.dataset_format import MixedFormatDatasetError

UTC = datetime.timezone.utc


def _table(ids):
    return pa.table(
        {
            "id": pa.array(ids, pa.int64()),
            "name": pa.array([None if i % 4 == 0 else f"n{i}" for i in ids], pa.string()),
            "score": pa.array([i / 2 for i in ids], pa.float64()),
            "ts": pa.array([datetime.datetime(2026, 1, 1) + datetime.timedelta(hours=i) for i in ids], pa.timestamp("us")),
            "price": pa.array([decimal.Decimal(f"{i}.50") for i in ids], pa.decimal128(9, 2)),
            "d": pa.array([datetime.date(2026, 2, 1) + datetime.timedelta(days=i) for i in ids]),
            "flag": pa.array([i % 2 == 1 for i in ids], pa.bool_()),
        }
    )


def _write(path, table, **options):
    with open(path, "wb") as f:
        po.write_table(table, f, **options)
    return path


def _rows(sql):
    session = opteryx.session()
    rows = []
    for morsel in session.execute_to_morsels(sql):
        columns = [morsel.column(name).to_pylist() for name in morsel.column_names]
        rows.extend(zip(*columns))
    assert "NativeOrcScanSource" in set(session.telemetry["scan_sources"].values())
    return sorted(rows, key=repr)


def _expected(ids, columns):
    table = _table(ids).to_pydict()
    table["ts"] = [v.replace(tzinfo=UTC) for v in table["ts"]]
    return [tuple(table[c][k] for c in columns) for k in range(len(ids))]


def test_a_folder_of_orc_files_is_one_table(tmp_path):
    _write(tmp_path / "a.orc", _table([1, 2]), compression="zlib")
    _write(tmp_path / "b.orc", _table([3]), compression="zstd")
    cols = ["id", "name", "score", "ts", "price", "d", "flag"]
    assert _rows(f"SELECT {', '.join(cols)} FROM '{tmp_path}'") == sorted(_expected([1, 2, 3], cols), key=repr)
    assert _rows(f"SELECT COUNT(*) FROM '{tmp_path}'") == [(3,)]


def test_projection_filter_and_limit(tmp_path):
    _write(tmp_path / "a.orc", _table(range(1, 6)))
    assert _rows(f"SELECT name FROM '{tmp_path}' WHERE id > 3") == [("n5",), (None,)]
    assert _rows(f"SELECT SUM(id) FROM '{tmp_path}' WHERE score >= 1.5") == [(12,)]
    assert len(_rows(f"SELECT id FROM '{tmp_path}' LIMIT 2")) == 2


def test_many_stripes_in_many_files(tmp_path):
    ids = list(range(20000))
    for k in range(3):
        _write(tmp_path / f"f{k}.orc", _table([i + k * 100000 for i in ids]), stripe_size=64 * 1024, compression="snappy")
    assert _rows(f"SELECT COUNT(*), SUM(id) FROM '{tmp_path}'") == [(60000, 3 * sum(ids) + 100000 * 20000 * (0 + 1 + 2))]


def test_a_later_file_is_read_as_the_first_widening_and_adding_nulls(tmp_path):
    _write(tmp_path / "a.orc", _table([1]))
    narrow = pa.table({"id": pa.array([2], pa.int64()), "name": pa.array(["two"], pa.string())})
    _write(tmp_path / "b.orc", narrow)   # lacks score, ts, price, d, flag
    assert _rows(f"SELECT id, name, score FROM '{tmp_path}'") == [(1, "n1", 0.5), (2, "two", None)]


def test_a_narrower_integer_in_a_later_file_widens(tmp_path):
    _write(tmp_path / "a.orc", pa.table({"x": pa.array([1], pa.int64())}))
    _write(tmp_path / "b.orc", pa.table({"x": pa.array([2], pa.int32())}))
    assert _rows(f"SELECT x FROM '{tmp_path}'") == [(1,), (2,)]


def test_non_orc_files_in_the_folder_are_not_data(tmp_path):
    _write(tmp_path / "a.orc", _table([1]))
    (tmp_path / "README.md").write_text("not data")
    assert _rows(f"SELECT COUNT(*) FROM '{tmp_path}'") == [(1,)]


def test_orc_and_another_format_in_one_folder_is_refused(tmp_path):
    _write(tmp_path / "a.orc", _table([1]))
    (tmp_path / "b.jsonl").write_text('{"id": 2}\n')
    with pytest.raises(MixedFormatDatasetError):
        _rows(f"SELECT id FROM '{tmp_path}'")


def test_a_file_that_cannot_resolve_fails_the_query(tmp_path):
    _write(tmp_path / "a.orc", pa.table({"x": pa.array([1], pa.int64())}))
    _write(tmp_path / "b.orc", pa.table({"x": pa.array(["s"], pa.string())}))
    with pytest.raises(DatasetReadError):
        _rows(f"SELECT x FROM '{tmp_path}'")


def test_a_column_the_reader_cannot_surface_names_itself_at_bind_time(tmp_path):
    table = pa.table({"ok": pa.array([1], pa.int64()), "m": pa.array([[(1, 2)]], pa.map_(pa.int64(), pa.int64()))})
    _write(tmp_path / "a.orc", table)
    with pytest.raises(NotSupportedError, match=r"column 'm'.*MAP has LONG keys"):
        _rows(f"SELECT ok FROM '{tmp_path}'")


def test_a_corrupt_first_file_names_itself_at_bind_time(tmp_path):
    (tmp_path / "a.orc").write_bytes(b"not an orc file")
    with pytest.raises(Exception, match=r"a\.orc.*could not be read"):
        _rows(f"SELECT id FROM '{tmp_path}'")


if __name__ == "__main__":  # pragma: no cover
    pytest.main([__file__, "-v"])
