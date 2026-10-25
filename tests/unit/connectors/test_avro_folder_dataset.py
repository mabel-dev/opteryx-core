# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""A folder of .avro files is a dataset: `SELECT ... FROM '<folder>'`, no READ_AVRO.

The relation is the FIRST file's header schema and every file is read with it as the
reader schema, matched by name - READ_AVRO's rules (docs/AVRO_READER_DESIGN.md §19.3) -
served by the same native source. Every expected value is the value the test wrote.
"""

import datetime
import decimal
import os
import sys

import pytest

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

import fastavro

import opteryx
from opteryx.exceptions import DatasetReadError
from opteryx.exceptions import NotSupportedError
from opteryx.models.dataset_format import MixedFormatDatasetError

UTC = datetime.timezone.utc

SCHEMA = {
    "type": "record",
    "name": "r",
    "fields": [
        {"name": "id", "type": "long"},
        {"name": "name", "type": ["null", "string"]},
        {"name": "score", "type": ["null", "double"]},
        {"name": "ts", "type": ["null", {"type": "long", "logicalType": "timestamp-micros"}]},
        {"name": "price", "type": ["null", {"type": "bytes", "logicalType": "decimal", "precision": 9, "scale": 2}]},
    ],
}


def _record(i):
    return {
        "id": i,
        "name": f"n{i}",
        "score": i / 2,
        "ts": datetime.datetime(2026, 1, i, tzinfo=UTC),
        "price": decimal.Decimal(f"{i}.50"),
    }


def _write(path, schema, records, codec="deflate"):
    with open(path, "wb") as f:
        fastavro.writer(f, schema, records, codec=codec)
    return path


def _rows(sql):
    session = opteryx.session()
    rows = []
    for morsel in session.execute_to_morsels(sql):
        columns = [morsel.column(name).to_pylist() for name in morsel.column_names]
        rows.extend(zip(*columns))
    assert "NativeAvroScanSource" in set(session.telemetry["scan_sources"].values())
    return sorted(rows, key=repr)


def test_a_folder_of_avro_files_is_one_table(tmp_path):
    _write(tmp_path / "a.avro", SCHEMA, [_record(1), _record(2)])
    _write(tmp_path / "b.avro", SCHEMA, [_record(3)], codec="null")
    assert _rows(f"SELECT id, name, score, ts, price FROM '{tmp_path}'") == sorted(
        [(r["id"], r["name"], r["score"], r["ts"], r["price"]) for r in map(_record, (1, 2, 3))],
        key=repr,
    )
    assert _rows(f"SELECT COUNT(*) FROM '{tmp_path}'") == [(3,)]


def test_projection_filter_and_limit(tmp_path):
    _write(tmp_path / "a.avro", SCHEMA, [_record(i) for i in range(1, 6)])
    assert _rows(f"SELECT name FROM '{tmp_path}' WHERE id > 3") == [("n4",), ("n5",)]
    assert _rows(f"SELECT SUM(id) FROM '{tmp_path}' WHERE score >= 1.5") == [(12,)]
    assert len(_rows(f"SELECT id FROM '{tmp_path}' LIMIT 2")) == 2


def test_a_file_whose_schema_evolved_resolves_onto_the_first(tmp_path):
    _write(tmp_path / "a.avro", SCHEMA, [_record(1)])
    # b: id written as int, an extra field, fields reordered, no score / ts / price
    later = {
        "type": "record",
        "name": "r",
        "fields": [
            {"name": "extra", "type": "string"},
            {"name": "name", "type": ["null", "string"]},
            {"name": "id", "type": "int"},
        ],
    }
    _write(tmp_path / "b.avro", later, [{"extra": "x", "name": "two", "id": 2}])
    assert _rows(f"SELECT id, name, score FROM '{tmp_path}'") == [(1, "n1", 0.5), (2, "two", None)]


def test_non_avro_files_in_the_folder_are_not_data(tmp_path):
    _write(tmp_path / "a.avro", SCHEMA, [_record(1)])
    (tmp_path / "README.md").write_text("not data")
    assert _rows(f"SELECT COUNT(*) FROM '{tmp_path}'") == [(1,)]


def test_avro_and_another_format_in_one_folder_is_refused(tmp_path):
    _write(tmp_path / "a.avro", SCHEMA, [_record(1)])
    (tmp_path / "b.jsonl").write_text('{"id": 2}\n')
    with pytest.raises(MixedFormatDatasetError):
        _rows(f"SELECT id FROM '{tmp_path}'")


def test_a_file_that_cannot_resolve_fails_the_query(tmp_path):
    _write(tmp_path / "a.avro", SCHEMA, [_record(1)])
    wrong = {"type": "record", "name": "r", "fields": [{"name": "id", "type": "string"}]}
    _write(tmp_path / "b.avro", wrong, [{"id": "x"}])
    with pytest.raises(DatasetReadError):
        _rows(f"SELECT id FROM '{tmp_path}'")


def test_a_corrupt_first_file_names_itself_at_bind_time(tmp_path):
    (tmp_path / "a.avro").write_bytes(b"not an avro file")
    with pytest.raises(Exception, match=r"a\.avro.*could not be read"):
        _rows(f"SELECT id FROM '{tmp_path}'")


if __name__ == "__main__":  # pragma: no cover
    pytest.main([__file__, "-v"])
