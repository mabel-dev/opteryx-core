# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Tests for opteryx.connectors.parquet_io.parquet_writer.write_morsel.

write_morsel uses the native (zero-pyarrow) rugo writer. PyArrow is used here
ONLY as the read-side oracle (tests may use pyarrow).
"""

import os
import re

import pytest

import opteryx
from draken.morsels.morsel import Morsel
from opteryx.connectors.parquet_io.parquet_writer import write_morsel


def _morsel(sql: str) -> Morsel:
    return list(opteryx.session().execute_to_morsels(sql))[0]


@pytest.fixture
def tmp_relation_dir(tmp_path):
    relation_dir = tmp_path / "test_relation"
    relation_dir.mkdir()
    return str(relation_dir)


def test_write_morsel_creates_file(tmp_relation_dir):
    """Write a morsel, assert file exists with the expected name format."""
    morsel = _morsel("SELECT i, s FROM (VALUES (1,'a'),(2,'b'),(3,'c')) AS t(i,s)")
    entry = write_morsel(morsel, tmp_relation_dir)

    full_path = os.path.join(tmp_relation_dir, entry.file_path)
    assert os.path.isfile(full_path)
    assert re.match(r"^data-[0-9A-Za-z]{32}\.parquet$", entry.file_path)


def test_write_morsel_empty_raises(tmp_relation_dir):
    """Zero-row morsel raises ValueError."""
    empty = Morsel.combine([])
    with pytest.raises(ValueError, match="cannot write empty morsel"):
        write_morsel(empty, tmp_relation_dir)


def test_file_entry_record_count_matches(tmp_relation_dir):
    morsel = _morsel(
        "SELECT i FROM (VALUES " + ",".join("(%d)" % n for n in range(100)) + ") AS t(i)"
    )
    entry = write_morsel(morsel, tmp_relation_dir)
    assert entry.record_count == 100


def test_file_entry_size_matches_disk(tmp_relation_dir):
    morsel = _morsel("SELECT i FROM (VALUES (1),(2),(3),(4),(5)) AS t(i)")
    entry = write_morsel(morsel, tmp_relation_dir)
    full_path = os.path.join(tmp_relation_dir, entry.file_path)
    assert entry.file_size_in_bytes == os.path.getsize(full_path)


def test_round_trip_via_pyarrow(tmp_relation_dir):
    """Write morsel, read back with PyArrow, assert values match."""
    import pyarrow.parquet as pq

    morsel = _morsel(
        "SELECT i, s, b FROM (VALUES (1,'a',true),(2,'b',false),(3,'c',true)) AS t(i,s,b)"
    )
    entry = write_morsel(morsel, tmp_relation_dir)
    full_path = os.path.join(tmp_relation_dir, entry.file_path)

    t = pq.read_table(full_path)
    assert t.column("i").to_pylist() == [1, 2, 3]
    assert t.column("s").to_pylist() == ["a", "b", "c"]
    assert t.column("b").to_pylist() == [True, False, True]


def test_bounds_for_int_column(tmp_relation_dir):
    morsel = _morsel("SELECT i FROM (VALUES (5),(1),(9),(3)) AS t(i)")
    entry = write_morsel(morsel, tmp_relation_dir)

    assert entry.lower_bounds is not None and 0 in entry.lower_bounds
    assert entry.upper_bounds is not None and 0 in entry.upper_bounds
    assert int.from_bytes(entry.lower_bounds[0], "big", signed=True) == 1
    assert int.from_bytes(entry.upper_bounds[0], "big", signed=True) == 9


def test_bounds_for_string_column(tmp_relation_dir):
    morsel = _morsel("SELECT s FROM (VALUES ('banana'),('apple'),('cherry')) AS t(s)")
    entry = write_morsel(morsel, tmp_relation_dir)

    assert entry.lower_bounds[0].decode("utf-8") == "apple"
    assert entry.upper_bounds[0].decode("utf-8") == "cherry"


def test_bounds_omit_logical_typed_columns(tmp_relation_dir):
    """DECIMAL/DATE bounds are not in the FileEntry bound set (only plain
    int/float/bool/str carry bounds); the int column still does."""
    morsel = _morsel(
        "SELECT i, CAST(v AS DECIMAL(10,2)) AS dec "
        "FROM (VALUES (5, 1.5),(1, 2.5)) AS t(i, v)"
    )
    entry = write_morsel(morsel, tmp_relation_dir)
    assert 0 in entry.lower_bounds            # int column has bounds
    assert 1 not in entry.lower_bounds        # decimal omitted


def test_atomic_write_no_tmp_left(tmp_relation_dir):
    morsel = _morsel("SELECT i FROM (VALUES (1),(2),(3)) AS t(i)")
    entry = write_morsel(morsel, tmp_relation_dir)
    tmp_path = os.path.join(tmp_relation_dir, entry.file_path + ".tmp")
    assert not os.path.exists(tmp_path)


def test_two_writes_distinct_files(tmp_relation_dir):
    m1 = _morsel("SELECT i FROM (VALUES (1),(2),(3)) AS t(i)")
    m2 = _morsel("SELECT i FROM (VALUES (4),(5),(6)) AS t(i)")
    e1 = write_morsel(m1, tmp_relation_dir)
    e2 = write_morsel(m2, tmp_relation_dir)
    assert e1.file_path != e2.file_path
    assert os.path.isfile(os.path.join(tmp_relation_dir, e1.file_path))
    assert os.path.isfile(os.path.join(tmp_relation_dir, e2.file_path))
