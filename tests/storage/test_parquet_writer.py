# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Tests for opteryx.connectors.parquet_io.parquet_writer.open_data_file_writer -
the local store's streaming data file writer.

Streams row groups through the native (zero-pyarrow) rugo writer into one
file. PyArrow is used here ONLY as the read-side oracle (tests may use pyarrow).

Bounds: INT64 and FLOAT64 columns carry min/max merged across row groups.
The whole-morsel writer this replaced took bounds from the parquet statistics
of single-row-group files, which also covered BOOL and UTF8 strings; the
native vector min/max kernels do not answer for those, so on this connector
those columns carry no bounds now. Pinned below so the loss is visible, not
accidental.
"""

import os
import re

import pytest

import opteryx
from draken.morsels.morsel import Morsel
from opteryx.connectors.parquet_io.parquet_writer import open_data_file_writer


def _morsel(sql: str) -> Morsel:
    return list(opteryx.session().execute_to_morsels(sql))[0]


def _write(relation_dir, *morsels):
    writer = open_data_file_writer(relation_dir)
    for morsel in morsels:
        writer.write_row_group(morsel)
    return writer.close()


@pytest.fixture
def tmp_relation_dir(tmp_path):
    relation_dir = tmp_path / "test_relation"
    relation_dir.mkdir()
    return str(relation_dir)


def test_writer_creates_file(tmp_relation_dir):
    """Write a row group, assert file exists with the expected name format."""
    morsel = _morsel("SELECT i, s FROM (VALUES (1,'a'),(2,'b'),(3,'c')) AS t(i,s)")
    entry = _write(tmp_relation_dir, morsel)

    full_path = os.path.join(tmp_relation_dir, entry.file_path)
    assert os.path.isfile(full_path)
    # {time_ns:x}-{mac:x}-{pid:x} — the platform's one collision-proof id shape
    assert re.match(r"^data-[0-9a-f]+-[0-9a-f]+-[0-9a-f]+\.parquet$", entry.file_path)
    assert entry.row_group_count == 1


def test_empty_row_group_raises_and_empty_file_cannot_close(tmp_relation_dir):
    empty = Morsel.combine([])
    writer = open_data_file_writer(tmp_relation_dir)
    with pytest.raises(ValueError, match="empty row group"):
        writer.write_row_group(empty)
    with pytest.raises(ValueError, match="no row groups"):
        writer.close()
    writer.abort()
    assert os.listdir(tmp_relation_dir) == []


def test_file_entry_record_count_sums_row_groups(tmp_relation_dir):
    first = _morsel(
        "SELECT i FROM (VALUES " + ",".join("(%d)" % n for n in range(100)) + ") AS t(i)"
    )
    second = _morsel("SELECT i FROM (VALUES (1),(2),(3)) AS t(i)")
    entry = _write(tmp_relation_dir, first, second)
    assert entry.record_count == 103
    assert entry.row_group_count == 2
    assert entry.uncompressed_size_in_bytes == first.nbytes + second.nbytes


def test_file_entry_size_matches_disk(tmp_relation_dir):
    morsel = _morsel("SELECT i FROM (VALUES (1),(2),(3),(4),(5)) AS t(i)")
    entry = _write(tmp_relation_dir, morsel)
    full_path = os.path.join(tmp_relation_dir, entry.file_path)
    assert entry.file_size_in_bytes == os.path.getsize(full_path)


def test_round_trip_via_pyarrow_across_row_groups(tmp_relation_dir):
    """Two row groups in, ONE file out; PyArrow reads both back in order."""
    import pyarrow.parquet as pq

    first = _morsel(
        "SELECT i, s, b FROM (VALUES (1,'a',true),(2,'b',false),(3,'c',true)) AS t(i,s,b)"
    )
    second = _morsel("SELECT i, s, b FROM (VALUES (4,'d',false)) AS t(i,s,b)")
    entry = _write(tmp_relation_dir, first, second)
    full_path = os.path.join(tmp_relation_dir, entry.file_path)

    assert pq.ParquetFile(full_path).metadata.num_row_groups == 2
    t = pq.read_table(full_path)
    assert t.column("i").to_pylist() == [1, 2, 3, 4]
    assert t.column("s").to_pylist() == ["a", "b", "c", "d"]
    assert t.column("b").to_pylist() == [True, False, True, False]


def test_bounds_for_int_column_merge_across_row_groups(tmp_relation_dir):
    first = _morsel("SELECT i FROM (VALUES (5),(1),(9),(3)) AS t(i)")
    second = _morsel("SELECT i FROM (VALUES (-2),(7)) AS t(i)")
    entry = _write(tmp_relation_dir, first, second)

    assert entry.lower_bounds is not None and 0 in entry.lower_bounds
    assert entry.upper_bounds is not None and 0 in entry.upper_bounds
    assert int.from_bytes(entry.lower_bounds[0], "big", signed=True) == -2
    assert int.from_bytes(entry.upper_bounds[0], "big", signed=True) == 9


def test_bounds_for_float_column(tmp_relation_dir):
    import struct

    morsel = _morsel("SELECT f FROM (VALUES (1.5),(-0.5),(2.25)) AS t(f)")
    entry = _write(tmp_relation_dir, morsel)
    assert struct.unpack(">d", entry.lower_bounds[0])[0] == -0.5
    assert struct.unpack(">d", entry.upper_bounds[0])[0] == 2.25


def test_bounds_omit_all_null_and_non_numeric_columns(tmp_relation_dir):
    """DECIMAL/DATE bounds are not in the FileEntry bound set; nor, on this
    connector, are string and boolean bounds (see the module docstring). An
    all-null numeric column has nothing to bound. The int column still does."""
    morsel = _morsel(
        "SELECT i, CAST(v AS DECIMAL(10,2)) AS dec, s, b, CAST(NULL AS INT64) AS n "
        "FROM (VALUES (5, 1.5, 'banana', true),(1, 2.5, 'apple', false)) AS t(i, v, s, b)"
    )
    entry = _write(tmp_relation_dir, morsel)
    assert 0 in entry.lower_bounds            # int column has bounds
    assert 1 not in entry.lower_bounds        # decimal omitted
    assert 2 not in entry.lower_bounds        # string: no native min/max kernel
    assert 3 not in entry.lower_bounds        # bool: no native min/max kernel
    assert 4 not in entry.lower_bounds        # all-null


def test_atomic_write_no_tmp_left(tmp_relation_dir):
    morsel = _morsel("SELECT i FROM (VALUES (1),(2),(3)) AS t(i)")
    entry = _write(tmp_relation_dir, morsel)
    tmp_path = os.path.join(tmp_relation_dir, entry.file_path + ".tmp")
    assert not os.path.exists(tmp_path)


def test_abort_leaves_nothing_behind(tmp_relation_dir):
    morsel = _morsel("SELECT i FROM (VALUES (1),(2),(3)) AS t(i)")
    writer = open_data_file_writer(tmp_relation_dir)
    writer.write_row_group(morsel)
    writer.abort()
    assert os.listdir(tmp_relation_dir) == []
    with pytest.raises(ValueError, match="finished writer"):
        writer.write_row_group(morsel)


def test_two_writers_distinct_files(tmp_relation_dir):
    m1 = _morsel("SELECT i FROM (VALUES (1),(2),(3)) AS t(i)")
    m2 = _morsel("SELECT i FROM (VALUES (4),(5),(6)) AS t(i)")
    e1 = _write(tmp_relation_dir, m1)
    e2 = _write(tmp_relation_dir, m2)
    assert e1.file_path != e2.file_path
    assert os.path.isfile(os.path.join(tmp_relation_dir, e1.file_path))
    assert os.path.isfile(os.path.join(tmp_relation_dir, e2.file_path))
