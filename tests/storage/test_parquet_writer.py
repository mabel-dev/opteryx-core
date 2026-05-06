# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Tests for parquet_writer module.
"""

import os
import re
import struct

import pytest

import pyarrow as pa
from draken.morsels.morsel import Morsel
from opteryx.connectors.parquet_io.parquet_writer import write_morsel


@pytest.fixture
def tmp_relation_dir(tmp_path):
    """Create a temporary relation directory."""
    relation_dir = tmp_path / "test_relation"
    relation_dir.mkdir()
    return str(relation_dir)


def test_write_morsel_creates_file(tmp_relation_dir):
    """Write a morsel, assert file exists at returned path with correct name format."""
    table = pa.table({"col1": [1, 2, 3], "col2": ["a", "b", "c"]})
    morsel = Morsel.from_arrow(table)

    entry = write_morsel(morsel, tmp_relation_dir)

    full_path = os.path.join(tmp_relation_dir, entry.file_path)
    assert os.path.isfile(full_path)

    assert re.match(r"^data-[0-9a-f]{32}\.parquet$", entry.file_path)


def test_write_morsel_empty_raises(tmp_relation_dir):
    """Zero-row morsel raises ValueError."""
    table = pa.table({"col1": pa.array([], type=pa.int64())})
    morsel = Morsel.from_arrow(table)

    with pytest.raises(ValueError, match="cannot write empty morsel"):
        write_morsel(morsel, tmp_relation_dir)


def test_file_entry_record_count_matches(tmp_relation_dir):
    """Write 100 rows, assert record_count matches."""
    data = {
        "col1": list(range(100)),
        "col2": ["row_" + str(i) for i in range(100)],
    }
    table = pa.table(data)
    morsel = Morsel.from_arrow(table)

    entry = write_morsel(morsel, tmp_relation_dir)

    assert entry.record_count == 100


def test_file_entry_size_matches_disk(tmp_relation_dir):
    """Assert FileEntry.file_size_in_bytes matches actual disk size."""
    table = pa.table({"col1": [1, 2, 3, 4, 5]})
    morsel = Morsel.from_arrow(table)

    entry = write_morsel(morsel, tmp_relation_dir)

    full_path = os.path.join(tmp_relation_dir, entry.file_path)
    actual_size = os.path.getsize(full_path)

    assert entry.file_size_in_bytes == actual_size


def test_round_trip_via_rugo(tmp_relation_dir):
    """Write morsel, read back via rugo, assert values match.

    NOTE: Float columns are temporarily excluded due to Draken bug where
    Float64Vector.to_arrow() fails with "Missing values buffer" error when
    the vector is decoded from Parquet via rugo. This is tracked as a
    Draken/rugo issue, not a writer issue.
    """
    from rugo.parquet_reader import read_parquet

    original_data = {
        "int_col": [1, 2, 3, 4, 5],
        "str_col": ["a", "b", "c", "d", "e"],
        "bool_col": [True, False, True, False, True],
    }
    table = pa.table(original_data)
    original_morsel = Morsel.from_arrow(table)

    entry = write_morsel(original_morsel, tmp_relation_dir)

    full_path = os.path.join(tmp_relation_dir, entry.file_path)
    with open(full_path, "rb") as f:
        file_bytes = f.read()

    morsels = read_parquet(file_bytes)
    combined = Morsel.combine(morsels)

    assert len(combined) == len(original_morsel)
    assert combined.column_names == original_morsel.column_names

    original_arrow = original_morsel.to_arrow().to_pydict()
    read_arrow = combined.to_arrow().to_pydict()

    for col_name in original_data:
        assert read_arrow[col_name] == original_arrow[col_name]


def test_bounds_for_int_column(tmp_relation_dir):
    """Write int64 column, read bounds, decode and verify."""
    table = pa.table({"int_col": [5, 1, 9, 3]})
    morsel = Morsel.from_arrow(table)

    entry = write_morsel(morsel, tmp_relation_dir)

    assert entry.lower_bounds is not None
    assert entry.upper_bounds is not None

    assert 0 in entry.lower_bounds
    assert 0 in entry.upper_bounds

    lower = int.from_bytes(entry.lower_bounds[0], "big", signed=True)
    upper = int.from_bytes(entry.upper_bounds[0], "big", signed=True)

    assert lower == 1
    assert upper == 9


def test_bounds_for_string_column(tmp_relation_dir):
    """Write string column, read bounds, verify min/max strings."""
    table = pa.table({"str_col": ["banana", "apple", "cherry"]})
    morsel = Morsel.from_arrow(table)

    entry = write_morsel(morsel, tmp_relation_dir)

    assert entry.lower_bounds is not None
    assert entry.upper_bounds is not None

    lower = entry.lower_bounds[0].decode("utf-8")
    upper = entry.upper_bounds[0].decode("utf-8")

    assert lower == "apple"
    assert upper == "cherry"


def test_bounds_skip_unsupported_types(tmp_relation_dir):
    """Morsel with list column skips bounds for that column."""
    table = pa.table({
        "list_col": pa.array([[1, 2], [3, 4]], type=pa.list_(pa.int64())),
    })
    morsel = Morsel.from_arrow(table)

    entry = write_morsel(morsel, tmp_relation_dir)

    assert entry.lower_bounds is None or 0 not in entry.lower_bounds
    assert entry.upper_bounds is None or 0 not in entry.upper_bounds


def test_atomic_write_no_tmp_left(tmp_relation_dir):
    """After successful write, no .tmp file remains."""
    table = pa.table({"col1": [1, 2, 3]})
    morsel = Morsel.from_arrow(table)

    entry = write_morsel(morsel, tmp_relation_dir)

    tmp_path = os.path.join(tmp_relation_dir, entry.file_path + ".tmp")
    assert not os.path.exists(tmp_path)


def test_two_writes_distinct_files(tmp_relation_dir):
    """Two writes produce different filenames and both exist."""
    table1 = pa.table({"col1": [1, 2, 3]})
    table2 = pa.table({"col1": [4, 5, 6]})

    morsel1 = Morsel.from_arrow(table1)
    morsel2 = Morsel.from_arrow(table2)

    entry1 = write_morsel(morsel1, tmp_relation_dir)
    entry2 = write_morsel(morsel2, tmp_relation_dir)

    assert entry1.file_path != entry2.file_path

    path1 = os.path.join(tmp_relation_dir, entry1.file_path)
    path2 = os.path.join(tmp_relation_dir, entry2.file_path)

    assert os.path.isfile(path1)
    assert os.path.isfile(path2)
