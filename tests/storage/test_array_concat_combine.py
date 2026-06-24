# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Cross-file combine of an ARRAY (list) column.

When a dataset is split across several SMALL files, opteryx combines the
per-file morsels (`query_session._flush_buffer` -> `Morsel.combine` ->
draken `vector_concat`) before handing them downstream. `vector_concat`
previously had no ARRAY path, so any list column raised
`concat: unsupported type` on the combine — independent of schema evolution
(it failed even when every file carried the column).

These tests scan multiple small files at a morsel size large enough to force
the combine, and assert the array values survive intact across the boundary.

pyarrow is used here ONLY to author the test fixtures; the read+combine path
under test is opteryx's native scan + draken concat.
"""

import os
import shutil
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import pyarrow as pa
import pyarrow.parquet as pq

import opteryx


def _write(tmp_dir, tables):
    shutil.rmtree(tmp_dir, ignore_errors=True)
    os.makedirs(tmp_dir, exist_ok=True)
    for i, t in enumerate(tables):
        pq.write_table(t, os.path.join(tmp_dir, f"part_{i}.parquet"))


def _scan(sql, max_size=65_536):
    rows = []
    for m in opteryx.session().execute_to_morsels(sql, max_size=max_size):
        for i in range(len(m)):
            rows.append(m[i])
    return rows


def test_array_string_column_combined_across_files():
    """Two small files, both carrying a list<string> column. The combine must
    concatenate the arrays (offsets + child) rather than raise."""
    tmp = "_ac_str"
    _write(tmp, [
        pa.table({"id": [1, 2, 3], "tags": [["a", "b"], [], ["c"]]}),
        pa.table({"id": [4, 5], "tags": [["d", "e", "f"], None]}),
    ])
    try:
        rows = {r[0]: r[1] for r in _scan("SELECT id, tags FROM _ac_str")}
        assert rows[1] == ["a", "b"], rows
        assert rows[2] == [], rows
        assert rows[3] == ["c"], rows
        assert rows[4] == ["d", "e", "f"], rows
        assert rows[5] is None, rows
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


def test_array_int_column_combined_across_files():
    """Numeric child elements survive the combine."""
    tmp = "_ac_int"
    _write(tmp, [
        pa.table({"id": [1, 2], "nums": [[1, 2, 3], []]}),
        pa.table({"id": [3, 4], "nums": [[4], [5, 6]]}),
    ])
    try:
        rows = {r[0]: r[1] for r in _scan("SELECT id, nums FROM _ac_int")}
        assert rows[1] == [1, 2, 3], rows
        assert rows[2] == [], rows
        assert rows[3] == [4], rows
        assert rows[4] == [5, 6], rows
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


def test_array_column_combined_with_null_typed_part():
    """A file whose array column is entirely NULL (no concrete child type can be
    inferred from its values) combined with a file that carries real elements:
    the concrete element type is adopted, the all-null file's rows stay NULL."""
    tmp = "_ac_nullpart"
    _write(tmp, [
        pa.table({"id": [1, 2], "tags": pa.array([None, None], type=pa.list_(pa.string()))}),
        pa.table({"id": [3, 4], "tags": [["x"], ["y", "z"]]}),
    ])
    try:
        rows = {r[0]: r[1] for r in _scan("SELECT id, tags FROM _ac_nullpart")}
        assert rows[1] is None, rows
        assert rows[2] is None, rows
        assert rows[3] == ["x"], rows
        assert rows[4] == ["y", "z"], rows
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


if __name__ == "__main__":  # pragma: no cover
    test_array_string_column_combined_across_files()
    test_array_int_column_combined_across_files()
    test_array_column_combined_with_null_typed_part()
    print("✅ okay")
