# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Schema evolution on the NATIVE parquet scan path.

A dataset whose files do not all share one schema (a column ADDED over time, so
older files lack it) must scan without crashing: the union schema declares the
column, the files that predate it do not carry it. The native scan fills the
absent column with a typed all-NULL vector, keeping the positional name<->vector
pairing intact.

Before the fix, a projected column absent from a file desynced the
name<->vector pairing in the native scan: `SELECT <col>` over such a dataset
SIGSEGV'd (out-of-bounds column access in cxx_select), and even when it did not
crash a present column was silently mislabeled as the missing one.

pyarrow is used here ONLY to author the test fixtures (differing-schema files);
the read path under test is opteryx's native scan.
"""

import os
import shutil
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import opteryx


def _write(tmp_dir, tables):
    """Write each pyarrow table to its own small parquet file (small => the engine
    combines the per-file morsels, exercising the cross-file concat path)."""
    shutil.rmtree(tmp_dir, ignore_errors=True)
    os.makedirs(tmp_dir, exist_ok=True)
    for i, t in enumerate(tables):
        pq.write_table(t, os.path.join(tmp_dir, f"part_{i}.parquet"))


def _scan(dataset, sql, max_size=65_536):
    rows = []
    for m in opteryx.session().execute_to_morsels(sql, max_size=max_size):
        for i in range(len(m)):
            rows.append(m[i])
    return rows


def test_scalar_column_missing_from_some_files():
    """A scalar column present in one file, absent in another. The absent file's
    rows must read back as NULL — not crash, not mislabel a neighbour column."""
    tmp = "_se_scalar"
    _write(tmp, [
        pa.table({"id": [1, 2, 3], "x": [10, 20, 30]}),
        pa.table({"id": [4, 5, 6]}),  # no 'x'
    ])
    try:
        rows = sorted(_scan(tmp, "SELECT id, x FROM _se_scalar"))
        assert rows == [(1, 10), (2, 20), (3, 30), (4, None), (5, None), (6, None)], rows
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


def test_only_projected_column_missing_from_a_file():
    """The exact original crash shape: the ONLY projected column is absent from a
    file (so that file decodes zero columns) — the row count must still flow and
    the rows read back NULL."""
    tmp = "_se_only"
    _write(tmp, [
        pa.table({"id": [1, 2], "x": [10, 20]}),
        pa.table({"id": [3, 4, 5]}),  # no 'x'
    ])
    try:
        vals = [r[0] for r in _scan(tmp, "SELECT x FROM _se_only")]
        # 5 rows total: 2 real values + 3 nulls.
        assert len(vals) == 5, vals
        assert vals.count(None) == 3, vals
        assert sorted(v for v in vals if v is not None) == [10, 20], vals
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


def test_array_column_missing_from_some_files():
    """list<string> column (the reported `cves` shape) absent from a later file.
    The absent file's rows must read back NULL; the present file's array values
    must survive intact.

    `max_size=1` keeps each row in its own morsel so the assertion does not depend
    on cross-morsel array concatenation, which `vector_concat` does not support
    (a separate, pre-existing limitation independent of this missing-column fix —
    it fails identically for array columns with no missing column at all)."""
    tmp = "_se_array"
    _write(tmp, [
        pa.table({"id": [1, 2, 3], "tags": [["a", "b"], [], None]}),
        pa.table({"id": [4, 5]}),  # no 'tags'
    ])
    try:
        rows = {r[0]: r[1] for r in _scan(tmp, "SELECT id, tags FROM _se_array", max_size=1)}
        assert rows[1] == ["a", "b"], rows
        assert rows[2] == [], rows
        assert rows[3] is None, rows
        assert rows[4] is None, rows
        assert rows[5] is None, rows
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


def test_present_column_not_mislabeled_when_neighbour_missing():
    """When a column ahead of others is missing in a file, the columns that ARE
    present must keep their own values (no positional aliasing)."""
    tmp = "_se_alias"
    _write(tmp, [
        pa.table({"id": [1], "a": [100], "b": [200]}),
        pa.table({"id": [2], "b": [201]}),  # 'a' missing; 'b' must stay 'b'
    ])
    try:
        rows = {r[0]: (r[1], r[2]) for r in _scan(tmp, "SELECT id, a, b FROM _se_alias")}
        assert rows[1] == (100, 200), rows
        assert rows[2] == (None, 201), rows  # a=NULL, b keeps its real value
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


if __name__ == "__main__":  # pragma: no cover
    test_scalar_column_missing_from_some_files()
    test_only_projected_column_missing_from_a_file()
    test_array_column_missing_from_some_files()
    test_present_column_not_mislabeled_when_neighbour_missing()
    print("✅ okay")
