# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
The SUM statistic (docs/MANIFEST_SUM_STATISTIC_DESIGN.md, P1): every producer
records the EXACT integer sum of a column's non-null values, and the manifest
carries it.

  - rugo writes a `rugo.sum` column-chunk key_value_metadata entry for every
    integer chunk (none for float / temporal / decimal), readable by any
    parquet reader;
  - rugo's footer read rolls the chunk sums into a file sum (AggColumnStat),
    and trusts them only in a file rugo wrote;
  - the opteryx writer (FileStats), ANALYZE and the parquet footer each put a
    sum in the manifest cell, and the manifest's sums_hi / sums_lo columns
    round-trip it exactly - past INT64, and for UINT64 values past INT64_MAX;
  - a statistic nobody recorded is None, never 0.

Run as a script (CLAUDE.md §10) or under pytest.
"""

import io
import os
import shutil
import sys
from pathlib import Path

sys.path.insert(1, os.path.join(os.path.dirname(__file__), "..", ".."))

import pyarrow.parquet as pq
import pytest

import opteryx
from opteryx.compiled.planner.native_manifest import (
    FileStats,
    NativeManifestBuilder,
    decode_manifest_parquet,
)
from opteryx.connectors.parquet_io.pool_reader import fetch_column_stats_many
from opteryx.models.manifest_io import DATASET_MANIFEST_NAME
from opteryx.types.logical_type import DrakenType as DT
from rugo.parquet import write_parquet

_SESSION = opteryx.session()

# i32: 1..n; u64: UINT64 values past INT64_MAX with every third row NULL;
# neg: negative INT8s; f: a float (no sum); d: a DATE (no sum).
_SQL = (
    "SELECT CAST(g AS INT32) AS i32, "
    "CASE WHEN g % 3 = 0 THEN NULL "
    "ELSE CAST(g AS UINT64) + CAST(18446744073709000000 AS UINT64) END AS u64, "
    "CAST(g % 100 AS INT8) - 50 AS neg, "
    "CAST(g AS FLOAT64) AS f, "
    "CAST(g AS DATE) AS d "
    "FROM generate_series(1, {n}) AS g"
)
_COLUMNS = ("i32", "u64", "neg", "f", "d")
_PHYSICAL = (DT.INT32, DT.UINT64, DT.INT8, DT.FLOAT64, DT.DATE32)
_SUMMED = (0, 1, 2)   # positions of the integer columns


def _morsels(n):
    return list(_SESSION.execute_to_morsels(_SQL.format(n=n)))


def _expected_sums(morsels):
    """The exact sums, computed in Python from the values the engine produced;
    None for a column that must carry no sum."""
    totals = [0 if k in _SUMMED else None for k in range(len(_COLUMNS))]
    for morsel in morsels:
        for i in range(morsel.num_rows):
            row = morsel[i]
            for k in _SUMMED:
                if row[k] is not None:
                    totals[k] += row[k]
    return totals


def _cell_sums(manifest, row=0):
    return [manifest.cell(row, k)["sum"] for k in range(len(_COLUMNS))]


def test_rugo_writes_chunk_sums_any_reader_can_read():
    morsel = _morsels(600)[0]
    data = write_parquet(morsel, max_rows_per_row_group=250)
    metadata = pq.ParquetFile(io.BytesIO(data)).metadata
    assert metadata.num_row_groups > 1
    start = 1
    for rg in range(metadata.num_row_groups):
        group = metadata.row_group(rg)
        rows = range(start, start + group.num_rows)
        start += group.num_rows
        chunk = {group.column(c).path_in_schema: group.column(c).metadata for c in range(group.num_columns)}
        assert chunk["i32"] == {b"rugo.sum": str(sum(rows)).encode()}
        expected_u64 = sum(g + 18446744073709000000 for g in rows if g % 3)
        assert chunk["u64"] == {b"rugo.sum": str(expected_u64).encode()}
        assert chunk["neg"] == {b"rugo.sum": str(sum((g % 100) - 50 for g in rows)).encode()}
        # float and temporal chunks carry no sum
        assert not chunk["f"] and not chunk["d"]


def test_writer_file_stats_record_exact_sums_and_round_trip():
    morsels = _morsels(150_000)
    stats = FileStats()
    rows = 0
    for morsel in morsels:
        stats.add_row_group(morsel)
        rows += morsel.num_rows
    manifest = stats.file_row("f.parquet", "PARQUET", rows, 1, 1, 1)
    expected = _expected_sums(morsels)
    assert expected[1] > 2**63, "the UINT64 sum must exceed INT64 for this test to mean anything"
    assert _cell_sums(manifest) == expected

    decoded = decode_manifest_parquet(manifest.to_parquet(), _COLUMNS, _PHYSICAL, {}, True, True)
    assert _cell_sums(decoded) == expected


def test_parquet_footer_sums_reach_the_manifest(tmp_path):
    morsels = _morsels(600)
    path = str(tmp_path / "sums.parquet")
    with open(path, "wb") as handle:
        handle.write(write_parquet(morsels[0], max_rows_per_row_group=250))
    ((rows, _groups, footer),) = fetch_column_stats_many(None, [path])
    builder = NativeManifestBuilder(_COLUMNS, _PHYSICAL, True, True)
    row = builder.add_file(path, "PARQUET", rows, 1)
    builder.set_footer(row, footer)
    manifest = builder.build({})
    assert [manifest.cell(0, k)["footer"]["sum"] for k in range(len(_COLUMNS))] == _expected_sums(morsels)


def test_a_foreign_writers_rugo_sum_is_not_trusted(tmp_path):
    # Same bytes, but created_by no longer names rugo: the chunk keys are still
    # there, and must be ignored - a sum is a claim only rugo's own files make.
    data = write_parquet(_morsels(600)[0], max_rows_per_row_group=250)
    marker = b"opteryx-rugo version"
    assert data.count(marker) == 1
    forged = data.replace(marker, b"opteryx-xxxx version")
    path = str(tmp_path / "forged.parquet")
    with open(path, "wb") as handle:
        handle.write(forged)
    ((rows, _groups, footer),) = fetch_column_stats_many(None, [path])
    builder = NativeManifestBuilder(_COLUMNS, _PHYSICAL, True, True)
    row = builder.add_file(path, "PARQUET", rows, 1)
    builder.set_footer(row, footer)
    manifest = builder.build({})
    assert [manifest.cell(0, k)["footer"]["sum"] for k in range(len(_COLUMNS))] == [None] * len(_COLUMNS)


def test_analyze_records_sums_and_clearing_forgets_them():
    ds_dir = Path("testdata") / "sum_statistic"
    if ds_dir.exists():
        shutil.rmtree(ds_dir)
    ds_dir.mkdir(parents=True)
    try:
        batches = [_morsels(1_000)[0], _morsels(3_000)[0]]
        for i, morsel in enumerate(batches):
            with open(ds_dir / f"part-{i}.parquet", "wb") as handle:
                handle.write(write_parquet(morsel))
        list(_SESSION.execute_to_morsels("ANALYZE TABLE testdata.sum_statistic"))
        with open(ds_dir / DATASET_MANIFEST_NAME, "rb") as handle:
            manifest = decode_manifest_parquet(handle.read(), _COLUMNS, _PHYSICAL, {}, True, True)
        by_path = {manifest.file_row(r)["file_path"]: r for r in range(2)}
        for i, morsel in enumerate(batches):
            row = by_path[str(ds_dir / f"part-{i}.parquet")]
            assert _cell_sums(manifest, row) == _expected_sums([morsel])

        # DROP STATISTICS for one column clears its sum and keeps the others'
        list(_SESSION.execute_to_morsels("DROP STATISTICS ON testdata.sum_statistic FOR COLUMNS i32"))
        with open(ds_dir / DATASET_MANIFEST_NAME, "rb") as handle:
            manifest = decode_manifest_parquet(handle.read(), _COLUMNS, _PHYSICAL, {}, True, True)
        for r in range(2):
            sums = _cell_sums(manifest, r)
            assert sums[0] is None
            assert sums[1] is not None and sums[2] is not None
    finally:
        shutil.rmtree(ds_dir)


def _local_store_sums(store_root, relation, physical):
    """Each current file's (column sums) from the local store's snapshot manifest."""
    import json

    relation_dir = os.path.join(store_root, *relation.split("."))
    with open(os.path.join(relation_dir, "dataset.json")) as handle:
        current = json.load(handle)["current_snapshot"]
    with open(os.path.join(relation_dir, current)) as handle:
        snapshot = json.load(handle)
    with open(os.path.join(relation_dir, snapshot["manifest_file"]), "rb") as handle:
        data = handle.read()
    names = ("v", "f")
    manifest = decode_manifest_parquet(data, names, physical, {}, snapshot.get("bounds_are_ordinal", False), True)
    return sorted(tuple(manifest.cell(r, k)["sum"] for k in range(2)) for r in range(len(manifest)))


def test_local_store_sums_survive_an_integer_widening(tmp_path):
    from opteryx.connectors import register_workspace
    from opteryx.connectors.local_store_connector import LocalStoreConnector

    register_workspace("sumws", LocalStoreConnector, store_root=str(tmp_path))
    session = opteryx.session()

    def run(sql):
        return [tuple(m[i]) for m in session.execute_to_morsels(sql) for i in range(m.num_rows)]

    run("CREATE TABLE sumws.t (v INT32, f FLOAT64)")
    run("INSERT INTO sumws.t VALUES (1, 1.5), (2, 2.5), (NULL, 3.5), (2000000000, 0.5)")
    run("INSERT INTO sumws.t VALUES (2000000000, 1.0), (-7, 2.0)")
    # one file per INSERT; the float column never carries a sum
    assert _local_store_sums(str(tmp_path), "sumws.t", (DT.INT32, DT.FLOAT64)) == [
        (1999999993, None),
        (2000000003, None),
    ]

    # INT32 -> INT64 keeps every value, so every carried sum stays true
    run("ALTER TABLE sumws.t ALTER COLUMN v TYPE INT64")
    run("INSERT INTO sumws.t VALUES (5000000000, 9.0)")
    sums = _local_store_sums(str(tmp_path), "sumws.t", (DT.INT64, DT.FLOAT64))
    assert sums == [(1999999993, None), (2000000003, None), (5000000000, None)]
    assert run("SELECT SUM(v) FROM sumws.t") == [(sum(s for s, _ in sums),)]


def test_a_manifest_without_sum_columns_decodes_with_sums_unknown():
    # A manifest from before sums existed: build one with no sum, drop nothing -
    # an untracked file writes EMPTY sums lists and must read back as unknown.
    builder = NativeManifestBuilder(_COLUMNS, _PHYSICAL, True, True)
    builder.add_file("old.parquet", "PARQUET", 10, 1)
    manifest = builder.build({})
    decoded = decode_manifest_parquet(manifest.to_parquet(), _COLUMNS, _PHYSICAL, {}, True, True)
    assert _cell_sums(decoded) == [None] * len(_COLUMNS)


def test_set_sum_rejects_values_outside_int128():
    builder = NativeManifestBuilder(_COLUMNS, _PHYSICAL, True, True)
    row = builder.add_file("x.parquet", "PARQUET", 1, 1)
    builder.set_sum(row, 0, -(2**127))
    builder.set_sum(row, 1, 2**127 - 1)
    with pytest.raises(ValueError):
        builder.set_sum(row, 2, 2**127)
    manifest = builder.build({})
    assert _cell_sums(manifest)[:2] == [-(2**127), 2**127 - 1]
    decoded = decode_manifest_parquet(manifest.to_parquet(), _COLUMNS, _PHYSICAL, {}, True, True)
    assert _cell_sums(decoded)[:2] == [-(2**127), 2**127 - 1]


if __name__ == "__main__":  # pragma: no cover
    sys.exit(pytest.main([__file__, "-v"]))
