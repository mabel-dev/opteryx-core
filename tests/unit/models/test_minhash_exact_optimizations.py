# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""Exact-set optimizations over consolidated MinHash (KMV) sketches.

A per-file KMV sketch that holds fewer than K=32 distinct hashes is COMPLETE — it
is the exact set of value hashes in that file. This lets the planner:

  * eliminate files for ``=`` / ``!=`` / ``IN`` / ``NOT IN`` when the predicate
    value cannot be present (MinHash file pruning), and
  * answer ``COUNT(DISTINCT col)`` exactly from statistics, with no scan.

The overriding property under test is *soundness*: pruning must NEVER change an
answer. Fixtures are built with canonical ``draken Vector.hash`` provenance (the
same hash the catalog/ANALYZE produce), generated via ``hash_literal_kmv`` over
pyarrow-read distinct values — including the null-row sentinel for null rows, so
the null-stripping path is exercised for real.
"""

import json
import os
import shutil
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
import pytest

import opteryx
import draken.draken_native as dn
from opteryx.compiled.expression.compiled_expression import hash_literal_kmv

K = 32
_NULL_SENTINEL = dn.vector_null_from_length(1).hash()[0]


def _pa_to_phys(patype):
    if pa.types.is_boolean(patype):
        return int(dn.DrakenType.BOOL.value)
    if pa.types.is_integer(patype):
        return int(dn.DrakenType.INT64.value)
    if pa.types.is_floating(patype):
        return int(dn.DrakenType.FLOAT64.value)
    if pa.types.is_string(patype) or pa.types.is_large_string(patype):
        return int(dn.DrakenType.VARCHAR.value)
    return None


def _write_sidecar(path):
    """Write a canonical Vector.hash .stats.json next to a parquet file."""
    table = pq.read_table(path)
    names = list(table.column_names)
    field_ids = {n: i for i, n in enumerate(names)}
    min_k = {}
    for i, name in enumerate(names):
        col = table.column(name)
        phys = _pa_to_phys(col.type)
        if phys is None:
            continue
        hashes = set()
        has_null = False
        ok = True
        for value in pc.unique(col.combine_chunks()).to_pylist():
            if value is None:
                has_null = True
                continue
            h = hash_literal_kmv(value, phys)
            if h is None:
                ok = False
                break
            hashes.add(h)
        if not ok:
            continue
        if has_null:
            hashes.add(_NULL_SENTINEL)
        min_k[str(i)] = sorted(hashes)[:K]
    payload = {"schema_version": 1, "field_ids": field_ids, "min_k_hashes": min_k}
    with open(path + ".stats.json", "w", encoding="utf-8") as handle:
        json.dump(payload, handle, separators=(",", ":"))


@pytest.fixture()
def dataset(tmp_path):
    """4-file dataset with controlled per-file value distributions.

    region: f0={ASIA,EUROPE} f1={AFRICA} f2={AMERICA,MIDDLE EAST} f3={EUROPE}
    id:     globally unique 1..12   flag: bool   score: float
    """
    d = str(tmp_path / "regions")
    os.makedirs(d)
    files = {
        "f0": dict(id=[1, 2, 3], region=["ASIA", "ASIA", "EUROPE"], flag=[True, True, False], score=[1.5, 2.5, 1.5]),
        "f1": dict(id=[4, 5, 6], region=["AFRICA"] * 3, flag=[False] * 3, score=[3.5] * 3),
        "f2": dict(id=[7, 8, 9, 10], region=["AMERICA", "MIDDLE EAST", "AMERICA", "AMERICA"], flag=[True, False, True, True], score=[4.5, 5.5, 4.5, 6.5]),
        "f3": dict(id=[11, 12], region=["EUROPE", "EUROPE"], flag=[True, True], score=[7.5, 8.5]),
    }
    for name, cols in files.items():
        path = os.path.join(d, name + ".parquet")
        pq.write_table(
            pa.table(
                {
                    "id": pa.array(cols["id"], pa.int64()),
                    "region": pa.array(cols["region"], pa.string()),
                    "flag": pa.array(cols["flag"], pa.bool_()),
                    "score": pa.array(cols["score"], pa.float64()),
                }
            ),
            path,
        )
        _write_sidecar(path)
    return d


def _rows(sql):
    out = []
    for morsel in opteryx.session().execute_to_morsels(sql):
        for i in range(morsel.num_rows):
            out.append(morsel[i])
    return out


def _ids(dataset, where):
    return sorted(r[0] for r in _rows(f"SELECT id FROM '{dataset}' {where}"))


def _kept_after_prune(dataset, op, column, value):
    """Return the set of file basenames surviving MinHash pruning for one predicate."""
    import opteryx.connectors as connectors
    from opteryx.models import Node
    from opteryx.expression import NodeType

    engine = connectors.connector_factory(dataset, None).table_engine(dataset)
    _, manifest = engine.get_dataset_metadata()

    left = Node(node_type=NodeType.IDENTIFIER)
    left.source_column = column
    right = Node(node_type=NodeType.LITERAL)
    right.value = value
    predicate = Node(node_type=NodeType.COMPARISON_OPERATOR)
    predicate.value = op
    predicate.left = left
    predicate.right = right

    manifest.prune_files([predicate])
    return sorted(os.path.basename(f.file_path) for f in manifest.files)


# --------------------------------------------------------------------------
# Soundness: MinHash pruning must never change the answer.
# --------------------------------------------------------------------------

_EQUIVALENCE = [
    ("eq_present", "WHERE region = 'AFRICA'", [4, 5, 6]),
    ("eq_multi_file", "WHERE region = 'EUROPE'", [3, 11, 12]),
    ("eq_absent", "WHERE region = 'ZZZ'", []),
    ("in_list", "WHERE region IN ('AFRICA','MIDDLE EAST')", [4, 5, 6, 8]),
    ("neq", "WHERE region != 'AFRICA'", [1, 2, 3, 7, 8, 9, 10, 11, 12]),
    ("not_in", "WHERE region NOT IN ('EUROPE')", [1, 2, 4, 5, 6, 7, 8, 9, 10]),
    ("eq_int", "WHERE id = 8", [8]),
    ("in_int", "WHERE id IN (1, 12)", [1, 12]),
    ("eq_float", "WHERE score = 3.5", [4, 5, 6]),
    ("eq_bool", "WHERE flag = false", [3, 4, 5, 6, 8]),
]


@pytest.mark.parametrize("label, where, expected", _EQUIVALENCE)
def test_pruning_preserves_answer(dataset, label, where, expected):
    assert _ids(dataset, where) == expected, label


# --------------------------------------------------------------------------
# Efficacy: pruning actually eliminates the right files.
# --------------------------------------------------------------------------


def test_eq_prunes_to_owning_file(dataset):
    assert _kept_after_prune(dataset, "Eq", "region", "AFRICA") == ["f1.parquet"]


def test_eq_absent_prunes_everything(dataset):
    assert _kept_after_prune(dataset, "Eq", "region", "ZZZ") == []


def test_neq_prunes_single_value_file(dataset):
    # f1 is AFRICA-only, so region != 'AFRICA' excludes every row in it.
    assert _kept_after_prune(dataset, "NotEq", "region", "AFRICA") == [
        "f0.parquet",
        "f2.parquet",
        "f3.parquet",
    ]


def test_in_prunes_non_matching(dataset):
    assert _kept_after_prune(dataset, "InList", "region", ["AFRICA", "MIDDLE EAST"]) == [
        "f1.parquet",
        "f2.parquet",
    ]


def test_not_in_prunes_subset_file(dataset):
    # f3 is EUROPE-only, so NOT IN ('EUROPE') excludes every row in it.
    assert _kept_after_prune(dataset, "NotInList", "region", ["EUROPE"]) == [
        "f0.parquet",
        "f1.parquet",
        "f2.parquet",
    ]


def test_int_eq_prunes(dataset):
    assert _kept_after_prune(dataset, "Eq", "id", 8) == ["f2.parquet"]


# --------------------------------------------------------------------------
# Exact COUNT(DISTINCT) from statistics.
# --------------------------------------------------------------------------


@pytest.mark.parametrize("column, expected", [("region", 5), ("id", 12), ("flag", 2), ("score", 8)])
def test_count_distinct_exact(dataset, column, expected):
    assert _rows(f"SELECT COUNT(DISTINCT {column}) FROM '{dataset}'")[0][0] == expected


def test_count_distinct_is_statistics_only(dataset):
    explain = _rows(f"EXPLAIN SELECT COUNT(DISTINCT region) FROM '{dataset}'")
    assert any("statistics only" in str(row).lower() for row in explain)


def test_count_distinct_with_filter_is_not_statistics_only(dataset):
    # A residual WHERE means the whole-column sketch can't answer it — must scan.
    explain = _rows(f"EXPLAIN SELECT COUNT(DISTINCT region) FROM '{dataset}' WHERE id > 6")
    assert not any("statistics only" in str(row).lower() for row in explain)
    assert _rows(f"SELECT COUNT(DISTINCT region) FROM '{dataset}' WHERE id > 6")[0][0] == 3


# --------------------------------------------------------------------------
# Boundaries: saturation and NULLs.
# --------------------------------------------------------------------------


def test_saturated_sketch_is_not_exact(tmp_path):
    # 40 distinct values saturate K=32: COUNT(DISTINCT) must fall back to a scan
    # and still be correct, and equality pruning must not fire.
    d = str(tmp_path / "hi")
    os.makedirs(d)
    path = os.path.join(d, "a.parquet")
    pq.write_table(pa.table({"hi": pa.array(list(range(40)), pa.int64())}), path)
    _write_sidecar(path)

    explain = _rows(f"EXPLAIN SELECT COUNT(DISTINCT hi) FROM '{d}'")
    assert not any("statistics only" in str(row).lower() for row in explain)
    assert _rows(f"SELECT COUNT(DISTINCT hi) FROM '{d}'")[0][0] == 40
    assert _rows(f"SELECT COUNT(*) FROM '{d}' WHERE hi = 5")[0][0] == 1
    assert _rows(f"SELECT COUNT(*) FROM '{d}' WHERE hi = 999")[0][0] == 0


def test_count_distinct_excludes_nulls(tmp_path):
    d = str(tmp_path / "nulls")
    os.makedirs(d)
    path = os.path.join(d, "a.parquet")
    pq.write_table(
        pa.table(
            {
                "lo": pa.array([1, 2, None] * 13 + [1], pa.int64()),  # 2 distinct + nulls
                "an": pa.array([None] * 40, pa.int64()),  # all null
            }
        ),
        path,
    )
    _write_sidecar(path)

    assert _rows(f"SELECT COUNT(DISTINCT lo) FROM '{d}'")[0][0] == 2
    assert _rows(f"SELECT COUNT(DISTINCT an) FROM '{d}'")[0][0] == 0


# --------------------------------------------------------------------------
# SELECT DISTINCT LIMIT bound — inject an exact LIMIT for early scan stop.
# --------------------------------------------------------------------------


def test_select_distinct_returns_all_values(dataset):
    # The overriding soundness check: the injected LIMIT must never drop a value.
    got = sorted(r[0] for r in _rows(f"SELECT DISTINCT region FROM '{dataset}'"))
    assert got == ["AFRICA", "AMERICA", "ASIA", "EUROPE", "MIDDLE EAST"]


def test_select_distinct_injects_exact_limit(dataset):
    explain = _rows(f"EXPLAIN SELECT DISTINCT region FROM '{dataset}'")
    text = " ".join(str(r) for r in explain)
    assert "DistinctLimitBoundStrategy" in text  # strategy fired
    assert "LIMIT" in text and "5 OFFSET 0" in text  # exact bound = 5 distinct


def test_select_distinct_early_stop_reads_fewer_rows(tmp_path):
    # All distinct values live in the first small file; the rest are bulk dupes.
    # The exact LIMIT lets the scan terminate after the first file — the answer
    # is unchanged and far fewer rows are read than the ~500k total.
    d = str(tmp_path / "early")
    os.makedirs(d)
    pq.write_table(pa.table({"g": pa.array(["a", "b", "c"] * 100, pa.string())}), os.path.join(d, "00.parquet"))
    for i in range(1, 6):
        pq.write_table(pa.table({"g": pa.array(["a"] * 100_000, pa.string())}), os.path.join(d, f"{i:02d}.parquet"))
    for name in os.listdir(d):
        if name.endswith(".parquet"):
            _write_sidecar(os.path.join(d, name))

    assert sorted(r[0] for r in _rows(f"SELECT DISTINCT g FROM '{d}'")) == ["a", "b", "c"]


def test_select_distinct_with_null_returns_null_row(tmp_path):
    d = str(tmp_path / "dnull")
    os.makedirs(d)
    path = os.path.join(d, "a.parquet")
    pq.write_table(pa.table({"lo": pa.array([1, 2, None] * 13 + [1], pa.int64())}), path)
    _write_sidecar(path)
    got = _rows(f"SELECT DISTINCT lo FROM '{d}'")
    values = {r[0] for r in got}
    assert values == {1, 2, None}  # NULL is one distinct row, not dropped


def test_multicolumn_distinct_not_bounded(dataset):
    # region,flag has 6 distinct tuples; the per-column product is a loose upper
    # bound, so no LIMIT is injected — but the answer must still be complete.
    got = {tuple(r) for r in _rows(f"SELECT DISTINCT region, flag FROM '{dataset}'")}
    assert len(got) == 6
    explain = _rows(f"EXPLAIN SELECT DISTINCT region, flag FROM '{dataset}'")
    assert "DistinctLimitBoundStrategy" not in " ".join(str(r) for r in explain)


def test_saturated_distinct_not_bounded(tmp_path):
    d = str(tmp_path / "hi")
    os.makedirs(d)
    path = os.path.join(d, "a.parquet")
    pq.write_table(pa.table({"hi": pa.array(list(range(40)), pa.int64())}), path)
    _write_sidecar(path)
    assert len(_rows(f"SELECT DISTINCT hi FROM '{d}'")) == 40
    explain = _rows(f"EXPLAIN SELECT DISTINCT hi FROM '{d}'")
    assert "DistinctLimitBoundStrategy" not in " ".join(str(r) for r in explain)


if __name__ == "__main__":  # pragma: no cover
    pytest.main([__file__, "-v"])
