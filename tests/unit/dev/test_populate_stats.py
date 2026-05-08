"""Tests for dev/populate_stats.py and the filesystem_connector sidecar loader."""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path

import pytest

# dev/ is not on the Python path normally; make it importable for tests.
_DEV_DIR = Path(__file__).resolve().parents[3] / "dev"
if str(_DEV_DIR) not in sys.path:
    sys.path.insert(0, str(_DEV_DIR))

import populate_stats  # noqa: E402

K = populate_stats.K
HASH_RANGE = 2**64


pa = pytest.importorskip("pyarrow")
pq = pytest.importorskip("pyarrow.parquet")


def _write_parquet(path: Path, table) -> None:
    pq.write_table(table, str(path))


def _ndv_from_sketch(hashes: list[int]) -> int:
    """Mirror Manifest.estimate_cardinality so tests are self-contained."""
    if len(hashes) < K:
        return len(hashes)
    return int((K - 1) * HASH_RANGE / hashes[K - 1])


# ---------------------------------------------------------------------------
# KMV computation
# ---------------------------------------------------------------------------


def test_compute_sketch_few_distinct_values_yields_exact_count():
    sketch = populate_stats.compute_sketch([1, 2, 3, 4, 5, 5, 4, 3])
    assert len(sketch) == 5
    assert _ndv_from_sketch(sketch) == 5


def test_compute_sketch_caps_at_K_for_many_distinct_values():
    sketch = populate_stats.compute_sketch(range(10_000))
    assert len(sketch) == K


def test_compute_sketch_skips_nulls():
    sketch = populate_stats.compute_sketch([None, None, None])
    assert sketch == []


def test_compute_sketch_sorted_ascending():
    sketch = populate_stats.compute_sketch(range(1_000))
    assert sketch == sorted(sketch)


# ---------------------------------------------------------------------------
# CLI behaviour
# ---------------------------------------------------------------------------


def test_dry_run_does_not_write_sidecar(tmp_path: Path):
    parquet = tmp_path / "t.parquet"
    _write_parquet(parquet, pa.table({"a": list(range(10))}))

    populate_stats.populate(tmp_path, dry_run=True)
    assert not (tmp_path / "t.parquet.stats.json").exists()


def test_populate_writes_sidecar(tmp_path: Path):
    parquet = tmp_path / "t.parquet"
    _write_parquet(parquet, pa.table({"a": list(range(10)), "b": ["x"] * 10}))

    populate_stats.populate(tmp_path)
    sidecar = tmp_path / "t.parquet.stats.json"
    assert sidecar.exists()

    data = json.loads(sidecar.read_text())
    assert data["schema_version"] == populate_stats.SCHEMA_VERSION
    assert data["field_ids"] == {"a": 0, "b": 1}
    # 10 distinct in 'a', 1 distinct in 'b'
    assert _ndv_from_sketch(data["min_k_hashes"]["0"]) == 10
    assert _ndv_from_sketch(data["min_k_hashes"]["1"]) == 1


def test_populate_skips_fresh_sidecar(tmp_path: Path):
    parquet = tmp_path / "t.parquet"
    _write_parquet(parquet, pa.table({"a": [1, 2, 3]}))

    populate_stats.populate(tmp_path)
    sidecar = tmp_path / "t.parquet.stats.json"
    first_mtime = sidecar.stat().st_mtime

    # Make sidecar look newer than the parquet so the freshness check skips.
    os.utime(sidecar, (first_mtime + 10, first_mtime + 10))

    populate_stats.populate(tmp_path)
    assert sidecar.stat().st_mtime == first_mtime + 10  # untouched


def test_populate_force_overwrites(tmp_path: Path):
    parquet = tmp_path / "t.parquet"
    _write_parquet(parquet, pa.table({"a": [1, 2, 3]}))

    populate_stats.populate(tmp_path)
    sidecar = tmp_path / "t.parquet.stats.json"
    sidecar.write_text(json.dumps({"schema_version": 1, "field_ids": {}, "min_k_hashes": {}}))
    bad = sidecar.stat().st_mtime
    os.utime(sidecar, (bad + 100, bad + 100))

    populate_stats.populate(tmp_path, force=True)
    data = json.loads(sidecar.read_text())
    assert data["field_ids"] == {"a": 0}


# ---------------------------------------------------------------------------
# Filesystem connector loader
# ---------------------------------------------------------------------------


def _build_relation(tmp_path: Path, with_sidecar: bool):
    """Write a parquet relation under tmp_path/data and return (table, dataset_dir)."""
    dataset_dir = tmp_path / "data"
    dataset_dir.mkdir()
    parquet = dataset_dir / "part-0.parquet"
    table = pa.table(
        {
            "id": list(range(1000)),  # 1000 distinct
            "category": [f"cat_{i % 7}" for i in range(1000)],  # 7 distinct
        }
    )
    _write_parquet(parquet, table)

    if with_sidecar:
        populate_stats.populate(dataset_dir)

    return parquet, dataset_dir


def _load_table(dataset_dir: Path):
    from opteryx.connectors.filesystem_connector import FileSystemTable
    from opteryx.connectors.io_systems.local_filesystem import OpteryxLocalFileSystem
    from opteryx.models.query_telemetry import QueryTelemetry

    fs = OpteryxLocalFileSystem()
    table = FileSystemTable(
        dataset=str(dataset_dir),
        filesystem=fs,
        storage_type="LOCAL",
        telemetry=QueryTelemetry(),
    )
    return table.get_dataset_metadata()


def test_loader_round_trip_populates_min_k_hashes(tmp_path: Path):
    parquet, dataset_dir = _build_relation(tmp_path, with_sidecar=True)
    schema, manifest = _load_table(dataset_dir)

    assert len(manifest.files) == 1
    fe = manifest.files[0]
    assert fe.min_k_hashes is not None
    assert len(fe.min_k_hashes) == len(schema.columns)

    # KMV-32 has ~1/sqrt(K-2) ~= 18% standard error; allow a 30% band.
    ndv_id = manifest.estimate_cardinality("id")
    ndv_cat = manifest.estimate_cardinality("category")
    assert ndv_id is not None
    assert 700 <= ndv_id <= 1300  # 1000 distinct +/- 30%
    assert ndv_cat == 7  # < K, exact


def test_loader_returns_none_without_sidecar(tmp_path: Path):
    parquet, dataset_dir = _build_relation(tmp_path, with_sidecar=False)
    schema, manifest = _load_table(dataset_dir)

    assert len(manifest.files) == 1
    assert manifest.files[0].min_k_hashes is None
    assert manifest.estimate_cardinality("id") is None


def test_loader_rejects_unknown_schema_version(tmp_path: Path):
    parquet, dataset_dir = _build_relation(tmp_path, with_sidecar=True)
    sidecar = parquet.with_name(parquet.name + ".stats.json")

    payload = json.loads(sidecar.read_text())
    payload["schema_version"] = 2
    sidecar.write_text(json.dumps(payload))

    schema, manifest = _load_table(dataset_dir)
    assert manifest.files[0].min_k_hashes is None


def test_loader_rejects_field_id_mismatch(tmp_path: Path, capsys):
    parquet, dataset_dir = _build_relation(tmp_path, with_sidecar=True)
    sidecar = parquet.with_name(parquet.name + ".stats.json")

    payload = json.loads(sidecar.read_text())
    # Swap field ids — schema is (id=0, category=1), claim the opposite.
    payload["field_ids"] = {"id": 1, "category": 0}
    sidecar.write_text(json.dumps(payload))

    schema, manifest = _load_table(dataset_dir)
    assert manifest.files[0].min_k_hashes is None
    err = capsys.readouterr().err
    assert "stale stats sidecar" in err


def test_loader_handles_malformed_json(tmp_path: Path):
    parquet, dataset_dir = _build_relation(tmp_path, with_sidecar=True)
    sidecar = parquet.with_name(parquet.name + ".stats.json")
    sidecar.write_text("{ this is not valid json")

    schema, manifest = _load_table(dataset_dir)
    assert manifest.files[0].min_k_hashes is None
