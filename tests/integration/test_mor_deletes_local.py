"""Merge-on-read deletes, end to end through the engine — local disk, no GCS.

The catalog side (sidecar format, commit path, GC protection) is tested in
opteryx-catalog/tests/test_mor_deletes.py. This module proves the ENGINE half:

* the binding resolves delete vectors onto FileEntry (opteryx_connector),
* the parquet read node subtracts deleted ordinals per row group,
* COUNT(*) answered from the manifest is the LIVE count,
* per-column statistics-only answers (MIN/MAX) are NOT taken from the
  manifest when deletes exist — the min row of a file may be a deleted row.

Data files live on local disk; the catalog is a fake around a real
SimpleDataset whose delete_rows() wrote a real sidecar, so every byte the
engine reads went through the production write path.
"""

import os
import sys

import pytest

sys.path.insert(1, os.path.join(os.path.dirname(__file__), "..", ".."))

_CATALOG_REPO = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "..", "..", "opteryx-catalog")
)
if os.path.isdir(_CATALOG_REPO) and _CATALOG_REPO not in sys.path:
    sys.path.insert(1, _CATALOG_REPO)

import opteryx  # noqa: E402
from opteryx.connectors import OpteryxConnector  # noqa: E402

try:
    import opteryx_catalog  # noqa: F401

    _HAVE_CATALOG = True
except ImportError:
    _HAVE_CATALOG = False

pytestmark = pytest.mark.skipif(
    not _HAVE_CATALOG,
    reason=f"opteryx_catalog not importable (expected sibling repo at {_CATALOG_REPO})",
)

WORKSPACE = "morws"
DATASET = f"{WORKSPACE}.col.ds"


class _LocalDiskIO:
    """Catalog-side FileIO over absolute local paths."""

    class _In:
        def __init__(self, path):
            self._path = path

        def open(self):
            return open(self._path, "rb")

    class _Out:
        def __init__(self, path):
            self._path = path
            self._chunks = []

        def create(self):
            return self

        def write(self, data):
            self._chunks.append(data)

        def close(self):
            with open(self._path, "wb") as f:
                for chunk in self._chunks:
                    f.write(chunk)

    def new_input(self, path):
        return self._In(path)

    def new_output(self, path):
        return self._Out(path)


@pytest.fixture(scope="module")
def dataset(tmp_path_factory):
    """A real SimpleDataset on local disk: two files, one with deleted rows."""
    from draken.interop.vector_sequence import vector_from_sequence
    from draken.morsels.morsel import Morsel
    from rugo.parquet import write_parquet

    from opteryx_catalog.catalog.dataset import SimpleDataset
    from opteryx_catalog.catalog.manifest import build_parquet_manifest_entry_from_bytes
    from opteryx_catalog.catalog.manifest import clear_parsed_manifest_cache
    from opteryx_catalog.catalog.metadata import DatasetMetadata
    from opteryx_catalog.catalog.metadata import Snapshot
    from opteryx_catalog.opteryx_catalog import OpteryxCatalog

    clear_parsed_manifest_cache()
    root = tmp_path_factory.mktemp("morws")
    location = str(root)
    os.makedirs(f"{location}/metadata", exist_ok=True)
    disk_io = _LocalDiskIO()

    # f1: a = 1..8 (we will delete a=1 — the global minimum — and a=5)
    # f2: a = 100..103
    files = {
        f"{location}/data/f1.parquet": [1, 2, 3, 4, 5, 6, 7, 8],
        f"{location}/data/f2.parquet": [100, 101, 102, 103],
    }
    os.makedirs(f"{location}/data", exist_ok=True)
    entries = []
    for path, values in files.items():
        m = Morsel()
        m.append_vector("a", vector_from_sequence(values, dtype="INTEGER"))
        data = write_parquet(m, compression="zstd")
        with open(path, "wb") as f:
            f.write(data)
        entries.append(
            build_parquet_manifest_entry_from_bytes(
                data, path, len(data), field_id_by_name={"a": 1}
            ).to_dict()
        )

    class _ManifestWriterCatalog:
        io = disk_io
        write_parquet_manifest = OpteryxCatalog.write_parquet_manifest

        def save_snapshot(self, identifier, snapshot):
            pass

        def save_dataset_metadata(self, identifier, metadata):
            pass

    writer_catalog = _ManifestWriterCatalog()
    snapshot_id = 1000
    manifest_path = writer_catalog.write_parquet_manifest(snapshot_id, entries, location)

    meta = DatasetMetadata(
        dataset_identifier="col.ds",
        location=location,
        schema=None,
        properties={},
        schemas=[
            {
                "schema_id": "s1",
                "columns": [{"id": 1, "name": "a", "type": "INTEGER"}],
            }
        ],
        current_schema_id="s1",
    )
    meta.snapshots.append(
        Snapshot(
            snapshot_id=snapshot_id,
            timestamp_ms=snapshot_id,
            author="seed",
            sequence_number=1,
            user_created=True,
            operation_type="append",
            manifest_list=manifest_path,
            schema_id="s1",
        )
    )
    meta.current_snapshot_id = snapshot_id

    ds = SimpleDataset(identifier="col.ds", _metadata=meta)
    ds.io = disk_io
    ds.catalog = writer_catalog

    # The production delete path: ordinals 0 (a=1, the global min) and 4 (a=5).
    ds.delete_rows({f"{location}/data/f1.parquet": [0, 4]}, author="tester")
    return ds


@pytest.fixture(scope="module")
def catalog_connector(dataset):
    import opteryx.connectors as connectors

    class _FakeCatalog:
        def __init__(self, workspace=None, **kwargs):
            self.workspace = workspace
            self.io = dataset.io

        def load_dataset(self, identifier):
            if identifier != "col.ds":
                raise KeyError(identifier)
            return dataset

        def get_relation(self, identifier):
            if identifier == "col.ds":
                return "dataset", dataset
            return None, None

    saved_default = connectors._default_connector
    saved_prefixes = dict(connectors._storage_prefixes)
    saved_cache = dict(connectors._connector_cache)
    connectors._storage_prefixes.pop(WORKSPACE, None)
    connectors._connector_cache.clear()

    opteryx.set_default_connector(OpteryxConnector, catalog=_FakeCatalog)
    try:
        yield
    finally:
        connectors._default_connector = saved_default
        connectors._storage_prefixes.clear()
        connectors._storage_prefixes.update(saved_prefixes)
        connectors._connector_cache.clear()
        connectors._connector_cache.update(saved_cache)


def _rows(sql):
    session = opteryx.session()
    out = []
    for morsel in session.execute_to_morsels(sql):
        for i in range(morsel.num_rows):
            out.append(morsel[i])
    return out


def test_scan_eliminates_deleted_rows(catalog_connector):
    values = sorted(r[0] for r in _rows(f"SELECT a FROM {DATASET}"))
    # a=1 and a=5 were deleted; every other physical row is served.
    assert values == [2, 3, 4, 6, 7, 8, 100, 101, 102, 103], values


def test_filter_composes_with_delete_vector(catalog_connector):
    # The pushed predicate runs AFTER delete elimination: a=5 matches the
    # predicate but is deleted, so it must not appear.
    values = sorted(r[0] for r in _rows(f"SELECT a FROM {DATASET} WHERE a >= 4"))
    assert values == [4, 6, 7, 8, 100, 101, 102, 103], values


def test_count_star_is_live_count(catalog_connector):
    # The statistics-only rewrite answers this from the manifest — which must
    # report live rows (12 physical - 2 deleted).
    assert _rows(f"SELECT COUNT(*) FROM {DATASET}")[0][0] == 10


def test_min_not_answered_from_stale_manifest(catalog_connector):
    # The manifest's min for `a` is 1 — a deleted row. The statistics-only
    # strategy must decline and let the scan produce the true live min.
    assert _rows(f"SELECT MIN(a) FROM {DATASET}")[0][0] == 2


def test_count_with_predicate_scans_live_rows(catalog_connector):
    assert _rows(f"SELECT COUNT(*) FROM {DATASET} WHERE a < 100")[0][0] == 6
