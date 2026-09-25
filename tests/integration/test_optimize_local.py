"""OPTIMIZE TABLE, end to end through the engine — local disk, no GCS.

What this proves is the shape of the OUTPUT. The first engine implementation
wrote one file per 262,144-row batch, so a pass over many small files produced
exactly as many small files and the next pass selected them again
("Compaction: 43 files -> 43 files", in production). Now the sink streams row
groups into one file and rolls at the target size, and registers each file from
statistics gathered as it wrote - the output is never read back.

Selection and the sink are unit-tested in tests/planner and tests/operators;
this is the two halves and the catalog's commit together, through SQL.
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

WORKSPACE = "optws"
TARGET = f"{WORKSPACE}.col.tgt"
ROWS_PER_SEED_FILE = 120_000
SEED_FILES = 3


class _LocalDiskIO:
    """Catalog-side FileIO over absolute local paths, counting reads."""

    def __init__(self):
        self.reads = []

    class _In:
        def __init__(self, path):
            self._path = path

        def open(self):
            return open(self._path, "rb")

    class _Out:
        def __init__(self, path):
            self._path = path
            self._chunks = []
            self.aborted = False

        def create(self):
            return self

        def write(self, data):
            self._chunks.append(bytes(data))

        def close(self):
            os.makedirs(os.path.dirname(self._path), exist_ok=True)
            with open(self._path, "wb") as f:
                for chunk in self._chunks:
                    f.write(chunk)

        def abort(self):
            self.aborted = True
            self._chunks = []

    def new_input(self, path):
        self.reads.append(path)
        return self._In(path)

    def new_output(self, path):
        return self._Out(path)

    def delete(self, path):
        os.remove(path)


def _build_dataset(location, identifier, disk_io):
    """A real SimpleDataset on local disk holding SEED_FILES small data files."""
    from draken.interop.vector_sequence import vector_from_sequence
    from draken.morsels.morsel import Morsel
    from rugo.parquet import write_parquet

    from opteryx_catalog.catalog.dataset import SimpleDataset
    from opteryx_catalog.catalog.manifest import build_parquet_manifest_entry_from_bytes
    from opteryx_catalog.catalog.metadata import DatasetMetadata
    from opteryx_catalog.catalog.metadata import Snapshot
    from opteryx_catalog.opteryx_catalog import OpteryxCatalog

    os.makedirs(f"{location}/data", exist_ok=True)
    os.makedirs(f"{location}/metadata", exist_ok=True)

    entries = []
    for n in range(SEED_FILES):
        morsel = Morsel()
        start = n * ROWS_PER_SEED_FILE
        morsel.append_vector(
            "k", vector_from_sequence(list(range(start, start + ROWS_PER_SEED_FILE)), dtype="INTEGER")
        )
        data = write_parquet(morsel, compression="zstd")
        path = f"{location}/data/seed_{n}.parquet"
        with open(path, "wb") as f:
            f.write(data)
        entries.append(
            build_parquet_manifest_entry_from_bytes(
                data, path, len(data), field_id_by_name={"k": 1}
            ).to_dict()
        )

    class _ManifestWriterCatalog:
        io = disk_io
        write_parquet_manifest = OpteryxCatalog.write_parquet_manifest

        def save_snapshot(self, identifier, snapshot):
            pass

        def save_dataset_metadata(self, identifier, metadata, **kwargs):
            pass

    writer_catalog = _ManifestWriterCatalog()
    snapshot_id = 1000
    manifest_path = writer_catalog.write_parquet_manifest(snapshot_id, entries, location)

    meta = DatasetMetadata(
        dataset_identifier=identifier,
        location=location,
        schema=None,
        properties={},
        schemas=[{"schema_id": "s1", "columns": [{"id": 1, "name": "k", "type": "INTEGER"}]}],
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

    ds = SimpleDataset(identifier=identifier, _metadata=meta)
    ds.io = disk_io
    ds.catalog = writer_catalog
    return ds


@pytest.fixture
def optimize_env(tmp_path):
    from opteryx_catalog.catalog.manifest import clear_parsed_manifest_cache

    import opteryx.connectors as connectors

    clear_parsed_manifest_cache()
    disk_io = _LocalDiskIO()
    target = _build_dataset(str(tmp_path / "tgt"), "col.tgt", disk_io)
    datasets = {"col.tgt": target}

    class _FakeCatalog:
        def __init__(self, workspace=None, **kwargs):
            self.workspace = workspace
            self.io = disk_io

        def dataset_exists(self, identifier):
            return identifier in datasets

        def load_dataset(self, identifier):
            if identifier not in datasets:
                raise KeyError(identifier)
            return datasets[identifier]

        def get_relation(self, identifier):
            if identifier in datasets:
                return "dataset", datasets[identifier]
            return None, None

    saved_default = connectors._default_connector
    saved_prefixes = dict(connectors._storage_prefixes)
    saved_cache = dict(connectors._connector_cache)
    connectors._storage_prefixes.pop(WORKSPACE, None)
    connectors._connector_cache.clear()

    opteryx.set_default_connector(OpteryxConnector, catalog=_FakeCatalog)
    try:
        yield target, disk_io
    finally:
        connectors._default_connector = saved_default
        connectors._storage_prefixes.clear()
        connectors._storage_prefixes.update(saved_prefixes)
        connectors._connector_cache.clear()
        connectors._connector_cache.update(saved_cache)


def _scalar(sql):
    from opteryx_catalog.catalog.manifest import clear_parsed_manifest_cache

    clear_parsed_manifest_cache()
    session = opteryx.session(user="tester")
    (morsel,) = list(session.execute_to_morsels(sql))
    return morsel[0][0]


def _current_entries(ds):
    from opteryx_catalog.catalog.manifest import read_manifest_rows

    with ds.io.new_input(ds.snapshot(None).manifest_list).open() as f:
        return read_manifest_rows(f.read())


def _row_group_count(path):
    from rugo.parquet import read_parquet

    with open(path, "rb") as f:
        data = f.read()
    with read_parquet(data) as reader:
        return sum(1 for _ in reader)


def test_optimize_writes_one_file_of_many_row_groups_and_never_reads_it_back(optimize_env):
    target, disk_io = optimize_env
    total = SEED_FILES * ROWS_PER_SEED_FILE
    assert _scalar(f"SELECT COUNT(*) FROM {TARGET}") == total
    assert len(_current_entries(target)) == SEED_FILES
    before = target.metadata.current_snapshot_id

    disk_io.reads.clear()
    list(opteryx.session(user="tester").execute_to_morsels(f"OPTIMIZE TABLE {TARGET}"))

    snap = target.snapshot(None)
    assert target.metadata.current_snapshot_id != before
    assert snap.operation_type == "compact"
    assert snap.summary["deleted-data-files"] == SEED_FILES
    assert snap.summary["added-data-files"] == 1, "one file, not one per batch"
    assert snap.summary["added-records"] == total

    entries = _current_entries(target)
    assert len(entries) == 1
    (out,) = entries
    assert out["record_count"] == total
    # Manifest round-trips hand list columns back as tuples.
    assert list(out["field_ids"]) == [1]
    assert list(out["min_values"]) == [0] and list(out["max_values"]) == [total - 1]
    assert out["uncompressed_size_in_bytes"] > 0

    # 360,000 rows at 65,536 per row group (the sink's batch = row group, capped at
    # rugo's DEFAULT_ROWS_PER_ROW_GROUP) is six row groups of ONE file.
    assert _row_group_count(out["file_path"]) == 6

    # The output was described as it was written, never downloaded to be read.
    assert out["file_path"] not in disk_io.reads

    # And it holds exactly the rows the inputs did.
    assert _scalar(f"SELECT COUNT(*) FROM {TARGET}") == total
    assert _scalar(f"SELECT SUM(k) FROM {TARGET}") == total * (total - 1) // 2

    # A second pass has nothing left to do: one file, no delete debt.
    list(opteryx.session(user="tester").execute_to_morsels(f"OPTIMIZE TABLE {TARGET}"))
    assert target.snapshot(None).snapshot_id == snap.snapshot_id
