"""OPTIMIZE TABLE on a relation with a vector index — local disk, through SQL (§5.6, D-14).

Compaction never embeds. Indexed inputs give outputs whose index files are CARRIED from
the inputs' vectors and land in the compaction commit itself. What this protects:
  * OPTIMIZE asks its scan for row origins only when the relation has an index;
  * every output row that had a vector keeps exactly that vector (looked up through its
    text, which the static-hash embedder maps to one vector), and no other row is indexed;
  * a row deleted before the compaction is let go, not carried;
  * the compaction holds the maintenance lease, and is refused while an index build does.
"""

import os
import sys

import pytest

sys.path.insert(1, os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.insert(1, os.path.dirname(__file__))

_CATALOG_REPO = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..", "opteryx-catalog"))
if os.path.isdir(_CATALOG_REPO) and _CATALOG_REPO not in sys.path:
    sys.path.insert(1, _CATALOG_REPO)

import opteryx  # noqa: E402
from opteryx.connectors import OpteryxConnector  # noqa: E402

from test_optimize_local import _LocalDiskIO  # noqa: E402
from test_optimize_local import pytestmark  # noqa: E402,F401

WORKSPACE = "carryws"
TABLE = f"{WORKSPACE}.col.docs"
WORDS = ["red", "planet", "gas", "giant", "ice", "moon", "ring", "storm", "dust", "orbit"]
FILES, ROWS = 3, 300


def _text(f, i):
    # Unique per row (the lookup below goes text -> vector); every 37th is null.
    return None if i % 37 == 5 else " ".join(WORDS[(i * k + f) % 10] for k in (1, 3, 7)) + f" {f}-{i}"


def _build_dataset(location, identifier, disk_io):
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
    for f in range(FILES):
        morsel = Morsel()
        morsel.append_vector("id", vector_from_sequence([f * ROWS + i for i in range(ROWS)], dtype="INTEGER"))
        morsel.append_vector("body", vector_from_sequence([_text(f, i) for i in range(ROWS)], dtype="VARCHAR"))
        data = write_parquet(morsel, compression="zstd")
        path = f"{location}/data/seed_{f}.parquet"
        with open(path, "wb") as out:
            out.write(data)
        entries.append(build_parquet_manifest_entry_from_bytes(
            data, path, len(data), field_id_by_name={"id": 1, "body": 2}).to_dict())

    class _ManifestWriterCatalog:
        io = disk_io
        write_parquet_manifest = OpteryxCatalog.write_parquet_manifest

        def save_snapshot(self, identifier, snapshot):
            pass

        def save_dataset_metadata(self, identifier, metadata, **kwargs):
            pass

    writer = _ManifestWriterCatalog()
    manifest = writer.write_parquet_manifest(1000, entries, location)
    meta = DatasetMetadata(
        dataset_identifier=identifier, location=location, schema=None, properties={},
        schemas=[{"schema_id": "s1", "columns": [
            {"id": 1, "name": "id", "type": "INTEGER"}, {"id": 2, "name": "body", "type": "VARCHAR"},
        ]}],
        current_schema_id="s1",
    )
    meta.snapshots.append(Snapshot(
        snapshot_id=1000, timestamp_ms=1000, author="seed", sequence_number=1, user_created=True,
        operation_type="append", manifest_list=manifest, schema_id="s1",
    ))
    meta.current_snapshot_id = 1000
    ds = SimpleDataset(identifier=identifier, _metadata=meta)
    ds.io = disk_io
    ds.catalog = writer
    return ds


@pytest.fixture
def env(tmp_path):
    from types import SimpleNamespace

    import opteryx.connectors as connectors
    from opteryx_catalog.catalog.maintenance_lease import MaintenanceLease
    from opteryx_catalog.catalog.maintenance_lease import describe_holder
    from opteryx_catalog.catalog.manifest import clear_parsed_manifest_cache
    from opteryx_catalog.catalog.vector_indexes import new_index_definition
    from opteryx_catalog.catalog.vector_indexes import normalize_index_name
    from opteryx_catalog.exceptions import MaintenanceLeaseHeld
    from opteryx_catalog.exceptions import VectorIndexNotFound

    clear_parsed_manifest_cache()
    disk_io = _LocalDiskIO()
    dataset = _build_dataset(str(tmp_path / "docs"), "col.docs", disk_io)
    datasets = {"col.docs": dataset}
    indexes: dict = {}
    leases: dict = {}
    lease_log: list = []

    class _FakeCatalog:
        def __init__(self, workspace=None, **kwargs):
            self.workspace = workspace
            self.io = disk_io

        def dataset_exists(self, identifier):
            return identifier in datasets

        def load_dataset(self, identifier):
            return datasets[identifier]

        def get_relation(self, identifier):
            return ("dataset", datasets[identifier]) if identifier in datasets else (None, None)

        def create_vector_index(self, identifier, name, column, *, embedding_identity, dimensions,
                                author, build=None, clusters=0):
            record = new_index_definition(
                name=name, column=column, method="ivf", metric="cosine", build=build, clusters=clusters,
                embedding_identity=embedding_identity, dimensions=dimensions,
                author=author, created_at_ms=1,
            )
            indexes[(identifier, record["name"])] = record
            return record

        def get_vector_index(self, identifier, name):
            key = (identifier, normalize_index_name(name))
            if key not in indexes:
                raise VectorIndexNotFound(name)
            return indexes[key]

        def drop_vector_index(self, identifier, name, *, author):
            del indexes[(identifier, normalize_index_name(name))]

        def list_vector_indexes(self, identifier):
            return [r for (i, _), r in sorted(indexes.items()) if i == identifier]

        def claim_maintenance_lease(self, identifier, *, holder, operation, ttl_seconds):
            if identifier in leases:
                raise MaintenanceLeaseHeld(f"{identifier} is held for {describe_holder(leases[identifier].to_document())}")
            leases[identifier] = MaintenanceLease(
                dataset=identifier, claim_id=f"c{len(lease_log)}", holder=holder, operation=operation,
                claimed_at_ms=1, expires_at_ms=1 + ttl_seconds * 1000,
            )
            lease_log.append(("claim", operation))
            return leases[identifier]

        def renew_maintenance_lease(self, lease, *, ttl_seconds):
            return lease

        def release_maintenance_lease(self, lease):
            if leases.get(lease.dataset) is not lease:
                return False
            del leases[lease.dataset]
            lease_log.append(("release", lease.operation))
            return True

    saved_default = connectors._default_connector
    saved_prefixes = dict(connectors._storage_prefixes)
    saved_cache = dict(connectors._connector_cache)
    connectors._storage_prefixes.pop(WORKSPACE, None)
    connectors._connector_cache.clear()
    opteryx.set_default_connector(OpteryxConnector, catalog=_FakeCatalog)
    try:
        yield SimpleNamespace(dataset=dataset, indexes=indexes, leases=leases, lease_log=lease_log)
    finally:
        connectors._default_connector = saved_default
        connectors._storage_prefixes.clear()
        connectors._storage_prefixes.update(saved_prefixes)
        connectors._connector_cache.clear()
        connectors._connector_cache.update(saved_cache)


def _run(sql):
    return list(opteryx.session(user="tester").execute_to_morsels(sql))


def _entries(dataset):
    from opteryx_catalog.catalog.manifest import get_parsed_manifest

    return get_parsed_manifest(dataset.io, dataset.snapshot(None).manifest_list)


def _vectors_by_ordinal(path):
    import skene

    data = open(path, "rb").read()
    out = {}
    for g in range(len(skene.read_metadata(data)["row_groups"])):
        m = skene.read_morsel(data, g)
        m.materialize()
        out.update(zip(m.column("ordinal").to_pylist(), m.column("embedding").to_pylist()))
    return out


def _texts(path):
    from rugo.parquet import read_parquet

    texts = []
    for morsel in read_parquet(path, columns=["body"]):
        texts.extend(morsel.column("body").to_pylist())
    return texts


def _vector_of_text(dataset, index_id):
    """text -> vector, over every file's index."""
    from opteryx_catalog.catalog.vector_indexes import index_refs

    found = {}
    for entry in _entries(dataset):
        refs = index_refs(entry)
        if index_id not in refs:
            continue
        texts = _texts(entry["file_path"])
        for ordinal, vector in _vectors_by_ordinal(refs[index_id].vectors).items():
            found[texts[ordinal]] = vector
    return found


def test_optimize_carries_every_vector_into_the_compaction_commit(env):
    from opteryx_catalog.catalog.vector_indexes import index_refs

    _run(f"CREATE INDEX body_idx ON {TABLE} USING IVF (body) WITH (build = 'sync')")
    (record,) = env.indexes.values()
    index_id = record["index-id"]
    before = _vector_of_text(env.dataset, index_id)
    seeded = {e["file_path"] for e in _entries(env.dataset)}
    env.lease_log.clear()

    _run(f"OPTIMIZE TABLE {TABLE}")

    snap = env.dataset.snapshot(None)
    assert snap.operation_type == "compact"
    entries = _entries(env.dataset)
    assert not seeded & {e["file_path"] for e in entries}            # every seed file retired
    for entry in entries:
        refs = index_refs(entry)
        assert set(refs) == {index_id}                                 # carried, same commit
        texts = _texts(entry["file_path"])
        carried = _vectors_by_ordinal(refs[index_id].vectors)
        assert {texts[o]: v for o, v in carried.items()} == {
            t: before[t] for t in texts if t is not None
        }
        assert os.path.getsize(refs[index_id].vectors) == refs[index_id].vectors_bytes
    assert env.lease_log == [("claim", "compaction"), ("release", "compaction")]


def test_optimize_lets_deleted_rows_go(env):
    _run(f"CREATE INDEX body_idx ON {TABLE} USING IVF (body) WITH (build = 'sync')")
    (record,) = env.indexes.values()
    before = _vector_of_text(env.dataset, record["index-id"])
    first = sorted(e["file_path"] for e in _entries(env.dataset))[0]
    env.dataset.delete_rows({first: [0, 1]}, author="tester")
    gone = set(_texts(first)[:2])

    _run(f"OPTIMIZE TABLE {TABLE}")
    after = _vector_of_text(env.dataset, record["index-id"])
    assert after == {t: v for t, v in before.items() if t not in gone}


def test_optimize_is_refused_while_an_index_build_holds_the_lease(env):
    from opteryx_catalog.catalog.maintenance_lease import MaintenanceLease

    _run(f"CREATE INDEX body_idx ON {TABLE} USING IVF (body) WITH (build = 'sync')")
    head = env.dataset.metadata.current_snapshot_id
    env.leases["col.docs"] = MaintenanceLease(
        dataset="col.docs", claim_id="other", holder="REFRESH INDEX body_idx by olive",
        operation="index-build", claimed_at_ms=1, expires_at_ms=10**15,
    )
    with pytest.raises(Exception, match="index-build by REFRESH INDEX body_idx by olive"):
        _run(f"OPTIMIZE TABLE {TABLE}")
    assert env.dataset.metadata.current_snapshot_id == head


def test_optimize_merges_only_files_with_the_same_coverage(env):
    """Two of three files indexed: the pass merges the indexed pair (carrying their
    vectors) and leaves the unindexed file alone - never a mixed-coverage output."""
    import opteryx.operators._operators as operators
    from draken.ops.kernels._kernel_registry import lookup_kernel
    from opteryx_catalog.catalog.vector_indexes import IndexFiles
    from opteryx_catalog.catalog.vector_indexes import index_refs
    from opteryx_catalog.catalog.vector_indexes import vector_index_paths

    _run(f"CREATE INDEX body_idx ON {TABLE} USING IVF (body)")              # async: builds nothing
    (record,) = env.indexes.values()
    index_id = record["index-id"]
    paths = sorted(e["file_path"] for e in _entries(env.dataset))
    fn, _ = lookup_kernel("draken_embed")
    files = {}
    for path in paths[:2]:
        vectors, centroids = vector_index_paths(env.dataset.metadata.location, index_id, path)
        os.makedirs(os.path.dirname(vectors), exist_ok=True)
        built = operators.build_vector_index_local(path, "body", [], fn, record["dimensions"], vectors, centroids)
        files[path] = IndexFiles(vectors, centroids, built["vectors_bytes"], built["centroids_bytes"], built["logical_bytes"])
    env.dataset.commit_vector_index_files(index_id, files, author="tester", agent="test")
    before = _vector_of_text(env.dataset, index_id)

    _run(f"OPTIMIZE TABLE {TABLE}")

    entries = {e["file_path"]: e for e in _entries(env.dataset)}
    assert paths[2] in entries and index_refs(entries[paths[2]]) == {}     # untouched, unindexed
    assert not set(paths[:2]) & set(entries)                               # the pair was retired
    assert _vector_of_text(env.dataset, index_id) == before                # every vector carried
