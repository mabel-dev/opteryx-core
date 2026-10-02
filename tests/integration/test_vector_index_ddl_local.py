"""CREATE / ALTER / DROP INDEX (vector index) end to end through SQL — local disk, no GCS.

The fake catalog stores definitions in a dict but validates them with the REAL catalog's
`new_index_definition`, so the rules exercised are the catalog's own; the dataset is a real
SimpleDataset, so DROP INDEX runs the real `remove_vector_index_files` commit.
What this protects:
  * CREATE defines with the active embedding identity and width, `async` by default
    (ruled 2026-10-02), honouring WITH options and IF NOT EXISTS;
  * ALTER changes only the build mode; RENAME is refused;
  * DROP removes the definition (IF EXISTS tolerated);
  * every misuse is refused at bind with the reason: unknown column, non-text column,
    unknown option, bad build mode, another index method, a partial index.
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

WORKSPACE = "idxws"
TABLE = f"{WORKSPACE}.col.docs"

sys.path.insert(1, os.path.dirname(__file__))
from test_optimize_local import _LocalDiskIO  # noqa: E402


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
    morsel = Morsel()
    morsel.append_vector("id", vector_from_sequence([1, 2, 3], dtype="INTEGER"))
    morsel.append_vector("body", vector_from_sequence(["red planet", "gas giant", "ice"], dtype="VARCHAR"))
    data = write_parquet(morsel, compression="zstd")
    path = f"{location}/data/seed.parquet"
    with open(path, "wb") as f:
        f.write(data)
    entry = build_parquet_manifest_entry_from_bytes(
        data, path, len(data), field_id_by_name={"id": 1, "body": 2}
    ).to_dict()

    class _ManifestWriterCatalog:
        io = disk_io
        write_parquet_manifest = OpteryxCatalog.write_parquet_manifest

        def save_snapshot(self, identifier, snapshot):
            pass

        def save_dataset_metadata(self, identifier, metadata, **kwargs):
            pass

    writer = _ManifestWriterCatalog()
    manifest = writer.write_parquet_manifest(1000, [entry], location)
    meta = DatasetMetadata(
        dataset_identifier=identifier,
        location=location,
        schema=None,
        properties={},
        schemas=[{"schema_id": "s1", "columns": [
            {"id": 1, "name": "id", "type": "INTEGER"},
            {"id": 2, "name": "body", "type": "VARCHAR"},
        ]}],
        current_schema_id="s1",
    )
    meta.snapshots.append(Snapshot(
        snapshot_id=1000, timestamp_ms=1000, author="seed", sequence_number=1,
        user_created=True, operation_type="append", manifest_list=manifest, schema_id="s1",
    ))
    meta.current_snapshot_id = 1000
    ds = SimpleDataset(identifier=identifier, _metadata=meta)
    ds.io = disk_io
    ds.catalog = writer
    return ds


@pytest.fixture
def index_env(tmp_path):
    import opteryx.connectors as connectors
    from opteryx_catalog.catalog.manifest import clear_parsed_manifest_cache
    from opteryx_catalog.catalog.vector_indexes import new_index_definition
    from opteryx_catalog.catalog.vector_indexes import normalize_index_name
    from opteryx_catalog.exceptions import VectorIndexAlreadyExists
    from opteryx_catalog.exceptions import VectorIndexNotFound

    clear_parsed_manifest_cache()
    disk_io = _LocalDiskIO()
    dataset = _build_dataset(str(tmp_path / "docs"), "col.docs", disk_io)
    datasets = {"col.docs": dataset}
    indexes: dict = {}

    class _FakeCatalog:
        def __init__(self, workspace=None, **kwargs):
            self.workspace = workspace
            self.io = disk_io

        def dataset_exists(self, identifier):
            return identifier in datasets

        def load_dataset(self, identifier):
            return datasets[identifier]

        def get_relation(self, identifier):
            if identifier in datasets:
                return "dataset", datasets[identifier]
            return None, None

        def create_vector_index(self, identifier, name, column, *, embedding_identity, dimensions,
                                author, build=None, clusters=0, nprobe=32):
            record = new_index_definition(
                name=name, column=column, method="ivf", metric="cosine", build=build,
                clusters=clusters, nprobe=nprobe, embedding_identity=embedding_identity,
                dimensions=dimensions, author=author, created_at_ms=1,
            )
            if (identifier, record["name"]) in indexes:
                raise VectorIndexAlreadyExists(f"Index {record['name']} already exists")
            indexes[(identifier, record["name"])] = record
            return record

        def get_vector_index(self, identifier, name):
            key = (identifier, normalize_index_name(name))
            if key not in indexes:
                raise VectorIndexNotFound(name)
            return indexes[key]

        def alter_vector_index(self, identifier, name, *, build, author):
            record = self.get_vector_index(identifier, name)
            record["build"] = build
            return record

        def drop_vector_index(self, identifier, name, *, author):
            record = self.get_vector_index(identifier, name)
            datasets[identifier].remove_vector_index_files(record["index-id"], author=author)
            del indexes[(identifier, record["name"])]

        def list_vector_indexes(self, identifier):
            return sorted((r for (i, _), r in indexes.items() if i == identifier), key=lambda r: r["name"])

    saved_default = connectors._default_connector
    saved_prefixes = dict(connectors._storage_prefixes)
    saved_cache = dict(connectors._connector_cache)
    connectors._storage_prefixes.pop(WORKSPACE, None)
    connectors._connector_cache.clear()
    opteryx.set_default_connector(OpteryxConnector, catalog=_FakeCatalog)
    try:
        yield indexes
    finally:
        connectors._default_connector = saved_default
        connectors._storage_prefixes.clear()
        connectors._storage_prefixes.update(saved_prefixes)
        connectors._connector_cache.clear()
        connectors._connector_cache.update(saved_cache)


def _run(sql):
    return list(opteryx.session(user="tester").execute_to_morsels(sql))


def test_create_alter_drop(index_env):
    from opteryx.types.vectors.embedding_capability import active_embedding_capability

    _run(f"CREATE INDEX body_idx ON {TABLE} USING IVF (body)")
    (record,) = index_env.values()
    assert record["name"] == "body_idx" and record["column"] == "body"
    assert record["build"] == "async"                     # the ruled default
    assert record["embedding-identity"] == active_embedding_capability().identity
    assert record["dimensions"] == active_embedding_capability().dimensions

    _run(f"CREATE INDEX IF NOT EXISTS body_idx ON {TABLE} USING IVF (body) WITH (build = 'sync')")
    assert record["build"] == "async"                     # IF NOT EXISTS left it alone
    with pytest.raises(Exception, match="already exists"):
        _run(f"CREATE INDEX body_idx ON {TABLE} USING IVF (body)")

    _run(f"ALTER INDEX body_idx ON {TABLE} SET (build = 'sync')")
    assert record["build"] == "sync"

    _run(f"DROP INDEX body_idx ON {TABLE}")
    assert index_env == {}
    _run(f"DROP INDEX IF EXISTS body_idx ON {TABLE}")
    with pytest.raises(Exception, match="no index"):
        _run(f"DROP INDEX body_idx ON {TABLE}")


def test_create_with_options(index_env):
    _run(f"CREATE INDEX b ON {TABLE} USING IVF (BODY) WITH (build = 'SYNC', clusters = 64, nprobe = 8)")
    (record,) = index_env.values()
    assert (record["build"], record["clusters"], record["nprobe"], record["column"]) == ("sync", 64, 8, "body")


@pytest.mark.parametrize(
    "sql, message",
    [
        (f"CREATE INDEX i ON {TABLE} USING IVF (nope)", "no column"),
        (f"CREATE INDEX i ON {TABLE} USING IVF (id)", "embeds text"),
        (f"CREATE INDEX i ON {TABLE} USING IVF (body) WITH (speed = 1)", "Unknown index option"),
        (f"CREATE INDEX i ON {TABLE} USING IVF (body) WITH (build = 'later')", "'sync' or 'async'"),
        (f"CREATE INDEX i ON {TABLE} USING IVF (body) WITH (nprobe = 0)", "nprobe"),
        (f"CREATE INDEX i ON {TABLE} USING BTREE (body)", "IVF"),
        (f"CREATE INDEX i ON {TABLE} (body)", "IVF"),
        (f"CREATE UNIQUE INDEX i ON {TABLE} USING IVF (body)", "UNIQUE"),
        (f"CREATE INDEX i ON {TABLE} USING IVF (body, id)", "exactly one text column"),
        (f"CREATE INDEX i ON {TABLE} USING IVF (body) WHERE id > 1", "partial"),
        (f"ALTER INDEX i ON {TABLE} SET (build = 'sync')", "no index"),
        ("ALTER INDEX i RENAME TO j", "cannot be renamed"),
        ("DROP INDEX i", "needs the relation"),
    ],
)
def test_refusals(index_env, sql, message):
    with pytest.raises(Exception, match=message):
        _run(sql)
    assert index_env == {}
