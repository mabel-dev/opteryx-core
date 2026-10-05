"""CREATE / ALTER / DROP / REFRESH INDEX (vector index) end to end through SQL — local disk, no GCS.

The fake catalog stores definitions in a dict but validates them with the REAL catalog's
`new_index_definition`, so the rules exercised are the catalog's own; the dataset is a real
SimpleDataset, so DROP INDEX runs the real `remove_vector_index_files` commit.
What this protects:
  * CREATE defines with the active embedding identity and width, `async` by default
    (ruled 2026-10-02), honouring WITH options and IF NOT EXISTS;
  * ALTER changes only the build mode; RENAME is refused;
  * DROP removes the definition (IF EXISTS tolerated);
  * REFRESH builds the index files of every uncovered file natively, commits `index-build`
    with the sizes the build wrote, is a no-op once up to date, holds the maintenance lease
    only while it runs and is refused while someone else holds it, and refuses an index
    defined against another embedder;
  * every misuse is refused at bind with the reason: unknown column, non-text column,
    unknown option, bad build mode, another index method, a partial index.
"""

import os
import sys
from types import SimpleNamespace

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
def catalog_env(tmp_path):
    """The fake catalog installed as the default connector: its index definitions, the
    real dataset behind it, and the fake lease's state."""
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
            if identifier in datasets:
                return "dataset", datasets[identifier]
            return None, None

        def create_vector_index(self, identifier, name, column, *, embedding_identity, dimensions,
                                author, build=None, clusters=0):
            record = new_index_definition(
                name=name, column=column, method="ivf", metric="cosine", build=build,
                clusters=clusters, embedding_identity=embedding_identity,
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

        # SHOW CREATE TABLE reads the table's declared relationships; it has none.
        def list_relationships(self, identifier):
            return []

        # The lease's rules are the catalog's (Firestore transactions, tested there); this
        # keeps one claim per dataset and records what happened to it.
        def claim_maintenance_lease(self, identifier, *, holder, operation, ttl_seconds):
            from opteryx_catalog.catalog.maintenance_lease import MaintenanceLease
            from opteryx_catalog.catalog.maintenance_lease import describe_holder
            from opteryx_catalog.exceptions import MaintenanceLeaseHeld

            held = leases.get(identifier)
            if held is not None:
                raise MaintenanceLeaseHeld(f"{identifier} is held for {describe_holder(held.to_document())}")
            lease = MaintenanceLease(
                dataset=identifier, claim_id=f"claim-{len(lease_log)}", holder=holder,
                operation=operation, claimed_at_ms=1, expires_at_ms=1 + ttl_seconds * 1000,
            )
            leases[identifier] = lease
            lease_log.append(("claim", holder, operation))
            return lease

        def renew_maintenance_lease(self, lease, *, ttl_seconds):
            return lease

        def release_maintenance_lease(self, lease):
            if leases.get(lease.dataset) != lease:
                return False
            del leases[lease.dataset]
            lease_log.append(("release", lease.holder, lease.operation))
            return True

    saved_default = connectors._default_connector
    saved_prefixes = dict(connectors._storage_prefixes)
    saved_cache = dict(connectors._connector_cache)
    connectors._storage_prefixes.pop(WORKSPACE, None)
    connectors._connector_cache.clear()
    opteryx.set_default_connector(OpteryxConnector, catalog=_FakeCatalog)
    try:
        yield SimpleNamespace(indexes=indexes, dataset=dataset, leases=leases, lease_log=lease_log)
    finally:
        connectors._default_connector = saved_default
        connectors._storage_prefixes.clear()
        connectors._storage_prefixes.update(saved_prefixes)
        connectors._connector_cache.clear()
        connectors._connector_cache.update(saved_cache)


@pytest.fixture
def index_env(catalog_env):
    return catalog_env.indexes


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
    _run(f"CREATE INDEX b ON {TABLE} USING IVF (BODY) WITH (build = 'SYNC', clusters = 64)")
    (record,) = index_env.values()
    assert (record["build"], record["clusters"], record["column"]) == ("sync", 64, "body")
    assert "nprobe" not in record                         # a session setting only (2026-10-03)


@pytest.mark.parametrize(
    "sql, message",
    [
        (f"CREATE INDEX i ON {TABLE} USING IVF (nope)", "no column"),
        (f"CREATE INDEX i ON {TABLE} USING IVF (id)", "embeds text"),
        (f"CREATE INDEX i ON {TABLE} USING IVF (body) WITH (speed = 1)", "Unknown index option"),
        (f"CREATE INDEX i ON {TABLE} USING IVF (body) WITH (build = 'later')", "'sync' or 'async'"),
        (f"CREATE INDEX i ON {TABLE} USING IVF (body) WITH (nprobe = 8)", "Unknown index option"),
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


# --- REFRESH INDEX (D-16) ---------------------------------------------------


def _messages(sql):
    session = opteryx.session(user="tester")
    list(session.execute_to_morsels(sql))
    return session.messages


def _index_refs(dataset):
    from opteryx_catalog.catalog.manifest import get_parsed_manifest
    from opteryx_catalog.catalog.vector_indexes import index_refs

    entries = get_parsed_manifest(dataset.io, dataset.snapshot(None).manifest_list)
    return {e["file_path"]: index_refs(e) for e in entries}


def _indexed_ordinals(path):
    sys.path.insert(1, os.path.join(os.path.dirname(__file__), "..", "unit", "core"))
    from test_vector_index_build import stored_vectors

    return sorted(stored_vectors(path))


def test_refresh_builds_commits_and_is_then_a_no_op(catalog_env):
    _run(f"CREATE INDEX body_idx ON {TABLE} USING IVF (body)")
    (record,) = catalog_env.indexes.values()
    dataset = catalog_env.dataset

    assert _messages(f"REFRESH INDEX body_idx ON {TABLE}") == [
        f"refreshed index `body_idx` on `{TABLE}`: 1 file(s) indexed"
    ]
    assert dataset.snapshot(None).operation_type == "index-build"
    (data_file, refs), = _index_refs(dataset).items()
    files = refs[record["index-id"]]
    # the sizes committed are the file the build wrote
    assert os.path.getsize(files.path) == files.file_bytes
    assert 0 < files.footer_bytes < files.file_bytes
    assert files.path.startswith(f"{dataset.metadata.location}/index/{record['index-id']}/")
    assert _indexed_ordinals(files.path) == [0, 1, 2]
    summary = dataset.snapshot(None).summary
    assert summary["total-index-files"] == 1
    assert summary["total-index-size"] == files.file_bytes

    head = dataset.metadata.current_snapshot_id
    assert _messages(f"REFRESH INDEX body_idx ON {TABLE}") == [
        f"refreshed index `body_idx` on `{TABLE}`: 0 file(s) indexed"
    ]
    assert dataset.metadata.current_snapshot_id == head          # nothing to commit
    # the lease was held for each run and released after it
    assert catalog_env.lease_log == [
        ("claim", "REFRESH INDEX body_idx by tester", "index-build"),
        ("release", "REFRESH INDEX body_idx by tester", "index-build"),
    ] * 2
    assert catalog_env.leases == {}


def test_refresh_leaves_out_rows_deleted_at_plan_time(catalog_env):
    _run(f"CREATE INDEX body_idx ON {TABLE} USING IVF (body)")
    (record,) = catalog_env.indexes.values()
    dataset = catalog_env.dataset
    (data_file,) = _index_refs(dataset)
    dataset.delete_rows({data_file: [1]}, author="tester")

    _run(f"REFRESH INDEX body_idx ON {TABLE}")
    files = _index_refs(dataset)[data_file][record["index-id"]]
    assert _indexed_ordinals(files.path) == [0, 2]            # physical ordinals kept


def test_refresh_is_refused_while_another_holds_the_lease(catalog_env):
    from opteryx_catalog.catalog.maintenance_lease import MaintenanceLease

    _run(f"CREATE INDEX body_idx ON {TABLE} USING IVF (body)")
    head = catalog_env.dataset.metadata.current_snapshot_id
    catalog_env.leases["col.docs"] = MaintenanceLease(
        dataset="col.docs", claim_id="other", holder="OPTIMIZE by someone", operation="compaction",
        claimed_at_ms=1, expires_at_ms=10**15,
    )
    with pytest.raises(Exception, match="compaction by OPTIMIZE by someone"):
        _run(f"REFRESH INDEX body_idx ON {TABLE}")
    assert catalog_env.dataset.metadata.current_snapshot_id == head
    assert catalog_env.leases["col.docs"].claim_id == "other"     # left alone


def test_refresh_refuses_an_index_defined_against_another_embedder(catalog_env):
    _run(f"CREATE INDEX body_idx ON {TABLE} USING IVF (body)")
    (record,) = catalog_env.indexes.values()
    record["embedding-identity"] = "minilm-l6-v2:256:sha256:" + "0" * 64
    with pytest.raises(Exception, match="defined against the embedder"):
        _run(f"REFRESH INDEX body_idx ON {TABLE}")
    assert catalog_env.lease_log == []                            # refused before the lease


def test_refresh_of_an_unknown_index_is_refused(catalog_env):
    with pytest.raises(Exception, match="no index"):
        _run(f"REFRESH INDEX nope ON {TABLE}")


@pytest.mark.parametrize(
    "sql",
    ["REFRESH INDEX body_idx", f"REFRESH INDEX body_idx ON {TABLE} WITH (x = 1)", "REFRESH INDEX ON t"],
)
def test_malformed_refresh_is_refused_by_name(catalog_env, sql):
    with pytest.raises(Exception, match=r"REFRESH INDEX\*\* <name> \*\*ON\*\* <relation>"):
        _run(sql)


# --- sync CREATE INDEX (D-7): builds before it returns, under the lease -------------------


def test_sync_create_builds_every_file_before_returning(catalog_env):
    assert _messages(f"CREATE INDEX body_idx ON {TABLE} USING IVF (body) WITH (build = 'sync')") == [
        f"created index `body_idx` on `{TABLE}`: 1 file(s) indexed"
    ]
    (record,) = catalog_env.indexes.values()
    (refs,) = _index_refs(catalog_env.dataset).values()
    assert _indexed_ordinals(refs[record["index-id"]].path) == [0, 1, 2]
    assert [event for event, *_ in catalog_env.lease_log] == ["claim", "release"]
    assert catalog_env.lease_log[0][1:] == ("CREATE INDEX body_idx by tester", "index-build")


def test_async_create_builds_nothing_and_takes_no_lease(catalog_env):
    assert _messages(f"CREATE INDEX body_idx ON {TABLE} USING IVF (body)") == [
        f"created index `body_idx` on `{TABLE}`"
    ]
    assert list(_index_refs(catalog_env.dataset).values()) == [{}]
    assert catalog_env.lease_log == []


def test_sync_create_without_the_lease_creates_nothing(catalog_env):
    from opteryx_catalog.catalog.maintenance_lease import MaintenanceLease

    catalog_env.leases["col.docs"] = MaintenanceLease(
        dataset="col.docs", claim_id="other", holder="OPTIMIZE by someone", operation="compaction",
        claimed_at_ms=1, expires_at_ms=10**15,
    )
    with pytest.raises(Exception, match="compaction by OPTIMIZE by someone"):
        _run(f"CREATE INDEX body_idx ON {TABLE} USING IVF (body) WITH (build = 'sync')")
    assert catalog_env.indexes == {}


def test_a_failed_sync_build_leaves_no_index(catalog_env, monkeypatch):
    import opteryx.operators._operators as operators

    def _fail(*args, **kwargs):
        raise RuntimeError("vector index build: injected failure")

    monkeypatch.setattr(operators, "build_vector_index_local", _fail)
    head = catalog_env.dataset.metadata.current_snapshot_id
    with pytest.raises(RuntimeError, match="injected failure"):
        _run(f"CREATE INDEX body_idx ON {TABLE} USING IVF (body) WITH (build = 'sync')")
    assert catalog_env.indexes == {}
    assert catalog_env.dataset.metadata.current_snapshot_id == head
    assert catalog_env.leases == {}                                   # released on failure


# --- sync indexes build inside the write (D-7): the new file is indexed in its own commit --


def _texts_of(path):
    from rugo.parquet import read_parquet

    texts = []
    for morsel in read_parquet(path, columns=["body"]):
        texts.extend(morsel.column("body").to_pylist())
    return texts


def test_insert_into_a_sync_indexed_table_indexes_the_new_file_in_the_same_commit(catalog_env):
    _run(f"CREATE INDEX body_idx ON {TABLE} USING IVF (body) WITH (build = 'sync')")
    (record,) = catalog_env.indexes.values()
    before = set(_index_refs(catalog_env.dataset))

    _run(f"INSERT INTO {TABLE} (id, body) VALUES (4, 'dust storm'), (5, NULL), (6, 'ring of ice')")

    snap = catalog_env.dataset.snapshot(None)
    assert snap.operation_type == "add-files"                   # one commit, no index-build after it
    refs = _index_refs(catalog_env.dataset)
    (added,) = set(refs) - before
    files = refs[added][record["index-id"]]
    texts = _texts_of(added)
    assert [texts[o] for o in _indexed_ordinals(files.path)] == ["dust storm", "ring of ice"]
    assert os.path.getsize(files.path) == files.file_bytes
    # A write takes no maintenance lease: it indexes only its own new files.
    assert [e for e, *_ in catalog_env.lease_log] == ["claim", "release"]   # the CREATE's only


def test_update_of_a_sync_indexed_table_indexes_the_rewritten_rows(catalog_env):
    _run(f"CREATE INDEX body_idx ON {TABLE} USING IVF (body) WITH (build = 'sync')")
    (record,) = catalog_env.indexes.values()
    before = set(_index_refs(catalog_env.dataset))

    _run(f"UPDATE {TABLE} SET body = 'frozen moon' WHERE id = 3")

    refs = _index_refs(catalog_env.dataset)
    (added,) = set(refs) - before
    files = refs[added][record["index-id"]]
    texts = _texts_of(added)
    assert [texts[o] for o in _indexed_ordinals(files.path)] == ["frozen moon"]


def test_insert_into_an_async_indexed_table_leaves_the_new_file_to_refresh(catalog_env):
    _run(f"CREATE INDEX body_idx ON {TABLE} USING IVF (body)")
    before = set(_index_refs(catalog_env.dataset))
    _run(f"INSERT INTO {TABLE} (id, body) VALUES (4, 'dust storm')")
    refs = _index_refs(catalog_env.dataset)
    (added,) = set(refs) - before
    assert refs[added] == {}


# --- discovery (C4, §7A, D-15) ------------------------------------------------


def _table(sql):
    out = []
    for morsel in opteryx.session(user="tester").execute_to_morsels(sql):
        morsel.materialize()
        names = morsel.column_names
        keys = [n.decode() if type(n) is bytes else n for n in names]
        out.extend(dict(zip(keys, row)) for row in zip(*[morsel.column(c).to_pylist() for c in names]))
    return out


def test_show_indexes_lists_each_index_with_its_coverage(catalog_env):
    assert _table(f"SHOW INDEXES FROM {TABLE}") == []
    _run(f"CREATE INDEX lag_idx ON {TABLE} USING IVF (body)")
    _run(f"CREATE INDEX body_idx ON {TABLE} USING IVF (body) WITH (build = 'sync', clusters = 2)")
    sync, lagging = _table(f"SHOW INDEXES FROM {TABLE}")      # in name order
    files = sum(f.logical_bytes for refs in _index_refs(catalog_env.dataset).values() for f in refs.values())
    assert (sync["name"], sync["column"], sync["method"], sync["metric"], sync["build"], sync["clusters"]) == (
        "body_idx", "body", "ivf", "cosine", "sync", 2,
    )
    assert (sync["files_indexed"], sync["files_total"], sync["index_bytes"]) == (1, 1, files)
    assert sync["index_bytes"] > 0 and sync["created_by"] == "tester" and sync["embedding"]
    assert sync["created_at"] is not None
    # An async index nothing has refreshed yet: its lag is visible.
    assert (lagging["name"], lagging["build"], lagging["files_indexed"], lagging["files_total"]) == (
        "lag_idx", "async", 0, 1,
    )
    assert lagging["index_bytes"] == 0


@pytest.mark.parametrize(
    "sql",
    [
        f"SHOW INDEX FROM {TABLE}",
        f"SHOW INDEXES ON {TABLE}",
        f"SHOW KEYS FROM {TABLE}",
        "SHOW INDEXES",
    ],
)
def test_show_indexes_has_one_spelling(catalog_env, sql):
    with pytest.raises(Exception, match="SHOW INDEXES FROM <table>"):
        _run(sql)


def test_show_indexes_outside_the_catalog_is_refused(catalog_env):
    with pytest.raises(Exception, match="no indexes to show|cannot show"):
        _run("SHOW INDEXES FROM $planets")


def test_show_create_table_recreates_the_indexes(catalog_env):
    _run(f"CREATE INDEX body_idx ON {TABLE} USING IVF (body)")
    _run(f"CREATE INDEX b2 ON {TABLE} USING IVF (body) WITH (build = 'sync', clusters = 7)")
    ((_, ddl),) = [tuple(row.values()) for row in _table(f"SHOW CREATE TABLE {TABLE}")]
    statements = [s.strip() for s in ddl.rstrip(";").split(";\n\n")]
    assert statements[0].startswith("CREATE TABLE")
    assert statements[1:] == [
        f"CREATE INDEX b2 ON {TABLE} USING IVF (body) WITH (build = 'sync', clusters = 7)",
        f"CREATE INDEX body_idx ON {TABLE} USING IVF (body) WITH (build = 'async')",
    ]
