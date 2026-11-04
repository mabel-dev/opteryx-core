"""A local-disk catalog for tests: the production catalog stack, minus the services.

`OpteryxConnector` over `opteryx_catalog`'s real `SimpleDataset`s — real manifests,
real snapshots, real merge-on-read delete vectors, real `merge_commit` /
`compaction_commit` — with the metastore (the Firestore/Postgres documents that
name datasets and hold their metadata) replaced by memory, and the FileIO by a
directory on local disk. This is the write path production takes (MERGE, UPDATE,
DELETE, OPTIMIZE, VERSION AS OF); `LocalStoreConnector` is not.

Manifests and schema documents are produced by `OpteryxCatalog`'s own code
(`write_parquet_manifest`, `_schema_to_columns`, `_initial_field_ids`), borrowed
unbound: none of them touches the metastore, so the shapes cannot drift from what
the real catalog writes.

Used by the SLT driver (tests/tools/sqllogictest/opteryx_driver.py) and by the
catalog-backed integration tests, so the two cannot drift either.

    catalog = local_catalog_class(root)          # one shared store under `root`
    opteryx.set_default_connector(OpteryxConnector, catalog=catalog)

The class closes over one in-memory store; every instance the connector creates
(it builds one per workspace) sees the same datasets.
"""

from __future__ import annotations

import os
import shutil
import sys

# The catalog is the sibling checkout on a dev machine (no installed copy - see the
# stale-install trap: an installed opteryx_catalog shadows the sibling) and the
# pip-installed package in CI, where the sibling does not exist.
_CATALOG_REPO = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "..", "..", "opteryx-catalog")
)
if os.path.isdir(_CATALOG_REPO) and _CATALOG_REPO not in sys.path:
    sys.path.insert(1, _CATALOG_REPO)


class LocalDiskIO:
    """Catalog-side FileIO over absolute local paths. `reads` records every path
    asked for as input (tests assert a file was never read back)."""

    def __init__(self):
        self.reads: list = []

    class _In:
        def __init__(self, path):
            self._path = path

        def open(self):
            return open(self._path, "rb")

    class _Out:
        def __init__(self, path):
            self._path = path
            self._chunks: list = []
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

    def new_input(self, path):
        self.reads.append(path)
        return self._In(path)

    def new_output(self, path):
        return self._Out(path)

    def delete(self, path):
        os.remove(path)

    # The listings expiration and deep clean reconcile a dataset's location against.
    def list_files(self, prefix):
        return set(self.list_files_with_stats(prefix))

    def list_files_with_stats(self, prefix):
        """path -> (age in ms, size in bytes) for every file under `prefix`."""
        import time

        now_ms = int(time.time() * 1000)
        out = {}
        for directory, _, names in os.walk(prefix):
            for name in names:
                path = os.path.join(directory, name)
                stat = os.stat(path)
                out[path] = (now_ms - int(stat.st_mtime * 1000), stat.st_size)
        return out


class _MemoryDoc:
    """A metastore document and its subcollections, in memory. Stands in for the
    Firestore refs the borrowed catalog code reaches through (`.get().exists`,
    `.collection(..).document(..)`, `.set`, `.to_dict`)."""

    def __init__(self, exists=False):
        self.exists = exists
        self._data = None
        self._children: dict = {}

    def get(self):
        return self

    def to_dict(self):
        return self._data

    def set(self, data, merge=False):
        self._data = dict(data)
        self.exists = True

    def collection(self, name):
        return self._children.setdefault(name, _MemoryCollection())


class _MemoryCollection:
    def __init__(self):
        self._docs: dict = {}

    def document(self, doc_id):
        return self._docs.setdefault(doc_id, _MemoryDoc())


def local_catalog_class(root: str, disk_io: LocalDiskIO | None = None):
    """A catalog class (what `set_default_connector(..., catalog=...)` takes) whose
    datasets live under `root` on local disk. Returns the class; its `store`
    attribute is the shared {"workspace.collection.dataset": SimpleDataset} map, for tests that seed
    or inspect it directly."""
    from opteryx_catalog.catalog.dataset import SimpleDataset
    from opteryx_catalog.catalog.maintenance_lease import MaintenanceLease
    from opteryx_catalog.catalog.metadata import DatasetMetadata
    from opteryx_catalog.exceptions import DatasetAlreadyExists
    from opteryx_catalog.exceptions import DatasetNotFound
    from opteryx_catalog.exceptions import ForkError
    from opteryx_catalog.opteryx_catalog import OpteryxCatalog

    io = disk_io if disk_io is not None else LocalDiskIO()
    datasets: dict = {}
    leases: dict = {}
    # {qualified upstream: {qualified fork: registry row}} - the upstream's `forks/`
    # subcollection, which expiration reads for pins and drop reads to refuse.
    forks: dict = {}
    # Per-dataset metastore documents beyond existence (the orphan quarantine record).
    documents: dict = {}

    class LocalCatalog:
        # Shared state, visible to every instance the connector creates.
        store = datasets

        # Stands in for the native OpteryxCatalog, whose catalog records ARE its
        # datasets - see opteryx_catalog's `Metastore.serves_catalog_records`.
        serves_catalog_records = True

        # Borrowed from the real catalog: none of these touch the metastore.
        write_parquet_manifest = OpteryxCatalog.write_parquet_manifest
        write_manifest_bytes = OpteryxCatalog.write_manifest_bytes
        open_pending_data_file_writer = OpteryxCatalog.open_pending_data_file_writer
        _initial_field_ids = OpteryxCatalog._initial_field_ids
        _schema_to_columns = OpteryxCatalog._schema_to_columns

        # CREATE TABLE ... CLONE runs the real `clone_dataset` and the name/entry
        # helpers it calls; only its metastore touches are stood in for below.
        supports_forking = True
        clone_dataset = OpteryxCatalog.clone_dataset
        _qualify = OpteryxCatalog._qualify
        _split_qualified = staticmethod(OpteryxCatalog._split_qualified)
        _local_parts = OpteryxCatalog._local_parts
        _load_qualified = OpteryxCatalog._load_qualified
        _upstream_entries = OpteryxCatalog._upstream_entries
        fork_registry_id = OpteryxCatalog.fork_registry_id

        def _dataset_location(self, collection, dataset_name):
            return os.path.join(root, self.workspace or "_", collection, dataset_name)

        # Local locations derive from the name, so the "reservation" a CTAS's
        # pending writers take is just the path create_dataset will use.
        def _reserve_location(self, identifier):
            return self._dataset_location(*identifier.split("."))

        def _is_minted_location(self, location):
            workspace_root = os.path.join(root, self.workspace or "_")
            return os.path.dirname(os.path.dirname(location)) == workspace_root

        def _dataset_doc_ref(self, collection, dataset_name):
            """The dataset's metastore document: it exists iff the name is in the store,
            and its subcollections (the orphan quarantine record) persist across calls."""
            key = f"{self.workspace}.{collection}.{dataset_name}"
            doc = documents.setdefault(key, _MemoryDoc())
            doc.exists = key in datasets
            return doc

        def _catalog_for(self, workspace):
            return self if workspace == self.workspace else type(self)(workspace=workspace)

        def _stored_schema_of(self, identifier):
            """The upstream's current stored columns - what the real one reads from its
            schema document, which here is the dataset's own `schemas` list."""
            metadata = datasets[self._qualify(identifier)].metadata
            (current,) = [
                s for s in metadata.schemas if s["schema_id"] == metadata.current_schema_id
            ]
            return {"columns": current["columns"]}

        # ---- the fork registry on the upstream (FORKS_DESIGN.md S5) ------------
        def register_fork(self, upstream_identifier, fork_identifier, pinned_snapshot_id, author=None):
            forks.setdefault(self._qualify(upstream_identifier), {})[fork_identifier] = {
                "fork": fork_identifier,
                "pinned-snapshot": int(pinned_snapshot_id),
                "created-by": author,
            }

        def deregister_fork(self, upstream_identifier, fork_identifier):
            forks.get(self._qualify(upstream_identifier), {}).pop(fork_identifier, None)

        def list_forks(self, upstream_identifier):
            rows = forks.get(self._qualify(upstream_identifier), {}).values()
            return sorted((dict(row) for row in rows), key=lambda row: row["fork"])

        # No tags: nothing pins a snapshot but the fork registry.
        def list_tags(self, identifier):
            return []

        def _snapshots_collection(self, collection, dataset_name):
            """Expiration's tombstone writes. A tombstoned snapshot is one the real
            loader filters out of `metadata.snapshots`, so that is what `update` does."""
            dataset = datasets[f"{self.workspace}.{collection}.{dataset_name}"]

            class _Snapshots:
                def stream(self):
                    return []  # no tombstone documents persist to purge

                def document(self, snapshot_id):
                    class _Snapshot:
                        def update(self, fields):
                            dataset.metadata.snapshots = [
                                s for s in dataset.metadata.snapshots
                                if str(s.snapshot_id) != snapshot_id
                            ]

                    return _Snapshot()

            return _Snapshots()

        def __init__(self, workspace=None, **kwargs):
            self.workspace = workspace
            self.io = io

        # ---- metastore: memory in place of Firestore/Postgres documents -------
        def save_snapshot(self, identifier, snapshot):
            pass  # the snapshot already lives on the dataset's in-memory metadata

        def save_dataset_metadata(self, identifier, metadata, **kwargs):
            pass  # likewise

        def _key(self, identifier):
            return f"{self.workspace}.{identifier}"

        def dataset_exists(self, identifier):
            return self._key(identifier) in datasets

        def load_dataset(self, identifier, load_history=False):
            key = self._key(identifier)
            if key not in datasets:
                raise DatasetNotFound(identifier)
            return datasets[key]

        def get_relation(self, identifier):
            key = self._key(identifier)
            if key in datasets:
                return "dataset", datasets[key]
            return None, None

        def create_dataset(self, identifier, schema, properties=None, author=None, location=None):
            key, ds = self._new_dataset(identifier, schema, properties, author, location)
            datasets[key] = ds
            return ds

        def create_dataset_with_files(
            self, identifier, schema, *, entries=None, manifest=None, author=None,
            location=None, commit_message=None, read_sources=None, produced_by=None,
            properties=None,
        ):
            # The store entry IS the dataset's visibility: it is published only once
            # the first snapshot is built, mirroring the real catalog's one transaction.
            key, ds = self._new_dataset(identifier, schema, properties, author, location)
            snapshot = ds._stage_add_files(
                entries=entries, manifest=manifest, author=author,
                commit_message=commit_message, read_sources=read_sources,
                produced_by=produced_by,
            )
            datasets[key] = ds
            ds._after_commit(author, snapshot)
            return ds

        def _new_dataset(self, identifier, schema, properties, author, location):
            """A dataset object for `identifier`, not yet published to the store."""
            if author is None:
                raise ValueError("author must be provided when creating a dataset")
            key = self._key(identifier)
            if key in datasets:
                raise DatasetAlreadyExists(f"Dataset already exists: {identifier}")
            collection, name = identifier.split(".")
            expected = self._dataset_location(collection, name)
            if location is not None and location != expected:
                raise ValueError(f"location {location!r} was not reserved for {identifier!r}")
            location = expected
            os.makedirs(os.path.join(location, "data"), exist_ok=True)
            os.makedirs(os.path.join(location, "metadata"), exist_ok=True)
            metadata = DatasetMetadata(
                dataset_identifier=identifier,
                schema=schema,
                location=location,
                properties=properties or {},
            )
            metadata.author = author
            if schema is not None:
                field_ids = self._initial_field_ids(schema)
                if field_ids is not None:
                    metadata.next_field_id = len(field_ids) + 1
                metadata.schemas = [{
                    "schema_id": "s1",
                    "columns": self._schema_to_columns(schema, field_ids=field_ids),
                }]
                metadata.current_schema_id = "s1"
            ds = SimpleDataset(identifier=identifier, _metadata=metadata)
            ds.io = io
            ds.catalog = self
            return key, ds

        def drop_dataset(self, identifier, author=None):
            key = self._key(identifier)
            if key not in datasets:
                raise DatasetNotFound(identifier)
            # The real drop's fork rules: refuse while forks stand on this dataset,
            # and a fork being dropped releases its pin on the upstream.
            standing = self.list_forks(key)
            if standing:
                names = ", ".join(row["fork"] for row in standing)
                raise ForkError(f"Cannot drop {identifier}: it is the upstream of {names}")
            fork = datasets[key].metadata.fork
            if fork is not None:
                self.deregister_fork(fork.source.dataset, key)
            documents.pop(key, None)
            location = datasets.pop(key).metadata.location
            shutil.rmtree(location, ignore_errors=True)

        # No vector indexes: writes build no index files, OPTIMIZE carries none.
        def list_vector_indexes(self, identifier):
            return []

        # No triggers: a commit fires none (opteryx_catalog.trigger_firing asks).
        def list_triggers(self, identifier=None):
            return []

        # ---- the maintenance lease every compaction holds (§5.7) --------------
        def claim_maintenance_lease(self, identifier, *, holder, operation, ttl_seconds):
            key = self._key(identifier)
            if key in leases:
                raise RuntimeError(f"{identifier} already leased")
            leases[key] = MaintenanceLease(
                dataset=identifier, claim_id="local", holder=holder, operation=operation,
                claimed_at_ms=1, expires_at_ms=1 + ttl_seconds * 1000,
            )
            return leases[key]

        def renew_maintenance_lease(self, lease, *, ttl_seconds):
            return lease

        def release_maintenance_lease(self, lease):
            key = self._key(lease.dataset)
            return leases.pop(key, None) is lease

    return LocalCatalog
