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
    from opteryx_catalog.opteryx_catalog import OpteryxCatalog

    io = disk_io if disk_io is not None else LocalDiskIO()
    datasets: dict = {}
    leases: dict = {}

    class LocalCatalog:
        # Shared state, visible to every instance the connector creates.
        store = datasets

        # Borrowed from the real catalog: none of these touch the metastore.
        write_parquet_manifest = OpteryxCatalog.write_parquet_manifest
        open_pending_data_file_writer = OpteryxCatalog.open_pending_data_file_writer
        _initial_field_ids = OpteryxCatalog._initial_field_ids
        _schema_to_columns = OpteryxCatalog._schema_to_columns

        def _dataset_location(self, collection, dataset_name):
            return os.path.join(root, self.workspace or "_", collection, dataset_name)

        def _dataset_doc_ref(self, collection, dataset_name):
            """The one metastore read the borrowed methods make: does the name exist."""
            exists = f"{self.workspace}.{collection}.{dataset_name}" in datasets

            class _Doc:
                def get(self):
                    return self

            doc = _Doc()
            doc.exists = exists
            return doc

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

        def load_dataset(self, identifier):
            key = self._key(identifier)
            if key not in datasets:
                raise DatasetNotFound(identifier)
            return datasets[key]

        def get_relation(self, identifier):
            key = self._key(identifier)
            if key in datasets:
                return "dataset", datasets[key]
            return None, None

        def create_dataset(self, identifier, schema, properties=None, author=None):
            if author is None:
                raise ValueError("author must be provided when creating a dataset")
            key = self._key(identifier)
            if key in datasets:
                raise DatasetAlreadyExists(f"Dataset already exists: {identifier}")
            collection, name = identifier.split(".")
            location = self._dataset_location(collection, name)
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
            datasets[key] = ds
            return ds

        def drop_dataset(self, identifier, author=None):
            key = self._key(identifier)
            if key not in datasets:
                raise DatasetNotFound(identifier)
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
