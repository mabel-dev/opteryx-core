# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Local file-based storage connector implementing Writable interface.
"""

import json
import os
import re
import shutil
from datetime import datetime, timezone
from typing import Dict, List, Optional

import opteryx
from opteryx.connectors import TableType
from opteryx.connectors.base.base_connector import BaseConnector, BaseTable
from opteryx.connectors.capabilities import Eidetic
from opteryx.connectors.capabilities import Writable
from opteryx.connectors.capabilities.eidetic import ViewDefinition
from opteryx.exceptions import ConcurrentModificationError, DatasetNotFoundError
from opteryx.models.dataset_descriptor import DatasetDescriptor
from opteryx.models.file_entry import FileEntry
from opteryx.models.manifest import Manifest
from opteryx.models.manifest_io import read_manifest_file_entries
from opteryx.models.manifest_io import write_manifest_parquet
from opteryx.types.schema import RelationSchema


def _now_utc_iso() -> str:
    """Get current UTC time as ISO 8601 string (microsecond precision)."""
    return datetime.now(timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z")


def _ts_for_filename(iso: str) -> str:
    """Convert ISO 8601 timestamp to filename-safe format.

    Replaces : and . with - for use in filenames.
    """
    return iso.replace(":", "-").replace(".", "-")


# Capability mixins come BEFORE BaseConnector: BaseConnector defines the capability flags
# as False defaults, so a mixin listed after it is shadowed by MRO and its capability is
# silently OFF. That is what disabled views on this connector — CREATE VIEW wrote a
# view.json that could never be read back. See tests/unit/connectors/test_capability_flags.py.
class LocalStoreConnector(Eidetic, Writable, BaseConnector):
    """Local file-based storage connector.

    Stores relations as JSON metadata + Parquet data files organized by relation name.
    Implements optimistic concurrency control for snapshot commits.

    Views are stored as a single JSON definition file at the same directory
    identity as a relation would use; a name is exclusively a table or a view.

    Filesystem layout:
        {store_root}/{schema}/{table}/dataset.json
        {store_root}/{schema}/{table}/snapshot-{ts}.json      (small pointer: see below)
        {store_root}/{schema}/{table}/manifest-{ts}.parquet   (the file list + stats)
        {store_root}/{schema}/{table}/data-{uuid}.parquet
        {store_root}/{schema}/{view}/view.json

    The manifest (file list + per-file stats/sketches) is a Parquet file in the
    SAME format opteryx_catalog's manifest is — see opteryx.models.manifest_io.
    One manifest format everywhere, read/written natively, no JSON-boxed file
    list. snapshot-{ts}.json is a tiny commit-log pointer only: format_version,
    created_at, parent_snapshot (unchanged OCC bookkeeping), and manifest_file
    (the sibling Parquet manifest's name).
    """

    __mode__ = "Blob"

    supports_predicate_pushdown = False
    supports_limit_pushdown = False
    # No Iceberg-style field-id lineage to preserve, unlike the catalog
    # connector - REPLACE can freely change schema here.
    supports_schema_evolution_on_replace = True

    def __init__(self, store_root: Optional[str] = None, **kwargs):
        """Initialize LocalStoreConnector.

        Args:
            store_root: Root directory for storage. Defaults to config.LOCAL_STORE_ROOT.
            **kwargs: Additional configuration parameters (telemetry, prefix, etc.)
                accepted from connector_factory and ignored. Matches the pattern
                used by FileSystemConnector — BaseConnector has no __init__.
        """
        self.store_root = store_root or opteryx.config.LOCAL_STORE_ROOT
        # For Stage 1, we don't implement read path
        self._pre_commit_recheck_hook = None

    def _validate_relation_name(self, relation_name: str) -> None:
        """Validate relation name format.

        Rejects empty parts, slashes, backslashes, or non-identifier characters.

        Args:
            relation_name: Relation name to validate

        Raises:
            ValueError: If relation name is invalid
        """
        if not relation_name:
            raise ValueError("invalid relation name: empty string")

        parts = relation_name.split(".")
        for part in parts:
            if not part:
                raise ValueError(f"invalid relation name: {relation_name}")
            if not re.match(r"^[A-Za-z_][A-Za-z0-9_]*$", part):
                raise ValueError(f"invalid relation name: {relation_name}")

    def _relation_dir(self, relation_name: str) -> str:
        """Compute directory path for a relation.

        Args:
            relation_name: Fully-qualified relation name (e.g., "a.b.c.events")

        Returns:
            Absolute or relative path to relation directory
        """
        parts = relation_name.split(".")
        return os.path.join(self.store_root, *parts)

    def _read_dataset_json(self, relation_dir: str) -> Optional[DatasetDescriptor]:
        """Read dataset.json from relation directory.

        Args:
            relation_dir: Path to relation directory

        Returns:
            DatasetDescriptor if dataset.json exists, None otherwise
        """
        dataset_path = os.path.join(relation_dir, "dataset.json")
        if not os.path.isfile(dataset_path):
            return None
        with open(dataset_path, "r") as f:
            data = json.load(f)
        return DatasetDescriptor.from_dict(data)

    def _read_snapshot(self, relation_dir: str, snapshot_name: Optional[str]) -> dict:
        """Read the snapshot commit-log pointer.

        Args:
            relation_dir: Path to relation directory
            snapshot_name: Filename of snapshot (e.g., "snapshot-2026-05-06T12-34-56-123456Z.json")

        Returns:
            Small dict: format_version, created_at, parent_snapshot, manifest_file
            (the sibling Parquet manifest's name; None entries have no manifest).

        Raises:
            FileNotFoundError: If snapshot doesn't exist
        """
        if snapshot_name is None:
            return {"manifest_file": None}
        snapshot_path = os.path.join(relation_dir, snapshot_name)
        with open(snapshot_path, "r") as f:
            return json.load(f)

    def _read_current_file_entries(
        self, relation_dir: str, descriptor: DatasetDescriptor
    ) -> List[FileEntry]:
        """Resolve a dataset's current file list via its snapshot pointer + manifest Parquet."""
        snapshot = self._read_snapshot(relation_dir, descriptor.current_snapshot)
        manifest_file = snapshot.get("manifest_file")
        if not manifest_file:
            return []
        with open(os.path.join(relation_dir, manifest_file), "rb") as f:
            manifest_bytes = f.read()
        entries, _native = read_manifest_file_entries(manifest_bytes)
        return entries

    def create_relation(
        self, relation_name: str, schema: RelationSchema, author: Optional[str] = None
    ) -> None:
        """Create a new relation.

        `author` is accepted for the Writable contract but not recorded - dataset.json
        carries no attribution field, unlike view.json's `owner`.

        Args:
            relation_name: Fully-qualified relation name
            schema: RelationSchema for the table
            author: session user, unused by this store

        Raises:
            ValueError: If relation already exists or name is invalid
        """
        self._validate_relation_name(relation_name)
        relation_dir = self._relation_dir(relation_name)

        if os.path.isfile(os.path.join(relation_dir, "dataset.json")):
            raise ValueError(f"relation already exists: {relation_name}")
        if os.path.isfile(os.path.join(relation_dir, "view.json")):
            raise ValueError(f"view already exists: {relation_name}")

        os.makedirs(relation_dir, exist_ok=True)

        descriptor = DatasetDescriptor(
            format_version=1,
            relation_name=relation_name,
            schema=schema,
            current_snapshot=None,
            created_at=_now_utc_iso(),
        )

        dataset_path = os.path.join(relation_dir, "dataset.json")
        tmp_path = dataset_path + ".tmp"
        with open(tmp_path, "w") as f:
            json.dump(descriptor.to_dict(), f)
        os.replace(tmp_path, dataset_path)

    def drop_relation(
        self, relation_name: str, if_exists: bool = False, author: Optional[str] = None
    ) -> None:
        """Drop a relation.

        Args:
            relation_name: Fully-qualified relation name
            if_exists: If True, don't raise error if relation doesn't exist
            author: session user, unused by this store (see create_relation)

        Raises:
            ValueError: If relation doesn't exist and if_exists is False
        """
        self._validate_relation_name(relation_name)
        relation_dir = self._relation_dir(relation_name)

        if not os.path.isfile(os.path.join(relation_dir, "dataset.json")):
            if if_exists:
                return
            raise ValueError(f"relation does not exist: {relation_name}")

        shutil.rmtree(relation_dir)

    def truncate_relation(self, relation_name: str, author: Optional[str] = None) -> None:
        """Truncate a relation (remove all rows).

        Args:
            relation_name: Fully-qualified relation name
            author: session user, unused by this store (see create_relation)

        Raises:
            ValueError: If relation doesn't exist
        """
        self._validate_relation_name(relation_name)
        relation_dir = self._relation_dir(relation_name)

        if not os.path.isfile(os.path.join(relation_dir, "dataset.json")):
            raise ValueError(f"relation does not exist: {relation_name}")

        self._commit(relation_name, [])

    def relation_exists(self, relation_name: str) -> bool:
        """Check if a relation exists.

        Args:
            relation_name: Fully-qualified relation name

        Returns:
            True if relation exists, False otherwise
        """
        relation_dir = self._relation_dir(relation_name)
        return os.path.isfile(os.path.join(relation_dir, "dataset.json"))

    def _view_path(self, view_name: str) -> str:
        """Compute path to a view's definition file.

        Args:
            view_name: Fully-qualified view name (e.g., "a.b.c.my_view")

        Returns:
            Absolute or relative path to the view's view.json
        """
        return os.path.join(self._relation_dir(view_name), "view.json")

    def view_exists(self, view_name: str) -> bool:
        """Check if a view exists.

        Args:
            view_name: Fully-qualified view name

        Returns:
            True if the view exists, False otherwise
        """
        return os.path.isfile(self._view_path(view_name))

    def locate_object(self, name: str):
        """Determine if a name refers to a table or view managed by this connector."""
        if self.relation_exists(name):
            return (TableType.Table, None)
        if self.view_exists(name):
            return (TableType.View, None)
        return (None, None)

    def table_engine(self, name: str, telemetry=None, **kwargs):
        """Create a transient table reader for the named relation."""
        return LocalStoreTable(
            dataset=name,
            store_root=self.store_root,
            telemetry=telemetry,
        )

    def write_morsel(self, relation_name: str, morsel) -> FileEntry:
        """Write a morsel as a parquet file into the relation's directory.

        Creates the directory if needed - this runs before create_relation/
        replace_relation (deferred to EOS for atomicity), so the relation's
        catalog entry (dataset.json) does not exist yet when the first morsel
        arrives. An empty directory with no dataset.json is invisible to
        relation_exists() and everything else, so this doesn't compromise
        atomicity - only a dataset.json write makes a relation "exist".
        """
        from opteryx.connectors.parquet_io.parquet_writer import write_morsel as _write_morsel

        relation_dir = self._relation_dir(relation_name)
        os.makedirs(relation_dir, exist_ok=True)
        return _write_morsel(morsel, relation_dir)

    def insert(
        self,
        relation_name: str,
        file_entries: List[FileEntry],
        author: Optional[str] = None,
    ) -> None:
        """Commit pre-written data files into a new snapshot.

        Args:
            relation_name: Fully-qualified relation name
            file_entries: List of FileEntry objects to append to the relation
            author: session user, unused by this store (see create_relation)

        Raises:
            ValueError: If relation doesn't exist
            ConcurrentModificationError: If relation was modified concurrently
        """
        self._validate_relation_name(relation_name)
        relation_dir = self._relation_dir(relation_name)

        if not os.path.isfile(os.path.join(relation_dir, "dataset.json")):
            raise ValueError(f"relation does not exist: {relation_name}")

        # Read current file list and append new entries
        base_descriptor = self._read_dataset_json(relation_dir)
        current_files = self._read_current_file_entries(relation_dir, base_descriptor)

        new_files = current_files + file_entries
        self._commit(relation_name, new_files)

    def replace_relation(
        self,
        relation_name: str,
        schema: RelationSchema,
        file_entries: List[FileEntry],
        author: Optional[str] = None,
    ) -> None:
        """Atomically replace a relation's entire contents with the given files,
        as a single new snapshot (CREATE OR REPLACE ... AS SELECT).

        Unlike the catalog connector, schema CAN change here - there's no
        Iceberg-style field-id lineage to preserve, so this is the path that
        exercises or_replace's schema-changing case in tests.

        Args:
            relation_name: Fully-qualified relation name
            schema: RelationSchema the new data conforms to (may differ from current)
            file_entries: List of FileEntry objects that become the relation's entire contents
            author: session user, unused by this store (see create_relation)

        Raises:
            ValueError: If relation doesn't exist
            ConcurrentModificationError: If relation was modified concurrently
        """
        self._validate_relation_name(relation_name)
        relation_dir = self._relation_dir(relation_name)

        if not os.path.isfile(os.path.join(relation_dir, "dataset.json")):
            raise ValueError(f"relation does not exist: {relation_name}")

        self._commit(relation_name, file_entries, schema=schema)

    def relation_column_names(self, relation_name: str) -> List[str]:
        """Return the relation's current column names only (not full type fidelity)."""
        relation_dir = self._relation_dir(relation_name)
        descriptor = self._read_dataset_json(relation_dir)
        if descriptor is None:
            raise ValueError(f"relation does not exist: {relation_name}")
        return [c.name for c in descriptor.schema.columns]

    def _commit(
        self,
        relation_name: str,
        new_files: List[FileEntry],
        schema: Optional[RelationSchema] = None,
    ) -> None:
        """Optimistic concurrency control commit protocol.

        Atomically updates the relation's current snapshot, guarding against concurrent
        modifications via re-check before the final dataset.json write.

        Args:
            relation_name: Fully-qualified relation name
            new_files: Complete list of files for the new snapshot
            schema: Schema for the new snapshot; defaults to the relation's current
                schema (pass explicitly only when the schema is changing, e.g. REPLACE)

        Raises:
            ConcurrentModificationError: If relation was modified concurrently
        """
        relation_dir = self._relation_dir(relation_name)

        # Step 1: Read base descriptor and capture current snapshot
        base_descriptor = self._read_dataset_json(relation_dir)
        base_snapshot = base_descriptor.current_snapshot
        new_schema = schema if schema is not None else base_descriptor.schema

        # Step 2: Build the manifest Parquet (the file list + stats) and its
        # tiny commit-log pointer. created_at is shared between both filenames
        # so they visibly pair up on disk.
        created_at = _now_utc_iso()
        ts = _ts_for_filename(created_at)

        manifest_bytes = write_manifest_parquet(new_files, new_schema)

        manifest_name = f"manifest-{ts}.parquet"
        counter = 1
        manifest_base = manifest_name
        while os.path.isfile(os.path.join(relation_dir, manifest_name)):
            manifest_name = f"{manifest_base[:-8]}-{counter}.parquet"
            counter += 1

        manifest_path = os.path.join(relation_dir, manifest_name)
        manifest_tmp = manifest_path + ".tmp"
        with open(manifest_tmp, "wb") as f:
            f.write(manifest_bytes)
        os.replace(manifest_tmp, manifest_path)

        new_snapshot_dict = {
            "format_version": 1,
            "created_at": created_at,
            "parent_snapshot": base_snapshot,
            "manifest_file": manifest_name,
        }

        # Step 3: Choose snapshot name with collision guard
        snapshot_name = f"snapshot-{ts}.json"
        counter = 1
        snapshot_base = snapshot_name
        while os.path.isfile(os.path.join(relation_dir, snapshot_name)):
            # Same-millisecond collision: append counter
            snapshot_name = f"{snapshot_base[:-5]}-{counter}.json"
            counter += 1

        # Step 4: Atomic snapshot write
        snapshot_path = os.path.join(relation_dir, snapshot_name)
        snapshot_tmp = snapshot_path + ".tmp"
        with open(snapshot_tmp, "w") as f:
            json.dump(new_snapshot_dict, f)
        os.replace(snapshot_tmp, snapshot_path)

        # Step 5: Call hook if present (for testing concurrent modification)
        if self._pre_commit_recheck_hook:
            self._pre_commit_recheck_hook()

        # Step 6: Re-check dataset.json before final write
        check_descriptor = self._read_dataset_json(relation_dir)
        if check_descriptor.current_snapshot != base_snapshot:
            try:
                os.remove(snapshot_path)
            except OSError:
                pass
            try:
                os.remove(manifest_path)
            except OSError:
                pass
            raise ConcurrentModificationError(
                f"relation {relation_name} was modified concurrently"
            )

        # Step 7: Build and write new descriptor
        new_descriptor = DatasetDescriptor(
            format_version=base_descriptor.format_version,
            relation_name=base_descriptor.relation_name,
            schema=new_schema,
            current_snapshot=snapshot_name,
            created_at=base_descriptor.created_at,
        )

        # Step 8: Atomic descriptor write
        dataset_path = os.path.join(relation_dir, "dataset.json")
        dataset_tmp = dataset_path + ".tmp"
        with open(dataset_tmp, "w") as f:
            json.dump(new_descriptor.to_dict(), f)
        os.replace(dataset_tmp, dataset_path)

    # View operations (Eidetic capability)

    def get_view(self, view_name: str) -> ViewDefinition:
        """Retrieve the definition of the specified view."""
        view_path = self._view_path(view_name)
        if not os.path.isfile(view_path):
            raise DatasetNotFoundError(dataset=view_name, connector=self.__class__.__name__)

        with open(view_path, "r") as f:
            data = json.load(f)
        return ViewDefinition(
            name=data["name"],
            statement=data["statement"],
            owner=data.get("owner"),
            last_row_count=data.get("last_row_count"),
            description=data.get("description"),
            describer=data.get("describer"),
        )

    def list_views(self, prefix: Optional[str] = None) -> List[ViewDefinition]:
        """List all views managed by this connector, optionally under a name prefix."""
        search_root = self.store_root if not prefix else self._relation_dir(prefix)
        if not os.path.isdir(search_root):
            return []

        views = []
        for dirpath, _dirnames, filenames in os.walk(search_root):
            if "view.json" not in filenames:
                continue
            with open(os.path.join(dirpath, "view.json"), "r") as f:
                data = json.load(f)
            views.append(
                ViewDefinition(
                    name=data["name"],
                    statement=data["statement"],
                    owner=data.get("owner"),
                    last_row_count=data.get("last_row_count"),
                    description=data.get("description"),
                    describer=data.get("describer"),
                )
            )
        return views

    def create_view(
        self,
        view_name: str,
        statement: str,
        update_if_exists: bool = False,
        owner: Optional[str] = None,
    ) -> None:
        """Create (or replace) a view definition.

        Args:
            view_name: Fully-qualified view name
            statement: SQL statement defining the view
            update_if_exists: If True, overwrite an existing view definition
            owner: Optional owner attribution

        Raises:
            ValueError: If the name is already used by a relation, or the view
                already exists and update_if_exists is False
        """
        self._validate_relation_name(view_name)
        relation_dir = self._relation_dir(view_name)
        view_path = os.path.join(relation_dir, "view.json")

        if os.path.isfile(os.path.join(relation_dir, "dataset.json")):
            raise ValueError(f"relation already exists: {view_name}")
        if os.path.isfile(view_path) and not update_if_exists:
            raise ValueError(f"view already exists: {view_name}")

        os.makedirs(relation_dir, exist_ok=True)

        definition = {
            "format_version": 1,
            "name": view_name,
            "statement": statement,
            "owner": owner,
            "last_row_count": None,
            "description": None,
            "describer": None,
            "created_at": _now_utc_iso(),
        }

        tmp_path = view_path + ".tmp"
        with open(tmp_path, "w") as f:
            json.dump(definition, f)
        os.replace(tmp_path, view_path)

    def drop_view(self, view_name: str, author: Optional[str] = None) -> None:
        """Drop the specified view.

        Args:
            view_name: Fully-qualified view name
            author: session user, unused by this store (see create_relation)

        Raises:
            ValueError: If the view doesn't exist
        """
        view_path = self._view_path(view_name)
        if not os.path.isfile(view_path):
            raise ValueError(f"view does not exist: {view_name}")

        os.remove(view_path)
        relation_dir = self._relation_dir(view_name)
        if not os.listdir(relation_dir):
            os.rmdir(relation_dir)


class LocalStoreTable(BaseTable):
    """Transient reader for a LocalStoreConnector-managed relation."""

    __mode__ = "Blob"
    __synchronousity__ = "synchronous"

    supports_predicate_pushdown = False
    supports_limit_pushdown = False
    supports_async = False

    def __init__(self, dataset, store_root, telemetry=None, **kwargs):
        BaseTable.__init__(self, dataset=dataset, telemetry=telemetry, **kwargs)
        self.store_root = store_root
        self.__type__ = "LOCAL_STORE"
        self.schema = None
        self.manifest = None

    def _relation_dir(self) -> str:
        parts = self.dataset.split(".")
        return os.path.join(self.store_root, *parts)

    def get_dataset_schema(self) -> RelationSchema:
        if self.schema is not None:
            return self.schema
        schema, _ = self.get_dataset_metadata()
        return schema

    def get_dataset_metadata(self):
        if self.schema is not None and self.manifest is not None:
            return self.schema, self.manifest

        relation_dir = self._relation_dir()
        dataset_path = os.path.join(relation_dir, "dataset.json")
        if not os.path.isfile(dataset_path):
            raise DatasetNotFoundError(dataset=self.dataset, connector=self.__type__)

        with open(dataset_path, "r") as f:
            descriptor_dict = json.load(f)
        descriptor = DatasetDescriptor.from_dict(descriptor_dict)
        self.schema = descriptor.schema

        file_entries: List[FileEntry] = []
        min_k_vector = None
        histogram_vector = None
        char_class_vector = None
        if descriptor.current_snapshot is not None:
            snapshot_path = os.path.join(relation_dir, descriptor.current_snapshot)
            with open(snapshot_path, "r") as f:
                snapshot = json.load(f)
            manifest_file = snapshot.get("manifest_file")
            if manifest_file:
                with open(os.path.join(relation_dir, manifest_file), "rb") as f:
                    manifest_bytes = f.read()
                file_entries, native = read_manifest_file_entries(manifest_bytes)
                for fe in file_entries:
                    fe.file_path = os.path.join(relation_dir, fe.file_path)
                min_k_vector = native.get("min_k_hashes")
                histogram_vector = native.get("histogram_counts")
                char_class_vector = native.get("char_class_counts")

        self.manifest = Manifest(
            file_entries,
            self.schema,
            min_k_vector=min_k_vector,
            histogram_vector=histogram_vector,
            char_class_vector=char_class_vector,
        )
        return self.schema, self.manifest
