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
from opteryx.connectors.base.base_connector import BaseConnector
from opteryx.connectors.capabilities import Writable
from opteryx.exceptions import ConcurrentModificationError
from opteryx.models.dataset_descriptor import DatasetDescriptor
from opteryx.models.file_entry import FileEntry
from opteryx.types.schema import RelationSchema


def _now_utc_iso() -> str:
    """Get current UTC time as ISO 8601 string (microsecond precision)."""
    return datetime.now(timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z")


def _ts_for_filename(iso: str) -> str:
    """Convert ISO 8601 timestamp to filename-safe format.

    Replaces : and . with - for use in filenames.
    """
    return iso.replace(":", "-").replace(".", "-")


class LocalStoreConnector(BaseConnector, Writable):
    """Local file-based storage connector.

    Stores relations as JSON metadata + Parquet data files organized by relation name.
    Implements optimistic concurrency control for snapshot commits.

    Filesystem layout:
        {store_root}/{schema}/{table}/dataset.json
        {store_root}/{schema}/{table}/snapshot-{ts}.json
        {store_root}/{schema}/{table}/data-{uuid}.parquet
    """

    __mode__ = "Blob"

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
        """Read snapshot JSON file.

        Args:
            relation_dir: Path to relation directory
            snapshot_name: Filename of snapshot (e.g., "snapshot-2026-05-06T12-34-56-123456Z.json")

        Returns:
            Snapshot dict with format_version, created_at, parent_snapshot, files

        Raises:
            FileNotFoundError: If snapshot doesn't exist
        """
        if snapshot_name is None:
            return {"files": []}
        snapshot_path = os.path.join(relation_dir, snapshot_name)
        with open(snapshot_path, "r") as f:
            return json.load(f)

    def create_relation(self, relation_name: str, schema: RelationSchema) -> None:
        """Create a new relation.

        Args:
            relation_name: Fully-qualified relation name
            schema: RelationSchema for the table

        Raises:
            ValueError: If relation already exists or name is invalid
        """
        self._validate_relation_name(relation_name)
        relation_dir = self._relation_dir(relation_name)

        if os.path.isfile(os.path.join(relation_dir, "dataset.json")):
            raise ValueError(f"relation already exists: {relation_name}")

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

    def drop_relation(self, relation_name: str, if_exists: bool = False) -> None:
        """Drop a relation.

        Args:
            relation_name: Fully-qualified relation name
            if_exists: If True, don't raise error if relation doesn't exist

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

    def truncate_relation(self, relation_name: str) -> None:
        """Truncate a relation (remove all rows).

        Args:
            relation_name: Fully-qualified relation name

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

    def insert(self, relation_name: str, file_entries: List[FileEntry]) -> None:
        """Commit pre-written data files into a new snapshot.

        Args:
            relation_name: Fully-qualified relation name
            file_entries: List of FileEntry objects to append to the relation

        Raises:
            ValueError: If relation doesn't exist
            ConcurrentModificationError: If relation was modified concurrently
        """
        self._validate_relation_name(relation_name)
        relation_dir = self._relation_dir(relation_name)

        if not os.path.isfile(os.path.join(relation_dir, "dataset.json")):
            raise ValueError(f"relation does not exist: {relation_name}")

        # Read current snapshot and append new entries
        base_descriptor = self._read_dataset_json(relation_dir)
        current_snapshot = self._read_snapshot(relation_dir, base_descriptor.current_snapshot)
        current_files = [FileEntry.from_json_dict(fe) for fe in current_snapshot.get("files", [])]

        new_files = current_files + file_entries
        self._commit(relation_name, new_files)

    def _commit(self, relation_name: str, new_files: List[FileEntry]) -> None:
        """Optimistic concurrency control commit protocol.

        Atomically updates the relation's current snapshot, guarding against concurrent
        modifications via re-check before the final dataset.json write.

        Args:
            relation_name: Fully-qualified relation name
            new_files: Complete list of files for the new snapshot

        Raises:
            ConcurrentModificationError: If relation was modified concurrently
        """
        relation_dir = self._relation_dir(relation_name)

        # Step 1: Read base descriptor and capture current snapshot
        base_descriptor = self._read_dataset_json(relation_dir)
        base_snapshot = base_descriptor.current_snapshot

        # Step 2: Build new snapshot
        created_at = _now_utc_iso()
        new_snapshot_dict = {
            "format_version": 1,
            "created_at": created_at,
            "parent_snapshot": base_snapshot,
            "files": [fe.to_json_dict() for fe in new_files],
        }

        # Step 3: Choose snapshot name with collision guard
        snapshot_name = f"snapshot-{_ts_for_filename(created_at)}.json"
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
            raise ConcurrentModificationError(
                f"relation {relation_name} was modified concurrently"
            )

        # Step 7: Build and write new descriptor
        new_descriptor = DatasetDescriptor(
            format_version=base_descriptor.format_version,
            relation_name=base_descriptor.relation_name,
            schema=base_descriptor.schema,
            current_snapshot=snapshot_name,
            created_at=base_descriptor.created_at,
        )

        # Step 8: Atomic descriptor write
        dataset_path = os.path.join(relation_dir, "dataset.json")
        dataset_tmp = dataset_path + ".tmp"
        with open(dataset_tmp, "w") as f:
            json.dump(new_descriptor.to_dict(), f)
        os.replace(dataset_tmp, dataset_path)
