# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Local file-based storage connector implementing Writable interface.
"""

import json
import logging
import os
import time
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
from opteryx.exceptions import ColumnNotFoundError, ConcurrentModificationError, DatasetNotFoundError
from opteryx.models.dataset_descriptor import DatasetDescriptor
from opteryx.models.file_entry import FileEntry
from opteryx.models.manifest import Manifest
from opteryx.models.manifest_io import read_manifest_file_entries
from opteryx.models.manifest_io import write_manifest_parquet
from opteryx.types.schema import RelationSchema

logger = logging.getLogger(__name__)
from opteryx.utils import suggest_alternative, unique_id


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
        self.assert_name_free(relation_name, "table")

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
            raise DatasetNotFoundError(
                dataset=relation_name, connector=self.__class__.__name__
            )

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
            raise DatasetNotFoundError(
                dataset=relation_name, connector=self.__class__.__name__
            )

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

    # Materialized view operations. The registration record (defining SQL,
    # source tables) is a sidecar file next to dataset.json - the backing
    # table is an ordinary relation in every other respect. This store has no
    # trigger machinery, so registration records the sources verbatim and
    # nothing ever fires; it exists so the MV statement surface is fully
    # exercisable without a remote catalog.
    def _mv_path(self, relation_name: str) -> str:
        return os.path.join(self._relation_dir(relation_name), "materialized_view.json")

    def is_materialized_view(self, relation_name: str) -> bool:
        return os.path.isfile(self._mv_path(relation_name))

    # `workspace.collection.<object>` is ONE namespace: a name identifies a
    # table, a view or a task, never two of them. All three sentinels sit in the
    # same directory, so this is the list every creator checks.
    _NAME_SENTINELS = (("dataset.json", "table"), ("view.json", "view"), ("task.json", "task"))

    def name_holder(self, relation_name: str) -> Optional[str]:
        """Which kind of object holds this name, or None if it is free."""
        relation_dir = self._relation_dir(relation_name)
        for sentinel, kind in self._NAME_SENTINELS:
            if os.path.isfile(os.path.join(relation_dir, sentinel)):
                return kind
        return None

    def assert_name_free(self, relation_name: str, kind: str) -> None:
        """Refuse the name if another KIND already holds it.

        The same-kind case is the creator's own business, which knows whether it
        is a replace; this answers only "is this name someone else's".
        """
        holder = self.name_holder(relation_name)
        if holder is not None and holder != kind:
            raise ValueError(
                f"{relation_name} already exists as a {holder}. A table, a view and a "
                "task share one namespace, so a name identifies exactly one of them."
            )

    def _task_path(self, relation_name: str) -> str:
        return os.path.join(self._relation_dir(relation_name), "task.json")

    def is_task(self, relation_name: str) -> bool:
        return os.path.isfile(self._task_path(relation_name))

    def create_trigger(
        self,
        relation_name: str,
        trigger_name: str,
        task_name: str,
        author: Optional[str] = None,
        or_replace: bool = False,
    ) -> None:
        triggers = self.list_triggers(relation_name)
        existing = next((t for t in triggers if t.get("name") == trigger_name), None)
        if existing is not None:
            claimed = existing.get("target-task") or existing.get("target-view")
            if not or_replace and claimed != task_name:
                # Same guard the catalog applies: a blind overwrite would leave
                # the first target with no trigger and nothing to report it.
                raise ValueError(
                    f"trigger {trigger_name} on {relation_name} already runs {claimed}; "
                    f"refusing to repoint it at {task_name}"
                )
            triggers = [t for t in triggers if t.get("name") != trigger_name]
        triggers.append(
            {
                "name": trigger_name,
                "kind": "task",
                "target-task": task_name,
                "created-by": author,
                # The identity an UNATTENDED run carries. Pinned to the author on
                # creation; moved only by ALTER TRIGGER ... OWNER TO.
                "runs-as": (existing or {}).get("runs-as") or author,
                "suspended-at-ms": (existing or {}).get("suspended-at-ms"),
            }
        )
        self._write_triggers(relation_name, triggers)

    def set_trigger_owner(
        self,
        relation_name: str,
        trigger_name: str,
        new_owner: str,
        author: Optional[str] = None,
    ) -> None:
        triggers = self.list_triggers(relation_name)
        target = next((t for t in triggers if t.get("name") == trigger_name), None)
        if target is None:
            raise ValueError(f"trigger not found: {trigger_name} on {relation_name}")
        target["runs-as"] = new_owner
        self._write_triggers(relation_name, triggers)

    def set_trigger_suspended(
        self,
        relation_name: str,
        trigger_name: str,
        suspended: bool,
        author: Optional[str] = None,
    ) -> None:
        triggers = self.list_triggers(relation_name)
        target = next((t for t in triggers if t.get("name") == trigger_name), None)
        if target is None:
            raise ValueError(f"trigger not found: {trigger_name} on {relation_name}")
        target["suspended-at-ms"] = int(time.time() * 1000) if suspended else None
        target["suspended-by"] = author if suspended else None
        self._write_triggers(relation_name, triggers)

    def create_task(
        self,
        relation_name: str,
        statement: str,
        author: Optional[str] = None,
        or_replace: bool = False,
    ) -> None:
        self.assert_name_free(relation_name, "task")
        task_path = self._task_path(relation_name)
        if os.path.isfile(task_path) and not or_replace:
            raise ValueError(f"task already exists: {relation_name}")
        os.makedirs(os.path.dirname(task_path), exist_ok=True)
        existing = {}
        if os.path.isfile(task_path):
            with open(task_path) as f:
                existing = json.load(f)
        with open(task_path, "w") as f:
            # No `runs-as`: a task carries no identity. EXECUTE runs it as the
            # invoker, and an unattended run carries the TRIGGER's owner.
            json.dump({"sql": statement, "author": author}, f)

    def _rewrite_task(self, relation_name: str, **fields) -> None:
        task_path = self._task_path(relation_name)
        if not os.path.isfile(task_path):
            raise ValueError(f"task not found: {relation_name}")
        with open(task_path) as f:
            record = json.load(f)
        record.update(fields)
        with open(task_path, "w") as f:
            json.dump(record, f)

    def drop_task(
        self, relation_name: str, if_exists: bool = False, author: Optional[str] = None
    ) -> None:
        task_path = self._task_path(relation_name)
        if not os.path.isfile(task_path):
            if if_exists:
                return
            raise ValueError(f"task not found: {relation_name}")
        os.remove(task_path)

    def task_definition(self, relation_name: str) -> str:
        task_path = self._task_path(relation_name)
        if not os.path.isfile(task_path):
            raise ValueError(f"{relation_name} is not a task")
        with open(task_path) as f:
            record = json.load(f)
        sql = record.get("sql")
        if not sql:
            raise ValueError(
                f"task {relation_name} has no statement recorded; it cannot be executed."
            )
        return sql

    def _read_mv_record(self, relation_name: str) -> Optional[dict]:
        mv_path = self._mv_path(relation_name)
        if not os.path.isfile(mv_path):
            return None
        with open(mv_path) as f:
            return json.load(f)

    def materialized_view_definition(self, relation_name: str) -> str:
        record = self._read_mv_record(relation_name)
        if record is None:
            raise ValueError(f"{relation_name} is not a materialized view")
        sql = record.get("sql")
        if not sql:
            raise ValueError(
                f"materialized view {relation_name} has no defining SELECT recorded; "
                "it cannot be refreshed."
            )
        return sql

    def materialized_view_sources(self, relation_name: str) -> List[str]:
        """The sidecar's recorded sources, mirroring the catalog's `source-tables`."""
        record = self._read_mv_record(relation_name)
        if record is None:
            raise ValueError(f"{relation_name} is not a materialized view")
        return list(record.get("source_tables") or [])

    def set_materialized_view_owner(
        self, relation_name: str, new_owner: str, author: str = None
    ) -> None:
        """Repoint the sidecar's `runs-as`, mirroring the catalog's field."""
        record = self._read_mv_record(relation_name)
        if record is None:
            raise ValueError(f"{relation_name} is not a materialized view")
        record["runs-as"] = new_owner
        with open(self._mv_path(relation_name), "w") as f:
            json.dump(record, f)

    def set_materialized_view_suspended(
        self, relation_name: str, suspended: bool, author: str = None
    ) -> None:
        """Record suspended state on the sidecar, mirroring the catalog's fields."""
        record = self._read_mv_record(relation_name)
        if record is None:
            raise ValueError(f"{relation_name} is not a materialized view")
        record["suspended-at-ms"] = int(time.time() * 1000) if suspended else None
        record["suspended-by"] = author if suspended else None
        with open(self._mv_path(relation_name), "w") as f:
            json.dump(record, f)

    def mark_materialized_view_refreshed(
        self, relation_name: str, status: str, author: str = None
    ) -> None:
        """Stamp refresh state onto the sidecar, mirroring the catalog's fields."""
        record = self._read_mv_record(relation_name)
        if record is None:
            raise ValueError(f"{relation_name} is not a materialized view")
        record["last-refreshed-at-ms"] = int(time.time() * 1000)
        record["last-refresh-status"] = status
        record["last-refresh-author"] = author
        with open(self._mv_path(relation_name), "w") as f:
            json.dump(record, f)

    # Trigger records mirror the catalog's: one refresh trigger per (source,
    # MV) pair, held in a `triggers.json` sidecar next to the SOURCE table's
    # dataset.json - the trigger hangs off the table whose commits would fire
    # it, not off the MV. Nothing in this store ever fires them; they exist so
    # DROP TRIGGER / SHOW TRIGGERS surfaces are fully exercisable without a
    # remote catalog.
    def _triggers_path(self, relation_name: str) -> str:
        return os.path.join(self._relation_dir(relation_name), "triggers.json")

    @staticmethod
    def _mv_trigger_name(relation_name: str) -> str:
        """The auto-generated name of an MV's refresh trigger on a source -
        same convention as the catalog's, derived from the MV's name relative
        to its workspace (`refresh__<collection>__<dataset>`)."""
        relative = relation_name.split(".")[1:] or [relation_name]
        return "refresh__" + "__".join(relative)

    def list_triggers(self, relation_name: str) -> List[dict]:
        triggers_path = self._triggers_path(relation_name)
        if not os.path.isfile(triggers_path):
            return []
        with open(triggers_path) as f:
            return json.load(f)

    def _write_triggers(self, relation_name: str, triggers: List[dict]) -> None:
        triggers_path = self._triggers_path(relation_name)
        if not triggers:
            if os.path.isfile(triggers_path):
                os.remove(triggers_path)
            return
        tmp_path = triggers_path + ".tmp"
        with open(tmp_path, "w") as f:
            json.dump(triggers, f)
        os.replace(tmp_path, triggers_path)

    # --- declared relationships (ALTER TABLE ... ADD/DROP CONSTRAINT) --------
    #
    # Kept in the relation's OWN directory, beside triggers.json, mirroring the
    # catalog's relationships subcollection under the dataset document. That is
    # what makes "what relates to THIS dataset" a keyed read rather than a scan,
    # and it means a dropped relation takes its declarations with it.
    #
    # It is not a relation and cannot become one: this is a file, where a
    # dataset is a directory containing dataset.json, so no scan can resolve it.

    # JSON LINES, not a JSON document. The neighbouring triggers.json is an
    # array, but a relation carries a handful of triggers and may accumulate
    # relationships without limit; an array would mean rewriting the whole file
    # to add one row. It is also the row-oriented shape the catalog stores, so
    # the two do not diverge.
    _RELATIONSHIP_STORE_FILE = "relationships.jsonl"

    def _relationship_store_path(self, relation_parts: List[str]) -> str:
        """Where this relation keeps the relationships declared ON it."""
        return os.path.join(
            self._relation_dir(".".join(relation_parts)), self._RELATIONSHIP_STORE_FILE
        )

    def _read_relationships(self, relation_parts: List[str]) -> List[dict]:
        path = self._relationship_store_path(relation_parts)
        if not os.path.isfile(path):
            return []
        with open(path) as f:
            return [json.loads(line) for line in f if line.strip()]

    def _append_relationship(self, relation_parts: List[str], row: dict) -> None:
        """Add one row without touching the ones already there."""
        path = self._relationship_store_path(relation_parts)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "a") as f:
            f.write(json.dumps(row) + "\n")

    def _rewrite_relationships(self, relation_parts: List[str], rows: List[dict]) -> None:
        """Replace the whole store. Only a removal needs this; an add appends."""
        path = self._relationship_store_path(relation_parts)
        if not rows:
            if os.path.isfile(path):
                os.remove(path)
            return
        os.makedirs(os.path.dirname(path), exist_ok=True)
        tmp_path = path + ".tmp"
        with open(tmp_path, "w") as f:
            for row in rows:
                f.write(json.dumps(row) + "\n")
        os.replace(tmp_path, path)

    def declare_relationship(
        self,
        relation_parts: List[str],
        column_name: str,
        references_relation_parts: List[str],
        references_column_name: str,
        constraint_name: str,
        cardinality: str,
        author: Optional[str] = None,
    ) -> None:
        rows = self._read_relationships(relation_parts)

        # A constraint name is the only handle DROP CONSTRAINT has, so two of
        # them on one relation would make a drop ambiguous. Scoped to the
        # relation, not the workspace: two tables may each have an `owner_fk`.
        # Scoped to this relation because the store is: two relations may each
        # carry an `owner_fk`, and neither can see the other's.
        for row in rows:
            if row["constraint_name"] == constraint_name:
                raise ValueError(
                    f"constraint already exists: {constraint_name} on "
                    f"{'.'.join(relation_parts)}"
                )

        self._append_relationship(
            relation_parts,
            {
                "relationship_id": unique_id(),
                "kind": "maps",
                "constraint_name": constraint_name,
                # Parts, never a dotted string - the whole reason names are
                # carried split from the parser down to here.
                "from_workspace": relation_parts[0],
                "from_relation": list(relation_parts),
                "from_column": column_name,
                "to_workspace": references_relation_parts[0],
                "to_relation": list(references_relation_parts),
                "to_column": references_column_name,
                "cardinality": cardinality,
                # Asserted by a person, never inferred, so no confidence and no
                # evidence - those belong to a proposal, which this is not.
                "origin": "asserted",
                "confidence": None,
                "evidence": None,
                "status": "active",
                "asserted_by": author,
                "asserted_at": datetime.now(timezone.utc).isoformat(),
                "verified_at": None,
            },
        )

    def drop_relationship(
        self,
        relation_parts: List[str],
        constraint_name: str,
        if_exists: bool = False,
        author: Optional[str] = None,
    ) -> bool:
        rows = self._read_relationships(relation_parts)
        remaining = [row for row in rows if row["constraint_name"] != constraint_name]
        if len(remaining) == len(rows):
            if if_exists:
                return False
            raise ValueError(
                f"There is no constraint {constraint_name} on {'.'.join(relation_parts)}."
            )
        self._rewrite_relationships(relation_parts, remaining)

    def relationships_through_column(
        self, relation_name: str, column_name: str
    ) -> List[dict]:
        """Declared relationships through one column of this relation.

        OUTBOUND ONLY, and the omission is structural rather than an oversight.
        This store is one file per relation with no reverse index, so "what
        points AT this column" would mean walking every relation directory
        under the store root - the exact scan the catalog answers with one
        collection group query. The local store exists so the engine stays
        testable without a catalog, not to reproduce its indexes, and the cost
        of the gap is bounded: an inbound reference here is left stale, which
        is what the catalog did for everything until §9.
        """
        rows = []
        for row in self._read_relationships(relation_name.split(".")):
            if row.get("from_column") != column_name:
                continue
            rows.append(
                {
                    "constraint_name": row.get("constraint_name"),
                    "origin": row.get("origin"),
                    "status": row.get("status"),
                    "kind": row.get("kind"),
                    "inbound": False,
                    "references": ".".join(
                        list(row.get("to_relation") or []) + [str(row.get("to_column"))]
                    ),
                }
            )
        return rows

    def break_relationships_through_column(
        self, relation_name: str, column_name: str, author: Optional[str] = None
    ) -> List[dict]:
        """Mark broken - never remove - what a dropped column orphaned."""
        relation_parts = relation_name.split(".")
        rows = self._read_relationships(relation_parts)

        broken = []
        changed = False
        for row in rows:
            if row.get("from_column") != column_name or row.get("status") == "broken":
                continue
            row["status"] = "broken"
            row["broken_reason"] = "column-dropped"
            row["broken_detail"] = f"column {relation_name}.{column_name} was dropped"
            row["verified_at"] = datetime.now(timezone.utc).isoformat()
            changed = True
            broken.append(dict(row))
        if changed:
            self._rewrite_relationships(relation_parts, rows)
        return broken
        return True

    def drop_trigger(
        self,
        relation_name: str,
        trigger_name: str,
        author: Optional[str] = None,
        missing_ok: bool = False,
    ) -> None:
        self._validate_relation_name(relation_name)
        triggers = self.list_triggers(relation_name)
        remaining = [t for t in triggers if t.get("name") != trigger_name]
        if len(remaining) == len(triggers):
            if missing_ok:
                return
            raise ValueError(
                f"trigger {trigger_name} does not exist on {relation_name} "
                "(use DROP TRIGGER IF EXISTS to make this quiet)"
            )
        self._write_triggers(relation_name, remaining)

    def _land_refresh_trigger(
        self, source: str, mv_relation_name: str, author: Optional[str]
    ) -> None:
        """Upsert this MV's refresh trigger on one source table, with the same
        field names (kebab-case) the catalog stores."""
        name = self._mv_trigger_name(mv_relation_name)
        triggers = [t for t in self.list_triggers(source) if t.get("name") != name]
        now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
        triggers.append(
            {
                "name": name,
                "kind": "materialized_view_refresh",
                "target-view": mv_relation_name,
                "statement-id": None,
                "created-by": author,
                "created-at-ms": now_ms,
                "last-fired-at-ms": None,
                "last-fired-status": None,
            }
        )
        self._write_triggers(source, triggers)

    def register_materialized_view(
        self,
        relation_name: str,
        sql: str,
        source_tables: List[str],
        author: Optional[str] = None,
    ) -> None:
        self._validate_relation_name(relation_name)
        if not self.relation_exists(relation_name):
            raise ValueError(
                f"materialized view backing table does not exist: {relation_name}"
            )
        # Re-registration (CREATE OR REPLACE) reconciles triggers against the
        # new source list - a source no longer read must not keep firing.
        previous = self._read_mv_record(relation_name) or {}
        record = {
            "sql": sql,
            "source_tables": list(source_tables),
            "author": author,
            # Pinned exactly as the catalog pins it: an existing owner survives
            # re-registration, so editing a view never transfers whose
            # authority refreshes it.
            "runs-as": previous.get("runs-as") or author,
            "registered_at": _now_utc_iso(),
        }
        mv_path = self._mv_path(relation_name)
        tmp_path = mv_path + ".tmp"
        with open(tmp_path, "w") as f:
            json.dump(record, f)
        os.replace(tmp_path, mv_path)

        trigger_name = self._mv_trigger_name(relation_name)
        for source in source_tables:
            self._land_refresh_trigger(source, relation_name, author)
        for stale in set(previous.get("source_tables") or []) - set(source_tables):
            self.drop_trigger(stale, trigger_name, author=author, missing_ok=True)

    def drop_materialized_view(
        self, relation_name: str, if_exists: bool = False, author: Optional[str] = None
    ) -> None:
        self._validate_relation_name(relation_name)
        if not self.relation_exists(relation_name):
            if if_exists:
                return
            raise DatasetNotFoundError(
                dataset=relation_name, connector=self.__class__.__name__
            )
        if not self.is_materialized_view(relation_name):
            raise ValueError(
                f"{relation_name} is not a materialized view; "
                "use DROP TABLE or DROP VIEW"
            )
        # A dropped MV takes its refresh triggers with it - they live on the
        # source tables, so remove them before the MV's own directory goes.
        record = self._read_mv_record(relation_name) or {}
        trigger_name = self._mv_trigger_name(relation_name)
        for source in record.get("source_tables") or []:
            self.drop_trigger(source, trigger_name, author=author, missing_ok=True)
        shutil.rmtree(self._relation_dir(relation_name))

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
        commit_message: Optional[str] = None,
    ) -> None:
        """Commit pre-written data files into a new snapshot.

        Args:
            relation_name: Fully-qualified relation name
            file_entries: List of FileEntry objects to append to the relation
            author: session user, unused by this store (see create_relation)
            commit_message: what this append was, unused by this store - its
                snapshot records carry no author or message (see `_commit`)

        Raises:
            ValueError: If relation doesn't exist
            ConcurrentModificationError: If relation was modified concurrently
        """
        self._validate_relation_name(relation_name)
        relation_dir = self._relation_dir(relation_name)

        if not os.path.isfile(os.path.join(relation_dir, "dataset.json")):
            raise DatasetNotFoundError(
                dataset=relation_name, connector=self.__class__.__name__
            )

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
        commit_message: Optional[str] = None,
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
            commit_message: what this replace was, unused by this store - its
                snapshot records carry no author or message (see `_commit`)

        Raises:
            ValueError: If relation doesn't exist
            ConcurrentModificationError: If relation was modified concurrently
        """
        self._validate_relation_name(relation_name)
        relation_dir = self._relation_dir(relation_name)

        if not os.path.isfile(os.path.join(relation_dir, "dataset.json")):
            raise DatasetNotFoundError(
                dataset=relation_name, connector=self.__class__.__name__
            )

        self._commit(relation_name, file_entries, schema=schema)

    def relation_column_names(self, relation_name: str) -> List[str]:
        """Return the relation's current column names only (not full type fidelity)."""
        relation_dir = self._relation_dir(relation_name)
        descriptor = self._read_dataset_json(relation_dir)
        if descriptor is None:
            raise DatasetNotFoundError(
                dataset=relation_name, connector=self.__class__.__name__
            )
        return [c.name for c in descriptor.schema.columns]

    def _patch_column(
        self,
        relation_name: str,
        new_columns: List,
        drop: Optional[List[str]] = None,
        rename: Optional[Dict[str, str]] = None,
        add: Optional[List[bytes]] = None,
        retype: Optional[Dict[str, bytes]] = None,
        keep: Optional[List[int]] = None,
    ) -> None:
        """Rewrite every live file's SHAPE, then commit the new schema.

        Each file is patched by `rugo.parquet.patch_columns`, which copies the
        surviving columns' encoded pages byte-for-byte and writes a new footer -
        nothing is decoded, so the cost tracks bytes on disk rather than values
        stored.

        Patched files are written to NEW paths and only the new snapshot points
        at them. The originals stay where they are, still referenced by earlier
        snapshots, so time travel keeps answering with the shape those snapshots
        were written under. That leaves the superseded files to be reclaimed
        later, exactly as DROP TABLE already does (see drop_relation).

        `add` is a donor file per column being appended (see
        `build_column_donor`); the patcher synthesises each one's chunks as a
        single repeated value, so an added column costs a few bytes per row
        group whatever the row count.

        `retype` is a donor per column being re-declared. Free when parquet's
        physical type does not change; otherwise that ONE column is decoded and
        re-encoded and every other column is still copied verbatim.

        `keep` is the source position of each surviving column, in the new
        order, or None when positions are unchanged (a rename, or an add -
        appending shifts nothing). Manifest
        statistics for this store are keyed BY POSITION, so dropping a column
        shifts every later column's stats and they have to be remapped in step -
        leaving them alone would silently attribute one column's min/max to
        another.
        """
        import dataclasses

        import rugo.parquet as _rugo_parquet

        relation_dir = self._relation_dir(relation_name)
        descriptor = self._read_dataset_json(relation_dir)
        if descriptor is None:
            raise DatasetNotFoundError(
                dataset=relation_name, connector=self.__class__.__name__
            )

        current_files = self._read_current_file_entries(relation_dir, descriptor)

        def _remap_dict(mapping):
            if mapping is None or keep is None:
                return mapping
            return {j: mapping[s] for j, s in enumerate(keep) if s in mapping}

        def _remap_list(values):
            if values is None or keep is None:
                return values
            return [values[s] if s < len(values) else None for s in keep]

        new_entries: List[FileEntry] = []
        for entry in current_files:
            source_path = os.path.join(relation_dir, entry.file_path)
            with open(source_path, "rb") as f:
                patched = _rugo_parquet.patch_columns(
                    f.read(), drop=drop, rename=rename, add=add, retype=retype
                )

            file_name = f"data-{unique_id()}.parquet"
            full_path = os.path.join(relation_dir, file_name)
            tmp_path = f"{full_path}.tmp"
            with open(tmp_path, "wb") as f:
                f.write(patched)
            os.replace(tmp_path, full_path)

            new_entries.append(
                dataclasses.replace(
                    entry,
                    file_path=file_name,
                    file_size_in_bytes=os.path.getsize(full_path),
                    lower_bounds=_remap_dict(entry.lower_bounds),
                    upper_bounds=_remap_dict(entry.upper_bounds),
                    null_value_counts=_remap_dict(entry.null_value_counts),
                    min_length_bounds=_remap_dict(entry.min_length_bounds),
                    max_length_bounds=_remap_dict(entry.max_length_bounds),
                    min_values=_remap_list(entry.min_values),
                    max_values=_remap_list(entry.max_values),
                    null_counts=_remap_list(entry.null_counts),
                    min_lengths=_remap_list(entry.min_lengths),
                    max_lengths=_remap_list(entry.max_lengths),
                    char_total_bytes=_remap_list(entry.char_total_bytes),
                    column_uncompressed_sizes_in_bytes=_remap_list(
                        entry.column_uncompressed_sizes_in_bytes
                    ),
                    # A prebuilt native stats accelerator is keyed by the OLD
                    # column positions. Dropping it costs a rebuild; keeping a
                    # stale one would answer for the wrong column.
                    column_stats=entry.column_stats if keep is None else None,
                )
            )

        new_schema = RelationSchema(name=relation_name, columns=new_columns)
        self._commit(relation_name, new_entries, schema=new_schema)

    def add_column(
        self,
        relation_name: str,
        column_name: str,
        column_type,
        nullable: bool = True,
        default=None,
        if_not_exists: bool = False,
        author: Optional[str] = None,
    ) -> None:
        """Append a column, filling existing rows with one repeated value.

        `default` is the fill value written into the files now - NULL when
        none was given. Opteryx honours no defaults afterwards and has no NULL
        constraints, so nothing about it is stored: the only question it
        answers is what goes in the file for the rows that already exist.
        """
        from opteryx.connectors.capabilities.writable import build_column_donor
        from opteryx.types.schema import SchemaColumn
        from opteryx.types.schema import mint_column_identity

        self._validate_relation_name(relation_name)
        relation_dir = self._relation_dir(relation_name)
        descriptor = self._read_dataset_json(relation_dir)
        if descriptor is None:
            raise DatasetNotFoundError(
                dataset=relation_name, connector=self.__class__.__name__
            )

        columns = list(descriptor.schema.columns)
        if column_name in {c.name for c in columns}:
            if if_not_exists:
                return
            raise ValueError(
                f"cannot add {column_name} to {relation_name}: it already has a column "
                f"of that name"
            )

        # Only files need a donor - an empty relation is a pure schema change,
        # and building one for a type the patcher could not synthesise anyway
        # would refuse a statement that has no data to write.
        donors = None
        if self._read_current_file_entries(relation_dir, descriptor):
            donors = [build_column_donor(column_name, column_type, default)]

        columns.append(
            SchemaColumn(
                name=column_name,
                column_type=column_type,
                nullable=nullable,
                identity=mint_column_identity("$add_column", column_name),
            )
        )
        # keep=None: appending shifts no existing column's position, so the
        # per-position manifest statistics stay attached to the right columns.
        self._patch_column(relation_name, new_columns=columns, add=donors)

    def drop_column(
        self,
        relation_name: str,
        column_name: str,
        if_exists: bool = False,
        author: Optional[str] = None,
    ) -> None:
        """Remove a column without decoding the ones that stay."""
        self._validate_relation_name(relation_name)
        relation_dir = self._relation_dir(relation_name)
        descriptor = self._read_dataset_json(relation_dir)
        if descriptor is None:
            raise DatasetNotFoundError(
                dataset=relation_name, connector=self.__class__.__name__
            )

        columns = list(descriptor.schema.columns)
        names = [c.name for c in columns]
        if column_name not in names:
            if if_exists:
                return
            raise ColumnNotFoundError(
                column=column_name,
                dataset=relation_name,
                suggestion=suggest_alternative(column_name, names),
            )
        if len(columns) == 1:
            raise ValueError(
                f"cannot drop the last column of {relation_name}: a relation with no "
                "columns is not a relation"
            )

        keep = [i for i, c in enumerate(columns) if c.name != column_name]
        self._patch_column(
            relation_name,
            new_columns=[columns[i] for i in keep],
            drop=[column_name],
            keep=keep,
        )

        # After the patch, never before: a break recorded against a column that
        # then failed to drop would be a lie about the data.
        # Non-fatal: the column is already gone, and raising here would fail a
        # statement that has in fact succeeded. What a failed sweep leaves is a
        # relationship still marked active against a column that no longer
        # exists - which is the state this whole check improves on, and which
        # `fsck` finds - rather than a half-applied DDL statement.
        try:
            self.break_relationships_through_column(relation_name, column_name, author=author)
        except Exception:  # noqa: BLE001
            logger.warning(
                "dropped %s.%s but could not mark the relationships through it broken; "
                "they now reference a column that does not exist",
                relation_name,
                column_name,
                exc_info=True,
            )

    def rename_column(
        self,
        relation_name: str,
        old_column_name: str,
        new_column_name: str,
        author: Optional[str] = None,
    ) -> None:
        """Rename a column, touching no data at all."""
        import dataclasses

        self._validate_relation_name(relation_name)
        relation_dir = self._relation_dir(relation_name)
        descriptor = self._read_dataset_json(relation_dir)
        if descriptor is None:
            raise DatasetNotFoundError(
                dataset=relation_name, connector=self.__class__.__name__
            )

        columns = list(descriptor.schema.columns)
        names = [c.name for c in columns]
        if old_column_name not in names:
            raise ColumnNotFoundError(
                column=old_column_name,
                dataset=relation_name,
                suggestion=suggest_alternative(old_column_name, names),
            )
        if new_column_name in names:
            raise ValueError(
                f"cannot rename {old_column_name} to {new_column_name}: "
                f"{relation_name} already has a column called {new_column_name}"
            )

        new_columns = [
            dataclasses.replace(c, name=new_column_name) if c.name == old_column_name else c
            for c in columns
        ]
        self._patch_column(
            relation_name,
            new_columns=new_columns,
            rename={old_column_name: new_column_name},
        )

    def alter_column_type(
        self, relation_name: str, column_name: str, new_type, author: Optional[str] = None
    ) -> None:
        """Re-declare a column as a wider type.

        The widening's legality was settled at bind time (`is_legal_widen`), so
        this only has to make the files say the new type. Most of the lattice
        costs nothing on disk - INT8/INT16/INT32 all ride parquet's physical
        int32, and FLOAT32 is already written as float64 - so only the
        annotation changes and every page is copied verbatim. Widening to
        INT64/UINT64 does change the physical type, and then that one column is
        decoded and re-encoded while the rest of the file is still copied.
        """
        import dataclasses

        from opteryx.connectors.capabilities.writable import build_column_donor

        self._validate_relation_name(relation_name)
        relation_dir = self._relation_dir(relation_name)
        descriptor = self._read_dataset_json(relation_dir)
        if descriptor is None:
            raise DatasetNotFoundError(
                dataset=relation_name, connector=self.__class__.__name__
            )

        columns = list(descriptor.schema.columns)
        names = [c.name for c in columns]
        if column_name not in names:
            raise ColumnNotFoundError(
                column=column_name,
                dataset=relation_name,
                suggestion=suggest_alternative(column_name, names),
            )

        # Only files need a donor; on an empty relation this is a pure schema
        # change, the same posture add_column takes.
        donors = None
        if self._read_current_file_entries(relation_dir, descriptor):
            donors = {column_name: build_column_donor(column_name, new_type, None)}

        new_columns = [
            dataclasses.replace(c, column_type=new_type) if c.name == column_name else c
            for c in columns
        ]
        # keep=None: retyping shifts no column's position. The retyped column's
        # own min/max bounds stay VALID under a widening - the value set is
        # unchanged and the ordering is the same in the wider domain - so they
        # are carried over rather than dropped.
        self._patch_column(relation_name, new_columns=new_columns, retype=donors)

    def relation_schema(self, relation_name: str) -> RelationSchema:
        """The relation's current schema, whole - see Writable.relation_schema."""
        relation_dir = self._relation_dir(relation_name)
        descriptor = self._read_dataset_json(relation_dir)
        if descriptor is None:
            raise DatasetNotFoundError(
                dataset=relation_name, connector=self.__class__.__name__
            )
        return descriptor.schema

    def list_relationships(self, relation_name: str) -> List[dict]:
        """Relationships declared ON this relation - see Writable.list_relationships.

        Broken rows are skipped: a relationship whose column was dropped is kept
        as a record of what went wrong, not as a declaration to re-issue, and
        rendering one into a CREATE TABLE would produce a statement that names a
        column the CREATE does not declare.
        """
        declarations = []
        for row in self._read_relationships(relation_name.split(".")):
            if row.get("status") == "broken":
                continue
            declarations.append(
                {
                    "constraint_name": row["constraint_name"],
                    "column_name": row["from_column"],
                    "references_relation_parts": list(row["to_relation"]),
                    "references_column_name": row["to_column"],
                    "cardinality": row.get("cardinality"),
                }
            )
        return declarations

    def relation_column_types(self, relation_name: str) -> Dict[str, "ColumnType"]:
        """Return the relation's current column name -> ColumnType mapping."""
        relation_dir = self._relation_dir(relation_name)
        descriptor = self._read_dataset_json(relation_dir)
        if descriptor is None:
            raise DatasetNotFoundError(
                dataset=relation_name, connector=self.__class__.__name__
            )
        return {c.name: c.column_type for c in descriptor.schema.columns}

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

        self.assert_name_free(view_name, "view")
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
