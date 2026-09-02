# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Opteryx Connector - Refactored Architecture

Architecture:
- OpteryxConnector: Long-lived catalog gateway (handles catalog operations, views, introspection)
- OpteryxTable: Transient table-specific engine (handles data reading for one table)
"""

import logging
from typing import Any, Dict, List, Optional, Tuple

from opteryx.connectors import TableType

logger = logging.getLogger(__name__)

# One-shot guard for the "backend has no native sketch vectors" report, keyed by
# the CLASS that failed the probe rather than by process. A global flag meant the
# first backend to degrade silenced every other one for the life of the process,
# so in a deployment mixing a native workspace with a third-party one you only
# ever heard about whichever happened to be read first - and the line named no
# dataset, so you could not tell which.
_warned_no_native_sketches: set = set()


def _warn_no_native_sketches(table: Any) -> None:
    """Report, once per backend class, that a table exposes no sketch vectors.

    Do not prescribe an upgrade unconditionally here. `manifest_sketch_vectors`
    is probed by duck typing against whatever `Dataset` implementation the
    workspace is registered with, and a missing accessor has two unrelated
    causes:

      * an opteryx_catalog older than the accessor - genuinely stale, and
        upgrading is the fix, so this is a WARNING; or
      * a catalog backend whose format simply has no sketch statistics to give.
        Apache Iceberg is the case in point: its manifests have no field for
        NDV/histogram sketches, so no version of opteryx_catalog would add them
        and "upgrade opteryx_catalog" is advice the operator cannot act on. That
        is a property of the format, not a fault, so it is logged at DEBUG.

    The two are told apart by where the implementing class comes from, which is
    the only thing the engine actually knows. Third-party backends should define
    the accessor and return `{}` to declare "no sketches" explicitly rather than
    relying on this path.
    """
    cls = type(table)
    key = f"{cls.__module__}.{cls.__qualname__}"
    if key in _warned_no_native_sketches:
        return
    _warned_no_native_sketches.add(key)

    native = cls.__module__.split(".")[0] == "opteryx_catalog"
    detail = (
        f"{key} does not implement manifest_sketch_vectors, so whole-column "
        f"NDV/histogram sketches are not available natively; the planner uses the "
        f"per-file Python fallback where per-file sketch stats exist, and no sketch "
        f"statistics at all where they do not."
    )
    if native:
        logger.warning(
            f"{detail} This is an opteryx_catalog dataset predating the accessor - "
            f"upgrade opteryx_catalog to enable native sketch reductions."
        )
    else:
        logger.debug(
            f"{detail} This is a non-opteryx_catalog backend; if its format carries "
            f"no sketch statistics this is expected and the backend should define "
            f"manifest_sketch_vectors returning an empty dict to say so."
        )
from opteryx.connectors.base.base_connector import BaseTable
from opteryx.connectors.capabilities import Diachronic, Eidetic, PredicatePushable, Writable
from opteryx.connectors.capabilities.writable import EgressRefusal
from opteryx.connectors.manifest_disk_cache import CachingFileIO
from opteryx.connectors.manifest_disk_cache import manifest_cache_tiers
from opteryx.exceptions import (
    CollectionNotEmptyError,
    DatasetNotFoundError,
    DatasetReadError,
    InvalidInternalStateError,
)
from opteryx.models import FileEntry, Manifest
from opteryx.types.logical_type import LogicalCategory
from opteryx.types.schema import SchemaColumn, RelationSchema


class OpteryxTable(BaseTable, Diachronic, PredicatePushable):
    """
    Plan-time table metadata provider for Opteryx tables.

    This is a transient object created per-table during planning that handles:
    - Schema resolution
    - Manifest building (file list + statistics)
    - Time-travel query resolution

    This class is PLAN-TIME ONLY - it does not perform any data reading.
    Execution uses generic filesystem readers based on file paths from the manifest.

    It derives ``BaseTable`` for the same reason every other table engine does:
    the optimizer probes capabilities off the table engine the binder puts on
    ``Scan.connector``, and ``BaseTable`` is where the full set of capability
    defaults lives. Declaring them by hand here instead left this class blind to
    every capability added later - which is exactly how it came to be missing
    ``supports_int64_timestamp_retag``.
    """

    __mode__ = "Blob"
    __type__ = "OPTERYX"
    __synchronousity__ = "asynchronous"

    # Capability declarations (for plan-time). Only the ones that differ from
    # the BaseTable defaults belong here.
    supports_diachronic = True  # Time-travel queries
    supports_version_travel = True  # VERSION AS OF <snapshot id / PREVIOUS>
    supports_statistics = True  # Manifest provides stats
    supports_predicate_pushdown = True  # Allow optimizer to push predicates to reader
    supports_limit_pushdown = True  # Allow optimizer to push LIMIT to OpteryxTable
    # The reader that serves a catalog scan is not this class - it is chosen
    # from the manifest's FileEntry.file_format (physical_planner
    # `_scan_reader_for_manifest`), and a catalog manifest is parquet-only
    # (`FileEntry.from_datafile` types every entry PARQUET). That reader is
    # ParquetReadNode, which honours a scan-declared TIMESTAMP64 on an
    # int64-stored column. If the catalog ever hands back a non-parquet format,
    # this has to become format-aware the way FileSystemTable's is.
    supports_int64_timestamp_retag = True

    PUSHABLE_OPS: Dict[str, bool] = {
        "Eq": True,
        "NotEq": True,
        "Gt": True,
        "GtEq": True,
        "Lt": True,
        "LtEq": True,
        "Like": True,
        "NotLike": True,
        "ILike": True,
        "NotILike": True,
        "InStr": True,
        "NotInStr": True,
        "IInStr": True,
        "NotIInStr": True,
        "InList": True,
        "NotInList": True,
        "RLike": True,
        "NotRLike": True,
        "Between": True,
        "IsNull": True,
        "IsNotNull": True,
        "IsEmpty": True,
        "IsNotEmpty": True,
    }

    def __init__(self, dataset: str, catalog, workspace: str, **kwargs):
        """
        Initialize the plan-time table metadata provider.

        Args:
            dataset: The table name (after catalog prefix is removed)
            catalog: The Opteryx Catalog instance
            workspace: The workspace name
            **kwargs: Additional parameters (telemetry, etc.)
        """
        # Resolved up front by the catalog resolution step, if available, so we
        # can skip the per-table catalog round trip below.
        prefetched_table = kwargs.pop("prefetched_table", None)

        Diachronic.__init__(self, **kwargs)
        PredicatePushable.__init__(self, **kwargs)

        self.dataset = dataset.replace("/", ".")
        self.catalog = catalog
        self.workspace = workspace
        self.telemetry = kwargs.get("telemetry")

        # Initialize state
        self.snapshot_id = None
        self.snapshot = None
        self.dataset_committed_at = None
        self.schema = None
        self.manifest = None

        # Load table from catalog
        from opteryx_catalog.exceptions import DatasetNotFound

        try:
            if prefetched_table is not None:
                self.table = prefetched_table
            else:
                self.table = self.catalog.load_dataset(self.dataset)
            self.snapshot = self.table.snapshot()
            self.snapshot_id = None if self.snapshot is None else self.snapshot.snapshot_id
        except DatasetNotFound as exc:
            raise DatasetNotFoundError(dataset=self.dataset, connector=self.__type__) from exc

    @staticmethod
    def _normalize_type(
        raw_type: Any, default: Optional[LogicalCategory] = LogicalCategory.VARCHAR
    ) -> Optional[LogicalCategory]:
        if isinstance(raw_type, LogicalCategory):
            return raw_type

        candidate = raw_type
        if getattr(raw_type, "name", None) is not None:
            candidate = raw_type.name
        elif getattr(raw_type, "value", None) is not None:
            candidate = raw_type.value

        if candidate is None:
            return default

        from opteryx.types.logical_type import try_parse_column_type

        parsed = try_parse_column_type(str(candidate))
        return default if parsed is None else parsed.category

    @classmethod
    def _normalize_schema(
        cls, schema: Any, relation_name: Optional[str] = None
    ) -> RelationSchema:
        if isinstance(schema, RelationSchema):
            if relation_name:
                schema.name = relation_name
            return schema

        columns = []
        for column in getattr(schema, "columns", []) or []:
            if isinstance(column, SchemaColumn):
                normalized = column
            else:
                name = getattr(column, "name", None)
                if name is None and isinstance(column, dict):
                    name = column.get("name")
                if name is None:
                    continue

                raw_type = getattr(column, "type", None)
                if raw_type is None and isinstance(column, dict):
                    raw_type = column.get("type")
                raw_element_type = getattr(column, "element_type", None)
                if raw_element_type is None and isinstance(column, dict):
                    raw_element_type = column.get("element_type") or column.get("element-type")
                raw_field_id = getattr(column, "id", None)
                if raw_field_id is None and isinstance(column, dict):
                    raw_field_id = column.get("id")

                from opteryx.types import logical_type as _lt
                from opteryx.types.logical_type import _CATEGORY_TO_CANONICAL
                from opteryx.types.logical_type import try_parse_column_type
                _ot = cls._normalize_type(raw_type, default=LogicalCategory.VARCHAR)
                _et = (cls._normalize_type(raw_element_type, default=None)
                       if raw_element_type is not None else None)
                _p = getattr(column, "precision", None)
                _s = getattr(column, "scale", None)
                # Take the stored name at face value FIRST, and only fall back to
                # the LogicalCategory round-trip when it isn't a name we can parse.
                #
                # The category round-trip is lossy for any type carrying
                # information the category cannot hold, and it is lossy in a
                # direction that silently WIDENS: IPv4's category is INTEGER
                # (deliberately — that is what makes ordering, grouping and joins
                # run on the raw uint32), and so is every unsigned width's, so
                # `IPV4`, `UINT32` and `UINT64` all came back out of
                # _CATEGORY_TO_CANONICAL as plain INT64. Descriptor destroyed,
                # scan retag never fires, addresses render as integers — and an
                # unsigned column silently becomes signed.
                #
                # Parsing the name directly is exact for all of those, and this
                # is behaviour-neutral for what the catalog stores TODAY:
                # `INTEGER`/`VARCHAR`/`TIMESTAMP`/`BOOLEAN`/`DOUBLE`/`BLOB` parse
                # to the same types the category path produced. It is what makes
                # a catalog that starts persisting exact type strings read back
                # correctly, with no second change needed here.
                #
                # DECIMAL and ARRAY deliberately fall THROUGH: the catalog stores
                # them bare, with precision/scale and element-type in separate
                # columns, so the bare names do not parse (that is why this uses
                # try_parse_column_type rather than the fail-loud entry point) and
                # the parameter-aware branches below are still the only correct
                # readers for them. A parameterized `DECIMAL(10, 2)` in the stored
                # name is handled by the parse and never reaches them.
                _raw_name = getattr(raw_type, "name", None) or (
                    str(raw_type) if raw_type is not None else None
                )
                _exact = try_parse_column_type(str(_raw_name)) if _raw_name is not None else None
                if _exact is not None:
                    _ct = _exact
                elif _ot == LogicalCategory.DECIMAL and _p is not None and _s is not None:
                    _ct = _lt.DECIMAL(_p, _s)
                elif _ot == LogicalCategory.ARRAY:
                    _elem = _CATEGORY_TO_CANONICAL.get(_et, _lt.VARIANT) if _et is not None else _lt.VARIANT
                    _ct = _lt.ARRAY(_elem)
                else:
                    _ct = _CATEGORY_TO_CANONICAL.get(_ot)
                from opteryx.types.schema import mint_column_identity
                normalized = SchemaColumn(
                    name=name,
                    column_type=_ct,
                    nullable=getattr(column, "nullable", True),
                    identity=mint_column_identity(relation_name or getattr(schema, "name", None), name),
                    field_id=raw_field_id,
                )

            columns.append(normalized)

        return RelationSchema(
            name=relation_name or getattr(schema, "name", "dataset"), columns=columns
        )

    def get_dataset_schema(self) -> RelationSchema:
        """
        Get the dataset's column schema, without building a Manifest.

        Same schema `get_dataset_metadata` returns, resolved from the same snapshot,
        reached without the `table.scan()` that lists every data file and its
        per-column statistics. That scan is the expensive half of reading a catalog
        relation and it is worth skipping only for a caller that will never look at a
        file - the edit-time check, which stops at the end of binding.

        Every other caller wants `get_dataset_metadata`: a Scan bound through here
        carries no manifest, so it cannot be pruned, costed or turned into a physical
        plan.

        A relation with nothing committed resolves to no snapshot at all (see
        `_resolve_snapshot`) and is served its DECLARED schema. That branch is
        shared with `get_dataset_metadata` so the edit-time check and the run
        cannot disagree about whether such a relation is readable.

        Returns:
            RelationSchema
        """
        self._resolve_snapshot()
        if self.snapshot is None:
            self.dataset_committed_at = None
            return self.get_declared_schema()
        raw_schema = self.table.schema(self.snapshot.schema_id)
        self.schema = self._normalize_schema(raw_schema, relation_name=self.dataset)
        self.dataset_committed_at = self.snapshot.timestamp_ms
        return self.schema

    def get_declared_schema(self) -> RelationSchema:
        """The dataset's registered schema, read WITHOUT resolving a snapshot.

        Overrides `BaseTable.get_declared_schema` because this reader's schema
        is otherwise snapshot-scoped: every other schema path here goes through
        `_resolve_snapshot` and answers as of the snapshot it settles on. A
        write target has no snapshot to answer as of - a dataset created and not
        yet written to has a schema (`metadata.current_schema_id`) and zero
        snapshots, and reaching it through the snapshot-scoped path is what made
        the FIRST `INSERT INTO` a freshly-created relation impossible.

        Deliberately the CURRENT registered schema, not the current snapshot's:
        rows being inserted now must conform to the relation as it is declared
        now, not as it was at whatever commit the head points at.

        The read paths call this too, but only for a relation with nothing
        committed - where the declared schema is the only schema there is.

        Returns:
            RelationSchema
        """
        raw_schema = self.table.schema()
        if raw_schema is None:
            raise DatasetReadError(
                f"The dataset {self.dataset} has no registered schema."
            )
        self.schema = self._normalize_schema(raw_schema, relation_name=self.dataset)
        return self.schema

    def get_snapshots(self) -> list:
        """The relation's commit history, newest first, for `SHOW SNAPSHOTS FOR`.

        Rows are the `opteryx.models.snapshot_history` shape, not the catalog's
        `Snapshot` dataclass: normalizing HERE is what keeps that module free of
        any opteryx_catalog import, so the statement's output shape is defined
        once and a second connector with a commit log answers it by producing
        the same dicts.

        This reloads the dataset with `load_history=True` rather than reading
        `self.table`, which was loaded without it and therefore carries only the
        current snapshot - the same reload `_resolve_snapshot` performs for time
        travel. It is a second catalog round trip and it is the statement's whole
        result, so it is paid on this path only, never on a normal read.

        Ordering is decided here, once: `snapshots_to_morsel` emits rows in the
        order it receives them. Newest first, breaking ties on `snapshot_id` so
        two commits sharing a millisecond do not order arbitrarily between runs.

        Expired snapshots are absent - the catalog's loader tombstones them out
        of the history it returns. A TAGGED snapshot can never be one of them: a
        tag holds its snapshot from expiry, which is why the `tags` column is
        also the answer to "why is this old snapshot still here".
        """
        from opteryx.models.snapshot_history import normalize_snapshot

        dataset = self.catalog.load_dataset(self.dataset, load_history=True)
        snapshots = dataset.snapshots()
        if not snapshots:
            return []

        # The catalog's head pointer. `current`, not `latest`: a rollback moves
        # it BACKWARDS, so the snapshot it names is not necessarily the newest
        # one generated - the same reason the stored key has always been
        # `current-snapshot-id`.
        current_snapshot_id = dataset.metadata.current_snapshot_id
        ordered = sorted(
            snapshots, key=lambda s: (s.timestamp_ms, s.snapshot_id), reverse=True
        )

        # Tags point AT snapshots, so they are grouped by target here rather than
        # read off each snapshot. One subcollection read for the whole statement,
        # on a path that is already doing a full history load.
        tags_by_snapshot: dict = {}
        for tag in self.catalog.list_tags(self.dataset):
            tags_by_snapshot.setdefault(tag["snapshot-id"], []).append(tag["name"])

        # `current` and `previous` are VIRTUAL tags: neither is in the tags
        # subcollection and neither pins anything, they simply name the snapshots
        # those two words resolve to today. They appear in this column because
        # that is where a reader looks to find out what a name resolves to, and
        # `VERSION AS OF current` reads exactly like `VERSION AS OF <any real
        # tag>`. They are added here, after the real tags, so no dataset can be
        # missing them and none can have two. `create_tag` refuses both names for
        # the same reason.
        #
        # `previous` is the previous version of the DATA, NOT the previous
        # snapshot: compaction and statistics refresh commit snapshots that
        # rewrite files without changing a row, and putting `previous` on one of
        # those would name a snapshot holding exactly the data an unqualified read
        # already returns - a time-travel answer indistinguishable from no time
        # travel at all. `previous_user_snapshot` walks past them, and is the SAME
        # resolver `VERSION AS OF PREVIOUS` reads through, so the name shown here
        # and the name written in SQL cannot drift apart.
        #
        # The dataset is already history-loaded, so that walk's parent hops are in
        # memory - this column costs no extra catalog round trip.
        previous_snapshot = dataset.previous_user_snapshot()
        previous_snapshot_id = (
            previous_snapshot.snapshot_id if previous_snapshot is not None else None
        )

        def _virtual_tags(snapshot_id: int) -> list:
            # Ordered, not sorted: `current` before `previous` reads as the
            # timeline does. The two cannot land on the same snapshot -
            # `previous_user_snapshot` begins its second walk at the parent of the
            # user commit the head rests on, so it can never return the head.
            names = []
            if current_snapshot_id is not None and snapshot_id == current_snapshot_id:
                names.append("current")
            if previous_snapshot_id is not None and snapshot_id == previous_snapshot_id:
                names.append("previous")
            return names

        return [
            normalize_snapshot(
                snapshot,
                current_snapshot_id,
                tags=sorted(tags_by_snapshot.get(snapshot.snapshot_id, []))
                + _virtual_tags(snapshot.snapshot_id),
            )
            for snapshot in ordered
        ]

    def _resolve_snapshot(self) -> None:
        """Settle which snapshot this read sees, honouring time travel.

        Sets `self.snapshot` and `self.snapshot_id`. Shared by the schema-only and
        the full-metadata reads so a statement cannot resolve to one snapshot when
        checked and a different one when run.
        """
        if self.version_tag is not None:
            # A tag is resolved by NAME, through the catalog, as one document get
            # by id - `tags/{name}` under the dataset. NOT by scanning any
            # in-memory tag map: that map is populated only by a history load, and
            # this path deliberately does not do one, so a scan would find nothing
            # and report every tag as unknown.
            #
            # A tag pins its snapshot from expiry for as long as it exists, so a
            # tag that resolves to a snapshot which then cannot be read is a BROKEN
            # PIN, not a stale reference to shrug at. Both failures below say what
            # they are and stop; neither falls back to current data, which would
            # answer a question about February with March's numbers.
            from opteryx_catalog.exceptions import TagNotFound

            if self.version_tag.lower() == "current":
                # The virtual tag. It names the head, which is where an
                # unqualified read already goes, so this resolves without a
                # catalog lookup and cannot fail the way a real tag can. It
                # exists so the name a reader sees in `SHOW SNAPSHOTS` is a name
                # they can also write - and `create_tag` refuses to let anything
                # take it, so no real tag can shadow this branch.
                self.snapshot = self.table.snapshot()
                if self.snapshot is None:
                    raise DatasetReadError(
                        f"The dataset {self.dataset} exists, but no data has been "
                        "committed to it yet."
                    )
                self.snapshot_id = self.snapshot.snapshot_id
                return

            try:
                snapshot_id = self.catalog.resolve_tag(self.dataset, self.version_tag)
            except TagNotFound as exc:
                # Translated at the boundary, as DatasetNotFound already is above:
                # a catalog exception type is not something a reader of SQL should
                # ever see. Deliberately does NOT list the tags that do exist -
                # someone who cannot see a dataset's tags must not learn them from
                # a failed guess.
                raise DatasetReadError(
                    f"No tag {self.version_tag} on {self.dataset}."
                ) from exc

            target = self.table.snapshot(snapshot_id)
            if target is None:
                raise DatasetReadError(
                    f"Tag {self.version_tag} of {self.dataset} names snapshot {snapshot_id}, "
                    "which could not be read. A tag holds its snapshot from expiry, so this "
                    "is a broken pin rather than an expired version."
                )

            self.snapshot_id = target.snapshot_id
            self.snapshot = target

        elif self.version is not None:
            # No history reload: the current snapshot is already in memory (set at
            # construction), and a snapshot fetched by id is a single targeted
            # lookup (Dataset.snapshot's own doc, not the whole history) - see
            # Dataset.snapshot in opteryx_catalog. VERSION AS OF never needs every
            # snapshot, only the one it names.
            current = self.table.snapshot()
            if current is None:
                raise DatasetReadError(
                    f"The dataset {self.dataset} exists, but no data has been committed to it yet."
                )

            if self.version == 0:
                # The rewriter's sentinel for VERSION AS OF PREVIOUS.
                #
                # NOT `current.parent_snapshot_id`. Somebody asking for the
                # previous version is asking about their DATA, and compaction
                # and statistics refresh commit snapshots that change no rows -
                # so the literal parent is routinely the same data this read
                # would return with no time-travel clause at all, which is the
                # one answer a time-travel read must never give. The catalog
                # walks past those; see `previous_user_snapshot`.
                previous = self.table.previous_user_snapshot()
                if previous is None:
                    raise DatasetReadError(
                        f"No previous version for {self.dataset} - snapshot "
                        f"{current.snapshot_id} is the earliest version of its data."
                    )
                target_id = previous.snapshot_id
            else:
                target_id = self.version

            target = self.table.snapshot(target_id)
            if target is None:
                raise DatasetReadError(
                    f"No snapshot {target_id} for dataset {self.dataset} - it may not exist, or may have expired."
                )

            self.snapshot_id = target.snapshot_id
            self.snapshot = target

        elif self.at_date is not None:
            # reload the dataset with history enabled
            self.table = self.catalog.load_dataset(self.dataset, load_history=True)
            snapshots = self.table.snapshots()

            if not snapshots:
                raise DatasetReadError("No data available for the specified date.")

            # Only the history the head can actually see. A rollback moves the
            # head backwards and leaves the snapshots it moved off live, so
            # without this a point-in-time read would happily return the version
            # that was rolled back - the one version the dataset's owner has
            # said nobody should be reading. Naming such a snapshot's id
            # explicitly still reads it: the version is retired, not hidden.
            #
            # The rule lives in the catalog (`visible_history`), with the
            # `last_user_snapshot` and expiration uses of it, so a rolled-off
            # snapshot cannot be invisible to one of them and visible to another.
            from opteryx_catalog.catalog.dataset import visible_history

            snapshots = visible_history(self.table.snapshot(), snapshots)
            if not snapshots:
                raise DatasetReadError("No data available for the specified date.")

            snapshots = sorted(snapshots, key=lambda s: s.timestamp_ms, reverse=False)

            # Honor dates before the first snapshot by rejecting them, but treat
            # dates after the newest snapshot as selecting the newest snapshot
            first_committed = snapshots[0].timestamp_ms
            last_committed = snapshots[-1].timestamp_ms

            at_ms = int(self.at_date.timestamp() * 1000)

            if at_ms < first_committed:
                # Point-in-time read is before our first snapshot — no data available then
                import datetime

                first_timestamp = datetime.datetime.fromtimestamp(first_committed / 1000)
                raise DatasetReadError(
                    f"No data available for the specified date - first available snapshot is {first_timestamp}."
                )
            elif at_ms > last_committed:
                # Point-in-time read after the newest snapshot — return the data
                # a read with no time-travel clause would return
                selected = snapshots[-1]
            else:
                selected = snapshots[0]
                for candidate in snapshots:
                    if candidate.timestamp_ms <= at_ms:
                        selected = candidate
                    else:
                        break

            self.snapshot_id = selected.snapshot_id
            self.snapshot = self.table.snapshot(self.snapshot_id)

            # Only reachable from this branch: a snapshot the history listed but
            # the catalog would not hand back. It is NOT the no-data case handled
            # below - falling through to that would answer a question about
            # February with "this relation has never been written to".
            if self.snapshot is None:
                raise DatasetReadError(
                    f"Snapshot {self.snapshot_id} of {self.dataset} is in the relation's "
                    "history but could not be read."
                )

        else:
            # No time-travel clause: the read sees the head.
            #
            # A relation with NO head has a registered schema and has never been
            # committed to. That is the ONLY thing zero snapshots can mean here:
            # `truncate()` appends a snapshot rather than clearing the chain, and
            # nothing in the catalog ever clears `current-snapshot-id`, so this
            # cannot be a half-written document or an expiry artefact - and a
            # relation that does not exist at all raised DatasetNotFound before
            # this reader was constructed.
            #
            # Such a relation reads as the schema it declares and no rows, which
            # is the answer a TRUNCATEd relation already gives (its snapshot
            # carries an empty manifest, and the scan serves that as one empty
            # morsel). Erring here instead made the two states differ by an
            # exception while being identical in data.
            #
            # `snapshot` and `snapshot_id` are left None rather than filled with
            # a fabricated Snapshot: the two readers below branch on that
            # explicitly, so nothing downstream resolves a schema, a scan or a
            # commit timestamp against a snapshot that does not exist.
            self.snapshot = self.table.snapshot()
            self.snapshot_id = None if self.snapshot is None else self.snapshot.snapshot_id

    def get_dataset_metadata(self) -> Tuple[RelationSchema, Manifest]:
        """
        Get dataset schema and build manifest from catalog.

        Returns both schema and manifest to make the dual purpose explicit.
        Manifest contains file-level statistics from table.scan().

        Returns:
            Tuple of (RelationSchema, Manifest)
        """
        self._resolve_snapshot()

        if self.snapshot is None:
            # Nothing committed - see _resolve_snapshot. The relation is served
            # as declared with no files, which the scan turns into a single
            # empty morsel: the same result a TRUNCATEd relation gives through
            # its own empty manifest. There is no scan to run and no commit to
            # timestamp, so neither is invented.
            #
            # `bounds_are_ordinal` is still demanded of the dataset even though
            # an empty file list carries no bounds to misread: the declaration
            # is a property of the metastore implementation, not of whether it
            # holds data, so a backend missing it fails on the first READ rather
            # than silently waiting for the first commit to corrupt pruning.
            self.dataset_committed_at = None
            self.schema = self.get_declared_schema()
            bounds_are_ordinal = getattr(self.table, "bounds_are_ordinal", None)
            if bounds_are_ordinal is None:
                raise DatasetReadError(
                    f"{type(self.table).__name__} does not declare `bounds_are_ordinal`, so the "
                    "encoding of its manifest min/max bounds is unknown. Implementations of "
                    "opteryx-catalog's `Dataset` must set it (True for Vector.ordinalize() keys, "
                    "False for real decoded values); guessing either way silently corrupts pruning."
                )
            self.manifest = Manifest(
                files=[],
                schema=self.schema,
                bounds_are_ordinal=bounds_are_ordinal,
            )
            return self.schema, self.manifest

        raw_schema = self.table.schema(self.snapshot.schema_id)
        self.schema = self._normalize_schema(raw_schema, relation_name=self.dataset)
        self.dataset_committed_at = self.snapshot.timestamp_ms

        # Build Manifest from catalog table.scan()
        # scan() returns an iterable of DataFile objects
        scan = self.table.scan(snapshot_id=self.snapshot_id)

        # Build FileEntry for each file
        file_entries = []
        protocols = set()

        # The manifest rows carry per-column stats as POSITIONAL lists in schema
        # order and no `field_ids` key of their own, but every reader of those
        # stats resolves a column through `Manifest._resolve_field_id`, which
        # returns the catalog field_id this schema assigns. Hand the schema's
        # field ids down so both sides speak one key space - see the keying note
        # in `FileEntry.from_datafile` for what the mismatch silently did.
        schema_field_ids = [column.field_id for column in self.schema.columns]

        for data_file in scan:
            file_entry = FileEntry.from_datafile(data_file, schema_field_ids=schema_field_ids)
            file_entries.append(file_entry)

            # Extract protocol for validation (gs://, s3://, file://)
            if "://" in file_entry.file_path:
                protocol = file_entry.file_path.split("://")[0]
                protocols.add(protocol)

        # Validate all files use same protocol
        if len(protocols) > 1:
            raise DatasetReadError(
                f"Mixed protocols in manifest: {protocols}. All files must use the same protocol."
            )

        # Whole-column native sketch vectors (min_k_hashes / histogram_counts) from
        # the same cached manifest read, so the planner reduces them with native
        # kernels instead of the per-file boxed lists. A backend that does not
        # implement the accessor keeps working - via the Manifest's Python
        # fallback if it emits per-file sketch stats, and with no sketches at all
        # if it does not - and _warn_no_native_sketches reports which of those it
        # is, once per backend class, at a severity matching whether it is
        # actually fixable. Note this branch is NOT reached by a backend that
        # implements the accessor and returns {} to declare "no sketches"; that
        # is the supported way to say so and is not reported at all.
        sketch_vectors_fn = getattr(self.table, "manifest_sketch_vectors", None)
        if sketch_vectors_fn is not None:
            sketch_vectors = sketch_vectors_fn(self.snapshot_id)
        else:
            sketch_vectors = {}
            _warn_no_native_sketches(self.table)

        # Merge-on-read deletes: resolve each delete-bearing file's row
        # ordinals NOW, at binding, from the dataset's sidecar(s) — one small
        # cached read via the same manifest LRU the scan itself warmed. The
        # positions ride on the FileEntry so the read node can subtract them
        # per row group without any further catalog round-trip. Resolution is
        # fail-closed on both sides: the dataset raises if a referenced
        # sidecar is unreadable, and a FileEntry left with
        # deleted_record_count > 0 but no positions is refused by the reader
        # — either way deleted rows are never silently served.
        if any(fe.deleted_record_count for fe in file_entries):
            delete_vectors_fn = getattr(self.table, "delete_vectors", None)
            if delete_vectors_fn is None:
                raise DatasetReadError(
                    f"{type(self.table).__name__} reports merge-on-read deletes but does not "
                    "implement delete_vectors(); refusing to scan and resurrect deleted rows."
                )
            resolved = delete_vectors_fn(self.snapshot_id)
            for fe in file_entries:
                if not fe.deleted_record_count:
                    continue
                positions = resolved.get(fe.file_path)
                if positions is None:
                    raise DatasetReadError(
                        f"Manifest attributes {fe.deleted_record_count} deleted rows to "
                        f"{fe.file_path} but the delete sidecar holds no vector for it."
                    )
                fe.delete_positions = tuple(positions)

        # Create Manifest with files and schema.
        #
        # bounds_are_ordinal is asked of the DATASET, never assumed here. This
        # connector serves every metastore opteryx-catalog's `Dataset` interface
        # covers -- the native catalog (ordinal keys) and external catalogs such
        # as opteryx-iceberg (real decoded values, `from_bytes` off the Iceberg
        # manifest) -- and the encoding travels with whoever produced the bounds.
        # Hardcoding True here read an Iceberg VARCHAR's real `str` bound as an
        # ordinal int64: `SELECT * ... WHERE <int col> ... ORDER BY ... LIMIT n`
        # raised "'<' not supported between instances of 'str' and 'int'" out of
        # the unguarded max/min in the planner's _narrow_filter_columns, and --
        # worse, because it was silent -- `WHERE <double col> >= 250.0` pruned
        # every file and returned ZERO rows.
        #
        # A dataset that declares nothing is an ERROR, not a defaulting case:
        # True and False are each silently wrong for one of the two producers.
        bounds_are_ordinal = getattr(self.table, "bounds_are_ordinal", None)
        if bounds_are_ordinal is None:
            raise DatasetReadError(
                f"{type(self.table).__name__} does not declare `bounds_are_ordinal`, so the "
                "encoding of its manifest min/max bounds is unknown. Implementations of "
                "opteryx-catalog's `Dataset` must set it (True for Vector.ordinalize() keys, "
                "False for real decoded values); guessing either way silently corrupts pruning."
            )
        #
        # For the native catalog (True) the stats builder stores min/max as
        # `Vector.ordinalize()` keys, not real values (see the catalog's
        # _compute_column_stats). For most types that key IS the value — an
        # identity widen for signed ints, and for DATE/TIMESTAMP/TIME the raw
        # physical integer, which is also what the binder normalises those
        # literals to — which is why pruning appeared to work. FLOAT is the
        # exception and was silently WRONG: its ordinal key is an
        # order-preserving BIT transform, so a file whose gm ranges 0.1..0.9
        # stored bounds of 4591870180066957722..4606281698874543309, and
        # `WHERE gm = 0.5` compared 0.5 against those and pruned the file that
        # actually held the matching rows. Declaring the encoding sends
        # predicate literals through ColumnType.ordinalize first, so both
        # sides are in the same space.
        self.manifest = Manifest(
            files=file_entries,
            schema=self.schema,
            min_k_vector=sketch_vectors.get("min_k_hashes"),
            histogram_vector=sketch_vectors.get("histogram_counts"),
            char_class_vector=sketch_vectors.get("char_class_counts"),
            bounds_are_ordinal=bounds_are_ordinal,
        )

        return self.schema, self.manifest


class OpteryxConnector(Eidetic, Writable, PredicatePushable):
    """
    Long-lived Opteryx catalog gateway supporting multiple catalogs.

    This connector handles:
    - Multi-catalog management (lazy instantiation)
    - Object introspection (locate_object)
    - View operations (create/drop/list views)
    - Factory method for creating table engines
    """

    eidetic = True

    # Capability declarations - what OpteryxTable readers support
    supports_diachronic = True  # Time-travel via OpteryxTable
    supports_version_travel = True  # VERSION AS OF <snapshot id / PREVIOUS>
    supports_predicate_pushdown = True  # Via FileSystemTable base
    supports_limit_pushdown = True  # Via FileSystemTable base
    supports_statistics = True  # Opteryx manifests provide stats
    requires_execution_context = True  # information_schema row-level permission filtering

    PUSHABLE_OPS: Dict[str, bool] = {
        "Eq": True,
        "NotEq": True,
        "Gt": True,
        "GtEq": True,
        "Lt": True,
        "LtEq": True,
        "Like": True,
        "NotLike": True,
        "ILike": True,
        "NotILike": True,
        "InStr": True,
        "NotInStr": True,
        "IInStr": True,
        "NotIInStr": True,
        "InList": True,
        "NotInList": True,
        "RLike": True,
        "NotRLike": True,
        "Between": True,
        "IsNull": True,
        "IsNotNull": True,
        "IsEmpty": True,
        "IsNotEmpty": True,
    }

    def __init__(self, *args, catalog=None, telemetry=None, **kwargs):
        """
        Initialize the Opteryx catalog connector.

        Args:
            catalog: Optional pre-configured catalog instance or catalog factory function
            **kwargs: Configuration (firestore_project, firestore_database, gcs_bucket, etc.)
        """
        Eidetic.__init__(self, **kwargs)
        PredicatePushable.__init__(self, **kwargs)

        self.telemetry = telemetry
        self.kwargs = kwargs
        self.kwargs.pop("connector", None)
        self.kwargs.pop("prefix", None)
        self.catalog_factory = catalog

    def _get_catalog(self, catalog_name: str):
        """
        Get or create a catalog instance for the specified catalog name.

        Args:
            catalog_name: The catalog name to connect to

        Returns:
            Opteryx Catalog instance
        """
        # Require a catalog factory/class/instance to be configured
        if self.catalog_factory is None:
            raise ValueError("Opteryx connector requires a catalog parameter")

        # Ensure we have a per-connector cache for instantiated catalogs
        if getattr(self, "_catalog_cache", None) is None:
            self._catalog_cache = {}

        # Return cached instance when available - but not blindly. This
        # connector (and the module-level cache in
        # opteryx.connectors.connector_factory that hands connectors out) is
        # process-long-lived, keyed by workspace name for the life of the
        # process rather than per-query or per-request, and a production
        # deployment runs many such processes at once. The catalog's own
        # existence/deletion gate only ever runs in __init__ (see
        # opteryx_catalog.OpteryxCatalog.__init__), which a cache hit skips
        # entirely - so a workspace dropped by DROP WORKSPACE (run against
        # some OTHER process, or even this one before this fix) would stay
        # queryable from here indefinitely in every process that had already
        # cached it, forever bypassing the drop. A cache hit gets one cheap
        # re-check first: a single `$properties` doc read
        # (get_workspace_properties(), which deliberately does not itself
        # gate on deletion), not a full reconstruction. A cache miss already
        # goes through __init__'s gate for free below.
        #
        # An empty result means the `$properties` doc is gone - DROP
        # WORKSPACE removes it outright, it doesn't just flag it - and a
        # cache entry only exists because construction succeeded once
        # before, so "gone now" is unambiguous, not a workspace that merely
        # hasn't been provisioned yet.
        if catalog_name in self._catalog_cache:
            cached = self._catalog_cache[catalog_name]
            try:
                props = cached.get_workspace_properties()
                still_live = bool(props) and props.get("deleted-at-ms") is None
            except Exception:
                # A transient read failure here must not evict a perfectly
                # good cached handle over a blip - same conservative
                # direction the constructor's own read-failure handling
                # takes (opteryx_catalog.py: "don't fail catalog init on
                # transient Firestore errors, and don't claim a workspace is
                # missing when we simply couldn't look").
                still_live = True
            if still_live:
                return cached
            del self._catalog_cache[catalog_name]

        factory = self.catalog_factory

        # If an instance (non-callable, non-class) was provided, cache and return it
        if not isinstance(factory, type) and not callable(factory):
            self._catalog_cache[catalog_name] = factory
            return factory

        instance = None
        # If a class was provided, instantiate with workspace=catalog_name and allow exceptions to propagate
        if isinstance(factory, type):
            instance = factory(workspace=catalog_name, **self.kwargs)
        else:
            # Callable factory: call with workspace and let errors propagate
            instance = factory(workspace=catalog_name, **self.kwargs)

        # Serve manifest reads from the configured cache tiers (local disk, shared KV)
        # rather than re-fetching from object storage. We wrap the FileIO the catalog
        # chose for itself rather than constructing one, so the gcs/no-gcs decision stays
        # owned by the catalog. `io` is read when each Dataset is created, which happens
        # after this, so wrapping here takes effect.
        tiers = manifest_cache_tiers()
        if tiers:
            instance.io = CachingFileIO(instance.io, tiers)

        self._catalog_cache[catalog_name] = instance
        return instance

    def _parse_identifier(self, name) -> Tuple[str, str]:
        """
        Parse a fully qualified name into catalog and relative identifier.

        Accepts either a string (e.g. 'benchmarks.clickbench.hits') or an
        identifier tuple/list returned by some catalog APIs (e.g. ('clickbench', 'hits')).

        Returns a tuple of (catalog_name, relative_identifier).
        """
        # If caller passed an identifier tuple/list (catalog APIs often use these),
        # treat it as a relative identifier and use the default catalog.
        if isinstance(name, (tuple, list)):
            if len(name) == 0:
                return "default", ""
            # Join tuple parts into a dot-separated relative id
            return "default", ".".join(map(str, name))

        # Otherwise expect a string
        parts = str(name).split(".", 1)
        if len(parts) == 2:
            return parts[0], parts[1]
        else:
            return "default", str(name)

    def _try_load_dataset(self, catalog, identifier):
        """
        Attempt to load an object as a dataset.

        Returns (found: bool, dataset_or_error_msg: Any).
        If found, returns (True, dataset_object).
        If not found, returns (False, error_message) for diagnostics.
        """
        try:
            dataset = catalog.load_dataset(identifier)
            return True, dataset
        except Exception as err:
            logger.debug(f"Not a dataset '{identifier}': {err}")
            return False, str(err)

    def _try_load_view(self, catalog, identifier):
        """
        Attempt to load an object as a view.

        Returns (found: bool, view_or_error_msg: Any).
        If found, returns (True, view_object).
        If not found, returns (False, error_message) for diagnostics.
        """
        try:
            view = catalog.load_view(identifier)
            return True, view
        except Exception as err:
            logger.debug(f"Not a view '{identifier}': {err}")
            return False, str(err)

    def locate_object(self, name: str) -> Tuple[Optional[TableType], any]:
        """
        Ask the connector if it knows about a specific object (table or view).

        Attempts to load the object as a dataset first, then as a view.
        The order matters: if both exist with the same name, dataset takes precedence.

        Args:
            name: The fully qualified table/view name (catalog.namespace.name)

        Returns:
            Tuple of (TableType | None, metadata):
            - If table exists: (TableType.Table, table metadata)
            - If view exists: (TableType.View, view metadata)
            - If nothing exists: (None, None)
        """
        # Parse catalog name and relative identifier
        catalog_name, relative_id = self._parse_identifier(name)
        catalog = self._get_catalog(catalog_name)

        # Try to load as dataset first (explicit attempt, logged on failure)
        found, result = self._try_load_dataset(catalog, relative_id)
        if found:
            return TableType.Table, result

        # Try to load as view (explicit attempt, logged on failure)
        found, result = self._try_load_view(catalog, relative_id)
        if found:
            return TableType.View, result

        # Not found as either type
        return None, None

    def table_engine(self, name: str, **kwargs):
        """
        Create a table-specific engine for reading data.

        Args:
            name: The fully qualified table name (catalog.namespace.name)
            **kwargs: Additional parameters (telemetry, etc.)

        Returns:
            OpteryxTable instance configured for the specific table, or an
            information_schema table reader when the relative identifier's
            first segment is the reserved `information_schema` schema name.
        """
        # Parse catalog name and relative identifier
        workspace, relative_id = self._parse_identifier(name)
        catalog = self._get_catalog(workspace)

        # Pop so it never reaches OpteryxTable below - only information_schema uses it.
        execution_context = kwargs.pop("execution_context", None)

        schema_segment, _, info_table_name = relative_id.partition(".")
        if schema_segment == "information_schema" and info_table_name:
            from opteryx.connectors.information_schema import build_information_schema_table

            return build_information_schema_table(
                info_table_name,
                catalog=catalog,
                workspace=workspace,
                telemetry=kwargs.get("telemetry"),
                execution_context=execution_context,
            )

        # Merge stored kwargs with provided kwargs (provided takes precedence)
        merged_kwargs = {**self.kwargs, **kwargs}
        return OpteryxTable(
            dataset=relative_id, catalog=catalog, workspace=workspace, **merged_kwargs
        )

    def view_engine(self, name: str):
        """
        Get view definition (for expansion in AST).

        Args:
            name: The view name

        Returns:
            ViewDefinition object
        """
        return self.get_view(name)

    def get_relation(self, name: str):
        """Catalog resolution step: resolve a relation to its kind + payload in
        a single catalog round trip (one get_all over the dataset and view docs).

        Returns ('dataset', SimpleDataset), ('view', ViewDefinition) or
        (None, None). The dataset object can be handed back to table_engine via
        `prefetched_table=` so binding does not re-read the catalog.
        """
        from opteryx.connectors.capabilities.eidetic import ViewDefinition

        workspace, relative_id = self._parse_identifier(name)

        # information_schema is a reserved nested schema served by table_engine(),
        # not a catalog-stored dataset or view - skip the catalog round trip.
        if relative_id.partition(".")[0] == "information_schema":
            return None, None

        catalog = self._get_catalog(workspace)

        kind, obj = catalog.get_relation(relative_id)
        if kind == "view":
            return "view", ViewDefinition(
                name=obj.name,
                statement=obj.definition,
                owner=obj.metadata.author,
                last_row_count=obj.metadata.last_execution_records,
            )
        if kind == "dataset":
            return "dataset", obj
        return None, None

    # Relation operations (Writable capability)
    def _dataset_location(self, relation_name: str) -> str:
        """Resolve the GCS location data files for this relation live under.

        Called from `write_morsel`, which runs before the relation is
        necessarily registered in the catalog (CREATE OR REPLACE writes files
        before creating/replacing the catalog entry at EOS - see insert.pyx).
        For an existing relation this reads its real registered location; for
        one that doesn't exist yet, it mirrors the exact formula
        `catalog.create_dataset` will use for that identifier, since no
        location has been assigned yet.
        """
        workspace, relative_id = self._parse_identifier(relation_name)
        catalog = self._get_catalog(workspace)
        if catalog.dataset_exists(relative_id):
            return catalog.load_dataset(relative_id).metadata.location
        collection, dataset_name = relative_id.split(".")
        return f"gs://{catalog.gcs_bucket}/{catalog.workspace}/{collection}/{dataset_name}"

    def write_morsel(self, relation_name: str, morsel) -> FileEntry:
        """Write a morsel as a parquet file via the catalog's own GCS-aware
        FileIO. Opteryx has no GCS write path of its own; this reuses the
        exact write primitive `opteryx_catalog`'s `SimpleDataset.append`/
        `overwrite` use internally (`catalog.io.new_output(...)` +
        `rugo.parquet.write_parquet`), so a CTAS/REPLACE writing many morsels
        lands them the same way the catalog would land a single one.
        """
        from rugo.parquet import write_parquet

        from opteryx.utils import unique_id

        workspace, relative_id = self._parse_identifier(relation_name)
        catalog = self._get_catalog(workspace)
        location = self._dataset_location(relation_name)

        file_name = f"data-{unique_id()}.parquet"
        data_path = f"{location}/data/{file_name}"

        pdata = write_parquet(morsel, compression="zstd", bloom_filters=True)

        out = catalog.io.new_output(data_path).create()
        out.write(pdata)
        out.close()

        return FileEntry(
            file_path=data_path,
            file_format="PARQUET",
            record_count=len(morsel),
            file_size_in_bytes=len(pdata),
        )

    def create_relation(self, relation_name: str, schema, author: Optional[str] = None) -> None:
        """Create a new dataset in the catalog."""
        workspace, relative_id = self._parse_identifier(relation_name)
        catalog = self._get_catalog(workspace)
        catalog.create_dataset(relative_id, schema, author=author)

    def drop_relation(
        self, relation_name: str, if_exists: bool = False, author: Optional[str] = None
    ) -> None:
        """Drop a dataset from the catalog.

        This removes the dataset's catalog entry and snapshot history; the data
        files it referenced are left in storage, and the catalog tombstones the
        location so the expiration job can reclaim them.
        """
        workspace, relative_id = self._parse_identifier(relation_name)
        catalog = self._get_catalog(workspace)

        if not catalog.dataset_exists(relative_id):
            if if_exists:
                return
            raise DatasetNotFoundError(dataset=relation_name, connector=self.__class__.__name__)

        catalog.drop_dataset(relative_id, author=author)

    def collection_exists(self, collection_name: str) -> bool:
        """Check if a collection exists in the catalog."""
        workspace, relative_id = self._parse_identifier(collection_name)
        catalog = self._get_catalog(workspace)
        return catalog.collection_exists(relative_id)

    def create_collection(
        self, collection_name: str, if_not_exists: bool = False, author: Optional[str] = None
    ) -> None:
        """Create a collection in the catalog.

        Existence is the catalog's to decide, in one atomic call - deliberately
        NOT a `collection_exists` check followed by a create. That would be a
        race, and it would also make CREATE COLLECTION depend on
        `collection_exists`, which this connector's catalog does not currently
        provide (the same gap that blocks drop_collection).
        """
        workspace, relative_id = self._parse_identifier(collection_name)
        catalog = self._get_catalog(workspace)

        catalog.create_collection(relative_id, exists_ok=if_not_exists, author=author)

    def drop_collection(
        self, collection_name: str, if_exists: bool = False, author: Optional[str] = None
    ) -> None:
        """Drop an empty collection from the catalog.

        A collection owns no storage of its own, so unlike drop_relation this
        is not tombstoned - it either succeeds outright or is rejected because
        datasets/views remain in it.
        """
        from opteryx_catalog.exceptions import CollectionNotEmpty

        workspace, relative_id = self._parse_identifier(collection_name)
        catalog = self._get_catalog(workspace)

        if not catalog.collection_exists(relative_id):
            if if_exists:
                return
            raise DatasetNotFoundError(dataset=collection_name, connector=self.__class__.__name__)

        try:
            catalog.drop_collection(relative_id, author=author)
        except CollectionNotEmpty as exc:
            raise CollectionNotEmptyError(collection_name) from exc

    def truncate_relation(self, relation_name: str, author: Optional[str] = None) -> None:
        """Remove all rows from a dataset, retaining the dataset and its schema."""
        workspace, relative_id = self._parse_identifier(relation_name)
        catalog = self._get_catalog(workspace)
        catalog.load_dataset(relative_id).truncate(author=author)

    def set_cluster_by(
        self, relation_name: str, columns: List[str], author: Optional[str] = None
    ) -> None:
        """Set the dataset's clustering (sort-order) columns in the catalog.

        Replaces any previously configured sort order outright - CLUSTER BY
        re-declares the physical layout, it does not append to it.
        """
        workspace, relative_id = self._parse_identifier(relation_name)
        catalog = self._get_catalog(workspace)
        catalog.update_dataset_sort_order(relative_id, columns, author=author)

    def optimize_relation(self, relation_name: str, author: Optional[str] = None) -> bool:
        """Compact a dataset's small data files, via the catalog's compactor.

        Strategy is auto-detected by DatasetCompactor from the dataset's
        stored sort order (see set_cluster_by) - "brute" bin-packing with no
        sort order set, "performance" sort-aware compaction with one.
        """
        from opteryx_catalog.catalog import DatasetCompactor

        workspace, relative_id = self._parse_identifier(relation_name)
        catalog = self._get_catalog(workspace)
        dataset = catalog.load_dataset(relative_id)
        compactor = DatasetCompactor(dataset, strategy=None, author=author, agent="opteryx-sql")
        snapshot = compactor.compact(dry_run=False)
        return snapshot is not None

    def rename_relation(
        self, relation_name: str, new_relation_name: str, author: Optional[str] = None
    ) -> None:
        """Rename a dataset in the catalog, optionally moving it between collections.

        The catalog moves everything - data files, every snapshot's manifest,
        and the catalog entry - so the storage prefix keeps matching the
        relation name and no two datasets can ever share a location. Snapshot
        history survives the rename.

        That makes this O(all bytes), not a metadata edit: the catalog copies
        every file the dataset references (server-side, but still per-object),
        so renaming a large dataset is a long-running operation behind a
        statement that reads as instant. The vacated prefix is handed to the
        existing 24h reclamation sweep rather than deleted inline.
        """
        workspace, relative_id = self._parse_identifier(relation_name)
        new_workspace, new_relative_id = self._parse_identifier(new_relation_name)
        if workspace != new_workspace:
            raise InvalidInternalStateError(
                f"rename_relation reached the connector with two workspaces "
                f"({workspace} -> {new_workspace}); the planner should have rejected this."
            )

        catalog = self._get_catalog(workspace)
        catalog.rename_dataset(relative_id, new_relative_id, author=author)

    def set_workspace_property(
        self, workspace_name: str, property_name: str, value, author: Optional[str] = None
    ) -> None:
        """Set a property on the workspace's `$properties` document in the catalog.

        The catalog's setter merges, so sending the single changed property is
        enough - no read-modify-write here, and no window in which a concurrent
        change to a different property could be lost.
        """
        catalog = self._get_catalog(workspace_name)
        catalog.set_workspace_properties({property_name: value}, author=author)

    def drop_workspace(self, workspace_name: str, author: Optional[str] = None) -> None:
        """Permanently drop every dataset and view in the workspace, then the
        workspace itself. Refuses (raises) if deletion_protection is on -
        the catalog's own guard, checked inside OpteryxCatalog.drop_workspace,
        same gate `soft_delete_workspace` used to enforce.

        Evicts the connector's own cache entry for this workspace immediately
        rather than waiting for the next call's re-check (see _get_catalog) -
        this process's very next statement against the name should see it
        gone without a further round trip, even though other processes still
        need that re-check to catch up.
        """
        catalog = self._get_catalog(workspace_name)
        catalog.drop_workspace(author=author)
        self._catalog_cache.pop(workspace_name, None)

    def egress_verdict(
        self,
        target_relation: str,
        source_relations: "List[str]",
        secured: Optional[str] = None,
    ) -> "List[EgressRefusal]":
        """Which workspaces refuse to let this write copy their data out.

        The catalog owns the decision (`egress_protection` on the SOURCE
        workspace, which is on unless explicitly turned off); this method's job
        is to turn relation names into the workspaces they live in and ask.
        `enforce_egress_policy` is this plus a raise, inherited from `Writable`,
        so the resolution below happens in one place for both shapes.

        Sources in the target's own workspace are dropped before asking: a copy
        that stays inside one workspace is not egress, and it is by far the
        common case, so it must not cost a Firestore read. When nothing
        cross-workspace remains there is nothing to ask about at all.

        Any workspace's `$properties` is readable through any handle in the same
        Firestore database, so the target's catalog can answer for the sources
        without constructing a handle per source workspace - which would re-run
        the constructor's existence and soft-delete gates and raise for exactly
        the workspaces the question is about.
        """
        from opteryx.exceptions import EgressRestrictedError

        target_workspace, _ = self._parse_identifier(target_relation)

        source_workspaces = []
        for source in source_relations:
            source_workspace, _ = self._parse_identifier(source)
            if source_workspace == target_workspace:
                continue
            if source_workspace not in source_workspaces:
                source_workspaces.append(source_workspace)
        if not source_workspaces:
            return []

        catalog = self._get_catalog(target_workspace)

        # Fail closed on a catalog too old to hold the gate. Raising rather than
        # returning no refusals: an empty verdict means "nothing objected", and
        # a version skew is "nobody could be asked" - reporting the second as
        # the first would turn it into an unenforced security control, the one
        # outcome worse than refusing a legitimate copy.
        verdict = getattr(catalog, "egress_verdict", None)
        if verdict is None:
            raise EgressRestrictedError(
                f"Cannot write {target_relation} from another workspace's data: this "
                "deployment's opteryx-catalog is too old to evaluate egress protection. "
                "Upgrade opteryx-catalog, or run the statement within one workspace."
            )

        return [
            EgressRefusal(
                workspace=refusal.workspace,
                remediation=refusal.remediation,
                message=str(refusal),
            )
            for refusal in verdict(
                source_workspaces,
                target_workspace,
                f"write {target_relation}",
                secured=secured,
            )
        ]

    def mark_workspace_secure(
        self,
        workspace_name: str,
        object_identifier: str,
        destinations: "List[str]",
        author: Optional[str] = None,
    ) -> None:
        """Record a SECURE sanction on the SOURCE workspace's catalog entry.

        The handle is the source's, and the catalog only ever writes its own
        workspace's properties - which is what makes "only the source can
        sanction this" a fact about where the record lives rather than a check.
        """
        catalog = self._get_catalog(workspace_name)
        # Same skew posture as egress_verdict: a catalog without the SECURE API
        # cannot record the sanction, and a statement that reported success
        # while writing nothing would be the worst available outcome.
        mark = getattr(catalog, "mark_secure", None)
        if mark is None:
            raise NotImplementedError(
                f"Cannot mark {object_identifier} SECURE in {workspace_name}: this "
                "deployment's opteryx-catalog is too old to hold SECURE exemptions. "
                "Upgrade opteryx-catalog."
            )
        mark(object_identifier, destinations, author=author)

    def clear_workspace_secure(
        self, workspace_name: str, object_identifier: str, author: Optional[str] = None
    ) -> None:
        """Withdraw a SECURE sanction from the SOURCE workspace's catalog entry."""
        catalog = self._get_catalog(workspace_name)
        clear = getattr(catalog, "clear_secure", None)
        if clear is None:
            raise NotImplementedError(
                f"Cannot clear SECURE on {object_identifier} in {workspace_name}: this "
                "deployment's opteryx-catalog is too old to hold SECURE exemptions. "
                "Upgrade opteryx-catalog."
            )
        try:
            clear(object_identifier, author=author)
        except KeyError as exc:
            raise ValueError(
                f"{object_identifier} is not marked SECURE in workspace {workspace_name}"
            ) from exc

    def relation_exists(self, relation_name: str) -> bool:
        """Check whether a dataset exists in the catalog."""
        workspace, relative_id = self._parse_identifier(relation_name)
        catalog = self._get_catalog(workspace)
        return catalog.dataset_exists(relative_id)

    def insert(
        self,
        relation_name: str,
        file_entries,
        author: Optional[str] = None,
        commit_message: Optional[str] = None,
    ) -> None:
        """Commit pre-written parquet files into the catalog as a new snapshot,
        appended to whatever the dataset already contains.

        `commit_message` is passed through as given, including None: the catalog
        composes its own default ("add files by <author>") for an append that
        has nothing more specific to say."""
        workspace, relative_id = self._parse_identifier(relation_name)
        catalog = self._get_catalog(workspace)
        file_paths = [fe.file_path for fe in file_entries]
        self._commit(
            relation_name,
            lambda: catalog.load_dataset(relative_id).add_files(
                file_paths, author=author, commit_message=commit_message
            ),
        )

    def _commit(self, relation_name: str, commit):
        """Run one catalog commit, translating a lost race into the engine's own error.

        The store's exception type never travels into the engine - the same
        boundary rule `EgressRefusal` follows. What reaches the caller says what
        happened to THEIR statement, not what the metastore called it.

        No retry here, by decision: whether the work survives a race depends on
        what won it (an append leaves row addresses valid, a compaction does
        not), and the caller re-running is always correct where the engine
        guessing is only sometimes. See ConcurrentModificationError.
        """
        from opteryx_catalog.exceptions import SnapshotRaceError

        from opteryx.exceptions import ConcurrentModificationError

        try:
            return commit()
        except SnapshotRaceError as err:
            raise ConcurrentModificationError(relation_name) from err

    def merge_commit(
        self,
        relation_name: str,
        file_entries,
        delete_positions,
        author: Optional[str] = None,
        commit_message: Optional[str] = None,
        operation: str = "merge",
    ) -> None:
        """Commit pre-written parquet files and row-level deletes as ONE snapshot.

        The write half of MERGE - see `Writable.merge_commit` for why the two
        halves cannot be two commits. `delete_positions` maps data-file paths as
        they appear in the current manifest to file-local row ordinals; those
        paths are the same strings `write_morsel` produced and the scan read
        back, so no translation happens here.

        `commit_message` is passed through as given, including None: the catalog
        composes its own default naming the file and row counts.

        `operation` names the statement - the catalog stamps it on the snapshot
        and the audit record, and validates it against its own vocabulary."""
        workspace, relative_id = self._parse_identifier(relation_name)
        catalog = self._get_catalog(workspace)
        file_paths = [fe.file_path for fe in file_entries]
        self._commit(
            relation_name,
            lambda: catalog.load_dataset(relative_id).merge_commit(
                file_paths,
                delete_positions,
                author=author,
                commit_message=commit_message,
                operation=operation,
            ),
        )

    def replace_relation(
        self,
        relation_name: str,
        schema,
        file_entries,
        author: Optional[str] = None,
        commit_message: Optional[str] = None,
    ) -> None:
        """Atomically replace a dataset's entire contents with the given files,
        as a single new snapshot (CREATE OR REPLACE ... AS SELECT). Schema is
        unchanged - this does not evolve the dataset's schema.

        `commit_message` is passed through as given, including None: the catalog
        composes its own default ("truncate and add files by <author>") for a
        replace that has nothing more specific to say."""
        workspace, relative_id = self._parse_identifier(relation_name)
        catalog = self._get_catalog(workspace)
        file_paths = [fe.file_path for fe in file_entries]
        self._commit(
            relation_name,
            lambda: catalog.load_dataset(relative_id).truncate_and_add_files(
                file_paths, author=author, commit_message=commit_message
            ),
        )

    def relation_column_names(self, relation_name: str):
        """Return the dataset's current column names only (not full type fidelity)."""
        workspace, relative_id = self._parse_identifier(relation_name)
        catalog = self._get_catalog(workspace)
        schema = catalog.load_dataset(relative_id).schema()
        return [c.name for c in schema.columns]

    def relation_schema(self, relation_name: str) -> RelationSchema:
        """The dataset's current schema, whole - see Writable.relation_schema.

        Normalized on the way out, exactly as a scan normalizes it: the catalog
        stores a column's type as the STRING `str(ColumnType)` produces, and a
        caller rendering a column definition needs the object.
        """
        workspace, relative_id = self._parse_identifier(relation_name)
        catalog = self._get_catalog(workspace)
        raw_schema = catalog.load_dataset(relative_id).schema()
        return OpteryxTable._normalize_schema(raw_schema, relation_name=relation_name)

    def cluster_by_columns(self, relation_name: str) -> List[str]:
        """The dataset's clustering columns - see Writable.cluster_by_columns.

        The stored value is read by opteryx.models.sort_order, which knows the
        three shapes it has been written in; resolving a field id or a position
        to a name needs the schema, so it is loaded alongside.
        """
        from opteryx.models.sort_order import sort_order_column_names

        workspace, relative_id = self._parse_identifier(relation_name)
        catalog = self._get_catalog(workspace)
        dataset = catalog.load_dataset(relative_id)
        return sort_order_column_names(dataset.metadata.sort_orders, dataset.schema())

    def list_relationships(self, relation_name: str) -> List[dict]:
        """Relationships declared ON this dataset - see Writable.list_relationships.

        Broken rows are skipped: one whose column was dropped is a record of
        what went wrong, not a declaration to re-issue.
        """
        workspace, relative_id = self._parse_identifier(relation_name)
        catalog = self._get_catalog(workspace)
        declarations = []
        for relationship in catalog.list_relationships(relative_id):
            if relationship.get("status") == "broken":
                continue
            declarations.append(
                {
                    "constraint_name": relationship.get("name"),
                    "column_name": relationship.get("column"),
                    # The catalog stores the far end as collection + dataset,
                    # which is the split the parts list wants anyway.
                    "references_relation_parts": [
                        workspace,
                        relationship.get("references-collection"),
                        relationship.get("references-dataset"),
                    ],
                    "references_column_name": relationship.get("references-column"),
                    "cardinality": relationship.get("cardinality"),
                }
            )
        return declarations

    def relation_column_types(self, relation_name: str):
        """Return the dataset's current column name -> ColumnType mapping.

        The catalog stores each column's type as the STRING `str(ColumnType)`
        produces (`INT8`, `DECIMAL(10, 2)`, `TIMESTAMP[ms]`, `ARRAY<VARCHAR>`),
        so it is parsed back here rather than read off the schema column - which
        carries the spelling, not the object.
        """
        from opteryx.types.logical_type import parse_column_type

        workspace, relative_id = self._parse_identifier(relation_name)
        catalog = self._get_catalog(workspace)
        schema = catalog.load_dataset(relative_id).schema()
        return {c.name: parse_column_type(c.type) for c in schema.columns}

    def _alter_columns(self, relation_name: str, author: Optional[str], **changes) -> None:
        """Rewrite every data file to a new column shape and commit it.

        The catalog owns this end to end - it holds the storage IO, the manifest
        writer and the snapshot commit, exactly as it does for `rename_relation`
        and compaction. Doing the file half here instead would mean a second
        implementation of the commit protocol living outside the catalog that
        defines it.
        """
        from opteryx.exceptions import UnsupportedSyntaxError

        workspace, relative_id = self._parse_identifier(relation_name)
        catalog = self._get_catalog(workspace)
        dataset = catalog.load_dataset(relative_id)

        # Fail loudly on a catalog too old to carry column DDL, rather than
        # letting a bare AttributeError out. Same posture as `egress_verdict`.
        alter = getattr(dataset, "alter_columns", None)
        if alter is None:
            raise UnsupportedSyntaxError(
                "Cannot change this relation's columns: this deployment's "
                "opteryx-catalog is too old for column DDL. Upgrade opteryx-catalog."
            )

        alter(author=author, **changes)

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
        """Append a column, backfilling existing rows with `default` (NULL when
        none was given).

        `nullable` is carried into the schema but enforces nothing, and no
        default is stored for later inserts to consult - a column DEFAULT here
        is only the value written into the file for the rows that already
        exist. See `Writable.add_column`.
        """
        from opteryx.connectors.capabilities.writable import build_column_donor

        if if_not_exists and column_name in self.relation_column_names(relation_name):
            return
        self._alter_columns(
            relation_name,
            author,
            add=[
                {
                    "name": column_name,
                    "column_type": column_type,
                    "donor": build_column_donor(column_name, column_type, default),
                }
            ],
        )

    def drop_column(
        self,
        relation_name: str,
        column_name: str,
        if_exists: bool = False,
        author: Optional[str] = None,
    ) -> None:
        """Remove a column without decoding the ones that stay."""
        if if_exists and column_name not in self.relation_column_names(relation_name):
            return
        self._alter_columns(relation_name, author, drop=[column_name])

        # The column is gone, so every relationship through it now points at
        # nothing. Marked broken, never deleted - the row is the record that
        # this column was depended on, and it is what an owner reads to find
        # out. The plan-time guard already refused the case worth refusing (an
        # asserted relationship declared HERE); what is left is proposals and
        # inbound references from datasets this caller may not be able to see.
        #
        # After the drop, not before: a break recorded against a column that
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
        """Rename a column, touching no data at all.

        Unlike renaming the RELATION - which moves every byte so the storage
        prefix keeps matching the name - this rewrites only each file's footer.
        """
        self._alter_columns(
            relation_name, author, rename={old_column_name: new_column_name}
        )

    def alter_column_type(
        self, relation_name: str, column_name: str, new_type, author: Optional[str] = None
    ) -> None:
        """Re-declare a column as a wider type.

        The widening's legality was settled at bind time (`is_legal_widen`).
        Most of the lattice costs nothing on disk - parquet has no physical
        int8/int16, so INT8/INT16/INT32 all ride physical int32 - and only a
        widening to INT64/UINT64 re-encodes, and then only that column.
        """
        from opteryx.connectors.capabilities.writable import build_column_donor

        self._alter_columns(
            relation_name,
            author,
            retype={
                column_name: {
                    "column_type": new_type,
                    "donor": build_column_donor(column_name, new_type, None),
                }
            },
        )

    def is_materialized_view(self, relation_name: str) -> bool:
        """Whether the dataset carries the catalog's materialized-view marker."""
        workspace, relative_id = self._parse_identifier(relation_name)
        catalog = self._get_catalog(workspace)

        # A catalog without the MV API (older library, or a test double) has
        # no materialized views - checked before importing the MV exception
        # types, which the older library does not define either.
        if getattr(catalog, "get_materialized_view", None) is None:
            return False

        from opteryx_catalog.exceptions import DatasetNotFound
        from opteryx_catalog.exceptions import MaterializedViewError

        try:
            catalog.get_materialized_view(relative_id)
        except (DatasetNotFound, MaterializedViewError):
            return False
        return True

    def create_task(
        self,
        relation_name: str,
        statement: str,
        author: Optional[str] = None,
        or_replace: bool = False,
        writes: Optional[List[str]] = None,
    ) -> None:
        """Register a task in the catalog.

        The binder has already established that `author` could have run
        `statement` themselves and may own a task at all, so nothing here
        re-litigates either - this records what it is given, as every other
        connector write does.

        NO `runs_as` is passed, because a task carries no identity: it is stored
        SQL. `EXECUTE` runs it as the invoker, and an unattended run carries the
        TRIGGER's pinned owner, resolved from the trigger's own record when it
        fires. `author` is recorded for attribution, not authority.

        """
        try:
            from opteryx_catalog.exceptions import TaskAlreadyExists
        except ImportError:
            # An installed opteryx_catalog wheel that predates tasks - same
            # skew tolerance as drop_trigger below. The real TaskAlreadyExists
            # subclasses KeyError, so this stays correct once it arrives.
            TaskAlreadyExists = KeyError

        workspace, relative_id = self._parse_identifier(relation_name)
        catalog = self._get_catalog(workspace)

        try:
            catalog.create_task(
                relative_id,
                sql=statement,
                author=author,
                update_if_exists=or_replace,
                # Derived from the statement's own AST, so it cannot disagree
                # with it. Passed unconditionally - a catalog too old to accept
                # it raises here rather than recording a task whose outputs are
                # silently invisible to the workflow graph.
                writes=list(writes or []),
            )
        except TaskAlreadyExists as exc:
            raise ValueError(
                f"task {relation_name} already exists "
                "(use CREATE OR REPLACE TASK to redefine it)"
            ) from exc

    def drop_task(
        self, relation_name: str, if_exists: bool = False, author: Optional[str] = None
    ) -> None:
        """Drop a task from the catalog.

        The catalog's own `drop_task` returns quietly when the task is absent,
        so the not-found case is detected here rather than caught - that is what
        makes plain DROP TASK an error and DROP TASK IF EXISTS a no-op.
        """
        try:
            from opteryx_catalog.exceptions import TaskNotFound
        except ImportError:
            TaskNotFound = KeyError

        workspace, relative_id = self._parse_identifier(relation_name)
        catalog = self._get_catalog(workspace)

        if not if_exists:
            try:
                catalog.get_task(relative_id)
            except TaskNotFound as exc:
                raise ValueError(
                    f"task {relation_name} does not exist "
                    "(use DROP TASK IF EXISTS to make this quiet)"
                ) from exc

        catalog.drop_task(relative_id, author=author)

    def task_writes(self, relation_name: str) -> List[str]:
        """The relations the task's statement writes, from its catalog record."""
        workspace, relative_id = self._parse_identifier(relation_name)
        catalog = self._get_catalog(workspace)

        from opteryx_catalog.exceptions import TaskError
        from opteryx_catalog.exceptions import TaskNotFound

        try:
            record = catalog.get_task(relative_id)
        except (TaskNotFound, TaskError) as exc:
            raise ValueError(f"{relation_name} is not a task") from exc

        # Returned exactly as recorded. These are the names `plan_create_task`
        # derived from the statement's AST and `visit_create_task` checked WRITE
        # on, in that spelling - so checking READ on the same strings asks the
        # capability the same question it already answered once.
        return list(record.get("writes") or [])

    def add_listener(self, relation_name: str, user: str, outcome: str) -> None:
        """Record a subscription to a task's run outcomes."""
        from opteryx_catalog.exceptions import ListenerAlreadyExists

        workspace, relative_id = self._parse_identifier(relation_name)
        catalog = self._get_catalog(workspace)

        try:
            catalog.add_listener(relative_id, user=user, outcome=outcome)
        except ListenerAlreadyExists as exc:
            # Rendered as the SQL that changes it. The statement knows both
            # halves - the task and the outcome just asked for - so it hands the
            # caller the pair to run rather than describing them.
            raise ValueError(
                f"You already listen to **{relation_name}**. A task has one "
                f"subscription per user; change it with: **UNLISTEN** "
                f"{relation_name}; **LISTEN TO** {relation_name} **FOR "
                f"{outcome}**"
            ) from exc

    def drop_listener(self, relation_name: str, user: str) -> None:
        """Remove a subscription to a task's run outcomes."""
        from opteryx_catalog.exceptions import ListenerNotFound

        workspace, relative_id = self._parse_identifier(relation_name)
        catalog = self._get_catalog(workspace)

        try:
            catalog.drop_listener(relative_id, user=user)
        except ListenerNotFound as exc:
            raise ValueError(
                f"You do not listen to **{relation_name}**, so there is nothing "
                "to unlisten. **SHOW LISTENERS** lists what you do listen to."
            ) from exc

    def list_listeners_for_user(self, user: str) -> List[dict]:
        """The caller's own subscriptions in this connector's workspace."""
        catalog = self._get_catalog(self.workspace)

        # A catalog that predates listeners has none - the same skew tolerance
        # `is_task` applies to the task API itself.
        lister = getattr(catalog, "list_listeners_for_user", None)
        if lister is None:
            return []

        return [
            {
                "task_catalog": row.get("workspace"),
                "task_collection": row.get("collection"),
                "task_name": row.get("task"),
                "outcome": row.get("outcome"),
                "created_at": row.get("created-at-ms"),
            }
            for row in lister(user)
        ]

    def _holder_kwargs(self, relation_name: str) -> dict:
        """`{"holder_kind": "task"}` when `relation_name` is a task, else `{}`.

        The catalog keys a trigger on what HOLDS it: the dataset whose commits
        fire a commit trigger, or the task a schedule or signal trigger fires.
        Its trigger methods take `holder_kind` to say which, defaulting to a
        dataset - so the keyword is passed only when the holder is a task, and
        a dataset call keeps exactly the shape it had. That is what lets this
        connector sit in front of an older catalog, or a test double, that has
        never heard of task-held triggers.
        """
        return {"holder_kind": "task"} if self.is_task(relation_name) else {}

    def create_trigger(
        self,
        relation_name: str,
        trigger_name: str,
        task_name: str,
        author: Optional[str] = None,
        or_replace: bool = False,
        event_kind: str = "commit",
        schedule: Optional[str] = None,
        time_zone: Optional[str] = None,
        window_source: Optional[str] = None,
    ) -> None:
        """Attach a task trigger to its holder: the dataset whose commits fire
        it, or - for a schedule or signal trigger - the task itself."""
        from opteryx_catalog.exceptions import MaterializedViewError

        workspace, relative_id = self._parse_identifier(relation_name)
        catalog = self._get_catalog(workspace)

        holder = {}
        event = {}
        if event_kind != "commit":
            # No source dataset: the trigger lives under the task it fires. The
            # event keywords go only on this path, so a commit trigger's call is
            # byte-for-byte what it was before the other two events existed.
            holder = {"holder_kind": "task"}
            source = None
            if window_source:
                source_workspace, source = self._parse_identifier(window_source)
                if source_workspace != workspace:
                    # The binder refuses this first; kept here so the connector
                    # never lands a window it could not bind at fire time.
                    raise ValueError(
                        f"trigger {trigger_name} on {relation_name} cannot be windowed "
                        f"over {window_source}, which is in workspace {source_workspace}; "
                        "the window is read from a dataset in the task's own workspace"
                    )
            event = {
                "event_kind": event_kind,
                "schedule": schedule,
                "time_zone": time_zone,
                "window_source": source,
            }

        if or_replace:
            # `create_trigger` refuses to repoint an existing trigger, which is
            # right for the implicit MV path but is exactly what OR REPLACE asks
            # for. Drop first so the guard has nothing to refuse.
            catalog.drop_trigger(relative_id, trigger_name, author=author, missing_ok=True, **holder)

        try:
            catalog.create_trigger(
                relative_id,
                trigger_name,
                target_task=task_name,
                kind="task",
                author=author,
                **holder,
                **event,
            )
        except MaterializedViewError as exc:
            # The repoint guard, and the one-trigger rule. Surfaced as ValueError
            # so the statement reads as a caller error rather than an MV failure,
            # which it is not.
            raise ValueError(str(exc)) from exc

    def set_trigger_owner(
        self,
        relation_name: str,
        trigger_name: str,
        new_owner: str,
        author: Optional[str] = None,
    ) -> None:
        """Repoint the identity a trigger's unattended runs execute as."""
        from opteryx_catalog.exceptions import TriggerNotFound

        workspace, relative_id = self._parse_identifier(relation_name)
        catalog = self._get_catalog(workspace)

        try:
            catalog.set_trigger_owner(
                relative_id,
                trigger_name,
                new_owner,
                author=author,
                **self._holder_kwargs(relation_name),
            )
        except TriggerNotFound as exc:
            raise ValueError(
                f"trigger {trigger_name} does not exist on {relation_name}"
            ) from exc

    def set_trigger_suspended(
        self,
        relation_name: str,
        trigger_name: str,
        suspended: bool,
        author: Optional[str] = None,
    ) -> None:
        """Suspend or resume a trigger, leaving it in place either way."""
        from opteryx_catalog.exceptions import TriggerNotFound

        workspace, relative_id = self._parse_identifier(relation_name)
        catalog = self._get_catalog(workspace)

        try:
            catalog.set_trigger_suspended(
                relative_id,
                trigger_name,
                suspended,
                author=author,
                **self._holder_kwargs(relation_name),
            )
        except TriggerNotFound as exc:
            raise ValueError(
                f"trigger {trigger_name} does not exist on {relation_name}"
            ) from exc

    def set_trigger_minimum_interval(
        self,
        relation_name: str,
        trigger_name: str,
        seconds: int,
        author: Optional[str] = None,
    ) -> None:
        """Set the floor between two firings of a trigger in the catalog; 0 removes it."""
        from opteryx_catalog.exceptions import TriggerNotFound

        workspace, relative_id = self._parse_identifier(relation_name)
        catalog = self._get_catalog(workspace)

        try:
            catalog.set_trigger_minimum_interval(relative_id, trigger_name, seconds, author=author)
        except TriggerNotFound as exc:
            raise ValueError(
                f"trigger {trigger_name} does not exist on {relation_name}"
            ) from exc

    def is_task(self, relation_name: str) -> bool:
        """Whether the catalog holds a task under this name."""
        workspace, relative_id = self._parse_identifier(relation_name)
        catalog = self._get_catalog(workspace)

        # A catalog without the task API (older library, or a test double) has
        # no tasks - checked before importing the exception types, which the
        # older library does not define either.
        if getattr(catalog, "get_task", None) is None:
            return False

        from opteryx_catalog.exceptions import TaskError
        from opteryx_catalog.exceptions import TaskNotFound

        try:
            catalog.get_task(relative_id)
        except (TaskNotFound, TaskError):
            return False
        return True

    def task_definition(self, relation_name: str) -> str:
        """The task's current statement, from the catalog's statement record."""
        workspace, relative_id = self._parse_identifier(relation_name)
        catalog = self._get_catalog(workspace)

        from opteryx_catalog.exceptions import TaskError
        from opteryx_catalog.exceptions import TaskNotFound

        try:
            record = catalog.get_task(relative_id)
        except (TaskNotFound, TaskError) as exc:
            raise ValueError(f"{relation_name} is not a task") from exc

        sql = record.get("sql")
        if not sql:
            # Registered as a task but with no statement behind it - refuse
            # rather than execute nothing and report success.
            raise ValueError(
                f"task {relation_name} has no statement recorded; it cannot be "
                "executed. Recreate it with CREATE OR REPLACE TASK."
            )
        return sql

    def materialized_view_definition(self, relation_name: str) -> str:
        """The view's current defining SELECT, from the catalog's statement record."""
        workspace, relative_id = self._parse_identifier(relation_name)
        catalog = self._get_catalog(workspace)

        from opteryx_catalog.exceptions import DatasetNotFound
        from opteryx_catalog.exceptions import MaterializedViewError

        try:
            record = catalog.get_materialized_view(relative_id)
        except (DatasetNotFound, MaterializedViewError) as exc:
            raise ValueError(f"{relation_name} is not a materialized view") from exc

        sql = record.get("sql")
        if not sql:
            # Registered as a view but with no statement behind it - refuse
            # rather than refresh it into an empty table.
            raise ValueError(
                f"materialized view {relation_name} has no defining SELECT recorded; "
                "it cannot be refreshed. Recreate it with CREATE OR REPLACE "
                "MATERIALIZED VIEW."
            )
        return sql

    def materialized_view_sources(self, relation_name: str) -> List[str]:
        """The view's recorded sources, from the same record the definition comes from."""
        workspace, relative_id = self._parse_identifier(relation_name)
        catalog = self._get_catalog(workspace)

        from opteryx_catalog.exceptions import DatasetNotFound
        from opteryx_catalog.exceptions import MaterializedViewError

        try:
            record = catalog.get_materialized_view(relative_id)
        except (DatasetNotFound, MaterializedViewError) as exc:
            raise ValueError(f"{relation_name} is not a materialized view") from exc

        # The catalog spells this `source-tables`; the local store's sidecar
        # spells it `source_tables`. Each store's own spelling, read here.
        return list(record.get("source-tables") or [])

    def set_materialized_view_owner(
        self, relation_name: str, new_owner: str, author: str = None
    ) -> None:
        """Repoint `runs-as` on every refresh trigger of the view, in one catalog batch."""
        from opteryx_catalog.exceptions import MaterializedViewError

        workspace, relative_id = self._parse_identifier(relation_name)
        catalog = self._get_catalog(workspace)
        try:
            catalog.set_materialized_view_owner(relative_id, new_owner, author=author)
        except MaterializedViewError as exc:
            raise ValueError(f"ALTER MATERIALIZED VIEW {relation_name} OWNER TO: {exc}") from exc

    def set_materialized_view_suspended(
        self, relation_name: str, suspended: bool, author: str = None
    ) -> None:
        """Suspend or resume the view's automatic refresh in the catalog."""
        from opteryx_catalog.exceptions import MaterializedViewError

        workspace, relative_id = self._parse_identifier(relation_name)
        catalog = self._get_catalog(workspace)
        try:
            catalog.set_materialized_view_suspended(relative_id, suspended, author=author)
        except MaterializedViewError as exc:
            raise ValueError(f"ALTER MATERIALIZED VIEW {relation_name}: {exc}") from exc

    def mark_materialized_view_refreshed(
        self, relation_name: str, status: str, author: str = None
    ) -> None:
        """Stamp the view's refresh state after a successful manual refresh."""
        workspace, relative_id = self._parse_identifier(relation_name)
        catalog = self._get_catalog(workspace)
        catalog.mark_materialized_view_refreshed(relative_id, status=status, author=author)

    def register_materialized_view(
        self,
        relation_name: str,
        sql: str,
        source_tables,
        author: Optional[str] = None,
    ) -> None:
        """Register the (already-created) backing table as a materialized view.

        The catalog stores the defining SQL as a versioned statement, records
        the source list, and lands one refresh trigger on each source dataset.
        `update_if_exists=True` because this is the CoRTAS path's registration
        too - re-running the statement writes a new statement version and
        reconciles triggers against the new source list.

        Names are handed over fully qualified, which is how the catalog stores
        them. It accepts the workspace-relative form too, but stripping the
        workspace here would only put the ambiguity back: `a.b.c` cannot be read
        as a collection and a dotted dataset or as another workspace's table
        once the prefix is gone.
        """
        from opteryx_catalog.exceptions import MaterializedViewError

        workspace, _ = self._parse_identifier(relation_name)
        catalog = self._get_catalog(workspace)

        for source in source_tables:
            source_workspace, _ = self._parse_identifier(source)
            if source_workspace != workspace:
                raise ValueError(
                    f"materialized view {relation_name} cannot read across workspaces "
                    f"(source {source} is in workspace {source_workspace}); refresh "
                    "triggers only exist within the MV's own workspace"
                )

        try:
            catalog.create_materialized_view(
                relation_name,
                sql,
                list(source_tables),
                author=author,
                update_if_exists=True,
            )
        except MaterializedViewError as exc:
            raise ValueError(f"CREATE MATERIALIZED VIEW {relation_name}: {exc}") from exc

    def drop_materialized_view(
        self, relation_name: str, if_exists: bool = False, author: Optional[str] = None
    ) -> None:
        """Drop a materialized view: its refresh triggers, then its backing dataset."""
        from opteryx_catalog.exceptions import MaterializedViewError

        workspace, relative_id = self._parse_identifier(relation_name)
        catalog = self._get_catalog(workspace)

        if not catalog.dataset_exists(relative_id):
            if if_exists:
                return
            raise DatasetNotFoundError(dataset=relation_name, connector=self.__class__.__name__)

        try:
            catalog.drop_materialized_view(relative_id, author=author)
        except MaterializedViewError as exc:
            raise ValueError(
                f"{relation_name} is not a materialized view; use DROP TABLE or DROP VIEW"
            ) from exc

    def drop_trigger(
        self,
        relation_name: str,
        trigger_name: str,
        author: Optional[str] = None,
        missing_ok: bool = False,
    ) -> None:
        """Remove a trigger from the holder that carries it - a dataset, or a
        task for a schedule or signal trigger - delegating to the catalog. A
        missing trigger is translated into a clear ValueError unless missing_ok
        (IF EXISTS) - the catalog's own drop_trigger honours missing_ok, so that
        branch never raises."""
        try:
            from opteryx_catalog.exceptions import TriggerNotFound
        except ImportError:
            # An installed opteryx_catalog wheel that predates triggers (same
            # skew tolerance as information_schema._normalize_sort_order).
            # The real TriggerNotFound subclasses KeyError, so this stays
            # correct when the newer wheel arrives.
            TriggerNotFound = KeyError

        workspace, relative_id = self._parse_identifier(relation_name)
        catalog = self._get_catalog(workspace)

        try:
            catalog.drop_trigger(
                relative_id,
                trigger_name,
                author=author,
                missing_ok=missing_ok,
                **self._holder_kwargs(relation_name),
            )
        except TriggerNotFound as exc:
            raise ValueError(
                f"trigger {trigger_name} does not exist on {relation_name} "
                "(use DROP TRIGGER IF EXISTS to make this quiet)"
            ) from exc

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
        """Record one declared, unenforced relationship, delegating to the catalog.

        The catalog holds it as a subcollection under the dataset the
        constraint is declared on - the same shape as triggers - so "what
        relates to this dataset" is a keyed read. Nothing is enforced: a write
        that breaks the relationship succeeds.

        Names arrive split and are rejoined only at this boundary, because the
        catalog's API takes an identifier. What the catalog then STORES is split
        again, into its own fields; the dotted form never reaches storage.
        """
        workspace, relative_id = self._parse_identifier(".".join(relation_parts))
        _, references_relative_id = self._parse_identifier(".".join(references_relation_parts))
        catalog = self._get_catalog(workspace)
        catalog.declare_relationship(
            relative_id,
            constraint_name,
            column_name,
            references_relative_id,
            references_column_name,
            cardinality,
            author=author,
        )

    def drop_relationship(
        self,
        relation_parts: List[str],
        constraint_name: str,
        if_exists: bool = False,
        author: Optional[str] = None,
    ) -> bool:
        """Remove one declared relationship by name, delegating to the catalog.

        A missing constraint is translated into a clear ValueError unless
        `if_exists`, in which case the catalog returns False and nothing is
        raised.
        """
        try:
            from opteryx_catalog.exceptions import ConstraintNotFound
        except ImportError:
            # An installed opteryx_catalog wheel that predates declared
            # relationships - same skew tolerance as drop_trigger above. The
            # real ConstraintNotFound subclasses KeyError, so this stays correct
            # when the newer wheel arrives.
            ConstraintNotFound = KeyError

        workspace, relative_id = self._parse_identifier(".".join(relation_parts))
        catalog = self._get_catalog(workspace)
        try:
            return catalog.drop_relationship(
                relative_id, constraint_name, author=author, missing_ok=if_exists
            )
        except ConstraintNotFound as exc:
            raise ValueError(
                f"constraint {constraint_name} does not exist on {'.'.join(relation_parts)} "
                "(use DROP CONSTRAINT IF EXISTS to make this quiet)"
            ) from exc

    def relationships_through_column(
        self, relation_name: str, column_name: str
    ) -> List[dict]:
        """Declared relationships through one column, from the catalog.

        Normalised on the way out: the catalog stores hyphenated, split fields
        and the binder wants one flat shape it can read whichever connector
        answered. The far end is rejoined into a dotted string HERE and only
        for display - nothing reads it back, so the dots-in-names ambiguity the
        split storage exists to avoid is not reintroduced.

        Inbound rows are carried through with their flag intact and are NOT
        stripped: the binder needs to know they exist in order to not act on
        them, and dropping them here would make that decision unmakeable.
        """
        workspace, relative_id = self._parse_identifier(relation_name)
        catalog = self._get_catalog(workspace)

        # A catalog wheel that predates §9's verification has no such method.
        # Same skew tolerance as drop_relationship: the drop then proceeds
        # unguarded, exactly as it did before this existed.
        lookup = getattr(catalog, "relationships_through_column", None)
        if lookup is None:
            return []

        rows = []
        for row in lookup(relative_id, column_name):
            far = ".".join(
                part
                for part in (
                    row.get("references-collection"),
                    row.get("references-dataset"),
                    row.get("references-column"),
                )
                if part
            )
            rows.append(
                {
                    "constraint_name": row.get("name"),
                    "origin": row.get("origin"),
                    "status": row.get("status"),
                    "kind": row.get("kind"),
                    "inbound": bool(row.get("inbound")),
                    "references": far,
                }
            )
        return rows

    def break_relationships_through_column(
        self, relation_name: str, column_name: str, author: Optional[str] = None
    ) -> List[dict]:
        """Mark broken what the dropped column left pointing at nothing."""
        workspace, relative_id = self._parse_identifier(relation_name)
        catalog = self._get_catalog(workspace)

        breaker = getattr(catalog, "break_relationships_through_column", None)
        if breaker is None:
            return []
        return breaker(relative_id, column_name, author=author)

    def resolve_named_version(self, relation_name: str, word: str) -> int:
        """The snapshot id `CURRENT` or `PREVIOUS` names for this relation, now.

        The public face of `_resolve_version_spec` for EXECUTE's symbolic
        arguments: same resolver, same virtual-tag semantics (`previous` is the
        previous version of the DATA, walking past compaction), but with tags
        excluded - a task argument names a moment, and a tag smuggled through
        here would be a value the argument's word does not say it is.
        """
        workspace, relative_id = self._parse_identifier(relation_name)
        catalog = self._get_catalog(workspace)
        dataset = catalog.load_dataset(relative_id)
        return self._resolve_version_spec(
            catalog, dataset, relative_id, relation_name, word, allow_tag=False
        )

    def _resolve_version_spec(
        self,
        catalog,
        dataset,
        relative_id: str,
        relation_name: str,
        version_spec: Optional[str],
        allow_tag: bool,
    ) -> int:
        """The snapshot id a written version spec names.

        One resolver for every statement that names a version, so `CURRENT` and
        `PREVIOUS` cannot come to mean one thing on a read and another in DDL.
        The spellings are:

          <digits>    that snapshot id, verbatim
          current     the head - what an unqualified read returns
          previous    the previous VERSION OF THE DATA, which steps over the
                      compaction and statistics commits that changed no rows
          <name>      a tag, when `allow_tag` (rollback takes one, CREATE TAG
                      does not - a tag whose version is another tag is a copy
                      that silently stops tracking it)

        The four cases cannot collide: `current` and `previous` are names the
        catalog refuses to let a tag take, so a bare word is a tag name or it is
        nothing.
        """
        spec = (version_spec or "current").strip()

        if spec.isdigit():
            return int(spec)

        lowered = spec.lower()

        if lowered == "current":
            current = dataset.snapshot()
            if current is None:
                raise ValueError(
                    f"The dataset {relation_name} exists, but no data has been committed "
                    "to it yet, so there is no version to name."
                )
            return current.snapshot_id

        if lowered == "previous":
            if dataset.snapshot() is None:
                raise ValueError(
                    f"The dataset {relation_name} exists, but no data has been committed "
                    "to it yet, so there is no version to name."
                )
            previous = dataset.previous_user_snapshot()
            if previous is None:
                raise ValueError(
                    f"No previous version for {relation_name} - it is at the earliest "
                    "version of its data."
                )
            return previous.snapshot_id

        if not allow_tag:
            raise ValueError(
                f"'{spec}' is not a version. Write a snapshot id, CURRENT or PREVIOUS."
            )

        from opteryx_catalog.exceptions import TagNotFound

        try:
            return catalog.resolve_tag(relative_id, spec)
        except TagNotFound as exc:
            # Deliberately does NOT list the tags that do exist - somebody who
            # cannot see a dataset's tags must not learn them from a failed
            # guess. Same rule as the read path's tag resolution.
            raise ValueError(f"No tag {spec} on {relation_name}.") from exc

    def rollback_relation(
        self,
        relation_name: str,
        version_spec: str,
        author: Optional[str] = None,
    ) -> dict:
        """Move the relation's head to an older snapshot - `ROLLBACK TO VERSION`.

        Every unqualified read of the relation sees the head, so this restores
        that version of the data for everybody at once without moving a byte.
        Nothing is deleted: the snapshots it moves off stay readable by id and
        still appear in `SHOW SNAPSHOTS`, so the rollback is itself reversible
        by rolling forward to the id it moved off - which the catalog returns.

        Those snapshots are not PINNED, though. Ordinary retention still applies
        to them and they expire on schedule, after which the rollback can no
        longer be undone; a tag is what holds one indefinitely.
        """
        workspace, relative_id = self._parse_identifier(relation_name)
        catalog = self._get_catalog(workspace)
        dataset = catalog.load_dataset(relative_id)

        snapshot_id = self._resolve_version_spec(
            catalog, dataset, relative_id, relation_name, version_spec, allow_tag=True
        )

        from opteryx_catalog.exceptions import DatasetLocked
        from opteryx_catalog.exceptions import SnapshotMissingError

        try:
            return catalog.rollback_dataset(relative_id, snapshot_id, author=author)
        except (SnapshotMissingError, DatasetLocked) as exc:
            # Translated at the boundary, as `create_tag` and `drop_tag` do: an
            # `opteryx_catalog.exceptions` class name is not something a reader
            # of SQL should be shown. The catalog's message already names the
            # snapshot and what is wrong with it, so it is kept verbatim rather
            # than reworded into a second place for that wording to drift.
            raise ValueError(str(exc)) from exc

    def create_tag(
        self,
        relation_name: str,
        tag_name: str,
        version_spec: str,
        author: Optional[str] = None,
    ) -> dict:
        """Bind a name to one snapshot and pin that snapshot from expiry.

        `version_spec` is what the reader wrote - a snapshot id, `current` or
        `previous` - and is resolved to an id HERE, where the catalog is, rather
        than in the planner. The resolution is deliberately identical to the read
        path's, so `CREATE TAG t AS OF VERSION PREVIOUS` names exactly the
        snapshot `VERSION AS OF PREVIOUS` would have read - including PREVIOUS
        meaning the previous VERSION OF THE DATA rather than the literal parent
        snapshot. See `_resolve_version_spec`.

        The tag stores an ID, never the word: a tag is immutable, and one holding
        the phrase "current" would silently mean something different tomorrow.
        """
        workspace, relative_id = self._parse_identifier(relation_name)
        catalog = self._get_catalog(workspace)
        dataset = catalog.load_dataset(relative_id)

        snapshot_id = self._resolve_version_spec(
            catalog, dataset, relative_id, relation_name, version_spec, allow_tag=False
        )

        from opteryx_catalog.exceptions import SnapshotMissingError
        from opteryx_catalog.exceptions import TagError

        try:
            return catalog.create_tag(relative_id, tag_name, snapshot_id, author=author)
        except (TagError, SnapshotMissingError) as exc:
            # Translated at the boundary, like TagNotFound below and DatasetNotFound
            # above: an `opteryx_catalog.exceptions` class name is not something a
            # reader of SQL should ever be shown. The catalog's message is kept
            # verbatim - it already names the tag, the snapshot it holds, and what
            # to do about it, and rewording it here would be a second place for
            # that wording to drift. SnapshotMissingError is in the same list
            # because a version that does not resolve is the other way this
            # statement fails, and it must not be the one that leaks.
            raise ValueError(str(exc)) from exc

    def drop_tag(
        self,
        relation_name: str,
        tag_name: str,
        author: Optional[str] = None,
    ) -> None:
        """Remove a tag, releasing the snapshot it held.

        The snapshot returns to the ordinary retention rules at once, and expires
        on the next run if it is already past the window. That is the point of the
        statement, not a side effect of it.
        """
        from opteryx_catalog.exceptions import TagNotFound

        workspace, relative_id = self._parse_identifier(relation_name)
        catalog = self._get_catalog(workspace)

        try:
            catalog.drop_tag(relative_id, tag_name, author=author)
        except TagNotFound as exc:
            raise ValueError(f"There is no tag {tag_name} on {relation_name}.") from exc

    def list_tags(self, relation_name: str) -> list:
        """The tags on a dataset, as the catalog's plain dicts."""
        workspace, relative_id = self._parse_identifier(relation_name)
        catalog = self._get_catalog(workspace)
        return catalog.list_tags(relative_id)

    def list_triggers(self, relation_name: str) -> list:
        """The triggers a holder carries - a dataset's, or a task's for a
        schedule or signal trigger - as the catalog's plain dicts."""
        workspace, relative_id = self._parse_identifier(relation_name)
        catalog = self._get_catalog(workspace)
        return catalog.list_triggers(relative_id, **self._holder_kwargs(relation_name))

    # View operations (Eidetic capability)
    def get_view(self, view_name: str):
        """Retrieve the definition of the specified view."""
        from opteryx.connectors.capabilities.eidetic import ViewDefinition

        # Parse catalog name and relative identifier
        workspace, relative_id = self._parse_identifier(view_name)
        catalog = self._get_catalog(workspace)

        # Parse relative_id into collection and name
        # For "clickbench.q01": collection="clickbench", name="q01"
        parts = relative_id.split(".")
        name = parts[-1]
        collection = ".".join(parts[:-1])

        identifier = (collection, name)
        view = catalog.load_view(identifier)

        return ViewDefinition(
            name=view.name,
            statement=view.definition,
            owner=view.metadata.author,
            last_row_count=view.metadata.last_execution_records,
        )

    def list_views(self, prefix: str = None) -> list:
        """List all available views in the specified catalog and schema."""
        from opteryx.connectors.capabilities.eidetic import ViewDefinition

        # Determine namespace to list from
        namespace = prefix or "default"

        # Resolve catalog for namespace
        catalog = self._get_catalog(namespace)

        # Get view identifiers from catalog
        view_identifiers = catalog.list_views(namespace)

        # Load each view and convert to ViewDefinition
        views = []
        for identifier in view_identifiers:
            try:
                view = catalog.load_view(identifier)
                views.append(
                    ViewDefinition(
                        name=view.name,
                        statement=view.metadata.sql_text,
                        owner=view.metadata.author,
                        last_row_count=view.metadata.last_row_count,
                    )
                )
            except (KeyError, AttributeError):
                # Skip views that can't be loaded or have missing attributes
                pass

        return views

    def create_view(
        self, view_name: str, statement: str, update_if_exists: bool = False, owner: str = None
    ):
        """Create a new view with the given name and definition."""
        # Parse view_name into workspace and relative identifier
        workspace, relative_id = self._parse_identifier(view_name)
        catalog = self._get_catalog(workspace)

        # Split relative identifier into collection and name for catalog
        parts = relative_id.split(".")
        name = parts[-1]
        collection = ".".join(parts[:-1])

        identifier = (collection, name)
        catalog.create_view(
            identifier=identifier, sql=statement, update_if_exists=update_if_exists, author=owner
        )

    def drop_view(self, view_name: str, author: Optional[str] = None):
        """Drop the specified view."""
        # Parse view_name into workspace and relative identifier
        workspace, relative_id = self._parse_identifier(view_name)
        catalog = self._get_catalog(workspace)

        # Split relative identifier into collection and name for catalog
        parts = relative_id.split(".")
        name = parts[-1]
        collection = ".".join(parts[:-1])

        identifier = (collection, name)
        catalog.drop_view(identifier, author=author)

    def view_exists(self, view_name: str) -> bool:
        """Check if the specified view exists."""
        # Parse view_name into workspace and relative identifier
        workspace, relative_id = self._parse_identifier(view_name)
        catalog = self._get_catalog(workspace)

        # Split relative identifier into collection and name for catalog
        parts = relative_id.split(".")
        name = parts[-1]
        collection = ".".join(parts[:-1])

        identifier = (collection, name)
        return catalog.view_exists(identifier)

    def set_comment(self, object_name: str, comment: str, describer: str = "system"):
        """Set a comment on a view or table."""
        # Parse object_name into workspace and relative identifier
        workspace, relative_id = self._parse_identifier(object_name)
        catalog = self._get_catalog(workspace)

        # Split relative identifier into collection and name for catalog
        parts = relative_id.split(".")
        name = parts[-1]
        collection = ".".join(parts[:-1])

        identifier = (collection, name)

        object_name_type, _ = self.locate_object(object_name)
        if object_name_type == TableType.Table:
            # Update table comment
            catalog.update_dataset_description(
                identifier=identifier, description=comment, describer=describer
            )
            return
        if object_name_type == TableType.View:
            # Update view comment
            catalog.update_view_description(
                identifier=identifier, description=comment, describer=describer
            )
            return

        raise DatasetNotFoundError(connector=self, dataset=object_name)
