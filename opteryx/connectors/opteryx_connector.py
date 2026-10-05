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

import decimal
import logging
import threading
from collections import OrderedDict
from contextlib import contextmanager
from typing import Any, Dict, List, NamedTuple, Optional, Tuple

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
from opteryx.connectors.capabilities import Diachronic, Eidetic, PredicatePushable, TopNPushable, Writable
from opteryx.connectors.capabilities.topn_pushable import single_physical_column_topn
from opteryx.connectors.capabilities.writable import EgressRefusal
from opteryx.connectors.manifest_disk_cache import CachingFileIO
from opteryx.connectors.manifest_disk_cache import manifest_cache_tiers
from opteryx.exceptions import (
    CollectionNotEmptyError,
    DatasetNotFoundError,
    DatasetReadError,
    InvalidInternalStateError,
    UnsupportedSyntaxError,
)
from opteryx.exceptions import md_code
from opteryx.models import Manifest
from opteryx.types.logical_type import LogicalCategory
from opteryx.types.schema import SchemaColumn, RelationSchema, ColumnDescriptor, RelationDescriptor


def _accepts_include_expired(loader) -> bool:
    """Whether this catalog's `load_dataset` can be asked for tombstones.

    `SHOW ALL SNAPSHOTS FOR` needs a catalog new enough to read expired
    snapshots, and a deployment can be mid-upgrade. Asked of the signature
    rather than caught as a TypeError from the call: a TypeError raised INSIDE
    the loader looks identical from out here, and reporting a real fault as
    "this catalog is too old" sends whoever reads it looking in the wrong
    place. A loader taking **kwargs is taken at its word.
    """
    import inspect

    try:
        parameters = inspect.signature(loader).parameters
    except (TypeError, ValueError):  # pragma: no cover - unintrospectable callable
        return True
    if "include_expired" in parameters:
        return True
    return any(p.kind is inspect.Parameter.VAR_KEYWORD for p in parameters.values())


class OpteryxTable(BaseTable, Diachronic, PredicatePushable, TopNPushable):
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
    supports_vector_indexes = True  # CREATE / ALTER / DROP INDEX (vector index)
    supports_statistics = True  # Manifest provides stats
    supports_predicate_pushdown = True  # Allow optimizer to push predicates to reader
    supports_limit_pushdown = True  # Allow optimizer to push LIMIT to OpteryxTable
    # Served by ParquetReadNode (see below), which consumes the single-key
    # top-N spec; the stamp also arms TopNManifestPruningStrategy.
    supports_topn_pushdown = True
    # The reader that serves a catalog scan is not this class - it is chosen
    # from the manifest's file formats (physical_planner
    # `_scan_reader_for_manifest`), and a catalog manifest is parquet-only
    # (`_catalog_manifest` types every file PARQUET). That reader is
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
        # `workspace name -> catalog`, handed in by OpteryxConnector.table_engine
        # (its `_get_catalog`). A receipt names sources across workspace
        # boundaries (PROVENANCE_DESIGN.md S4.4), and `self.catalog` can only
        # answer for this table's own workspace; SHOW LINEAGE needs the others
        # to say whether a source snapshot still exists. Optional, because this
        # class is also constructed directly over one catalog, and then a
        # foreign source's existence is simply unknown rather than an error.
        catalog_resolver = kwargs.pop("catalog_resolver", None)

        Diachronic.__init__(self, **kwargs)
        PredicatePushable.__init__(self, **kwargs)

        self.dataset = dataset.replace("/", ".")
        self.catalog = catalog
        self.workspace = workspace
        self.telemetry = kwargs.get("telemetry")
        self._catalog_resolver = catalog_resolver
        # (dataset, snapshot_id) -> bool | None, for one statement: this object
        # is built per Scan per statement, so a cache on it lives exactly as
        # long as the SHOW LINEAGE that fills it. See `_source_snapshot_exists`.
        self._source_exists_cache: dict = {}

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

    def can_push_topn(self, order_by) -> bool:
        return single_physical_column_topn(order_by)

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
    ) -> RelationDescriptor:
        # A catalog describes its relations; it never hands over BOUND columns -
        # those carry identities minted in some other binding (architect ruling
        # 2026-09-26: no dual path). Read one through the generic branch below
        # and it has no `.type`, so it would come back silently as VARCHAR.
        if isinstance(schema, RelationSchema):
            raise DatasetReadError(
                f"The catalog described {relation_name or schema.name} with a bound "
                "RelationSchema; a catalog must describe relations with a "
                "RelationDescriptor."
            )
        if isinstance(schema, RelationDescriptor):
            if relation_name:
                schema.name = relation_name
            return schema

        columns = []
        for column in getattr(schema, "columns", []) or []:
            if isinstance(column, SchemaColumn):
                raise DatasetReadError(
                    f"The catalog described column {column.name} of "
                    f"{relation_name or getattr(schema, 'name', 'dataset')} as a bound "
                    "SchemaColumn; a catalog must describe columns with a ColumnDescriptor."
                )
            if isinstance(column, ColumnDescriptor):
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
                normalized = ColumnDescriptor(
                    name=name,
                    column_type=_ct,
                    nullable=getattr(column, "nullable", True),
                    field_id=raw_field_id,
                )

            columns.append(normalized)

        return RelationDescriptor(
            name=relation_name or getattr(schema, "name", "dataset"), columns=columns
        )

    def get_dataset_schema(self) -> RelationDescriptor:
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
            RelationDescriptor
        """
        self._resolve_snapshot()
        if self.snapshot is None:
            self.dataset_committed_at = None
            return self.get_declared_schema()
        raw_schema = self.table.schema(self.snapshot.schema_id)
        self.schema = self._normalize_schema(raw_schema, relation_name=self.dataset)
        self.dataset_committed_at = self.snapshot.timestamp_ms
        return self.schema

    def get_declared_schema(self) -> RelationDescriptor:
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
            RelationDescriptor
        """
        raw_schema = self.table.schema()
        if raw_schema is None:
            raise DatasetReadError(
                f"The dataset {self.dataset} has no registered schema."
            )
        self.schema = self._normalize_schema(raw_schema, relation_name=self.dataset)
        return self.schema

    def get_all_snapshots(self) -> list:
        """The same history plus the TOMBSTONES, for `SHOW ALL SNAPSHOTS FOR`.

        Expired snapshots are records of what is still restorable, not versions:
        their files are in the orphan quarantine or GCS soft-delete, reading one
        by id is refused everywhere, and the record itself is purged when the
        recovery window closes. The rows say so in `expired_at` and
        `is_queryable` - the two columns this form adds - rather than sitting
        in the list looking like history.

        Gated at MANIFEST (owner) by the binder, not READ like `get_snapshots`:
        what is asked here is what this relation is still holding in the restore
        window and how long it has, which is an operational question about the
        storage rather than a question about data the caller can already read.
        """
        return self._snapshot_rows(include_expired=True)

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
        also the answer to "why is this old snapshot still here". The ALL form
        above is the one statement that sees them.
        """
        return self._snapshot_rows(include_expired=False)

    def _snapshot_rows(self, include_expired: bool) -> list:
        """The rows behind both SHOW SNAPSHOTS forms. One implementation, so the
        two cannot order, tag or count a history differently."""
        from opteryx.models.snapshot_history import SnapshotRows
        from opteryx.models.snapshot_history import normalize_snapshot

        if include_expired:
            # A deployment can be mid-upgrade: the engine has the statement and
            # the catalog cannot answer it. Refused, rather than quietly served
            # the live history under a statement that asked for more.
            if not _accepts_include_expired(self.catalog.load_dataset):
                from opteryx.exceptions import UnsupportedSyntaxError

                raise UnsupportedSyntaxError(
                    "`SHOW ALL SNAPSHOTS FOR` needs a catalog that can read expired "
                    "snapshots; this deployment's catalog cannot. `SHOW SNAPSHOTS FOR` "
                    "answers the live history."
                )
            dataset = self.catalog.load_dataset(
                self.dataset, load_history=True, include_expired=True
            )
        else:
            dataset = self.catalog.load_dataset(self.dataset, load_history=True)
        snapshots = list(dataset.snapshots())
        # Tombstones arrive in a list of their own and are merged only here: to
        # the catalog they are not history, and to this statement they are rows.
        expired = list(dataset.expired_snapshots()) if include_expired else []
        if not snapshots and not expired:
            return SnapshotRows((), include_expiry=include_expired)

        # The catalog's head pointer. `current`, not `latest`: a rollback moves
        # it BACKWARDS, so the snapshot it names is not necessarily the newest
        # one generated - the same reason the stored key has always been
        # `current-snapshot-id`.
        current_snapshot_id = dataset.metadata.current_snapshot_id
        ordered = sorted(
            snapshots + expired, key=lambda s: (s.timestamp_ms, s.snapshot_id), reverse=True
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

        return SnapshotRows(
            (
                normalize_snapshot(
                    snapshot,
                    current_snapshot_id,
                    tags=sorted(tags_by_snapshot.get(snapshot.snapshot_id, []))
                    + _virtual_tags(snapshot.snapshot_id),
                    include_expiry=include_expired,
                )
                for snapshot in ordered
            ),
            include_expiry=include_expired,
        )

    def get_lineage(self) -> list:
        """The relation's receipts, newest snapshot first, for `SHOW LINEAGE FOR`.

        Rows are the `opteryx.models.lineage_history` shape, normalized here
        for the reason `get_snapshots` normalizes: that module must not import
        opteryx_catalog. The same `load_history=True` reload, paid on this path
        only - the receipt is a field of each snapshot document, so listing
        them is the history load and nothing cheaper.

        Names are returned WHOLE. Elision is the binder's (visit_show_lineage):
        it has the session, this class does not, and a connector that decided
        what a caller may see would be a second permissions implementation.

        `source_exists` is resolved here, though, because it is a catalog
        question: does the snapshot the receipt names still exist in the
        workspace it names? Every distinct (dataset, snapshot) pair costs one
        lookup, once - see `_source_snapshot_exists`.
        """
        from opteryx.models.lineage_history import normalize_lineage

        dataset = self.catalog.load_dataset(self.dataset, load_history=True)
        snapshots = dataset.snapshots()
        if not snapshots:
            return []

        current_snapshot_id = dataset.metadata.current_snapshot_id
        # The same order as SHOW SNAPSHOTS, decided by the same key, so the two
        # statements list a history identically.
        ordered = sorted(
            snapshots, key=lambda s: (s.timestamp_ms, s.snapshot_id), reverse=True
        )

        rows = []
        for snapshot in ordered:
            rows.extend(normalize_lineage(snapshot, current_snapshot_id))
        for row in rows:
            if row["source_dataset"] is not None:
                row["source_exists"] = self._source_snapshot_exists(
                    row["source_dataset"], row["source_snapshot_id"]
                )
        return rows

    def _source_snapshot_exists(self, source: str, snapshot_id) -> Optional[bool]:
        """Whether `snapshot_id` of the fully-qualified `source` can still be
        read - True, False, or None for "could not look".

        None is an answer of its own and never a failure of the statement: the
        receipt is the history, and a source workspace that is unreachable, or
        one this connector cannot resolve a catalog for, must not take the
        history down with it. A source with no snapshot id was read before its
        first commit; there is no snapshot to look for, and None is the honest
        answer rather than False, which would say a version had expired.

        A source dataset that no longer exists is False, not None: the snapshot
        certainly cannot be read, and that is the fact the column reports.

        Cached per (dataset, snapshot) on this object. A receipt that names
        the same upstream at the same version across fifty commits - which is
        what a scheduled append against a slow-moving source looks like - is
        one catalog read, not fifty.
        """
        if snapshot_id is None:
            return None
        key = (source, snapshot_id)
        if key in self._source_exists_cache:
            return self._source_exists_cache[key]

        from opteryx_catalog.exceptions import DatasetNotFound

        workspace, _, relative = str(source).partition(".")
        verdict: Optional[bool]
        try:
            if workspace == self.workspace:
                catalog = self.catalog
            elif self._catalog_resolver is not None:
                catalog = self._catalog_resolver(workspace)
            else:
                catalog = None
            if catalog is None or not relative:
                verdict = None
            else:
                verdict = catalog.load_dataset(relative).snapshot(snapshot_id) is not None
        except DatasetNotFound:
            verdict = False
        except Exception:  # noqa: BLE001 - see docstring: unknown, never fatal
            verdict = None
        self._source_exists_cache[key] = verdict
        return verdict

    def get_sources(self) -> list:
        """The relation's standing source list, for `SHOW SOURCES FOR`.

        Rows are the `opteryx.models.source_list` shape. Read off the dataset
        this object already loaded - `sources` is maintained ON the dataset
        document precisely so that answering this costs one document read
        (PROVENANCE_DESIGN.md S2.2), so unlike the two history statements this
        one performs no reload.

        A dataset object with no `sources` attribute at all is one from a
        catalog older than the field. That is reported as an empty list with
        `complete` unknown (None), which is what it is: the catalog did not
        say, and a fabricated `false` would claim it had.

        Names are returned whole; elision is the binder's, as for lineage.
        """
        from opteryx.models.source_list import normalize_sources

        metadata = self.table.metadata
        names = getattr(metadata, "sources", None)
        complete = getattr(metadata, "sources_complete", None)
        return normalize_sources(list(names or []), complete)

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

    def get_dataset_metadata(self) -> Tuple[RelationDescriptor, Manifest]:
        """
        Get dataset schema and build manifest from catalog.

        Returns both schema and manifest to make the dual purpose explicit.
        The manifest comes from whichever producer the dataset declares
        (`has_opteryx_manifest`): the snapshot's manifest parquet decoded
        natively, or - for a backend with no opteryx-format manifest
        (opteryx-iceberg) - its `scan()` rows.

        Returns:
            Tuple of (RelationDescriptor, Manifest)
        """
        self._resolve_snapshot()

        # bounds_are_ordinal is asked of the DATASET, never assumed here. This
        # connector serves every metastore opteryx-catalog's `Dataset` interface
        # covers -- the native catalog (ordinal keys) and external catalogs such
        # as opteryx-iceberg (real decoded values, `from_bytes` off the Iceberg
        # manifest) -- and the encoding travels with whoever produced the bounds.
        # Hardcoding True read an Iceberg VARCHAR's real `str` bound as an
        # ordinal and pruned every file of `WHERE <double col> >= 250.0`.
        # A dataset that declares nothing is an ERROR, not a defaulting case:
        # True and False are each silently wrong for one of the two producers.
        # It is demanded even of an empty relation: the declaration is a
        # property of the metastore implementation, not of whether it holds
        # data, so a backend missing it fails on the first READ rather than
        # silently waiting for the first commit to corrupt pruning.
        bounds_are_ordinal = self.table.bounds_are_ordinal
        if bounds_are_ordinal is None:
            raise DatasetReadError(
                f"{type(self.table).__name__} does not declare `bounds_are_ordinal`, so the "
                "encoding of its manifest min/max bounds is unknown. Implementations of "
                "opteryx-catalog's `Dataset` must set it (True for Vector.ordinalize() keys, "
                "False for real decoded values); guessing either way silently corrupts pruning."
            )

        if self.snapshot is None:
            # Nothing committed - see _resolve_snapshot. The relation is served
            # as declared with no files, which the scan turns into a single
            # empty morsel: the same result a TRUNCATEd relation gives through
            # its own empty manifest. There is no scan to run and no commit to
            # timestamp, so neither is invented. A relation with no committed
            # snapshot genuinely HAS no rows: authoritative.
            self.dataset_committed_at = None
            self.schema = self.get_declared_schema()
            self.manifest = _catalog_manifest(self.schema, bounds_are_ordinal, [], {}, None)
            return self.schema, self.manifest

        raw_schema = self.table.schema(self.snapshot.schema_id)
        self.schema = self._normalize_schema(raw_schema, relation_name=self.dataset)
        self.dataset_committed_at = self.snapshot.timestamp_ms

        has_opteryx_manifest = self.table.has_opteryx_manifest
        if has_opteryx_manifest is None:
            raise DatasetReadError(
                f"{type(self.table).__name__} does not declare `has_opteryx_manifest`, so how "
                "its snapshots' manifests are read is unknown. Implementations of "
                "opteryx-catalog's `Dataset` must set it (True: `manifest_bytes()` serves the "
                "opteryx manifest parquet; False: planning reads `scan()` rows)."
            )
        if has_opteryx_manifest:
            self.manifest = Manifest(self._decoded_manifest(bounds_are_ordinal), self.schema)
        else:
            self.manifest = self._row_manifest(bounds_are_ordinal)
        return self.schema, self.manifest

    def _decoded_manifest(self, bounds_are_ordinal: bool):
        """The resolved snapshot's manifest parquet, decoded natively over the
        relation's schema, with its merge-on-read deletes resolved.

        Cached by the manifest's location - written once per snapshot, never
        rewritten - and the layout it was decoded against. The bytes are read
        for the RESOLVED snapshot by its own id: asking for "current" again
        could meet a commit that landed since, pairing this snapshot's schema
        with the next one's files."""
        from opteryx.compiled.planner.native_manifest import NativeManifestBuilder
        from opteryx.compiled.planner.native_manifest import decode_manifest_parquet

        columns = self.schema.columns
        names = tuple(column.name for column in columns)
        physical = tuple(column.column_type.physical for column in columns)
        position_of_field_id = {
            column.field_id: position
            for position, column in enumerate(columns)
            if column.field_id is not None
        }
        location = self.snapshot.manifest_list
        if not location:
            # a snapshot with no manifest is an empty dataset
            return NativeManifestBuilder(names, physical, bounds_are_ordinal, True).build({})

        key = (location, names, physical, tuple(sorted(position_of_field_id.items())), bounds_are_ordinal)
        native = _decoded_manifest_cache_get(key)
        if native is not None:
            return native

        data = self.table.manifest_bytes(self.snapshot.snapshot_id)
        if data is None:
            raise DatasetReadError(
                f"Snapshot {self.snapshot.snapshot_id} of {self.dataset} names the manifest "
                f"{location} but the catalog served none."
            )
        # Written by the commit that produced these files; they cannot
        # disagree with the data: authoritative.
        native = decode_manifest_parquet(
            data, names, physical, position_of_field_id, bounds_are_ordinal, True
        )

        protocols = {path.split("://")[0] for path in native.file_paths() if "://" in path}
        if len(protocols) > 1:
            raise DatasetReadError(
                f"Mixed protocols in manifest: {protocols}. All files must use the same protocol."
            )

        # Merge-on-read deletes: each delete-bearing file's row ordinals are
        # resolved NOW, at binding, from the dataset's sidecar(s), so the read
        # node subtracts them per row group with no further catalog round-trip.
        # Fail-closed on both sides: the dataset raises if a sidecar is
        # unreadable, and resolve_deletes for a file left without a vector.
        pending = native.unresolved_deletes()
        if pending:
            native.resolve_deletes(self.table.delete_vectors_for(pending))

        _decoded_manifest_cache_put(key, native)
        return native

    def _row_manifest(self, bounds_are_ordinal: bool) -> Manifest:
        """The manifest of a backend with no opteryx-format manifest
        (opteryx-iceberg), from its `scan()` rows."""
        entries = [data_file.entry for data_file in self.table.scan(snapshot_id=self.snapshot.snapshot_id)]

        protocols = {
            entry.get("file_path").split("://")[0]
            for entry in entries
            if "://" in entry.get("file_path")
        }
        if len(protocols) > 1:
            raise DatasetReadError(
                f"Mixed protocols in manifest: {protocols}. All files must use the same protocol."
            )

        # Whole-column native sketch vectors. A backend that does not implement
        # the accessor has no sketches, and _warn_no_native_sketches reports it
        # once per backend class. A backend that returns {} has declared "no
        # sketches" and is not reported.
        sketch_vectors_fn = getattr(self.table, "manifest_sketch_vectors", None)
        if sketch_vectors_fn is not None:
            sketch_vectors = sketch_vectors_fn(self.snapshot.snapshot_id)
        else:
            sketch_vectors = {}
            _warn_no_native_sketches(self.table)

        resolved_deletes = None
        if any(entry.get("deleted_record_count") for entry in entries):
            resolved_deletes = self.table.delete_vectors(self.snapshot.snapshot_id)

        return _catalog_manifest(
            self.schema, bounds_are_ordinal, entries, sketch_vectors, resolved_deletes
        )

    # --- vector search (docs/VECTOR_INDEX_DESIGN.md §7-§8) ---

    def vector_indexes(self) -> list:
        """The vector indexes defined on this table, as the catalog's plain dicts - from the
        dataset this scan loaded (the definitions ride on its document), so planning an
        index search reads nothing more."""
        return list(self.table.metadata.vector_indexes)

    def vector_index_sizes(self, index_id: str) -> dict:
        """{data file path: (index file bytes, its footer bytes)} for the files one index
        covers at the snapshot this scan reads (no credentials: for the plan's per-file
        choice between the index and an exact search)."""
        return {
            path: (f.file_bytes, f.footer_bytes)
            for path, f in self.table.vector_index_files(index_id, self.snapshot_id).items()
        }

    def vector_search_indexes(self, index_id: str) -> dict:
        """{data file path: (index file, its bytes, its footer bytes, auth header)} for one
        index at the snapshot this scan reads, each location one the native reader can open
        with that header (see _index_reads). A live file absent here is not covered yet
        and is searched exactly."""
        readable = _index_reads()
        out = {}
        for path, f in self.table.vector_index_files(index_id, self.snapshot_id).items():
            location, auth_header = readable(f.path)
            out[path] = (location, f.file_bytes, f.footer_bytes, auth_header)
        return out

# REFRESH INDEX's maintenance lease (design §5.7): held for _LEASE_SECONDS and renewed
# every _LEASE_RENEW_SECONDS while files build, so a crashed holder frees the table
# within ten minutes.
_LEASE_SECONDS = 600
_LEASE_RENEW_SECONDS = 120
# A compaction's sink renews only between its stages (no timer thread outlives a failed
# statement), so it claims the longest lease; one that dies holds the table for an hour.
_COMPACTION_LEASE_SECONDS = 3600
# A SigV4 presigned URL's longest life: one file's build can run for hours.
_SIGNED_URL_SECONDS = 7 * 24 * 3600


def _index_reads():
    """A function mapping an index or data file location to (the location the native
    readers open, the Authorization header they send - "" for none).

    A gs:// object stays gs:// and is read with this process's bearer token, minted once
    here and shared by every file the caller maps: GCS signing has no local key on Cloud
    Run, so it is an IAM signBlob call the service account may not be allowed to make. The
    token is not refreshed - a native read that outlives it fails on GCS's 401. An s3://
    object is presigned (SigV4 signs locally) and needs no header; a local path is as is."""
    bearer = None

    def readable(path: str) -> tuple:
        nonlocal bearer
        if path.startswith("gs://"):
            if bearer is None:
                from opteryx.connectors.io_systems import OpteryxGcsFileSystem

                bearer = OpteryxGcsFileSystem()._bearer
            return path, bearer
        if path.startswith("s3://"):
            from opteryx.connectors.io_systems.s3_filesystem import OpteryxS3FileSystem

            return OpteryxS3FileSystem().rewrite_to_signed_url(path, _SIGNED_URL_SECONDS), ""
        return path, ""

    return readable


def _carry_on_gcs(io, specs, recorders, dims, targets, options, carry_to_sessions):
    """Carry into GCS: each output's index file streams into its own resumable session,
    which the carry finishes - one object per output, as a build's. A session whose output
    carried nothing, or every session when the carry fails, is cancelled."""
    sessions = [io.open_upload_session(path) for path in targets]
    try:
        built = carry_to_sessions(specs, recorders, dims, sessions, **options)
    except BaseException:
        for session in sessions:
            io.cancel_upload_session(session)
        raise
    for session, result in zip(sessions, built):
        if result is None:
            io.cancel_upload_session(session)
    return built


class _MaintenanceLeaseHandle:
    """A claimed maintenance lease (§5.7), for an operation the engine runs across many
    calls (a compaction's sink). `renew` extends it; `release` frees it and says whether
    this claim still held it."""

    def __init__(self, connector, relation_name: str, holder: str, operation: str):
        from opteryx_catalog.exceptions import MaintenanceLeaseHeld

        from opteryx.exceptions import ExecutionError

        workspace, relative_id = connector._parse_identifier(relation_name)
        self._catalog = connector._get_catalog(workspace)
        try:
            self._lease = self._catalog.claim_maintenance_lease(
                relative_id, holder=holder, operation=operation, ttl_seconds=_COMPACTION_LEASE_SECONDS
            )
        except MaintenanceLeaseHeld as exc:
            raise ExecutionError(str(exc)) from exc

    def renew(self) -> None:
        from opteryx_catalog.exceptions import MaintenanceLeaseLost

        from opteryx.exceptions import ExecutionError

        try:
            self._lease = self._catalog.renew_maintenance_lease(
                self._lease, ttl_seconds=_COMPACTION_LEASE_SECONDS
            )
        except MaintenanceLeaseLost as exc:
            raise ExecutionError(str(exc)) from exc

    def release(self) -> bool:
        return self._catalog.release_maintenance_lease(self._lease)


def _build_index_files(catalog, definition, data_file, data_bytes, deleted, index_path):
    """Build ONE data file's index file for `definition` - natively, GIL released - and
    return its IndexFiles, or None when the file has no indexable row (nothing written).

    A data file on GCS is read with this process's bearer token (not refreshed: a build
    that outlives it fails on GCS's 401) and its index file streamed into a resumable
    upload session, whose URI is its own credential."""
    import os

    from opteryx_catalog.catalog.vector_indexes import IndexFiles

    from draken.ops.kernels._kernel_registry import lookup_kernel
    from opteryx import config
    from opteryx.exceptions import NotSupportedError
    from opteryx.operators._operators import build_vector_index_local
    from opteryx.operators._operators import build_vector_index_to_session

    embed_fn, _ = lookup_kernel("draken_embed")
    threads = config.resolve_max_execution_workers()
    options = dict(clusters=definition["clusters"], embed_threads=threads, train_threads=threads)
    task = _IndexTarget(data_file, data_bytes, tuple(deleted), index_path)
    if data_file.startswith("gs://"):
        built = _build_index_on_gcs(
            catalog.io, task, definition["column"], embed_fn, definition["dimensions"], options,
            build_vector_index_to_session,
        )
    elif "://" not in data_file:
        os.makedirs(os.path.dirname(index_path), exist_ok=True)
        built = build_vector_index_local(
            data_file, definition["column"], list(deleted), embed_fn, definition["dimensions"],
            index_path, data_bytes=data_bytes, **options,
        )
    else:
        raise NotSupportedError(
            f"Vector indexes are built for files on local disk or GCS; {data_file} is neither."
        )
    if built is None:
        return None
    return IndexFiles(
        path=index_path, file_bytes=built["file_bytes"], footer_bytes=built["footer_bytes"],
        logical_bytes=built["logical_bytes"],
    )


class _IndexTarget(NamedTuple):
    """One data file to index and where its index file goes (what `_build_index_on_gcs`
    reads; the catalog's IndexBuildTask has the same fields)."""

    data_file: str
    data_bytes: int
    deleted: tuple
    path: str


def _require_index_embedder(definition: dict, relation_name: str) -> None:
    """An index is built only by the embedder it was defined against (identity and width)."""
    from opteryx.exceptions import ExecutionError
    from opteryx.types.vectors.embedding_capability import active_embedding_capability

    capability = active_embedding_capability()
    if (capability.identity, capability.dimensions) != (
        definition["embedding-identity"], definition["dimensions"]
    ):
        raise ExecutionError(
            f"Index {definition['name']} on {relation_name} was defined against the embedder "
            f"{definition['embedding-identity']} ({definition['dimensions']} dimensions); this "
            f"engine embeds with {capability.identity} ({capability.dimensions}). Install that "
            "embedder to build it, or drop and re-create the index."
        )


@contextmanager
def _index_build_lease(catalog, relative_id: str, holder: str):
    """Hold the dataset's maintenance lease (`index-build`, design §5.7) for the block.

    Refused loudly when someone else holds it. Renewed on a timer while the block runs -
    the native build releases the GIL - and yields `lost()`, which raises once the lease
    has been lost: a lost lease cannot stop a build in flight, so it stops the commit
    after it. Released on the way out; an overrun (the lease expired and was claimed
    again) is reported unless another error is already leaving."""
    import threading

    from opteryx_catalog.exceptions import MaintenanceLeaseHeld
    from opteryx_catalog.exceptions import MaintenanceLeaseLost

    from opteryx.exceptions import ExecutionError

    try:
        lease = catalog.claim_maintenance_lease(
            relative_id, holder=holder, operation="index-build", ttl_seconds=_LEASE_SECONDS
        )
    except MaintenanceLeaseHeld as exc:
        raise ExecutionError(str(exc)) from exc

    stop = threading.Event()
    lost_with: list = []

    def _renew():
        held = lease
        while not stop.wait(_LEASE_RENEW_SECONDS):
            try:
                held = catalog.renew_maintenance_lease(held, ttl_seconds=_LEASE_SECONDS)
            except MaintenanceLeaseLost as exc:
                lost_with.append(exc)
                return

    def lost() -> None:
        if lost_with:
            raise ExecutionError(str(lost_with[0])) from lost_with[0]

    renewer = threading.Thread(target=_renew, name="index-build-lease", daemon=True)
    renewer.start()
    try:
        yield lost
    except BaseException:
        stop.set()
        renewer.join()
        catalog.release_maintenance_lease(lease)
        raise
    stop.set()
    renewer.join()
    if not catalog.release_maintenance_lease(lease):
        raise ExecutionError(
            f"{holder} finished, but its maintenance lease on {relative_id} had expired and been "
            "claimed again before it did."
        )


def _build_index_on_gcs(io, task, column, embed_fn, dims, build_options, build_to_session):
    """Build one GCS data file's index file - see refresh_vector_index.

    The file streams natively into a resumable upload session while the build runs, and
    the build finishes the session: one object, written once. Returns the build's dict, or
    None (nothing written, the session cancelled) when the file has no indexable row."""
    data_file, auth_header = _index_reads()(task.data_file)
    session = io.open_upload_session(task.path)
    try:
        built = build_to_session(
            data_file, column, list(task.deleted), embed_fn, dims, session,
            data_bytes=task.data_bytes, auth_header=auth_header, **build_options,
        )
    except BaseException:
        io.cancel_upload_session(session)
        raise
    if built is None:
        io.cancel_upload_session(session)
    return built


# Decoded catalog manifests, shared across queries (the connector is recreated
# per query). Keyed by manifest location + the layout decoded against; bounded
# by entry count AND by the manifests' own resident bytes
# (NativeManifest.resident_bytes), LRU eviction. An entry is only ever read: a
# Manifest narrows by COPYING (subset / with_paths), never in place.
_DECODED_MANIFESTS: "OrderedDict" = OrderedDict()
_DECODED_MANIFEST_COSTS: dict = {}
_DECODED_MANIFEST_MAX_ENTRIES = 32
_DECODED_MANIFEST_BUDGET_BYTES = 128 * 1024 * 1024
_DECODED_MANIFEST_LOCK = threading.Lock()


def _decoded_manifest_cache_get(key):
    with _DECODED_MANIFEST_LOCK:
        native = _DECODED_MANIFESTS.get(key)
        if native is not None:
            _DECODED_MANIFESTS.move_to_end(key)
        return native


def _decoded_manifest_cache_put(key, native) -> None:
    cost = native.resident_bytes()
    if cost > _DECODED_MANIFEST_BUDGET_BYTES:
        return  # served, never cached: one entry would own the budget
    with _DECODED_MANIFEST_LOCK:
        _DECODED_MANIFESTS[key] = native
        _DECODED_MANIFESTS.move_to_end(key)
        _DECODED_MANIFEST_COSTS[key] = cost
        while _DECODED_MANIFESTS and (
            len(_DECODED_MANIFESTS) > _DECODED_MANIFEST_MAX_ENTRIES
            or sum(_DECODED_MANIFEST_COSTS.values()) > _DECODED_MANIFEST_BUDGET_BYTES
        ):
            evicted, _ = _DECODED_MANIFESTS.popitem(last=False)
            _DECODED_MANIFEST_COSTS.pop(evicted)


_NO_BOUND = -(1 << 63)          # the ordinal NULL_FLAG: "no bound"
_INT64_MIN = -(1 << 63)
_INT64_MAX = (1 << 63) - 1


def _catalog_manifest(schema, bounds_are_ordinal, entries, sketch_vectors, resolved_deletes):
    """The Manifest for a snapshot of a backend with NO opteryx-format manifest
    (`has_opteryx_manifest` False - opteryx-iceberg): its `scan()` rows
    (`Datafile.entry` dicts) into the native builder. Also the empty manifest
    of a relation with no committed snapshot. A backend whose manifests ARE the
    opteryx manifest parquet is decoded natively instead
    (`OpteryxTable._decoded_manifest`; architect ruling 2026-09-27 (6b)).

    Every per-column stat a row carries (min/max values, lengths, null counts,
    distinct counts, char bytes, column sizes) is a POSITIONAL list in the
    row's own column order, keyed by the row's `field_ids`. Each maps to its
    load-time position through the schema's field ids - one key space. A row
    with no `field_ids` of its own was written in schema order, so its lists
    are positional. A list that cannot be lined up with its keys is DROPPED:
    no stats is correct but slower, keyed by the wrong column is a wrong answer.
    """
    from opteryx.compiled.planner.native_manifest import NativeManifestBuilder

    columns = schema.columns
    builder = NativeManifestBuilder(
        tuple(column.name for column in columns),
        tuple(column.column_type.physical for column in columns),
        bounds_are_ordinal,
        True,
    )
    width = len(columns)
    position_of_field = {
        column.field_id: position for position, column in enumerate(columns) if column.field_id is not None
    }
    schema_fully_keyed = len(position_of_field) == width

    for vector_row, entry in enumerate(entries):
        file_path = entry.get("file_path")
        deleted = int(entry.get("deleted_record_count") or 0)
        delete_positions = None
        if deleted:
            vector = resolved_deletes.get(file_path)
            if vector is None:
                raise DatasetReadError(
                    f"Manifest attributes {deleted} deleted rows to "
                    f"{file_path} but the delete sidecar holds no vector for it."
                )
            delete_positions = tuple(vector)
        uncompressed = entry.get("uncompressed_size_in_bytes")
        row = builder.add_file(
            file_path,
            "PARQUET",
            entry.get("record_count", 0),
            entry.get("file_size_in_bytes", 0),
            -1,
            -1 if uncompressed is None else uncompressed,
            # 0 is the writer's "no histogram" marker, not a bin count
            entry.get("histogram_bins") or -1,
            deleted,
            entry.get("delete_file_path"),
            delete_positions,
            vector_row,
        )

        row_field_ids = entry.get("field_ids")
        if row_field_ids and type(row_field_ids) in (list, tuple):
            keys = [position_of_field.get(fid) for fid in row_field_ids]
            strict = True
        else:
            keys = range(width)
            strict = schema_fully_keyed

        def keyed(name):
            values = entry.get(name)
            if not values or type(values) not in (list, tuple):
                return ()
            if strict and len(values) != len(keys):
                return ()
            return [(position, value) for position, value in zip(keys, values)
                    if position is not None and value is not None]

        for is_min, name in ((True, "min_values"), (False, "max_values")):
            for position, value in keyed(name):
                _set_catalog_bound(builder, row, position, is_min, value, bounds_are_ordinal)
        for name, argument in (
            ("null_counts", "null_count"),
            ("min_lengths", "min_length"),
            ("max_lengths", "max_length"),
            ("char_total_bytes", "char_total_bytes"),
            ("column_uncompressed_sizes_in_bytes", "uncompressed_size"),
        ):
            for position, value in keyed(name):
                builder.set_counts(row, position, **{argument: value})
        # ESTIMATE-ONLY: the format does not persist exactness, so never exact.
        for position, value in keyed("distinct_counts"):
            builder.set_distinct_count(row, position, value, False)

    return Manifest(builder.build(dict(sketch_vectors)), schema)


def _set_catalog_bound(builder, row, position, is_min, value, bounds_are_ordinal):
    """One catalog bound into the builder, in the manifest's declared dialect."""
    kind = type(value)
    if bounds_are_ordinal:
        if kind is not int:
            raise DatasetReadError(f"An ordinal-dialect manifest bound of type {kind.__name__} ({value!r}).")
        if value != _NO_BOUND:
            builder.set_ordinal_bound(row, position, is_min, value)
    elif kind is bool:
        builder.set_bool_bound(row, position, is_min, value)
    elif kind is int and value > _INT64_MAX:
        builder.set_uint_bound(row, position, is_min, value)
    elif kind is int:
        builder.set_int_bound(row, position, is_min, value)
    elif kind is float:
        builder.set_double_bound(row, position, is_min, value)
    elif kind is str:
        builder.set_text_bound(row, position, is_min, value)
    elif kind is bytes:
        builder.set_bytes_bound(row, position, is_min, value)
    elif kind is decimal.Decimal:
        sign, digits, exponent = value.as_tuple()
        unscaled = int("".join(map(str, digits)) or "0") * (-1 if sign else 1)
        # A DECIMAL128-range bound has no int64 home; like the writers, which
        # key no DECIMAL128, the column is left unbounded (no stats, not wrong).
        if _INT64_MIN <= unscaled <= _INT64_MAX:
            builder.set_decimal_bound(row, position, is_min, unscaled, -exponent, float(value))
    else:
        raise DatasetReadError(
            f"A decoded manifest bound of type {kind.__name__} ({value!r}) has no manifest representation."
        )


def _normalized_view_schema(stored, view_name: str) -> Optional[RelationDescriptor]:
    """A view's stored schema as an engine RelationDescriptor, or None.

    The catalog hands back its own dependency-free schema object - the same one
    `SimpleDataset.schema()` returns - so this is the identical normalization a
    dataset's schema gets, and a type means the same thing whichever of the two
    it was read from.

    None is a view registered before schemas were stored, not a view with no
    columns: nothing can be registered without binding, and a definition that
    bound produced at least one column.
    """
    if stored is None:
        return None
    return OpteryxTable._normalize_schema(stored, relation_name=view_name)


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
    supports_vector_indexes = True  # CREATE / ALTER / DROP INDEX (vector index)
    supports_predicate_pushdown = True  # Via FileSystemTable base
    supports_limit_pushdown = True  # Via FileSystemTable base
    supports_statistics = True  # Opteryx manifests provide stats
    requires_execution_context = True  # information_schema row-level permission filtering
    requires_original_case = False  # table_engine() takes the case-folded relation name

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
        # The field-ids the catalog keys each data file this connector has
        # written (and not yet committed or deleted) by, by path: the commit
        # writes them into the manifest it hands the catalog (see
        # _catalog_manifest_bytes). This connector is shared across queries,
        # hence the lock.
        self._written_field_ids: dict = {}
        self._written_lock = threading.Lock()

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
        # The table gets a way to reach OTHER workspaces' catalogs, for the one
        # question that crosses a boundary: whether a source a receipt names
        # still exists (SHOW LINEAGE). The resolver is this connector's own
        # `_get_catalog`, so those lookups share its cache and its liveness
        # re-check rather than constructing a second catalog handle.
        return OpteryxTable(
            dataset=relative_id,
            catalog=catalog,
            workspace=workspace,
            catalog_resolver=self._get_catalog,
            **merged_kwargs,
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
                schema=_normalized_view_schema(obj.metadata.schema, name),
            )
        if kind == "dataset":
            return "dataset", obj
        return None, None

    def get_relations(self, names):
        """The plural of `get_relation`: resolve several relations in as few catalog
        round trips as the catalog can do them in.

        Returns ``{name: (kind, payload)}`` in the same shapes `get_relation`
        answers with, and omits nothing - a name the catalog does not hold maps to
        ``(None, None)``, exactly as the singular call reports it.

        Names are grouped by WORKSPACE because a round trip is made to a catalog and
        one connector serves several. A catalog with no plural `get_relations` is
        asked one name at a time here, so it costs what it costs today and nothing
        upstream has to know which kind it is talking to.
        """
        from opteryx.connectors.capabilities.eidetic import ViewDefinition

        by_workspace: Dict[str, List[Tuple[str, str]]] = {}
        answers: Dict[str, Tuple] = {}
        for name in names:
            workspace, relative_id = self._parse_identifier(name)
            # Reserved nested schema served by table_engine(), not a catalog document
            # - the singular path skips the round trip for it and so does this.
            if relative_id.partition(".")[0] == "information_schema":
                answers[name] = (None, None)
                continue
            by_workspace.setdefault(workspace, []).append((name, relative_id))

        for workspace, pairs in by_workspace.items():
            catalog = self._get_catalog(workspace)
            plural = getattr(catalog, "get_relations", None)
            if plural is None:
                raw = {rel: catalog.get_relation(rel) for _name, rel in pairs}
            else:
                raw = plural([rel for _name, rel in pairs])
            for name, relative_id in pairs:
                kind, obj = raw.get(relative_id, (None, None))
                if kind == "view":
                    answers[name] = (
                        "view",
                        ViewDefinition(
                            name=obj.name,
                            statement=obj.definition,
                            owner=obj.metadata.author,
                            last_row_count=obj.metadata.last_execution_records,
                            schema=_normalized_view_schema(obj.metadata.schema, name),
                        ),
                    )
                elif kind == "dataset":
                    answers[name] = ("dataset", obj)
                else:
                    answers[name] = (None, None)
        return answers

    # Relation operations (Writable capability)
    def open_data_file_writer(
        self,
        relation_name: str,
        sorted_by: Optional[str] = None,
        sorted_descending: bool = False,
        write_profile: str = "fast",
        pending_schema=None,
    ):
        """Open one streaming data file - see Writable.open_data_file_writer.

        The catalog owns the file's name, its storage stream and its manifest
        entry (`Dataset.open_data_file_writer`); this wraps that handle so the
        engine sees a native file row on close. The two write profiles are the
        catalog's own two option sets: "fast" for ingest and CTAS, "storage"
        for a compaction rewrite that is read many times.

        `pending_schema` names the CTAS case, where the dataset will not exist
        until this statement's files are all durable: the catalog opens the
        writer from the schema instead of from a registered dataset
        (`open_pending_data_file_writer`). Both the location and the field-ids
        it keys the file's statistics with are derived there, by the helpers
        `create_dataset` itself uses - deliberately NOT mirrored here, because
        a second copy of either formula in the engine drifts from the catalog's
        silently, and a file keyed by stale field-ids describes its columns
        under ids the finished dataset gives to other columns.
        """
        from opteryx_catalog.iops.fileio import COMPACTION_WRITE_PARQUET_OPTIONS
        from opteryx_catalog.iops.fileio import WRITE_PARQUET_OPTIONS

        if write_profile == "fast":
            options = WRITE_PARQUET_OPTIONS
        elif write_profile == "storage":
            options = COMPACTION_WRITE_PARQUET_OPTIONS
        else:
            raise ValueError(
                f"open_data_file_writer: write_profile must be 'fast' or 'storage', "
                f"got {write_profile!r}"
            )

        workspace, relative_id = self._parse_identifier(relation_name)
        catalog = self._get_catalog(workspace)
        # statistics=False: the engine describes its own files natively
        # (_DataFileWriterHandle) and commits them as a manifest.
        if pending_schema is not None:
            handle = catalog.open_pending_data_file_writer(
                relative_id,
                pending_schema,
                sorted_by=sorted_by,
                sorted_descending=sorted_descending,
                write_options=options,
                statistics=False,
            )
        else:
            dataset = catalog.load_dataset(relative_id)
            handle = dataset.open_data_file_writer(
                sorted_by=sorted_by,
                sorted_descending=sorted_descending,
                write_options=options,
                statistics=False,
            )
        return _DataFileWriterHandle(handle, self)

    def _record_written(self, file_path: str, field_id_by_name: dict) -> None:
        with self._written_lock:
            self._written_field_ids[file_path] = field_id_by_name

    def _catalog_manifest_bytes(self, operation: str, rows) -> bytes:
        """The batch `rows` (native file rows, statistics computed as the files
        streamed out) as the manifest parquet the catalog commits, keyed by the
        field-ids the catalog gave the files, taken off the record.

        A file with no record came from somewhere other than
        open_data_file_writer, and that is a bug to name; files of one commit
        keyed by two different field-id maps describe two different schemas.
        """
        paths = rows.file_paths()
        with self._written_lock:
            missing = [path for path in paths if path not in self._written_field_ids]
            if missing:
                raise ValueError(
                    f"{operation}: {len(missing)} output file(s) were not written through "
                    f"open_data_file_writer ({missing[:3]})"
                )
            maps = [self._written_field_ids.pop(path) for path in paths]
        if any(field_ids != maps[0] for field_ids in maps[1:]):
            raise ValueError(f"{operation}: the output files are keyed by different field-ids")
        field_id_by_name = maps[0] if maps else {}
        return rows.to_parquet([field_id_by_name.get(name) for name in rows.columns])

    def delete_data_file(self, relation_name: str, file_path: str) -> None:
        """Remove one data file this session wrote, through the catalog's FileIO.

        For a writer cleaning up after a refused commit: the file is referenced
        by nothing, so leaving it is pure orphaned storage. Deliberately NOT a
        general delete — it takes a path the caller just wrote and removes it,
        which is the only case any operator has.
        """
        workspace, _ = self._parse_identifier(relation_name)
        self._get_catalog(workspace).io.delete(file_path)
        with self._written_lock:
            self._written_field_ids.pop(file_path, None)

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

    def supports_forking(self, relation_name: str) -> bool:
        """Whether this relation's workspace is backed by the native metastore.

        THE METASTORE ANSWERS, NOT THIS CONNECTOR. This one class fronts the
        Opteryx catalog for one workspace and an external metastore - Iceberg,
        and whatever follows it - for the next, so the question has to be put
        to the store that actually holds the relation. See
        `BaseConnector.supports_forking` for why only the native store may say
        yes, and `Metastore.supports_forking` (opteryx-catalog) for the
        declaration itself.

        An implementation that does not declare it is an ERROR rather than a
        refusal: the same posture `bounds_are_ordinal` takes in
        `get_dataset_metadata`, and for the same reason - a store that has not
        said whether its files are ours to borrow has not been thought about,
        and guessing either way is worse than saying so.
        """
        workspace, _ = self._parse_identifier(relation_name)
        catalog = self._get_catalog(workspace)
        declared = getattr(catalog, "supports_forking", None)
        if declared is None:
            raise UnsupportedSyntaxError(
                f"{type(catalog).__name__} does not declare `supports_forking`, so whether "
                f"{md_code(relation_name)} can be cloned is unknown. Implementations of "
                "opteryx-catalog's `Metastore` must set it (True only for a store providing "
                "Opteryx snapshot and fork-registry mechanics)."
            )
        return bool(declared)

    def clone_relation(
        self,
        target_relation: str,
        source_relation: str,
        author: Optional[str] = None,
        snapshot_id: Optional[int] = None,
    ) -> int:
        """Create `target_relation` as a fork of `source_relation`, copying nothing.

        The target's first manifest lists the same files the source's listed, so
        the cost is one manifest whether the source is 3 MB or 3 TB, and every
        statistic in it was computed once - by the writer that had the bytes -
        rather than rediscovered by reading them all back.

        THE TARGET'S CATALOG DOES THE WORK. A fork is a write to the target and
        a read of the source, and the target's workspace is where the dataset
        document, the manifest and the storage all land; the source's workspace
        is reached for its entries and its `forks/` registry, which any handle
        in the same Firestore database can do (see `_foreign_dataset_doc_ref`).
        The alternative - the source's catalog writing into the target - would
        need a handle per source workspace and would re-run the constructor's
        gates for exactly the workspaces a clone most wants to read.
        """
        workspace, relative_target = self._parse_identifier(target_relation)
        catalog = self._get_catalog(workspace)
        # The source goes through unsplit: the planner hands over a fully
        # qualified name, and the catalog's own `_qualify` is the single place
        # a workspace is ever inferred for one that is not.
        catalog.clone_dataset(
            str(source_relation),
            relative_target,
            author=author,
            snapshot_id=snapshot_id,
        )
        return 1

    def clone_collection(
        self, target_collection: str, source_collection: str, author: Optional[str] = None
    ) -> int:
        """Fork every dataset in `source_collection` into `target_collection`.

        Returns the number of datasets forked, which is what the caller can go
        and look at - and is a count of datasets, never of rows: no rows were
        read to produce them.
        """
        workspace, relative_target = self._parse_identifier(target_collection)
        catalog = self._get_catalog(workspace)
        return catalog.clone_collection(
            str(source_collection), relative_target, author=author
        )

    def resync_relation(
        self, relation_name: str, author: Optional[str] = None, force: bool = False
    ) -> int:
        """Make a fork equal its upstream's current content again."""
        workspace, relative_id = self._parse_identifier(relation_name)
        catalog = self._get_catalog(workspace)
        catalog.resync_fork(relative_id, author=author, force=force)
        return 1

    def detach_relation(self, relation_name: str, author: Optional[str] = None) -> int:
        """Materialise a fork's borrowed files and end the relationship."""
        workspace, relative_id = self._parse_identifier(relation_name)
        catalog = self._get_catalog(workspace)
        result = catalog.detach_fork(relative_id, author=author)
        return int(result.get("files_copied", 0))

    def fork_state(self, relation_name: str) -> Optional[dict]:
        """How far a fork has diverged from its upstream, or None if not a fork.

        Both numbers are UPPER BOUNDS - see `SimpleDataset.fork_state`. A caller
        rendering them must say "at most N revisions", because a sequence number
        advances on every commit, maintenance included.
        """
        workspace, relative_id = self._parse_identifier(relation_name)
        catalog = self._get_catalog(workspace)
        dataset = catalog.load_dataset(relative_id, load_history=True)
        if dataset is None:
            raise DatasetNotFoundError(
                dataset=relation_name, connector=self.__class__.__name__
            )
        return dataset.fork_state()

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

    def drop_secret(
        self,
        workspace_name: str,
        secret_name: str,
        author: Optional[str] = None,
        if_exists: bool = False,
    ) -> bool:
        """DROP SECRET - see `Writable.drop_secret`. The catalog owns the record."""
        catalog = self._get_catalog(workspace_name)
        drop = getattr(catalog, "drop_secret", None)
        if drop is None:
            raise NotImplementedError("this catalog does not hold secrets")
        try:
            from opteryx_catalog.secrets import SecretNotFound
        except ImportError:  # pragma: no cover - an older catalog has no secrets
            SecretNotFound = KeyError
        try:
            return drop(secret_name, author=author, if_exists=if_exists)
        except SecretNotFound as exc:
            raise ValueError(
                f"secret {workspace_name}.{secret_name} does not exist "
                "(use DROP SECRET IF EXISTS to make this quiet)"
            ) from exc

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


    # ── provenance ──────────────────────────────────────────────────────────

    def _provenance_kwargs(self, method, read_sources, produced_by) -> dict:
        """The receipt kwargs, or nothing, by what the catalog accepts.

        Engine and catalog deploy separately. A catalog older than the receipt
        has commit methods without these parameters, and a `TypeError` from a
        commit is a write that reported failure after its files landed - the
        worst available outcome. So the kwargs are passed only when the bound
        method takes them; otherwise they are dropped, the receipt is absent,
        and the CATALOG (once upgraded) is what reports the gap. Same posture
        as `mark_secure` above, decided per method rather than per module.
        """
        import inspect

        try:
            parameters = inspect.signature(method).parameters
        except (TypeError, ValueError):
            return {}
        kwargs = {}
        if "read_sources" in parameters:
            kwargs["read_sources"] = None if read_sources is None else list(read_sources)
        if "produced_by" in parameters:
            kwargs["produced_by"] = self._qualified_producer(produced_by)
        return kwargs

    def _qualified_producer(self, produced_by: Optional[str]) -> Optional[str]:
        """`task:<name>` / `view:<name>` with the name fully qualified. The
        binder sees the name as it was written, which may omit the workspace;
        the connector is where a workspace is known for certain."""
        if not produced_by:
            return None
        kind, _, name = produced_by.partition(":")
        if not name:
            return produced_by
        workspace, relative = self._parse_identifier(name)
        return f"{kind}:{workspace}.{relative}"

    def insert(
        self,
        relation_name: str,
        rows,
        author: Optional[str] = None,
        commit_message: Optional[str] = None,
        read_sources: Optional[list] = None,
        produced_by: Optional[str] = None,
    ) -> None:
        """Commit pre-written parquet files into the catalog as a new snapshot,
        appended to whatever the dataset already contains.

        `commit_message` is passed through as given, including None: the catalog
        composes its own default ("add files by <author>") for an append that
        has nothing more specific to say."""
        workspace, relative_id = self._parse_identifier(relation_name)
        catalog = self._get_catalog(workspace)
        manifest = self._catalog_manifest_bytes("insert", rows)
        index_files = self._sync_index_files(catalog, relation_name, relative_id, rows)

        def _commit_add_files():
            dataset = catalog.load_dataset(relative_id)
            return dataset.add_files(
                manifest=manifest,
                author=author,
                commit_message=commit_message,
                index_files=index_files,
                **self._provenance_kwargs(dataset.add_files, read_sources, produced_by),
            )

        self._commit(relation_name, _commit_add_files)

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
        rows,
        delete_positions,
        author: Optional[str] = None,
        commit_message: Optional[str] = None,
        operation: str = "merge",
        read_sources: Optional[list] = None,
        produced_by: Optional[str] = None,
    ) -> None:
        """Commit pre-written parquet files and row-level deletes as ONE snapshot.

        The write half of MERGE - see `Writable.merge_commit` for why the two
        halves cannot be two commits. `delete_positions` maps data-file paths as
        they appear in the current manifest to file-local row ordinals; those
        paths are the same strings the data file writer produced and the scan read
        back, so no translation happens here.

        `commit_message` is passed through as given, including None: the catalog
        composes its own default naming the file and row counts.

        `operation` names the statement - the catalog stamps it on the snapshot
        and the audit record, and validates it against its own vocabulary."""
        workspace, relative_id = self._parse_identifier(relation_name)
        catalog = self._get_catalog(workspace)
        manifest = self._catalog_manifest_bytes("merge_commit", rows)
        index_files = self._sync_index_files(catalog, relation_name, relative_id, rows)

        def _commit_merge():
            dataset = catalog.load_dataset(relative_id)
            return dataset.merge_commit(
                manifest=manifest,
                positions=delete_positions,
                index_files=index_files,
                author=author,
                commit_message=commit_message,
                operation=operation,
                **self._provenance_kwargs(dataset.merge_commit, read_sources, produced_by),
            )

        self._commit(relation_name, _commit_merge)

    def compaction_commit(
        self,
        relation_name: str,
        rows,
        retired_files,
        author: Optional[str] = None,
        baseline_snapshot_id: Optional[int] = None,
        commit_message: Optional[str] = None,
        index_files: Optional[dict] = None,
    ) -> None:
        """Retire whole data files and add their replacements as ONE snapshot.

        The commit half of OPTIMIZE. `index_files` are the outputs' CARRIED vector index
        files (`carry_compaction_vectors`), committed in the same snapshot (§5.6). `rows` are the outputs the sink
        already wrote; `retired_files` are the manifest paths they replace.

        Whole-file retirement rather than `merge_commit`'s row ordinals: a
        compaction pass replaces files wholesale, and expressing that as
        ordinals would mean naming every row. The catalog enforces the
        row-count invariant and raises rather than returning, so the caller
        can remove its outputs — see `Dataset.compaction_commit`.
        """
        workspace, relative_id = self._parse_identifier(relation_name)
        catalog = self._get_catalog(workspace)
        manifest = self._catalog_manifest_bytes("compaction_commit", rows)
        self._commit(
            relation_name,
            lambda: catalog.load_dataset(relative_id).compaction_commit(
                manifest=manifest,
                retired_files=retired_files,
                author=author,
                baseline_snapshot_id=baseline_snapshot_id,
                commit_message=commit_message,
                index_files=index_files,
            ),
        )

    def vector_index_coverage(self, relation_name: str, snapshot_id: Optional[int]) -> dict:
        """{data file path: the vector index ids covering it} at a snapshot - compaction
        selection groups by it (§5.6)."""
        workspace, relative_id = self._parse_identifier(relation_name)
        return self._get_catalog(workspace).load_dataset(relative_id).vector_index_coverage(snapshot_id)

    def claim_compaction_lease(self, relation_name: str, holder: str):
        """The dataset's maintenance lease for a compaction (§5.7): refused loudly while an
        index build holds it. Returns a handle with `renew()` and `release()`."""
        return _MaintenanceLeaseHandle(self, relation_name, holder, "compaction")

    def carry_compaction_vectors(
        self, relation_name: str, retired_files, baseline_snapshot_id: int, outputs, recorders
    ) -> dict:
        """Carry the retired files' vector indexes into a compaction's outputs (§5.6).

        `retired_files` in SCAN order (a recorded `$file` is a position in it), `outputs`
        the written data files and `recorders` their row-origin maps, aligned. Returns
        `{output path: {index id: IndexFiles}}` - empty when the inputs are unindexed.
        Control plane only: ONE native carry per index reads every input's vectors and
        writes every output's index files; nothing here embeds. The inputs' coverage must
        be uniform (selection grouped them so); the catalog refuses otherwise."""
        import os

        from opteryx_catalog.catalog.vector_indexes import IndexFiles
        from opteryx_catalog.catalog.vector_indexes import vector_index_path

        from opteryx import config
        from opteryx.exceptions import NotSupportedError
        from opteryx.operators._operators import carry_vector_index_local
        from opteryx.operators._operators import carry_vector_index_to_sessions

        workspace, relative_id = self._parse_identifier(relation_name)
        catalog = self._get_catalog(workspace)
        dataset = catalog.load_dataset(relative_id)
        inputs = dataset.compaction_carry_inputs(retired_files, baseline_snapshot_id)
        coverages = {frozenset(refs) for refs, _ in inputs.values()}
        if coverages == {frozenset()}:
            return {}
        if len(coverages) != 1:
            from opteryx.exceptions import InvalidInternalStateError

            raise InvalidInternalStateError(
                f"compaction of {relation_name} selected files with different vector index coverage"
            )
        if len(recorders) != len(outputs):
            from opteryx.exceptions import InvalidInternalStateError

            raise InvalidInternalStateError("compaction recorded row origins for a different number of files")
        definitions = {d["index-id"]: d for d in catalog.list_vector_indexes(relative_id)}
        threads = config.resolve_max_execution_workers()
        carried: dict = {path: {} for path in outputs}
        index_ids = sorted(coverages.pop())
        for position, index_id in enumerate(index_ids):
            definition = definitions.get(index_id)
            if definition is None:
                raise ValueError(
                    f"{relation_name}: the files being compacted are indexed by {index_id}, which "
                    "is no longer defined; drop its files (DROP INDEX) before compacting."
                )
            # A recorder is spent by a carry, so each index after the first carries from a copy.
            spent = recorders if position == len(index_ids) - 1 else [r.copy() for r in recorders]
            remote = [f for f in retired_files if "://" in f and not f.startswith("gs://")]
            if remote:
                raise NotSupportedError(f"Vector carry reads local or GCS files; {remote[0]} is neither.")
            specs = []
            readable = _index_reads()
            for path in retired_files:
                refs, deleted = inputs[path]
                files = refs[index_id]
                location, auth_header = readable(files.path)
                specs.append((location, files.file_bytes, files.footer_bytes, list(deleted), auth_header))
            targets = [vector_index_path(dataset.metadata.location, index_id, out) for out in outputs]
            options = dict(clusters=definition["clusters"], train_threads=threads)
            if all(out.startswith("gs://") for out in outputs):
                built = _carry_on_gcs(catalog.io, specs, spent, definition["dimensions"], targets,
                                      options, carry_vector_index_to_sessions)
            elif not any("://" in out for out in outputs):
                for target in targets:
                    os.makedirs(os.path.dirname(target), exist_ok=True)
                built = carry_vector_index_local(specs, spent, definition["dimensions"], targets, **options)
            else:
                raise NotSupportedError("Vector carry writes outputs on local disk or GCS.")
            for out, target, result in zip(outputs, targets, built):
                if result is None:
                    raise NotSupportedError(
                        f"compaction output {out} carries no vector for index {definition['name']}: "
                        "every row it holds was unindexed. A file with no indexable row cannot be "
                        "recorded as indexed, and compaction may not change coverage (§5.6)."
                    )
                carried[out][index_id] = IndexFiles(
                    path=target, file_bytes=result["file_bytes"], footer_bytes=result["footer_bytes"],
                    logical_bytes=result["logical_bytes"],
                )
        return carried

    def replace_relation(
        self,
        relation_name: str,
        schema,
        rows,
        author: Optional[str] = None,
        commit_message: Optional[str] = None,
        read_sources: Optional[list] = None,
        produced_by: Optional[str] = None,
    ) -> None:
        """Atomically replace a dataset's entire contents with the given files,
        as a single new snapshot (CREATE OR REPLACE ... AS SELECT). Schema is
        unchanged - this does not evolve the dataset's schema.

        `commit_message` is passed through as given, including None: the catalog
        composes its own default ("truncate and add files by <author>") for a
        replace that has nothing more specific to say."""
        workspace, relative_id = self._parse_identifier(relation_name)
        catalog = self._get_catalog(workspace)
        manifest = self._catalog_manifest_bytes("replace_relation", rows)
        index_files = self._sync_index_files(catalog, relation_name, relative_id, rows)

        def _commit_replace():
            dataset = catalog.load_dataset(relative_id)
            return dataset.truncate_and_add_files(
                manifest=manifest,
                index_files=index_files,
                author=author,
                commit_message=commit_message,
                **self._provenance_kwargs(
                    dataset.truncate_and_add_files, read_sources, produced_by
                ),
            )

        self._commit(relation_name, _commit_replace)

    def relation_column_names(self, relation_name: str):
        """Return the dataset's current column names only (not full type fidelity)."""
        workspace, relative_id = self._parse_identifier(relation_name)
        catalog = self._get_catalog(workspace)
        schema = catalog.load_dataset(relative_id).schema()
        return [c.name for c in schema.columns]

    def relation_schema(self, relation_name: str) -> RelationDescriptor:
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
        reads: Optional[List[str]] = None,
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

        # `reads` is passed only to a catalog that takes it: it is the newer of
        # the two declarations, and a registration that failed on it would
        # refuse the task for the sake of a field the sweep can backfill.
        import inspect

        extra = {}
        try:
            if "reads" in inspect.signature(catalog.create_task).parameters:
                extra["reads"] = list(reads or [])
        except (TypeError, ValueError):
            pass

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
                **extra,
            )
        except TaskAlreadyExists as exc:
            raise ValueError(
                f"task {relation_name} already exists "
                "(use CREATE OR REPLACE TASK to redefine it)"
            ) from exc

    def alter_task_statement(
        self,
        relation_name: str,
        statement: str,
        author: Optional[str] = None,
        writes: Optional[List[str]] = None,
        reads: Optional[List[str]] = None,
    ) -> None:
        """ALTER TASK <name> AS <statement>: redefine the SQL body only.

        Routes to the catalog's own `alter_task_statement`, a method
        dedicated to this narrow edit rather than `create_task` called with
        a flag - see that method's docstring for why. A catalog that
        predates it (this connector installed against an older wheel) has
        no fallback: ALTER TASK is simply unavailable rather than degrading
        to a full CREATE OR REPLACE TASK, which would reset the trigger's
        due instant this statement exists to avoid resetting.
        """
        try:
            from opteryx_catalog.exceptions import TaskNotFound
        except ImportError:
            TaskNotFound = KeyError

        workspace, relative_id = self._parse_identifier(relation_name)
        catalog = self._get_catalog(workspace)

        try:
            catalog.alter_task_statement(
                relative_id,
                sql=statement,
                author=author,
                writes=list(writes or []),
                reads=list(reads or []),
            )
        except TaskNotFound as exc:
            raise ValueError(f"task {relation_name} does not exist") from exc

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
        """The relations the task's statement writes, from its catalog record.

        Tasks only. A materialized view IS what it writes, so the binder never
        asks this of one - see `_bind_subscription`.
        """
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
                "object_catalog": row.get("workspace"),
                "object_collection": row.get("collection"),
                "object_name": row.get("object"),
                "kind": row.get("kind"),
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

    # --- vector indexes (docs/VECTOR_INDEX_DESIGN.md; definitions in the catalog's
    # `indexes` subcollection, sidecars referenced per data file from the manifest) ---

    def create_vector_index(
        self,
        relation_name: str,
        index_name: str,
        column_name: str,
        *,
        options: dict,
        embedding_identity: str,
        dimensions: int,
        if_not_exists: bool,
        author: Optional[str] = None,
    ) -> Optional[int]:
        """Define a vector index. Returns the number of files built (0 for an `async`
        index, whose files REFRESH INDEX builds), or None when IF NOT EXISTS met an
        existing index (which is then left exactly as it was).

        A `sync` index builds every existing file before this returns (D-7), under the
        maintenance lease (§5.7), which is claimed BEFORE the definition is created: if
        it cannot be had, the definition is not created. A build that fails drops the
        definition again (with any files it had committed), so a failed CREATE leaves no
        index behind."""
        from opteryx_catalog.exceptions import VectorIndexAlreadyExists

        workspace, relative_id = self._parse_identifier(relation_name)
        catalog = self._get_catalog(workspace)

        def _define():
            try:
                return catalog.create_vector_index(
                    relative_id,
                    index_name,
                    column_name,
                    embedding_identity=embedding_identity,
                    dimensions=dimensions,
                    author=author,
                    build=options.get("build"),
                    clusters=options.get("clusters", 0),
                )
            except VectorIndexAlreadyExists as exc:
                if if_not_exists:
                    return None
                # Translated at the boundary, like the tag errors: the catalog's message
                # already names the index and what to do.
                raise ValueError(str(exc)) from exc

        if options.get("build") != "sync":
            return None if _define() is None else 0

        with _index_build_lease(catalog, relative_id, f"CREATE INDEX {index_name} by {author}") as lost:
            definition = _define()
            if definition is None:
                return None
            try:
                _require_index_embedder(definition, relation_name)
                return self._build_uncovered_files(
                    catalog, relation_name, relative_id, definition, author, lost
                )
            except BaseException:
                catalog.drop_vector_index(relative_id, definition["name"], author=author)
                raise

    def alter_vector_index_build(
        self, relation_name: str, index_name: str, build: str, author: Optional[str] = None
    ) -> dict:
        from opteryx_catalog.exceptions import VectorIndexNotFound

        workspace, relative_id = self._parse_identifier(relation_name)
        catalog = self._get_catalog(workspace)
        try:
            return catalog.alter_vector_index(relative_id, index_name, build=build, author=author)
        except VectorIndexNotFound as exc:
            raise ValueError(f"There is no index {index_name} on {relation_name}.") from exc

    def drop_vector_index(
        self, relation_name: str, index_name: str, if_exists: bool, author: Optional[str] = None
    ) -> None:
        from opteryx_catalog.exceptions import VectorIndexNotFound

        workspace, relative_id = self._parse_identifier(relation_name)
        catalog = self._get_catalog(workspace)
        try:
            catalog.drop_vector_index(relative_id, index_name, author=author)
        except VectorIndexNotFound as exc:
            if if_exists:
                return
            raise ValueError(f"There is no index {index_name} on {relation_name}.") from exc

    def list_vector_indexes(self, relation_name: str) -> list:
        """The vector indexes defined on a relation, as the catalog's plain dicts."""
        workspace, relative_id = self._parse_identifier(relation_name)
        return self._get_catalog(workspace).list_vector_indexes(relative_id)

    def vector_index_status(self, relation_name: str) -> list:
        """SHOW INDEXES (§7A): each index's definition with how far it covers the head
        snapshot - `files_indexed` of `files_total` live data files - and the logical
        bytes of its files (what is billed, §5.5). Read from the catalog and the head
        manifest only; no index file is opened."""
        workspace, relative_id = self._parse_identifier(relation_name)
        catalog = self._get_catalog(workspace)
        definitions = catalog.list_vector_indexes(relative_id)
        if not definitions:
            return []
        dataset = catalog.load_dataset(relative_id)
        files_total = len(dataset.vector_index_coverage())
        out = []
        for definition in sorted(definitions, key=lambda d: d["name"]):
            files = dataset.vector_index_files(definition["index-id"])
            out.append({
                **definition,
                "files_indexed": len(files),
                "files_total": files_total,
                "index_bytes": sum(f.logical_bytes for f in files.values()),
            })
        return out

    def refresh_vector_index(self, relation_name: str, index_name: str, author: Optional[str] = None) -> int:
        """REFRESH INDEX (D-16): index every live data file the index does not cover yet,
        under the maintenance lease (§5.7). Returns the number of files indexed."""
        from opteryx_catalog.exceptions import VectorIndexNotFound

        workspace, relative_id = self._parse_identifier(relation_name)
        catalog = self._get_catalog(workspace)
        try:
            definition = catalog.get_vector_index(relative_id, index_name)
        except VectorIndexNotFound as exc:
            raise ValueError(f"There is no index {index_name} on {relation_name}.") from exc
        _require_index_embedder(definition, relation_name)
        with _index_build_lease(catalog, relative_id, f"REFRESH INDEX {index_name} by {author}") as lost:
            return self._build_uncovered_files(catalog, relation_name, relative_id, definition, author, lost)

    def _build_uncovered_files(self, catalog, relation_name, relative_id, definition, author, lost) -> int:
        """Build and commit the index files of every live data file `definition` does not
        cover. The caller holds the maintenance lease; `lost()` raises once it is gone.

        Each file is committed as soon as it is built, so a failure keeps every file
        indexed before it. A file with no indexable row (all null, deleted or without a
        defined cosine) gets no index file and stays searched exactly."""
        index_id = definition["index-id"]
        indexed = 0
        for task in catalog.load_dataset(relative_id).vector_index_build_plan(index_id):
            files = _build_index_files(
                catalog, definition, task.data_file, task.data_bytes, task.deleted, task.path,
            )
            lost()
            if files is None:
                continue
            self._commit(
                relation_name,
                lambda: catalog.load_dataset(relative_id).commit_vector_index_files(
                    index_id, {task.data_file: files}, author=author, agent="refresh-index"
                ),
            )
            indexed += 1
        return indexed

    def _sync_index_files(self, catalog, relation_name: str, relative_id: str, rows) -> Optional[dict]:
        """A SYNC index's files for the data files a write is about to commit (D-7): built
        here, before the commit, and referenced in the same snapshot, so the new files are
        never unindexed. `{file path: {index id: IndexFiles}}`, or None with no sync index.

        Takes no maintenance lease (§5.7): a write indexes only its OWN new files, which no
        compaction can have selected yet. A file with no indexable row gets none."""
        from opteryx_catalog.catalog.vector_indexes import vector_index_path

        sync = [d for d in catalog.list_vector_indexes(relative_id) if d.get("build") == "sync"]
        if not sync:
            return None
        for definition in sync:
            _require_index_embedder(definition, relation_name)
        location = catalog.load_dataset(relative_id).metadata.location
        built: dict = {}
        for path, size in zip(rows.file_paths(), rows.file_sizes()):
            for definition in sync:
                target = vector_index_path(location, definition["index-id"], path)
                files = _build_index_files(catalog, definition, path, size, (), target)
                if files is not None:
                    built.setdefault(path, {})[definition["index-id"]] = files
        return built

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
            schema=_normalized_view_schema(view.metadata.schema, view_name),
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
                        schema=_normalized_view_schema(view.metadata.schema, view.name),
                    )
                )
            except (KeyError, AttributeError):
                # Skip views that can't be loaded or have missing attributes
                pass

        return views

    def create_view(
        self,
        view_name: str,
        statement: str,
        update_if_exists: bool = False,
        owner: str = None,
        schema: Optional[RelationDescriptor] = None,
    ):
        """Create a new view with the given name and definition.

        `schema` is the view's output columns as the binder resolved them from
        `statement` - recorded as catalog metadata, never read back to expand
        the view. The catalog stores it in its own column spelling, the same one
        a dataset's schema document uses.
        """
        # Parse view_name into workspace and relative identifier
        workspace, relative_id = self._parse_identifier(view_name)
        catalog = self._get_catalog(workspace)

        # Split relative identifier into collection and name for catalog
        parts = relative_id.split(".")
        name = parts[-1]
        collection = ".".join(parts[:-1])

        identifier = (collection, name)
        catalog.create_view(
            identifier=identifier,
            sql=statement,
            update_if_exists=update_if_exists,
            author=owner,
            schema=schema,
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


class _DataFileWriterHandle:
    """The engine's view of one streaming data file (Writable.open_data_file_writer).

    Wraps the catalog's DataFileWriter opened WITHOUT statistics: the engine
    describes every file itself, natively (FileStats - the catalog manifest's
    full statistic set), as the row groups stream out. `close` answers with the
    native file row and records, on the connector, the field-ids the catalog
    keys this file's statistics by - which the commit writes into the manifest
    it hands the catalog.
    """

    def __init__(self, inner, connector):
        from opteryx.compiled.planner.native_manifest import FileStats

        self._inner = inner
        self._connector = connector
        self._stats = FileStats()

    @property
    def file_path(self) -> str:
        return self._inner.data_path

    @property
    def uncompressed_size_in_bytes(self) -> int:
        return self._stats.uncompressed_size

    @property
    def record_count(self) -> int:
        return self._inner.record_count

    def write_row_group(self, morsel) -> None:
        # Statistics first: a column the kernels refuse is found before any
        # bytes of this row group are encoded or sent.
        self._stats.add_row_group(morsel)
        self._inner.write_row_group(morsel)

    def close(self):
        written = self._inner.close()
        self._connector._record_written(written.file_path, self._inner.field_id_by_name)
        return self._stats.file_row(
            written.file_path,
            "PARQUET",
            written.record_count,
            written.file_size_in_bytes,
            written.row_group_count,
            self._stats.uncompressed_size,
        )

    def abort(self) -> None:
        self._inner.abort()
