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

# One-shot guard so a catalog too old to expose native sketch vectors reports the
# degraded (Python-fallback) stats path once per process instead of silently.
_warned_no_native_sketches = False


def _warn_no_native_sketches() -> None:
    global _warned_no_native_sketches
    if not _warned_no_native_sketches:
        _warned_no_native_sketches = True
        logger.warning(
            "Catalog does not expose manifest_sketch_vectors; manifest statistics "
            "(NDV / histogram) fall back to the slower Python path. Upgrade the "
            "opteryx_catalog package to enable native sketch reductions."
        )
from opteryx.connectors.base.base_connector import BaseTable
from opteryx.connectors.capabilities import Diachronic, Eidetic, PredicatePushable, Writable
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

        Returns:
            RelationSchema
        """
        self._resolve_snapshot()
        raw_schema = self.table.schema(self.snapshot.schema_id)
        self.schema = self._normalize_schema(raw_schema, relation_name=self.dataset)
        self.dataset_committed_at = self.snapshot.timestamp_ms
        return self.schema

    def _resolve_snapshot(self) -> None:
        """Settle which snapshot this read sees, honouring time travel.

        Sets `self.snapshot` and `self.snapshot_id`. Shared by the schema-only and
        the full-metadata reads so a statement cannot resolve to one snapshot when
        checked and a different one when run.
        """
        if self.at_date is not None:
            # reload the dataset with history enabled
            self.table = self.catalog.load_dataset(self.dataset, load_history=True)
            snapshots = self.table.snapshots()

            if not snapshots:
                raise DatasetReadError("No data available for the specified date.")

            snapshots = sorted(snapshots, key=lambda s: s.timestamp_ms, reverse=False)

            # Honor dates before the first snapshot by rejecting them, but treat
            # dates after the latest snapshot as selecting the latest snapshot
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
                # Point-in-time read after the latest snapshot — return current data
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

        # If the table has no snapshot and the read is not time-travel, use
        # the table's declared schema (from metadata) and return an empty result set.
        if self.snapshot is None:
            self.snapshot = self.table.snapshot()
            if self.snapshot is None:
                raise DatasetReadError(
                    "The dataset exists, but no data has been committed to it yet."
                )
            self.snapshot_id = self.snapshot.snapshot_id

    def get_dataset_metadata(self) -> Tuple[RelationSchema, Manifest]:
        """
        Get dataset schema and build manifest from catalog.

        Returns both schema and manifest to make the dual purpose explicit.
        Manifest contains file-level statistics from table.scan().

        Returns:
            Tuple of (RelationSchema, Manifest)
        """
        self._resolve_snapshot()

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
        # kernels instead of the per-file boxed lists. A catalog that predates this
        # accessor keeps working via the Manifest's Python fallback — but that
        # degradation is announced once (not silent) so a stale catalog is visible.
        sketch_vectors_fn = getattr(self.table, "manifest_sketch_vectors", None)
        if sketch_vectors_fn is not None:
            sketch_vectors = sketch_vectors_fn(self.snapshot_id)
        else:
            sketch_vectors = {}
            _warn_no_native_sketches()

        # Create Manifest with files and schema.
        #
        # bounds_are_ordinal: the catalog's stats builder stores min/max as
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
            bounds_are_ordinal=True,
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

        # Return cached instance when available
        if catalog_name in self._catalog_cache:
            return self._catalog_cache[catalog_name]

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

        from opteryx.utils import random_string

        workspace, relative_id = self._parse_identifier(relation_name)
        catalog = self._get_catalog(workspace)
        location = self._dataset_location(relation_name)

        file_name = f"data-{random_string(32)}.parquet"
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

    def enforce_egress_policy(
        self, target_relation: str, source_relations: "List[str]"
    ) -> None:
        """Refuse a write that would copy another workspace's data into this one.

        The catalog owns the decision (`egress_protection` on the SOURCE
        workspace, which is on unless explicitly turned off); this method's job
        is to turn relation names into the workspaces they live in and ask.

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
            return

        catalog = self._get_catalog(target_workspace)

        # Fail closed on a catalog too old to hold the gate - both halves of it,
        # the method and the exception it signals with. Silently skipping would
        # turn a version skew into an unenforced security control, which is the
        # one outcome worse than refusing a legitimate copy.
        try:
            from opteryx_catalog.exceptions import EgressRestricted
        except ImportError:
            EgressRestricted = None
        enforce = getattr(catalog, "enforce_egress_policy", None)
        if enforce is None or EgressRestricted is None:
            raise EgressRestrictedError(
                f"Cannot write {target_relation} from another workspace's data: this "
                "deployment's opteryx-catalog is too old to evaluate egress protection. "
                "Upgrade opteryx-catalog, or run the statement within one workspace."
            )

        try:
            enforce(
                source_workspaces,
                target_workspace,
                f"write {target_relation}",
            )
        except EgressRestricted as exc:
            raise EgressRestrictedError(str(exc)) from exc

    def relation_exists(self, relation_name: str) -> bool:
        """Check whether a dataset exists in the catalog."""
        workspace, relative_id = self._parse_identifier(relation_name)
        catalog = self._get_catalog(workspace)
        return catalog.dataset_exists(relative_id)

    def insert(
        self, relation_name: str, file_entries, author: Optional[str] = None
    ) -> None:
        """Commit pre-written parquet files into the catalog as a new snapshot,
        appended to whatever the dataset already contains."""
        workspace, relative_id = self._parse_identifier(relation_name)
        catalog = self._get_catalog(workspace)
        file_paths = [fe.file_path for fe in file_entries]
        catalog.load_dataset(relative_id).add_files(file_paths, author=author)

    def replace_relation(
        self, relation_name: str, schema, file_entries, author: Optional[str] = None
    ) -> None:
        """Atomically replace a dataset's entire contents with the given files,
        as a single new snapshot (CREATE OR REPLACE ... AS SELECT). Schema is
        unchanged - this does not evolve the dataset's schema."""
        workspace, relative_id = self._parse_identifier(relation_name)
        catalog = self._get_catalog(workspace)
        file_paths = [fe.file_path for fe in file_entries]
        catalog.load_dataset(relative_id).truncate_and_add_files(file_paths, author=author)

    def relation_column_names(self, relation_name: str):
        """Return the dataset's current column names only (not full type fidelity)."""
        workspace, relative_id = self._parse_identifier(relation_name)
        catalog = self._get_catalog(workspace)
        schema = catalog.load_dataset(relative_id).schema()
        return [c.name for c in schema.columns]

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

    def set_materialized_view_owner(
        self, relation_name: str, new_owner: str, author: str = None
    ) -> None:
        """Repoint the view's `runs-as` identity in the catalog."""
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
        """Remove a trigger from the dataset that carries it, delegating to the
        catalog. A missing trigger is translated into a clear ValueError unless
        missing_ok (IF EXISTS) - the catalog's own drop_trigger honours
        missing_ok, so that branch never raises."""
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
            catalog.drop_trigger(relative_id, trigger_name, author=author, missing_ok=missing_ok)
        except TriggerNotFound as exc:
            raise ValueError(
                f"trigger {trigger_name} does not exist on {relation_name} "
                "(use DROP TRIGGER IF EXISTS to make this quiet)"
            ) from exc

    def list_triggers(self, relation_name: str) -> list:
        """The triggers attached to a dataset, as the catalog's plain dicts."""
        workspace, relative_id = self._parse_identifier(relation_name)
        catalog = self._get_catalog(workspace)
        return catalog.list_triggers(relative_id)

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
