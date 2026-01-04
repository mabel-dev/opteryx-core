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

import datetime
from typing import Optional
from typing import Tuple

from orso.schema import RelationSchema
from orso.types import OrsoTypes

from opteryx.connectors import TableType
from opteryx.connectors.capabilities import Diachronic
from opteryx.connectors.capabilities import Eidetic
from opteryx.exceptions import DatasetNotFoundError
from opteryx.exceptions import DatasetReadError
from opteryx.models import FileEntry
from opteryx.models import Manifest
from opteryx.models import RelationStatistics


class OpteryxTable(Diachronic):
    """
    Plan-time table metadata provider for Opteryx tables.

    This is a transient object created per-table during planning that handles:
    - Schema resolution
    - Manifest building (file list + statistics)
    - Time-travel query resolution

    This class is PLAN-TIME ONLY - it does not perform any data reading.
    Execution uses generic filesystem readers based on file paths from the manifest.
    """

    __mode__ = "Blob"
    __type__ = "OPTERYX"
    __synchronousity__ = "asynchronous"  # Used by physical planner to select operator

    # Capability declarations (for plan-time)
    supports_diachronic = True  # Time-travel queries
    supports_statistics = True  # Manifest provides stats

    def __init__(self, dataset: str, catalog, workspace: str, **kwargs):
        """
        Initialize the plan-time table metadata provider.

        Args:
            dataset: The table name (after catalog prefix is removed)
            catalog: The Opteryx Catalog instance
            workspace: The workspace name
            **kwargs: Additional parameters (telemetry, start_date, end_date, etc.)
        """
        Diachronic.__init__(self, **kwargs)

        self.dataset = dataset.replace("/", ".")
        self.catalog = catalog
        self.workspace = workspace
        self.at_date = None
        self.telemetry = kwargs.get("telemetry")

        # Initialize state
        self.snapshot_id = None
        self.snapshot = None
        self.dataset_committed_at = None
        self.schema = None
        self.manifest = None
        self.relation_statistics = None

        # Load table from catalog
        from opteryx_catalog.exceptions import DatasetNotFound

        try:
            self.table = self.catalog.load_dataset(self.dataset)
            self.snapshot = self.table.snapshot()
            self.snapshot_id = None if self.snapshot is None else self.snapshot.snapshot_id
        except DatasetNotFound as exc:
            raise DatasetNotFoundError(dataset=self.dataset, connector=self.__type__) from exc

    def get_dataset_metadata(self) -> Tuple[RelationSchema, Manifest]:
        """
        Get dataset schema and build manifest from catalog.

        Returns both schema and manifest to make the dual purpose explicit.
        Manifest contains file-level statistics from table.scan().

        Returns:
            Tuple of (RelationSchema, Manifest)
        """
        if self.at_date is not None:
            snapshots = self.table.inspect.snapshots().sort_by("committed_at")
            snapshot_rows = snapshots.to_pylist()

            if not snapshot_rows:
                raise DatasetReadError("No data available for the specified date.")

            # Honor dates before the first snapshot by rejecting them, but treat
            # dates after the latest snapshot as selecting the latest snapshot
            first_committed = snapshot_rows[0]["committed_at"]
            last_committed = snapshot_rows[-1]["committed_at"]

            if self.at_date < first_committed:
                # Point-in-time read is before our first snapshot — no data available then
                raise DatasetReadError("No data available for the specified date.")
            elif self.at_date > last_committed:
                # Point-in-time read after the latest snapshot — return current data
                selected = snapshot_rows[-1]
                # ensure we store the commit time for telemetry/context
                self.telemetry.dataset_committed_at = selected["committed_at"].isoformat()
                self.dataset_committed_at = self.telemetry.dataset_committed_at
            else:
                selected = snapshot_rows[0]
                for candidate in snapshot_rows:
                    if candidate["committed_at"] <= self.at_date:
                        self.telemetry.dataset_committed_at = candidate["committed_at"].isoformat()
                        self.dataset_committed_at = self.telemetry.dataset_committed_at
                        selected = candidate
                    else:
                        break

            self.snapshot_id = selected["snapshot_id"]
            self.snapshot = self.table.snapshot_by_id(self.snapshot_id)

        # If the table has no snapshot and the read is not time-travel, use
        # the table's declared schema (from metadata) and return an empty result set.
        if self.snapshot is None:
            self.schema = self.table.schema()
        else:
            self.schema = self.table.schema(self.snapshot.schema_id)
            try:
                self.telemetry.dataset_committed_at = datetime.datetime.fromtimestamp(
                    self.snapshot.timestamp_ms / 1000.0
                ).isoformat()
            except (ValueError, OSError, OverflowError):
                pass

        # Build Manifest from catalog table.scan()
        # scan() returns an iterable of DataFile objects
        try:
            # Get file list from catalog via table.scan()
            scan = self.table.scan(snapshot_id=self.snapshot_id)

            # Build FileEntry for each file
            file_entries = []
            protocols = set()

            for data_file in scan:
                file_entry = FileEntry.from_datafile(data_file)
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

            # Create Manifest with files and schema
            self.manifest = Manifest(files=file_entries, schema=self.schema)

        except Exception as e:
            # Fallback: create empty Manifest if scan fails
            self.manifest = Manifest(files=[], schema=self.schema)

        # For backward compatibility: populate relation_statistics
        # Eventually this can be removed
        relation_statistics = RelationStatistics()
        relation_statistics.record_count = self.manifest.get_record_count()
        self.relation_statistics = relation_statistics

        return self.schema, self.manifest

    def map_statistics(
        self, statistics: RelationStatistics, schema: RelationSchema
    ) -> RelationSchema:
        """
        Map statistics to schema columns.

        For backward compatibility with the binder.
        This will be removed once all connectors use manifests.
        """
        if statistics is None:
            return schema

        schema.row_count_metric = statistics.record_count

        # Map bounds from statistics to schema columns
        for column in schema.columns:
            column_key = column.name.encode() if isinstance(column.name, str) else column.name
            if hasattr(statistics, "upper_bounds"):
                column.highest_value = statistics.upper_bounds.get(column_key, None)
            if hasattr(statistics, "lower_bounds"):
                column.lowest_value = statistics.lower_bounds.get(column_key, None)
            if hasattr(statistics, "null_count"):
                column.null_count = statistics.null_count.get(column_key, None)

        return schema


class OpteryxConnector(Eidetic):
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

    def __init__(self, *args, catalog=None, telemetry=None, **kwargs):
        """
        Initialize the Opteryx catalog connector.

        Args:
            catalog: Optional pre-configured catalog instance or catalog factory function
            **kwargs: Configuration (firestore_project, firestore_database, gcs_bucket, etc.)
        """
        Eidetic.__init__(self, **kwargs)
        self.telemetry = telemetry
        self.kwargs = kwargs
        self.kwargs.pop("connector", None)
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
        if not hasattr(self, "_catalog_cache"):
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

        # Cache and return instance
        # Diagnostic logging: report workspace and whether tests_temp exists
        try:
            dbg_workspace = getattr(instance, "workspace", None)
            print(
                f"DEBUG [_get_catalog]: catalog_name={catalog_name} instance_type={type(instance)} workspace={dbg_workspace}"
            )
            try:
                datasets = instance.list_datasets("tests_temp")
                print(
                    f"DEBUG [_get_catalog]: tests_temp datasets_count={len(datasets)} sample={datasets[:5]}"
                )
            except Exception as e:
                print(f"DEBUG [_get_catalog]: list_datasets error: {e}")

            # Additional diagnostics: Firestore client and GCS bucket
            try:
                fc = getattr(instance, "firestore_client", None)
                fc_project = getattr(fc, "project", None) or getattr(fc, "_client_info", None)
                fc_db = getattr(fc, "_database", None) or getattr(fc, "database", None)
                print(
                    f"DEBUG [_get_catalog]: firestore_client={fc} project={fc_project} database={fc_db}"
                )
            except Exception as e:
                print(f"DEBUG [_get_catalog]: firestore_client inspect error: {e}")
            try:
                print(f"DEBUG [_get_catalog]: gcs_bucket={getattr(instance, 'gcs_bucket', None)}")
            except Exception as e:
                print(f"DEBUG [_get_catalog]: gcs_bucket inspect error: {e}")
        except Exception:
            # Never fail on diagnostics
            pass

        self._catalog_cache[catalog_name] = instance
        return instance

    def _parse_identifier(self, name: str) -> Tuple[str, str]:
        """
        Parse a fully qualified name into catalog and relative identifier.

        For 'benchmarks.clickbench.hits':
        - catalog_name = 'benchmarks'
        - relative_id = 'clickbench.hits'

        Args:
            name: Fully qualified table/view name

        Returns:
            Tuple of (catalog_name, relative_identifier)
        """
        parts = name.split(".", 1)
        if len(parts) == 2:
            return parts[0], parts[1]
        else:
            # No catalog specified, use default
            return "default", name

    def locate_object(self, name: str) -> Tuple[Optional[TableType], any]:
        """
        Ask the connector if it knows about a specific object (table or view).

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

        # Check if it is a dataset
        try:
            dataset = catalog.load_dataset(relative_id)
            return TableType.Table, dataset
        except Exception:
            pass

        # Check if it is a view
        try:
            view = catalog.load_view(relative_id)
            return TableType.View, view
        except Exception:
            pass

        return None, None

    def table_engine(self, name: str, **kwargs):
        """
        Create a table-specific engine for reading data.

        Args:
            name: The fully qualified table name (catalog.namespace.name)
            **kwargs: Additional parameters (start_date, end_date, telemetry, etc.)

        Returns:
            OpteryxTable instance configured for the specific table
        """
        # Parse catalog name and relative identifier
        workspace, relative_id = self._parse_identifier(name)
        catalog = self._get_catalog(workspace)

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

    def drop_view(self, view_name: str):
        """Drop the specified view."""
        # Parse view_name into workspace and relative identifier
        workspace, relative_id = self._parse_identifier(view_name)
        catalog = self._get_catalog(workspace)

        # Split relative identifier into collection and name for catalog
        parts = relative_id.split(".")
        name = parts[-1]
        collection = ".".join(parts[:-1])

        identifier = (collection, name)
        catalog.drop_view(identifier)

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
