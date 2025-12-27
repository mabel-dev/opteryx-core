# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Iceberg Connector - Refactored Architecture

Architecture:
- IcebergConnector: Long-lived catalog gateway (handles catalog operations, views, introspection)
- IcebergTable: Transient table-specific engine (handles data reading for one table)
"""

import datetime
import struct
from decimal import Decimal
from typing import Dict
from typing import List
from typing import Optional
from typing import Tuple
from typing import Union

import numpy
import pyarrow
from orso.schema import FlatColumn
from orso.schema import RelationSchema
from orso.tools import single_item_cache
from orso.types import OrsoTypes

from opteryx.connectors import TableType
from opteryx.connectors.capabilities import Diachronic
from opteryx.connectors.capabilities import Eidetic
from opteryx.connectors.capabilities import Statistics
from opteryx.connectors.filesystem_connector import FileSystemTable
from opteryx.exceptions import DatasetNotFoundError
from opteryx.exceptions import DatasetReadError
from opteryx.exceptions import NotSupportedError
from opteryx.exceptions import UnsupportedSyntaxError
from opteryx.managers.expression import NodeType
from opteryx.models import RelationStatistics


@single_item_cache
def to_iceberg_filter(root):
    """
    Convert a filter to Iceberg filter form.

    This is specifically opinionated for the Iceberg reader.
    """
    import pyiceberg
    import pyiceberg.expressions

    ICEBERG_FILTERS = {
        "GtEq": pyiceberg.expressions.GreaterThanOrEqual,
        "Eq": pyiceberg.expressions.EqualTo,
        "Gt": pyiceberg.expressions.GreaterThan,
        "Lt": pyiceberg.expressions.LessThan,
        "LtEq": pyiceberg.expressions.LessThanOrEqual,
        "NotEq": pyiceberg.expressions.NotEqualTo,
    }

    def _predicate_to_iceberg_filter(root):
        # Reduce look-ahead effort by using Exceptions to control flow
        if root.node_type == NodeType.AND:  # pragma: no cover
            left = _predicate_to_iceberg_filter(root.left)
            right = _predicate_to_iceberg_filter(root.right)
            if not isinstance(left, list):
                left = [left]
            if not isinstance(right, list):
                right = [right]
            left.extend(right)
            return left
        if root.node_type != NodeType.COMPARISON_OPERATOR:
            raise NotSupportedError()

        left_node = root.left
        right_node = root.right

        if left_node.node_type != NodeType.IDENTIFIER:
            left_node, right_node = right_node, left_node

        right_value = right_node.value
        right_type = right_node.schema_column.type
        left_type = left_node.schema_column.type

        if right_type == OrsoTypes.DATE:
            date_val = right_value
            if hasattr(date_val, "item"):
                date_val = date_val.item()
            right_value = datetime.datetime.combine(date_val, datetime.time.min)
            right_type = OrsoTypes.TIMESTAMP
        if left_type == OrsoTypes.DATE:
            left_type = OrsoTypes.TIMESTAMP
        if left_node.node_type != NodeType.IDENTIFIER:
            raise NotSupportedError()
        if right_node.node_type != NodeType.LITERAL:
            raise NotSupportedError()
        if left_type == OrsoTypes.VARCHAR:
            left_type = OrsoTypes.BLOB
        if right_type == OrsoTypes.VARCHAR:
            right_type = OrsoTypes.BLOB
        if right_type != left_type:
            raise NotSupportedError(f"{right_type} != {left_type}")
        if right_type == OrsoTypes.DOUBLE:
            # iceberg needs doubles to be cast to floats
            right_value = float(right_value)
        if right_type == OrsoTypes.INTEGER:
            # iceberg doesn't like integers unless we convert to strings
            right_value = str(right_value)
        if right_type == OrsoTypes.TIMESTAMP and isinstance(right_value, numpy.datetime64):
            # iceberg doesn't like timestamps unless we convert to strings
            right_value = right_value.astype(datetime.datetime)
        return ICEBERG_FILTERS[root.value](left_node.value, right_value)

    iceberg_filter = None
    unsupported = []
    if not isinstance(root, list):
        root = [root]
    for predicate in root:
        try:
            converted = _predicate_to_iceberg_filter(predicate)
            if iceberg_filter is None:
                iceberg_filter = converted
            else:
                iceberg_filter = pyiceberg.expressions.And(iceberg_filter, converted)
        except NotSupportedError:
            unsupported.append(predicate)

    return iceberg_filter if iceberg_filter else "True", unsupported


class IcebergTable(FileSystemTable, Diachronic, Statistics):
    """
    Table-specific engine for reading Iceberg tables.

    This is a transient object created per-table that handles:
    - Schema resolution
    - Statistics gathering
    - Data reading
    - Predicate pushdown
    - Time-travel queries

    Inherits from FileSystemTable to reuse filesystem-based reading logic.
    """

    __mode__ = "Blob"
    __type__ = "ICEBERG"
    __synchronousity__ = "asynchronous"

    # Capability declarations
    supports_diachronic = True  # Time-travel queries
    supports_predicate_pushdown = True  # Via FileSystemTable
    supports_limit_pushdown = True  # Via FileSystemTable
    supports_statistics = True  # Iceberg manifest stats

    PUSHABLE_OPS: Dict[str, bool] = {
        "Eq": True,
        "NotEq": True,  # nulls not handled correctly
        "Gt": True,
        "GtEq": True,
        "Lt": True,
        "LtEq": True,
    }

    PUSHABLE_TYPES = {
        OrsoTypes.BLOB,
        OrsoTypes.BOOLEAN,
        OrsoTypes.DOUBLE,
        OrsoTypes.INTEGER,
        OrsoTypes.VARCHAR,
        OrsoTypes.TIMESTAMP,
        OrsoTypes.DATE,
    }

    def __init__(self, dataset: str, catalog, catalog_name: str, **kwargs):
        """
        Initialize the table engine for a specific Iceberg table.

        Args:
            dataset: The table name (after catalog prefix is removed)
            catalog: The pyiceberg Catalog instance
            catalog_name: The catalog name
            **kwargs: Additional parameters (telemetry, start_date, end_date, etc.)
        """
        self.dataset = dataset
        self.catalog = catalog
        self.catalog_name = catalog_name

        # Iceberg currently always uses GCS for storage
        # Create the appropriate filesystem for reading data files
        from opteryx.connectors.io_systems import OpteryxGcsFileSystem

        filesystem = OpteryxGcsFileSystem()

        # Call FileSystemTable.__init__ which calls BaseTable.__init__
        FileSystemTable.__init__(
            self, dataset=dataset, filesystem=filesystem, storage_type="ICEBERG", **kwargs
        )
        Diachronic.__init__(self, **kwargs)
        Statistics.__init__(self, **kwargs)

        self.dataset = self.dataset.replace("/", ".")

        # Initialize state
        self.snapshot_id = None
        self.snapshot = None
        self.dataset_committed_at = None

        import pyiceberg

        try:
            self.table = self.catalog.load_table(self.dataset)
            self.snapshot = self.table.current_snapshot()
            self.snapshot_id = None if self.snapshot is None else self.snapshot.snapshot_id
        except pyiceberg.exceptions.NoSuchTableError:
            raise DatasetNotFoundError(dataset=self.dataset, connector=self.__type__)

    def get_dataset_schema(self) -> RelationSchema:
        if self.start_date != self.end_date:
            if self.start_date.date() != self.end_date.date():
                raise UnsupportedSyntaxError("This table only supports point in time reads.")
            raise UnsupportedSyntaxError(
                "This table only supports point in time reads. Are you missing the time component from your FOR clause?"
            )

        if self.start_date is not None:
            snapshots = self.table.inspect.snapshots().sort_by("committed_at")
            snapshot_rows = snapshots.to_pylist()

            if not snapshot_rows:
                raise DatasetReadError("No data available for the specified date.")

            # Honor dates before the first snapshot by rejecting them, but treat
            # dates after the latest snapshot as selecting the latest snapshot
            first_committed = snapshot_rows[0]["committed_at"]
            last_committed = snapshot_rows[-1]["committed_at"]

            if self.start_date < first_committed:
                # Point-in-time read is before our first snapshot — no data available then
                raise DatasetReadError("No data available for the specified date.")
            elif self.start_date > last_committed:
                # Point-in-time read after the latest snapshot — return current data
                selected = snapshot_rows[-1]
                # ensure we store the commit time for telemetry/context
                self.telemetry.dataset_committed_at = selected["committed_at"].isoformat()
                self.dataset_committed_at = self.telemetry.dataset_committed_at
            else:
                selected = snapshot_rows[0]
                for candidate in snapshot_rows:
                    if candidate["committed_at"] <= self.start_date:
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
            iceberg_schema = self.table.schema()
        else:
            iceberg_schema = self.table.schemas()[self.snapshot.schema_id]
            try:
                self.telemetry.dataset_committed_at = datetime.datetime.fromtimestamp(
                    self.snapshot.timestamp_ms / 1000.0
                ).isoformat()
            except (ValueError, OSError, OverflowError):
                pass
        arrow_schema = iceberg_schema.as_arrow()

        self.schema = RelationSchema(
            name=self.dataset,
            columns=[FlatColumn.from_arrow(field) for field in arrow_schema],
        )

        # Get statistics
        relation_statistics = RelationStatistics()

        column_names = {col.field_id: col.name for col in iceberg_schema.columns}

        files = self.table.inspect.files(snapshot_id=self.snapshot_id)

        # No files = empty table, no stats
        if len(files.column("file_path")) == 0:
            self.relation_statistics = relation_statistics
            return self.schema

        relation_statistics.record_count = pyarrow.compute.sum(files.column("record_count")).as_py()

        if "distinct_counts" in files.columns:
            for file in files.column("distinct_counts"):
                for k, v in file:
                    relation_statistics.set_cardinality_estimate(column_names[k], v)

        if "value_counts" in files.columns:
            for file in files.column("value_counts"):
                for k, v in file:
                    relation_statistics.add_count(column_names[k], v)

        self.relation_statistics = relation_statistics

        return self.schema

    def get_list_of_blob_names(self, *, prefix: str = None, predicates: list = []) -> List[str]:
        pushed_filters, _ = to_iceberg_filter(predicates)

        # Get the list of data files to read
        data_files = self.table.scan(
            row_filter=pushed_filters,  # Iceberg expression
            snapshot_id=self.snapshot_id,
        ).plan_files()

        return [data_file.file.file_path for data_file in data_files]

    @staticmethod
    def decode_iceberg_value(
        value: Union[int, float, bytes], data_type: str, scale: int = None
    ) -> Union[int, float, str, datetime.datetime, Decimal, bool]:
        """
        Decode Iceberg-encoded values based on the specified data type.
        """
        import pyiceberg

        data_type_class = data_type.__class__

        if data_type_class == pyiceberg.types.LongType:
            return int.from_bytes(value, "big", signed=True)
        elif data_type_class == pyiceberg.types.DoubleType:
            # IEEE 754 encoded floats are typically decoded directly
            return struct.unpack(">d", value)[0]  # 8-byte IEEE 754 double
        elif data_type_class in (pyiceberg.types.TimestampType, pyiceberg.types.TimestamptzType):
            # Iceberg stores timestamps as microseconds since epoch
            interval = int.from_bytes(value, "big", signed=True)
            if interval < 0:
                # Windows specifically doesn't like negative timestamps
                return datetime.datetime(1970, 1, 1) + datetime.timedelta(microseconds=interval)
            return datetime.datetime.fromtimestamp(interval / 1_000_000)
        elif data_type == "date":
            # Iceberg stores dates as days since epoch (1970-01-01)
            interval = int.from_bytes(value, "big", signed=True)
            return datetime.datetime(1970, 1, 1) + datetime.timedelta(days=interval)
        elif data_type_class == pyiceberg.types.StringType:
            # Assuming UTF-8 encoded bytes (or already decoded string)
            return value.decode("utf-8") if isinstance(value, bytes) else str(value)
        elif data_type_class == pyiceberg.types.BinaryType:
            return value
        elif str(data_type).startswith("decimal"):
            # Iceberg stores decimals as unscaled integers
            int_value = int.from_bytes(value, byteorder="big", signed=True)
            return Decimal(int_value) / (10**data_type.scale)
        elif data_type_class == pyiceberg.types.BooleanType:
            return bool(value)

        ValueError(f"Unsupported data type: {data_type}, {str(data_type)}")


class IcebergConnector(Eidetic):
    """
    Long-lived Iceberg catalog gateway supporting multiple catalogs.

    This connector handles:
    - Multi-catalog management (lazy instantiation)
    - Object introspection (locate_object)
    - View operations (create/drop/list views)
    - Factory method for creating table engines
    """

    eidetic = True

    # Capability declarations - what IcebergTable readers support
    supports_diachronic = True  # Time-travel via IcebergTable
    supports_predicate_pushdown = True  # Via FileSystemTable base
    supports_limit_pushdown = True  # Via FileSystemTable base
    supports_statistics = True  # Iceberg manifests provide stats

    def __init__(self, *args, catalog=None, **kwargs):
        """
        Initialize the Iceberg catalog connector.

        Args:
            catalog: Optional pre-configured catalog instance or catalog factory function
            **kwargs: Configuration (firestore_project, firestore_database, gcs_bucket, etc.)
        """
        Eidetic.__init__(self, **kwargs)
        self.kwargs = kwargs
        self.catalog_factory = catalog
        self.catalogs = {}  # Cache of instantiated catalogs by name

        import pyiceberg

        # If a pre-configured catalog instance was provided, cache it
        if isinstance(catalog, pyiceberg.catalog.Catalog):
            self.catalogs[catalog.name] = catalog

    def _get_catalog(self, catalog_name: str):
        """
        Get or create a catalog instance for the specified catalog name.

        Args:
            catalog_name: The catalog name to connect to

        Returns:
            PyIceberg Catalog instance
        """
        import pyiceberg

        if catalog_name in self.catalogs:
            return self.catalogs[catalog_name]

        # Create new catalog instance
        if self.catalog_factory is None:
            raise ValueError("Iceberg connector requires a catalog parameter")

        if isinstance(self.catalog_factory, pyiceberg.catalog.Catalog):
            # Already have an instance, just return it
            return self.catalog_factory

        # Call factory to create catalog
        catalog_instance = self.catalog_factory(
            catalog_name=catalog_name,
            firestore_project=self.kwargs.get("firestore_project"),
            firestore_database=self.kwargs.get("firestore_database"),
            gcs_bucket=self.kwargs.get("gcs_bucket"),
        )

        self.catalogs[catalog_name] = catalog_instance
        return catalog_instance

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
        import pyiceberg.exceptions

        # Parse catalog name and relative identifier
        catalog_name, relative_id = self._parse_identifier(name)
        catalog = self._get_catalog(catalog_name)

        # Check if it is a table
        try:
            table = catalog.load_table(relative_id)
            return TableType.Table, table
        except pyiceberg.exceptions.NoSuchTableError:
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
            IcebergTable instance configured for the specific table
        """
        # Parse catalog name and relative identifier
        catalog_name, relative_id = self._parse_identifier(name)
        catalog = self._get_catalog(catalog_name)

        # Merge stored kwargs with provided kwargs (provided takes precedence)
        merged_kwargs = {**self.kwargs, **kwargs}
        return IcebergTable(
            dataset=relative_id, catalog=catalog, catalog_name=catalog_name, **merged_kwargs
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
        catalog_name, relative_id = self._parse_identifier(view_name)
        catalog = self._get_catalog(catalog_name)

        # Parse relative_id into namespace and name
        # For "clickbench.q01": namespace="clickbench", name="q01"
        parts = relative_id.split(".")
        if len(parts) >= 2:
            name = parts[-1]
            namespace = ".".join(parts[:-1])
        else:
            namespace = catalog_name
            name = relative_id

        identifier = (namespace, name)
        view = catalog.load_view(identifier)

        return ViewDefinition(
            name=view.name,
            statement=view.metadata.sql_text,
            owner=view.metadata.author,
            last_row_count=view.metadata.last_row_count,
        )

    def list_views(self, prefix: str = None) -> list:
        """List all available views in the specified catalog and schema."""
        from opteryx.connectors.capabilities.eidetic import ViewDefinition

        # Determine namespace to list from
        if prefix:
            namespace = prefix
        else:
            namespace = self.catalog_name

        # Get view identifiers from catalog
        view_identifiers = self.catalog.list_views(namespace)

        # Load each view and convert to ViewDefinition
        views = []
        for identifier in view_identifiers:
            try:
                view = self.catalog.load_view(identifier)
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

    def create_view(self, view_name: str, statement: str, owner: str = None):
        """Create a new view with the given name and definition."""
        # Parse view_name - it might include namespace
        if "." in view_name:
            namespace, name = view_name.rsplit(".", 1)
        else:
            namespace = self.catalog_name
            name = view_name

        identifier = (namespace, name)
        self.catalog.create_view(identifier=identifier, sql=statement, author=owner)

    def drop_view(self, view_name: str):
        """Drop the specified view."""
        # Parse view_name - it might include namespace
        if "." in view_name:
            namespace, name = view_name.rsplit(".", 1)
        else:
            namespace = self.catalog_name
            name = view_name

        identifier = (namespace, name)
        self.catalog.drop_view(identifier)
