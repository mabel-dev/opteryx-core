# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Local Filesystem PyIceberg Catalog Shim

Provides a PyIceberg-compatible catalog interface over local filesystem storage.
Tables are directories containing parquet/data files. Schema is inferred on-the-fly.
"""

import os
from typing import List
from typing import Optional

import pyarrow as pa
import pyarrow.parquet as pq
from pyiceberg.catalog import Catalog
from pyiceberg.catalog import Identifier
from pyiceberg.exceptions import NoSuchNamespaceError
from pyiceberg.exceptions import NoSuchTableError
from pyiceberg.schema import Schema
from pyiceberg.table import Table
from pyiceberg.types import NestedField

from opteryx.connectors.io_systems import OpteryxLocalFileSystem


class LocalFileCatalog(Catalog):
    """
    Read-only PyIceberg catalog shim for local filesystem.

    Structure:
    - Root directory contains namespaces (subdirectories)
    - Each namespace contains tables (subdirectories or files)
    - Tables are either:
      * A directory containing parquet files
      * A single parquet/csv/jsonl file
    """

    def __init__(self, name: str, root_path: str = ".", **properties):
        """
        Initialize the local file catalog.

        Args:
            name: Catalog name
            root_path: Root directory path
            **properties: Additional properties (ignored)
        """
        super().__init__(name, **properties)
        self.root_path = os.path.abspath(root_path)
        self.filesystem = OpteryxLocalFileSystem()

    def _resolve_path(self, identifier: Identifier) -> str:
        """Convert identifier to filesystem path."""
        if isinstance(identifier, str):
            parts = [identifier]
        else:
            parts = list(identifier)
        return os.path.join(self.root_path, *parts)

    def _is_table(self, path: str) -> bool:
        """Check if path represents a valid table."""
        if not os.path.exists(path):
            return False

        # Single file table
        if os.path.isfile(path):
            return path.endswith((".parquet", ".csv", ".jsonl", ".json"))

        # Directory table - must contain data files
        if os.path.isdir(path):
            for item in os.listdir(path):
                if item.endswith((".parquet", ".csv", ".jsonl", ".json")):
                    return True
        return False

    def _infer_schema(self, path: str) -> Schema:
        """Infer PyIceberg schema from data files."""
        # Find a parquet file to read schema from
        parquet_file = None

        if os.path.isfile(path) and path.endswith(".parquet"):
            parquet_file = path
        elif os.path.isdir(path):
            for item in os.listdir(path):
                if item.endswith(".parquet"):
                    parquet_file = os.path.join(path, item)
                    break

        if not parquet_file:
            raise ValueError(f"No parquet files found at {path}")

        # Read Arrow schema and convert to Iceberg schema
        arrow_schema = pq.read_schema(parquet_file)

        # Convert Arrow schema to Iceberg schema
        fields = []
        for i, field in enumerate(arrow_schema):
            # Simple type mapping - can be expanded
            iceberg_type = self._arrow_to_iceberg_type(field.type)
            fields.append(
                NestedField(
                    field_id=i,
                    name=field.name,
                    field_type=iceberg_type,
                    required=not field.nullable,
                )
            )

        return Schema(*fields)

    def _arrow_to_iceberg_type(self, arrow_type):
        """Convert Arrow type to Iceberg type."""
        from pyiceberg.types import BinaryType
        from pyiceberg.types import BooleanType
        from pyiceberg.types import DateType
        from pyiceberg.types import DoubleType
        from pyiceberg.types import FloatType
        from pyiceberg.types import IntegerType
        from pyiceberg.types import LongType
        from pyiceberg.types import StringType
        from pyiceberg.types import TimestampType
        from pyiceberg.types import TimestamptzType

        if pa.types.is_boolean(arrow_type):
            return BooleanType()
        elif pa.types.is_int32(arrow_type):
            return IntegerType()
        elif pa.types.is_int64(arrow_type):
            return LongType()
        elif pa.types.is_float32(arrow_type):
            return FloatType()
        elif pa.types.is_float64(arrow_type):
            return DoubleType()
        elif pa.types.is_string(arrow_type) or pa.types.is_large_string(arrow_type):
            return StringType()
        elif pa.types.is_binary(arrow_type):
            return BinaryType()
        elif pa.types.is_date(arrow_type):
            return DateType()
        elif pa.types.is_timestamp(arrow_type):
            if arrow_type.tz is not None:
                return TimestamptzType()
            return TimestampType()
        else:
            # Default to string for unknown types
            return StringType()

    def load_table(self, identifier: Identifier) -> Table:
        """Load table metadata (inferred from files)."""
        path = self._resolve_path(identifier)

        if not self._is_table(path):
            raise NoSuchTableError(f"Table not found: {identifier}")

        # For this shim, we return a minimal stub
        # The actual IcebergTable will handle reading
        # This is just enough to satisfy the catalog interface

        # Create a minimal table object with inferred schema
        schema = self._infer_schema(path)

        # Return a pseudo-table that contains the necessary info
        # Note: We're creating a minimal structure, not a full Iceberg table
        from pyiceberg.table.metadata import TableMetadataV2

        # Create minimal metadata
        metadata = TableMetadataV2(
            location=path,
            table_uuid="00000000-0000-0000-0000-000000000000",  # Dummy UUID
            last_updated_ms=0,
            last_column_id=len(schema.fields),
            schemas=[schema],
            current_schema_id=0,
            partition_specs=[],
            default_spec_id=0,
            last_partition_id=0,
            properties={},
            current_snapshot_id=None,
            snapshots=[],
            snapshot_log=[],
            metadata_log=[],
            sort_orders=[],
            default_sort_order_id=0,
        )

        return Table(
            identifier=identifier if isinstance(identifier, tuple) else (identifier,),
            metadata=metadata,
            metadata_location="",
            io=None,
            catalog=self,
        )

    def list_tables(self, namespace: str) -> List[Identifier]:
        """List all tables in a namespace."""
        namespace_path = self._resolve_path(namespace)

        if not os.path.exists(namespace_path):
            return []

        tables = []
        for item in os.listdir(namespace_path):
            item_path = os.path.join(namespace_path, item)
            if self._is_table(item_path):
                tables.append((namespace, item))

        return tables

    def list_namespaces(self, namespace: Optional[str] = None) -> List[Identifier]:
        """List all namespaces (subdirectories)."""
        base_path = self._resolve_path(namespace) if namespace else self.root_path

        if not os.path.exists(base_path):
            return []

        namespaces = []
        for item in os.listdir(base_path):
            item_path = os.path.join(base_path, item)
            if os.path.isdir(item_path):
                if namespace:
                    namespaces.append((namespace, item))
                else:
                    namespaces.append((item,))

        return namespaces

    def create_table(self, identifier: Identifier, schema: Schema, **kwargs) -> Table:
        """Not supported - read-only catalog."""
        raise NotImplementedError("LocalFileCatalog is read-only")

    def create_namespace(self, namespace: str, properties: dict = None) -> None:
        """Not supported - read-only catalog."""
        raise NotImplementedError("LocalFileCatalog is read-only")

    def drop_table(self, identifier: Identifier) -> None:
        """Not supported - read-only catalog."""
        raise NotImplementedError("LocalFileCatalog is read-only")

    def rename_table(self, from_identifier: Identifier, to_identifier: Identifier) -> Table:
        """Not supported - read-only catalog."""
        raise NotImplementedError("LocalFileCatalog is read-only")

    def load_namespace_properties(self, namespace: str) -> dict:
        """Return empty properties."""
        namespace_path = self._resolve_path(namespace)
        if not os.path.exists(namespace_path):
            raise NoSuchNamespaceError(f"Namespace not found: {namespace}")
        return {}
