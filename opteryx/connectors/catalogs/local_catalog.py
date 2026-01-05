# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Local Filesystem Opteryx Catalog Shim

Provides an Opteryx catalog shim that implements the Opteryx Catalog interface over local filesystem storage.
Tables are directories containing parquet/data files. Schema is inferred on-the-fly.
"""

import os
from types import SimpleNamespace
from typing import Any
from typing import List
from typing import Optional
from typing import Tuple

import pyarrow as pa
import pyarrow.parquet as pq

from opteryx.connectors.io_systems import OpteryxLocalFileSystem


class MinimalSchema:
    """Lightweight schema wrapper exposing what OpteryxTable expects."""

    def __init__(self, pa_schema: pa.Schema):
        self._pa = pa_schema
        self.column_names = list(pa_schema.names)
        # columns is a simple structure used by some callers; keep it generic
        self.columns = [
            {"name": n, "arrow_type": t} for n, t in zip(pa_schema.names, pa_schema.types)
        ]


class _Inspector:
    def snapshots(self):
        class _S:
            def sort_by(self, key):
                class _T:
                    def to_pylist(self):
                        return []

                return _T()

        return _S()


class OpteryxShimTable:
    """Minimal table-like object returned by the Opteryx catalog shim."""

    def __init__(self, identifier: Any, schema: MinimalSchema, metadata: Any):
        self.identifier = identifier if isinstance(identifier, tuple) else (identifier,)
        self._schema = schema
        self.metadata = metadata
        self.io = None

    def schema(self, snapshot_id=None):
        return self._schema

    def snapshot(self):
        return None

    def snapshot_by_id(self, _id):
        return None

    def inspect(self):
        return _Inspector()


class LocalOpteryxCatalog:
    """A minimal Opteryx Catalog shim backed by the local filesystem.

    This implements the subset of the Opteryx Catalog interface required by
    `OpteryxTable` (notably `load_dataset`, `list_tables`, `list_namespaces`) and
    remains PyIceberg-compatible where possible. It avoids hard dependency on
    `pyiceberg` for the primary Opteryx use-case.
    """

    """
    Read-only Opteryx catalog shim for local filesystem (PyIceberg-compatible).

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
        self.name = name
        self.root_path = os.path.abspath(root_path)
        self.filesystem = OpteryxLocalFileSystem()

    def _resolve_path(self, identifier: Any) -> str:
        """Convert identifier to filesystem path."""
        parts = [identifier] if isinstance(identifier, str) else list(identifier)
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

    def _infer_schema(self, path: str) -> "MinimalSchema":
        """Infer schema for Opteryx catalog from data files.

        Returns a lightweight MinimalSchema that wraps a PyArrow schema and
        exposes `column_names` and `columns` for OpteryxTable compatibility.
        """
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

        # Read Arrow schema and wrap it for Opteryx
        arrow_schema = pq.read_schema(parquet_file)
        return MinimalSchema(arrow_schema)

    def load_dataset(self, identifier: Any) -> OpteryxShimTable:
        """Opteryx-compatible dataset loader.

        Returns an `OpteryxShimTable` that provides the minimal API expected by
        `OpteryxTable` (schema(), metadata.location, inspect(), snapshot()).
        """
        path = self._resolve_path(identifier)

        if not self._is_table(path):
            raise FileNotFoundError(f"Table not found: {identifier}")

        schema = self._infer_schema(path)
        metadata = SimpleNamespace(location=path)
        return OpteryxShimTable(identifier, schema, metadata)

    def list_tables(self, namespace: str) -> List[Tuple[str, str]]:
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

    def list_namespaces(self, namespace: Optional[str] = None) -> List[Tuple[str, ...]]:
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

    def create_table(self, identifier: Any, schema: Any, **kwargs) -> Any:
        """Not supported - read-only catalog."""
        raise NotImplementedError("LocalOpteryxCatalog is read-only")

    def create_namespace(self, namespace: str, properties: dict = None) -> None:
        """Not supported - read-only catalog."""
        raise NotImplementedError("LocalOpteryxCatalog is read-only")

    def drop_table(self, identifier: Any) -> None:
        """Not supported - read-only catalog."""
        raise NotImplementedError("LocalOpteryxCatalog is read-only")

    def rename_table(self, from_identifier: Any, to_identifier: Any) -> Any:
        """Not supported - read-only catalog."""
        raise NotImplementedError("LocalOpteryxCatalog is read-only")

    def load_namespace_properties(self, namespace: str) -> dict:
        """Return empty properties."""
        namespace_path = self._resolve_path(namespace)
        if not os.path.exists(namespace_path):
            raise FileNotFoundError(f"Namespace not found: {namespace}")
        return {}
