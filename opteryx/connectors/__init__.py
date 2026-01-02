# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Data Source Connectors with Lazy Loading

This module provides connectors to various data sources, enabling Opteryx to query
data from files, databases, cloud storage, and other systems. Connectors are lazily
loaded to improve startup performance and reduce memory footprint.

Architecture:
Connectors abstract different data sources behind a common interface (BaseConnector),
allowing the query engine to work with any data source transparently. Each connector
is responsible for:
- Reading data and converting it to PyArrow format
- Providing schema information
- Supporting predicate pushdown when possible
- Handling authentication and connection management

Connector Types:

Core Connectors:
- FileSystemConnector: Generic filesystem access (local, S3, GCS)
- OpteryxConnector: Opteryx table format

Special Connectors:
- VirtualDataConnector: In-memory datasets and computed tables
- InformationSchemaConnector: System metadata tables

Legacy Compatibility:
The following names are supported for backward compatibility and map to FileSystemConnector:
- DiskConnector: Local file system access
- AwsS3Connector: Amazon S3 storage
- GcpCloudStorageConnector: Google Cloud Storage

Lazy Loading:
Connectors are only imported when actually needed, which significantly improves
module import time. The lazy loading is transparent to users - all import patterns
work normally, but the actual connector classes are loaded on first access.

Usage Patterns:

1. Direct Import:
   from opteryx.connectors import ArrowConnector

2. Registration:
   opteryx.register_workspace("my_prefix", my_connector_instance)

3. Query Usage:
   opteryx.query("SELECT * FROM s3://bucket/file.parquet")

Connector Development:
1. Inherit from BaseConnector
2. Implement required methods (read_dataset, get_dataset_schema)
3. Add optional optimizations (predicate pushdown, column pruning)
4. Register with appropriate prefixes
5. Add comprehensive tests

Example Custom Connector:
    class MyConnector(BaseConnector):
        def read_dataset(self, dataset, **kwargs):
            # Read data and return PyArrow table
            return pa.table(data)

        def get_dataset_schema(self, dataset):
            # Return schema information
            return pa.schema([...])

Performance Considerations:
- Implement predicate pushdown to reduce data transfer
- Support column pruning for wide tables
- Use async operations for I/O bound connectors
- Cache schema information when appropriate
- Consider connection pooling for database connectors

The lazy loading system maps prefixes to connector classes and loads them
on demand, significantly reducing initial import time while maintaining
full functionality.
"""


# Lazy imports - connectors are only loaded when actually needed
# This significantly improves module import time from ~500ms to ~130ms

from enum import Enum

# load the base set of prefixes
# fmt:off


class TableType(str, Enum):
    """Enum representing the type of object in a data catalog"""
    Table = "Table"
    View = "View"



_storage_prefixes = {
    "information_schema": "InformationSchema",
}

# Cache for connector instances (keyed by prefix)
_connector_cache = {}

# Default connector configuration (separate from prefix registry)
_default_connector = None
# fmt:on


__all__ = (
    # Core connectors
    "OpteryxConnector",
    "OpteryxTable",
    "FileSystemConnector",
    # Factory functions for filesystem connectors
    "create_local_connector",
    "create_gcs_connector",
    "create_s3_connector",
    # Utilities
    "set_default_connector",
    "TableType",
    # Legacy names (backward compatibility) - map to factories
    "AwsS3Connector",
    "DiskConnector",
    "GcpCloudStorageConnector",
)


def register_workspace(prefix, connector, **kwargs):
    """Register a connector for a specific prefix."""
    # Accept both uninstantiated classes and factory functions
    if not (isinstance(connector, type) or callable(connector)):
        raise ValueError(
            "connectors registered with `register_workspace` must be uninstantiated (a class or factory function)."
        )

    # Store connector class/factory directly (not as a string)
    _storage_prefixes[prefix] = {
        "connector": connector,  # type: ignore
        "prefix": prefix,
        **kwargs,
    }


def set_default_connector(connector, **kwargs):
    """
    Set the default connector to use when no prefix matches.

    Args:
        connector: Connector class to use as default
        **kwargs: Configuration parameters for the connector

    Example:
        set_default_connector(OpteryxConnector,
                            catalog=FirestoreCatalog,
                            firestore_project="my-project",
                            ...)
    """
    global _default_connector

    if not isinstance(connector, type):
        raise ValueError("Default connector must be an uninstantiated class.")

    _default_connector = {
        "connector": connector,
        **kwargs,
    }


def create_local_connector(**kwargs):
    """
    Create a FileSystemConnector for local storage.

    Args:
        **kwargs: Additional parameters passed to FileSystemConnector

    Returns:
        FileSystemConnector configured for local storage
    """
    from opteryx.connectors.filesystem_connector import FileSystemConnector
    from opteryx.connectors.io_systems import OpteryxLocalFileSystem

    filesystem = OpteryxLocalFileSystem()
    return FileSystemConnector(filesystem=filesystem, storage_type="LOCAL", **kwargs)


def create_gcs_connector(bucket=None, **kwargs):
    """
    Create a FileSystemConnector for Google Cloud Storage.

    Args:
        bucket: GCS bucket name (optional)
        **kwargs: Additional parameters passed to FileSystemConnector

    Returns:
        FileSystemConnector configured for GCS
    """
    from opteryx.connectors.filesystem_connector import FileSystemConnector
    from opteryx.connectors.io_systems import OpteryxGcsFileSystem

    filesystem = OpteryxGcsFileSystem(bucket=bucket, **kwargs)
    return FileSystemConnector(filesystem=filesystem, storage_type="GCS", **kwargs)


def create_s3_connector(bucket=None, region=None, **kwargs):
    """
    Create a FileSystemConnector for S3/MinIO storage.

    Args:
        bucket: S3 bucket name (optional)
        region: AWS region (optional)
        **kwargs: Additional parameters passed to FileSystemConnector

    Returns:
        FileSystemConnector configured for S3
    """
    from opteryx.connectors.filesystem_connector import FileSystemConnector
    from opteryx.connectors.io_systems import OpteryxS3FileSystem

    filesystem = OpteryxS3FileSystem(bucket=bucket, region=region, **kwargs)
    return FileSystemConnector(filesystem=filesystem, storage_type="S3", **kwargs)


def known_prefix(prefix) -> bool:
    return prefix in _storage_prefixes


def connector_factory(dataset, telemetry, **config):
    """
    Get or create a connector instance for the given dataset's prefix.

    Connectors are now long-lived and cached by prefix/catalog, not by specific dataset.
    The connector acts as a gateway to the catalog and can be queried about specific tables/views.

    Args:
        dataset: The dataset reference (e.g., "catalog.schema.table")
        telemetry: Query telemetry object
        **config: Additional configuration

    Returns:
        A cached connector instance for the prefix
    """

    # if it starts with a $, it's a special internal dataset
    if dataset[0] == "$":
        from opteryx.connectors.virtual_data_connector import VirtualDataConnector

        # Virtual data connector is a gateway - it doesn't need dataset/telemetry
        # Those are passed when creating the table reader via table_engine()
        return VirtualDataConnector()

    # Look up the prefix from the registered prefixes
    connector_entry: dict = config.copy()
    connector = None
    matched_prefix = None

    for prefix, storage_details in _storage_prefixes.items():
        if dataset == prefix or dataset.startswith(prefix + "."):
            if isinstance(storage_details, dict):
                connector_entry.update(storage_details.copy())
                connector = connector_entry.get("connector")
                matched_prefix = prefix
            else:
                # storage_details is a string (connector class name)
                connector = storage_details
                matched_prefix = prefix
                connector_entry["prefix"] = prefix
            break

    if connector is None:
        # Fall back to the default connector
        if _default_connector is not None:
            connector_entry = _default_connector.copy()
            connector = connector_entry.get("connector")
            matched_prefix = None  # No prefix matched, using default
        else:
            # No default set, use local disk with FileSystemConnector
            from opteryx.connectors.filesystem_connector import FileSystemConnector
            from opteryx.connectors.io_systems import OpteryxLocalFileSystem

            filesystem = OpteryxLocalFileSystem()
            connector_instance = FileSystemConnector(
                filesystem=filesystem, storage_type="LOCAL", telemetry=telemetry, **connector_entry
            )
            connector_instance._matched_prefix = None
            _connector_cache[(None, ())] = connector_instance
            return connector_instance

    # Generate a cache key based on prefix and relevant config
    cache_key = (
        matched_prefix or "_default",
        tuple(
            sorted((k, v) for k, v in connector_entry.items() if k not in ("prefix", "connector"))
        ),
    )

    # Check if we have a cached connector instance
    if cache_key in _connector_cache:
        return _connector_cache[cache_key]

    # Handle string-based connector names - map to appropriate factories
    if isinstance(connector, str):
        if connector == "DiskConnector":
            from opteryx.connectors.filesystem_connector import FileSystemConnector
            from opteryx.connectors.io_systems import OpteryxLocalFileSystem

            filesystem = OpteryxLocalFileSystem()
            connector_instance = FileSystemConnector(
                filesystem=filesystem, storage_type="LOCAL", telemetry=telemetry, **connector_entry
            )
        elif connector == "GcpCloudStorageConnector":
            from opteryx.connectors.filesystem_connector import FileSystemConnector
            from opteryx.connectors.io_systems import OpteryxGcsFileSystem

            filesystem = OpteryxGcsFileSystem(**connector_entry)
            connector_instance = FileSystemConnector(
                filesystem=filesystem, storage_type="GCS", telemetry=telemetry, **connector_entry
            )
        elif connector == "AwsS3Connector":
            from opteryx.connectors.filesystem_connector import FileSystemConnector
            from opteryx.connectors.io_systems import OpteryxS3FileSystem

            filesystem = OpteryxS3FileSystem(**connector_entry)
            connector_instance = FileSystemConnector(
                filesystem=filesystem, storage_type="S3", telemetry=telemetry, **connector_entry
            )
        else:
            # Unknown string connector - try __getattr__
            connector_class = __getattr__(connector)
            connector_instance = connector_class(telemetry=telemetry, **connector_entry)
    elif isinstance(connector, type):
        # Connector is a class, instantiate directly
        connector_instance = connector(telemetry=telemetry, **connector_entry)
    elif callable(connector):
        # Connector is a factory function (like create_local_connector, create_s3_connector, etc.)
        connector_instance = connector(telemetry=telemetry, **connector_entry)
    else:
        raise ValueError(f"Invalid connector type: {type(connector)}")

    # Store the matched prefix and config so binder can extract dataset names
    connector_instance._matched_prefix = matched_prefix

    # Cache the instance
    _connector_cache[cache_key] = connector_instance

    return connector_instance


def __getattr__(connector_name: str):
    """Lazy load connector classes on first access."""
    if connector_name == "OpteryxConnector":
        from opteryx.connectors.opteryx_connector import OpteryxConnector

        return OpteryxConnector
    if connector_name == "FileSystemConnector":
        from opteryx.connectors.filesystem_connector import FileSystemConnector

        return FileSystemConnector
    if connector_name == "GcpCloudStorageConnector":
        # Return FileSystemConnector with GCS filesystem
        return create_gcs_connector
    if connector_name == "AwsS3Connector":
        # Return FileSystemConnector with S3 filesystem
        return create_s3_connector
    if connector_name == "DiskConnector":
        # Return FileSystemConnector with local filesystem
        return create_local_connector

    raise AttributeError(f"module {__name__!r} has no attribute {connector_name!r}")
