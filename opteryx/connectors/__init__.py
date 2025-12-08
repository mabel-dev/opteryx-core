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

File System Connectors:
- DiskConnector: Local file system access
- AwsS3Connector: Amazon S3 storage
- GcpCloudStorageConnector: Google Cloud Storage

Format Connectors:
- ArrowConnector: Apache Arrow tables and datasets
- FileConnector: Generic file format handler
- IcebergConnector: Apache Iceberg tables

Special Connectors:
- VirtualDataConnector: In-memory datasets and computed tables
- InformationSchemaConnector: System metadata tables

Lazy Loading:
Connectors are only imported when actually needed, which significantly improves
module import time. The lazy loading is transparent to users - all import patterns
work normally, but the actual connector classes are loaded on first access.

Usage Patterns:

1. Direct Import:
   from opteryx.connectors import ArrowConnector

2. Registration:
   opteryx.register_store("my_prefix", my_connector_instance)

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

# load the base set of prefixes
# fmt:off
_storage_prefixes = {
    "information_schema": "InformationSchema",
}
# fmt:on

__all__ = (
    "AwsS3Connector",
    "DiskConnector",
    "GcpCloudStorageConnector",
    "IcebergConnector",
)


def _lazy_import_connector(connector_name: str):
    """
    Lazy import a connector class by name.

    This function is called by __getattr__ when a connector is accessed,
    or by connector_factory when a connector needs to be instantiated.

    Args:
        connector_name: The name of the connector class to import

    Returns:
        The connector class

    Raises:
        ValueError: If the connector name is unknown
    """
    if connector_name == "AwsS3Connector":
        from opteryx.connectors.aws_s3_connector import AwsS3Connector

        return AwsS3Connector

    elif connector_name == "DiskConnector":
        from opteryx.connectors.disk_connector import DiskConnector

        return DiskConnector

    elif connector_name == "GcpCloudStorageConnector":
        from opteryx.connectors.gcp_cloudstorage_connector import GcpCloudStorageConnector

        return GcpCloudStorageConnector

    elif connector_name == "IcebergConnector":
        from opteryx.connectors.iceberg_connector import IcebergConnector

        return IcebergConnector

    else:
        raise ValueError(f"Unknown connector: {connector_name}")


def register_store(prefix, connector, *, remove_prefix: bool = False, **kwargs):
    """add a prefix"""
    if not isinstance(connector, type):  # type: ignore
        # uninstantiated classes aren't a type
        raise ValueError("connectors registered with `register_store` must be uninstantiated.")

    # Store connector class directly (not as a string)
    _storage_prefixes[prefix] = {
        "connector": connector,  # type: ignore
        "prefix": prefix,
        "remove_prefix": remove_prefix,
        **kwargs,
    }


def known_prefix(prefix) -> bool:
    return prefix in _storage_prefixes


def connector_factory(dataset, telemetry, **config):
    """
    Work out which connector will service the access to this dataset.
    """

    # if it starts with a $, it's a special internal dataset
    if dataset[0] == "$":
        from opteryx.connectors import virtual_data

        return virtual_data.SampleDataConnector(dataset=dataset, telemetry=telemetry)

    # Look up the prefix from the registered prefixes
    connector_entry: dict = config
    connector = None

    for prefix, storage_details in _storage_prefixes.items():
        if dataset == prefix or dataset.startswith(prefix + "."):
            connector_entry.update(storage_details.copy())  # type: ignore
            connector = connector_entry.get("connector")
            break

    if connector is None:
        # fall back to the default connector (local disk if not set)
        connector_entry = _storage_prefixes.get("_default", {})
        connector = connector_entry.get("connector", "DiskConnector")
        remove_prefix = connector_entry.get("remove_prefix", False)
        if remove_prefix and "." in dataset:
            dataset = dataset.split(".", 1)[1]

    if isinstance(connector, str):
        connector_class = _lazy_import_connector(connector)
    else:
        connector_class = connector
        connector = connector.__name__

    prefix = connector_entry.get("prefix", "")
    remove_prefix = connector_entry.get("remove_prefix", False)
    if prefix and remove_prefix and dataset.startswith(prefix):
        # Remove the prefix. If there's a separator (. or //) after the prefix, skip it too
        dataset = dataset[len(prefix) :]
        if dataset.startswith(".") or dataset.startswith("//"):
            dataset = dataset[1:] if dataset.startswith(".") else dataset[2:]

    # DEBUG: print(prefix, dataset, connector, _storage_prefixes)

    return connector_class(dataset=dataset, telemetry=telemetry, **connector_entry)


def __getattr__(name):
    """
    Lazy load connector classes when accessed as module attributes.

    This allows the standard import pattern to work:
        from opteryx.connectors import ArrowConnector

    But defers the actual import until the connector is accessed,
    significantly improving initial module load time.

    Args:
        name: The attribute name being accessed

    Returns:
        The connector class if it exists in __all__

    Raises:
        AttributeError: If the attribute doesn't exist
    """
    if name in __all__:
        return _lazy_import_connector(name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
