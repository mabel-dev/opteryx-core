"""
Custom Arrow FileSystem implementations using Opteryx's optimized I/O.

These filesystems implement the pyarrow.fs.FileSystem interface but use
Opteryx's memory-view-based readers and stream wrappers for optimal performance.
"""

__all__ = [
    "OpteryxLocalFileSystem",
    "OpteryxGcsFileSystem",
    "OpteryxHttpFileSystem",
    "create_filesystem",
]


def create_filesystem(protocol: str):
    """
    Factory function to instantiate appropriate filesystem based on protocol.

    Used by execution operators to create filesystem from file path protocol prefix.
    This enables generic execution that works across all storage types.

    Args:
        protocol: Protocol string from file path (e.g., "gs", "file")

    Returns:
        Appropriate filesystem instance

    Raises:
        ValueError: If protocol is not supported

    Example:
        >>> protocol = "gs"  # from "gs://bucket/file.parquet"
        >>> fs = create_filesystem(protocol)
        >>> # fs is an OpteryxGcsFileSystem instance
    """
    protocol_map = {
        "gs": "OpteryxGcsFileSystem",
        "gcs": "OpteryxGcsFileSystem",
        "http": "OpteryxHttpFileSystem",
        "https": "OpteryxHttpFileSystem",
        "file": "OpteryxLocalFileSystem",
        "": "OpteryxLocalFileSystem",  # No protocol = local file
    }

    if protocol not in protocol_map:
        raise ValueError(
            f"Unsupported storage protocol: {protocol}. "
            f"Supported protocols: {list(protocol_map.keys())}"
        )

    filesystem_class_name = protocol_map[protocol]
    filesystem_class = __getattr__(filesystem_class_name)
    return filesystem_class()


def __getattr__(file_system: str):
    """Lazy load connector classes on first access."""
    if file_system == "OpteryxGcsFileSystem":
        from opteryx.connectors.io_systems.gcs_filesystem import OpteryxGcsFileSystem

        return OpteryxGcsFileSystem
    if file_system == "OpteryxLocalFileSystem":
        from opteryx.connectors.io_systems.local_filesystem import OpteryxLocalFileSystem

        return OpteryxLocalFileSystem
    if file_system == "OpteryxHttpFileSystem":
        from opteryx.connectors.io_systems.http_filesystem import OpteryxHttpFileSystem

        return OpteryxHttpFileSystem
    raise AttributeError(f"module {__name__} has no attribute {file_system}")
