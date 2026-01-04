"""
Custom Arrow FileSystem implementations using Opteryx's optimized I/O.

These filesystems implement the pyarrow.fs.FileSystem interface but use
Opteryx's memory-view-based readers and stream wrappers for optimal performance.
"""

from opteryx.connectors.io_systems.gcs_filesystem import OpteryxGcsFileSystem
from opteryx.connectors.io_systems.local_filesystem import OpteryxLocalFileSystem
from opteryx.connectors.io_systems.s3_filesystem import OpteryxS3FileSystem

__all__ = [
    "OpteryxLocalFileSystem",
    "OpteryxGcsFileSystem",
    "OpteryxS3FileSystem",
    "create_filesystem",
]


def create_filesystem(protocol: str):
    """
    Factory function to instantiate appropriate filesystem based on protocol.

    Used by execution operators to create filesystem from file path protocol prefix.
    This enables generic execution that works across all storage types.

    Args:
        protocol: Protocol string from file path (e.g., "gs", "s3", "file")

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
        "gs": OpteryxGcsFileSystem,
        "gcs": OpteryxGcsFileSystem,
        "s3": OpteryxS3FileSystem,
        "file": OpteryxLocalFileSystem,
        "": OpteryxLocalFileSystem,  # No protocol = local file
    }

    if protocol not in protocol_map:
        raise ValueError(
            f"Unsupported storage protocol: {protocol}. "
            f"Supported protocols: {list(protocol_map.keys())}"
        )

    filesystem_class = protocol_map[protocol]
    return filesystem_class()
