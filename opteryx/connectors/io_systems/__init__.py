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
]
