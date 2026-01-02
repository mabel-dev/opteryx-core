# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
GCS Catalog Shim

Provides an OpteryxCatalog-compatible catalog interface over Google Cloud Storage.
"""

from opteryx.connectors.catalogs.local_catalog import LocalFileCatalog
from opteryx.connectors.io_systems import OpteryxGcsFileSystem


class GcsCatalog(LocalFileCatalog):
    """
    Read-only OpteryxCatalog-compatible shim for Google Cloud Storage.

    Uses OpteryxGcsFileSystem for optimized GCS access.
    """

    def __init__(self, name: str, bucket: str, **properties):
        """
        Initialize the GCS catalog.

        Args:
            name: Catalog name
            bucket: GCS bucket name
            **properties: Additional properties
        """
        super().__init__(name, root_path=f"gs://{bucket}", **properties)
        self.bucket = bucket
        # Replace with optimized GCS filesystem
        self.filesystem = OpteryxGcsFileSystem(bucket=bucket, **properties)
