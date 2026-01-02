# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
S3 Catalog Shim

Provides an OpteryxCatalog-compatible catalog interface over S3/MinIO storage.
"""

from opteryx.connectors.catalogs.local_catalog import LocalFileCatalog
from opteryx.connectors.io_systems import OpteryxS3FileSystem


class S3Catalog(LocalFileCatalog):
    """
    Read-only OpteryxCatalog-compatible shim for S3/MinIO storage.

    Uses OpteryxS3FileSystem for optimized S3 access.
    """

    def __init__(self, name: str, bucket: str, endpoint: str = None, **properties):
        """
        Initialize the S3 catalog.

        Args:
            name: Catalog name
            bucket: S3 bucket name
            endpoint: S3 endpoint (for MinIO)
            **properties: Additional properties (access keys, etc)
        """
        super().__init__(name, root_path=f"s3://{bucket}", **properties)
        self.bucket = bucket
        self.endpoint = endpoint
        # Replace with optimized S3 filesystem
        self.filesystem = OpteryxS3FileSystem(
            bucket=bucket, region=properties.get("region"), **properties
        )
