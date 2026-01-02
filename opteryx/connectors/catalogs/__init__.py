# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
OpteryxCatalog Shims

These shims implement the OpteryxCatalog interface for non-Opteryx storage systems,
allowing uniform catalog-based access patterns across all storage types.

Key characteristics:
- Read-only (no table/view creation)
- No persistent metadata (schema inferred from files)
- Filesystem-based table discovery
- Compatible with OpteryxCatalog API
"""

from opteryx.connectors.catalogs.gcs_catalog import GcsCatalog
from opteryx.connectors.catalogs.local_catalog import LocalFileCatalog
from opteryx.connectors.catalogs.s3_catalog import S3Catalog

__all__ = ["LocalFileCatalog", "GcsCatalog", "S3Catalog"]
