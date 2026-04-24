"""
Parquet Row-Group × Column-Chunk Reader
========================================

Implements the design from docs/parquet-column-reads-design.md:
- Footer-first metadata planning
- Row-group and column pruning
- Selective byte-range reads via filesystem abstraction
- Pluggable caching (in-process, Redis, etc.)

Orchestrates between:
  - Filesystem layer: format-agnostic read_ranges()
  - Parquet layer: footer parsing, range planning, column decoding
  - Execution layer: operators that consume decoded columns

Usage
-----
::

    from opteryx.connectors.io_systems.local_filesystem import OpteryxLocalFileSystem
    from opteryx.connectors.parquet_io import fetch_columns

    fs = OpteryxLocalFileSystem()

    # Fetch decoded columns for row group 0
    columns = fetch_columns(fs, "/path/to/file.parquet", rg_idx=0, column_names=["user_id", "revenue"])

    # columns is a dict: {"user_id": Vector, "revenue": Vector}
"""

from opteryx.compiled.structures.footer_cache import ParquetFooterBytesCache
from opteryx.connectors.parquet_io.cache import InMemoryParquetCache
from opteryx.connectors.parquet_io.cache import ParquetCache
from opteryx.connectors.parquet_io.predicates import extract_predicate_stats
from opteryx.connectors.parquet_io.predicates import row_group_may_satisfy
from opteryx.connectors.parquet_io.reader import ListColumnError
from opteryx.connectors.parquet_io.reader import fetch_columns
from opteryx.connectors.parquet_io.reader import fetch_footer
from opteryx.connectors.parquet_io.reader import iter_row_groups

__all__ = [
    "fetch_footer",
    "fetch_columns",
    "iter_row_groups",
    "ListColumnError",
    "ParquetCache",
    "InMemoryParquetCache",
    "ParquetFooterBytesCache",
    "extract_predicate_stats",
    "row_group_may_satisfy",
]
