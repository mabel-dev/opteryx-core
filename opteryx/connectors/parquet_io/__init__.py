import ctypes
import os
import sys

from opteryx.compiled.structures.footer_cache import ParquetFooterBytesCache
from opteryx.connectors.parquet_io.predicates import extract_predicate_stats
from opteryx.connectors.parquet_io.predicates import row_group_may_satisfy

# Load pool_reader with RTLD_GLOBAL so its rugo decode symbols (e.g.
# DecodeColumnFromChunk, compiled only here — see setup.py's pool_reader
# Extension sources) are visible to consumer extensions compiled against
# rugo/src/parquet/io_pipeline.hpp at runtime (opteryx.operators._operators'
# native_parquet_scan_source.hpp #includes that header directly). Must happen
# before any such consumer extension is imported. Mirrors draken/__init__.py's
# RTLD_GLOBAL load of draken_native for the same reason.
_flags = sys.getdlopenflags()
sys.setdlopenflags(ctypes.RTLD_GLOBAL | os.RTLD_NOW)
from opteryx.connectors.parquet_io import pool_reader  # noqa: F401, E402
sys.setdlopenflags(_flags)

from opteryx.connectors.parquet_io.pool_reader import fetch_column_chunk_info
from opteryx.connectors.parquet_io.pool_reader import fetch_column_stats
from opteryx.connectors.parquet_io.pool_reader import iter_row_groups_ipc as iter_row_groups
from opteryx.connectors.parquet_io.pool_reader import iter_pass2_row_groups_ipc

__all__ = [
    "fetch_column_chunk_info",
    "fetch_column_stats",
    "iter_row_groups",
    "iter_pass2_row_groups_ipc",
    "ParquetFooterBytesCache",
    "extract_predicate_stats",
    "row_group_may_satisfy",
]
