# cython: language_level=3, boundscheck=False, wraparound=False, cdivision=True

"""
Parquet row-group transport over MemoryPool with C++ threading.

Bridge: Uses the existing pool_reader.py implementation but with C++ ThreadPool
instead of Python's ThreadPoolExecutor. This eliminates Python thread overhead
while keeping the complex dispatch logic intact.

Usage:
    from opteryx.connectors.parquet_io.pool_reader import iter_row_groups_pool
    for row_group in iter_row_groups_pool(paths, columns, ...):
        # row_group is Dict[str, Vector]
"""

from __future__ import annotations

# For now, re-export the pure Python implementation.
# The integration with C++ threading will happen via import-time injection:
# pool_reader.py will use CppThreadPool when available instead of ThreadPoolExecutor.

from opteryx.connectors.parquet_io.pool_reader import (
    iter_row_groups_pool,
    _stable_u64,
)

__all__ = ["iter_row_groups_pool"]
