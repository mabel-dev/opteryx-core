"""
Pluggable caching strategies for Parquet row-group × column data.

Two independent cache layers:
1. Footer cache: (path) → decoded metadata dict
2. Column cache: (path, rg_idx, column_name) → decoded Draken vector

Strategies:
- InMemoryParquetCache: Process-local dict (default)
- RedisParquetCache: Distributed cache (future)
- NoOpParquetCache: Bypass caching entirely (for testing)
"""

from __future__ import annotations

from abc import ABC
from abc import abstractmethod
from typing import Any
from typing import Optional


class ParquetCache(ABC):
    """Base class for pluggable Parquet caching strategies."""

    @abstractmethod
    def get_footer(self, path: str) -> Optional[dict]:
        """Retrieve cached footer metadata, or None if not cached."""

    @abstractmethod
    def set_footer(self, path: str, metadata: dict) -> None:
        """Store footer metadata in cache."""

    @abstractmethod
    def get_column(self, path: str, rg_idx: int, column_name: str) -> Optional[Any]:
        """Retrieve cached column chunk data, or None if not cached."""

    @abstractmethod
    def set_column(self, path: str, rg_idx: int, column_name: str, data: Any) -> None:
        """Store decoded column chunk in cache."""

    @abstractmethod
    def clear(self) -> None:
        """Flush all caches."""


class InMemoryParquetCache(ParquetCache):
    """Null cache — all gets return misses, all sets are no-ops.

    Caching is intentionally disabled until a proper bounded, eviction-aware
    cache is implemented.  Leaving decoded column data in an unbounded dict
    across a scan inflates GC pressure without meaningful re-use benefit.
    """

    def get_footer(self, path: str) -> Optional[dict]:
        return None

    def set_footer(self, path: str, metadata: dict) -> None:
        pass

    def get_column(self, path: str, rg_idx: int, column_name: str) -> Optional[Any]:
        return None

    def set_column(self, path: str, rg_idx: int, column_name: str, data: Any) -> None:
        pass

    def clear(self) -> None:
        pass

    def stats(self) -> dict:
        """Return cache sizes for observability."""
        return {
            "footer_entries": len(self._footer_cache),
            "column_entries": len(self._column_cache),
        }


class NoOpParquetCache(ParquetCache):
    """Cache that does nothing. Useful for benchmarking pure I/O cost."""

    def get_footer(self, path: str) -> Optional[dict]:
        return None

    def set_footer(self, path: str, metadata: dict) -> None:
        pass

    def get_column(self, path: str, rg_idx: int, column_name: str) -> Optional[Any]:
        return None

    def set_column(self, path: str, rg_idx: int, column_name: str, data: Any) -> None:
        pass

    def clear(self) -> None:
        pass
