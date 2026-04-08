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
    """Simple in-memory footer and decoded-column cache.

    This cache stores parsed footer metadata and decoded column payloads for a
    single process lifetime. It is intentionally simple: there is no eviction or
    persistence, but it avoids repeated footer parsing and repeated column decode
    work within the same query execution.
    """

    def __init__(self):
        self._footer_cache: dict[str, dict] = {}
        self._column_cache: dict[tuple[str, int, str], Any] = {}

    def get_footer(self, path: str) -> Optional[dict]:
        return self._footer_cache.get(path)

    def set_footer(self, path: str, metadata: dict) -> None:
        self._footer_cache[path] = metadata

    def get_column(self, path: str, rg_idx: int, column_name: str) -> Optional[Any]:
        return self._column_cache.get((path, rg_idx, column_name))

    def set_column(self, path: str, rg_idx: int, column_name: str, data: Any) -> None:
        self._column_cache[(path, rg_idx, column_name)] = data

    def clear(self) -> None:
        self._footer_cache.clear()
        self._column_cache.clear()

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
