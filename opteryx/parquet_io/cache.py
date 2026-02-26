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
from typing import Dict
from typing import Optional
from typing import Tuple


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
    """Process-local in-memory cache using dicts.

    Suitable for single-process or local testing. For distributed
    workloads, use a Redis-backed cache instead.

    Note: No memory limits or eviction policy. In production,
    wrap with LRU or TTL logic as needed.
    """

    def __init__(self):
        self._footer_cache: Dict[str, dict] = {}
        self._column_cache: Dict[Tuple[str, int, str], Any] = {}

    def get_footer(self, path: str) -> Optional[dict]:
        return self._footer_cache.get(path)

    def set_footer(self, path: str, metadata: dict) -> None:
        self._footer_cache[path] = metadata

    def get_column(self, path: str, rg_idx: int, column_name: str) -> Optional[Any]:
        key = (path, rg_idx, column_name)
        return self._column_cache.get(key)

    def set_column(self, path: str, rg_idx: int, column_name: str, data: Any) -> None:
        key = (path, rg_idx, column_name)
        self._column_cache[key] = data

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
