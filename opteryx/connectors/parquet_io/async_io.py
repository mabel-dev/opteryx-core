"""Async I/O support for Parquet reading.

This module provides async variants of I/O operations for integration
with event-driven I/O paths (e.g., aiohttp for HTTP range reads).

Currently experimental - opt-in via config flags.
"""

from __future__ import annotations

import asyncio
import io
from typing import Any, List, Optional, Tuple

from opteryx import config as _cfg


async def async_read_column_task(
    filesystem: Any,
    path: str,
    rg_idx: int,
    column_name: str,
    offset: int,
    length: int,
    http_session: Optional[Any] = None,
    submitted_ns: Optional[int] = None,
) -> dict:
    """Async variant of column read task using event-driven I/O.

    Provides non-blocking I/O for HTTP(S) via aiohttp.ClientSession.
    Falls back to sync read_ranges for local/GCS filesystems.

    Args:
        filesystem: Filesystem instance (e.g., OpteryxHttpFileSystem)
        path: File path to read from
        rg_idx: Row group index (for logging)
        column_name: Column name (for logging)
        offset: Byte offset in file
        length: Number of bytes to read
        http_session: Optional aiohttp.ClientSession for async HTTP
        submitted_ns: Submission time in nanoseconds (for queue latency calculation)

    Returns:
        Dict with keys: raw_bytes, bytes_fetched, bytes_requested, read_ns, queue_wait_ns, task_total_ns
    """
    import time
    from opteryx.tracing import record_event
    from opteryx import config as _trace_cfg

    task_start_ns = time.monotonic_ns()
    queue_wait_ns = (task_start_ns - submitted_ns) if submitted_ns else 0

    if _trace_cfg.OPTERYX_TRACE:
        kwargs = {
            "file_id": path,
            "component": "columns",
            "rg_idx": rg_idx,
            "column": column_name,
        }
        record_event("download_start", **kwargs)

    read_start_ns = time.monotonic_ns()

    # Use async path if http_session provided (HTTP filesystem)
    if http_session is not None and hasattr(filesystem, "async_stream_to"):
        # Use async_stream_to with a BytesIO sink
        sink = io.BytesIO()
        try:
            # Note: This uses stream_to semantics (full file read + slice)
            # For true range reads via async, would need HTTP Range support in async path
            bytes_fetched = await filesystem.async_stream_to(
                path,
                sink,
                http_session=http_session,
                chunk_size=1 << 20,  # 1 MiB chunks
            )
            # Extract the requested range from the full file
            # This is a limitation - ideally we'd do HTTP range requests asynchronously
            sink.seek(offset)
            raw_bytes = sink.read(length)
            bytes_fetched = len(raw_bytes)  # Track actual bytes read for this range
        except Exception as e:
            # Fall back to sync read_ranges
            (raw_bytes,) = filesystem.read_ranges(path, [(offset, length)])
            bytes_fetched = len(raw_bytes)
    else:
        # Use sync read_ranges for non-HTTP filesystems
        (raw_bytes,) = filesystem.read_ranges(path, [(offset, length)])
        bytes_fetched = len(raw_bytes)

    read_ns = time.monotonic_ns() - read_start_ns

    if _trace_cfg.OPTERYX_TRACE:
        kwargs = {
            "file_id": path,
            "component": "columns",
            "rg_idx": rg_idx,
            "column": column_name,
            "bytes_received": bytes_fetched,
        }
        record_event("download_complete", **kwargs)

    return {
        "name": column_name,
        "raw_bytes": raw_bytes,
        "bytes_fetched": bytes_fetched,
        "bytes_requested": length,
        "range_request_count": 1,
        "read_ns": read_ns,
        "queue_wait_ns": queue_wait_ns,
        "task_total_ns": time.monotonic_ns() - task_start_ns,
    }


async def async_read_multiple_ranges(
    filesystem: Any,
    path: str,
    ranges: List[Tuple[int, int]],
    http_session: Optional[Any] = None,
) -> List[bytes]:
    """Async variant for reading multiple ranges with event-driven I/O.

    Attempts to use async operations if available, falls back to sync.

    Args:
        filesystem: Filesystem instance
        path: File path to read from
        ranges: List of (offset, length) tuples
        http_session: Optional aiohttp.ClientSession for async HTTP

    Returns:
        List of byte buffers in same order as ranges
    """
    # If no ranges, return empty list
    if not ranges:
        return []

    # If only one range, use single read (no async benefit)
    if len(ranges) == 1:
        (raw_bytes,) = filesystem.read_ranges(path, ranges)
        return [raw_bytes]

    # For multiple ranges, try async path if available
    if http_session is not None and hasattr(filesystem, "async_stream_to"):
        # Async variant: stream full file and extract ranges
        # This is less efficient than true async range requests
        # but provides event-driven I/O without blocking the event loop
        try:
            sink = io.BytesIO()
            await filesystem.async_stream_to(
                path,
                sink,
                http_session=http_session,
                chunk_size=1 << 20,
            )
            full_content = sink.getvalue()

            # Extract requested ranges
            result = []
            for offset, length in ranges:
                result.append(full_content[offset : offset + length])
            return result
        except Exception:
            # Fall back to sync
            pass

    # Fall back to sync read_ranges for all cases
    return filesystem.read_ranges(path, ranges)


class AsyncIOPool:
    """Optional async I/O pool for event-driven parquet reading.

    Manages aiohttp.ClientSession and coordinates async I/O tasks
    with optional metrics collection.

    Experimental - opt-in via OPTERYX_ENABLE_ASYNC_PARQUET_IO config flag.
    """

    def __init__(self, max_concurrent: int = 96):
        """Initialize async I/O pool.

        Args:
            max_concurrent: Maximum concurrent async tasks (default: 96, aggressive config)
        """
        self.max_concurrent = max_concurrent
        self.session: Optional[Any] = None
        self._semaphore: Optional[asyncio.Semaphore] = None
        self._stats = {
            "total_tasks": 0,
            "total_bytes": 0,
            "total_time_ns": 0,
            "errors": 0,
        }

    async def initialize(self):
        """Initialize aiohttp session and semaphore."""
        try:
            import aiohttp
        except ImportError:
            raise RuntimeError(
                "aiohttp required for async I/O pool. Install with: pip install aiohttp"
            )

        self.session = aiohttp.ClientSession()
        self._semaphore = asyncio.Semaphore(self.max_concurrent)

    async def close(self):
        """Clean up session."""
        if self.session:
            await self.session.close()

    async def read_column(
        self,
        filesystem: Any,
        path: str,
        rg_idx: int,
        column_name: str,
        offset: int,
        length: int,
    ) -> dict:
        """Read column using async I/O with concurrency control.

        Args:
            filesystem: Filesystem instance
            path: File path
            rg_idx: Row group index
            column_name: Column name
            offset: Byte offset
            length: Bytes to read

        Returns:
            Result dict from async_read_column_task
        """
        if not self.session:
            raise RuntimeError("AsyncIOPool not initialized. Call initialize() first.")

        async with self._semaphore:
            import time

            start_ns = time.monotonic_ns()
            result = await async_read_column_task(
                filesystem,
                path,
                rg_idx,
                column_name,
                offset,
                length,
                http_session=self.session,
            )
            elapsed_ns = time.monotonic_ns() - start_ns

            # Update stats
            self._stats["total_tasks"] += 1
            self._stats["total_bytes"] += result.get("bytes_fetched", 0)
            self._stats["total_time_ns"] += elapsed_ns

            return result

    def get_stats(self) -> dict:
        """Return performance statistics."""
        return self._stats.copy()


__all__ = [
    "async_read_column_task",
    "async_read_multiple_ranges",
    "AsyncIOPool",
]
