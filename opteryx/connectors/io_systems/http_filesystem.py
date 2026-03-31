"""HTTP(S) filesystem for remote file access via HTTP Range requests.

Provides efficient byte-range reads and async streaming support for HTTP/HTTPS URLs.
Standalone implementation with no Arrow/PyArrow dependencies.
"""

import threading
import urllib.parse
from concurrent.futures import as_completed
from dataclasses import dataclass
from enum import Enum
from typing import List
from typing import Tuple
from typing import Union

from opteryx.connectors.parquet_io.thread_pool_manager import LazyPoolProxy
from opteryx.connectors.parquet_io.thread_pool_manager import get_filesystem_pool
from opteryx.exceptions import DatasetReadError
from opteryx.exceptions import MissingDependencyError


# File type enumeration (minimal, no Arrow dependency)
class FileType(Enum):
    """File type enumeration."""

    File = "file"
    Directory = "directory"
    NotFound = "not_found"


@dataclass
class FileInfo:
    """File metadata container (standalone, no Arrow dependency)."""

    path: str
    type: FileType
    size: int = 0


# HTTP range-read pool.
# Module-level reuse avoids per-call thread creation/destruction overhead.
# Aggressive config: 96 workers, 32 buffer slots = 128 total
_MAX_PARALLEL_RANGE_READS = 96


def _get_http_range_pool():
    """Get HTTP range-read pool via thread_pool_manager."""
    return get_filesystem_pool(protocol="http", max_workers=_MAX_PARALLEL_RANGE_READS)


class _FileBuffer:
    """Lightweight file-like wrapper around a bytes object.

    Holds the raw bytes and exposes a zero-copy memoryview for Arrow/rugo.
    Avoids the unnecessary copy that io.BytesIO(data) would introduce —
    BytesIO copies bytes into its own internal buffer, but callers only
    ever access `.memoryview` here.
    """

    __slots__ = ("_data",)

    def __init__(self, data: bytes) -> None:
        self._data = data

    @property
    def memoryview(self) -> memoryview:
        return memoryview(self._data)

    def close(self) -> None:
        self._data = b""


# Module-level thread pool proxy: lazy wrapper that always defers to thread_pool_manager cache.
# This ensures that even if pools are shut down (e.g., in tests), the proxy will
# get the fresh recreated pool from the cache on next access.
_HTTP_RANGE_POOL = LazyPoolProxy(_get_http_range_pool)


class OpteryxHttpFileSystem:
    """HTTP(S) filesystem using HTTP Range requests for partial file access.

    Supports both sync and async operations:
    - Sync: read_ranges(), stream_to() use libcurl with native connection pooling
    - Async: async_stream_to() uses caller-provided aiohttp.ClientSession

    Standalone implementation with no external pip dependencies (libcurl via C extension).
    """

    def __init__(self, base_url: str = "", **kwargs):
        """Initialize HTTP filesystem.

        Args:
            base_url: Optional base URL for relative path resolution (e.g., "https://example.com/data/")
            **kwargs: Additional arguments (ignored, for compatibility)
        """
        self.base_url = base_url.rstrip("/") if base_url else ""

        try:
            from opteryx.compiled.http_client import HttpClient
        except (ImportError, AttributeError) as err:  # pragma: no cover
            raise RuntimeError(
                f"HTTP client extension import failed: {err}\n\n"
                "This should not happen - http_client is a required extension. "
                "The build system should have failed if it couldn't be built."
            ) from err

        # Create HTTP client with connection pooling via libcurl CURLM.
        # Aggressive config: 128 max connections (96 workers + 32 buffer)
        # Timeout: 60 seconds for overall request
        self.http_client = HttpClient(max_connections=128, timeout_ms=60000)
        self._lock = threading.Lock()

    def _normalize_url(self, path: str) -> str:
        """Convert path to full HTTP URL.

        Args:
            path: URL path (can be absolute http:// URL or relative path)

        Returns:
            Full HTTP(S) URL

        Raises:
            ValueError: If path cannot be resolved to valid URL
        """
        if path.startswith(("http://", "https://")):
            return path
        if self.base_url:
            return f"{self.base_url}/{path.lstrip('/')}"
        raise ValueError(
            f"Invalid HTTP path '{path}'. Either provide absolute URL "
            f"(http://... or https://...) or set base_url in OpteryxHttpFileSystem()"
        )

    def get_file_info(self, paths: Union[str, List[str]]) -> Union[FileInfo, List[FileInfo]]:
        """Get file metadata via HTTP HEAD requests.

        Args:
            paths: Single path string or list of path strings

        Returns:
            Single FileInfo (if paths was str) or list of FileInfo objects
        """
        single_path = isinstance(paths, str)
        if single_path:
            paths = [paths]

        def _head_one(idx: int, path: str) -> Tuple[int, FileInfo]:
            url = self._normalize_url(path)
            try:
                headers = self.http_client.head(url)
                # HTTP HEAD succeeded (http_client raises RuntimeError on error)
                size = int(headers.get("content-length", 0))
                return idx, FileInfo(path=path, type=FileType.File, size=size)
            except Exception:
                return idx, FileInfo(path=path, type=FileType.NotFound)

        # Fast path: avoid pool overhead for single file
        if len(paths) == 1:
            _, info = _head_one(0, paths[0])
            return info if single_path else [info]

        # Parallel HEAD requests; preserve caller's path order
        infos: List[FileInfo] = [None] * len(paths)  # type: ignore[assignment]
        futures = [_HTTP_RANGE_POOL.submit(_head_one, idx, path) for idx, path in enumerate(paths)]
        for fut in as_completed(futures):
            idx, info = fut.result()
            infos[idx] = info

        return infos[0] if single_path else infos

    def read_ranges(self, path: str, ranges: List[Tuple[int, int]]) -> List[bytes]:
        """Read multiple byte ranges from HTTP resource using Range requests.

        Args:
            path: HTTP(S) URL or relative path (requires base_url if relative)
            ranges: List of (offset, length) tuples specifying byte ranges to read

        Returns:
            List of byte buffers in the same order as ranges
        """
        if not ranges:
            return []

        url = self._normalize_url(path)

        # Avoid thread pool overhead for single range
        if len(ranges) == 1:
            offset, length = ranges[0]
            try:
                data = self.http_client.get(
                    url,
                    headers={"Range": f"bytes={offset}-{offset + length - 1}"},
                )
                return [data]
            except RuntimeError as err:
                raise DatasetReadError(f"Unable to read '{path}' - {err}") from err

        # Parallel range reads; preserve output order
        result: List[bytes] = [b""] * len(ranges)

        def _fetch(idx: int, offset: int, length: int) -> Tuple[int, bytes]:
            try:
                data = self.http_client.get(
                    url,
                    headers={"Range": f"bytes={offset}-{offset + length - 1}"},
                )
                return idx, data
            except RuntimeError as err:
                raise DatasetReadError(f"Unable to read '{path}' - {err}") from err

        futures = [
            _HTTP_RANGE_POOL.submit(_fetch, idx, offset, length)
            for idx, (offset, length) in enumerate(ranges)
        ]
        for fut in as_completed(futures):
            idx, chunk = fut.result()
            result[idx] = chunk

        return result

    def stream_to(self, path: str, sink, chunk_size: int = 1 << 20) -> int:
        """Stream HTTP resource into sink without intermediate buffer.

        Calls ``sink.write(chunk)`` for each network chunk received.

        Args:
            path: HTTP(S) URL or relative path (requires base_url if relative)
            sink: Object with ``write(bytes) -> int`` method
            chunk_size: HTTP streaming chunk size in bytes (default 1 MiB)

        Returns:
            Total bytes written to sink
        """
        url = self._normalize_url(path)
        try:
            data = self.http_client.get(url)
        except RuntimeError as err:
            raise DatasetReadError(f"Unable to read '{path}' - {err}") from err

        mv = memoryview(data)
        total = 0
        for i in range(0, len(data), chunk_size):
            sink.write(mv[i : i + chunk_size])
            total += chunk_size if i + chunk_size <= len(data) else len(data) - i
        return total

    async def async_stream_to(
        self,
        path: str,
        sink,
        http_session=None,
        chunk_size: int = 1 << 20,
    ) -> int:
        """Async streaming via aiohttp (caller-managed ClientSession).

        Uses native aiohttp streaming so each ``await`` fully releases the GIL,
        allowing many concurrent downloads on a single event-loop thread.

        The caller is responsible for:
        - Creating and owning the ``aiohttp.ClientSession``
        - Holding an ``asyncio.Lock`` around token refresh if needed

        Args:
            path: HTTP(S) URL or relative path (requires base_url if relative)
            sink: Object with ``write(bytes) -> int`` method
            http_session: ``aiohttp.ClientSession`` to use for the request
            chunk_size: Streaming chunk size in bytes (default 1 MiB)

        Returns:
            Total bytes written to sink
        """
        if http_session is None:
            raise ValueError("async_stream_to requires caller-provided aiohttp.ClientSession")

        url = self._normalize_url(path)
        async with http_session.get(url) as response:
            if response.status != 200:
                raise DatasetReadError(f"Unable to read '{path}' - {response.status}")

            total = 0
            async for chunk in response.content.iter_chunked(chunk_size):
                sink.write(chunk)
                total += len(chunk)
            return total

    def open_input_stream(self, path: str, columns=None, filters=None):
        """Open HTTP resource for sequential reading as file-like object.

        Args:
            path: HTTP(S) URL or relative path (requires base_url if relative)
            columns: Not supported for HTTP
            filters: Not supported for HTTP

        Returns:
            BytesIO-like object with read(), seek(), tell() methods
        """
        if columns or filters:
            raise NotImplementedError(
                "Column projection and filtering are not supported for HTTP open_input_stream. "
                "Use fetch_columns() or range-read based APIs for selective reads."
            )

        url = self._normalize_url(path)
        try:
            data = self.http_client.get(url)
        except RuntimeError as err:
            raise DatasetReadError(f"Unable to read '{path}' - {err}") from err

        return _FileBuffer(data)

    def open_input_file(self, path: str, columns=None, filters=None):
        """Open HTTP resource for random access reading.

        Args:
            path: HTTP(S) URL or relative path (requires base_url if relative)
            columns: Not supported for HTTP
            filters: Not supported for HTTP

        Returns:
            BytesIO-like object supporting both sequential and random access
        """
        if columns or filters:
            raise NotImplementedError(
                "Column projection and filtering are not supported for HTTP open_input_file. "
                "Use fetch_columns() or range-read based APIs for selective reads."
            )

        # HTTP requires full file load for random access (no server-side seeking)
        # For efficiency, use BytesIO which supports both sequential and random access
        return self.open_input_stream(path)
