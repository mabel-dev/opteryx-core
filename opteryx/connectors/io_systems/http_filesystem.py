"""HTTP(S) filesystem for remote file access via HTTP Range requests.

Provides efficient byte-range reads and async streaming support for HTTP/HTTPS URLs.
Standalone implementation with no Arrow/PyArrow dependencies.
"""

import io
import threading
import urllib.parse
from concurrent.futures import as_completed
from dataclasses import dataclass
from enum import Enum
from typing import List, Optional, Tuple, Union

from opteryx.connectors.parquet_io.thread_pool_manager import (
    get_filesystem_pool,
    LazyPoolProxy,
)
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


# Module-level thread pool proxy: lazy wrapper that always defers to thread_pool_manager cache.
# This ensures that even if pools are shut down (e.g., in tests), the proxy will
# get the fresh recreated pool from the cache on next access.
_HTTP_RANGE_POOL = LazyPoolProxy(_get_http_range_pool)


class OpteryxHttpFileSystem:
    """HTTP(S) filesystem using HTTP Range requests for partial file access.

    Supports both sync and async operations:
    - Sync: read_ranges(), stream_to() use requests library with optimized pooling
    - Async: async_stream_to() uses caller-provided aiohttp.ClientSession

    Standalone implementation with no external dependencies beyond requests/aiohttp.
    """

    def __init__(self, base_url: str = "", **kwargs):
        """Initialize HTTP filesystem.

        Args:
            base_url: Optional base URL for relative path resolution (e.g., "https://example.com/data/")
            **kwargs: Additional arguments (ignored, for compatibility)
        """
        self.base_url = base_url.rstrip("/") if base_url else ""

        try:
            import requests
            from requests.adapters import HTTPAdapter
        except (ImportError, AttributeError) as err:  # pragma: no cover
            name = getattr(err, "name", None) or str(err)
            raise MissingDependencyError(name) from err

        # Create a HTTP connection session to reduce overhead for each fetch.
        # Aggressive pool config: 128 max connections (96 workers + 32 buffer)
        # pool_connections=1: single pool for host (multi-host would need refactoring)
        self.session = requests.Session()
        adapter = HTTPAdapter(
            pool_connections=1,
            pool_maxsize=128,  # 96 workers + 32 buffer
        )
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)
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
                response = self.session.head(url, timeout=10)
                if response.status_code == 200:
                    size = int(response.headers.get("content-length", 0))
                    return idx, FileInfo(path=path, type=FileType.File, size=size)
                else:
                    return idx, FileInfo(path=path, type=FileType.NotFound)
            except Exception:
                return idx, FileInfo(path=path, type=FileType.NotFound)

        # Fast path: avoid pool overhead for single file
        if len(paths) == 1:
            _, info = _head_one(0, paths[0])
            return info if single_path else [info]

        # Parallel HEAD requests; preserve caller's path order
        infos: List[FileInfo] = [None] * len(paths)  # type: ignore[assignment]
        futures = [
            _HTTP_RANGE_POOL.submit(_head_one, idx, path) for idx, path in enumerate(paths)
        ]
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
            response = self.session.get(
                url,
                headers={"Range": f"bytes={offset}-{offset + length - 1}"},
                timeout=30,
            )
            if response.status_code not in (200, 206):
                raise DatasetReadError(f"Unable to read '{path}' - {response.status_code}")
            return [response.content]

        # Parallel range reads; preserve output order
        result: List[bytes] = [b""] * len(ranges)

        def _fetch(idx: int, offset: int, length: int) -> Tuple[int, bytes]:
            response = self.session.get(
                url,
                headers={"Range": f"bytes={offset}-{offset + length - 1}"},
                timeout=30,
            )
            if response.status_code not in (200, 206):
                raise DatasetReadError(f"Unable to read '{path}' - {response.status_code}")
            return idx, response.content

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
        response = self.session.get(url, stream=True, timeout=30)

        if response.status_code != 200:
            raise DatasetReadError(f"Unable to read '{path}' - {response.status_code}")

        total = 0
        for chunk in response.iter_content(chunk_size=chunk_size):
            sink.write(chunk)
            total += len(chunk)
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
        response = self.session.get(url, timeout=30)

        if response.status_code != 200:
            raise DatasetReadError(f"Unable to read '{path}' - {response.status_code}")

        # Wrap content in BytesIO and attach memoryview for Arrow compatibility
        bio = io.BytesIO(response.content)
        bio.memoryview = memoryview(response.content)  # type: ignore[attr-defined]
        return bio

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
