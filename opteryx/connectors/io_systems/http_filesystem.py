"""HTTP(S) filesystem for remote file access via HTTP Range requests.

Provides efficient byte-range reads and async streaming support for HTTP/HTTPS URLs.
Standalone implementation with no Arrow dependencies.
"""

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


# File type enumeration (minimal, standalone)
class FileType(Enum):
    """File type enumeration."""

    File = "file"
    Directory = "directory"
    NotFound = "not_found"


@dataclass
class FileInfo:
    """File metadata container (standalone)."""

    path: str
    type: FileType
    size: int = 0


# HTTP HEAD-request pool.
# Used only by get_file_info() for parallel HEAD requests when checking
# multiple paths at once. Range reads use get_many() which runs all transfers
# concurrently inside C++ with no Python thread overhead.
_MAX_PARALLEL_HEAD_REQUESTS = 16


def _get_http_head_pool():
    """Get HTTP HEAD-request pool via thread_pool_manager."""
    return get_filesystem_pool(protocol="http", max_workers=_MAX_PARALLEL_HEAD_REQUESTS)


class _FileBuffer:
    """Lightweight file-like wrapper around a bytes object.

    Holds the raw bytes and exposes a zero-copy memoryview for consumers.
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
_HTTP_HEAD_POOL = LazyPoolProxy(_get_http_head_pool)


class OpteryxHttpFileSystem:
    """HTTP(S) filesystem using HTTP Range requests for partial file access.

    Supports synchronous operations:
    - Sync: `read_ranges()` and `stream_to()` use the native compiled HTTP client
      with connection pooling for efficient range reads.

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

        # Create HTTP client. max_connections caps per-host concurrent connections
        # inside each get_many() call's local CURLM event loop, and sets the
        # connection cache size (CURLMOPT_MAXCONNECTS). 128 ensures no column is
        # queued behind another even for very wide projections across many
        # simultaneously in-flight row groups.
        self.http_client = HttpClient(max_connections=128, timeout_ms=60000)

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
        futures = [_HTTP_HEAD_POOL.submit(_head_one, idx, path) for idx, path in enumerate(paths)]
        for fut in as_completed(futures):
            idx, info = fut.result()
            infos[idx] = info

        return infos[0] if single_path else infos

    def read_ranges(self, path: str, ranges: List[Tuple[int, int]]) -> List[bytes]:
        """Read multiple byte ranges from HTTP resource using Range requests.

        All ranges are fetched concurrently via a single get_many() call.
        The C++ CURLM event loop handles all transfers on one thread with the
        GIL released — no Python thread-pool overhead.

        Args:
            path: HTTP(S) URL or relative path (requires base_url if relative)
            ranges: List of (offset, length) tuples specifying byte ranges to read

        Returns:
            List of byte buffers in the same order as ranges
        """
        if not ranges:
            return []

        url = self._normalize_url(path)

        requests = [
            (url, {"Range": f"bytes={offset}-{offset + length - 1}"}) for offset, length in ranges
        ]

        try:
            return self.http_client.get_many(requests)
        except RuntimeError as err:
            raise DatasetReadError(f"Unable to read '{path}' - {err}") from err

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
