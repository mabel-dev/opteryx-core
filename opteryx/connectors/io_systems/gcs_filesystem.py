"""
Google Cloud Storage filesystem implementation using Opteryx's optimized I/O.

This implements pyarrow.fs.FileSystem interface but uses Opteryx's
stream wrappers for high-performance GCS access.
"""

import io
import os
import threading
import urllib.parse
from concurrent.futures import as_completed
from typing import List
from typing import Tuple
from typing import Union

from opteryx.connectors.parquet_io.thread_pool_manager import LazyPoolProxy
from opteryx.connectors.parquet_io.thread_pool_manager import get_filesystem_pool
from opteryx.exceptions import DatasetReadError
from opteryx.exceptions import MissingDependencyError

# GCS range-read pool.
#
# With the v2 scheduler running up to 64 row-group tasks concurrently
# (PARQUET_GLOBAL_RANGE_READERS=64), and each row-group task internally
# fanning out N column ranges to this pool, we need substantially more than
# 32 workers to avoid excessive queuing.  128 workers keeps 64 row-group tasks
# busy even on wide-column projections while staying well within OS thread
# limits.  Each worker is blocked on network I/O (GIL released), so real
# parallelism scales with the worker count.
_MAX_PARALLEL_RANGE_READS = 128


def _get_gcs_range_pool():
    """Get GCS range-read pool via thread_pool_manager."""
    return get_filesystem_pool(protocol="gcs", max_workers=_MAX_PARALLEL_RANGE_READS)


# Module-level thread pool proxy: lazy wrapper that always defers to thread_pool_manager cache.
# This ensures that even if pools are shut down (e.g., in tests), the proxy will
# get the fresh recreated pool from the cache on next access.
_GCS_RANGE_POOL = LazyPoolProxy(_get_gcs_range_pool)


def get_storage_credentials():
    """Get GCS credentials - copied from gcp_cloudstorage_connector."""
    try:
        from google.cloud import storage
    except (ImportError, AttributeError) as err:  # pragma: no cover
        name = getattr(err, "name", None) or str(err)
        raise MissingDependencyError(name) from err

    if os.environ.get("STORAGE_EMULATOR_HOST"):  # pragma: no cover
        from google.auth.credentials import AnonymousCredentials

        storage_client = storage.Client(credentials=AnonymousCredentials())
    else:  # pragma: no cover
        storage_client = storage.Client()
    return storage_client._credentials


class GcsFile(io.BytesIO):
    """
    File-like wrapper for GCS objects.

    Reads the entire object into memory on open for maximum performance.
    """

    def __init__(self, path: str, http_client, access_token):
        """Initialize GCS file by reading entire object."""
        from opteryx.utils import paths

        # strip gs:// prefix
        if path.startswith("gs://"):
            path = path[5:]

        bucket, _, _, _ = paths.get_parts(path)
        object_full_path = urllib.parse.quote(path[(len(bucket) + 1) :], safe="")
        url = f"https://storage.googleapis.com/{bucket}/{object_full_path}"

        try:
            data = http_client.get(
                url,
                headers={"Authorization": f"Bearer {access_token}", "Accept-Encoding": "identity"},
            )
        except RuntimeError as err:
            raise DatasetReadError(f"Unable to read '{path}' - {err}") from err

        # Initialize BytesIO with the content
        super().__init__(data)

    @property
    def memoryview(self):
        """Return a memoryview of the file content."""
        return memoryview(self.getbuffer())


class OpteryxGcsFileSystem:
    """
    Custom GCS filesystem using direct HTTP API for optimal performance.

    Uses direct GCS JSON API calls for 10% better performance than SDK,
    with libcurl connection pooling for efficiency. Provides Arrow-compatible
    filesystem interface via duck typing.
    """

    def __init__(self, bucket=None, **kwargs):
        self.bucket = bucket

        try:
            from google.auth.transport.requests import Request
            from opteryx.compiled.http_client import HttpClient
        except (ImportError, AttributeError) as err:  # pragma: no cover
            raise RuntimeError(
                f"HTTP client extension import failed: {err}\n\n"
                "This should not happen - http_client is a required extension. "
                "The build system should have failed if it couldn't be built."
            ) from err

        # Get GCS credentials
        self.client_credentials = get_storage_credentials()
        self._Request = Request  # stash for token refresh
        self._token_lock = threading.Lock()  # serialize concurrent token refreshes

        # Cache access tokens for accessing GCS
        if not self.client_credentials.valid:
            request = Request()
            self.client_credentials.refresh(request)

        # Create HTTP client with connection pooling via libcurl CURLM.
        # pool_maxsize must be at least _MAX_PARALLEL_RANGE_READS so that
        # concurrent range-read workers don't block waiting for a free connection.
        # 128 workers handles 64 concurrent row-group tasks efficiently.
        self.http_client = HttpClient(max_connections=_MAX_PARALLEL_RANGE_READS + 16, timeout_ms=60000)

    @property
    def _bearer(self) -> str:
        """Return a valid Bearer token, refreshing if the credential has expired.

        Uses a lock to ensure only one thread refreshes at a time — the 32-worker
        range-read pool can call this concurrently at token expiry boundaries.
        """
        if not self.client_credentials.valid:
            with self._token_lock:
                # Double-checked locking: re-test after acquiring the lock in case
                # another thread already refreshed while we were waiting.
                if not self.client_credentials.valid:
                    self.client_credentials.refresh(self._Request())
        return f"Bearer {self.client_credentials.token}"

    def get_file_info(self, paths: Union[str, List[str]]):
        """Get info about GCS objects."""
        from pyarrow.fs import FileInfo
        from pyarrow.fs import FileType

        # Handle both single path and list of paths
        single_path = isinstance(paths, str)
        if single_path:
            paths = [paths]

        from opteryx.utils import paths as path_utils

        def _head_one(idx: int, path: str, bearer: str) -> Tuple[int, "FileInfo"]:
            norm_path = path[5:] if path.startswith("gs://") else path
            bucket, _, _, _ = path_utils.get_parts(norm_path)
            object_full_path = urllib.parse.quote(norm_path[(len(bucket) + 1) :], safe="")
            url = f"https://storage.googleapis.com/{bucket}/{object_full_path}"
            try:
                headers = self.http_client.head(
                    url,
                    headers={"Authorization": bearer},
                )
                # HEAD succeeded (http_client raises RuntimeError on error)
                size = int(headers.get("content-length", 0))
                return idx, FileInfo(path=path, type=FileType.File, size=size)
            except Exception:
                return idx, FileInfo(path=path, type=FileType.NotFound)

        # Capture a single valid bearer token for this batch.
        bearer = self._bearer

        # Fast path: avoid pool overhead for the common single-path case.
        if len(paths) == 1:
            _, info = _head_one(0, paths[0], bearer)
            return info if single_path else [info]

        # Fan out HEAD requests in parallel; preserve caller's path order.
        infos: List["FileInfo"] = [None] * len(paths)  # type: ignore[assignment]
        futures = [
            _GCS_RANGE_POOL.submit(_head_one, idx, path, bearer) for idx, path in enumerate(paths)
        ]
        for fut in as_completed(futures):
            idx, info = fut.result()
            infos[idx] = info

        return infos[0] if single_path else infos

    def read_ranges(self, path: str, ranges: List[Tuple[int, int]]) -> List[bytes]:
        """Read multiple byte ranges from a GCS object using HTTP range requests.

        Args:
            path: GCS object path, with or without the ``gs://`` prefix.
            ranges: List of (offset, length) tuples specifying byte ranges to read.

        Returns:
            List of byte buffers in the same order as ranges.
        """
        # Normalize path
        if path.startswith("gs://"):
            path = path[5:]

        from opteryx.utils import paths as path_utils

        bucket, _, _, _ = path_utils.get_parts(path)
        object_full_path = urllib.parse.quote(path[(len(bucket) + 1) :], safe="")
        url = f"https://storage.googleapis.com/{bucket}/{object_full_path}"

        if not ranges:
            return []

        # Capture a single valid bearer token for this entire read_ranges call.
        # Using _bearer once here (rather than inside each _fetch closure) avoids
        # N redundant validity checks and string allocations across pool workers.
        bearer = self._bearer

        # Avoid threadpool overhead for trivial calls.
        if len(ranges) == 1:
            offset, length = ranges[0]
            end = offset + length - 1
            try:
                data = self.http_client.get(
                    url,
                    headers={
                        "Authorization": bearer,
                        "Range": f"bytes={offset}-{end}",
                    },
                )
                return [data]
            except RuntimeError as err:
                raise DatasetReadError(f"Unable to read '{path}' - {err}") from err

        # Range requests are network-bound; issue a small bounded fanout and
        # preserve the caller's range order in the output list.
        result: List[bytes] = [b""] * len(ranges)

        def _fetch(idx: int, offset: int, length: int) -> Tuple[int, bytes]:
            end = offset + length - 1
            try:
                data = self.http_client.get(
                    url,
                    headers={
                        "Authorization": bearer,
                        "Range": f"bytes={offset}-{end}",
                    },
                )
                return idx, data
            except RuntimeError as err:
                raise DatasetReadError(f"Unable to read '{path}' - {err}") from err

        futures = [
            _GCS_RANGE_POOL.submit(_fetch, idx, offset, length)
            for idx, (offset, length) in enumerate(ranges)
        ]
        for fut in as_completed(futures):
            idx, chunk = fut.result()
            result[idx] = chunk

        return result

    def stream_to(self, path: str, sink, chunk_size: int = 1 << 20) -> int:
        """Stream a GCS object directly into *sink* without an intermediate buffer.

        Calls ``sink.write(chunk)`` for each network chunk received, giving
        callers a zero-copy path when *sink* writes directly into a shared-memory
        slot.

        Refreshes the OAuth token if it has expired before making the request.

        Args:
            path:       GCS object path, with or without the ``gs://`` prefix.
                        Must include the bucket name as the first path component
                        (e.g. ``my-bucket/path/to/file.parquet``).
            sink:       Any object with a ``write(bytes) -> int`` method.
            chunk_size: HTTP streaming chunk size in bytes (default 1 MiB).

        Returns:
            Total bytes written to *sink*.
        """
        from opteryx.utils import paths

        if path.startswith("gs://"):
            path = path[5:]

        bucket, _, _, _ = paths.get_parts(path)
        object_full_path = urllib.parse.quote(path[(len(bucket) + 1) :], safe="")
        url = f"https://storage.googleapis.com/{bucket}/{object_full_path}"

        try:
            data = self.http_client.get(
                url,
                headers={"Authorization": self._bearer, "Accept-Encoding": "identity"},
            )
        except RuntimeError as err:
            raise DatasetReadError(f"Unable to read '{path}' - {err}") from err

        total = 0
        # Write in chunks to match streaming API
        for i in range(0, len(data), chunk_size):
            chunk = data[i : i + chunk_size]
            sink.write(chunk)
            total += len(chunk)
        return total

    def _refresh_credentials(self) -> None:
        """Synchronous credential refresh — safe to call from ``asyncio.to_thread``.

        Acquires the token lock to prevent concurrent refreshes when called
        alongside parallel _bearer accesses from the range-read pool.
        """
        with self._token_lock:
            self.client_credentials.refresh(self._Request())

    async def async_stream_to(
        self,
        path: str,
        sink,
        http_session,
        chunk_size: int = 1 << 20,
    ) -> int:
        """Async variant of ``stream_to`` using a caller-provided ``aiohttp.ClientSession``.

        Uses native aiohttp streaming so each ``await`` fully releases the GIL,
        allowing many concurrent downloads on a single event-loop thread without
        GIL contention.

        The caller is responsible for:
        - creating and owning the ``aiohttp.ClientSession``
        - holding an ``asyncio.Lock`` around token refresh and calling
          ``_refresh_credentials()`` via ``asyncio.to_thread`` before calling
          this method when ``self.client_credentials.valid`` is ``False``.

        Args:
            path:         GCS object path, with or without ``gs://`` prefix.
            sink:         Object with ``write(bytes) -> int``.
            http_session: ``aiohttp.ClientSession`` to use for the request.
            chunk_size:   Streaming chunk size in bytes (default 1 MiB).

        Returns:
            Total bytes written to *sink*.
        """
        from opteryx.utils import paths

        if path.startswith("gs://"):
            path = path[5:]

        bucket, _, _, _ = paths.get_parts(path)
        object_full_path = urllib.parse.quote(path[(len(bucket) + 1) :], safe="")
        url = f"https://storage.googleapis.com/{bucket}/{object_full_path}"

        headers = {
            "Authorization": self._bearer,
            "Accept-Encoding": "identity",
        }

        async with http_session.get(url, headers=headers) as response:
            if response.status != 200:
                raise DatasetReadError(f"Unable to read '{path}' - {response.status}")
            total = 0
            async for chunk in response.content.iter_chunked(chunk_size):
                sink.write(chunk)
                total += len(chunk)
        return total

    def open_input_stream(self, path: str, columns=None, filters=None):
        """Open a GCS object for reading as a stream.

        Args:
            path: GCS object path
            columns: Not supported on GCS
            filters: Not supported on GCS
        """
        if columns or filters:
            raise NotImplementedError(
                "Column projection and filtering are not supported for GCS open_input_stream/file. "
                "Use fetch_columns() for column-selective reads."
            )
        # Ensure token is fresh before handing it to GcsFile (which reads immediately).
        _ = self._bearer
        return GcsFile(path, self.http_client, self.client_credentials.token)

    def open_input_file(self, path: str, columns=None, filters=None):
        """Open a GCS object for random access reading.

        Args:
            path: GCS object path
            columns: Not supported on GCS
            filters: Not supported on GCS
        """
        if columns or filters:
            raise NotImplementedError(
                "Column projection and filtering are not supported for GCS open_input_stream/file. "
                "Use fetch_columns() for column-selective reads."
            )
        # Ensure token is fresh before handing it to GcsFile (which reads immediately).
        _ = self._bearer
        return GcsFile(path, self.http_client, self.client_credentials.token)
