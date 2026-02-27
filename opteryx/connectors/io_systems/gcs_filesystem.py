"""
Google Cloud Storage filesystem implementation using Opteryx's optimized I/O.

This implements pyarrow.fs.FileSystem interface but uses Opteryx's
stream wrappers for high-performance GCS access.
"""

import io
import os
import urllib.parse
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import as_completed
from typing import List
from typing import Tuple
from typing import Union

from opteryx.exceptions import DatasetReadError
from opteryx.exceptions import MissingDependencyError

_MAX_PARALLEL_RANGE_READS = 32


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

    def __init__(self, path: str, session, access_token):
        """Initialize GCS file by reading entire object."""
        from opteryx.utils import paths

        # strip gs:// prefix
        if path.startswith("gs://"):
            path = path[5:]

        bucket, _, _, _ = paths.get_parts(path)
        object_full_path = urllib.parse.quote(path[(len(bucket) + 1) :], safe="")
        url = f"https://storage.googleapis.com/{bucket}/{object_full_path}"

        response = session.get(
            url,
            headers={"Authorization": f"Bearer {access_token}", "Accept-Encoding": "identity"},
            timeout=30,
        )

        if response.status_code != 200:
            raise DatasetReadError(f"Unable to read '{path}' - {response.status_code}")

        # Initialize BytesIO with the content
        super().__init__(response.content)

    @property
    def memoryview(self):
        """Return a memoryview of the file content."""
        return memoryview(self.getbuffer())


class OpteryxGcsFileSystem:
    """
    Custom GCS filesystem using direct HTTP API for optimal performance.

    Uses direct GCS JSON API calls for 10% better performance than SDK,
    with connection pooling for efficiency. Provides Arrow-compatible
    filesystem interface via duck typing.
    """

    def __init__(self, bucket=None, **kwargs):
        self.bucket = bucket

        try:
            import requests
            from google.auth.transport.requests import Request
            from requests.adapters import HTTPAdapter
        except (ImportError, AttributeError) as err:  # pragma: no cover
            name = getattr(err, "name", None) or str(err)
            raise MissingDependencyError(name) from err

        # Get GCS credentials
        self.client_credentials = get_storage_credentials()

        # Cache access tokens for accessing GCS
        if not self.client_credentials.valid:
            request = Request()
            self.client_credentials.refresh(request)
        self.access_token = self.client_credentials.token

        # Create a HTTP connection session to reduce effort for each fetch
        self.session = requests.session()
        adapter = HTTPAdapter(pool_connections=100, pool_maxsize=100)
        self.session.mount("https://", adapter)

    def get_file_info(self, paths: Union[str, List[str]]):
        """Get info about GCS objects."""
        from pyarrow.fs import FileInfo
        from pyarrow.fs import FileType

        # Handle both single path and list of paths
        single_path = isinstance(paths, str)
        if single_path:
            paths = [paths]

        infos = []
        for path in paths:
            from opteryx.utils import paths as path_utils

            # Strip gs:// prefix so get_parts sees "bucket/folder/file.parquet"
            # (read_ranges does the same normalisation)
            norm_path = path[5:] if path.startswith("gs://") else path
            bucket, _, _, _ = path_utils.get_parts(norm_path)
            object_full_path = urllib.parse.quote(norm_path[(len(bucket) + 1) :], safe="")
            url = f"https://storage.googleapis.com/{bucket}/{object_full_path}"

            # Use HEAD request to check if object exists and get size
            response = self.session.head(
                url,
                headers={"Authorization": f"Bearer {self.access_token}"},
                timeout=10,
            )

            if response.status_code == 200:
                size = int(response.headers.get("content-length", 0))
                info = FileInfo(path=path, type=FileType.File, size=size)
            else:
                info = FileInfo(path=path, type=FileType.NotFound)
            infos.append(info)

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

        # Avoid threadpool overhead for trivial calls.
        if len(ranges) == 1:
            offset, length = ranges[0]
            end = offset + length - 1
            response = self.session.get(
                url,
                headers={
                    "Authorization": f"Bearer {self.access_token}",
                    "Range": f"bytes={offset}-{end}",
                },
                timeout=30,
            )
            return [response.content]

        # Range requests are network-bound; issue a small bounded fanout and
        # preserve the caller's range order in the output list.
        worker_count = min(_MAX_PARALLEL_RANGE_READS, len(ranges))
        result: List[bytes] = [b""] * len(ranges)

        def _fetch(idx: int, offset: int, length: int) -> Tuple[int, bytes]:
            end = offset + length - 1
            response = self.session.get(
                url,
                headers={
                    "Authorization": f"Bearer {self.access_token}",
                    "Range": f"bytes={offset}-{end}",
                },
                timeout=30,
            )
            return idx, response.content

        with ThreadPoolExecutor(max_workers=worker_count, thread_name_prefix="gcs-range") as pool:
            futures = [
                pool.submit(_fetch, idx, offset, length)
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
        slot (e.g. ``_SlotWriter`` in ``opteryx.iops.worker``).

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

        # Refresh the credential token if it has expired.
        if not self.client_credentials.valid:
            from google.auth.transport.requests import Request

            self.client_credentials.refresh(Request())
            self.access_token = self.client_credentials.token

        bucket, _, _, _ = paths.get_parts(path)
        object_full_path = urllib.parse.quote(path[(len(bucket) + 1) :], safe="")
        url = f"https://storage.googleapis.com/{bucket}/{object_full_path}"

        response = self.session.get(
            url,
            headers={"Authorization": f"Bearer {self.access_token}", "Accept-Encoding": "identity"},
            timeout=30,
            stream=True,
        )

        if response.status_code != 200:
            raise DatasetReadError(f"Unable to read '{path}' - {response.status_code}")

        total = 0
        for chunk in response.iter_content(chunk_size=chunk_size):
            sink.write(chunk)
            total += len(chunk)
        return total

    def _refresh_credentials(self) -> None:
        """Synchronous credential refresh — safe to call from ``asyncio.to_thread``."""
        from google.auth.transport.requests import Request

        self.client_credentials.refresh(Request())
        self.access_token = self.client_credentials.token

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
            "Authorization": f"Bearer {self.access_token}",
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
                "Column projection and filtering are only supported for S3/MinIO storage. "
                "Use S3 Select for remote filtering."
            )
        return GcsFile(path, self.session, self.access_token)

    def open_input_file(self, path: str, columns=None, filters=None):
        """Open a GCS object for random access reading.

        Args:
            path: GCS object path
            columns: Not supported on GCS
            filters: Not supported on GCS
        """
        if columns or filters:
            raise NotImplementedError(
                "Column projection and filtering are only supported for S3/MinIO storage. "
                "Use S3 Select for remote filtering."
            )
        return GcsFile(path, self.session, self.access_token)
