"""
Google Cloud Storage filesystem implementation using Opteryx's optimized I/O.

This implements pyarrow.fs.FileSystem interface but uses Opteryx's
stream wrappers for high-performance GCS access.
"""

import io
import os
import urllib.parse
from typing import List
from typing import Union

from opteryx.exceptions import DatasetReadError
from opteryx.exceptions import MissingDependencyError


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

            bucket, _, _, _ = path_utils.get_parts(path)
            object_full_path = urllib.parse.quote(path[(len(bucket) + 1) :], safe="")
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

    def open_input_stream(self, path: str):
        """Open a GCS object for reading as a stream."""
        return GcsFile(path, self.session, self.access_token)

    def open_input_file(self, path: str):
        """Open a GCS object for random access reading."""
        return GcsFile(path, self.session, self.access_token)

    async def async_read_blob(self, *, blob_name, pool, session, telemetry, **kwargs):
        import asyncio

        from opteryx.utils import paths

        # strip gs:// prefix
        if blob_name.startswith("gs://"):
            blob_name = blob_name[5:]

        bucket, _, _, _ = paths.get_parts(blob_name)
        # DEBUG: print("READ   ", blob_name)

        object_full_path = urllib.parse.quote(blob_name[(len(bucket) + 1) :], safe="")

        url = f"https://storage.googleapis.com/{bucket}/{object_full_path}"

        response = await session.get(
            url,
            headers={
                "Authorization": f"Bearer {self.access_token}",
                "Accept-Encoding": "identity",
            },
            timeout=30,
        )

        if response.status != 200:
            raise DatasetReadError(f"Unable to read '{blob_name}' - {response.status}")
        data = await response.read()
        ref = await pool.commit(data)
        # treat both None and -1 as commit failure and retry, but cap retries to avoid hanging
        max_retries = 10
        attempts = 0
        while (ref is None or ref == -1) and attempts < max_retries:
            attempts += 1
            telemetry.stalls_io_waiting_on_engine += 1
            telemetry.cpu_wait_seconds += 0.1
            await asyncio.sleep(0.1)
            try:
                ref = await pool.commit(data)
            except Exception as e:
                ref = None

        if ref is None or ref == -1:
            # Give up and raise so caller can handle the failure instead of hanging
            raise DatasetReadError(f"Unable to commit data to MemoryPool after {attempts} attempts")
        telemetry.bytes_read += len(data)
        return ref
