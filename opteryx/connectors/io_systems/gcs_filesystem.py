"""
Google Cloud Storage filesystem implementation using Opteryx's optimized I/O.

Uses Opteryx's stream wrappers for high-performance GCS access.
"""

import os
import threading
import urllib.parse
from typing import List
from typing import Tuple
from typing import Union

from opteryx.exceptions import DatasetReadError
from opteryx.exceptions import MissingDependencyError

_GCP_AUTH_SCOPES = ("https://www.googleapis.com/auth/cloud-platform",)


def get_storage_credentials():
    """Get GCS credentials - copied from gcp_cloudstorage_connector."""
    try:
        import google.auth
        from google.cloud import storage
    except (ImportError, AttributeError) as err:  # pragma: no cover
        name = getattr(err, "name", None) or str(err)
        raise MissingDependencyError(name) from err

    if os.environ.get("STORAGE_EMULATOR_HOST"):  # pragma: no cover
        from google.auth.credentials import AnonymousCredentials

        storage_client = storage.Client(credentials=AnonymousCredentials())
        return storage_client._credentials

    credentials, _ = google.auth.default(scopes=_GCP_AUTH_SCOPES)
    return credentials


class GcsFile:
    """
    File-like wrapper for GCS objects.

    Reads the entire object into memory on open for maximum performance.
    Holds the raw bytes directly rather than copying into a BytesIO buffer —
    callers only ever access `.memoryview`, so the BytesIO copy was waste.
    """

    __slots__ = ("_data",)

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
            self._data = http_client.get(
                url,
                headers={"Authorization": f"Bearer {access_token}", "Accept-Encoding": "identity"},
            )
        except RuntimeError as err:
            raise DatasetReadError(f"Unable to read '{path}' - {err}") from err

    @property
    def memoryview(self):
        """Return a zero-copy memoryview of the file content."""
        return memoryview(self._data)

    def close(self) -> None:
        self._data = b""


class OpteryxGcsFileSystem:
    """
    Custom GCS filesystem using direct HTTP API for optimal performance.

    Uses direct GCS JSON API calls for 10% better performance than SDK,
    with libcurl connection pooling for efficiency.
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

        self.http_client = HttpClient(max_connections=128, timeout_ms=60000)

    @property
    def _bearer(self) -> str:
        """Return a valid Bearer token, refreshing if the credential has expired.

        Uses a lock to ensure only one thread refreshes at a time — concurrent
        get_file_info() HEAD requests can hit this concurrently at token expiry.
        """
        if not self.client_credentials.valid:
            with self._token_lock:
                # Double-checked locking: re-test after acquiring the lock in case
                # another thread already refreshed while we were waiting.
                if not self.client_credentials.valid:
                    self.client_credentials.refresh(self._Request())
        return f"Bearer {self.client_credentials.token}"

    def _resolve_signing_service_account_email(self) -> str:
        """Return the concrete service account email used for IAM-backed signing.

        Workload-identity credentials on Cloud Run / GCE may initially expose the
        placeholder value ``default`` until they have refreshed against metadata.
        Signed URLs cannot be generated with that placeholder because IAM signBlob
        requires the actual service account email.
        """
        for attribute_name in ("service_account_email", "signer_email"):
            email = getattr(self.client_credentials, attribute_name, None)
            if email and email != "default":
                return email

        cred_info_getter = getattr(self.client_credentials, "get_cred_info", None)
        if callable(cred_info_getter):
            cred_info = cred_info_getter() or {}
            principal = cred_info.get("principal")
            if principal and principal != "default":
                return principal

        raise RuntimeError(
            "Unable to determine the service account email for GCS signed URL generation. "
            "Cloud Run / workload-identity credentials must expose a concrete service account "
            "identity after refresh."
        )

    def list_files(self, base_dir: str, recursive: bool = True) -> list:
        """Return the objects under ``base_dir`` as ``gs://bucket/name`` paths.

        ``base_dir`` is ``<bucket>/<prefix...>``, with or without a ``gs://`` scheme —
        the first path component is the bucket, matching every other method here.

        Returns FULLY-SCHEMED (``gs://``) paths, which is load-bearing, not cosmetic:
        downstream, `_is_local_path` (pool_reader) decides a file is local purely from the
        absence of a scheme. Bare ``bucket/object`` paths would be classed as local, admit
        the local-only native scan path, and have the C++ reader ``pread()`` them as
        on-disk files. Everything that consumes these paths (`get_file_info`,
        `rewrite_to_signed_url`, `GcsFile`) already strips ``gs://`` itself.

        The prefix is always terminated with ``/`` before listing: GCS prefix matching is
        a plain string match, so listing ``space_missions`` would also return
        ``space_missions_backup/...``, silently pulling a sibling dataset's blobs into
        this one. This mirrors the local filesystem's directory semantics.

        Paginates: a dataset can exceed the API's 1000-object page limit, and a truncated
        listing would silently under-read a dataset rather than fail.
        """
        import json

        path = base_dir[5:] if base_dir.startswith("gs://") else base_dir
        path = path.strip("/")
        if not path:
            raise ValueError("list_files: a GCS path must include a bucket")

        bucket, _, object_prefix = path.partition("/")
        # Trailing slash = directory semantics (see docstring). An empty object_prefix
        # means the whole bucket, where no prefix filter is correct.
        prefix = f"{object_prefix}/" if object_prefix else ""

        api = f"https://storage.googleapis.com/storage/v1/b/{urllib.parse.quote(bucket, safe='')}/o"
        bearer = self._bearer
        blobs: List[str] = []
        page_token = None

        while True:
            params = {"prefix": prefix, "fields": "items(name),nextPageToken"}
            if not recursive:
                # GCS is flat; a delimiter is what makes a listing non-recursive.
                params["delimiter"] = "/"
            if page_token:
                params["pageToken"] = page_token

            url = f"{api}?{urllib.parse.urlencode(params)}"
            try:
                raw = self.http_client.get(url, headers={"Authorization": bearer})
            except RuntimeError as err:
                raise DatasetReadError(f"Unable to list '{base_dir}' - {err}") from err

            payload = json.loads(raw) if raw else {}
            for item in payload.get("items", ()):
                name = item.get("name")
                # Skip the zero-byte placeholder objects the console creates for "folders" —
                # they are not readable data files.
                if name and not name.endswith("/"):
                    blobs.append(f"gs://{bucket}/{name}")

            page_token = payload.get("nextPageToken")
            if not page_token:
                return blobs

    def get_file_info(self, paths: Union[str, List[str]]):
        """Get info about GCS objects."""
        from dataclasses import dataclass
        from enum import Enum

        # Local file info without Arrow dependency
        class FileType(Enum):
            File = "file"

        @dataclass
        class FileInfo:
            path: str
            type: "FileType"
            size: int

        # Handle both single path and list of paths
        single_path = isinstance(paths, str)
        if single_path:
            paths = [paths]

        from opteryx.utils import paths as path_utils

        def _object_url(path: str) -> str:
            norm_path = path[5:] if path.startswith("gs://") else path
            bucket, _, _, _ = path_utils.get_parts(norm_path)
            object_full_path = urllib.parse.quote(norm_path[(len(bucket) + 1) :], safe="")
            return f"https://storage.googleapis.com/{bucket}/{object_full_path}"

        # Capture a single valid bearer token for this batch.
        bearer = self._bearer

        # Fast path: avoid batch overhead for the common single-path case.
        if len(paths) == 1:
            headers = self.http_client.head(
                _object_url(paths[0]),
                headers={"Authorization": bearer},
            )
            size = int(headers.get("content-length", 0))
            info = FileInfo(path=paths[0], type=FileType.File, size=size)
            return info if single_path else [info]

        # Fan out all HEAD requests in ONE native libcurl batch (a single C++
        # CURLM event loop, one GIL release for the whole call) instead of a
        # Python-level thread pool: dispatching per-path head() calls onto a
        # pool would force each worker thread to cross back into the
        # interpreter once per path, which is off-limits outside the
        # planning/execution hand-off.
        requests = [(_object_url(path), {"Authorization": bearer}) for path in paths]
        headers_list = self.http_client.head_many(requests)
        infos = [
            FileInfo(path=path, type=FileType.File, size=int(headers.get("content-length", 0)))
            for path, headers in zip(paths, headers_list)
        ]

        return infos[0] if single_path else infos

    def read_ranges(self, path: str, ranges: List[Tuple[int, int]]) -> List[bytes]:
        """Read multiple byte ranges from a GCS object using HTTP range requests.

        All ranges are fetched concurrently via a single get_many() call.
        The C++ CURLM event loop handles all transfers on one thread with the
        GIL released — no Python thread-pool overhead.

        Args:
            path: GCS object path, with or without the ``gs://`` prefix.
            ranges: List of (offset, length) tuples specifying byte ranges to read.

        Returns:
            List of byte buffers in the same order as ranges.
        """
        if not ranges:
            return []

        # Normalize path
        if path.startswith("gs://"):
            path = path[5:]

        from opteryx.utils import paths as path_utils

        bucket, _, _, _ = path_utils.get_parts(path)
        object_full_path = urllib.parse.quote(path[(len(bucket) + 1) :], safe="")
        url = f"https://storage.googleapis.com/{bucket}/{object_full_path}"

        # Capture a single valid bearer token for the entire batch.
        bearer = self._bearer

        requests = [
            (url, {"Authorization": bearer, "Range": f"bytes={offset}-{offset + length - 1}"})
            for offset, length in ranges
        ]

        try:
            return self.http_client.get_many(requests)
        except RuntimeError as err:
            raise DatasetReadError(f"Unable to read '{path}' - {err}") from err

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

        mv = memoryview(data)
        total = 0
        for i in range(0, len(data), chunk_size):
            sink.write(mv[i : i + chunk_size])
            total += chunk_size if i + chunk_size <= len(data) else len(data) - i
        return total

    def rewrite_to_signed_url(self, path: str, expiry_seconds: int = 3600) -> str:
        """Convert a gs:// path to a V4 signed HTTPS URL valid for expiry_seconds.

        The signed URL embeds authentication in its query parameters so the C++
        pipeline can fetch it via libcurl without any Authorization header.

        For service-account credentials (key file), the client library signs
        directly.  For Compute Engine / Cloud Run workload-identity credentials,
        it delegates to the IAM signBlob API using the service account email and
        a fresh access token.
        """
        import datetime

        from google.cloud import storage
        from google.oauth2.service_account import Credentials as SACredentials

        from opteryx.utils import paths as path_utils

        if path.startswith("gs://"):
            path = path[5:]

        bucket_name, _, _, _ = path_utils.get_parts(path)
        blob_name_str = path[len(bucket_name) + 1:]

        creds = self.client_credentials
        client = storage.Client(credentials=creds)
        blob = client.bucket(bucket_name).blob(blob_name_str)

        expiration = datetime.timedelta(seconds=expiry_seconds)

        if isinstance(creds, SACredentials):
            return blob.generate_signed_url(
                expiration=expiration,
                method="GET",
                version="v4",
            )

        # Compute Engine / Cloud Run: sign via IAM using the service account email
        # and a fresh access token so we never need the private key.
        _ = self._bearer  # ensure token is fresh
        signer_email = self._resolve_signing_service_account_email()
        return blob.generate_signed_url(
            expiration=expiration,
            method="GET",
            version="v4",
            service_account_email=signer_email,
            access_token=creds.token,
        )

    def _refresh_credentials(self) -> None:
        """Synchronous credential refresh — safe to call from ``asyncio.to_thread``.

        Acquires the token lock to prevent concurrent refreshes when called
        alongside parallel _bearer accesses from the range-read pool.
        """
        with self._token_lock:
            self.client_credentials.refresh(self._Request())

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
                "Column-selective reads go through the native Parquet scan path."
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
                "Column-selective reads go through the native Parquet scan path."
            )
        # Ensure token is fresh before handing it to GcsFile (which reads immediately).
        _ = self._bearer
        return GcsFile(path, self.http_client, self.client_credentials.token)
