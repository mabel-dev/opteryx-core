# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Object-store prefix listing - ONE implementation, credential-free
(docs/ANONYMOUS_REMOTE_READS_DESIGN.md §2.1).

The credentialed filesystems (OpteryxGcsFileSystem, OpteryxS3FileSystem) and the anonymous ones
used by READ_* (AnonymousGcsFileSystem, AnonymousS3FileSystem) list through these functions, so
pagination, the trailing-slash directory rule and placeholder handling exist once. The functions
FETCH NOTHING THEMSELVES and know nothing of credentials: the caller hands in an HTTP `get` and
the headers (GCS) or a page-URL builder (S3) that carry whatever authentication it has. This
module therefore contains no path to the platform's credentials, which is what lets the anonymous
classes use it under the READ_* rule (never the system's credentials).

A listing returns FULLY-SCHEMED paths (gs:// / s3://), with the size and modification time (ns)
the listing itself carries, skips the zero-byte placeholder objects consoles create for
"folders", and terminates the prefix with "/" so `space_missions` never matches
`space_missions_backup`.
"""

import datetime
import json
import urllib.parse

from opteryx.compiled.http_client import HttpStatusError
from opteryx.connectors.io_systems._file_info import FileInfoLike
from opteryx.exceptions import DatasetReadError
from opteryx.exceptions import md_code

GCS_API = "https://storage.googleapis.com"
LIST_PAGE_SIZE = 1000
S3_LIST_NAMESPACE = "{http://s3.amazonaws.com/doc/2006-03-01/}"

# What one READ_* glob may list. SQL text can point a listing at any public bucket, and an
# unbounded one would page a multi-million-object bucket.
DEFAULT_ANONYMOUS_LISTING_LIMIT = 10_000


def rfc3339_to_ns(stamp: str) -> int:
    """Nanoseconds since the epoch for an RFC 3339 timestamp (an object listing's
    `updated` / `LastModified`). Integer arithmetic, so no float rounding moves the value."""
    moment = datetime.datetime.fromisoformat(stamp)
    delta = moment - datetime.datetime(1970, 1, 1, tzinfo=datetime.timezone.utc)
    return (delta // datetime.timedelta(microseconds=1)) * 1000


def anonymous_listing_limit() -> int:
    """The most objects one anonymous (READ_*) listing may return: the
    READ_FUNCTION_LISTING_LIMIT setting, else the default. A value that is not a positive
    integer raises rather than silently meaning "unlimited"."""
    from opteryx import config

    raw = config.get("READ_FUNCTION_LISTING_LIMIT")
    if raw is None:
        return DEFAULT_ANONYMOUS_LISTING_LIMIT
    try:
        value = int(raw)
    except ValueError:
        value = 0
    if value <= 0:
        raise ValueError(f"READ_FUNCTION_LISTING_LIMIT must be a positive integer, got {raw!r}")
    return value


def _failed(base_dir: str, err: HttpStatusError, anonymous: bool) -> DatasetReadError:
    if anonymous and err.status in (401, 403):
        return DatasetReadError(
            f"Unable to list {md_code(base_dir)}: the bucket does not allow anonymous listing "
            f"(HTTP {err.status}). Name the file exactly instead."
        )
    return DatasetReadError(f"Unable to list {md_code(base_dir)} - {err}")


def _check_limit(count: int, limit, base_dir: str) -> None:
    if limit is not None and count > limit:
        raise DatasetReadError(
            f"Listing {md_code(base_dir)} returned more than {limit:,} objects, the most a READ_* "
            "glob may list (READ_FUNCTION_LISTING_LIMIT). Narrow the pattern."
        )


def list_gcs_objects(
    get,
    base_dir: str,
    recursive: bool = True,
    headers=None,
    anonymous: bool = False,
    limit=None,
    api: str = GCS_API,
) -> list:
    """The objects under `base_dir` (``<bucket>/<prefix>``, with or without ``gs://``), via the
    GCS JSON API. `get(url, headers)` performs the request and returns the body bytes;
    `headers` is whatever authentication the caller has (``{}`` = none). Paginates - a
    truncated listing would silently under-read rather than fail. A listing over `limit`
    objects raises."""
    path = base_dir[5:] if base_dir.startswith("gs://") else base_dir
    path = path.strip("/")
    if not path:
        raise ValueError("list_files: a GCS path must include a bucket")

    bucket, _, object_prefix = path.partition("/")
    # Trailing slash = directory semantics. An empty object_prefix means the whole bucket,
    # where no prefix filter is correct.
    prefix = f"{object_prefix}/" if object_prefix else ""
    url_base = f"{api}/storage/v1/b/{urllib.parse.quote(bucket, safe='')}/o"
    blobs: list = []
    page_token = None

    while True:
        params = {
            "prefix": prefix,
            "fields": "items(name,size,updated),nextPageToken",
            "maxResults": str(LIST_PAGE_SIZE),
        }
        if not recursive:
            # GCS is flat; a delimiter is what makes a listing non-recursive.
            params["delimiter"] = "/"
        if page_token:
            params["pageToken"] = page_token

        try:
            raw = get(f"{url_base}?{urllib.parse.urlencode(params)}", dict(headers or {}))
        except HttpStatusError as err:
            raise _failed(base_dir, err, anonymous) from err
        except RuntimeError as err:
            raise DatasetReadError(f"Unable to list {md_code(base_dir)} - {err}") from err

        payload = json.loads(raw) if raw else {}
        for item in payload.get("items", ()):
            name = item.get("name")
            # Skip the zero-byte placeholder objects the console creates for "folders" -
            # they are not readable data files.
            if name and not name.endswith("/"):
                blobs.append(
                    FileInfoLike(
                        path=f"gs://{bucket}/{name}",
                        size=int(item["size"]),
                        mtime=rfc3339_to_ns(item["updated"]),
                    )
                )
        _check_limit(len(blobs), limit, base_dir)

        page_token = payload.get("nextPageToken")
        if not page_token:
            return blobs


def list_s3_objects(
    get,
    page_url,
    base_dir: str,
    recursive: bool = True,
    anonymous: bool = False,
    limit=None,
) -> list:
    """The objects under `base_dir` (``<bucket>/<prefix>``, with or without ``s3://``), via
    ListObjectsV2. `page_url(bucket, query)` builds each page's URL - presigned for the
    credentialed filesystem, plain for the anonymous one - and `get(url, headers)` fetches
    it. Paginates; a listing over `limit` objects raises."""
    # nosec B405 - no defusedxml: the engine has zero dependencies. The stdlib expat enforces
    # entity amplification limits, and ElementTree never resolves external entities or fetches
    # DTDs. The document is the reply to a request this module addressed to an object store.
    import xml.etree.ElementTree as ElementTree  # nosec B405

    from opteryx.connectors.io_systems.s3_filesystem import split_path

    bucket, prefix = split_path(base_dir.rstrip("/"))
    # Trailing slash = directory semantics; an empty prefix means the whole bucket.
    if prefix:
        prefix = f"{prefix}/"

    blobs: list = []
    continuation_token = None

    while True:
        query = {"list-type": "2", "max-keys": str(LIST_PAGE_SIZE)}
        if prefix:
            query["prefix"] = prefix
        if not recursive:
            query["delimiter"] = "/"
        if continuation_token:
            query["continuation-token"] = continuation_token

        try:
            raw = get(page_url(bucket, query), {})
        except HttpStatusError as err:
            raise _failed(base_dir, err, anonymous) from err
        except RuntimeError as err:
            raise DatasetReadError(f"Unable to list {md_code(base_dir)} - {err}") from err

        # nosec B314 - see the import above.
        root = ElementTree.fromstring(raw) if raw else None  # nosec B314
        if root is None:
            return blobs

        for contents in root.findall(f"{S3_LIST_NAMESPACE}Contents"):
            key_element = contents.find(f"{S3_LIST_NAMESPACE}Key")
            key = key_element.text if key_element is not None else None
            # Skip the zero-byte placeholder objects consoles create for "folders".
            if key and not key.endswith("/"):
                blobs.append(
                    FileInfoLike(
                        path=f"s3://{bucket}/{key}",
                        size=int(contents.find(f"{S3_LIST_NAMESPACE}Size").text),
                        mtime=rfc3339_to_ns(contents.find(f"{S3_LIST_NAMESPACE}LastModified").text),
                    )
                )
        _check_limit(len(blobs), limit, base_dir)

        truncated = root.find(f"{S3_LIST_NAMESPACE}IsTruncated")
        if truncated is None or (truncated.text or "").lower() != "true":
            return blobs

        next_token = root.find(f"{S3_LIST_NAMESPACE}NextContinuationToken")
        continuation_token = next_token.text if next_token is not None else None
        if not continuation_token:
            return blobs
