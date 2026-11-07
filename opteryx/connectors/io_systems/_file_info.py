"""File metadata container for filesystem operations."""

from __future__ import annotations

from typing import Mapping
from typing import Optional

__slots__ = ("path", "size", "mtime", "version")


class FileInfoLike:
    """A file's path, size and modification time. `mtime` is whatever the producing
    filesystem's listing reports, consistently per filesystem (nanoseconds on the
    local and object-store listings); it is compared only against values from the
    same filesystem, never interpreted as a clock.

    `version` is the object's version token (see `version_from_headers`), or None
    when the producer did not observe one."""

    __slots__ = ("path", "size", "mtime", "version")

    def __init__(self, path: str, size: int = 0, mtime: float = 0.0, version: Optional[str] = None):
        self.path = path
        self.size = size
        self.mtime = mtime
        self.version = version

    def __repr__(self) -> str:
        return f"FileInfoLike(path={self.path!r}, size={self.size}, mtime={self.mtime})"


def version_from_headers(headers: Mapping[str, str]) -> Optional[str]:
    """The object's version token from a HEAD response, or None when the store sent
    nothing that identifies the bytes.

    A token changes whenever the object's bytes can have changed, so it is what the
    Parquet caches key a customer-owned (mutable) object by, next to its path
    (`pool_reader._object_versions`). The strongest identifier the store offers wins:

    - GCS `x-goog-generation`: a new number on every write, and a recreated bucket's
      objects never reuse an old one.
    - `ETag` (S3, and most HTTP servers): changes with the content.
    - `Last-Modified` + `Content-Length`: a weak last resort for plain HTTP servers.

    Header names are matched case-insensitively; HttpClient lower-cases them already.
    """
    lowered = {str(k).lower(): v for k, v in headers.items()}
    generation = lowered.get("x-goog-generation")
    if generation:
        return f"g:{generation}"
    etag = lowered.get("etag")
    if etag:
        return f"e:{etag}"
    modified = lowered.get("last-modified")
    if modified:
        return f"m:{modified}:{lowered.get('content-length', '')}"
    return None
