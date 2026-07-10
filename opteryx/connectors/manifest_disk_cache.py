"""
Disk-backed cache for catalog manifest objects.

Planning-phase only. This sits in front of the catalog's ``FileIO`` so a manifest
parquet already materialised on the instance's ephemeral disk is read locally
instead of re-fetched from object storage.

Manifests are addressed by snapshot id (``.../metadata/manifest-<id>.parquet``)
and are write-once: a new commit mints a new URI. A cached entry therefore can
never be stale, and needs no invalidation or TTL. Only manifests are cached.
Data files are not. The Firestore snapshot pointer is not — it stays on the
query path every time, as the freshness anchor.

The cache is bounded in bytes, not entries, because a manifest's size scales
with the file count of the dataset it describes.
"""

import os
import re
import threading
from collections import OrderedDict
from hashlib import sha256
from typing import Dict
from typing import Optional

# `<dataset_location>/metadata/manifest-<snapshot_id>.parquet`. The snapshot id in
# the name is what makes the entry immutable; a path without one is not cacheable.
_MANIFEST_URI = re.compile(r"/metadata/manifest-\d+\.parquet$")

_PARQUET_MAGIC = b"PAR1"

_metrics: Dict[str, int] = {
    "hits": 0,
    "misses": 0,
    "bypassed": 0,
    "writes": 0,
    "evictions": 0,
    "bytes_served": 0,
    "bytes_written": 0,
}


def get_manifest_cache_metrics() -> Dict[str, int]:
    """Counters for the process-wide manifest disk cache."""
    return dict(_metrics)


def is_manifest_uri(location: str) -> bool:
    return _MANIFEST_URI.search(location) is not None


class CorruptCacheEntry(Exception):
    """A cache entry exists but is not a well-formed parquet payload."""


class ManifestDiskCache:
    """Byte-bounded LRU of manifest parquet payloads on local disk.

    Entries are keyed by the SHA-256 of the full manifest URI, so two datasets
    that share a snapshot id (ids are millisecond timestamps, not UUIDs) cannot
    collide.
    """

    def __init__(self, directory: str, max_bytes: int):
        if max_bytes <= 0:
            raise ValueError(f"manifest cache max_bytes must be positive, got {max_bytes}")

        self._directory = directory
        self._max_bytes = max_bytes
        self._lock = threading.Lock()

        # digest -> size in bytes, in LRU order (oldest first)
        self._index: "OrderedDict[str, int]" = OrderedDict()
        self._resident = 0

        os.makedirs(directory, exist_ok=True)
        self._seed_index()

    def _seed_index(self) -> None:
        """Adopt entries left by an earlier process on this instance's disk.

        Ordered by mtime so the LRU approximates the order they were last written.
        Stale `.tmp` files from a crashed writer are removed; they are never read.
        """
        entries = []
        for name in os.listdir(self._directory):
            path = os.path.join(self._directory, name)
            if name.endswith(".tmp"):
                os.unlink(path)
                continue
            stat = os.stat(path)
            entries.append((stat.st_mtime, name, stat.st_size))

        for _, name, size in sorted(entries):
            self._index[name] = size
            self._resident += size

        self._evict_to_fit(0)

    def _path(self, digest: str) -> str:
        return os.path.join(self._directory, digest)

    def _evict_to_fit(self, incoming: int) -> None:
        """Drop least-recently-used entries until `incoming` more bytes fit.

        Caller holds the lock.
        """
        while self._index and self._resident + incoming > self._max_bytes:
            digest, size = self._index.popitem(last=False)
            os.unlink(self._path(digest))
            self._resident -= size
            _metrics["evictions"] += 1

    def get(self, uri: str) -> Optional[bytes]:
        """Return the cached payload for `uri`, or None on a miss.

        A present-but-malformed entry raises rather than silently re-fetching:
        a cache that quietly repairs itself hides a broken disk.
        """
        digest = sha256(uri.encode("utf-8")).hexdigest()

        with self._lock:
            if digest not in self._index:
                _metrics["misses"] += 1
                return None

            self._index.move_to_end(digest)
            with open(self._path(digest), "rb") as handle:
                payload = handle.read()

        if (
            len(payload) < 8
            or payload[:4] != _PARQUET_MAGIC
            or payload[-4:] != _PARQUET_MAGIC
        ):
            raise CorruptCacheEntry(
                f"manifest cache entry for {uri} is not a valid parquet payload "
                f"({len(payload)} bytes); disk at {self._directory} may be damaged"
            )

        _metrics["hits"] += 1
        _metrics["bytes_served"] += len(payload)
        return payload

    def put(self, uri: str, payload: bytes) -> None:
        """Store `payload` under `uri`, evicting LRU entries to stay in budget.

        A payload larger than the whole budget is not cached; caching it would
        evict every other entry to hold one item.
        """
        size = len(payload)
        if size > self._max_bytes:
            return

        digest = sha256(uri.encode("utf-8")).hexdigest()
        final = self._path(digest)
        tmp = f"{final}.{os.getpid()}.{threading.get_ident()}.tmp"

        with self._lock:
            if digest in self._index:
                return

            self._evict_to_fit(size)

            # Write-then-rename: a reader never observes a partial payload, and a
            # crashed writer leaves only a .tmp, which _seed_index sweeps.
            with open(tmp, "wb") as handle:
                handle.write(payload)
            os.replace(tmp, final)

            self._index[digest] = size
            self._resident += size
            _metrics["writes"] += 1
            _metrics["bytes_written"] += size


_shared_cache: Optional[ManifestDiskCache] = None
_shared_cache_lock = threading.Lock()


def shared_cache() -> Optional[ManifestDiskCache]:
    """The process-wide manifest cache, or None when no directory is configured."""
    global _shared_cache

    from opteryx import config

    if not config.MANIFEST_CACHE_PATH:
        return None

    with _shared_cache_lock:
        if _shared_cache is None:
            _shared_cache = ManifestDiskCache(
                directory=config.MANIFEST_CACHE_PATH,
                max_bytes=config.MANIFEST_CACHE_BYTES,
            )
    return _shared_cache


class DiskCachingFileIO:
    """Wraps a catalog ``FileIO``, serving manifest reads from local disk.

    Every non-manifest operation delegates untouched.
    """

    def __init__(self, inner, cache: ManifestDiskCache):
        self._inner = inner
        self._cache = cache

    def new_input(self, location: str):
        from opteryx_catalog.iops.fileio import InputFile

        if not is_manifest_uri(location):
            _metrics["bypassed"] += 1
            return self._inner.new_input(location)

        payload = self._cache.get(location)
        if payload is not None:
            return InputFile(location, payload)

        # The catalog's GcsFileIO.new_input reads the object eagerly; going through
        # open() keeps us on the public contract rather than its private buffer.
        with self._inner.new_input(location).open() as handle:
            payload = handle.read()

        self._cache.put(location, payload)
        return InputFile(location, payload)

    def new_output(self, location: str):
        return self._inner.new_output(location)

    def delete(self, location: str) -> None:
        return self._inner.delete(location)

    def exists(self, location: str) -> bool:
        return self._inner.exists(location)

    def list_files(self, prefix: str) -> list:
        return self._inner.list_files(prefix)

    ls = list_files
