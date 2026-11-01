"""
Cache for catalog manifest objects, in front of the catalog's ``FileIO``.

Planning-phase only. A manifest parquet already held in a cache tier is read from
there instead of being re-fetched from object storage.

Manifests are addressed by snapshot id (``.../metadata/manifest-<id>.parquet``)
and are write-once: a new commit mints a new URI. A cached entry therefore can
never be stale, and needs no invalidation or TTL. Only manifests are cached.
Data files are not. The Firestore snapshot pointer is not — it stays on the
query path every time, as the freshness anchor. That immutability is the whole
licence for this cache; anything cached here must be addressed by a URI that can
only ever name one payload.

Two tiers, read in order, each optional:

* **local disk** (``MANIFEST_CACHE_PATH``) — process/instance local, sub-millisecond.
* **remote KV** (``OPTERYX_MANIFEST_CACHE_LOCATION``, e.g. a shared Valkey) — survives
  the instance, so a manifest fetched by one instance is served to the next without a
  second trip to object storage. This is what pays on deployments that run many
  short-lived instances, where a local-only cache is cold on almost every request.

  This is *not* ``KVSTORE_LOCATION``: that configures the per-query shuffle/spill
  store, which is query-scoped and discarded when the query ends. Same machinery,
  opposite lifecycle — see ``MANIFEST_REMOTE_LOCATION`` in config.

A remote hit is promoted into the local tier, so the second read on an instance is
local. Both tiers are content-addressed by manifest URI; neither is scoped to a
query, which is the point — the cross-query hit is the whole value.

The disk tier is bounded in bytes, not entries, because a manifest's size scales
with the file count of the dataset it describes.

opteryx-core does not depend on any KV client library. The remote tier is built
through ``create_kv_store``, whose backends import their clients lazily; a deployment
that wants Valkey installs ``valkey`` itself and sets ``OPTERYX_MANIFEST_CACHE_LOCATION``.
With neither tier configured, ``manifest_cache_tiers()`` is empty, the wrapper is never
installed, and behaviour is exactly what it was before this cache existed.
"""

import re
import threading
from hashlib import sha256
from typing import Dict
from typing import List
from typing import Optional

from opteryx.connectors.disk_lru_cache import DiskLRUCache

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
    # remote tier, counted separately: a deployment needs to see whether the shared
    # cache is earning its keep, and a remote hit is a different cost to a local one.
    "remote_hits": 0,
    "remote_misses": 0,
    "remote_writes": 0,
    "remote_corrupt": 0,
    "remote_oversize": 0,
    "remote_bytes_served": 0,
}


def get_manifest_cache_metrics() -> Dict[str, int]:
    """Counters for the process-wide manifest cache, across both tiers."""
    return dict(_metrics)


def is_manifest_uri(location: str) -> bool:
    return _MANIFEST_URI.search(location) is not None


def _is_parquet(payload: bytes) -> bool:
    return (
        len(payload) >= 8
        and payload[:4] == _PARQUET_MAGIC
        and payload[-4:] == _PARQUET_MAGIC
    )


class ManifestDiskCache(DiskLRUCache):
    """Byte-bounded LRU of manifest parquet payloads on local disk, keyed by manifest URI."""

    def __init__(self, directory: str, max_bytes: int):
        super().__init__(directory, max_bytes, _is_parquet, "manifest", _metrics)


class RemoteManifestCache:
    """Manifest payloads in a shared KV store, keyed by a digest of the manifest URI.

    Unlike the disk tier, this is reached over the network and shared by every
    instance pointed at it, which changes two decisions:

    * A corrupt entry is a **miss**, not an exception. On disk, a malformed payload
      means *this instance's* disk is damaged and should say so loudly. In a shared
      store it means one bad write is being served to everyone, and raising would
      take down every reader; we drop it and re-fetch from origin instead.
    * Payloads above `max_value_bytes` are not written. Manifest size scales with a
      dataset's file count, and pushing a very large one over the wire on every miss
      can cost more than the object-storage read it is meant to save.

    Store failures never surface: the KV backends already degrade to returning None
    (`MAX_CONSECUTIVE_CACHE_FAILURES`), and anything they do raise is swallowed here.
    A cache outage must slow queries down, never fail them.
    """

    def __init__(self, store, max_value_bytes: int):
        self._store = store
        self._max_value_bytes = max_value_bytes

    @staticmethod
    def _key(uri: str) -> bytes:
        # Digest, not the raw URI: bounded key length, and no separator collisions
        # between a bucket path and the store's own key namespacing.
        return b"manifest/" + sha256(uri.encode("utf-8")).hexdigest().encode("ascii")

    def get(self, uri: str) -> Optional[bytes]:
        try:
            payload = self._store.get(self._key(uri))
        except Exception:  # pragma: no cover - backend-specific failure modes
            return None

        if not payload:
            _metrics["remote_misses"] += 1
            return None

        if not _is_parquet(payload):
            _metrics["remote_corrupt"] += 1
            return None

        _metrics["remote_hits"] += 1
        _metrics["remote_bytes_served"] += len(payload)
        return bytes(payload)

    def put(self, uri: str, payload: bytes) -> None:
        if len(payload) > self._max_value_bytes:
            _metrics["remote_oversize"] += 1
            return

        try:
            self._store.set(self._key(uri), payload)
        except Exception:  # pragma: no cover - backend-specific failure modes
            return

        _metrics["remote_writes"] += 1


_shared_cache: Optional[ManifestDiskCache] = None
_shared_cache_lock = threading.Lock()

_remote_cache: Optional[RemoteManifestCache] = None
_remote_cache_built = False
_remote_cache_lock = threading.Lock()


def shared_cache() -> Optional[ManifestDiskCache]:
    """The process-wide manifest disk cache, or None when no directory is configured."""
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


def remote_cache() -> Optional[RemoteManifestCache]:
    """The process-wide remote manifest cache, or None when no KV store is configured.

    `enforce_context_fields=()` opts out of the factory's default query/operator key
    scoping. That default is right for spill, where one query must never read
    another's bytes; here the cross-query hit *is* the feature, and the key already
    identifies the payload immutably.

    A store that cannot be constructed (missing client library, unparseable location)
    disables the tier rather than failing the process — a cache is an optimisation,
    and a deployment must not lose the ability to serve queries by misconfiguring one.
    """
    global _remote_cache, _remote_cache_built

    from opteryx import config

    with _remote_cache_lock:
        if _remote_cache_built:
            return _remote_cache
        _remote_cache_built = True

        if not config.MANIFEST_REMOTE_LOCATION:
            return None

        try:
            from opteryx.managers.kvstores import create_kv_store

            store = create_kv_store(
                config.MANIFEST_REMOTE_LOCATION, enforce_context_fields=()
            )
        except Exception as err:  # pragma: no cover - deployment misconfiguration
            from opteryx import logger

            logger.warning(f"Remote manifest cache disabled; KV store unavailable ({err}).")
            return None

        if store is None:
            return None

        _remote_cache = RemoteManifestCache(
            store, max_value_bytes=config.MANIFEST_REMOTE_MAX_VALUE_BYTES
        )
        return _remote_cache


def manifest_cache_tiers() -> List[object]:
    """Configured manifest cache tiers, fastest first. Empty when none are configured."""
    return [tier for tier in (shared_cache(), remote_cache()) if tier is not None]


class CachingFileIO:
    """Wraps a catalog ``FileIO``, serving manifest reads from a tier stack.

    Tiers are read fastest-first. A hit is promoted into every tier ahead of the one
    that served it, so the next read on this instance is local. A full miss reads
    from the wrapped ``FileIO`` and populates every tier.

    Every other operation delegates untouched, but the delegation is an EXPLICIT
    enumeration, not a `__getattr__` passthrough: what this wrapper forwards is
    stated here, where it can be read, rather than being whatever the inner
    object happens to have. The cost is that a method added to the catalog's
    `FileIO` and not added here becomes an `AttributeError` at the call site
    rather than working - which is how `copy` was missed. The test suite holds a
    drift guard comparing this surface to the catalog's for that reason; the
    answer to a failure there is a delegator here, never a passthrough.
    """

    def __init__(self, inner, tiers):
        # A bare cache object is accepted so the single-tier call site reads naturally.
        self._inner = inner
        self._tiers = list(tiers) if isinstance(tiers, (list, tuple)) else [tiers]

    def _populate(self, tiers, location: str, payload: bytes) -> None:
        for tier in tiers:
            tier.put(location, payload)

    def new_input(self, location: str):
        from opteryx_catalog.iops.fileio import InputFile

        if not is_manifest_uri(location):
            _metrics["bypassed"] += 1
            return self._inner.new_input(location)

        for index, tier in enumerate(self._tiers):
            payload = tier.get(location)
            if payload is not None:
                # Backfill the faster tiers this read skipped past.
                self._populate(self._tiers[:index], location, payload)
                return InputFile(location, payload)

        # The catalog's GcsFileIO.new_input reads the object eagerly; going through
        # open() keeps us on the public contract rather than its private buffer.
        with self._inner.new_input(location).open() as handle:
            payload = handle.read()

        self._populate(self._tiers, location, payload)
        return InputFile(location, payload)

    def new_output(self, location: str):
        return self._inner.new_output(location)

    def delete(self, location: str) -> None:
        return self._inner.delete(location)

    def exists(self, location: str) -> bool:
        return self._inner.exists(location)

    def copy(self, source: str, destination: str) -> None:
        # Server-side where the backend has it, so the bytes never come through
        # this process. Nothing about a copy touches the manifest cache: neither
        # end of it is read or written here, and a manifest URI names a
        # write-once object that is never a copy's destination.
        return self._inner.copy(source, destination)

    def list_files(self, prefix: str) -> list:
        return self._inner.list_files(prefix)

    ls = list_files

    # GCS only (the catalog's GcsFileIO): the vector index build streams its index file
    # into a resumable session the control plane opens. None of it is a manifest.
    def open_upload_session(self, location: str) -> str:
        return self._inner.open_upload_session(location)

    def cancel_upload_session(self, session_uri: str) -> None:
        return self._inner.cancel_upload_session(session_uri)
