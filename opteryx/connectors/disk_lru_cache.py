"""
Byte-bounded LRU of immutable payloads on local disk, shared by the metadata caches.

Planning-phase only. Every user caches payloads addressed by a key that can only ever
name one payload (a snapshot-addressed manifest URI, a write-once data file's path), so
an entry can never be stale and needs no invalidation or TTL. The budget is the only
bound: least-recently-READ entries are evicted first.

Recency survives the process. A hit stamps the entry's mtime (at most once per
`_TOUCH_INTERVAL_S`, so a hot entry is not a stream of metadata writes to a network
disk), and a new process seeds its LRU order from those mtimes.
"""

import os
import threading
import time
from collections import OrderedDict
from hashlib import sha256
from typing import Callable
from typing import Dict
from typing import List
from typing import Optional

# Recency only needs to be coarse: it orders eviction, it is not an access log.
_TOUCH_INTERVAL_S = 3600.0


class CorruptCacheEntry(Exception):
    """A cache entry exists but does not pass its cache's integrity check."""


class DiskLRUCache:
    """Byte-bounded LRU of payloads on local disk, evicting least-recently-read first.

    Entries are keyed by the SHA-256 of the full key string, so keys that share a
    suffix (snapshot ids are millisecond timestamps, not UUIDs) cannot collide.
    `metrics` is the caller's counter dict; this class increments `hits`, `misses`,
    `writes`, `evictions`, `bytes_served` and `bytes_written` in it.
    """

    def __init__(
        self,
        directory: str,
        max_bytes: int,
        is_valid: Callable[[bytes], bool],
        name: str,
        metrics: Dict[str, int],
    ):
        if max_bytes <= 0:
            raise ValueError(f"{name} cache max_bytes must be positive, got {max_bytes}")

        self._directory = directory
        self._max_bytes = max_bytes
        self._is_valid = is_valid
        self._name = name
        self._metrics = metrics
        self._lock = threading.Lock()

        # digest -> [size in bytes, last-read stamp], in LRU order (oldest first)
        self._index: "OrderedDict[str, List[float]]" = OrderedDict()
        self._resident = 0

        os.makedirs(directory, exist_ok=True)
        self._seed_index()

    def _seed_index(self) -> None:
        """Adopt entries left by an earlier process on this instance's disk.

        Ordered by mtime, which is each entry's last read (to `_TOUCH_INTERVAL_S`).
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

        for mtime, name, size in sorted(entries):
            self._index[name] = [size, mtime]
            self._resident += size

        self._evict_to_fit(0)

    def _path(self, digest: str) -> str:
        return os.path.join(self._directory, digest)

    def _evict_to_fit(self, incoming: int) -> None:
        """Drop least-recently-read entries until `incoming` more bytes fit.

        Caller holds the lock.
        """
        while self._index and self._resident + incoming > self._max_bytes:
            digest, (size, _) = self._index.popitem(last=False)
            os.unlink(self._path(digest))
            self._resident -= size
            self._metrics["evictions"] += 1

    def get(self, key: str) -> Optional[bytes]:
        """Return the cached payload for `key`, or None on a miss.

        A present-but-malformed entry raises rather than silently re-fetching:
        a cache that quietly repairs itself hides a broken disk.
        """
        digest = sha256(key.encode("utf-8")).hexdigest()

        with self._lock:
            entry = self._index.get(digest)
            if entry is None:
                self._metrics["misses"] += 1
                return None

            self._index.move_to_end(digest)
            path = self._path(digest)
            with open(path, "rb") as handle:
                payload = handle.read()

            now = time.time()
            if now - entry[1] >= _TOUCH_INTERVAL_S:
                os.utime(path, (now, now))
                entry[1] = now

        if not self._is_valid(payload):
            raise CorruptCacheEntry(
                f"{self._name} cache entry for {key} failed its integrity check "
                f"({len(payload)} bytes); disk at {self._directory} may be damaged"
            )

        self._metrics["hits"] += 1
        self._metrics["bytes_served"] += len(payload)
        return payload

    def put(self, key: str, payload: bytes) -> None:
        """Store `payload` under `key`, evicting LRU entries to stay in budget.

        A payload larger than the whole budget is not cached; caching it would
        evict every other entry to hold one item.
        """
        size = len(payload)
        if size > self._max_bytes:
            return

        digest = sha256(key.encode("utf-8")).hexdigest()
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

            self._index[digest] = [size, time.time()]
            self._resident += size
            self._metrics["writes"] += 1
            self._metrics["bytes_written"] += size
