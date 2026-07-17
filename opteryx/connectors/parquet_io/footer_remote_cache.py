"""
Shared (remote) cache of Parquet footer envelopes, in front of the per-file footer read.

A data file's footer must be fetched before any of its data can be read: it holds the
schema, the per-row-group column-chunk byte offsets, and the min/max/null statistics the
scan prunes on. That fetch is a serial object-storage range read on the critical path of
every cold file open — and on a deployment of many short-lived instances (Cloud Run), the
in-process footer cache is cold on almost every request, so those reads are paid again and
again for footers another instance already has.

This tier stores the raw footer **envelope bytes** (not a parsed structure) in a shared KV
store, keyed by a digest of the data-file path. A Parquet data file is write-once — a new
commit mints a new path — so a cached footer can never be stale and needs no invalidation.
On a hit, the bytes are reconstructed into `FileStats` through the *same* trusted native
parser (`ReadParquetMetadataFromBuffer`) a fresh fetch uses: there is no bespoke serializer
on the read path, so a cache hit cannot silently disagree with a cold read. The parse still
happens once per process (feeding the in-process `_PARSED_FOOTER_CACHE`); what this tier
removes is the network round trip, which dominates the cost.

The consumer (`pool_reader.open_ipc_source`) probes this tier for every uncached remote file
in **one** `get_many` call, pre-fills the in-process footer-bytes cache with the hits, and
lets the existing fetch loop skip them; genuine misses are fetched from origin and written
back here. That one call is a bounded number of round trips, not necessarily a single MGET —
see `_BATCH_CHUNK` for why a 900-file scan must not be one giant reply.

The in-process caches sit IN FRONT of this tier, so a warm process serves footers without
ever reaching it. That is correct — the fastest path wins — but it means this tier's hit rate
can only be observed across process boundaries, and that a scan reporting nothing from this
tier has NOT necessarily failed. `pool_reader`'s `footer_process_cache_hits` /
`footer_cache_hits` / `footer_cache_misses` telemetry exists to tell those states apart.

**Config:** the environment variable is `OPTERYX_FOOTER_CACHE_LOCATION`; `FOOTER_REMOTE_LOCATION`
(read below) is only its attribute name in `config.py`. The two names differ, `config.get()` does
no aliasing, and an unset location is a silent, legitimate "disabled" — so a deployment that sets
the attribute name as the env var disables this tier with no warning. It is independently
configured and disabled by default; it deliberately does NOT default from the manifest cache's
location, whose key population grows at a different rate (see the config docstring).

opteryx-core does not depend on any KV client library; the store is built through
`create_kv_store`, whose backends import their clients lazily. With no location configured,
`remote_footer_cache()` is None and the scan path is exactly what it was before.
"""

import threading
from hashlib import sha256
from typing import Dict
from typing import Iterable
from typing import List
from typing import Optional

_PARQUET_MAGIC = b"PAR1"

# A single MGET/MSET is one round trip regardless of key count, but the whole reply must
# arrive inside the KV client's operation timeout (0.5s for Valkey) and is assembled on
# the server's single thread. A cold ~900-file scan of ~64KB footers is ~60MB — enough to
# blow that timeout and self-disable the cache on exactly the workload it exists for. So we
# cap keys per round trip: ~128 x 64KB ~= 8MB per trip, ~8 trips for 900 files instead of
# one 60MB trip or 900 individual ones.
_BATCH_CHUNK = 128


def _chunks(seq: List, size: int):
    for start in range(0, len(seq), size):
        yield seq[start : start + size]


_metrics: Dict[str, int] = {
    "hits": 0,
    "misses": 0,
    "writes": 0,
    "corrupt": 0,
    "oversize": 0,
    "bytes_served": 0,
    "bytes_written": 0,
}


def get_footer_cache_metrics() -> Dict[str, int]:
    """Counters for the process-wide remote footer cache."""
    return dict(_metrics)


def _is_footer_envelope(payload: bytes) -> bool:
    """A footer envelope is the file's tail: it ends with the ``PAR1`` magic.

    This is the same cheap integrity guard the manifest cache uses, scoped to the tail
    (a footer envelope is only the end of the file, not the whole thing). A store entry
    that fails it is treated as a miss, not an error — one bad write must not be served
    to every reader, and re-fetching from origin is always correct.
    """
    return len(payload) >= 8 and payload[-4:] == _PARQUET_MAGIC


class RemoteFooterCache:
    """Parquet footer envelopes in a shared KV store, keyed by a digest of the file path.

    Mirrors the remote manifest cache's decisions, for the same reasons: a corrupt entry
    is a **miss** (raising would take down every reader of a shared store), payloads above
    `max_value_bytes` are not written (shipping a pathological footer can cost more than
    the read it saves), and store failures never surface (a cache outage slows queries,
    never fails them).
    """

    def __init__(self, store, max_value_bytes: int):
        self._store = store
        self._max_value_bytes = max_value_bytes

    @staticmethod
    def _key(path: str) -> bytes:
        # Digest, not the raw path: bounded key length, no separator collisions between a
        # bucket path and the store's own namespacing. Keyed by the canonical original
        # path — never a signed URL, whose token would make the key unstable per request.
        return b"pqfooter/" + sha256(path.encode("utf-8")).hexdigest().encode("ascii")

    def get_many(self, paths: Iterable[str]) -> Dict[str, bytes]:
        """Batched lookup for many files, returning ``{path: envelope}`` for hits.

        N files, a bounded number of round trips (see ``_BATCH_CHUNK``). A store without a
        native multi-get still works (the base class loops), just without the collapse. A
        chunk whose round trip fails is treated as all-miss; the remaining chunks still
        serve, so one slow shard degrades rather than blanks the whole probe.
        """
        unique = list(dict.fromkeys(paths))  # dedup, order-preserving
        if not unique:
            return {}

        out: Dict[str, bytes] = {}
        hits = 0
        corrupt = 0
        served = 0
        for chunk in _chunks(unique, _BATCH_CHUNK):
            key_to_path = {self._key(p): p for p in chunk}
            try:
                raw = self._store.get_many(list(key_to_path.keys()))
            except Exception:  # pragma: no cover - backend-specific failure modes
                continue
            for key, payload in raw.items():
                path = key_to_path.get(key)
                if path is None or not payload:
                    continue
                if not _is_footer_envelope(payload):
                    corrupt += 1
                    continue
                envelope = bytes(payload)
                out[path] = envelope
                hits += 1
                served += len(envelope)

        _metrics["hits"] += hits
        _metrics["corrupt"] += corrupt
        # Miss and corrupt are disjoint (a corrupt entry is not counted as a plain miss),
        # matching the manifest cache's accounting. One arithmetic path, no double count.
        _metrics["misses"] += len(unique) - hits - corrupt
        _metrics["bytes_served"] += served
        return out

    def put(self, path: str, envelope: bytes) -> None:
        """Cache one footer. A thin wrapper over ``put_many`` so writes share one path."""
        self.put_many([(path, envelope)])

    def put_many(self, pairs: Iterable) -> None:
        """Cache many ``(path, envelope)`` footers in a bounded number of round trips.

        Oversized envelopes are skipped individually (still fetched from origin, still
        cached in-process — only the remote write is dropped); a chunk whose write fails
        is dropped whole. Store failures never surface.
        """
        pairs = list(pairs)
        if not pairs:
            return

        for chunk in _chunks(pairs, _BATCH_CHUNK):
            batch: Dict[bytes, bytes] = {}
            written_bytes = 0
            for path, envelope in chunk:
                if len(envelope) > self._max_value_bytes:
                    _metrics["oversize"] += 1
                    continue
                data = bytes(envelope)
                batch[self._key(path)] = data
                written_bytes += len(data)
            if not batch:
                continue
            try:
                self._store.set_many(batch)
            except Exception:  # pragma: no cover - backend-specific failure modes
                continue
            _metrics["writes"] += len(batch)
            _metrics["bytes_written"] += written_bytes


_remote_cache: Optional[RemoteFooterCache] = None
_remote_cache_built = False
_remote_cache_lock = threading.Lock()


def remote_footer_cache() -> Optional[RemoteFooterCache]:
    """The process-wide remote footer cache, or None when no KV store is configured.

    Configured by the `OPTERYX_FOOTER_CACHE_LOCATION` environment variable — NOT by
    `FOOTER_REMOTE_LOCATION`, which is only the name of the attribute holding its value.
    An unset location returns None, which is a legitimate disabled state and so is
    deliberately silent; the cost is that a deployment which sets the wrong name gets no
    warning here. Diagnose that from the scan's telemetry (a cold scan reporting misses
    with zero hits), not from this function.

    `enforce_context_fields=()` opts out of the factory's default query/operator key
    scoping: the cross-query, cross-instance hit is the whole feature, and the key already
    identifies the payload immutably. A store that cannot be constructed (missing client
    library, unparseable location) disables the tier rather than failing the process — that
    case DOES warn, and is the one failure mode visible from the logs alone.
    """
    global _remote_cache, _remote_cache_built

    # Lock-free fast path: `_remote_cache_built` latches True permanently and is set only
    # AFTER `_remote_cache` is finalised (below), so a reader that sees it True also sees a
    # fully-built (or definitively-None) cache — never a half-built one.
    if _remote_cache_built:
        return _remote_cache

    from opteryx import config

    with _remote_cache_lock:
        if _remote_cache_built:
            return _remote_cache

        cache = None
        if config.FOOTER_REMOTE_LOCATION:
            store = None
            try:
                from opteryx.managers.kvstores import create_kv_store

                store = create_kv_store(
                    config.FOOTER_REMOTE_LOCATION, enforce_context_fields=()
                )
            except Exception as err:  # pragma: no cover - deployment misconfiguration
                from opteryx import logger

                logger.warning(
                    f"Remote footer cache disabled; KV store unavailable ({err})."
                )
            if store is not None:
                cache = RemoteFooterCache(
                    store, max_value_bytes=config.FOOTER_REMOTE_MAX_VALUE_BYTES
                )

        _remote_cache = cache
        _remote_cache_built = True  # set LAST — guards the fast path above
        return _remote_cache
