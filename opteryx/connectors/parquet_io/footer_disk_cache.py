"""
Local-disk cache of Parquet footer envelopes, between the in-process footer caches and
the shared (remote) footer tier.

The in-process caches are lost whenever the process restarts — on the fleet, every
deploy — so the first scan of every table after a restart pays a serial object-storage
round trip per file just to learn its layout. This tier keeps the raw envelope bytes on
the instance's disk, keyed by the data file's ORIGINAL path (a signed URL's token would
make the key unstable), so a restarted process reads them back in sub-milliseconds.

A Parquet data file is write-once, so a cached footer can never be stale; the byte
budget is the only bound, evicting the least-recently-read footer first (see
`DiskLRUCache`). A hit is parsed by the same native parser a fresh fetch uses, so a disk
hit cannot disagree with a cold read.

Configured by `OPTERYX_FOOTER_CACHE_PATH` / `OPTERYX_FOOTER_CACHE_BYTES`. Empty path
disables the tier, and the scan path is exactly what it was without it.
"""

import threading
from typing import Dict
from typing import Optional

from opteryx.connectors.disk_lru_cache import DiskLRUCache
from opteryx.connectors.parquet_io.footer_remote_cache import _is_footer_envelope

_metrics: Dict[str, int] = {
    "hits": 0,
    "misses": 0,
    "writes": 0,
    "evictions": 0,
    "bytes_served": 0,
    "bytes_written": 0,
}


def get_footer_disk_cache_metrics() -> Dict[str, int]:
    """Counters for the process-wide footer disk cache."""
    return dict(_metrics)


class FooterDiskCache(DiskLRUCache):
    """Byte-bounded LRU of Parquet footer envelopes on local disk, keyed by data-file path."""

    def __init__(self, directory: str, max_bytes: int):
        super().__init__(directory, max_bytes, _is_footer_envelope, "footer", _metrics)


_shared_cache: Optional[FooterDiskCache] = None
_shared_cache_lock = threading.Lock()


def footer_disk_cache() -> Optional[FooterDiskCache]:
    """The process-wide footer disk cache, or None when no directory is configured."""
    global _shared_cache

    from opteryx import config

    if not config.FOOTER_CACHE_PATH:
        return None

    with _shared_cache_lock:
        if _shared_cache is None:
            _shared_cache = FooterDiskCache(
                directory=config.FOOTER_CACHE_PATH,
                max_bytes=config.FOOTER_CACHE_BYTES,
            )
    return _shared_cache
