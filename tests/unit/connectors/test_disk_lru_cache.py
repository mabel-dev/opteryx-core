"""DiskLRUCache: byte budget, least-recently-READ eviction, and recency that survives a restart."""

import hashlib
import os

import pytest

from opteryx.connectors import disk_lru_cache
from opteryx.connectors.disk_lru_cache import CorruptCacheEntry
from opteryx.connectors.disk_lru_cache import DiskLRUCache


def _valid(payload):
    return payload.startswith(b"OK")


def _cache(path, max_bytes=30):
    metrics = {k: 0 for k in ("hits", "misses", "writes", "evictions", "bytes_served", "bytes_written")}
    return DiskLRUCache(str(path), max_bytes, _valid, "test", metrics)


def test_eviction_is_by_last_read_not_by_write(tmp_path):
    cache = _cache(tmp_path)
    cache.put("a", b"OK" + b"a" * 8)
    cache.put("b", b"OK" + b"b" * 8)
    cache.put("c", b"OK" + b"c" * 8)
    assert cache.get("a") is not None  # a is now the most recently read

    cache.put("d", b"OK" + b"d" * 8)  # over budget: b is the least recently read

    assert cache.get("b") is None
    assert cache.get("a") is not None
    assert cache.get("c") is not None
    assert cache.get("d") is not None


def test_read_recency_survives_a_restart(tmp_path, monkeypatch):
    monkeypatch.setattr(disk_lru_cache, "_TOUCH_INTERVAL_S", 0.0)
    cache = _cache(tmp_path)
    cache.put("a", b"OK" + b"a" * 8)
    cache.put("b", b"OK" + b"b" * 8)
    cache.put("c", b"OK" + b"c" * 8)
    # Write order a, b, c; force distinct, ordered mtimes, then read a so it is newest.
    for i, key in enumerate(("a", "b", "c")):
        path = cache._path(hashlib.sha256(key.encode()).hexdigest())
        os.utime(path, (1000 + i, 1000 + i))
        cache._index[os.path.basename(path)][1] = 1000 + i
    assert cache.get("a") is not None

    restarted = _cache(tmp_path)
    restarted.put("d", b"OK" + b"d" * 8)

    assert restarted.get("b") is None, "b was least recently read before the restart"
    assert restarted.get("a") is not None


def test_budget_is_enforced_on_adoption(tmp_path):
    cache = _cache(tmp_path, max_bytes=100)
    for key in ("a", "b", "c"):
        cache.put(key, b"OK" + key.encode() * 8)

    smaller = _cache(tmp_path, max_bytes=20)

    assert sum(1 for k in ("a", "b", "c") if smaller.get(k) is not None) == 2


def test_corrupt_entry_raises(tmp_path):
    cache = _cache(tmp_path)
    cache.put("a", b"OK" + b"a" * 8)
    path = cache._path(hashlib.sha256(b"a").hexdigest())
    with open(path, "wb") as handle:
        handle.write(b"broken")

    with pytest.raises(CorruptCacheEntry):
        cache.get("a")
