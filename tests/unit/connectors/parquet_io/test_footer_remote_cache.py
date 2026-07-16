"""Remote footer cache: batched probe/write-back, the immutability gate, and degradation.

The tier's licence is that a data-file path names one footer forever, so a hit can never be
stale. These tests pin the behaviour that keeps it a correct *accelerator*: what it serves,
what it refuses (corrupt, oversized), that N files cost a bounded number of round trips (not
one giant one, not N), and that a broken store slows things down rather than breaking them.
"""

import pytest

from opteryx.connectors.parquet_io.footer_remote_cache import RemoteFooterCache
from opteryx.connectors.parquet_io.footer_remote_cache import _BATCH_CHUNK
from opteryx.connectors.parquet_io.footer_remote_cache import _is_footer_envelope
from opteryx.connectors.parquet_io.footer_remote_cache import get_footer_cache_metrics
from opteryx.managers.kvstores.base_kv_store import BaseKeyValueStore

F1 = "gs://bucket/ds/data/part-0001.parquet"
F2 = "gs://bucket/ds/data/part-0002.parquet"
F3 = "gs://bucket/ds/data/part-0003.parquet"

# A footer envelope is the file tail: it ends with the PAR1 magic.
ENV1 = b"footer-one\x00\x11PAR1"
ENV2 = b"footer-two-longer\x22PAR1"


class LoopStore(BaseKeyValueStore):
    """Only get/set overridden — exercises the base-class get_many/set_many default loop."""

    def __init__(self):
        super().__init__(location=None)
        self.data = {}
        self.gets = 0
        self.sets = 0

    def get(self, key):
        self.gets += 1
        return self.data.get(bytes(key))

    def set(self, key, value):
        self.sets += 1
        self.data[bytes(key)] = bytes(value)


class BatchStore(BaseKeyValueStore):
    """Native multi-get/-set, counting round trips so we can prove chunking bounds them."""

    def __init__(self, failing=False):
        super().__init__(location=None)
        self.data = {}
        self.failing = failing
        self.mget_calls = 0
        self.mset_calls = 0

    def get(self, key):  # pragma: no cover - batch path is what we test here
        return self.data.get(bytes(key))

    def set(self, key, value):  # pragma: no cover
        self.data[bytes(key)] = bytes(value)

    def get_many(self, keys):
        self.mget_calls += 1
        if self.failing:
            raise ConnectionError("valkey is down")
        out = {}
        for k in keys:
            v = self.data.get(bytes(k))
            if v is not None:
                out[bytes(k)] = v
        return out

    def set_many(self, items):
        self.mset_calls += 1
        if self.failing:
            raise ConnectionError("valkey is down")
        for k, v in items.items():
            self.data[bytes(k)] = bytes(v)


def _fc(store, max_value_bytes=1024):
    return RemoteFooterCache(store, max_value_bytes=max_value_bytes)


def test_envelope_magic_guard():
    assert _is_footer_envelope(ENV1)
    assert not _is_footer_envelope(b"PAR1")  # too short
    assert not _is_footer_envelope(b"xxxxGARB")  # wrong trailing magic


def test_put_then_batched_get_roundtrips_with_a_miss():
    fc = _fc(BatchStore())
    fc.put_many([(F1, ENV1), (F2, ENV2)])
    assert fc.get_many([F1, F2, F3]) == {F1: ENV1, F2: ENV2}


def test_get_many_uses_base_default_loop_when_no_native_multiget():
    store = LoopStore()
    fc = _fc(store)
    fc.put(F1, ENV1)  # put -> put_many -> set_many default loop -> set
    assert store.sets == 1
    assert fc.get_many([F1, F2]) == {F1: ENV1}
    assert store.gets == 2  # default loop issues one get per key


def test_corrupt_entry_is_a_miss_not_served_and_not_counted_as_plain_miss():
    store = BatchStore()
    fc = _fc(store)
    store.data[RemoteFooterCache._key(F1)] = b"not-a-footer"  # passes no magic check
    before = get_footer_cache_metrics()
    assert fc.get_many([F1]) == {}
    after = get_footer_cache_metrics()
    assert after["corrupt"] - before["corrupt"] == 1
    assert after["misses"] - before["misses"] == 0  # disjoint from misses


def test_oversized_footer_is_not_written():
    store = BatchStore()
    fc = _fc(store, max_value_bytes=len(ENV1) - 1)
    fc.put(F1, ENV1)
    assert RemoteFooterCache._key(F1) not in store.data


def test_distinct_paths_do_not_collide():
    fc = _fc(BatchStore())
    fc.put_many([(F1, ENV1), (F2, ENV2)])
    got = fc.get_many([F1, F2])
    assert got[F1] == ENV1 and got[F2] == ENV2


def test_keys_are_stable_and_path_scoped():
    assert RemoteFooterCache._key(F1) == RemoteFooterCache._key(F1)
    assert RemoteFooterCache._key(F1).startswith(b"pqfooter/")
    assert RemoteFooterCache._key(F1) != RemoteFooterCache._key(F2)


def test_batched_reads_and_writes_are_bounded_not_one_trip_per_key():
    # The whole reason the tier chunks: a cold ~900-file scan must not be one oversized
    # round trip (timeout risk) nor N tiny ones. n keys -> ceil(n / _BATCH_CHUNK) trips.
    n = _BATCH_CHUNK * 2 + 5  # spans three chunks
    paths = [f"gs://bucket/ds/data/part-{i:05d}.parquet" for i in range(n)]
    store = BatchStore()
    fc = _fc(store)

    fc.put_many([(p, ENV1) for p in paths])
    assert store.mset_calls == 3, "writes must be chunked, not one giant MSET"

    got = fc.get_many(paths)
    assert len(got) == n
    assert store.mget_calls == 3, "reads must be chunked, not one giant MGET"


def test_duplicate_paths_are_deduped_before_the_probe():
    store = BatchStore()
    fc = _fc(store)
    fc.put(F1, ENV1)
    # 300 duplicate references to one file must not blow into 300 keys / extra chunks.
    assert fc.get_many([F1] * 300) == {F1: ENV1}
    assert store.mget_calls == 1


def test_store_outage_degrades_to_empty_rather_than_raising():
    store = BatchStore(failing=True)
    fc = _fc(store)
    # A get that raises inside the chunk must surface as all-miss, never propagate.
    assert fc.get_many([F1, F2]) == {}
    # A failing write must be swallowed too.
    fc.put_many([(F1, ENV1)])  # must not raise


if __name__ == "__main__":  # pragma: no cover
    pytest.main([__file__, "-v"])
