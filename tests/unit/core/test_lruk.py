"""LRU_K is backed by the C++ LRU2 (src/cpp/lru2.hpp): K=2 is fixed (the `k`
constructor argument is stored and ignored), with this eviction policy:

  - entries with fewer than 2 accesses ("young") are evicted before any entry with
    2+ accesses ("mature"), oldest first;
  - among mature entries, the one whose 2nd-most-recent access is oldest goes first.

A `set` is an access. `evict(True)` returns (key, value), `evict(False)` returns
(key, None), and (None, None) when empty.
"""

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

from opteryx.compiled.structures.lru_k import LRU_K
from tests import is_windows, skip_if


@skip_if(is_windows())
def test_lruk():
    lru = LRU_K()
    hits = misses = evictions = inserts = 0

    assert lru.stats == (hits, misses, evictions, inserts), lru.stats

    # a get from an empty cache is a miss
    assert lru.get(b"two") is None
    misses += 1
    assert lru.stats == (hits, misses, evictions, inserts), lru.stats

    # a set is an insert, not a hit or a miss
    lru.set(b"one", b"1", True)
    inserts += 1
    assert lru.stats == (hits, misses, evictions, inserts), lru.stats

    # a get of a stored key returns it and counts a hit; "one" now has 2 accesses
    assert lru.get(b"one") == b"1"
    hits += 1
    assert lru.stats == (hits, misses, evictions, inserts), lru.stats

    # "two" and "three" have one access each, so they go before the mature "one",
    # oldest first
    lru.set(b"two", b"2", True)
    lru.set(b"three", b"3", True)
    inserts += 2
    assert lru.evict(True) == (b"two", b"2")
    evictions += 1
    assert lru.stats == (hits, misses, evictions, inserts), lru.stats

    assert lru.get(b"two") is None
    misses += 1
    assert lru.stats == (hits, misses, evictions, inserts), lru.stats

    # "three" is still young, so it goes before "one" again
    assert lru.evict(True) == (b"three", b"3")
    evictions += 1
    assert lru.get(b"one") == b"1"
    hits += 1
    assert lru.stats == (hits, misses, evictions, inserts), lru.stats


def test_lru2():
    cache = LRU_K(k=2)

    cache.set(b"key1", b"value1", True)
    cache.set(b"key2", b"value2", True)

    # a get makes each mature: 2 accesses
    assert cache.get(b"key1") == b"value1"
    assert cache.get(b"key2") == b"value2"

    # a newcomer is young and goes first, however recently it arrived
    cache.set(b"key3", b"value3", True)
    assert cache.evict(True) == (b"key3", b"value3")
    assert cache.get(b"key3") is None

    # no young entries left: the mature entry with the oldest 2nd-most-recent
    # access goes. key1's accesses are (set, get) and key2's are later, so key1.
    assert cache.evict(True) == (b"key1", b"value1")
    assert cache.get(b"key1") is None
    assert cache.get(b"key2") == b"value2"

    # hits: key1, key2, key2 | misses: key3, key1 | evictions: 2 | inserts: 3
    assert cache.stats == (3, 2, 2, 3), cache.stats

    # reset empties the cache and keeps the counters; reset_stats clears them too
    cache.reset()
    assert len(cache) == 0
    assert cache.get(b"key2") is None
    assert cache.stats == (3, 3, 2, 3), cache.stats

    cache.set(b"key1", b"value1", True)
    cache.reset(reset_stats=True)
    assert cache.stats == (0, 0, 0, 0)


def test_lru_cache_eviction():
    lru = LRU_K()

    lru.set(b"a", b"1", True)
    lru.set(b"b", b"2", True)
    lru.set(b"c", b"3", True)

    # a get makes each mature
    assert lru.get(b"a") == b"1"
    assert lru.get(b"b") == b"2"
    assert lru.get(b"c") == b"3"

    # each newcomer is young, so it is the one evicted; the mature three stay
    for key, value in ((b"d", b"4"), (b"e", b"5"), (b"f", b"6")):
        lru.set(key, value, True)
        assert lru.evict(True) == (key, value)
        assert lru.get(key) is None
        assert lru.get(b"a") == b"1"
        assert lru.get(b"b") == b"2"
        assert lru.get(b"c") == b"3"

    # all mature now: "a" has the oldest 2nd-most-recent access
    assert lru.evict(True)[0] == b"a"


def test_get_non_existing_key_after_eviction():
    lru = LRU_K()

    # Add 3 items to the cache
    lru.set(b"a", b"1", False)
    lru.set(b"b", b"2", False)
    lru.set(b"c", b"3", False)

    # Add a fourth item to the cache, which should trigger an eviction of the least recently used item
    lru.set(b"d", b"4", False)
    lru.evict(False)

    # Try to get a non-existing key
    assert lru.get(b"e") is None


def test_overwrite_existing_key():
    lru = LRU_K()

    # Add 3 items to the cache
    lru.set(b"a", b"1", False)
    lru.set(b"b", b"2", False)
    lru.set(b"c", b"3", False)

    # Overwrite key "b"
    lru.set(b"b", b"20", False)

    # Check the new value of "b"
    assert lru.get(b"b") == b"20"


def test_evict_as_last_resort():
    """
    we only evict items with less than K accesses if we need to evict someting
    """
    lru = LRU_K()

    for item in (b"a", b"b", b"c", b"d", b"e", b"f"):
        lru.set(item, item, False)

    lru.evict(False)
    lru.evict(False)
    lru.evict(False)

    # Check the values
    assert lru.get(b"a") is None
    assert lru.get(b"b") is None
    assert lru.get(b"c") is None
    assert lru.get(b"d") == b"d"
    assert lru.get(b"e") == b"e"
    assert lru.get(b"f") == b"f"


def test_lru2_eviction_based_on_penultimate_access():
    lru = LRU_K(k=2)

    lru.set(b"a", b"1", True)
    lru.set(b"b", b"2", True)
    lru.set(b"c", b"3", True)

    # "a" and "b" are accessed twice more, "c" not at all
    lru.get(b"a")
    lru.get(b"b")
    lru.get(b"a")
    lru.get(b"b")

    lru.set(b"d", b"4", True)

    # young first, oldest first: "c" then "d"
    assert lru.evict(True) == (b"c", b"3")
    assert lru.evict(True) == (b"d", b"4")

    # then mature, by oldest 2nd-most-recent access: a's is older than b's
    assert lru.evict(True) == (b"a", b"1")
    assert lru.get(b"b") == b"2"


def test_young_entries_are_evicted_in_arrival_order():
    lru = LRU_K()

    for item in (b"a", b"b", b"c"):
        lru.set(item, item, True)
        if len(lru) > 2:
            lru.evict(True)

    # "b" reaches 2+ accesses and is protected from here on
    lru.get(b"b")
    lru.get(b"b")

    for item in (b"d", b"e", b"f"):
        lru.set(item, item, True)
        if len(lru) > 2:
            lru.evict(True)

    assert lru.get(b"a") is None
    assert lru.get(b"b") == b"b"
    assert lru.get(b"c") is None
    assert lru.get(b"d") is None
    assert lru.get(b"e") is None
    assert lru.get(b"f") == b"f"


@skip_if(is_windows())
def test_lruk_hits_do_not_grow_the_index():
    # A cache under budget never evicts; each hit used to leave a stale heap item
    # behind forever (the process-wide parquet footer cache hits once per file per query).
    lru = LRU_K()
    for i in range(100):
        lru.set(f"k{i}".encode(), b"v", False)
    for _ in range(1000):
        for i in range(100):
            assert lru.get(f"k{i}".encode()) == b"v"
    assert lru.size == 100
    assert lru.heap_items <= 2 * 100 + 64 + 1, lru.heap_items


@skip_if(is_windows())
def test_lruk_reinserted_key_is_not_evicted_by_its_stale_item():
    # Re-adding an erased key must start fresh: its old heap items (from before the
    # erase) must not make it the eviction victim ahead of an older young entry.
    lru = LRU_K()
    lru.set(b"a", b"1", False)
    lru.set(b"b", b"2", False)
    lru.get(b"a")
    lru.delete(b"a")
    lru.set(b"a", b"1", False)
    assert lru.evict(False) == (b"b", None)


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    import time
    start_time = time.perf_counter_ns()
    for i in range(1000):
        run_tests()
    end_time = time.perf_counter_ns()
    print(f"Tests completed in {(end_time - start_time) / 1e9}")
