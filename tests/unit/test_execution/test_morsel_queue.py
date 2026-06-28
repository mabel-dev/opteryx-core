"""
MorselQueue (opteryx/compiled/morsel_queue.pyx) — isolation tests.

Slice 1 of the native execution scheduler rewrite. The queue carries the C++
carrier `shared_ptr[CxxMorsel]`; no PyObject sits on it. These tests exercise the
Python test edge (`PyMorselQueue`) which round-trips draken Morsels through the
native queue: FIFO order, bounded backpressure across real threads, and
close()/drain semantics.

See docs/NATIVE_SCHEDULER_REWRITE_DESIGN.md §4.
"""
import os
import sys
import threading
import time

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

import draken.draken_native as dn
from draken.interop.vector_sequence import vector_from_sequence
from draken.morsels.morsel import Morsel

from opteryx.compiled.morsel_queue import PyMorselQueue

DT = dn.DrakenType


def _morsel(tag: int) -> Morsel:
    """A one-column, one-row morsel whose single value is `tag` (its identity)."""
    vs = [vector_from_sequence([tag], dtype=DT.INT64)]
    return Morsel.from_vectors([b"id"], vs)


def _tag_of(m: Morsel) -> int:
    return m._cxx_column(m.column_names[0]).to_pylist()[0]


def test_finish_drains_all_then_signals():
    """finish() is graceful: every queued morsel is delivered BEFORE the FINISHED
    sentinel — no data loss (contrast close(), which drops the remainder)."""
    from opteryx.compiled.morsel_queue import MQ_FINISHED

    q = PyMorselQueue(8)
    for i in range(5):
        assert q.put(_morsel(i)) is True
    assert q.finish() is True
    got = []
    while True:
        item = q.get()
        if item is MQ_FINISHED:
            break
        assert item is not None, "must not abandon — finish() drops nothing"
        got.append(_tag_of(item))
    assert got == [0, 1, 2, 3, 4]


def test_finish_vs_close_distinct():
    """close() abandons (get -> None, remainder dropped); finish() is graceful."""
    from opteryx.compiled.morsel_queue import MQ_FINISHED

    qa = PyMorselQueue(8)
    qa.put(_morsel(1))
    qa.close()
    assert qa.get() is None                      # abandoned: dropped + None

    qf = PyMorselQueue(8)
    qf.put(_morsel(1))
    qf.finish()
    assert _tag_of(qf.get()) == 1                # delivered first
    assert qf.get() is MQ_FINISHED               # then the sentinel


def test_fifo_round_trip():
    """put then get returns morsels in order, value-identical."""
    q = PyMorselQueue(8)
    for i in range(5):
        assert q.put(_morsel(i)) is True
    got = [_tag_of(q.get()) for _ in range(5)]
    assert got == [0, 1, 2, 3, 4]


def test_capacity_reported():
    assert PyMorselQueue(3).capacity == 3
    assert PyMorselQueue().capacity == 8  # default


def test_bounded_backpressure_blocks_producer():
    """A full queue blocks put() until the consumer drains a slot.

    Note: moodycamel FIFO is per-producer-token, so once a second thread enqueues
    we assert the multiset of received tags, not a cross-thread order.
    """
    q = PyMorselQueue(2)
    assert q.put(_morsel(0)) is True
    assert q.put(_morsel(1)) is True  # now full

    done = threading.Event()

    def producer():
        q.put(_morsel(2))  # must block until a slot frees
        done.set()

    t = threading.Thread(target=producer)
    t.start()
    # The third put cannot complete while the queue is full.
    assert not done.wait(timeout=0.2)
    # Free a slot; the blocked producer now completes.
    first = _tag_of(q.get())
    assert done.wait(timeout=2.0)
    t.join()
    rest = [_tag_of(q.get()), _tag_of(q.get())]
    assert sorted([first] + rest) == [0, 1, 2]


def test_close_unblocks_producer_and_returns_false():
    """close() on a full queue wakes a blocked producer, which returns False."""
    q = PyMorselQueue(1)
    assert q.put(_morsel(0)) is True  # full

    result = {}

    def producer():
        result["ok"] = q.put(_morsel(1))  # blocked on the full queue

    t = threading.Thread(target=producer)
    t.start()
    time.sleep(0.1)  # let the producer reach the blocking wait
    q.close()
    t.join(timeout=2.0)
    assert not t.is_alive()
    assert result["ok"] is False
    assert q.is_closed is True


def test_get_returns_none_when_closed_and_drained():
    """After close(), queued morsels are dropped; get() reports exhaustion."""
    q = PyMorselQueue(8)
    q.put(_morsel(0))
    q.put(_morsel(1))
    q.close()
    # close() drains the queued morsels; nothing remains to hand out.
    assert q.get() is None


def test_concurrent_producers_single_consumer():
    """N threads each push M morsels; the consumer receives exactly N*M, FIFO
    within each producer's stream is irrelevant — we assert the multiset."""
    q = PyMorselQueue(4)
    nproducers, per = 4, 50
    expected = sorted(p * 1000 + i for p in range(nproducers) for i in range(per))

    def producer(p):
        for i in range(per):
            assert q.put(_morsel(p * 1000 + i)) is True

    threads = [threading.Thread(target=producer, args=(p,)) for p in range(nproducers)]
    for t in threads:
        t.start()

    got = []
    for _ in range(nproducers * per):
        m = q.get()
        assert m is not None
        got.append(_tag_of(m))

    for t in threads:
        t.join()
    assert sorted(got) == expected


if __name__ == "__main__":  # pragma: no cover
    test_fifo_round_trip()
    test_finish_drains_all_then_signals()
    test_finish_vs_close_distinct()
    test_capacity_reported()
    test_bounded_backpressure_blocks_producer()
    test_close_unblocks_producer_and_returns_false()
    test_get_returns_none_when_closed_and_drained()
    test_concurrent_producers_single_consumer()
    print("✓ MorselQueue isolation tests passed")
