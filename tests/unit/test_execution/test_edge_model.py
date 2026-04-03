# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Tests for the edge-based execution model.

Validates that the Edge class correctly tracks state and handles
concurrent producer/consumer operations.
"""

import threading
import time
import sys
import traceback
import importlib.util

# Load Edge module directly to avoid triggering full opteryx package init
spec = importlib.util.spec_from_file_location('edge', './opteryx/execution/edge.py')
edge_module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(edge_module)
Edge = edge_module.Edge


class TestRunner:
    """Simple test runner without pytest dependency."""

    def __init__(self):
        self.passed = 0
        self.failed = 0
        self.tests = []

    def test(self, name):
        """Decorator to register a test."""
        def decorator(func):
            self.tests.append((name, func))
            return func
        return decorator

    def run_all(self):
        """Run all registered tests."""
        for name, func in self.tests:
            try:
                func()
                print(f"✓ {name}")
                self.passed += 1
            except AssertionError as e:
                print(f"✗ {name}: {e}")
                traceback.print_exc()
                self.failed += 1
            except Exception as e:
                print(f"✗ {name}: {e}")
                traceback.print_exc()
                self.failed += 1

    def summary(self):
        """Print summary and exit."""
        print(f"\n{self.passed} passed, {self.failed} failed")
        if self.failed > 0:
            sys.exit(1)


runner = TestRunner()


@runner.test("initial_state")
def test_initial_state():
    """Edge starts open with no queued or in-flight items."""
    edge = Edge("test")
    assert edge.is_open()
    assert edge.total_in_transit() == 0
    assert edge.has_data() == False
    assert edge.is_complete() == False


@runner.test("enqueue_dequeue_cycle")
def test_enqueue_dequeue_cycle():
    """Test basic enqueue/dequeue/complete cycle."""
    edge = Edge("test")

    # Enqueue increments queued
    edge.enqueue("morsel1")
    assert edge.total_in_transit() == 1
    assert edge.has_data() == True

    # Dequeue moves to in_flight
    morsel = edge.dequeue()
    assert morsel == "morsel1"
    assert edge.has_data() == False
    assert edge.total_in_transit() == 1

    # mark_complete decrements in_flight
    edge.mark_complete()
    assert edge.total_in_transit() == 0


@runner.test("completion_invariant")
def test_completion_invariant():
    """Completion invariant only true when open=False, queued=0, in_flight=0."""
    edge = Edge("test")

    # Not complete: still open
    assert edge.is_complete() == False

    # Add data
    edge.enqueue("m1")
    assert edge.is_complete() == False

    # Close edge
    edge.close()
    assert edge.is_complete() == False  # Data still queued

    # Dequeue and complete
    morsel = edge.dequeue()
    assert edge.is_complete() == False  # In flight
    edge.mark_complete()
    assert edge.is_complete() == True  # NOW complete


@runner.test("backpressure_enqueue")
def test_backpressure_enqueue():
    """Enqueue blocks when total_in_transit reaches target_queue_depth."""
    edge = Edge("test", target_queue_depth=3)

    # Fill to target depth
    for i in range(3):
        edge.enqueue(f"m{i}")
    assert edge.total_in_transit() == 3

    # Next enqueue blocks (in another thread)
    blocked = threading.Event()
    unblocked = threading.Event()

    def try_enqueue():
        blocked.set()
        edge.enqueue("m3")  # Should block
        unblocked.set()

    thread = threading.Thread(target=try_enqueue)
    thread.daemon = True
    thread.start()

    # Give thread time to start
    time.sleep(0.05)
    assert blocked.is_set()
    assert not unblocked.is_set()

    # Dequeue to make space
    edge.dequeue()
    edge.mark_complete()

    # Thread should unblock
    thread.join(timeout=1.0)
    assert unblocked.is_set()


@runner.test("dequeue_returns_none_when_closed_and_empty")
def test_dequeue_returns_none_when_closed_and_empty():
    """Dequeue returns None when queue is empty (open or closed)."""
    edge = Edge("test")

    # Empty while open
    assert edge.dequeue() is None

    # Close and still empty
    edge.close()
    assert edge.dequeue() is None


@runner.test("concurrent_enqueue_dequeue")
def test_concurrent_enqueue_dequeue():
    """Multiple threads can safely enqueue and dequeue."""
    edge = Edge("test", target_queue_depth=100)
    enqueued = []
    dequeued = []
    lock = threading.Lock()

    def producer(n):
        for i in range(n):
            morsel = f"p{threading.current_thread().ident}_m{i}"
            edge.enqueue(morsel)
            with lock:
                enqueued.append(morsel)
        edge.close()

    def consumer():
        while True:
            morsel = edge.dequeue()
            if morsel is None:
                if not edge.is_open():
                    break
                time.sleep(0.01)
                continue
            edge.mark_complete()
            with lock:
                dequeued.append(morsel)

    # Start 2 producers
    prod_threads = [threading.Thread(target=producer, args=(10,)) for _ in range(2)]
    for t in prod_threads:
        t.start()

    # Start 2 consumers
    cons_threads = [threading.Thread(target=consumer) for _ in range(2)]
    for t in cons_threads:
        t.start()

    # Wait for producers
    for t in prod_threads:
        t.join(timeout=5.0)

    # Wait for consumers
    for t in cons_threads:
        t.join(timeout=5.0)

    # All enqueued should be dequeued
    assert len(enqueued) == 20, f"Expected 20 enqueued, got {len(enqueued)}"
    assert len(dequeued) == 20, f"Expected 20 dequeued, got {len(dequeued)}"
    assert set(enqueued) == set(dequeued)
    assert edge.is_complete()


@runner.test("wait_for_completion")
def test_wait_for_completion():
    """wait_for_completion blocks until completion invariant is met."""
    edge = Edge("test")

    # Add some data
    edge.enqueue("m1")
    edge.enqueue("m2")

    # Start thread that will complete the edge
    def complete_later():
        time.sleep(0.1)
        edge.close()
        morsel1 = edge.dequeue()
        morsel2 = edge.dequeue()
        edge.mark_complete()
        edge.mark_complete()

    thread = threading.Thread(target=complete_later)
    thread.daemon = True
    thread.start()

    # Wait should block then return True
    start = time.monotonic()
    result = edge.wait_for_completion(timeout=1.0)
    elapsed = time.monotonic() - start

    assert result == True
    assert elapsed >= 0.1  # At least the time to complete_later

    thread.join(timeout=1.0)


@runner.test("wait_for_completion_timeout")
def test_wait_for_completion_timeout():
    """wait_for_completion returns False if timeout expires."""
    edge = Edge("test")
    edge.enqueue("m1")

    # Wait with short timeout should time out (data still queued, not dequeued)
    start = time.monotonic()
    result = edge.wait_for_completion(timeout=0.1)
    elapsed = time.monotonic() - start

    assert result == False
    assert elapsed >= 0.1


@runner.test("get_state_snapshot")
def test_get_state_snapshot():
    """get_state returns consistent snapshot of current state."""
    edge = Edge("test")
    edge.enqueue("m1")
    edge.enqueue("m2")

    state = edge.get_state()

    assert state["name"] == "test"
    assert state["queued"] == 2
    assert state["in_flight"] == 0
    assert state["open"] == True
    assert state["total_in_transit"] == 2
    assert state["complete"] == False

    m1 = edge.dequeue()
    state2 = edge.get_state()

    assert state2["queued"] == 1
    assert state2["in_flight"] == 1
    assert state2["total_in_transit"] == 2


@runner.test("multiple_sequential_dequeues")
def test_multiple_sequential_dequeues():
    """Can dequeue multiple times from same edge."""
    edge = Edge("test")

    for i in range(5):
        edge.enqueue(f"m{i}")

    for i in range(5):
        m = edge.dequeue()
        assert m == f"m{i}"
        edge.mark_complete()

    assert edge.total_in_transit() == 0


if __name__ == "__main__":
    runner.run_all()
    runner.summary()
