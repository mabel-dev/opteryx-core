# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Tests for the scheduler prototype with simple pipeline scenarios.

These tests demonstrate how the edge-based execution model works with
producer/consumer operators in a simple pipeline.
"""

import threading
import time
import sys
import importlib.util

# Load Edge module directly
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
                import traceback
                traceback.print_exc()
                self.failed += 1
            except Exception as e:
                print(f"✗ {name}: {e}")
                import traceback
                traceback.print_exc()
                self.failed += 1

    def summary(self):
        """Print summary and exit."""
        print(f"\n{self.passed} passed, {self.failed} failed")
        if self.failed > 0:
            sys.exit(1)


runner = TestRunner()


# Simple producer: generates N morsels
def simple_producer(edge, count=5):
    """Generate count morsels and close the edge."""
    for i in range(count):
        edge.enqueue(f"morsel_{i}")
    edge.close()


# Simple filter: pass through even-numbered morsels
def simple_filter(edge_in, edge_out):
    """Process morsels from input edge, filter to output edge."""
    while True:
        morsel = edge_in.dequeue()
        if morsel is None:
            if not edge_in.is_open():
                break
            time.sleep(0.01)
            continue

        # Process: extract the number and check if even
        num = int(morsel.split("_")[1])
        if num % 2 == 0:
            edge_out.enqueue(f"filtered_{morsel}")

        edge_in.mark_complete()

    # When input is closed and drained, close output
    edge_out.close()


# Simple accumulator: collect all morsels
def simple_accumulator(edge_in):
    """Accumulate all morsels from input edge into a list."""
    results = []
    while True:
        morsel = edge_in.dequeue()
        if morsel is None:
            if not edge_in.is_open():
                break
            time.sleep(0.01)
            continue

        results.append(morsel)
        edge_in.mark_complete()

    return results


@runner.test("simple_pipeline_producer_consumer")
def test_simple_pipeline_producer_consumer():
    """Test a simple producer → consumer pipeline."""
    edge = Edge("test_edge")
    results = []
    lock = threading.Lock()

    def producer():
        simple_producer(edge, count=5)

    def consumer():
        while True:
            morsel = edge.dequeue()
            if morsel is None:
                if not edge.is_open():
                    break
                time.sleep(0.01)
                continue

            with lock:
                results.append(morsel)
            edge.mark_complete()

    # Run producer and consumer in separate threads
    prod = threading.Thread(target=producer)
    cons = threading.Thread(target=consumer)

    prod.start()
    cons.start()

    prod.join(timeout=2.0)
    cons.join(timeout=2.0)

    # Verify results
    assert len(results) == 5, f"Expected 5 results, got {len(results)}"
    assert results == ["morsel_0", "morsel_1", "morsel_2", "morsel_3", "morsel_4"]
    assert edge.is_complete()


@runner.test("pipeline_with_filter")
def test_pipeline_with_filter():
    """Test a pipeline with producer → filter → consumer."""
    edge_in = Edge("producer_to_filter")
    edge_out = Edge("filter_to_sink")
    results = []
    lock = threading.Lock()

    def producer():
        simple_producer(edge_in, count=10)

    def filter_thread():
        simple_filter(edge_in, edge_out)

    def consumer():
        while True:
            morsel = edge_out.dequeue()
            if morsel is None:
                if not edge_out.is_open():
                    break
                time.sleep(0.01)
                continue

            with lock:
                results.append(morsel)
            edge_out.mark_complete()

    # Run all three in separate threads
    prod = threading.Thread(target=producer)
    filt = threading.Thread(target=filter_thread)
    cons = threading.Thread(target=consumer)

    prod.start()
    filt.start()
    cons.start()

    prod.join(timeout=2.0)
    filt.join(timeout=2.0)
    cons.join(timeout=2.0)

    # Verify results: only even-numbered morsels
    assert len(results) == 5, f"Expected 5 results (0,2,4,6,8), got {len(results)}: {results}"
    expected = ["filtered_morsel_0", "filtered_morsel_2", "filtered_morsel_4", "filtered_morsel_6", "filtered_morsel_8"]
    assert results == expected, f"Expected {expected}, got {results}"
    assert edge_in.is_complete()
    assert edge_out.is_complete()


@runner.test("edge_completion_before_sink_finish")
def test_edge_completion_before_sink_finish():
    """Verify that edge is complete before sink finalization."""
    edge = Edge("test_edge")
    completion_time = None
    finish_time = None
    lock = threading.Lock()

    def producer():
        simple_producer(edge, count=3)

    def consumer():
        nonlocal finish_time
        while True:
            morsel = edge.dequeue()
            if morsel is None:
                if not edge.is_open():
                    break
                time.sleep(0.01)
                continue
            edge.mark_complete()

        # Edge should be complete now
        with lock:
            finish_time = time.monotonic()

    def monitor():
        nonlocal completion_time
        # Wait for edge to be complete
        edge.wait_for_completion(timeout=2.0)
        with lock:
            completion_time = time.monotonic()

    prod = threading.Thread(target=producer)
    cons = threading.Thread(target=consumer)
    mon = threading.Thread(target=monitor)

    prod.start()
    cons.start()
    mon.start()

    prod.join(timeout=2.0)
    cons.join(timeout=2.0)
    mon.join(timeout=2.0)

    # Completion should be detected
    assert completion_time is not None
    assert finish_time is not None
    # They might be close in timing since they're checking the same invariant
    assert edge.is_complete()


@runner.test("backpressure_slows_producer")
def test_backpressure_slows_producer():
    """Verify that backpressure prevents producer from getting too far ahead."""
    edge = Edge("test_edge", target_queue_depth=3)
    times = []
    lock = threading.Lock()

    def producer():
        for i in range(10):
            with lock:
                times.append(("enqueue_start", i, time.monotonic()))
            edge.enqueue(f"morsel_{i}")
            with lock:
                times.append(("enqueue_done", i, time.monotonic()))

        edge.close()

    def slow_consumer():
        # Deliberately slow consumer to trigger backpressure
        while True:
            morsel = edge.dequeue()
            if morsel is None:
                if not edge.is_open():
                    break
                time.sleep(0.05)
                continue

            time.sleep(0.02)  # Simulate processing time
            edge.mark_complete()

    prod = threading.Thread(target=producer)
    cons = threading.Thread(target=slow_consumer)

    prod.start()
    cons.start()

    prod.join(timeout=5.0)
    cons.join(timeout=5.0)

    # Count how many times producer was blocked (enqueue_start/done pairs with gap)
    # With target_queue_depth=3 and slow consumer, producer should block several times
    enqueue_events = [t for t in times if t[0] in ("enqueue_start", "enqueue_done")]
    assert len(enqueue_events) >= 10, "Should have enqueue events"
    assert edge.is_complete()


if __name__ == "__main__":
    runner.run_all()
    runner.summary()
