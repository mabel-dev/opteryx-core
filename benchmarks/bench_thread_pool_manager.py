#!/usr/bin/env python
"""
Quick performance benchmark for thread_pool_manager improvements.

Measures:
1. Pool creation & caching efficiency
2. Task dispatch latency
3. Concurrent throughput
"""

import sys
import time
from concurrent.futures import ThreadPoolExecutor


# Benchmark pool creation overhead (cached vs non-cached)
def bench_pool_creation():
    """Measure pool creation and reuse efficiency."""
    from opteryx.connectors.parquet_io.thread_pool_manager import get_decode_pool
    from opteryx.connectors.parquet_io.thread_pool_manager import get_range_pool

    print("\n=== Pool Creation & Caching ===")

    # Measure first creation (slow path)
    start = time.monotonic_ns()
    pool1 = get_range_pool(name="bench", max_workers=32)
    first_create_ns = time.monotonic_ns() - start

    # Measure cache hit (fast path)
    start = time.monotonic_ns()
    pool2 = get_range_pool(name="bench", max_workers=32)
    cache_hit_ns = time.monotonic_ns() - start

    # Verify same pool
    assert pool1 is pool2, "Cache hit returned different pool"

    print(f"First pool creation: {first_create_ns/1000:.2f} µs")
    print(f"Cache hit (reuse):   {cache_hit_ns/1000:.2f} µs")
    print(f"Speedup:             {first_create_ns / cache_hit_ns:.1f}x")


def bench_task_dispatch_latency():
    """Measure per-task dispatch latency."""
    from opteryx.connectors.parquet_io.thread_pool_manager import get_range_pool

    print("\n=== Task Dispatch Latency ===")

    pool = get_range_pool(name="latency-bench", max_workers=8)

    def dummy_task(x):
        return x * 2

    # Warm up
    for _ in range(10):
        fut = pool.submit(dummy_task, 42)
        fut.result()

    # Measure dispatch latency (submit → task queue)
    latencies_ns = []
    for _ in range(100):
        start_ns = time.monotonic_ns()
        fut = pool.submit(dummy_task, 42)
        dispatch_ns = time.monotonic_ns() - start_ns
        latencies_ns.append(dispatch_ns)
        fut.result()  # wait for completion

    import statistics
    p50 = statistics.median(latencies_ns)
    p99 = sorted(latencies_ns)[int(0.99 * len(latencies_ns))]

    print(f"p50 dispatch latency: {p50/1000:.2f} µs")
    print(f"p99 dispatch latency: {p99/1000:.2f} µs")
    print(f"Max dispatch latency: {max(latencies_ns)/1000:.2f} µs")


def bench_concurrent_throughput():
    """Measure concurrent task throughput."""
    from opteryx.connectors.parquet_io.thread_pool_manager import get_range_pool

    print("\n=== Concurrent Throughput ===")

    pool = get_range_pool(name="throughput-bench", max_workers=32)

    def io_simulation(duration_ms=1):
        """Simulate I/O work."""
        start = time.monotonic()
        while time.monotonic() - start < duration_ms / 1000:
            pass
        return True

    # Submit 1000 concurrent tasks
    num_tasks = 1000
    start = time.monotonic_ns()
    futures = [pool.submit(io_simulation, 1) for _ in range(num_tasks)]
    submit_ns = time.monotonic_ns() - start

    # Wait for all to complete
    start = time.monotonic_ns()
    for fut in futures:
        fut.result()
    complete_ns = time.monotonic_ns() - start

    total_ns = submit_ns + complete_ns
    throughput = num_tasks / (total_ns / 1e9)

    print(f"Submitted {num_tasks} tasks in {submit_ns/1e6:.1f} ms")
    print(f"Completed {num_tasks} tasks in {complete_ns/1e6:.1f} ms")
    print(f"Total time: {total_ns/1e6:.1f} ms")
    print(f"Throughput: {throughput:.0f} tasks/sec")


def bench_lazy_pool_proxy():
    """Measure LazyPoolProxy overhead."""
    from opteryx.connectors.parquet_io.thread_pool_manager import LazyPoolProxy
    from opteryx.connectors.parquet_io.thread_pool_manager import get_range_pool

    print("\n=== LazyPoolProxy Overhead ===")

    def get_pool():
        return get_range_pool(name="proxy-bench", max_workers=16)

    proxy = LazyPoolProxy(get_pool)

    def dummy_task(x):
        return x

    # Warm up
    for _ in range(10):
        fut = proxy.submit(dummy_task, 42)
        fut.result()

    # Measure proxy submit latency
    latencies_ns = []
    for _ in range(100):
        start_ns = time.monotonic_ns()
        fut = proxy.submit(dummy_task, 42)
        latency_ns = time.monotonic_ns() - start_ns
        latencies_ns.append(latency_ns)
        fut.result()

    import statistics
    p50 = statistics.median(latencies_ns)

    print(f"LazyPoolProxy p50 submit: {p50/1000:.2f} µs")
    print("(Overhead should be <1 µs)")


if __name__ == "__main__":
    print("Thread Pool Manager Performance Benchmarks")
    print("=" * 50)

    try:
        bench_pool_creation()
        bench_task_dispatch_latency()
        bench_concurrent_throughput()
        bench_lazy_pool_proxy()

        print("\n" + "=" * 50)
        print("✓ All benchmarks completed successfully")
        sys.exit(0)
    except Exception as e:
        print(f"\n✗ Benchmark failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
