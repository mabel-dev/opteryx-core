"""
Comprehensive memory pool benchmark suite for performance comparison.

This benchmark tests various workload patterns and can be run against different
MemoryPool implementations to measure performance improvements.

Usage:
    python3 bench_memory_pool_comprehensive.py

The same benchmark can be run against old and new implementations by swapping
the import statement.
"""

import os
import sys
import time
import threading
import statistics
from typing import Callable, Tuple, List

# Make sure we can import opteryx
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../../'))

from opteryx.shared import MemoryPool
from orso.tools import random_string


class MemoryPoolBenchmark:
    """Benchmark suite for MemoryPool performance testing."""

    def __init__(self, name: str = "MemoryPool Benchmark"):
        self.name = name
        self.results = []

    def run_benchmark(
        self,
        test_name: str,
        test_func: Callable,
        iterations: int = 1,
        setup: Callable = None,
    ) -> dict:
        """Run a single benchmark test.

        Args:
            test_name: Name of the test
            test_func: Function that performs the test (should return ops/sec or similar)
            iterations: Number of times to run the test
            setup: Optional setup function called before each iteration

        Returns:
            Dictionary with timing statistics
        """
        times = []

        for i in range(iterations):
            if setup:
                setup()

            start = time.perf_counter()
            result = test_func()
            elapsed = time.perf_counter() - start
            times.append(elapsed)

        # Calculate statistics (in milliseconds)
        times_ms = [t * 1000 for t in times]
        stats = {
            'test': test_name,
            'iterations': iterations,
            'min_ms': min(times_ms),
            'max_ms': max(times_ms),
            'mean_ms': statistics.mean(times_ms),
            'median_ms': statistics.median(times_ms),
            'stdev_ms': statistics.stdev(times_ms) if len(times_ms) > 1 else 0,
        }

        self.results.append(stats)
        return stats

    def print_results(self):
        """Print all benchmark results in a formatted table."""
        if not self.results:
            print("No results to display")
            return

        print(f"\n{'='*80}")
        print(f"{self.name}")
        print(f"{'='*80}")
        print(f"{'Test Name':<40} {'Min':>10} {'Mean':>10} {'Median':>10} {'Max':>10}")
        print(f"{'-'*80}")

        for result in self.results:
            print(
                f"{result['test']:<40} "
                f"{result['min_ms']:>10.3f}ms "
                f"{result['mean_ms']:>10.3f}ms "
                f"{result['median_ms']:>10.3f}ms "
                f"{result['max_ms']:>10.3f}ms"
            )

        print(f"{'='*80}\n")


def test_small_allocations():
    """Benchmark: Many small allocations and reads."""
    pool = MemoryPool(size=10_000_000)  # 10MB
    data = b"x" * 100  # 100 bytes

    refs = []
    start = time.perf_counter()
    for _ in range(50000):
        ref = pool.commit(data)
        refs.append(ref)
    commit_time = time.perf_counter() - start

    start = time.perf_counter()
    for ref in refs:
        pool.read(ref)
    read_time = time.perf_counter() - start

    start = time.perf_counter()
    for ref in refs:
        pool.release(ref)
    release_time = time.perf_counter() - start

    return {
        'commit': commit_time,
        'read': read_time,
        'release': release_time,
        'total': commit_time + read_time + release_time
    }


def test_medium_allocations():
    """Benchmark: Medium-sized allocations."""
    pool = MemoryPool(size=50_000_000)  # 50MB
    data = b"y" * 10000  # 10KB

    refs = []
    start = time.perf_counter()
    for _ in range(2000):
        ref = pool.commit(data)
        refs.append(ref)
    commit_time = time.perf_counter() - start

    start = time.perf_counter()
    for ref in refs:
        pool.read(ref)
    read_time = time.perf_counter() - start

    start = time.perf_counter()
    for ref in refs:
        pool.release(ref)
    release_time = time.perf_counter() - start

    return {
        'commit': commit_time,
        'read': read_time,
        'release': release_time,
        'total': commit_time + read_time + release_time
    }


def test_large_allocations():
    """Benchmark: Few large allocations."""
    pool = MemoryPool(size=200_000_000)  # 200MB
    data = b"z" * (10_000_000)  # 10MB

    refs = []
    start = time.perf_counter()
    for _ in range(10):
        ref = pool.commit(data)
        refs.append(ref)
    commit_time = time.perf_counter() - start

    start = time.perf_counter()
    for ref in refs:
        pool.read(ref, zero_copy=True)
    read_time = time.perf_counter() - start

    start = time.perf_counter()
    for ref in refs:
        pool.release(ref)
    release_time = time.perf_counter() - start

    return {
        'commit': commit_time,
        'read': read_time,
        'release': release_time,
        'total': commit_time + read_time + release_time
    }


def test_mixed_workload():
    """Benchmark: Mixed allocation sizes and operations."""
    pool = MemoryPool(size=50_000_000)  # 50MB

    start = time.perf_counter()
    refs = []

    # Commit mixed sizes
    for i in range(5000):
        size = 100 + (i % 10000)  # Sizes from 100 to 10100 bytes
        data = b"m" * size
        ref = pool.commit(data)
        refs.append(ref)

        # Interleave some reads and releases
        if len(refs) > 100 and i % 10 == 0:
            read_ref = refs.pop(0)
            pool.read(read_ref)
            pool.release(read_ref)

    # Release remaining
    for ref in refs:
        pool.read(ref)
        pool.release(ref)

    total_time = time.perf_counter() - start

    return {'total': total_time}


def test_concurrent_access():
    """Benchmark: Concurrent access with multiple threads."""
    pool = MemoryPool(size=100_000_000)  # 100MB
    ops_per_thread = 2000
    num_threads = 8

    def thread_work():
        for _ in range(ops_per_thread):
            data = random_string(1000).encode()
            ref = pool.commit(data)
            if ref != -1:
                pool.read(ref)
                pool.release(ref)

    start = time.perf_counter()
    threads = []
    for _ in range(num_threads):
        t = threading.Thread(target=thread_work)
        threads.append(t)
        t.start()

    for t in threads:
        t.join()

    total_time = time.perf_counter() - start
    total_ops = num_threads * ops_per_thread * 3  # commit + read + release

    return {
        'total': total_time,
        'ops': total_ops,
        'throughput': total_ops / total_time
    }


def test_rapid_allocate_release():
    """Benchmark: Rapid allocate-release cycles."""
    pool = MemoryPool(size=20_000_000)  # 20MB
    data = b"r" * 1000  # 1KB

    start = time.perf_counter()
    for _ in range(10000):
        ref = pool.commit(data)
        pool.release(ref)
    total_time = time.perf_counter() - start

    return {'total': total_time}


def test_compaction_overhead():
    """Benchmark: Operations with compaction pressure."""
    pool = MemoryPool(size=10_000_000)  # 10MB (small to force compaction)

    start = time.perf_counter()
    refs = []

    for i in range(5000):
        # Allocate with varying sizes to cause fragmentation
        size = 500 + (i % 2000)
        data = b"c" * size
        ref = pool.commit(data)
        refs.append(ref)

        # Release every 10th allocation to trigger compaction
        if i % 10 == 0 and refs:
            pool.release(refs.pop(0))

    # Clean up
    for ref in refs:
        pool.release(ref)

    total_time = time.perf_counter() - start

    return {'total': total_time}


def test_zero_copy_vs_copy():
    """Benchmark: Zero-copy reads vs regular reads."""
    pool = MemoryPool(size=50_000_000)  # 50MB
    data = b"z" * 100000  # 100KB

    refs = []
    for _ in range(100):
        ref = pool.commit(data)
        refs.append(ref)

    # Test regular copy reads
    start = time.perf_counter()
    for ref in refs:
        pool.read(ref, zero_copy=False)
    copy_time = time.perf_counter() - start

    # Test zero-copy reads
    start = time.perf_counter()
    for ref in refs:
        pool.read(ref, zero_copy=True)
    zero_copy_time = time.perf_counter() - start

    return {
        'copy': copy_time,
        'zero_copy': zero_copy_time,
        'speedup': copy_time / zero_copy_time
    }


def print_test_results(test_name: str, results: dict):
    """Print results from a single test."""
    print(f"\n{test_name}")
    print("-" * 60)
    for key, value in results.items():
        if isinstance(value, float):
            if key == 'throughput':
                print(f"  {key:.<40} {value:>15,.0f} ops/sec")
            elif key == 'speedup':
                print(f"  {key:.<40} {value:>15.2f}x")
            else:
                print(f"  {key:.<40} {value:>15.3f} ms")
        else:
            print(f"  {key:.<40} {value:>15}")


def main():
    """Run all benchmarks."""
    print("\n" + "=" * 60)
    print("MemoryPool Comprehensive Benchmark Suite")
    print("=" * 60)

    print(f"\nTest Parameters:")
    print(f"  Python: {sys.version.split()[0]}")
    print(f"  Platform: {sys.platform}")

    # Run benchmarks
    print("\n" + "=" * 60)
    print("Running benchmarks...")
    print("=" * 60)

    results = {}

    print("\n[1/7] Small allocations (50k × 100 bytes)...", end="", flush=True)
    results['small'] = test_small_allocations()
    print(" ✓")

    print("[2/7] Medium allocations (2k × 10KB)...", end="", flush=True)
    results['medium'] = test_medium_allocations()
    print(" ✓")

    print("[3/7] Large allocations (10 × 10MB)...", end="", flush=True)
    results['large'] = test_large_allocations()
    print(" ✓")

    print("[4/7] Mixed workload...", end="", flush=True)
    results['mixed'] = test_mixed_workload()
    print(" ✓")

    print("[5/7] Concurrent access (8 threads)...", end="", flush=True)
    results['concurrent'] = test_concurrent_access()
    print(" ✓")

    print("[6/7] Rapid allocate-release cycles...", end="", flush=True)
    results['rapid'] = test_rapid_allocate_release()
    print(" ✓")

    print("[7/7] Zero-copy vs copy reads...", end="", flush=True)
    results['zero_copy'] = test_zero_copy_vs_copy()
    print(" ✓")

    # Print results
    print("\n" + "=" * 60)
    print("RESULTS")
    print("=" * 60)

    print_test_results("Small Allocations (50k × 100 bytes)", results['small'])
    print_test_results("Medium Allocations (2k × 10KB)", results['medium'])
    print_test_results("Large Allocations (10 × 10MB)", results['large'])
    print_test_results("Mixed Workload", results['mixed'])
    print_test_results("Concurrent Access (8 threads)", results['concurrent'])
    print_test_results("Rapid Allocate-Release", results['rapid'])
    print_test_results("Zero-Copy vs Copy Reads", results['zero_copy'])

    print("\n" + "=" * 60)
    print("Benchmark Complete!")
    print("=" * 60)

    # Summary stats
    print("\nKey Metrics:")
    small_ops = 50000 * 3 / results['small']['total']
    medium_ops = 2000 * 3 / results['medium']['total']
    print(f"  Small alloc throughput: {small_ops:,.0f} ops/sec")
    print(f"  Medium alloc throughput: {medium_ops:,.0f} ops/sec")
    if 'throughput' in results['concurrent']:
        print(f"  Concurrent throughput: {results['concurrent']['throughput']:,.0f} ops/sec")
    print(f"  Zero-copy speedup: {results['zero_copy'].get('speedup', 1):.2f}x")


if __name__ == '__main__':
    main()
