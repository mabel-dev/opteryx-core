"""
Benchmark connection pool saturation for GCS and local file ranges.

Tests different pool sizes (64, 96, 128, etc.) to find optimal concurrency
that maximizes throughput without contention overhead.
"""

import os
import sys
import time
import tempfile
from typing import List, Tuple
from concurrent.futures import ThreadPoolExecutor

# Setup path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../../.."))

import opteryx


def benchmark_pool_size(pool_size: int, query: str, iterations: int = 3) -> Tuple[float, float, float]:
    """
    Benchmark query execution with specific thread pool size.

    Returns: (min_time, avg_time, max_time) in seconds
    """
    # Set pool size via environment (framework uses this)
    os.environ["OPTERYX_THREAD_POOL_SIZE"] = str(pool_size)

    times = []
    for _ in range(iterations):
        session = opteryx.session()

        start = time.perf_counter()
        try:
            result = session.execute_to_arrow(query)
            _ = result.to_pylist()  # Force materialization
        except Exception as e:
            print(f"  Query failed with pool_size={pool_size}: {e}")
            return float('inf'), float('inf'), float('inf')
        elapsed = time.perf_counter() - start
        times.append(elapsed)

    times.sort()
    return min(times), sum(times) / len(times), max(times)


def main():
    """Benchmark pool sizes and find saturation point."""

    # Test query: Medium complexity query that reads from parquet
    query = """
    SELECT
        COUNT(*) as cnt,
        AVG(mass) as avg_mass,
        MAX(diameter) as max_diameter
    FROM testdata.planets
    GROUP BY id
    LIMIT 50
    """

    # Pool sizes to test
    pool_sizes = [32, 48, 56, 64, 80, 96, 112, 128]

    print("=" * 70)
    print("CONNECTION POOL SATURATION BENCHMARK")
    print("=" * 70)
    print(f"Query: {query[:60]}...")
    print(f"Testing pool sizes: {pool_sizes}")
    print()

    results = {}
    best_size = None
    best_avg = float('inf')

    for pool_size in pool_sizes:
        print(f"Testing pool_size={pool_size:3d}...", end=" ", flush=True)
        min_t, avg_t, max_t = benchmark_pool_size(pool_size, query, iterations=3)
        results[pool_size] = (min_t, avg_t, max_t)

        if avg_t < best_avg:
            best_avg = avg_t
            best_size = pool_size

        print(f"min={min_t:.3f}s avg={avg_t:.3f}s max={max_t:.3f}s")

    print()
    print("=" * 70)
    print("RESULTS")
    print("=" * 70)
    print(f"{'Pool Size':<12} {'Min (s)':<12} {'Avg (s)':<12} {'Max (s)':<12} {'Status':<20}")
    print("-" * 70)

    for pool_size in pool_sizes:
        min_t, avg_t, max_t = results[pool_size]
        status = "OPTIMAL" if pool_size == best_size else ""
        print(f"{pool_size:<12} {min_t:<12.4f} {avg_t:<12.4f} {max_t:<12.4f} {status:<20}")

    print()
    print(f"🏆 Optimal pool size: {best_size} (avg: {best_avg:.4f}s)")
    print()
    print("RECOMMENDATION:")
    print(f"Set _MAX_PARALLEL_RANGE_READS = {best_size} in local_filesystem.py")
    print(f"Set _MAX_PARALLEL_HEAD_REQUESTS = {max(16, best_size//4)} in gcs_filesystem.py")


if __name__ == "__main__":
    main()
