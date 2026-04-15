"""
Baseline performance measurement for distogram module.
Run this before refactoring to establish performance targets.

Usage:
  python tests/performance/distogram_baseline.py
"""
import time
import sys
import os

# Add repo root to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

from opteryx.third_party.maki_nage.distogram import (
    Distogram,
    update,
    merge,
    count_up_to,
)


def benchmark_update(iterations=1_000_000):
    """Measure update() throughput"""
    d = Distogram(bin_count=50)

    start = time.perf_counter()
    for i in range(iterations):
        update(d, float(i % 10000), 1)
    elapsed = time.perf_counter() - start

    ops_per_sec = iterations / elapsed
    print(f"update():      {ops_per_sec:>12,.0f} ops/sec  ({elapsed:.3f}s for {iterations:,} ops)")
    return ops_per_sec


def benchmark_merge(iterations=1000):
    """Measure merge() throughput"""
    d1 = Distogram(bin_count=50)
    d2 = Distogram(bin_count=50)

    # Pre-populate
    for i in range(1000):
        update(d1, float(i % 100), 1)
        update(d2, float(i % 100), 1)

    start = time.perf_counter()
    for _ in range(iterations):
        merge(d1, d2)
    elapsed = time.perf_counter() - start

    ops_per_sec = iterations / elapsed
    print(f"merge():       {ops_per_sec:>12,.0f} ops/sec  ({elapsed:.3f}s for {iterations:,} ops)")
    return ops_per_sec


def benchmark_count_up_to(iterations=100_000):
    """Measure count_up_to() throughput"""
    d = Distogram(bin_count=50)
    for i in range(10000):
        update(d, float(i), 1)

    start = time.perf_counter()
    for i in range(iterations):
        count_up_to(d, float(i % 10000))
    elapsed = time.perf_counter() - start

    ops_per_sec = iterations / elapsed
    print(f"count_up_to(): {ops_per_sec:>12,.0f} ops/sec  ({elapsed:.3f}s for {iterations:,} ops)")
    return ops_per_sec


if __name__ == "__main__":
    print("=" * 60)
    print("DISTOGRAM BASELINE PERFORMANCE MEASUREMENT")
    print("=" * 60)
    print()

    update_baseline = benchmark_update()
    merge_baseline = benchmark_merge()
    count_baseline = benchmark_count_up_to()

    print()
    print("=" * 60)
    print("BASELINE SUMMARY")
    print("=" * 60)
    print(f"update():      {update_baseline:>12,.0f} ops/sec")
    print(f"merge():       {merge_baseline:>12,.0f} ops/sec")
    print(f"count_up_to(): {count_baseline:>12,.0f} ops/sec")
    print()
    print("After Cython refactor, targets are:")
    print(f"  update():      >8,000,000 ops/sec (20x improvement)")
    print(f"  merge():       >100,000 ops/sec   (10x improvement)")
    print(f"  count_up_to(): >500,000 ops/sec   (5x improvement)")
    print()
