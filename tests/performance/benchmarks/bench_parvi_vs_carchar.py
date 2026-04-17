"""
Micro-benchmark comparing Parvi (fixed 16-slot) vs Carchar (dynamic) hash maps.

Parvi is optimized for small group-by results (≤16 groups).
Carchar is the general-purpose dynamic hash map.

Scenarios:
1. tiny (4 groups): parvi optimal
2. small (12 groups): parvi optimal
3. exact (16 groups): parvi at capacity
4. overflow (20 groups): parvi overflows to carchar mid-stream
5. large (200 groups): carchar optimal

Run with:
    python tests/performance/benchmarks/bench_parvi_vs_carchar.py
"""

import os
import sys
import statistics
import time

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

import numpy


def _measure_ms(fn, repeat: int) -> tuple[float, float]:
    """Measure best and mean time across repeats."""
    fn()  # warmup
    samples = []
    for _ in range(repeat):
        t0 = time.perf_counter_ns()
        fn()
        samples.append((time.perf_counter_ns() - t0) / 1_000_000.0)
    return min(samples), statistics.mean(samples)


def _build_workload(rows: int, unique_keys: int, seed: int = 7):
    """Build keys with specified duplication ratio."""
    rng = numpy.random.default_rng(seed)
    base_keys = rng.integers(0, 2**63 - 1, size=unique_keys, dtype=numpy.uint64)
    build_keys = numpy.empty(rows, dtype=numpy.uint64)
    for row_id in range(rows):
        build_keys[row_id] = base_keys[row_id % unique_keys]
    rng.shuffle(build_keys)
    return build_keys, base_keys


def benchmark_parvi_only():
    """Benchmark parvi-only scenarios."""
    from opteryx.compiled.structures.parvi_index import ParviMapWrapper

    scenarios = [
        ("tiny", 1000, 4),
        ("small", 1000, 12),
        ("exact", 1000, 16),
    ]

    print("=" * 120)
    print("Parvi Benchmark (fixed 16-slot inline map)")
    print("=" * 120)
    print(f"{'scenario':<20}  {'rows':>8}  {'groups':>8}  {'build (ms)':>15}  {'lookup (ms)':>15}  {'build M/s':>12}  {'lookup M/s':>12}")

    for scenario_name, rows, unique_keys in scenarios:
        build_keys, probe_keys = _build_workload(rows, unique_keys)

        def build():
            m = ParviMapWrapper()
            for row_id, key in enumerate(build_keys):
                m.insert_new(int(key), row_id)
            return m

        def lookup(m):
            count = 0
            for key in probe_keys:
                if m.lookup_fast(int(key)):
                    count += 1
            return count

        m = build()
        lookup_count = lookup(m)

        build_best, build_mean = _measure_ms(build, repeat=5)
        m_for_lookup = build()
        lookup_best, lookup_mean = _measure_ms(lambda: lookup(m_for_lookup), repeat=5)

        build_throughput = rows / (build_mean / 1000.0) / 1_000_000.0
        lookup_throughput = len(probe_keys) / (lookup_mean / 1000.0) / 1_000_000.0

        print(
            f"{scenario_name:<20}  {rows:8d}  {unique_keys:8d}  "
            f"{build_best:7.3f} / {build_mean:7.3f}  "
            f"{lookup_best:7.3f} / {lookup_mean:7.3f}  "
            f"{build_throughput:12.2f}  {lookup_throughput:12.2f}"
        )


def benchmark_carchar_only():
    """Benchmark carchar on the same workloads."""
    import opteryx

    scenarios = [
        ("tiny", 1000, 4),
        ("small", 1000, 12),
        ("exact", 1000, 16),
        ("overflow", 1000, 20),
        ("large", 1000, 200),
    ]

    print("\n" + "=" * 120)
    print("Carchar Baseline (dynamic resizing, general-purpose)")
    print("=" * 120)
    print(f"{'scenario':<20}  {'rows':>8}  {'groups':>8}  {'build (ms)':>15}  {'lookup (ms)':>15}  {'build M/s':>12}  {'lookup M/s':>12}")

    for scenario_name, rows, unique_keys in scenarios:
        build_keys, probe_keys = _build_workload(rows, unique_keys)

        # Access Carchar via Python wrapper if available
        try:
            from opteryx.compiled.structures.carchar_index import CarcharJoinIndexWrapper

            def build():
                idx = CarcharJoinIndexWrapper(16, 0.80)
                for row_id, key in enumerate(build_keys):
                    idx.insert_row(int(key), row_id)
                return idx

            def lookup(idx):
                count = 0
                for key in probe_keys:
                    count += len(idx.rows_for(int(key)))
                return count

            idx = build()
            lookup_count = lookup(idx)

            build_best, build_mean = _measure_ms(build, repeat=5)
            idx_for_lookup = build()
            lookup_best, lookup_mean = _measure_ms(lambda: lookup(idx_for_lookup), repeat=5)

            build_throughput = rows / (build_mean / 1000.0) / 1_000_000.0
            lookup_throughput = len(probe_keys) / (lookup_mean / 1000.0) / 1_000_000.0

            print(
                f"{scenario_name:<20}  {rows:8d}  {unique_keys:8d}  "
                f"{build_best:7.3f} / {build_mean:7.3f}  "
                f"{lookup_best:7.3f} / {lookup_mean:7.3f}  "
                f"{build_throughput:12.2f}  {lookup_throughput:12.2f}"
            )
        except ImportError:
            print(f"{scenario_name:<20}  {rows:8d}  {unique_keys:8d}  {'[CarcharJoinIndexWrapper not available]':>35}")


def benchmark_group_by_end_to_end():
    """End-to-end GROUP BY performance via Opteryx queries."""
    import opteryx

    print("\n" + "=" * 120)
    print("End-to-End GROUP BY Queries (parvi vs carchar selection)")
    print("=" * 120)
    print(f"{'scenario':<20}  {'query':>50}  {'time (ms)':>12}  {'selected map':>15}")

    # Small GROUP BY — should pick parvi if stats are good
    s = opteryx.session()
    queries = [
        ("tiny", "SELECT COUNT(*) as cnt FROM $planets GROUP BY id LIMIT 4"),
        ("all", "SELECT COUNT(*) as cnt FROM $planets GROUP BY id"),
    ]

    for scenario_name, query in queries:
        t0 = time.perf_counter_ns()
        result = list(s.execute_to_morsels(query))
        elapsed_ms = (time.perf_counter_ns() - t0) / 1_000_000.0

        # Try to extract telemetry if available
        selected_map = "unknown"
        print(f"{scenario_name:<20}  {query:>50}  {elapsed_ms:12.2f}  {selected_map:>15}")


if __name__ == "__main__":
    benchmark_parvi_only()
    benchmark_carchar_only()
    benchmark_group_by_end_to_end()
