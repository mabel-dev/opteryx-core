"""
Performance benchmark for Bloom filter and group-by optimizations (Fixes 1-4).

This benchmark measures the effectiveness of:
- Fix 1: Eliminating double lookup_fast calls
- Fix 2: Bloom filter pre-filter integration
- Fix 3: Pre-allocation of state vectors (amortised doubling)
- Fix 4: Reduced hash index load factor

The workload is Clickbench-like with high cardinality groups.

Run with:
    python tests/performance/benchmarks/bench_groupby_bloom_fixes.py
"""

from __future__ import annotations

import json
import os
import sys
import time

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

import numpy as np
import pyarrow as pa

from opteryx import EOS
from opteryx.models import QueryProperties
from opteryx.operators.shuffle import AggregationSpec, ShuffleGroupByOperation


def _create_clickbench_like_data(
    num_rows: int = 10_000_000,
    num_unique_groups: int = 1_000_000,
    morsel_size: int = 100_000,
) -> tuple[pa.Table, int]:
    """
    Create synthetic Clickbench-like data with high cardinality groups.

    Args:
        num_rows: Total rows to generate
        num_unique_groups: Number of unique group IDs
        morsel_size: Size of each morsel for ingest

    Returns:
        Tuple of (full table, number of morsels)
    """
    # Create group IDs with Zipfian-like distribution (realistic for URLs/UserIDs)
    # Most groups appear rarely, few groups appear frequently
    group_ids = np.random.choice(
        num_unique_groups,
        size=num_rows,
        p=None,  # Will use uniform for simplicity; could use Zipfian
    )

    # Create some aggregation values
    event_time = np.random.randint(0, 1000000, num_rows)
    value1 = np.random.rand(num_rows)
    value2 = np.random.randint(0, 100, num_rows)

    table = pa.table(
        {
            "group_id": pa.array(group_ids, type=pa.int64()),
            "event_time": pa.array(event_time, type=pa.int64()),
            "value1": pa.array(value1, type=pa.float64()),
            "value2": pa.array(value2, type=pa.int64()),
        }
    )

    num_morsels = (num_rows + morsel_size - 1) // morsel_size
    return table, num_morsels


def _run_groupby_benchmark(
    table: pa.Table,
    morsel_size: int = 100_000,
    name: str = "unnamed",
) -> dict:
    """
    Run a group-by operation on the table and collect telemetry.

    Args:
        table: PyArrow table with test data
        morsel_size: Size of each morsel
        name: Name of the benchmark run

    Returns:
        Dictionary of collected metrics
    """
    print(f"\n{'=' * 70}")
    print(f"Benchmark: {name}")
    print(f"Total rows: {len(table):,}")
    print(f"Morsel size: {morsel_size:,}")
    print(f"{'=' * 70}")

    groupby = ShuffleGroupByOperation(
        group_by_columns=["group_id"],
        aggregations=[
            AggregationSpec(alias="cnt", function="count", column="*"),
            AggregationSpec(alias="sum_val1", function="sum", column="value1"),
            AggregationSpec(alias="sum_val2", function="sum", column="value2"),
            AggregationSpec(alias="avg_val1", function="avg", column="value1"),
        ],
    )

    # Ingest data in morsels
    start_ingest = time.perf_counter()
    num_rows = len(table)
    for start_idx in range(0, num_rows, morsel_size):
        end_idx = min(start_idx + morsel_size, num_rows)
        morsel = table.slice(start_idx, end_idx - start_idx)
        groupby.ingest(morsel)

    ingest_time = time.perf_counter() - start_ingest

    # Finalize
    start_finalize = time.perf_counter()
    result = groupby.finalize()
    finalize_time = time.perf_counter() - start_finalize

    # Extract telemetry
    readings = groupby._operation.readings if hasattr(groupby, "_operation") else {}

    result_arrow = result.to_arrow() if hasattr(result, "to_arrow") else result
    num_groups = len(result_arrow) if result_arrow else 0

    metrics = {
        "name": name,
        "rows_in": num_rows,
        "groups_out": num_groups,
        "time_ingest_s": ingest_time,
        "time_finalize_s": finalize_time,
        "time_total_s": ingest_time + finalize_time,
        "rows_per_sec": num_rows / ingest_time if ingest_time > 0 else 0,
        # Telemetry from group-by engine
        "groupby_ingest_hits": readings.get("groupby_ingest_hits", 0),
        "groupby_ingest_misses": readings.get("groupby_ingest_misses", 0),
        "groupby_bloom_checks": readings.get("groupby_bloom_checks", 0),
        "groupby_bloom_skips": readings.get("groupby_bloom_skips", 0),
        "groupby_bloom_false_positives": readings.get("groupby_bloom_false_positives", 0),
        "time_groupby_ingest_ns": readings.get("time_groupby_ingest", 0),
        "time_groupby_hash_ns": readings.get("time_groupby_hash_ns", 0),
        "time_groupby_accumulate_ns": readings.get("time_groupby_accumulate_ns", 0),
        "time_groupby_ingest_state_assign_ns": readings.get(
            "time_groupby_ingest_state_assign_ns", 0
        ),
    }

    # Print results
    print(f"\nIngest phase:")
    print(f"  Time: {ingest_time:.2f}s ({metrics['rows_per_sec']:,.0f} rows/sec)")
    print(f"  Rows in: {num_rows:,}")
    print(f"  Groups out: {num_groups:,}")

    if metrics["groupby_ingest_hits"] + metrics["groupby_ingest_misses"] > 0:
        hit_rate = metrics["groupby_ingest_hits"] / (
            metrics["groupby_ingest_hits"] + metrics["groupby_ingest_misses"]
        )
        print(f"  Hit rate: {hit_rate:.1%}")

    if metrics["groupby_bloom_checks"] > 0:
        skip_rate = metrics["groupby_bloom_skips"] / metrics["groupby_bloom_checks"]
        fpr = metrics["groupby_bloom_false_positives"] / metrics["groupby_bloom_checks"]
        print(f"  Bloom checks: {metrics['groupby_bloom_checks']:,}")
        print(f"  Bloom skip rate: {skip_rate:.1%}")
        print(f"  Bloom false positive rate: {fpr:.2%}")

    print(f"\nFinalize phase:")
    print(f"  Time: {finalize_time:.2f}s")

    print(f"\nTotal query time: {ingest_time + finalize_time:.2f}s")

    return metrics


def _compare_metrics(baseline: dict, optimized: dict) -> None:
    """Print a comparison between baseline and optimized metrics."""
    print(f"\n{'=' * 70}")
    print("Performance Comparison")
    print(f"{'=' * 70}")

    baseline_time = baseline["time_total_s"]
    optimized_time = optimized["time_total_s"]
    improvement = (baseline_time - optimized_time) / baseline_time * 100
    speedup = baseline_time / optimized_time

    print(f"\nBaseline time:  {baseline_time:.2f}s")
    print(f"Optimized time: {optimized_time:.2f}s")
    print(f"Improvement:    {improvement:+.1f}% ({speedup:.2f}x speedup)")

    if baseline.get("groupby_bloom_checks", 0) > 0:
        print(f"\nBloom filter efficiency:")
        print(
            f"  Bloom skip rate: {baseline['groupby_bloom_skips'] / baseline['groupby_bloom_checks']:.1%}"
        )
        print(
            f"  Bloom false positive rate: {baseline['groupby_bloom_false_positives'] / baseline['groupby_bloom_checks']:.2%}"
        )


def run_groupby_performance_suite():
    """Run the complete performance benchmark suite."""
    print("\n" + "=" * 70)
    print("Opteryx Group-By Performance Benchmark")
    print("Measuring effectiveness of Fixes 1-4")
    print("=" * 70)

    # Test scenarios with varying cardinalities
    test_scenarios = [
        {
            "name": "Low Cardinality (100 groups)",
            "num_rows": 1_000_000,
            "num_unique_groups": 100,
        },
        {
            "name": "Medium Cardinality (100K groups)",
            "num_rows": 10_000_000,
            "num_unique_groups": 100_000,
        },
        {
            "name": "High Cardinality (1M groups)",
            "num_rows": 10_000_000,
            "num_unique_groups": 1_000_000,
        },
    ]

    results = []

    for scenario in test_scenarios:
        print(f"\n\n{'#' * 70}")
        print(f"# {scenario['name']}")
        print(f"{'#' * 70}")

        table, num_morsels = _create_clickbench_like_data(
            num_rows=scenario["num_rows"],
            num_unique_groups=scenario["num_unique_groups"],
        )

        print(f"Generated data with {num_morsels} morsels")

        # Run benchmark
        metrics = _run_groupby_benchmark(
            table,
            morsel_size=100_000,
            name=scenario["name"],
        )

        results.append(metrics)

    # Print summary
    print(f"\n\n{'=' * 70}")
    print("Summary of Results")
    print(f"{'=' * 70}\n")

    for result in results:
        print(f"{result['name']}:")
        print(f"  Throughput: {result['rows_per_sec']:,.0f} rows/sec")
        print(f"  Total time: {result['time_total_s']:.2f}s")
        if result["groupby_bloom_checks"] > 0:
            print(
                f"  Bloom skip rate: {result['groupby_bloom_skips'] / result['groupby_bloom_checks']:.1%}"
            )
        print()

    # Save results to JSON for analysis
    output_file = "bench_groupby_bloom_fixes_results.json"
    with open(output_file, "w") as f:
        json.dump(results, f, indent=2)
    print(f"Results saved to {output_file}")


if __name__ == "__main__":
    run_groupby_performance_suite()
