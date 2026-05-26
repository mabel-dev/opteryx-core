"""
Performance benchmarking for distogram.

This benchmark is intended to support a baseline -> change -> compare loop for
the distogram hot path used by the optimizer.

Usage:
- Run normally: `python tests/performance/test_distogram_perf.py`
- Save a baseline: `SAVE_BASELINE=1 python tests/performance/test_distogram_perf.py`
- Scale workload: `DISTOGRAM_N=500000 python tests/performance/test_distogram_perf.py`

The benchmark measures:
- scalar streaming updates
- bulk loading
- manifest-style histogram loading
- native-buffer histogram loading
- merge throughput
- query-time count_up_to
- query-time quantile
"""

from __future__ import annotations

import json
import os
import random
import sys
import time
from array import array
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from opteryx.third_party.maki_nage import distogram

BASELINE_FILE = Path(__file__).with_name(".perf_baseline_distogram.json")


def _gen_values(n: int, seed: int, kind: str = "normal") -> list[float]:
    rnd = random.Random(seed)
    if kind == "uniform":
        return [rnd.uniform(-1000.0, 1000.0) for _ in range(n)]
    if kind == "skewed":
        # Heavy duplicate rate and a long tail. This stresses trim/merge paths.
        values = []
        for _ in range(n):
            if rnd.random() < 0.8:
                values.append(float(rnd.randint(0, 63)))
            else:
                values.append(rnd.gauss(250.0, 120.0))
        return values
    return [rnd.gauss(0.0, 1.0) for _ in range(n)]


def _time_call(fn):
    start = time.perf_counter()
    result = fn()
    elapsed = time.perf_counter() - start
    return elapsed, result


def _format_ratio(ratio: float) -> str:
    if ratio > 1.05:
        return f"{ratio:.2f}x FASTER"
    if ratio < 0.95:
        return f"{1 / ratio:.2f}x SLOWER"
    return "~same"


def _print_results(results: dict[str, float], baseline: dict[str, float] | None) -> None:
    print("\nPERFORMANCE RESULTS")
    print("=" * 72)
    for key in (
        "scalar_update_time",
        "bulkload_time",
        "load_counts_time",
        "load_counts_i64_time",
        "merge_time",
        "count_up_to_time",
        "quantile_time",
    ):
        value = results[key]
        print(f"{key:<20} {value:>12.6f}s")

    if baseline is None:
        print("\nTip: run with SAVE_BASELINE=1 to create a baseline file.")
        return

    print("\nCOMPARISON TO BASELINE")
    print("=" * 72)
    for key in (
        "scalar_update_time",
        "bulkload_time",
        "load_counts_time",
        "load_counts_i64_time",
        "merge_time",
        "count_up_to_time",
        "quantile_time",
    ):
        current = results[key]
        base = baseline.get(key)
        if not base:
            print(f"{key:<20} baseline missing")
            continue
        ratio = base / current if current else float("inf")
        print(f"{key:<20} {_format_ratio(ratio)}")


def test_distogram_perf():
    """Benchmark distogram hot paths and optionally compare against a baseline."""
    n = int(os.environ.get("DISTOGRAM_N", "1000000"))
    bin_count = int(os.environ.get("DISTOGRAM_BIN_COUNT", "64"))
    query_count = int(os.environ.get("DISTOGRAM_QUERY_COUNT", "500000"))
    merge_inputs = int(os.environ.get("DISTOGRAM_MERGE_INPUTS", "4096"))
    seed = int(os.environ.get("DISTOGRAM_SEED", "42"))
    kind = os.environ.get("DISTOGRAM_KIND", "normal")

    values = _gen_values(n, seed, kind=kind)

    # Scalar streaming update baseline.
    h = distogram.Distogram(bin_count=bin_count)

    def _scalar_update():
        for v in values:
            distogram.update(h, v)

    scalar_time, _ = _time_call(_scalar_update)
    assert h.count() == n

    # Bulk load path. This is the most obvious place to improve ingestion.
    h_bulk = distogram.Distogram(bin_count=bin_count)
    bulk_time, _ = _time_call(lambda: h_bulk.bulkload(values))
    assert h_bulk.count() == n

    # Manifest histogram path. Statistics arrive as equi-width counts; avoid
    # constructing Python (center, count) tuples before entering Cython.
    hist_bucket_count = max(bin_count * 4, 256)
    hist_min = min(values)
    hist_max = max(values)
    hist_span = hist_max - hist_min
    hist_counts = [0] * hist_bucket_count
    for v in values:
        if hist_span == 0:
            hist_counts[0] += 1
        elif v == hist_max:
            hist_counts[-1] += 1
        else:
            hist_counts[int((v - hist_min) / hist_span * hist_bucket_count)] += 1

    load_counts_time, h_counts = _time_call(
        lambda: distogram.load_counts(hist_counts, hist_min, hist_max)
    )
    assert h_counts.count() == n

    hist_counts_i64 = array("q", hist_counts)
    load_counts_i64_time, h_counts_i64 = _time_call(
        lambda: distogram.load_counts_i64(hist_counts_i64, hist_min, hist_max)
    )
    assert h_counts_i64.count() == n

    def _build_histogram(target, seq):
        for v in seq:
            distogram.update(target, v)

    # Merge path. This matters for distributed or partitioned aggregation.
    merge_inputs = max(2, min(merge_inputs, n))
    chunk_size = max(1, n // merge_inputs)
    merge_hists = []
    for start in range(0, n, chunk_size):
        h_part = distogram.Distogram(bin_count=bin_count)
        _build_histogram(h_part, values[start : start + chunk_size])
        merge_hists.append(h_part)
        if len(merge_hists) >= merge_inputs:
            break

    h_accum = distogram.Distogram(bin_count=bin_count)

    def _merge_hists():
        result = h_accum
        for h_part in merge_hists:
            result = distogram.merge(result, h_part)
        return result

    merge_time, merged = _time_call(_merge_hists)
    assert merged.count() == sum(h.count() for h in merge_hists)

    # Query path. We sample values from the generated distribution so the
    # branch mix resembles real optimizer/selectivity use.
    queries = [values[(i * 997) % n] for i in range(query_count)]

    def _count_queries():
        total = 0.0
        for q in queries:
            total += distogram.count_up_to(h_bulk, q)
        return total

    def _quantile_queries():
        output = []
        denom = max(1, query_count - 1)
        for i in range(query_count):
            output.append(distogram.quantile(h_bulk, i / denom))
        return output

    count_time, count_total = _time_call(_count_queries)
    quantile_time, quantile_total = _time_call(_quantile_queries)
    assert count_total >= 0
    assert quantile_total[0] is not None
    assert quantile_total[-1] is not None

    results = {
        "scalar_update_time": scalar_time,
        "bulkload_time": bulk_time,
        "load_counts_time": load_counts_time,
        "load_counts_i64_time": load_counts_i64_time,
        "merge_time": merge_time,
        "count_up_to_time": count_time,
        "quantile_time": quantile_time,
    }

    print("=" * 72)
    print("DISTOGRAM PERFORMANCE")
    print("=" * 72)
    print(
        f"dataset              N={n:,} bin_count={bin_count} kind={kind} "
        f"queries={query_count:,} merge_inputs={len(merge_hists):,}"
    )
    print(f"stream update         {n / scalar_time:,.0f} rows/sec")
    print(f"bulkload              {n / bulk_time:,.0f} rows/sec")
    print(f"load_counts           {hist_bucket_count / load_counts_time:,.0f} buckets/sec")
    print(f"load_counts_i64       {hist_bucket_count / load_counts_i64_time:,.0f} buckets/sec")
    print(f"merge                 {n / merge_time:,.0f} rows/sec")
    print(f"count_up_to           {query_count / count_time:,.0f} queries/sec")
    print(f"quantile              {query_count / quantile_time:,.0f} queries/sec")

    baseline = None
    if os.environ.get("SAVE_BASELINE"):
        with BASELINE_FILE.open("w", encoding="utf-8") as f:
            json.dump(
                {
                    "n": n,
                    "bin_count": bin_count,
                    "query_count": query_count,
                    "merge_inputs": merge_inputs,
                    "kind": kind,
                    "seed": seed,
                    **results,
                },
                f,
                indent=2,
            )
        print(f"\nSaved baseline to {BASELINE_FILE}")
    elif BASELINE_FILE.exists():
        with BASELINE_FILE.open("r", encoding="utf-8") as f:
            baseline = json.load(f)
        if (
            baseline.get("n") != n
            or baseline.get("bin_count") != bin_count
            or baseline.get("query_count") != query_count
            or baseline.get("merge_inputs") != merge_inputs
            or baseline.get("kind") != kind
            or baseline.get("seed") != seed
        ):
            print(
                "\nBaseline exists but parameters differ: "
                f"n={baseline.get('n')} bin_count={baseline.get('bin_count')} "
                f"query_count={baseline.get('query_count')} kind={baseline.get('kind')} "
                f"merge_inputs={baseline.get('merge_inputs')} seed={baseline.get('seed')}"
            )
            baseline = None

    _print_results(results, baseline)


if __name__ == "__main__":  # pragma: no cover
    test_distogram_perf()
