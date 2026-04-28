"""
Set benchmark for DISTINCT-like workloads using Draken.

Compares:
1. CarcharSet (native C++)
2. Abseil FlatHashSet (Cython wrapper)
3. Python set

Uses Draken Morsel hashing to simulate real engine usage where CarcharSet
is fed pre-computed hashes from columnar data.

Run with:
  python tests/performance/benchmarks/bench_carchar_sets.py
  python tests/performance/benchmarks/bench_carchar_sets.py --rows 500000 --repeat 7
    python tests/performance/benchmarks/bench_carchar_sets.py --sweep

Notes:
    The printed `uniq` value is realized post-hash cardinality for the generated
    workload and can differ slightly from the target due to random collisions.
"""

from __future__ import annotations

import argparse
import importlib
import os
import statistics
import sys
import time

import numpy
import pyarrow as pa

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))


import draken as draken

DEFAULT_CPP_MODULE = "opteryx.compiled.nanobind.carchar_native"


def _measure_ms(fn, repeat: int) -> tuple[float, float]:
    fn()
    samples = []
    for _ in range(repeat):
        t0 = time.perf_counter_ns()
        fn()
        samples.append((time.perf_counter_ns() - t0) / 1_000_000.0)
    return min(samples), statistics.mean(samples)


def _build_workload(
    rows: int,
    unique_keys: int,
    probe_count: int,
    seed: int,
    dup_ratio: float | None,
    hit_ratio: float,
):
    """Create workload using draken Morsels.

    Returns:
        tuple: (build_hashes, probe_hashes) as uint64 arrays from draken hashing
    """
    if rows <= 0 or unique_keys <= 0 or probe_count <= 0:
        raise ValueError("rows, unique_keys, and probe_count must be positive")
    if dup_ratio is not None and not (0.0 <= dup_ratio < 1.0):
        raise ValueError("dup_ratio must be in [0.0, 1.0)")
    if not (0.0 <= hit_ratio <= 1.0):
        raise ValueError("hit_ratio must be in [0.0, 1.0]")

    if dup_ratio is not None:
        unique_keys = max(1, int(round(rows * (1.0 - dup_ratio))))
    unique_keys = min(unique_keys, rows)
    rng = numpy.random.default_rng(seed)

    # Create build dataset as Arrow table
    base_values = rng.integers(0, 2**31 - 1, size=unique_keys, dtype=numpy.int64)
    build_values = numpy.empty(rows, dtype=numpy.int64)
    for i in range(rows):
        build_values[i] = base_values[i % unique_keys]
    rng.shuffle(build_values)

    # Convert to draken Morsel and compute hashes
    build_table = pa.table({"key": build_values})
    build_morsel = draken.Morsel.from_arrow(build_table)
    build_hashes = numpy.array(build_morsel.hash())

    # Create probe dataset with configurable hit/miss ratio
    hit_count = int(round(probe_count * hit_ratio))
    miss_count = probe_count - hit_count
    hit_positions = rng.integers(0, rows, size=hit_count, dtype=numpy.int64)
    hit_probes = build_values[hit_positions]

    # Miss probes use different key space (won't collide with builds)
    miss_values = rng.integers(2**31, 2**32 - 1, size=miss_count, dtype=numpy.int64)
    probe_values = numpy.concatenate((hit_probes, miss_values))
    rng.shuffle(probe_values)

    # Convert probes to draken Morsel and compute hashes
    probe_table = pa.table({"key": probe_values})
    probe_morsel = draken.Morsel.from_arrow(probe_table)
    probe_hashes = numpy.array(probe_morsel.hash())

    return build_hashes, probe_hashes


class CarcharSetAdapter:
    name = "carchar-set"

    def __init__(self, module_name: str):
        module = importlib.import_module(module_name)
        cls = getattr(module, "CarcharSet", None)
        if cls is None:
            raise AttributeError(f"{module_name!r} does not export CarcharSet")
        self._cls = cls

    def build(self, hashes: numpy.ndarray):
        """Build from pre-computed draken hashes (uint64)."""
        s = self._cls(len(hashes), 0.80)
        s.reserve(len(hashes))
        s.insert_many(hashes)
        return s

    def probe(self, index, hashes: numpy.ndarray) -> int:
        """Probe with pre-computed draken hashes (uint64)."""
        return index.contains_many_count(hashes)

    def size(self, index) -> int:
        return index.size()


class AbseilSetAdapter:
    name = "abseil-flat_hash_set"

    def __init__(self):
        module = importlib.import_module("opteryx.third_party.abseil.containers")
        cls = getattr(module, "FlatHashSet", None)
        if cls is None:
            raise AttributeError(
                "opteryx.third_party.abseil.containers does not export FlatHashSet"
            )
        self._cls = cls

    def build(self, hashes: numpy.ndarray):
        """Build from pre-computed draken hashes (uint64)."""
        s = self._cls()
        s.add_many_count_new(hashes)
        return s

    def probe(self, index, hashes: numpy.ndarray) -> int:
        """Probe with pre-computed draken hashes (uint64)."""
        return index.has_many_count(hashes)

    def size(self, index) -> int:
        return index.items()


class PythonSetAdapter:
    name = "python-set"

    def build(self, hashes: numpy.ndarray):
        """Build from pre-computed draken hashes (uint64)."""
        s = set()
        for h in hashes:
            s.add(int(h))
        return s

    def probe(self, index, hashes: numpy.ndarray) -> int:
        """Probe with pre-computed draken hashes (uint64)."""
        hits = 0
        for h in hashes:
            if int(h) in index:
                hits += 1
        return hits

    def size(self, index) -> int:
        return len(index)


def _build_adapters(cpp_module: str):
    adapters = []
    try:
        adapters.append(CarcharSetAdapter(cpp_module))
    except (ImportError, AttributeError) as err:
        print(f"[skip] carchar set unavailable: {err}")
    try:
        adapters.append(AbseilSetAdapter())
    except (ImportError, AttributeError) as err:
        print(f"[skip] abseil set unavailable: {err}")
    adapters.append(PythonSetAdapter())
    return adapters


def _run_benchmark_case(
    rows: int,
    unique_keys: int,
    probe_count: int,
    repeat: int,
    seed: int,
    cpp_module: str,
    dup_ratio: float | None,
    hit_ratio: float,
):
    build_hashes, probe_hashes = _build_workload(
        rows,
        unique_keys,
        probe_count,
        seed,
        dup_ratio,
        hit_ratio,
    )
    workload_unique = len(set(build_hashes))
    adapters = _build_adapters(cpp_module)

    results = []
    for adapter in adapters:
        idx = adapter.build(build_hashes)
        hits = adapter.probe(idx, probe_hashes)
        size = adapter.size(idx)

        if size != workload_unique:
            raise RuntimeError(f"{adapter.name} size mismatch: {size} != {workload_unique}")

        def _build_once(current_adapter=adapter):
            return current_adapter.build(build_hashes)

        def _probe_once(current_adapter=adapter, current_index=idx):
            return current_adapter.probe(current_index, probe_hashes)

        build_best, build_mean = _measure_ms(_build_once, repeat)
        probe_best, probe_mean = _measure_ms(_probe_once, repeat)
        build_mops = rows / (build_mean / 1000.0) / 1_000_000.0
        probe_mops = probe_count / (probe_mean / 1000.0) / 1_000_000.0

        results.append(
            {
                "impl": adapter.name,
                "rows": rows,
                "uniq": workload_unique,
                "probes": probe_count,
                "build_best": build_best,
                "build_mean": build_mean,
                "probe_best": probe_best,
                "probe_mean": probe_mean,
                "hits": hits,
                "build_mops": build_mops,
                "probe_mops": probe_mops,
            }
        )

    return results, workload_unique


def _parse_ratio_list(raw: str, label: str, upper_open: bool) -> list[float]:
    values = []
    for token in raw.split(","):
        token = token.strip()
        if not token:
            continue
        value = float(token)
        if upper_open:
            if not (0.0 <= value < 1.0):
                raise ValueError(f"{label} values must be in [0.0, 1.0)")
        else:
            if not (0.0 <= value <= 1.0):
                raise ValueError(f"{label} values must be in [0.0, 1.0]")
        values.append(value)
    if not values:
        raise ValueError(f"{label} list cannot be empty")
    return values


def benchmark(
    rows: int,
    unique_keys: int,
    probe_count: int,
    repeat: int,
    seed: int,
    cpp_module: str,
    dup_ratio: float | None,
    hit_ratio: float,
):
    results, workload_unique = _run_benchmark_case(
        rows,
        unique_keys,
        probe_count,
        repeat,
        seed,
        cpp_module,
        dup_ratio,
        hit_ratio,
    )

    print("=" * 112)
    print("Set Benchmark (insert unique + membership probes) [Using Draken Morsel Hashing]")
    print("=" * 112)
    if dup_ratio is not None:
        print(f"workload: dup_ratio={dup_ratio:.2f}, hit_ratio={hit_ratio:.2f}")
    else:
        print(f"workload: unique_keys={unique_keys}, hit_ratio={hit_ratio:.2f}")
    print(
        f"{'impl':<24}  {'rows':>9}  {'uniq':>9}  {'probes':>9}  "
        f"{'build best':>11}  {'build mean':>11}  {'probe best':>11}  {'probe mean':>11}  {'hits':>9}"
        f"  {'build Mops/s':>13}  {'probe Mops/s':>13}"
    )

    for result in results:
        print(
            f"{result['impl']:<24}  {rows:9d}  {workload_unique:9d}  {probe_count:9d}  "
            f"{result['build_best']:11.2f}  {result['build_mean']:11.2f}  "
            f"{result['probe_best']:11.2f}  {result['probe_mean']:11.2f}  {result['hits']:9d}"
            f"  {result['build_mops']:13.2f}  {result['probe_mops']:13.2f}"
        )


def benchmark_sweep(
    rows: int,
    unique_keys: int,
    probe_count: int,
    repeat: int,
    seed: int,
    cpp_module: str,
    dup_ratios: list[float],
    hit_ratios: list[float],
):
    print("=" * 112)
    print("Set Benchmark Sweep (Draken Morsel Hashing)")
    print("=" * 112)
    print(
        f"{'impl':<24}  {'dup':>5}  {'hit':>5}  {'uniq':>9}  {'hits':>9}  "
        f"{'build Mops/s':>13}  {'probe Mops/s':>13}"
    )

    case_index = 0
    for dup_ratio in dup_ratios:
        for hit_ratio in hit_ratios:
            case_seed = seed + case_index
            case_index += 1
            results, workload_unique = _run_benchmark_case(
                rows,
                unique_keys,
                probe_count,
                repeat,
                case_seed,
                cpp_module,
                dup_ratio,
                hit_ratio,
            )
            for result in results:
                print(
                    f"{result['impl']:<24}  {dup_ratio:5.2f}  {hit_ratio:5.2f}  {workload_unique:9d}  "
                    f"{result['hits']:9d}  {result['build_mops']:13.2f}  {result['probe_mops']:13.2f}"
                )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Benchmark CarcharSet vs Abseil FlatHashSet vs Python set"
    )
    parser.add_argument("--rows", type=int, default=500_000)
    parser.add_argument("--unique-keys", type=int, default=250_000)
    parser.add_argument("--probe-count", type=int, default=250_000)
    parser.add_argument("--repeat", type=int, default=5)
    parser.add_argument("--seed", type=int, default=7)
    parser.add_argument(
        "--dup-ratio",
        type=float,
        default=None,
        help="Optional build duplicate ratio in [0,1). When set, unique_keys is derived from rows.",
    )
    parser.add_argument(
        "--hit-ratio",
        type=float,
        default=0.50,
        help="Probe hit ratio in [0,1]. 0.5 means half hits, half misses.",
    )
    parser.add_argument(
        "--sweep",
        action="store_true",
        help="Run a benchmark matrix across dup/hit ratios and print a compact table.",
    )
    parser.add_argument(
        "--sweep-dup-ratios",
        type=str,
        default="0.00,0.50,0.80",
        help="Comma-separated dup ratios for --sweep (values in [0,1)).",
    )
    parser.add_argument(
        "--sweep-hit-ratios",
        type=str,
        default="0.10,0.50,0.90",
        help="Comma-separated hit ratios for --sweep (values in [0,1]).",
    )
    parser.add_argument("--cpp-module", type=str, default=DEFAULT_CPP_MODULE)
    args = parser.parse_args()

    if args.sweep:
        dup_ratios = _parse_ratio_list(args.sweep_dup_ratios, "sweep-dup-ratios", upper_open=True)
        hit_ratios = _parse_ratio_list(args.sweep_hit_ratios, "sweep-hit-ratios", upper_open=False)
        benchmark_sweep(
            rows=args.rows,
            unique_keys=args.unique_keys,
            probe_count=args.probe_count,
            repeat=args.repeat,
            seed=args.seed,
            cpp_module=args.cpp_module,
            dup_ratios=dup_ratios,
            hit_ratios=hit_ratios,
        )
        raise SystemExit(0)

    benchmark(
        rows=args.rows,
        unique_keys=args.unique_keys,
        probe_count=args.probe_count,
        repeat=args.repeat,
        seed=args.seed,
        cpp_module=args.cpp_module,
        dup_ratio=args.dup_ratio,
        hit_ratio=args.hit_ratio,
    )
