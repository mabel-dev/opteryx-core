"""
Micro-benchmark for Carchar-style hash indexes.

Compares:
1. Abseil `FlatHashMap`
2. Optional native benchmark shim for the C++ Carchar core

This benchmark uses canonical 64-bit integer keys directly. It measures:
1. Build time: insert `(key, row_id)` pairs
2. Hit-probe time: probe existing keys and count matched rows

Miss probes are intentionally not benchmarked for Abseil because the current
`FlatHashMap.get()` wrapper uses `operator[]`, which mutates the table on miss.

Run with:
    python tests/performance/benchmarks/bench_carchar_maps.py
    python tests/performance/benchmarks/bench_carchar_maps.py --rows 500000 --repeat 7
    python tests/performance/benchmarks/bench_carchar_maps.py --cpp-module opteryx.nanobind.carchar_native
"""

from __future__ import annotations

import argparse
import importlib
import os
import statistics
import sys
import time
from dataclasses import dataclass
from typing import Any

import numpy

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

from opteryx.third_party.abseil.containers import FlatHashMap


DEFAULT_CPP_MODULES = (
    "opteryx.nanobind.carchar_native",
)
DEFAULT_LOCALITY_SWEEP_ROWS = (500, 2_000, 8_000, 32_000, 128_000, 300_000, 500_000)


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
) -> tuple[numpy.ndarray, numpy.ndarray]:
    if rows <= 0:
        raise ValueError("rows must be positive")
    if unique_keys <= 0:
        raise ValueError("unique_keys must be positive")
    if probe_count <= 0:
        raise ValueError("probe_count must be positive")

    unique_keys = min(unique_keys, rows)
    rng = numpy.random.default_rng(seed)
    base_keys = rng.integers(0, 2**63 - 1, size=unique_keys, dtype=numpy.uint64)

    build_keys = numpy.empty(rows, dtype=numpy.uint64)
    for row_id in range(rows):
        build_keys[row_id] = base_keys[row_id % unique_keys]

    rng.shuffle(build_keys)
    probe_positions = rng.integers(0, rows, size=probe_count, dtype=numpy.int64)
    probe_keys = build_keys[probe_positions]
    return build_keys, probe_keys


def _expected_probe_rows(build_keys: numpy.ndarray, probe_keys: numpy.ndarray) -> int:
    counts: dict[int, int] = {}
    for key in build_keys:
        normalized = int(key)
        counts[normalized] = counts.get(normalized, 0) + 1
    return sum(counts[int(key)] for key in probe_keys)


def _resolve_cpp_modules(cpp_module: str | None) -> tuple[str, ...]:
    if cpp_module:
        return (cpp_module,)
    return DEFAULT_CPP_MODULES


class _Adapter:
    name = "adapter"

    def build(self, keys: numpy.ndarray):
        raise NotImplementedError

    def probe(self, index, probe_keys: numpy.ndarray) -> int:
        raise NotImplementedError

    def inspect(self, index) -> dict[str, Any]:
        return {}


class AbseilAdapter(_Adapter):
    name = "abseil-flat_hash_map"

    def build(self, keys: numpy.ndarray) -> FlatHashMap:
        index = FlatHashMap()
        for row_id, key in enumerate(keys):
            index.insert(int(key), row_id)
        return index

    def probe(self, index: FlatHashMap, probe_keys: numpy.ndarray) -> int:
        if hasattr(index, "get_many_count"):
            return index.get_many_count(probe_keys)
        rows_seen = 0
        for key in probe_keys:
            if hasattr(index, "get_count"):
                rows_seen += index.get_count(int(key))
            else:
                rows_seen += len(index.get(int(key)))
        return rows_seen

    def inspect(self, index: FlatHashMap) -> dict[str, Any]:
        return {
            "index_size": index.size() if hasattr(index, "size") else None,
            "capacity": None,
            "bytes_estimate": None,
        }


class CppCarcharAdapter(_Adapter):
    name = "cpp-carchar"

    def __init__(self, module_name: str, probe_load_factor: float | None = None) -> None:
        module = importlib.import_module(module_name)
        index_cls = getattr(module, "CarcharJoinEngine", None)
        if index_cls is None:
            index_cls = getattr(module, "CarcharJoinIndex", None)
        if index_cls is None:
            index_cls = getattr(module, "CarcharIndex", None)
        if index_cls is None:
            raise AttributeError(
                f"module {module_name!r} does not export CarcharJoinEngine, CarcharJoinIndex or CarcharIndex"
            )
        self._module_name = module_name
        self._index_cls = index_cls
        self._probe_load_factor = probe_load_factor

    @property
    def name(self) -> str:
        return f"{self._module_name}"

    def build(self, keys: numpy.ndarray):
        try:
            if self._probe_load_factor is not None and self._index_cls.__name__ == "CarcharJoinEngine":
                index = self._index_cls(len(keys), 0, 0.80, self._probe_load_factor)
            else:
                index = self._index_cls(len(keys))
        except TypeError:
            index = self._index_cls()
        if hasattr(index, "reserve"):
            index.reserve(len(keys))
        row_ids = numpy.arange(len(keys), dtype=numpy.int64)
        if hasattr(index, "insert_batch"):
            index.insert_batch(keys, row_ids)
        elif hasattr(index, "insert_keys"):
            index.insert_keys(keys)
        else:
            for row_id, key in enumerate(keys):
                if hasattr(index, "insert_row"):
                    index.insert_row(int(key), row_id)
                elif hasattr(index, "insert"):
                    index.insert(int(key), row_id)
                else:
                    raise AttributeError("compiled Carchar index must provide insert_row() or insert()")
        if hasattr(index, "seal"):
            index.seal()
        return index

    def probe(self, index, probe_keys: numpy.ndarray) -> int:
        if hasattr(index, "probe_row_count_sum"):
            return index.probe_row_count_sum(probe_keys)
        if hasattr(index, "get_many_count"):
            return index.get_many_count(probe_keys)
        rows_seen = 0
        for key in probe_keys:
            if hasattr(index, "row_count_for"):
                rows_seen += index.row_count_for(int(key))
            elif hasattr(index, "get_count"):
                rows_seen += index.get_count(int(key))
            elif hasattr(index, "rows_for"):
                rows_seen += len(index.rows_for(int(key)))
            elif hasattr(index, "get"):
                rows_seen += len(index.get(int(key)))
            else:
                raise AttributeError(
                    "compiled Carchar index must provide row_count_for()/get_count() or rows_for()/get()"
                )
        return rows_seen

    def inspect(self, index) -> dict[str, Any]:
        info: dict[str, Any] = {
            "index_size": None,
            "capacity": None,
            "bytes_estimate": None,
            "average_lookup_probe_length": None,
            "max_lookup_probe_length": None,
        }
        if hasattr(index, "stats"):
            stats = index.stats()
            info["index_size"] = getattr(stats, "size", None)
            info["capacity"] = getattr(stats, "capacity", None)
            info["bytes_estimate"] = getattr(stats, "bytes_estimate", None)
            info["average_lookup_probe_length"] = getattr(stats, "average_lookup_probe_length", None)
            info["max_lookup_probe_length"] = getattr(stats, "max_lookup_probe_length", None)
            return info
        if hasattr(index, "size"):
            info["index_size"] = index.size()
        if hasattr(index, "capacity"):
            info["capacity"] = index.capacity()
        return info


@dataclass(slots=True)
class BenchmarkResult:
    scenario: str
    implementation: str
    rows: int
    unique_keys: int
    probe_count: int
    build_best_ms: float
    build_mean_ms: float
    probe_best_ms: float
    probe_mean_ms: float
    rows_seen: int
    index_size: int | None
    capacity: int | None
    bytes_estimate: int | None
    average_lookup_probe_length: float | None
    max_lookup_probe_length: int | None


@dataclass(frozen=True, slots=True)
class Scenario:
    name: str
    rows: int
    unique_keys: int
    probe_count: int


def _benchmark_adapter(
    adapter: _Adapter,
    scenario: str,
    unique_keys: int,
    build_keys: numpy.ndarray,
    probe_keys: numpy.ndarray,
    repeat: int,
) -> BenchmarkResult:
    build_index = adapter.build(build_keys)
    rows_seen = adapter.probe(build_index, probe_keys)
    inspect = adapter.inspect(build_index)

    build_best_ms, build_mean_ms = _measure_ms(lambda: adapter.build(build_keys), repeat)

    probe_index = adapter.build(build_keys)
    probe_best_ms, probe_mean_ms = _measure_ms(
        lambda: adapter.probe(probe_index, probe_keys), repeat
    )

    return BenchmarkResult(
        scenario=scenario,
        implementation=adapter.name,
        rows=len(build_keys),
        unique_keys=unique_keys,
        probe_count=len(probe_keys),
        build_best_ms=build_best_ms,
        build_mean_ms=build_mean_ms,
        probe_best_ms=probe_best_ms,
        probe_mean_ms=probe_mean_ms,
        rows_seen=rows_seen,
        index_size=inspect.get("index_size"),
        capacity=inspect.get("capacity"),
        bytes_estimate=inspect.get("bytes_estimate"),
        average_lookup_probe_length=inspect.get("average_lookup_probe_length"),
        max_lookup_probe_length=inspect.get("max_lookup_probe_length"),
    )


def _format_int_or_na(value: int | None, width: int) -> str:
    if value is None:
        return f"{'n/a':>{width}}"
    return f"{value:>{width}d}"


def _format_float_or_na(value: float | None, width: int, precision: int = 1) -> str:
    if value is None:
        return f"{'n/a':>{width}}"
    return f"{value:>{width}.{precision}f}"


def benchmark(
    rows: int = 300_000,
    repeat: int = 5,
    probe_count: int = 100_000,
    seed: int = 7,
    cpp_module: str | None = None,
    cpp_probe_load_factor: float | None = None,
) -> list[BenchmarkResult]:
    scenarios = (
        Scenario("high-dup", rows, max(1, rows // 256), probe_count),
        Scenario("medium-dup", rows, max(1, rows // 32), probe_count),
        Scenario("low-dup", rows, max(1, rows // 2), probe_count),
        Scenario("medium-dup probe-heavy", 500, max(1, 500 // 32), 500_000),
        Scenario("medium-dup build-heavy", 500_000, max(1, 500_000 // 32), 500),
    )

    adapters: list[_Adapter] = [AbseilAdapter()]
    for module_name in _resolve_cpp_modules(cpp_module):
        try:
            adapters.append(CppCarcharAdapter(module_name, cpp_probe_load_factor))
        except Exception as err:
            print(f"[skip] unable to load compiled Carchar module {module_name!r}: {err}")

    impl_width = max(34, max(len(adapter.name) for adapter in adapters) + 2)
    scenario_width = max(12, max(len(scenario.name) for scenario in scenarios) + 2)
    results: list[BenchmarkResult] = []

    print("=" * 146)
    print("Carchar Hash-Map Benchmark (build + hit-probe on canonical uint64 keys)")
    print("=" * 146)
    print(
        f"{'scenario':<{scenario_width}}  {'impl':<{impl_width}}  {'rows':>9}  {'uniq':>9}  {'probes':>9}"
        f"  {'build best':>11}  {'build mean':>11}  {'probe best':>11}  {'probe mean':>11}"
        f"  {'rows seen':>10}  {'entries':>9}  {'capacity':>10}  {'bytes':>12}  {'B/entry':>9}"
        f"  {'avgLkp':>7}  {'maxLkp':>7}"
        f"  {'build Mrows/s':>13}  {'probe Mops/s':>12}"
    )

    for scenario_offset, scenario in enumerate(scenarios):
        build_keys, hit_probe_keys = _build_workload(
            rows=scenario.rows,
            unique_keys=scenario.unique_keys,
            probe_count=scenario.probe_count,
            seed=seed + scenario_offset,
        )
        expected_rows_seen = _expected_probe_rows(build_keys, hit_probe_keys)

        for adapter in adapters:
            result = _benchmark_adapter(
                adapter, scenario.name, scenario.unique_keys, build_keys, hit_probe_keys, repeat
            )
            if result.rows_seen != expected_rows_seen:
                raise RuntimeError(
                    f"{adapter.name} returned {result.rows_seen} rows during probe, expected {expected_rows_seen}"
                )
            results.append(result)

            build_mrows_s = result.rows / (result.build_mean_ms / 1000.0) / 1_000_000.0
            probe_mops_s = result.probe_count / (result.probe_mean_ms / 1000.0) / 1_000_000.0
            bytes_per_entry = (
                result.bytes_estimate / result.index_size
                if result.bytes_estimate is not None and result.index_size not in (None, 0)
                else None
            )
            print(
                f"{result.scenario:<{scenario_width}}  {result.implementation:<{impl_width}}  {result.rows:9d}  {scenario.unique_keys:9d}"
                f"  {result.probe_count:9d}  {result.build_best_ms:11.2f}  {result.build_mean_ms:11.2f}"
                f"  {result.probe_best_ms:11.2f}  {result.probe_mean_ms:11.2f}  {result.rows_seen:10d}"
                f"  {_format_int_or_na(result.index_size, 9)}  {_format_int_or_na(result.capacity, 10)}"
                f"  {_format_int_or_na(result.bytes_estimate, 12)}  {_format_float_or_na(bytes_per_entry, 9)}"
                f"  {_format_float_or_na(result.average_lookup_probe_length, 7, 2)}"
                f"  {_format_int_or_na(result.max_lookup_probe_length, 7)}"
                f"  {build_mrows_s:13.2f}  {probe_mops_s:12.2f}"
            )

    return results


def benchmark_locality_sweep(
    repeat: int = 5,
    probe_count: int = 100_000,
    seed: int = 7,
    cpp_module: str | None = None,
    sweep_rows: tuple[int, ...] = DEFAULT_LOCALITY_SWEEP_ROWS,
    cpp_probe_load_factor: float | None = None,
) -> list[BenchmarkResult]:
    scenarios = tuple(
        Scenario(
            f"medium-dup rows={row_count}",
            row_count,
            max(1, row_count // 32),
            probe_count,
        )
        for row_count in sweep_rows
    )

    adapters: list[_Adapter] = [AbseilAdapter()]
    for module_name in _resolve_cpp_modules(cpp_module):
        try:
            adapters.append(CppCarcharAdapter(module_name, cpp_probe_load_factor))
        except Exception as err:
            print(f"[skip] unable to load compiled Carchar module {module_name!r}: {err}")

    impl_width = max(34, max(len(adapter.name) for adapter in adapters) + 2)
    scenario_width = max(20, max(len(scenario.name) for scenario in scenarios) + 2)
    results: list[BenchmarkResult] = []

    print("=" * 146)
    print("Carchar Locality Sweep (medium-dup ratio, varying build rows only)")
    print("=" * 146)
    print(
        f"{'scenario':<{scenario_width}}  {'impl':<{impl_width}}  {'rows':>9}  {'uniq':>9}  {'probes':>9}"
        f"  {'build mean':>11}  {'probe mean':>11}  {'rows seen':>10}  {'entries':>9}  {'capacity':>10}"
        f"  {'bytes':>12}  {'B/entry':>9}  {'avgLkp':>7}  {'maxLkp':>7}  {'build Mrows/s':>13}  {'probe Mops/s':>12}"
    )

    for scenario_offset, scenario in enumerate(scenarios):
        build_keys, hit_probe_keys = _build_workload(
            rows=scenario.rows,
            unique_keys=scenario.unique_keys,
            probe_count=scenario.probe_count,
            seed=seed + scenario_offset,
        )
        expected_rows_seen = _expected_probe_rows(build_keys, hit_probe_keys)

        for adapter in adapters:
            result = _benchmark_adapter(
                adapter, scenario.name, scenario.unique_keys, build_keys, hit_probe_keys, repeat
            )
            if result.rows_seen != expected_rows_seen:
                raise RuntimeError(
                    f"{adapter.name} returned {result.rows_seen} rows during probe, expected {expected_rows_seen}"
                )
            results.append(result)

            build_mrows_s = result.rows / (result.build_mean_ms / 1000.0) / 1_000_000.0
            probe_mops_s = result.probe_count / (result.probe_mean_ms / 1000.0) / 1_000_000.0
            bytes_per_entry = (
                result.bytes_estimate / result.index_size
                if result.bytes_estimate is not None and result.index_size not in (None, 0)
                else None
            )
            print(
                f"{result.scenario:<{scenario_width}}  {result.implementation:<{impl_width}}  {result.rows:9d}  {scenario.unique_keys:9d}"
                f"  {result.probe_count:9d}  {result.build_mean_ms:11.2f}  {result.probe_mean_ms:11.2f}"
                f"  {result.rows_seen:10d}  {_format_int_or_na(result.index_size, 9)}  {_format_int_or_na(result.capacity, 10)}"
                f"  {_format_int_or_na(result.bytes_estimate, 12)}  {_format_float_or_na(bytes_per_entry, 9)}"
                f"  {_format_float_or_na(result.average_lookup_probe_length, 7, 2)}"
                f"  {_format_int_or_na(result.max_lookup_probe_length, 7)}"
                f"  {build_mrows_s:13.2f}  {probe_mops_s:12.2f}"
            )

    return results


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Benchmark Abseil and native C++ Carchar hash maps")
    parser.add_argument("--rows", type=int, default=300_000)
    parser.add_argument("--probe-count", type=int, default=100_000)
    parser.add_argument("--repeat", type=int, default=5)
    parser.add_argument("--seed", type=int, default=7)
    parser.add_argument(
        "--locality-sweep",
        action="store_true",
        help="Run a medium-dup locality sweep varying only build-side row count",
    )
    parser.add_argument(
        "--sweep-rows",
        type=int,
        nargs="+",
        default=list(DEFAULT_LOCALITY_SWEEP_ROWS),
        help="Row counts for --locality-sweep",
    )
    parser.add_argument(
        "--cpp-module",
        type=str,
        default=None,
        help="Optional import path for a compiled Carchar module",
    )
    parser.add_argument(
        "--cpp-probe-load-factor",
        type=float,
        default=None,
        help="Optional probe-side compaction load factor for CarcharJoinEngine",
    )
    args = parser.parse_args()
    if args.locality_sweep:
        benchmark_locality_sweep(
            probe_count=args.probe_count,
            repeat=args.repeat,
            seed=args.seed,
            cpp_module=args.cpp_module,
            sweep_rows=tuple(args.sweep_rows),
            cpp_probe_load_factor=args.cpp_probe_load_factor,
        )
    else:
        benchmark(
            rows=args.rows,
            probe_count=args.probe_count,
            repeat=args.repeat,
            seed=args.seed,
            cpp_module=args.cpp_module,
            cpp_probe_load_factor=args.cpp_probe_load_factor,
        )
