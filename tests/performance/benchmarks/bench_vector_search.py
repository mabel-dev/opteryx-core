"""
Micro-benchmark for the native exact vector-search baseline.

Measures end-to-end top-k search over a dense float32 matrix for a range of
candidate sizes.

Run with:
    python tests/performance/benchmarks/bench_vector_search.py
    python tests/performance/benchmarks/bench_vector_search.py --rows 1000000 --dims 384 --repeat 5
"""

from __future__ import annotations

import argparse
import os
import statistics
import sys
import time

import numpy

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

from opteryx.compiled.nanobind import vector_search

DEFAULT_ROWS = (1_000, 10_000, 100_000, 1_000_000)


def _measure_ms(fn, repeat: int) -> tuple[float, float]:
    fn()
    samples = []
    for _ in range(repeat):
        t0 = time.perf_counter_ns()
        fn()
        samples.append((time.perf_counter_ns() - t0) / 1_000_000.0)
    return min(samples), statistics.mean(samples)


def _build_workload(
    rows: int, dims: int, seed: int
) -> tuple[numpy.ndarray, numpy.ndarray, numpy.ndarray]:
    rng = numpy.random.default_rng(seed)
    query = rng.standard_normal(size=dims, dtype=numpy.float32)
    vectors = rng.standard_normal(size=(rows, dims), dtype=numpy.float32)
    row_ids = numpy.arange(rows, dtype=numpy.int64)
    return query, row_ids, vectors


def _run_case(rows: int, dims: int, k: int, repeat: int, seed: int) -> None:
    query, row_ids, vectors = _build_workload(rows, dims, seed)

    def run_once():
        vector_search.exact_search_cosine(query, row_ids, vectors, k)

    best_ms, avg_ms = _measure_ms(run_once, repeat)
    best_rps = rows / (best_ms / 1000.0)
    avg_rps = rows / (avg_ms / 1000.0)
    print(
        f"rows={rows:>9,d} dims={dims} k={k:<3d} "
        f"best={best_ms:>8.2f} ms avg={avg_ms:>8.2f} ms "
        f"best_rows_per_sec={best_rps:>12,.0f} avg_rows_per_sec={avg_rps:>12,.0f}"
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--rows", type=int, default=0, help="Benchmark a single candidate size")
    parser.add_argument("--dims", type=int, default=384)
    parser.add_argument("--k", type=int, default=20)
    parser.add_argument("--repeat", type=int, default=3)
    parser.add_argument("--seed", type=int, default=7)
    args = parser.parse_args()

    rows_values = (args.rows,) if args.rows > 0 else DEFAULT_ROWS
    for rows in rows_values:
        _run_case(rows=rows, dims=args.dims, k=args.k, repeat=args.repeat, seed=args.seed)


if __name__ == "__main__":
    main()
