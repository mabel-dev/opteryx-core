"""
Compare the exact dense-vector baseline against transient USearch build+search.

Run with:
    python tests/performance/benchmarks/bench_vector_search_compare.py
    python tests/performance/benchmarks/bench_vector_search_compare.py --rows 100000 --repeat 3
"""

from __future__ import annotations

import argparse
import importlib
import os
import statistics
import sys
import time

import numpy

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

usearch_native = importlib.import_module("opteryx.nanobind.usearch_native")
vector_search = importlib.import_module("opteryx.nanobind.vector_search")


DEFAULT_ROWS = (1_000, 5_000, 10_000, 50_000, 100_000)


def _measure_ms(fn, repeat: int) -> tuple[float, float]:
    fn()
    samples = []
    for _ in range(repeat):
        t0 = time.perf_counter_ns()
        fn()
        samples.append((time.perf_counter_ns() - t0) / 1_000_000.0)
    return min(samples), statistics.mean(samples)


def _build_workload(
    rows: int, dims: int, queries: int, seed: int
) -> tuple[numpy.ndarray, numpy.ndarray, numpy.ndarray]:
    rng = numpy.random.default_rng(seed)
    query = rng.standard_normal(size=(queries, dims), dtype=numpy.float32)
    vectors = rng.standard_normal(size=(rows, dims), dtype=numpy.float32)
    row_ids = numpy.arange(rows, dtype=numpy.int64)
    return query, row_ids, vectors


def _run_case(
    rows: int,
    dims: int,
    k: int,
    queries: int,
    repeat: int,
    seed: int,
    expansion_add: int,
    expansion_search: int,
) -> None:
    query_batch, row_ids, vectors = _build_workload(rows, dims, queries, seed)

    def exact_once():
        for query in query_batch:
            vector_search.exact_search_cosine(query, row_ids, vectors, k)

    def usearch_build_once():
        index = usearch_native.UsearchIndex(
            dimensions=dims,
            capacity=rows,
            metric="cos",
            expansion_add=expansion_add,
            expansion_search=expansion_search,
        )
        index.add_batch(row_ids, vectors)
        return index

    index = usearch_build_once()

    def usearch_search_once():
        for query in query_batch:
            index.search(query, k)

    def usearch_total_once():
        local_index = usearch_build_once()
        for query in query_batch:
            local_index.search(query, k)

    exact_best, exact_avg = _measure_ms(exact_once, repeat)
    usearch_build_best, usearch_build_avg = _measure_ms(usearch_build_once, repeat)
    usearch_search_best, usearch_search_avg = _measure_ms(usearch_search_once, repeat)
    usearch_total_best, usearch_total_avg = _measure_ms(usearch_total_once, repeat)

    exact_best_per_query = exact_best / queries
    exact_avg_per_query = exact_avg / queries
    usearch_search_best_per_query = usearch_search_best / queries
    usearch_search_avg_per_query = usearch_search_avg / queries

    print(
        f"rows={rows:>9,d} dims={dims} k={k:<3d} queries={queries:<3d} "
        f"exp_add={expansion_add:<3d} exp_search={expansion_search:<3d} "
        f"exact_best={exact_best:>8.2f} ms exact_avg={exact_avg:>8.2f} ms "
        f"exact_best_per_query={exact_best_per_query:>7.2f} ms "
        f"build_best={usearch_build_best:>8.2f} ms build_avg={usearch_build_avg:>8.2f} ms "
        f"search_best={usearch_search_best:>8.2f} ms search_avg={usearch_search_avg:>8.2f} ms "
        f"search_best_per_query={usearch_search_best_per_query:>7.2f} ms "
        f"total_best={usearch_total_best:>8.2f} ms total_avg={usearch_total_avg:>8.2f} ms",
        flush=True,
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--rows", type=int, default=0)
    parser.add_argument("--dims", type=int, default=384)
    parser.add_argument("--k", type=int, default=20)
    parser.add_argument("--queries", type=int, default=1)
    parser.add_argument("--repeat", type=int, default=3)
    parser.add_argument("--seed", type=int, default=11)
    parser.add_argument("--expansion-add", type=int, default=0)
    parser.add_argument("--expansion-search", type=int, default=0)
    args = parser.parse_args()

    rows_values = (args.rows,) if args.rows > 0 else DEFAULT_ROWS
    for rows in rows_values:
        _run_case(
            rows=rows,
            dims=args.dims,
            k=args.k,
            queries=args.queries,
            repeat=args.repeat,
            seed=args.seed,
            expansion_add=args.expansion_add,
            expansion_search=args.expansion_search,
        )


if __name__ == "__main__":
    main()
