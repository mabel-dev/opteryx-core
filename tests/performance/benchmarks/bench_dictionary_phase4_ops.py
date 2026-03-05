"""
Phase 4 dictionary benchmark harness.

Compares dictionary fastpath vs compatibility/materialized paths for:
1. Numeric dictionary range operators.
2. String dictionary LIKE/ILIKE operators.

Run with:
    python tests/performance/benchmarks/bench_dictionary_phase4_ops.py
"""

import os
import statistics
import sys
import time

import pyarrow as pa

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

from opteryx.managers.expression.ops import _inner_filter_operations


def _measure(fn, repeat: int = 8):
    fn()  # warm-up
    samples = []
    for _ in range(repeat):
        start = time.perf_counter()
        fn()
        samples.append((time.perf_counter() - start) * 1000.0)
    return statistics.mean(samples)


def _build_numeric_dictionary_array(rows: int, cardinality: int):
    dictionary = pa.array([float(i) + 0.5 for i in range(cardinality)], type=pa.float64())
    indices = []
    for i in range(rows):
        if i % 97 == 0:
            indices.append(None)
        else:
            indices.append(i % cardinality)
    return pa.DictionaryArray.from_arrays(pa.array(indices, type=pa.int32()), dictionary)


def _build_string_dictionary_array(rows: int, cardinality: int):
    dictionary = pa.array([f"v{i:05d}" for i in range(cardinality)], type=pa.string())
    indices = []
    for i in range(rows):
        if i % 89 == 0:
            indices.append(None)
        else:
            indices.append(i % cardinality)
    return pa.DictionaryArray.from_arrays(pa.array(indices, type=pa.int32()), dictionary)


def benchmark_numeric_range(rows: int = 400_000, cardinalities=(32, 512, 8192)):
    print("=" * 88)
    print("Numeric Dictionary Range Benchmark (Lt / GtEq)")
    print("=" * 88)
    print(f"{'cardinality':>12}  {'dict-fast(ms)':>14}  {'materialized(ms)':>16}")

    for cardinality in cardinalities:
        dict_arr = _build_numeric_dictionary_array(rows, cardinality)
        mat_arr = dict_arr.dictionary_decode()
        literal = float(cardinality // 2) + 0.25

        fast_ms = _measure(
            lambda: (
                _inner_filter_operations(dict_arr, "Lt", literal),
                _inner_filter_operations(dict_arr, "GtEq", literal),
            )
        )

        mat_ms = _measure(
            lambda: (
                _inner_filter_operations(mat_arr, "Lt", literal),
                _inner_filter_operations(mat_arr, "GtEq", literal),
            )
        )

        print(f"{cardinality:12d}  {fast_ms:14.2f}  {mat_ms:16.2f}")


def benchmark_string_patterns(rows: int = 400_000, cardinalities=(32, 512, 8192)):
    print()
    print("=" * 88)
    print("String Dictionary Pattern Benchmark (Like / ILike)")
    print("=" * 88)
    print(f"{'cardinality':>12}  {'dict-fast(ms)':>14}  {'materialized(ms)':>16}")

    for cardinality in cardinalities:
        dict_arr = _build_string_dictionary_array(rows, cardinality)
        mat_arr = dict_arr.dictionary_decode()
        like_pattern = "v000%"
        ilike_pattern = "V000%"

        fast_ms = _measure(
            lambda: (
                _inner_filter_operations(dict_arr, "Like", like_pattern),
                _inner_filter_operations(dict_arr, "ILike", ilike_pattern),
            )
        )

        mat_ms = _measure(
            lambda: (
                _inner_filter_operations(mat_arr, "Like", like_pattern),
                _inner_filter_operations(mat_arr, "ILike", ilike_pattern),
            )
        )

        print(f"{cardinality:12d}  {fast_ms:14.2f}  {mat_ms:16.2f}")


if __name__ == "__main__":
    benchmark_numeric_range()
    benchmark_string_patterns()
