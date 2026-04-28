"""
Phase 3 dictionary group-by benchmark harness.

Compares dictionary group-by fast path vs materialized keys for:
1. GROUP BY key, COUNT(*)
2. GROUP BY key, COUNT(DISTINCT value)

Run with:
    python tests/performance/benchmarks/bench_dictionary_phase3_groupby.py
"""

import os
import statistics
import sys
import time

import pyarrow as pa

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

from draken.morsels.morsel import Morsel
from opteryx.operators.group_state_store import ShuffleGroupByOperationV2

from opteryx.operators.shuffle import AggregationSpec


def _measure(fn, repeat: int = 6):
    fn()  # warm-up
    samples = []
    for _ in range(repeat):
        start = time.perf_counter()
        fn()
        samples.append((time.perf_counter() - start) * 1000.0)
    return statistics.mean(samples)


def _normalize_key(value):
    if isinstance(value, bytes):
        return value.decode("utf-8")
    return value


def _rows_by_key(result_table, key_name: str, value_name: str):
    out = {}
    for row in result_table.to_pylist():
        out[_normalize_key(row[key_name])] = row[value_name]
    return out


def _run_group_by_count(table: pa.Table):
    op = ShuffleGroupByOperationV2(
        group_by_columns=["k"],
        aggregations=[AggregationSpec(alias="cnt", function="count", column="*")],
    )
    op.ingest(Morsel.from_arrow(table))
    return op.finalize().to_arrow()


def _run_group_by_count_distinct(table: pa.Table):
    op = ShuffleGroupByOperationV2(
        group_by_columns=["k"],
        aggregations=[AggregationSpec(alias="cd", function="count_distinct", column="v")],
    )
    op.ingest(Morsel.from_arrow(table))
    return op.finalize().to_arrow()


def _build_dictionary_table(rows: int, key_cardinality: int, value_cardinality: int):
    key_dict = pa.array([f"k{i:05d}" for i in range(key_cardinality)], type=pa.string())
    value_dict = pa.array([f"v{i:05d}" for i in range(value_cardinality)], type=pa.string())
    key_indices = []
    value_indices = []
    for i in range(rows):
        key_indices.append(i % key_cardinality)
        value_indices.append((i * 7) % value_cardinality)
    key = pa.DictionaryArray.from_arrays(pa.array(key_indices, type=pa.int32()), key_dict)
    value = pa.DictionaryArray.from_arrays(pa.array(value_indices, type=pa.int32()), value_dict)
    return pa.table({"k": key, "v": value})


def benchmark_group_by(
    rows: int = 300_000, key_cardinalities=(64, 1024, 8192), value_cardinality: int = 256
):
    print("=" * 96)
    print("Dictionary Group-By Benchmark (COUNT(*) and COUNT(DISTINCT))")
    print("=" * 96)
    print(
        f"{'key-card':>10}  {'count-fast(ms)':>14}  {'count-mat(ms)':>13}"
        f"  {'cd-fast(ms)':>11}  {'cd-mat(ms)':>10}  {'count-parity':>12}  {'cd-parity':>9}"
    )

    for key_cardinality in key_cardinalities:
        dict_table = _build_dictionary_table(rows, key_cardinality, value_cardinality)
        mat_table = pa.table(
            {
                "k": dict_table["k"].combine_chunks().dictionary_decode(),
                "v": dict_table["v"].combine_chunks().dictionary_decode(),
            }
        )

        fast_count_result = _run_group_by_count(dict_table)
        fast_cd_result = _run_group_by_count_distinct(dict_table)
        fast_count_ms = _measure(lambda: _run_group_by_count(dict_table))
        fast_cd_ms = _measure(lambda: _run_group_by_count_distinct(dict_table))

        mat_count_result = _run_group_by_count(mat_table)
        mat_cd_result = _run_group_by_count_distinct(mat_table)
        mat_count_ms = _measure(lambda: _run_group_by_count(mat_table))
        mat_cd_ms = _measure(lambda: _run_group_by_count_distinct(mat_table))

        count_parity = _rows_by_key(fast_count_result, "k", "cnt") == _rows_by_key(
            mat_count_result, "k", "cnt"
        )
        cd_parity = _rows_by_key(fast_cd_result, "k", "cd") == _rows_by_key(
            mat_cd_result, "k", "cd"
        )

        print(
            f"{key_cardinality:10d}  {fast_count_ms:14.2f}  {mat_count_ms:13.2f}"
            f"  {fast_cd_ms:11.2f}  {mat_cd_ms:10.2f}  {str(count_parity):>12}  {str(cd_parity):>9}"
        )


if __name__ == "__main__":
    benchmark_group_by()
