"""
Phase 3 dictionary spill benchmark harness.

Compares DRKM spill write/read time and size for dictionary-encoded morsels
versus materialized morsels with equivalent logical values.

Run with:
    python tests/performance/benchmarks/bench_dictionary_phase3_spill.py
"""

from __future__ import annotations

import argparse
import os
import statistics
import sys
import tempfile
import time
from pathlib import Path

import pyarrow as pa

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

from opteryx.compiled.draken import Morsel
from opteryx.compiled.draken.storage import read_morsel, write_morsel


def _median_ms(samples_ns: list[int]) -> float:
    return statistics.median(samples_ns) / 1_000_000.0


def _build_tables(
    rows: int, key_cardinality: int, value_cardinality: int
) -> tuple[pa.Table, pa.Table]:
    key_dictionary = pa.array([f"k{i:05d}" for i in range(key_cardinality)], type=pa.string())
    value_dictionary = pa.array([f"v{i:05d}" for i in range(value_cardinality)], type=pa.string())
    key_indices = [i % key_cardinality for i in range(rows)]
    value_indices = [(i * 7) % value_cardinality for i in range(rows)]
    metric_values = [float((i * 13) % 1000) for i in range(rows)]

    dictionary_table = pa.table(
        {
            "k": pa.DictionaryArray.from_arrays(
                pa.array(key_indices, type=pa.int32()), key_dictionary
            ),
            "v": pa.DictionaryArray.from_arrays(
                pa.array(value_indices, type=pa.int32()), value_dictionary
            ),
            "m": pa.array(metric_values, type=pa.float64()),
        }
    )
    materialized_table = pa.table(
        {
            "k": dictionary_table["k"].combine_chunks().dictionary_decode(),
            "v": dictionary_table["v"].combine_chunks().dictionary_decode(),
            "m": dictionary_table["m"].combine_chunks(),
        }
    )
    return dictionary_table, materialized_table


def _measure_spill_io(morsel: Morsel, codec: str, repeat: int) -> tuple[float, float, int]:
    write_samples_ns: list[int] = []
    read_samples_ns: list[int] = []
    sizes: list[int] = []

    with tempfile.TemporaryDirectory(prefix="dict-spill-bench-") as temp_dir:
        path = Path(temp_dir) / "payload.drkm"
        options = {"codec_default": codec, "checksum_enabled": False}

        write_morsel(path, morsel, options)
        _ = read_morsel(path, {"checksum_enabled": False})

        for _ in range(repeat):
            write_start = time.perf_counter_ns()
            write_morsel(path, morsel, options)
            write_samples_ns.append(time.perf_counter_ns() - write_start)
            sizes.append(path.stat().st_size)

            read_start = time.perf_counter_ns()
            _ = read_morsel(path, {"checksum_enabled": False})
            read_samples_ns.append(time.perf_counter_ns() - read_start)

    return _median_ms(write_samples_ns), _median_ms(read_samples_ns), int(statistics.median(sizes))


def benchmark_spill(
    rows: int, key_cardinalities: tuple[int, ...], value_cardinality: int, codec: str, repeat: int
):
    print("=" * 122)
    print("Dictionary Spill Benchmark (DRKM write/read + size)")
    print("=" * 122)
    print(
        f"{'key-card':>10}  {'dict-write(ms)':>14}  {'mat-write(ms)':>13}  "
        f"{'dict-read(ms)':>13}  {'mat-read(ms)':>12}  {'dict-size(MB)':>13}  {'mat-size(MB)':>12}"
    )

    for key_cardinality in key_cardinalities:
        dictionary_table, materialized_table = _build_tables(
            rows, key_cardinality, value_cardinality
        )
        dictionary_morsel = Morsel.from_arrow(dictionary_table)
        materialized_morsel = Morsel.from_arrow(materialized_table)

        dict_write_ms, dict_read_ms, dict_size = _measure_spill_io(dictionary_morsel, codec, repeat)
        mat_write_ms, mat_read_ms, mat_size = _measure_spill_io(materialized_morsel, codec, repeat)

        print(
            f"{key_cardinality:10d}  {dict_write_ms:14.2f}  {mat_write_ms:13.2f}  "
            f"{dict_read_ms:13.2f}  {mat_read_ms:12.2f}  "
            f"{dict_size / (1024 * 1024):13.2f}  {mat_size / (1024 * 1024):12.2f}"
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--rows", type=int, default=200_000)
    parser.add_argument("--value-cardinality", type=int, default=256)
    parser.add_argument("--key-cardinalities", type=int, nargs="+", default=[64, 1024, 8192])
    parser.add_argument("--codec", choices=("none", "lz4", "zstd"), default="lz4")
    parser.add_argument("--repeat", type=int, default=8)
    args = parser.parse_args()

    benchmark_spill(
        rows=args.rows,
        key_cardinalities=tuple(args.key_cardinalities),
        value_cardinality=args.value_cardinality,
        codec=args.codec,
        repeat=args.repeat,
    )
