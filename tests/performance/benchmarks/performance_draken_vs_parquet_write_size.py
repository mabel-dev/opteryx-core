"""
Benchmark DRKM vs Parquet write/read speed and file size.

Goal: answer "are we faster than parquet, or not?" for local spill-style IO.
"""

from __future__ import annotations

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

import argparse
import statistics
import tempfile
import time
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from opteryx.draken import Morsel
from opteryx.draken.storage import read_morsel
from opteryx.draken.storage import write_morsel


def _median_ms(values_ns):
    return statistics.median(values_ns) / 1_000_000.0


def _time_ns(fn):
    start = time.perf_counter_ns()
    fn()
    return time.perf_counter_ns() - start


def _print_section(title):
    print("\n" + title)
    print("-" * len(title))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--iterations", type=int, default=25)
    parser.add_argument("--rows", type=int, default=0, help="0 means all rows")
    parser.add_argument("--draken-codec", default="lz4", choices=("lz4", "zstd", "none"))
    parser.add_argument(
        "--parquet-compression",
        default="lz4",
        choices=("snappy", "zstd", "lz4", "none"),
    )
    args = parser.parse_args()

    root = Path(__file__).resolve().parents[3]
    source = root / "testdata" / "flat" / "formats" / "parquet" / "tweets.parquet"
    table = pq.read_table(source).combine_chunks()
    supported_columns = [
        field.name
        for field in table.schema
        if not pa.types.is_list(field.type) and not pa.types.is_large_list(field.type)
    ]
    table = table.select(supported_columns)
    if args.rows > 0:
        table = table.slice(0, args.rows)

    morsel = Morsel.from_arrow(table)
    parquet_compression = None if args.parquet_compression == "none" else args.parquet_compression

    draken_write_ns = []
    parquet_write_ns = []
    draken_read_ns = []
    parquet_read_ns = []
    draken_sizes = []
    parquet_sizes = []

    with tempfile.TemporaryDirectory(prefix="draken-vs-parquet-") as td:
        temp_dir = Path(td)

        for i in range(args.iterations):
            drkm_path = temp_dir / f"tweets-{i}.drkm"
            parquet_path = temp_dir / f"tweets-{i}.parquet"

            draken_write_ns.append(
                _time_ns(
                    lambda: write_morsel(
                        drkm_path,
                        morsel,
                        {"codec_default": args.draken_codec, "checksum_enabled": True},
                    )
                )
            )
            draken_sizes.append(drkm_path.stat().st_size)

            parquet_write_ns.append(
                _time_ns(lambda: pq.write_table(table, parquet_path, compression=parquet_compression))
            )
            parquet_sizes.append(parquet_path.stat().st_size)

            draken_read_ns.append(_time_ns(lambda: read_morsel(drkm_path)))
            parquet_read_ns.append(_time_ns(lambda: pq.read_table(parquet_path)))

    draken_write_ms = _median_ms(draken_write_ns)
    parquet_write_ms = _median_ms(parquet_write_ns)
    draken_read_ms = _median_ms(draken_read_ns)
    parquet_read_ms = _median_ms(parquet_read_ns)
    draken_size = int(statistics.median(draken_sizes))
    parquet_size = int(statistics.median(parquet_sizes))

    _print_section("Configuration")
    print(f"rows={table.num_rows}")
    print(f"iterations={args.iterations}")
    print(f"draken_codec={args.draken_codec}")
    print(f"parquet_compression={args.parquet_compression}")

    _print_section("Results")
    print(f"draken_write_ms_median={draken_write_ms:.2f}")
    print(f"parquet_write_ms_median={parquet_write_ms:.2f}")
    print(f"draken_read_ms_median={draken_read_ms:.2f}")
    print(f"parquet_read_ms_median={parquet_read_ms:.2f}")
    print(f"draken_size_bytes_median={draken_size}")
    print(f"parquet_size_bytes_median={parquet_size}")

    _print_section("Verdict")
    if draken_write_ms < parquet_write_ms:
        print("WRITE: Draken is faster than Parquet.")
    else:
        print("WRITE: Parquet is faster than Draken.")

    if draken_read_ms < parquet_read_ms:
        print("READ: Draken is faster than Parquet.")
    else:
        print("READ: Parquet is faster than Draken.")

    if draken_size < parquet_size:
        print("SIZE: Draken file is smaller.")
    else:
        print("SIZE: Parquet file is smaller.")


if __name__ == "__main__":
    main()
