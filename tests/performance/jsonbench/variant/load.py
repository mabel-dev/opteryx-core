#!/usr/bin/env python3
"""
JSONBench load, the legal form: each Bluesky record whole, in ONE `data` VARIANT column.

JSONBench disallows flattening JSON into several non-JSON columns at insert, which is
what ../parquet/convert.py does. Here each line is read whole
(`READ_JSONL(..., as_variant => 'data')`) and written by rugo's streaming Parquet
writer, which writes a VARIANT column as a shredded Parquet VARIANT
(docs/parquet_variant_design.md §5.4): the paths are typed columns inside the one
column, as ClickHouse's `JSON` type stores them.

One Parquet file per shard, under testdata/_downloads/jsonbench/variant/<n>shards/.
`ignore_errors => true` drops the dump's few malformed lines, as DuckDB's baseline
(`read_ndjson_objects(..., ignore_errors=true)`) does; the row count is printed.

Usage:
    python tests/performance/jsonbench/variant/load.py --size 10
    python tests/performance/jsonbench/variant/load.py --sources a.jsonl b.jsonl --out DIR
"""

from __future__ import annotations

import argparse
import glob
import os
import shutil
import sys
import time

_HERE = os.path.dirname(os.path.abspath(__file__))
_REPO_ROOT = os.path.abspath(os.path.join(_HERE, "..", "..", "..", ".."))
sys.path.insert(0, _REPO_ROOT)

JSONL_DIR = os.path.join(_REPO_ROOT, "testdata", "_downloads", "jsonbench", "decompressed")
VARIANT_DIR = os.path.join(_REPO_ROOT, "testdata", "_downloads", "jsonbench", "variant")


def target_dir(size: int) -> str:
    return os.path.join(VARIANT_DIR, f"{size}shards")


def shard_sources(size: int) -> list[str]:
    sources = sorted(glob.glob(os.path.join(JSONL_DIR, "file_*.jsonl")))[:size]
    if len(sources) < size:
        raise SystemExit(
            f"expected {size} decompressed shard(s) in {JSONL_DIR}, found {len(sources)} "
            f"— run `make jsonbench-data JSONBENCH_SIZE={size}` first"
        )
    return sources


def load(sources: list[str], out_dir: str) -> tuple[float, int, int]:
    """Write each source as one `data` VARIANT column. Returns (seconds, bytes, rows)."""
    import opteryx
    from rugo.parquet import write_parquet_stream

    # Rebuilt from scratch: a partial file from an interrupted load would be read by the
    # runner's glob as a truncated table.
    if os.path.isdir(out_dir):
        shutil.rmtree(out_dir)
    os.makedirs(out_dir)

    total_rows = 0
    started = time.monotonic()
    for source in sources:
        out_path = os.path.join(out_dir, os.path.basename(source).rsplit(".", 1)[0] + ".parquet")
        sql = f"SELECT data FROM READ_JSONL('{source}', as_variant => 'data', ignore_errors => true)"
        rows = 0

        def counted(morsels):
            nonlocal rows
            for morsel in morsels:
                rows += morsel.num_rows
                yield morsel

        with open(out_path, "wb") as handle:
            row_groups = write_parquet_stream(
                counted(opteryx.session().execute_to_morsels(sql)), handle.write, compression="zstd"
            )
        total_rows += rows
        print(
            f"  {os.path.basename(source):>16} -> {os.path.basename(out_path):<19} "
            f"{rows:>9,} rows  {row_groups:>3} row groups  {os.path.getsize(out_path) / 1e6:>7.1f} MB",
            flush=True,
        )
    elapsed = time.monotonic() - started
    written = sum(os.path.getsize(p) for p in glob.glob(os.path.join(out_dir, "*.parquet")))
    return elapsed, written, total_rows


def main() -> int:
    parser = argparse.ArgumentParser(description="Load JSONBench NDJSON into one VARIANT column")
    parser.add_argument("--size", type=int, default=10, help="Shards (millions of rows)")
    parser.add_argument("--sources", nargs="+", help="JSONL files to load instead of the shards")
    parser.add_argument("--out", help="Output directory (default: variant/<size>shards)")
    args = parser.parse_args()

    sources = args.sources or shard_sources(args.size)
    out_dir = args.out or target_dir(args.size)
    source_bytes = sum(os.path.getsize(p) for p in sources)
    print(f"\033[1mLoading {len(sources)} file(s) -> one `data` VARIANT column\033[0m", flush=True)
    elapsed, written, rows = load(sources, out_dir)
    print(
        f"  \033[1mtotal\033[0m  {rows:,} rows  {source_bytes / 1e9:.2f}GB JSONL -> "
        f"{written / 1e9:.2f}GB Parquet  in {elapsed:.1f}s ({source_bytes / 1e6 / elapsed:.0f} MB/s)"
    )
    print(f"  {out_dir}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
