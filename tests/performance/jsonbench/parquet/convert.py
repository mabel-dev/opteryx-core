#!/usr/bin/env python3
"""
Load the JSONBench Bluesky NDJSON into Parquet — the "loaded table" half of the
JSONBench comparison.

Why this exists
---------------
The upstream JSONBench leaderboard times queries against an ALREADY-LOADED
table. ClickHouse's `JSON` type, Doris's `VARIANT` and StarRocks's Flat-JSON all
shred each document into per-path, typed, compressed columnar subcolumns AT
INSERT. "Retain structure" on that leaderboard means the paths stay queryable
without declaring a schema — it does not mean the JSON text is what gets stored,
and it does not mean a document is parsed at query time. Nothing is.

`../runner.py` measures the opposite operation: a full scan-and-parse of raw
NDJSON on every iteration, with no persisted storage. That is a real and useful
number, but it is not the number the leaderboard's query column is reporting.
This script produces the missing counterpart — the same dataset as a real
columnar table — so `./runner.py` can measure the operation the other engines
are actually measuring.

The conversion runs entirely through Opteryx + rugo: READ_JSONL for the parse,
`->>` for the path extraction, rugo's streaming Parquet writer for the output.
No PyArrow, no external loader. The load cost reported here is therefore our own
load cost and belongs in any honest comparison alongside the query times.

Variants
--------
`full`   — every field the JSONL reader can see, with the free-form
           `commit.record` payload retained verbatim as JSON text. This is the
           lossless-as-we-can-get load; its size and load time are the ones to
           quote against another engine's load column.
`narrow` — only the five columns the five queries read. This is what a
           schema-tuned load looks like, and the pair exists to demonstrate that
           the difference between them at QUERY time is ~nothing: column
           projection means the unread payload is never touched.

Known fidelity gap (deliberately not papered over)
--------------------------------------------------
READ_JSONL discovers its column set from the first `infer_sample_size` records
(default 5). In this dataset `identity` — carried only by kind='identity'
records, ~0.53% of rows — does not appear that early, so at the default it is
not a column and `SELECT identity` raises ColumnNotFoundError. Those rows still
load, with a NULL `commit_*` set; only their `identity` payload is dropped. No
JSONBench query reads it.

This conversion deliberately stays at the DEFAULT sample rather than raising it
to capture `identity`: the point of the comparison is what the engine does out
of the box, and quietly widening the sample here would make the load look
lossless while hiding that the default does not reach a 0.53% key. Pass
`infer_sample_size => 200` to READ_JSONL if you want that column — verified to
work, including through chunked streaming, where absent records read as NULL.
The gap is reported at the end of every run rather than silently absorbed.

Usage:
    python tests/performance/jsonbench/parquet/convert.py --size 10
    python tests/performance/jsonbench/parquet/convert.py --size 10 --variant narrow
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

_JSONL_DIR = os.path.join(_REPO_ROOT, "testdata", "_downloads", "jsonbench", "decompressed")
_PARQUET_DIR = os.path.join(_REPO_ROOT, "testdata", "_downloads", "jsonbench", "parquet")

VARIANTS = ("full", "narrow")

# The five columns every query reads. `narrow` stops here.
_QUERY_COLUMNS = """
        did,
        time_us,
        kind,
        commit ->> 'operation'  AS commit_operation,
        commit ->> 'collection' AS commit_collection
"""

# `full` additionally carries the rest of the commit object, including the
# free-form `record` payload as JSON text — the bulk of the bytes, and the part
# no query touches.
_PAYLOAD_COLUMNS = """,
        commit ->> 'rev'    AS commit_rev,
        commit ->> 'rkey'   AS commit_rkey,
        commit ->> 'cid'    AS commit_cid,
        commit ->> 'record' AS commit_record
"""


class _RowCounter:
    """Pass morsels through to the writer, tallying rows on the way.

    The writer consumes the iterable itself, so the row count has to be
    collected in-line rather than by materialising the stream — the whole point
    of write_parquet_stream is that a shard never has to be resident at once.
    """

    def __init__(self, morsels):
        self._morsels = morsels
        self.rows = 0

    def __iter__(self):
        for morsel in self._morsels:
            self.rows += morsel.num_rows
            yield morsel


def target_dir(size: int, variant: str) -> str:
    return os.path.join(_PARQUET_DIR, variant, f"{size}shards")


def convert(size: int, variant: str, compression: str = "zstd") -> tuple[float, int, int]:
    """Convert `size` JSONL shards to Parquet. Returns (seconds, bytes, rows)."""
    import opteryx
    from rugo.parquet import write_parquet_stream

    sources = sorted(glob.glob(os.path.join(_JSONL_DIR, "file_*.jsonl")))[:size]
    if len(sources) < size:
        raise SystemExit(
            f"expected {size} decompressed shard(s) in {_JSONL_DIR}, found {len(sources)} "
            f"— run `make jsonbench-data JSONBENCH_SIZE={size}` first"
        )

    out_dir = target_dir(size, variant)
    # Rebuilt from scratch every run: a partial file left by an interrupted
    # conversion would otherwise be picked up by the runner's glob and silently
    # benchmark a truncated dataset.
    if os.path.isdir(out_dir):
        shutil.rmtree(out_dir)
    os.makedirs(out_dir)

    projection = _QUERY_COLUMNS + (_PAYLOAD_COLUMNS if variant == "full" else "")

    total_rows = 0
    started = time.monotonic()
    for source in sources:
        out_path = os.path.join(out_dir, os.path.basename(source).replace(".jsonl", ".parquet"))
        sql = f"""
            SELECT {projection}
            FROM READ_JSONL('{source}', ignore_errors => true)
        """
        session = opteryx.session()
        counted = _RowCounter(session.execute_to_morsels(sql))

        with open(out_path, "wb") as handle:
            row_groups = write_parquet_stream(counted, handle.write, compression=compression)
        rows = counted.rows

        total_rows += rows
        print(
            f"  {os.path.basename(source):>16} -> {os.path.basename(out_path):<19} "
            f"{rows:>9,} rows  {row_groups:>3} row groups  "
            f"{os.path.getsize(out_path) / 1e6:>7.1f} MB"
        )

    elapsed = time.monotonic() - started
    written = sum(os.path.getsize(p) for p in glob.glob(os.path.join(out_dir, "*.parquet")))
    return elapsed, written, total_rows


def main() -> int:
    parser = argparse.ArgumentParser(description="Load JSONBench Bluesky NDJSON into Parquet")
    parser.add_argument("--size", type=int, default=10, choices=(1, 10, 100), help="Shards (millions of rows)")
    parser.add_argument("--variant", choices=VARIANTS + ("both",), default="both")
    parser.add_argument("--compression", default="zstd", choices=("zstd", "none"))
    args = parser.parse_args()

    variants = VARIANTS if args.variant == "both" else (args.variant,)
    source_bytes = sum(
        os.path.getsize(p) for p in sorted(glob.glob(os.path.join(_JSONL_DIR, "file_*.jsonl")))[: args.size]
    )

    for variant in variants:
        print(f"\n\033[1mLoading {args.size}m rows -> Parquet ({variant}, {args.compression})\033[0m")
        elapsed, written, rows = convert(args.size, variant, args.compression)
        print(
            f"  \033[1mtotal\033[0m  {rows:,} rows  "
            f"{source_bytes / 1e9:.2f}GB JSONL -> {written / 1e9:.2f}GB Parquet "
            f"({source_bytes / written:.1f}x smaller)  in {elapsed:.1f}s "
            f"({source_bytes / 1e6 / elapsed:.0f} MB/s)"
        )
        print(f"  {os.path.relpath(target_dir(args.size, variant), _REPO_ROOT)}")

    print(
        "\n\033[38;2;255;184;108mNote:\033[0m READ_JSONL discovers columns from the first "
        "`infer_sample_size` records (default 5), so the `identity` key (kind='identity', ~0.53% of rows) "
        "is not reached at the default and its payload is not carried into Parquet. Those rows load with "
        "NULL commit_* columns. No JSONBench query reads it; `infer_sample_size => 200` would capture it."
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
