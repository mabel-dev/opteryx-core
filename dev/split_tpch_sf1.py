#!/usr/bin/env python3
"""
One-shot: split TPC-H SF=1 parquet files into chunks small enough that GitHub
will accept them (under the 50MB soft limit).

Reads each table under testdata/tpch_1/<table>/, splits the single .parquet
file into N chunks, and writes them as <table>_NNN.parquet in the same
directory. The original single-file parquet is removed.

Uses PyArrow to write — DuckDB's COPY ... TO ... (FORMAT PARQUET) wraps
columns in a `duckdb_schema` group node that Opteryx's parquet reader exposes
as a column-name prefix (so columns appear as `duckdb_schema.o_custkey` rather
than `o_custkey`). PyArrow's `write_table` writes a flat schema. PyArrow is
banned in production code but allowed in dev tooling.

Run from repo root:
    python dev/split_tpch_sf1.py

Targets ~25MB per chunk for headroom.
"""

from __future__ import annotations

import os
import sys

# Cap per-chunk parquet output. Stay well under GitHub's 50MB soft limit.
TARGET_BYTES = 25 * 1024 * 1024

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
TPCH_DIR = os.path.join(ROOT, "testdata", "tpch_1")


def split_table(table_dir: str) -> None:
    import pyarrow as pa
    import pyarrow.parquet as pq

    parquet_files = sorted(
        os.path.join(table_dir, f) for f in os.listdir(table_dir) if f.endswith(".parquet")
    )
    if not parquet_files:
        print(f"  (no parquet files in {table_dir})")
        return

    table_name = os.path.basename(table_dir)

    # Read everything (the source might already be split — we re-read all chunks
    # and rewrite, which lets us correct earlier writes that included the
    # `duckdb_schema` wrapper).
    table = pq.read_table(parquet_files[0]) if len(parquet_files) == 1 else pq.read_table(table_dir)

    # Coerce all fields to non-null (parquet `required`). TPC-H columns are
    # all NOT NULL by spec, and Opteryx's parquet decoder produces "Decoder
    # returned None" / "row_validity length must match codes length" errors
    # on PyArrow-written nullable columns where it doesn't on the equivalent
    # parquet-rs (tpchgen-cli) output.
    fields = [pa.field(f.name, f.type, nullable=False, metadata=f.metadata) for f in table.schema]
    table = table.cast(pa.schema(fields, metadata=table.schema.metadata))

    total_rows = table.num_rows
    total_size = sum(os.path.getsize(p) for p in parquet_files)

    if total_size <= TARGET_BYTES and len(parquet_files) == 1:
        print(
            f"  {os.path.basename(parquet_files[0])} is {total_size / 1024 / 1024:.1f}MB "
            "— under cap, skipping"
        )
        return

    # Decide chunk count from current total size; ZSTD typically compresses
    # well, so this is conservative.
    n_chunks = max(1, (total_size + TARGET_BYTES - 1) // TARGET_BYTES)
    rows_per_chunk = (total_rows + n_chunks - 1) // n_chunks
    print(
        f"  source {total_size / 1024 / 1024:.1f}MB, {total_rows:,} rows "
        f"→ {n_chunks} chunks of ~{rows_per_chunk:,} rows"
    )

    written = []
    for i in range(n_chunks):
        offset = i * rows_per_chunk
        length = min(rows_per_chunk, total_rows - offset)
        if length <= 0:
            break
        chunk = table.slice(offset, length)
        out_path = os.path.join(table_dir, f"{table_name}_{i:03d}.parquet")
        # Encoding/compression knobs chosen to round-trip cleanly through
        # Opteryx's parquet decoder:
        #   - Snappy compression (universally supported in the decoder).
        #   - Dictionary encoding enabled (matches what tpchgen-cli writes).
        #   - row_group_size capped — Opteryx's decoder fails on the very
        #     large row groups (~500K rows / 66MB) PyArrow writes by default
        #     for DECIMAL columns; smaller row groups round-trip cleanly.
        pq.write_table(
            chunk,
            out_path,
            compression="snappy",
            use_dictionary=True,
            row_group_size=64 * 1024,
        )
        out_size = os.path.getsize(out_path)
        if out_size > 50 * 1024 * 1024:
            print(
                f"    chunk {i:03d}: {out_size / 1024 / 1024:.1f}MB OVER 50MB CAP — aborting"
            )
            for p in written + [out_path]:
                os.remove(p)
            sys.exit(1)
        print(
            f"    chunk {i:03d}: {out_size / 1024 / 1024:.1f}MB ({length:,} rows)"
        )
        written.append(out_path)

    # Verify total row count matches.
    new_total = sum(pq.read_table(p).num_rows for p in written)
    if new_total != total_rows:
        print(f"  ROW COUNT MISMATCH: source={total_rows:,} chunks={new_total:,} — aborting")
        for p in written:
            os.remove(p)
        sys.exit(1)

    # Remove any source files that are no longer one of the chunks we just wrote.
    for old in parquet_files:
        if old not in written:
            os.remove(old)
    print(f"  ✓ {len(written)} chunks written")


def main() -> None:
    if not os.path.isdir(TPCH_DIR):
        print(f"ERROR: {TPCH_DIR} does not exist")
        sys.exit(1)

    print(f"Splitting TPC-H SF=1 files under {TPCH_DIR}")
    print(f"Target chunk size: {TARGET_BYTES / 1024 / 1024:.0f}MB")
    print()

    for table in sorted(os.listdir(TPCH_DIR)):
        table_dir = os.path.join(TPCH_DIR, table)
        if not os.path.isdir(table_dir):
            continue
        print(f"{table}:")
        split_table(table_dir)
        print()


if __name__ == "__main__":
    main()
