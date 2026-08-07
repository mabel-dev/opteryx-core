#!/usr/bin/env python3
"""
Convert a parquet dataset tree to its skene mirror.

Walks a source tree of `<table>/<file>.parquet` datasets (the testdata TPC-H
layout) and writes `<table>/<file>-rgNNNN.skene` alongside-structure into the
destination — one `.skene` file per parquet ROW GROUP, because one skene file
IS one row group (skene/FORMAT.md). Written with the storage posture
(read_acceleration + zstd-1), the same settings an optimised dataset uses.

Usage:
    python dev/parquet_to_skene.py testdata/tpch_1 testdata/tpch_1_skene

Dev tooling only — never imported by production code (repo rules §5).
"""

from __future__ import annotations

import os
import sys
import time

_REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, _REPO_ROOT)

import skene  # noqa: E402
from rugo.parquet import read_parquet  # noqa: E402


def convert_file(parquet_path: str, out_dir: str) -> tuple[int, int, int]:
    """One parquet file → one .skene per row group. Returns (files, rows, bytes)."""
    stem = os.path.splitext(os.path.basename(parquet_path))[0]
    files = rows = nbytes = 0
    with read_parquet(parquet_path) as reader:
        for rg_index, morsel in enumerate(reader):
            payload = skene.write_morsel(morsel, read_acceleration=True, zstd_level=1)
            out_path = os.path.join(out_dir, f"{stem}-rg{rg_index:04d}.skene")
            with open(out_path, "wb") as out:
                out.write(payload)
            files += 1
            rows += morsel.num_rows
            nbytes += len(payload)
    if files == 0:
        raise RuntimeError(f"{parquet_path}: no row groups read — refusing to write an empty table")
    return files, rows, nbytes


def main() -> int:
    if len(sys.argv) != 3:
        print(__doc__)
        return 1
    src_root, dst_root = sys.argv[1], sys.argv[2]
    if not os.path.isdir(src_root):
        print(f"ERROR: source dataset tree not found: {src_root}")
        return 1

    started = time.monotonic()
    total_files = total_rows = total_bytes = src_bytes = 0
    for dirpath, _dirnames, filenames in sorted(os.walk(src_root)):
        parquet_files = sorted(f for f in filenames if f.lower().endswith(".parquet"))
        if not parquet_files:
            continue
        rel = os.path.relpath(dirpath, src_root)
        out_dir = os.path.join(dst_root, rel) if rel != "." else dst_root
        os.makedirs(out_dir, exist_ok=True)
        for name in parquet_files:
            src_path = os.path.join(dirpath, name)
            files, rows, nbytes = convert_file(src_path, out_dir)
            src_size = os.path.getsize(src_path)
            src_bytes += src_size
            total_files += files
            total_rows += rows
            total_bytes += nbytes
            print(
                f"  {os.path.join(rel, name):<40} -> {files:>3} skene file(s), "
                f"{rows:>9,} rows, {src_size / 1e6:8.1f}MB -> {nbytes / 1e6:8.1f}MB"
            )

    if total_files == 0:
        print(f"ERROR: no .parquet files found under {src_root}")
        return 1

    elapsed = time.monotonic() - started
    print(
        f"\nConverted {total_rows:,} rows into {total_files} skene files in {elapsed:.1f}s "
        f"({src_bytes / 1e6:.1f}MB parquet -> {total_bytes / 1e6:.1f}MB skene, "
        f"{total_bytes / src_bytes:.2f}x)"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
