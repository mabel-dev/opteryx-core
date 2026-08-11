#!/usr/bin/env python3
"""
Rewrite a rugo-written parquet corpus through the CURRENT writer policy.

Why this exists: the benchmark corpora are rugo output, but there is no script
that produced them — they were written ad hoc by whichever rugo build was
current at the time. `hits_rugo_262k` carries `opteryx-rugo 0.9.42`, which
predates BOTH the per-column level policy and the keep-compressed floor. So a
corpus cannot be assumed to reflect the writer that exists today, and a benchmark
quoted against it is quoted against a writer nobody can name.

This round-trips each file through rugo's reader and writer. Values are
unchanged; only the writer's encoding and compression decisions are re-made.

⛔ FILE BOUNDARIES AND ROW GROUP SIZE ARE PRESERVED, deliberately. The scan's
unit of work is (file, row group), so changing either changes the benchmark's
parallelism and makes the new number incomparable to the old one for reasons
that have nothing to do with compression. One source file in, one destination
file out, re-chunked to the same ROWS_PER_ROW_GROUP.

⛔ WRITES TO A NEW TREE. It never overwrites its input: a corpus that took six
minutes to build should not be destroyed by a run that fails halfway, and the
old tree is the only way back if the new one is wrong.

Usage:
    python dev/rewrite_parquet_corpus.py <src_dir> <dst_dir> [--profile fast|storage]

Dev tooling only — never imported by production code (repo rules §5).
"""

from __future__ import annotations

import argparse
import os
import sys
import time

_REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, _REPO_ROOT)

from draken.morsels.morsel import Morsel  # noqa: E402
from rugo.parquet import open_parquet_writer  # noqa: E402
from rugo.parquet import read_parquet  # noqa: E402

# Matches the corpora being rewritten (verified against their footers) and rugo's
# own default. Not a format constant — see dev/parquet_to_skene.py for the same
# note about the skene side.
ROWS_PER_ROW_GROUP = 262_144


def _iter_parquet(src_dir: str):
    """(absolute path, path relative to src_dir) for every parquet file, sorted.

    Sorted so a partial run is resumable by inspection and two runs produce the
    same work order — the corpora are split across ~100 files whose names carry
    no ordering guarantee from the filesystem.
    """
    found = []
    for root, _dirs, files in os.walk(src_dir):
        for name in sorted(files):
            if name.endswith(".parquet"):
                full = os.path.join(root, name)
                found.append((full, os.path.relpath(full, src_dir)))
    found.sort(key=lambda pair: pair[1])
    return found


def rewrite_file(src: str, dst: str, profile: str) -> tuple[int, int]:
    """One parquet file in, one out. Returns (rows, row_groups)."""
    os.makedirs(os.path.dirname(dst), exist_ok=True)

    pending: list = []
    pending_rows = 0
    rows = 0
    row_groups = 0

    with open(dst, "wb") as fh:
        writer = open_parquet_writer(fh.write, compression="zstd", profile=profile)

        def emit(morsel) -> None:
            nonlocal rows, row_groups
            writer.write_row_group(morsel)
            rows += morsel.num_rows
            row_groups += 1

        with read_parquet(src) as reader:
            for morsel in reader:
                if morsel.num_rows == 0:
                    continue
                pending.append(morsel)
                pending_rows += morsel.num_rows
                while pending_rows >= ROWS_PER_ROW_GROUP:
                    merged = pending[0] if len(pending) == 1 else Morsel.combine(pending)
                    emit(merged.slice(0, ROWS_PER_ROW_GROUP))
                    remainder = merged.num_rows - ROWS_PER_ROW_GROUP
                    pending = (
                        [merged.slice(ROWS_PER_ROW_GROUP, remainder)] if remainder > 0 else []
                    )
                    pending_rows = remainder

        # The tail row group is SHORT. A corpus does not divide evenly and
        # padding it would change the row count.
        if pending_rows > 0:
            merged = pending[0] if len(pending) == 1 else Morsel.combine(pending)
            emit(merged)

        writer.close()

    if rows == 0:
        raise RuntimeError(f"{src}: read zero rows — refusing to leave an empty {dst}")
    return rows, row_groups


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("src")
    ap.add_argument("dst")
    ap.add_argument("--profile", default="storage", choices=("fast", "storage"))
    args = ap.parse_args()

    src = os.path.abspath(args.src)
    dst = os.path.abspath(args.dst)
    if not os.path.isdir(src):
        print(f"ERROR: {src} is not a directory")
        return 1
    if os.path.exists(dst) and os.listdir(dst):
        print(f"ERROR: {dst} exists and is not empty — refusing to write over a corpus")
        return 1

    files = _iter_parquet(src)
    if not files:
        print(f"ERROR: no .parquet files under {src}")
        return 1

    print(f"rewriting {len(files)} file(s): {src} -> {dst}  (profile={args.profile})")
    t0 = time.time()
    total_rows = 0
    total_rgs = 0
    src_bytes = 0
    dst_bytes = 0

    for i, (full, rel) in enumerate(files, 1):
        out = os.path.join(dst, rel)
        rows, rgs = rewrite_file(full, out, args.profile)
        total_rows += rows
        total_rgs += rgs
        src_bytes += os.path.getsize(full)
        dst_bytes += os.path.getsize(out)
        print(
            f"  [{i}/{len(files)}] {rel}  {rows:,} rows, {rgs} rg  "
            f"{os.path.getsize(full) / 1e6:.1f} -> {os.path.getsize(out) / 1e6:.1f} MB",
            flush=True,
        )

    elapsed = time.time() - t0
    print(
        f"\n{len(files)} files, {total_rows:,} rows, {total_rgs} row groups in {elapsed:.1f}s\n"
        f"{src_bytes / 1e6:.1f} MB -> {dst_bytes / 1e6:.1f} MB "
        f"({dst_bytes / src_bytes:.4f}x)"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
