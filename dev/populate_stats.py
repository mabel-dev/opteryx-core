#!/usr/bin/env python3
"""Offline KMV-32 stats populator for Parquet relations.

Walks a directory of parquet files, computes a K-Minimum-Values (K=32)
sketch per column, and writes a sibling ``<file>.stats.json`` sidecar that
the filesystem connector picks up at manifest-build time.

This is developer tooling. It is not packaged and must not be imported by
production code (CLAUDE.md). Reading parquet via pyarrow is permitted here.

Sidecar format (v1):

    {
      "schema_version": 1,
      "field_ids": {"col_a": 0, "col_b": 1, ...},
      "min_k_hashes": {
        "0": [<sorted ascending uint64 hashes>, ...],
        "1": [...]
      }
    }

The KMV sketch contract is the one consumed by
``opteryx.models.manifest.Manifest.estimate_cardinality``: K=32, sorted
ascending, 64-bit unsigned hashes, distinct.
"""

from __future__ import annotations

import argparse
import json
import os
import struct
import sys
from pathlib import Path
from typing import Any, Iterable, List

import pyarrow.compute as pc
import pyarrow.parquet as pq

try:
    import xxhash

    _HASH_BACKEND = "xxh3_64"

    def _hash_bytes(b: bytes) -> int:
        return xxhash.xxh3_64_intdigest(b)
except ImportError:  # pragma: no cover - xxhash is a stable dep
    import hashlib

    _HASH_BACKEND = "blake2b8"

    def _hash_bytes(b: bytes) -> int:
        return int.from_bytes(hashlib.blake2b(b, digest_size=8).digest(), "little")


SCHEMA_VERSION = 1
K = 32
SIDECAR_SUFFIX = ".stats.json"


def _hash_value(v: Any) -> int:
    """Stable 64-bit hash of a value. Caller must skip None upstream.

    Uses a tagged byte encoding so that ints, bools, and strings cannot
    collide via shared payloads.
    """
    if isinstance(v, bool):
        return _hash_bytes(b"B" + (b"\x01" if v else b"\x00"))
    if isinstance(v, int):
        # Signed 16-byte two's-complement covers Python's bigints up to int128;
        # values outside that range are rare in real data and degrade gracefully
        # via str fallback below. Use 16 bytes to give us headroom over int64.
        try:
            return _hash_bytes(b"I" + v.to_bytes(16, "little", signed=True))
        except OverflowError:
            return _hash_bytes(b"I" + str(v).encode("utf-8"))
    if isinstance(v, float):
        return _hash_bytes(b"F" + struct.pack("<d", v))
    if isinstance(v, bytes):
        return _hash_bytes(b"S" + v)
    if isinstance(v, str):
        return _hash_bytes(b"S" + v.encode("utf-8"))
    # Dates, decimals, timestamps, etc — fall through to a stable string form.
    return _hash_bytes(b"X" + repr(v).encode("utf-8"))


class _Sketch:
    """Bounded K-min sketch — keeps the K smallest distinct 64-bit hashes."""

    __slots__ = ("hashes", "threshold")

    def __init__(self) -> None:
        self.hashes: set[int] = set()
        # While the sketch is below capacity, every hash is a candidate.
        self.threshold: int = (1 << 64) - 1

    def offer(self, h: int) -> None:
        if h >= self.threshold and len(self.hashes) >= K:
            return
        if h in self.hashes:
            return
        if len(self.hashes) < K:
            self.hashes.add(h)
            if len(self.hashes) == K:
                self.threshold = max(self.hashes)
            return
        # At capacity and h < threshold: evict current max, insert h, update threshold.
        self.hashes.discard(self.threshold)
        self.hashes.add(h)
        self.threshold = max(self.hashes)

    def sorted_hashes(self) -> List[int]:
        return sorted(self.hashes)


def compute_sketch(values: Iterable[Any]) -> List[int]:
    """Compute a KMV-32 sketch from an iterable of values.

    None values are skipped. Returns a sorted ascending list of <= K
    distinct 64-bit hashes.
    """
    sk = _Sketch()
    for v in values:
        if v is None:
            continue
        sk.offer(_hash_value(v))
    return sk.sorted_hashes()


def _process_parquet(path: Path) -> dict:
    """Single-pass scan of a parquet file producing the sidecar payload."""
    pf = pq.ParquetFile(str(path))
    field_names = [f.name for f in pf.schema_arrow]
    sketches = [_Sketch() for _ in field_names]

    for batch in pf.iter_batches():
        for i in range(len(field_names)):
            arr = batch.column(i)
            # Hash only the distinct values per batch — arrow's unique kernel
            # collapses runs without a Python loop and keeps the per-row cost
            # down on low-cardinality columns. High-cardinality columns lose
            # nothing: every batch value is already distinct.
            try:
                distinct = pc.unique(arr).to_pylist()
            except Exception:
                distinct = arr.to_pylist()
            sk = sketches[i]
            for v in distinct:
                if v is None:
                    continue
                sk.offer(_hash_value(v))

    return {
        "schema_version": SCHEMA_VERSION,
        "field_ids": {name: i for i, name in enumerate(field_names)},
        "min_k_hashes": {
            str(i): sketches[i].sorted_hashes() for i in range(len(field_names))
        },
    }


def _find_parquet_files(relation_dir: Path) -> List[Path]:
    """Find *.parquet files in relation_dir, recursing one level."""
    if not relation_dir.is_dir():
        raise FileNotFoundError(f"Not a directory: {relation_dir}")
    out: List[Path] = []
    for entry in sorted(relation_dir.iterdir()):
        if entry.is_file() and entry.suffix.lower() == ".parquet":
            out.append(entry)
        elif entry.is_dir():
            for child in sorted(entry.iterdir()):
                if child.is_file() and child.suffix.lower() == ".parquet":
                    out.append(child)
    return out


def _sidecar_path(parquet_path: Path) -> Path:
    return parquet_path.with_name(parquet_path.name + SIDECAR_SUFFIX)


def _is_sidecar_fresh(sidecar: Path, parquet_path: Path) -> bool:
    if not sidecar.exists():
        return False
    return sidecar.stat().st_mtime >= parquet_path.stat().st_mtime


def populate(relation_dir: Path, *, force: bool = False, dry_run: bool = False) -> List[Path]:
    """Populate sidecars for every parquet file under relation_dir.

    Returns the list of paths that were (or would be, in dry_run) written.
    """
    written: List[Path] = []
    parquet_files = _find_parquet_files(relation_dir)
    if not parquet_files:
        print(f"no parquet files under {relation_dir}", file=sys.stderr)
        return written

    for parquet_path in parquet_files:
        sidecar = _sidecar_path(parquet_path)
        if not force and _is_sidecar_fresh(sidecar, parquet_path):
            print(f"skip (fresh): {sidecar}")
            continue

        payload = _process_parquet(parquet_path)
        if dry_run:
            print(
                f"[dry-run] would write {sidecar} "
                f"({len(payload['field_ids'])} cols, hash={_HASH_BACKEND})"
            )
            written.append(sidecar)
            continue

        tmp = sidecar.with_name(sidecar.name + ".tmp")
        tmp.write_text(json.dumps(payload, separators=(",", ":")))
        os.replace(tmp, sidecar)
        print(f"wrote {sidecar} ({len(payload['field_ids'])} cols)")
        written.append(sidecar)

    return written


def main(argv: List[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Compute KMV-32 column sketches and write per-file stats sidecars."
    )
    parser.add_argument("relation_dir", type=Path, help="Directory containing parquet files.")
    parser.add_argument(
        "--force",
        action="store_true",
        help="Overwrite existing sidecars even if newer than the parquet.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print what would be written without writing.",
    )
    args = parser.parse_args(argv)
    populate(args.relation_dir, force=args.force, dry_run=args.dry_run)
    return 0


if __name__ == "__main__":
    sys.exit(main())
