"""
JSONBench (ClickHouse) — data fetcher.

Downloads the Bluesky Jetstream NDJSON shards used by
https://github.com/ClickHouse/JSONBench from the public S3 bucket, then
decompresses each shard once into a cached `.jsonl` file (the DuckDB
calibration loads these), and recompresses it as zstd level 1 into
`file_NNNN.json.zst` (what `make jsonbench` reads).

The zstd shards are made locally because none are published: JSONBench's own
`_files_zstd` entry builds them exactly this way (`zstd -1` over each
decompressed `.json.gz` shard).

Each shard is exactly 1,000,000 rows. Sizes map to shard counts:

    1m   -> file_0001                 (1 file)
    10m  -> file_0001 .. file_0010    (10 files)
    100m -> file_0001 .. file_0100    (100 files)

Idempotent: skips any download/decompress whose output already exists.

Usage:
    python tests/performance/jsonbench/fetch_data.py            # 10m (default)
    python tests/performance/jsonbench/fetch_data.py --size 1
    python tests/performance/jsonbench/fetch_data.py --size 100
"""

from __future__ import annotations

import argparse
import gzip
import os
from compression import zstd
import sys
import urllib.request

_REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))

_BASE_URL = "https://clickhouse-public-datasets.s3.amazonaws.com/bluesky/file_{ix:04d}.json.gz"

_DOWNLOAD_DIR = os.path.join(_REPO_ROOT, "testdata", "_downloads", "jsonbench")
_JSONL_DIR = os.path.join(_DOWNLOAD_DIR, "decompressed")
_ZSTD_DIR = os.path.join(_DOWNLOAD_DIR, "zstd")

_SIZE_TO_FILES = {1: 1, 10: 10, 100: 100}


def _download(ix: int) -> str:
    gz_path = os.path.join(_DOWNLOAD_DIR, f"file_{ix:04d}.json.gz")
    if os.path.exists(gz_path):
        return gz_path
    url = _BASE_URL.format(ix=ix)
    tmp_path = gz_path + ".part"
    print(f"  downloading {url}")
    with urllib.request.urlopen(url) as resp, open(tmp_path, "wb") as out:
        while chunk := resp.read(1024 * 1024):
            out.write(chunk)
    os.replace(tmp_path, gz_path)
    return gz_path


def _decompress(gz_path: str) -> str:
    ix = os.path.basename(gz_path).split("_")[1].split(".")[0]
    jsonl_path = os.path.join(_JSONL_DIR, f"file_{ix}.jsonl")
    if os.path.exists(jsonl_path):
        return jsonl_path
    print(f"  decompressing {os.path.basename(gz_path)}")
    tmp_path = jsonl_path + ".part"
    with gzip.open(gz_path, "rb") as src, open(tmp_path, "wb") as out:
        while chunk := src.read(16 * 1024 * 1024):
            out.write(chunk)
    os.replace(tmp_path, jsonl_path)
    return jsonl_path


def _recompress_zstd(jsonl_path: str) -> str:
    ix = os.path.basename(jsonl_path).split("_")[1].split(".")[0]
    zst_path = os.path.join(_ZSTD_DIR, f"file_{ix}.json.zst")
    if os.path.exists(zst_path):
        return zst_path
    print(f"  compressing {os.path.basename(jsonl_path)} (zstd -1)")
    tmp_path = zst_path + ".part"
    with open(jsonl_path, "rb") as src, zstd.open(tmp_path, "wb", level=1) as out:
        while chunk := src.read(16 * 1024 * 1024):
            out.write(chunk)
    os.replace(tmp_path, zst_path)
    return zst_path


def fetch(size: int) -> list[str]:
    """Ensure `size` million rows (shard files) are downloaded + decompressed.

    Returns the sorted list of decompressed `.jsonl` paths.
    """
    if size not in _SIZE_TO_FILES:
        raise ValueError(f"size must be one of {sorted(_SIZE_TO_FILES)} (million rows), got {size}")
    num_files = _SIZE_TO_FILES[size]
    os.makedirs(_DOWNLOAD_DIR, exist_ok=True)
    os.makedirs(_JSONL_DIR, exist_ok=True)

    paths = []
    for ix in range(1, num_files + 1):
        gz_path = _download(ix)
        paths.append(_decompress(gz_path))
    return sorted(paths)


def fetch_zstd(size: int) -> list[str]:
    """Ensure `size` million rows exist as zstd level-1 shards (`file_NNNN.json.zst`).

    Built from the decompressed `.jsonl` shards (fetched first if missing).
    Returns the sorted list of `.json.zst` paths.
    """
    os.makedirs(_ZSTD_DIR, exist_ok=True)
    return sorted(_recompress_zstd(path) for path in fetch(size))


def main() -> int:
    parser = argparse.ArgumentParser(description="Fetch JSONBench Bluesky dataset")
    parser.add_argument(
        "--size",
        type=int,
        default=10,
        choices=sorted(_SIZE_TO_FILES),
        help="Dataset size in millions of rows (default: 10)",
    )
    args = parser.parse_args()

    num_files = _SIZE_TO_FILES[args.size]
    print(f"JSONBench data: {args.size}m rows ({num_files} shard(s) x 1,000,000 rows, ~135MB gz each)")
    paths = fetch(args.size)
    total_bytes = sum(os.path.getsize(p) for p in paths)
    print(f"Ready: {len(paths)} shard(s), {total_bytes / 1e9:.1f}GB decompressed, cached under {_JSONL_DIR}")
    zst_paths = fetch_zstd(args.size)
    zst_bytes = sum(os.path.getsize(p) for p in zst_paths)
    print(f"Ready: {len(zst_paths)} shard(s), {zst_bytes / 1e9:.1f}GB zstd, cached under {_ZSTD_DIR}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
