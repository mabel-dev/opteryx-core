"""
Build format files used by read/write benchmarks.

Currently supports generating Draken DRKM files for the tweets dataset.
"""

from __future__ import annotations

import argparse
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from opteryx.compiled.draken import Morsel
from opteryx.compiled.draken.storage import write_morsel


def build_draken_file(codec: str = "lz4") -> Path:
    root = Path(__file__).resolve().parent
    src = root / "parquet" / "tweets.parquet"
    dst_dir = root / "draken"
    dst_dir.mkdir(parents=True, exist_ok=True)
    dst = dst_dir / "tweets.drkm"

    table = pq.read_table(src).combine_chunks()
    supported_columns = [
        field.name
        for field in table.schema
        if not pa.types.is_list(field.type) and not pa.types.is_large_list(field.type)
    ]
    table = table.select(supported_columns)
    morsel = Morsel.from_arrow(table)

    stats = write_morsel(dst, morsel, {"codec_default": codec, "checksum_enabled": True})
    print(f"wrote {dst}")
    print(stats)
    print(f"file_size_bytes={dst.stat().st_size}")
    return dst


def main():
    parser = argparse.ArgumentParser(description="Build benchmark format files.")
    parser.add_argument(
        "--draken-codec",
        default="lz4",
        choices=("lz4", "zstd", "none"),
        help="Codec for DRKM output.",
    )
    args = parser.parse_args()
    build_draken_file(codec=args.draken_codec)


if __name__ == "__main__":
    main()
