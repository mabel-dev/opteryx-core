"""
Rewrite testdata/job/<table>/<table>.parquet with 500k-row row groups.

The original fetch_data.py writes one row group per file (workaround for an
all-null-column-in-RG decoder bug). Opteryx targets ~500k rows per RG, so
that's adversarial for the engine. This script reads each JOB parquet,
rewrites it with row_group_size=500_000, same SNAPPY compression and schema.

Dev dependency only — uses PyArrow exactly like fetch_data.py.

Usage:
    python dev/job_resize_row_groups.py
"""

from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "testdata" / "job"
ROW_GROUP_SIZE = 500_000


def main() -> int:
    import pyarrow.parquet as pq  # type: ignore

    files = sorted(DATA_DIR.glob("*/*.parquet"))
    if not files:
        print(f"no parquet files found under {DATA_DIR}")
        return 1

    print(f"rewriting {len(files)} files with row_group_size={ROW_GROUP_SIZE:,}")
    for src in files:
        tmp = src.with_suffix(".parquet.tmp")
        table = pq.read_table(src.as_posix())
        pq.write_table(
            table,
            tmp.as_posix(),
            compression="snappy",
            row_group_size=ROW_GROUP_SIZE,
        )
        tmp.replace(src)
        rgs = (table.num_rows + ROW_GROUP_SIZE - 1) // ROW_GROUP_SIZE
        print(f"  {src.relative_to(ROOT)}  rows={table.num_rows:>10,}  rgs={rgs}")
    print("done")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
