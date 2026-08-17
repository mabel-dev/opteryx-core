"""
Generate TPC-DS data via DuckDB's `tpcds` extension (the official dsdgen kit,
vendored inside the extension).

DEV TOOLING — not packaged, not run at test time. Writes the 24 TPC-DS tables
as parquet, one file per table, under testdata/tpcds_<scale>/<table>/data.parquet
— mirroring the testdata/tpch_<scale> layout so the same DiskConnector /
runner conventions apply.

Usage:
    python dev/tpcds/generate_data.py --scale 1
    python dev/tpcds/generate_data.py --scale 001   # SF 0.01 — matches testdata/tpch_001's label
"""

from __future__ import annotations

import argparse
import os
import sys
import time

_REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))

import duckdb  # dev/test data generator only — never imported by production code

# Directory-label -> numeric SF passed to dsdgen. "001" is the one precedent
# already in this repo (tests/integration/sql_battery/_tpch_golden.py:
# `TPCH_SCALE = "001"  # 0.01 scale factor`) — matched exactly so tpcds_001
# means the same thing tpch_001 does. Anything else is parsed as a literal
# float (e.g. "1" -> 1.0, "10" -> 10.0).
_SCALE_LABELS = {"001": 0.01}


def _scale_factor(label: str) -> float:
    return _SCALE_LABELS[label] if label in _SCALE_LABELS else float(label)

_TABLES = [
    "call_center",
    "catalog_page",
    "catalog_returns",
    "catalog_sales",
    "customer",
    "customer_address",
    "customer_demographics",
    "date_dim",
    "household_demographics",
    "income_band",
    "inventory",
    "item",
    "promotion",
    "reason",
    "ship_mode",
    "store",
    "store_returns",
    "store_sales",
    "time_dim",
    "warehouse",
    "web_page",
    "web_returns",
    "web_sales",
    "web_site",
]


def main() -> int:
    parser = argparse.ArgumentParser(description="Generate TPC-DS parquet data via DuckDB dsdgen")
    parser.add_argument("--scale", type=str, default="1", help="Scale factor (default: 1)")
    args = parser.parse_args()

    sf = _scale_factor(args.scale)
    out_dir = os.path.join(_REPO_ROOT, "testdata", f"tpcds_{args.scale}")
    os.makedirs(out_dir, exist_ok=True)

    con = duckdb.connect()
    con.execute("INSTALL tpcds")
    con.execute("LOAD tpcds")

    print(f"Generating TPC-DS SF{sf} (label: {args.scale})...")
    t0 = time.time()
    con.execute(f"CALL dsdgen(sf={sf})")
    print(f"  dsdgen: {time.time() - t0:.1f}s")

    for table in _TABLES:
        table_dir = os.path.join(out_dir, table)
        os.makedirs(table_dir, exist_ok=True)
        path = os.path.join(table_dir, "data.parquet")
        t0 = time.time()
        con.execute(f"COPY {table} TO '{path}' (FORMAT PARQUET)")
        rows = con.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
        print(f"  {table:<24} {rows:>10,} rows  ({time.time() - t0:.1f}s)")

    print(f"\nWrote {len(_TABLES)} tables to {os.path.relpath(out_dir, _REPO_ROOT)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
