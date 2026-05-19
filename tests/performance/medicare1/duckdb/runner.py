#!/usr/bin/env python3
"""
DuckDB calibration runner for the Medicare1 benchmark.

Registers Medicare1 tables under `testdata/medicare1/<table>/` as DuckDB views so
query files in `tests/performance/medicare1/queries/*.sql` can be executed with
their bare table names. Times each query and writes a per-name baseline JSON
that the Opteryx runner reads via `_common.load_duckdb_baseline`.

Usage:
    make medicare1-duckdb
    python tests/performance/medicare1/duckdb/runner.py
    python tests/performance/medicare1/duckdb/runner.py --iterations 3 --timeout 60
"""

from __future__ import annotations

import argparse
import datetime
import gc
import json
import os
import platform
import re
import sys
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
_REPO_ROOT = HERE.parents[3]
QUERIES_DIR = _REPO_ROOT / "tests" / "performance" / "medicare1" / "queries"
DATA_DIR = _REPO_ROOT / "testdata" / "medicare1"

TABLES = ["Medicare1_1", "Medicare1_2"]
QUERY_RE = re.compile(r"^(\d+)\.sql$")


def _query_sort_key(path: Path):
    m = QUERY_RE.match(path.name)
    if not m:
        return 10**9
    return int(m.group(1))


def _rewrite_query(sql: str) -> str:
    """Rewrite Medicare1 table names for DuckDB.

    DuckDB will have views registered for Medicare1_1 and Medicare1_2.
    We just need to:
    1. Replace Tableau aliases with colons (invalid in SQL)
    2. Remove all double quotes (which may confuse DuckDB with the views)
    """
    # Step 1: Handle problematic aliases - replace colons with underscores
    def fix_colon_identifiers(match):
        content = match.group(1)
        return content.replace(':', '_')

    sql = re.sub(r'"([^"]*:[^"]*)"', fix_colon_identifiers, sql)

    # Step 2: Remove all double quotes - DuckDB will resolve table names against views
    sql = sql.replace('"', '')

    return sql


def main() -> int:
    parser = argparse.ArgumentParser(description="DuckDB Medicare1 benchmark calibration")
    parser.add_argument(
        "--iterations",
        type=int,
        default=3,
        help="warm iterations per query (default: 3)",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=300.0,
        help="per-query wall-clock timeout in seconds (default: 300)",
    )
    parser.add_argument(
        "--filter",
        type=str,
        default=None,
        help="run only queries whose stem matches this regex",
    )
    parser.add_argument(
        "--output",
        type=str,
        default=str(HERE / "results.json"),
        help="output JSON path (default: tests/performance/medicare1/duckdb/results.json)",
    )
    args = parser.parse_args()

    if not DATA_DIR.is_dir():
        sys.exit(f"ERROR: {DATA_DIR} not found. Run fetch_data.py first.")

    try:
        import duckdb
    except ImportError:
        sys.exit("duckdb is required: pip install duckdb")

    queries = sorted(QUERIES_DIR.glob("*.sql"), key=_query_sort_key)
    queries = [q for q in queries if QUERY_RE.match(q.name)]
    if args.filter:
        flt = re.compile(args.filter)
        queries = [q for q in queries if flt.match(q.stem)]
    if not queries:
        sys.exit("no queries matched")

    # Register every table as a view that reads from the Parquet files
    con = duckdb.connect()
    for table in TABLES:
        table_path = DATA_DIR / table
        glob_path = str(table_path / "*.parquet").replace("'", "''")
        con.execute(
            f"CREATE VIEW {table} AS "
            f"SELECT * FROM read_parquet('{glob_path}')"
        )
        print(f"   Registered view: {table}")

    print(f"🐤 DuckDB Medicare1 calibration")
    print(f"   data path:  {DATA_DIR}")
    print(f"   queries:    {len(queries)}")
    print(f"   iterations: {args.iterations} warm runs")
    print(f"   timeout:    {args.timeout:.0f}s/query")
    print()

    def _drain(cur) -> tuple[int, int]:
        ncols = len(cur.description) if cur.description else 0
        nrows = 0
        while True:
            batch = cur.fetchmany(65_536)
            if not batch:
                break
            nrows += len(batch)
        return nrows, ncols

    print(f"   {'Query':<6} {'Min':>10} {'Max':>10} {'Avg':>10} {'Rows':>10}")
    print(f"   {'─' * 6} {'─' * 11} {'─' * 11} {'─' * 11} {'─' * 11}")

    results = []
    for path in queries:
        stem = path.stem
        sql = _rewrite_query(path.read_text())
        times: list[float] = []
        shape = (0, 0)
        try:
            # Warm-up (untimed) plus N timed iterations.
            t0 = time.perf_counter()
            nrows, ncols = _drain(con.execute(sql))
            warmup_ms = (time.perf_counter() - t0) * 1000.0
            shape = (nrows, ncols)
            if warmup_ms > args.timeout * 1000.0:
                print(
                    f"   {stem:<6} timeout on warm-up ({warmup_ms:.0f}ms > {args.timeout * 1000:.0f}ms)"
                )
                results.append(
                    {
                        "name": stem,
                        "min_ms": warmup_ms,
                        "max_ms": warmup_ms,
                        "avg_ms": warmup_ms,
                        "iterations": 0,
                        "times": [],
                        "shape": list(shape),
                        "status": "timeout",
                    }
                )
                continue
            for _ in range(args.iterations):
                gc.collect()
                t0 = time.perf_counter()
                _drain(con.execute(sql))
                times.append((time.perf_counter() - t0) * 1000.0)
        except Exception as e:
            msg = f"{type(e).__name__}: {e}"
            print(f"   {stem:<6} ERROR: {msg[:80]}")
            results.append(
                {
                    "name": stem,
                    "error": msg,
                    "shape": list(shape),
                    "status": "error",
                }
            )
            continue
        min_ms = min(times)
        max_ms = max(times)
        avg_ms = sum(times) / len(times)
        results.append(
            {
                "name": stem,
                "min_ms": min_ms,
                "max_ms": max_ms,
                "avg_ms": avg_ms,
                "iterations": len(times),
                "times": times,
                "shape": list(shape),
                "status": "ok",
            }
        )
        print(
            f"   {stem:<6} {min_ms:>10.1f} {max_ms:>10.1f} {avg_ms:>10.1f} {shape[0]:>10,d}"
        )

    record = {
        "system": "DuckDB (Medicare1 / Parquet views)",
        "date": datetime.date.today().isoformat(),
        "machine": platform.node(),
        "iterations": args.iterations,
        "data_path": str(DATA_DIR.relative_to(_REPO_ROOT)),
        "result": results,
    }

    output_path = args.output
    os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
    with open(output_path, "w") as f:
        json.dump(record, f, indent=2)
    print()
    print(f"✅ Results written to: {output_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
