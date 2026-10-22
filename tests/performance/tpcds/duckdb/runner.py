#!/usr/bin/env python3
"""
Run the TPC-DS queries against DuckDB on the parquet dataset and write the
baseline JSON the Opteryx TPC-DS runner compares against.

Usage:
    python tests/performance/tpcds/duckdb/runner.py --scale 1
    python tests/performance/tpcds/duckdb/runner.py --scale 001 --iterations 5

Input:   testdata/tpcds_<scale>/<table>/*.parquet     (python dev/tpcds/generate_data.py --scale <scale>)
Queries: tests/performance/tpcds/opteryx/queries/query*.sql   (shared with the Opteryx runner)
Output:  tests/performance/tpcds/duckdb/results.sf<scale>.json

The output JSON follows the same schema as the TPC-H and SSB baselines. Query
names are `Q01`..`Q99`, the labels the Opteryx runner prints. A query DuckDB
cannot run is recorded with status "error" and no `min_ms`, so it is simply
absent from the comparison rather than counted as zero.
"""

from __future__ import annotations

import argparse
import datetime
import gc
import glob
import json
import os
import platform
import sys
import time

_HERE = os.path.dirname(os.path.abspath(__file__))
_REPO_ROOT = os.path.abspath(os.path.join(_HERE, "..", "..", "..", ".."))
_QUERY_DIR = os.path.join(_HERE, "..", "opteryx", "queries")

# The placeholder dev/tpcds/generate_queries.py writes in front of every table reference.
_PLACEHOLDER = "testdata.tpcds_tiny."


def query_name(path: str) -> str:
    """query07.sql -> Q07"""
    stem = os.path.splitext(os.path.basename(path))[0]
    return f"Q{int(stem[5:]):02d}"


def load_queries() -> list[tuple[str, str]]:
    """[(name, sql)] with the dataset placeholder stripped: the tables are registered
    as DuckDB views under their bare names, which is how the templates spell them."""
    queries = []
    for path in sorted(glob.glob(os.path.join(_QUERY_DIR, "query*.sql"))):
        with open(path) as f:
            queries.append((query_name(path), f.read().replace(_PLACEHOLDER, "")))
    return queries


def drain(cur) -> tuple[int, int]:
    """Drain a DuckDB cursor in Arrow batches; return (rows, cols).

    Batch-level materialisation with no per-row Python objects, matching what the
    Opteryx worker does with morsels (see h2o/duckdb/runner.py `_drain`)."""
    cols = len(cur.description)
    rows = 0
    for batch in cur.to_arrow_reader(65_536):
        rows += batch.num_rows
    return rows, cols


def main() -> int:
    import duckdb

    parser = argparse.ArgumentParser(description="DuckDB TPC-DS benchmark")
    parser.add_argument("--scale", type=str, default="1", help="Scale suffix matching testdata/tpcds_<scale> (default: 1)")
    parser.add_argument("--iterations", type=int, default=3, help="Timed iterations after one warm-up (default: 3)")
    parser.add_argument("--output", type=str, default=None, help="Output JSON path")
    args = parser.parse_args()

    scale_path = os.path.join(_REPO_ROOT, "testdata", f"tpcds_{args.scale}")
    if not os.path.isdir(scale_path):
        print(f"ERROR: dataset not found at {scale_path}")
        print(f"       generate it: python dev/tpcds/generate_data.py --scale {args.scale}")
        return 1
    output_path = args.output or os.path.join(_HERE, f"results.sf{args.scale}.json")

    con = duckdb.connect()
    tables = {}
    for table in sorted(os.listdir(scale_path)):
        pattern = os.path.join(scale_path, table, "*.parquet")
        if not glob.glob(pattern):
            continue
        con.execute(f"CREATE VIEW \"{table}\" AS SELECT * FROM read_parquet('{pattern}')")
        rows = con.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0]
        tables[table] = {"rows": rows, "columns": len(con.execute(f'DESCRIBE "{table}"').fetchall())}

    queries = load_queries()
    print(f"DuckDB {duckdb.__version__} TPC-DS — SF {args.scale}")
    print(f"   path: {scale_path}")
    print(f"   iterations: {args.iterations} (+1 warm-up), queries: {len(queries)}")
    print()

    results = []
    for name, sql in queries:
        try:
            gc.collect()
            shape = drain(con.execute(sql))
            times = []
            for _ in range(args.iterations):
                gc.collect()
                t0 = time.perf_counter()
                drain(con.execute(sql))
                times.append((time.perf_counter() - t0) * 1000.0)
        except Exception as err:
            msg = f"{type(err).__name__}: {err}"
            print(f"   {name}  ERROR: {msg[:100]}")
            results.append({"name": name, "status": "error", "error": msg})
            continue
        results.append(
            {
                "name": name,
                "status": "ok",
                "min_ms": min(times),
                "max_ms": max(times),
                "avg_ms": sum(times) / len(times),
                "iterations": len(times),
                "times": times,
                "shape": list(shape),
            }
        )
        print(f"   {name}  min {min(times):9.1f}ms  avg {sum(times) / len(times):9.1f}ms  rows {shape[0]}")

    ok = [r for r in results if r["status"] == "ok"]
    record = {
        "system": f"DuckDB {duckdb.__version__} (Parquet, views)",
        "date": datetime.date.today().isoformat(),
        "machine": platform.node(),
        "scale_factor": args.scale,
        "iterations": args.iterations,
        "data_path": f"testdata/tpcds_{args.scale}",
        "tables": tables,
        "result": results,
    }
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, "w") as f:
        json.dump(record, f, indent=2)

    print(f"\n   TOTAL min {sum(r['min_ms'] for r in ok):.1f}ms over {len(ok)} of {len(results)} queries")
    print(f"Results written to: {output_path}")
    return 0 if len(ok) == len(results) else 1


if __name__ == "__main__":
    sys.exit(main())
