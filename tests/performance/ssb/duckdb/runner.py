#!/usr/bin/env python3
"""
Run the Star Schema Benchmark queries against DuckDB on the parquet dataset and
write the baseline JSON the Opteryx SSB runner compares against.

Usage:
    python tests/performance/ssb/duckdb/runner.py --scale 1
    python tests/performance/ssb/duckdb/runner.py --scale 10 --iterations 5

Input:   testdata/ssb_<scale>/<table>/*.parquet   (python dev/ssb_generate.py <scale>)
Queries: tests/performance/ssb/queries/q*.sql      (shared with the Opteryx runner)
Output:  tests/performance/ssb/duckdb/results.sf<scale>.json

The output JSON follows the same schema as the TPC-H baseline.
"""

import argparse
import datetime
import gc
import glob
import json
import os
import platform
import re
import time

_HERE = os.path.dirname(os.path.abspath(__file__))
_REPO_ROOT = os.path.abspath(os.path.join(_HERE, "..", "..", "..", ".."))
_QUERY_DIR = os.path.join(_HERE, "..", "queries")

_TABLE_REF = re.compile(r"testdata\.ssb\.(\w+)")


def query_name(path: str) -> str:
    """q11.sql -> Q1.1"""
    stem = os.path.splitext(os.path.basename(path))[0]
    return f"Q{stem[1]}.{stem[2]}"


def load_queries(scale_path: str) -> list[tuple[str, str]]:
    """[(name, sql)] with each `testdata.ssb.<table>` replaced by a parquet scan."""
    queries = []
    for path in sorted(glob.glob(os.path.join(_QUERY_DIR, "q*.sql"))):
        with open(path) as f:
            body = f.read()
        body = _TABLE_REF.sub(
            lambda m: f"read_parquet('{scale_path}/{m.group(1)}/*.parquet') AS {m.group(1)}", body
        )
        queries.append((query_name(path), body))
    return queries


def main() -> int:
    import duckdb

    parser = argparse.ArgumentParser(description="DuckDB SSB benchmark")
    parser.add_argument("--scale", type=str, default="10", help="Scale factor (1, 10, 100)")
    parser.add_argument("--iterations", type=int, default=10, help="Timed iterations (default: 10)")
    parser.add_argument("--output", type=str, default=None, help="Output JSON path")
    args = parser.parse_args()

    scale_path = os.path.join(_REPO_ROOT, "testdata", f"ssb_{args.scale}")
    if not os.path.isdir(scale_path):
        print(f"ERROR: dataset not found at {scale_path}")
        print(f"       generate it: python dev/ssb_generate.py {args.scale}")
        return 1
    output_path = args.output or os.path.join(_HERE, f"results.sf{args.scale}.json")

    tables = {}
    for table in sorted(os.listdir(scale_path)):
        pattern = os.path.join(scale_path, table, "*.parquet")
        if not glob.glob(pattern):
            continue
        rows = duckdb.sql(f"SELECT COUNT(*) FROM read_parquet('{pattern}')").fetchone()[0]
        cols = duckdb.sql(f"SELECT COUNT(*) FROM parquet_schema('{glob.glob(pattern)[0]}')").fetchone()[0] - 1
        tables[table] = {"rows": rows, "columns": cols}

    queries = load_queries(scale_path)
    print(f"DuckDB {duckdb.__version__} SSB — SF {args.scale}")
    print(f"   path: {scale_path}")
    print(f"   iterations: {args.iterations} (+1 warm-up), queries: {len(queries)}")
    for tname, tinfo in tables.items():
        print(f"     {tname:<10} {tinfo['rows']:>14,d} rows  {tinfo['columns']} cols")
    print()

    results = []
    for name, sql in queries:
        times = []
        shape = None
        for i in range(args.iterations + 1):
            gc.collect()
            t0 = time.perf_counter()
            result = duckdb.sql(sql).fetchall()
            elapsed = (time.perf_counter() - t0) * 1000.0
            if i == 0:
                shape = (len(result), len(result[0]) if result else 0)
                continue
            times.append(elapsed)
        results.append(
            {
                "name": name,
                "min_ms": min(times),
                "max_ms": max(times),
                "avg_ms": sum(times) / len(times),
                "iterations": len(times),
                "times": times,
                "shape": list(shape),
            }
        )
        print(f"   {name}  min {min(times):9.1f}ms  avg {sum(times) / len(times):9.1f}ms  rows {shape[0]}")

    record = {
        "system": f"DuckDB {duckdb.__version__} (Parquet, partitioned)",
        "date": datetime.date.today().isoformat(),
        "machine": platform.node(),
        "scale_factor": args.scale,
        "iterations": args.iterations,
        "data_path": f"testdata/ssb_{args.scale}",
        "tables": tables,
        "result": results,
    }
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, "w") as f:
        json.dump(record, f, indent=2)

    print(f"\n   TOTAL min {sum(r['min_ms'] for r in results):.1f}ms")
    print(f"Results written to: {output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
