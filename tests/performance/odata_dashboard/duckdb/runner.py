#!/usr/bin/env python3
"""
Run the odata_dashboard queries against DuckDB over the same parquet snapshots
and emit a results JSON file (baseline for `make dash`).

Usage:
    python tests/performance/odata_dashboard/duckdb/runner.py
    python tests/performance/odata_dashboard/duckdb/runner.py --iterations 5

Output:
    results.local.json

Schema matches the other suites' DuckDB baselines (tests/performance/_common.py
reads it via `load_duckdb_baseline`):
    {
        "system": "DuckDB (Parquet)",
        "date": "<ISO date>",
        "machine": "<hostname>",
        "result": [{"name": "01", "min_ms": ..., "max_ms": ..., "avg_ms": ...,
                     "iterations": N, "times": [...], "shape": [rows, cols]}, ...]
    }
"""

import argparse
import datetime
import gc
import importlib.util
import json
import os
import platform
import time

_REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..", ".."))
_DATA_DIR = os.path.join(_REPO_ROOT, "testdata", "public")

# Loaded by file path (not sys.path insertion) - see opteryx/runner.py for why:
# tests/performance/odata_dashboard/ has a sibling "opteryx" dir that would
# shadow the real top-level opteryx package if this directory were added to
# sys.path. Not a concern for this file (it never imports opteryx), but kept
# consistent with the sibling runner.
_queries_path = os.path.join(os.path.dirname(__file__), "..", "queries.py")
_spec = importlib.util.spec_from_file_location("odata_dashboard_queries", _queries_path)
_queries_module = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_queries_module)
QUERIES = _queries_module.QUERIES


def _duckdb_tables() -> dict:
    """{PLACEHOLDER: read_parquet(...)} for each dataset directory."""
    names = ["gdelt_events", "nvd_vulnerabilities", "exploited_vulnerabilities",
             "vulnerabilities_per_week", "exploit_db"]
    placeholders = ["GDELT", "NVD", "EXPLOITED", "VPW", "EXPLOITDB"]
    tables = {}
    for placeholder, name in zip(placeholders, names):
        glob_path = os.path.join(_DATA_DIR, name, "*.parquet")
        tables[placeholder] = f"read_parquet('{glob_path}')"
    return tables


def main() -> int:
    import duckdb

    parser = argparse.ArgumentParser(description="DuckDB odata_dashboard benchmark")
    parser.add_argument("--iterations", type=int, default=5, help="Timed iterations (default: 5)")
    parser.add_argument("--warm", action="store_true", default=True, help="Warm-up run before timing")
    parser.add_argument(
        "--output", type=str, default=None,
        help="Output JSON path (default: results.local.json next to this script)",
    )
    args = parser.parse_args()

    if not os.path.isdir(_DATA_DIR):
        print(f"ERROR: dataset not found at {_DATA_DIR}")
        print("       run dev/odata_benchmark/fetch_snapshots.py first")
        return 1

    output_path = args.output or os.path.join(os.path.dirname(__file__), "results.local.json")
    tables = _duckdb_tables()

    print("odata_dashboard DuckDB baseline")
    print(f"  data: {_DATA_DIR}")
    print(f"  iterations: {args.iterations}")
    print(f"  queries: {len(QUERIES)}")
    print()

    results = []
    for name, body in QUERIES:
        sql = body.format(**tables)
        times = []
        shape = (0, 0)
        for i in range(args.iterations + (1 if args.warm else 0)):
            gc.collect()
            t0 = time.perf_counter()
            result = duckdb.sql(sql).fetchall()
            elapsed_ms = (time.perf_counter() - t0) * 1000.0

            if i == 0 and args.warm:
                shape = (len(result), len(result[0]) if result else 0)
                print(f"   {name} warm: {elapsed_ms:8.1f}ms", end="\r")
                continue

            times.append(elapsed_ms)
            print(
                f"   {name} [{i:2d}]: {elapsed_ms:8.1f}ms",
                end="\r" if i < args.iterations else "\n",
            )

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

    record = {
        "system": "DuckDB (Parquet)",
        "date": datetime.date.today().isoformat(),
        "machine": platform.node(),
        "iterations": args.iterations,
        "data_path": os.path.relpath(_DATA_DIR, _REPO_ROOT),
        "result": results,
    }

    os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
    with open(output_path, "w") as f:
        json.dump(record, f, indent=2)

    print()
    print(f"    {'Query':<6} {'Min (ms)':>10} {'Max (ms)':>10} {'Avg (ms)':>10} {'Rows':>10} {'Cols':>6}")
    print(f"    {'-' * 6} {'-' * 10} {'-' * 10} {'-' * 10} {'-' * 10} {'-' * 6}")
    for entry in results:
        rows, cols = entry["shape"]
        print(
            f"    {entry['name']:<6} {entry['min_ms']:>10.1f} {entry['max_ms']:>10.1f} "
            f"{entry['avg_ms']:>10.1f} {rows:>10,d} {cols:>6d}"
        )
    total_min = sum(r["min_ms"] for r in results)
    print(f"    {'TOTAL':<6} {total_min:>10.1f}")
    print()
    print(f"Results written to: {output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
