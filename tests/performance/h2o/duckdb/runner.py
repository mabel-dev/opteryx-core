#!/usr/bin/env python3
"""
DuckDB calibration runner for the H2O db-benchmark.

For a given size (`small`, `medium`, `large`), registers the H2O tables
under `testdata/h2o/<size>/` as DuckDB views (`x` rewritten to `x_groupby`
for groupby queries) and times each query in
`tests/performance/h2o/queries/{g,j}*.sql`.

Writes a baseline JSON keyed by `<workload-prefix>/<query>` (e.g. `gro/g1`,
`joi/j2`) so the Opteryx runner's per-row labels match.

Usage:
    make h2o-bench-duckdb
    python tests/performance/h2o/duckdb/runner.py --size small --iterations 3
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
QUERIES_DIR = _REPO_ROOT / "tests" / "performance" / "h2o" / "queries"
DATA_BASE = _REPO_ROOT / "testdata" / "h2o"

QUERY_RE = re.compile(r"^([gj])([0-9]+)\.sql$")
JOIN_TABLES = ("x", "small", "medium", "big")


def _query_sort_key(path: Path):
    m = QUERY_RE.match(path.name)
    if not m:
        return ("z", 10**9)
    return (m.group(1), int(m.group(2)))


def _collect(workload: str, queries_dir: Path) -> list[Path]:
    prefix = "g" if workload == "groupby" else "j"
    files = [p for p in queries_dir.glob(f"{prefix}*.sql") if QUERY_RE.match(p.name)]
    return sorted(files, key=_query_sort_key)


def _drain(cur) -> tuple[int, int]:
    """Drain a DuckDB cursor in batches; return (row_count, col_count).

    Uses to_arrow_reader() — columnar, batch-level materialization with no
    per-row Python object construction — NOT fetchmany(), which previously
    measured here. fetchmany() pays a real but irrelevant ~10-13x per-row
    Python-tuple-construction tax in DuckDB's DB-API layer (confirmed by
    direct measurement: j1 4452ms via fetchmany() vs 362ms via
    to_arrow_reader(), same rows, same query, same connection). The
    Opteryx runner drains morsel batches (morsel.num_rows) with no per-row
    Python objects either, so to_arrow_reader() is the apples-to-apples
    match — both sides measure "produce all result batches", neither pays
    full per-row marshalling cost.
    """
    ncols = len(cur.description) if cur.description else 0
    nrows = 0
    for batch in cur.to_arrow_reader(65_536):
        nrows += batch.num_rows
    return nrows, ncols


def _make_connection(size: str, workload: str):
    """Return a DuckDB connection with views registered for this workload."""
    import duckdb

    con = duckdb.connect()
    base = DATA_BASE / size
    if workload == "groupby":
        # Groupby queries reference the table as `x` but the parquet sits under
        # `<size>/x_groupby/x_groupby.parquet`.
        path = base / "x_groupby"
        if not path.is_dir():
            sys.exit(f"ERROR: {path} not found. Run generate_data.py first.")
        glob = str(path / "*.parquet").replace("'", "''")
        con.execute(f"CREATE VIEW \"x\" AS SELECT * FROM read_parquet('{glob}')")
    else:
        for table in JOIN_TABLES:
            path = base / table
            if not path.is_dir():
                sys.exit(f"ERROR: {path} not found. Run generate_data.py first.")
            glob = str(path / "*.parquet").replace("'", "''")
            con.execute(
                f"CREATE VIEW \"{table}\" AS SELECT * FROM read_parquet('{glob}')"
            )
    return con


def main() -> int:
    parser = argparse.ArgumentParser(description="DuckDB H2O benchmark calibration")
    parser.add_argument(
        "--size",
        choices=["small", "medium", "large"],
        default="small",
    )
    parser.add_argument(
        "--workload",
        choices=["groupby", "join", "both"],
        default="both",
    )
    parser.add_argument(
        "--iterations",
        type=int,
        default=3,
        help="warm iterations per query (default: 3)",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=600.0,
        help="per-query wall-clock timeout in seconds (default: 600)",
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
        default=None,
        help="output JSON path (default: results.<size>.json)",
    )
    args = parser.parse_args()

    try:
        import duckdb  # noqa: F401
    except ImportError:
        sys.exit("duckdb is required: pip install duckdb")
    try:
        import pyarrow  # noqa: F401
    except ImportError:
        sys.exit("pyarrow is required (for to_arrow_reader() batch draining): pip install pyarrow")

    workloads = ["groupby", "join"] if args.workload == "both" else [args.workload]
    output_path = args.output or str(HERE / f"results.{args.size}.json")

    print(f"🐤 DuckDB H2O calibration — size={args.size}")
    print(f"   workloads:  {', '.join(workloads)}")
    print(f"   iterations: {args.iterations} warm runs")
    print(f"   timeout:    {args.timeout:.0f}s/query")
    print()
    print(f"   {'Label':<10} {'Min':>10} {'Max':>10} {'Avg':>10} {'Rows':>12}")
    print(f"   {'─' * 10} {'─' * 11} {'─' * 11} {'─' * 11} {'─' * 13}")

    results = []
    for workload in workloads:
        queries = _collect(workload, QUERIES_DIR)
        if args.filter:
            flt = re.compile(args.filter)
            queries = [q for q in queries if flt.match(q.stem)]
        if not queries:
            print(f"   (no queries for {workload})")
            continue
        con = _make_connection(args.size, workload)
        try:
            for path in queries:
                stem = path.stem
                label = f"{workload[:3]}/{stem}"
                sql = path.read_text()
                times: list[float] = []
                shape = (0, 0)
                try:
                    t0 = time.perf_counter()
                    nrows, ncols = _drain(con.execute(sql))
                    warmup_ms = (time.perf_counter() - t0) * 1000.0
                    shape = (nrows, ncols)
                    if warmup_ms > args.timeout * 1000.0:
                        print(f"   {label:<10} timeout on warm-up ({warmup_ms:.0f}ms)")
                        results.append(
                            {
                                "name": label,
                                "min_ms": warmup_ms,
                                "max_ms": warmup_ms,
                                "avg_ms": warmup_ms,
                                "iterations": 0,
                                "times": [],
                                "shape": list(shape),
                                "workload": workload,
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
                    print(f"   {label:<10} ERROR: {msg[:80]}")
                    results.append(
                        {
                            "name": label,
                            "error": msg,
                            "workload": workload,
                            "status": "error",
                        }
                    )
                    continue
                min_ms = min(times)
                max_ms = max(times)
                avg_ms = sum(times) / len(times)
                results.append(
                    {
                        "name": label,
                        "min_ms": min_ms,
                        "max_ms": max_ms,
                        "avg_ms": avg_ms,
                        "iterations": len(times),
                        "times": times,
                        "shape": list(shape),
                        "workload": workload,
                        "status": "ok",
                    }
                )
                print(
                    f"   {label:<10} {min_ms:>10.1f} {max_ms:>10.1f} {avg_ms:>10.1f} {shape[0]:>12,d}"
                )
        finally:
            con.close()

    record = {
        "system": "DuckDB (H2O / Parquet views)",
        "date": datetime.date.today().isoformat(),
        "machine": platform.node(),
        "size": args.size,
        "iterations": args.iterations,
        "data_path": str(DATA_BASE.relative_to(_REPO_ROOT)),
        "result": results,
    }

    os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
    with open(output_path, "w") as f:
        json.dump(record, f, indent=2)
    print()
    print(f"✅ Results written to: {output_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
