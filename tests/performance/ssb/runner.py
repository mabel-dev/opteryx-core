#!/usr/bin/env python3
"""
Star Schema Benchmark (SSB) runner + DuckDB comparison.

Runs each of the 13 SSB queries against Opteryx (warm, multi-iteration), compares
the best time to the DuckDB baseline at the same scale factor, and writes
per-iteration results to `results/<sha>-<ts>.csv`.

Usage:
    make ssb-sf1 | ssb-sf10 | ssb-sf100          # skene v3 mirror, 3 warm iterations
    python tests/performance/ssb/runner.py --scale 1 --variant skene

Inputs:
    tests/performance/ssb/queries/q*.sql                 - query bodies (shared with DuckDB)
    tests/performance/ssb/duckdb/results.sf{scale}.json  - DuckDB baseline
    testdata/ssb_<scale>[_<variant>]/<table>/            - python dev/ssb_generate.py <scale>

The DuckDB baseline is regenerated per scale via `make ssb-sf<scale>-duckdb`.
"""

from __future__ import annotations

import argparse
import gc
import glob
import os
import sys
import time

_REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
sys.path.insert(0, _REPO_ROOT)
sys.path.insert(0, os.path.join(_REPO_ROOT, "tests", "performance"))
from _common import (  # noqa: E402
    load_duckdb_baseline,
    load_duckdb_shapes,
    open_results_csv,
    print_banner,
    print_error_row,
    print_header,
    print_row,
    print_total_row,
)

import opteryx  # noqa: E402
from opteryx.connectors import DiskConnector  # noqa: E402

opteryx.register_workspace("testdata", DiskConnector)

_QUERY_DIR = os.path.join(os.path.dirname(__file__), "queries")
_DUCKDB_DIR = os.path.join(os.path.dirname(__file__), "duckdb")
_RESULTS_DIR = os.path.join(os.path.dirname(__file__), "results")


def _dataset_suffix(scale: str, variant: str) -> str:
    return f"ssb_{scale}_{variant}" if variant else f"ssb_{scale}"


def _load_queries(scale: str, variant: str) -> list[tuple[str, str]]:
    """[(name, sql)] sorted by name; the `testdata.ssb.` placeholder is rewritten."""
    dataset = f"testdata.{_dataset_suffix(scale, variant)}"
    queries = []
    for path in sorted(glob.glob(os.path.join(_QUERY_DIR, "q*.sql"))):
        stem = os.path.splitext(os.path.basename(path))[0]
        with open(path) as f:
            body = f.read().replace("testdata.ssb.", f"{dataset}.")
        queries.append((f"Q{stem[1]}.{stem[2]}", body))
    return queries


def _run_query(sql: str) -> tuple[float, int, int]:
    """Run one query, return (elapsed_ms, row_count, col_count)."""
    gc.collect()
    session = opteryx.session()
    try:
        rows = 0
        cols = 0
        t0 = time.monotonic_ns()
        for morsel in session.execute_to_morsels(sql):
            rows += morsel.num_rows
            if cols == 0:
                cols = len(morsel.column_names)
        return (time.monotonic_ns() - t0) / 1e6, rows, cols
    finally:
        session.close()


def main() -> int:
    parser = argparse.ArgumentParser(description="SSB benchmark vs DuckDB")
    parser.add_argument("--scale", type=str, default="10", help="Scale factor (1, 10, 100)")
    parser.add_argument("--iterations", type=int, default=3, help="Warm iterations per query")
    parser.add_argument(
        "--variant", type=str, default="",
        help="Format variant: runs against testdata/ssb_<scale>_<variant> (e.g. `skene`)",
    )
    parser.add_argument("--queries", type=str, default="", help="Comma-separated names, e.g. Q1.1,Q4.3")
    args = parser.parse_args()

    suffix = _dataset_suffix(args.scale, args.variant)
    dataset_path = os.path.join(_REPO_ROOT, "testdata", suffix)
    if not os.path.isdir(dataset_path):
        print(f"ERROR: dataset not found at {dataset_path}")
        print(f"       generate it: python dev/ssb_generate.py {args.scale}")
        if args.variant:
            print(f"       then:        python dev/parquet_to_skene.py testdata/ssb_{args.scale} {dataset_path}")
        return 1

    queries = _load_queries(args.scale, args.variant)
    if args.queries:
        wanted = [q.strip().upper() for q in args.queries.split(",") if q.strip()]
        unknown = [q for q in wanted if q not in {n for n, _ in queries}]
        if unknown:
            print(f"ERROR: unknown query name(s): {', '.join(unknown)}")
            return 1
        queries = [entry for entry in queries if entry[0] in wanted]

    baseline_path = os.path.join(_DUCKDB_DIR, f"results.sf{args.scale}.json")
    duckdb_min, duckdb_machine = load_duckdb_baseline(baseline_path)
    duckdb_shapes = load_duckdb_shapes(baseline_path)

    print("Warming up (cold start)...")
    start = time.monotonic_ns()
    warm_session = opteryx.session()
    try:
        for _ in warm_session.execute_to_morsels(f"SELECT COUNT(*) FROM testdata.{suffix}.lineorder;"):
            pass
    finally:
        warm_session.close()
    print(f"Cold start: {(time.monotonic_ns() - start) / 1e6:.2f}ms\n")

    print_banner(
        title="SSB BENCHMARK",
        opteryx_version=opteryx.__version__,
        metadata=[
            ("Scale factor", f"{args.scale}  (testdata.{suffix})"),
            ("Format", args.variant or "parquet"),
            ("Queries", str(len(queries))),
            ("Iterations", f"{args.iterations} warm runs per query"),
        ],
        duckdb_machine=duckdb_machine if duckdb_min else None,
        duckdb_query_count=len(duckdb_min) if duckdb_min else None,
    )
    print_header("Query", args.iterations, has_baseline=bool(duckdb_min))

    csv_writer, csv_path, csv_handle = open_results_csv(
        _RESULTS_DIR,
        fieldnames=["scale", "variant", "query", "run", "status", "elapsed_ms", "row_count",
                    "col_count", "duckdb_min_ms", "duckdb_rows", "duckdb_cols", "error"],
    )

    passed = 0
    failures: list[tuple[str, str]] = []
    suite_start = time.monotonic_ns()
    opteryx_total = 0.0
    duckdb_total = 0.0
    compared = 0

    try:
        for name, sql in queries:
            d_ms = duckdb_min.get(name)
            d_shape = duckdb_shapes.get(name)
            base = {
                "scale": args.scale, "variant": args.variant or "parquet", "query": name,
                "duckdb_min_ms": f"{d_ms:.3f}" if d_ms is not None else "",
                "duckdb_rows": d_shape[0] if d_shape is not None else "",
                "duckdb_cols": d_shape[1] if d_shape is not None else "",
            }
            times: list[float] = []
            rows = cols = 0
            failed = False
            for run_ix in range(1, args.iterations + 1):
                try:
                    elapsed_ms, rows, cols = _run_query(sql)
                except Exception as err:
                    msg = f"{type(err).__name__}: {err}"
                    failures.append((name, msg))
                    csv_writer.writerow({**base, "run": run_ix, "status": "error", "elapsed_ms": "",
                                         "row_count": 0, "col_count": 0, "error": msg})
                    csv_handle.flush()
                    print_error_row(name, msg)
                    failed = True
                    break
                times.append(elapsed_ms)
                csv_writer.writerow({**base, "run": run_ix, "status": "ok",
                                     "elapsed_ms": f"{elapsed_ms:.3f}", "row_count": rows,
                                     "col_count": cols, "error": ""})
                csv_handle.flush()
            if failed:
                continue

            # A timing baseline proves speed, not correctness: a shape mismatch
            # against DuckDB's recorded result is a failure, never a green row.
            if d_shape is not None and (rows, cols) != d_shape:
                shape_msg = (f"SHAPE MISMATCH: opteryx {rows} rows/{cols} cols "
                             f"vs duckdb {d_shape[0]} rows/{d_shape[1]} cols")
                failures.append((name, shape_msg))
                print_row(name, times, args.iterations, d_ms)
                print(f"          \033[38;2;255;69;69m⚠ {shape_msg}\033[0m")
                continue

            passed += 1
            print_row(name, times, args.iterations, d_ms)
            if d_ms is not None:
                opteryx_total += min(times)
                duckdb_total += d_ms
                compared += 1
    finally:
        csv_handle.close()

    print("─" * 100)
    if compared:
        print_total_row(opteryx_total, duckdb_total, compared, args.iterations)
    print()
    print(f"{passed} passed, {len(failures)} failed   ({(time.monotonic_ns() - suite_start) / 1e9:.1f}s)")
    print(f"  results: {os.path.relpath(csv_path, _REPO_ROOT)}")
    if failures:
        print("\nFAILURES")
        for name, err in failures:
            print(f"  {name}: {err}")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
