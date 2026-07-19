#!/usr/bin/env python3
"""
odata_dashboard benchmark + DuckDB comparison runner.

Runs each of the 37 query shapes mined from the odata.opteryx.app query log
against Opteryx (warm, multi-iteration), compares the best Opteryx time to the
DuckDB baseline, and writes per-iteration results to `results/<sha>-<ts>.csv`.

Usage:
    make dash
    python tests/performance/odata_dashboard/runner.py
    python tests/performance/odata_dashboard/runner.py --iterations 5

Inputs:
    testdata/public/{gdelt_events,nvd_vulnerabilities,exploited_vulnerabilities,
    vulnerabilities_per_week,exploit_db}       - pulled via
                                                  dev/odata_benchmark/fetch_snapshots.py
    tests/performance/odata_dashboard/queries.py            - shared query bodies
    tests/performance/odata_dashboard/duckdb/results.local.json - DuckDB baseline

The DuckDB baseline is regenerated separately via `make dash-duckdb`.
"""

from __future__ import annotations

import argparse
import gc
import importlib.util
import os
import sys
import time

_REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
sys.path.insert(0, _REPO_ROOT)

sys.path.insert(0, os.path.join(_REPO_ROOT, "tests", "performance"))
from _common import (  # noqa: E402
    load_duckdb_baseline,
    open_results_csv,
    print_banner,
    print_error_row,
    print_header,
    print_row,
    print_total_row,
)

import opteryx  # noqa: E402

_DATA_DIR = os.path.join(_REPO_ROOT, "testdata", "public")
_DUCKDB_DIR = os.path.join(os.path.dirname(__file__), "duckdb")
_RESULTS_DIR = os.path.join(os.path.dirname(__file__), "results")

# Loaded by file path, not sys.path insertion - adding this directory to
# sys.path would let `import opteryx` resolve to the sibling opteryx/
# subdirectory (the pytest battery package) instead of the real top-level
# opteryx package.
_queries_path = os.path.join(os.path.dirname(__file__), "queries.py")
_spec = importlib.util.spec_from_file_location("odata_dashboard_queries", _queries_path)
_queries_module = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_queries_module)
QUERIES = _queries_module.QUERIES

_TABLES = {
    "GDELT": "testdata.public.gdelt_events",
    "NVD": "testdata.public.nvd_vulnerabilities",
    "EXPLOITED": "testdata.public.exploited_vulnerabilities",
    "VPW": "testdata.public.vulnerabilities_per_week",
    "EXPLOITDB": "testdata.public.exploit_db",
}


def _run_query(sql: str) -> tuple[float, int]:
    """Run one query, return (elapsed_ms, row_count)."""
    gc.collect()
    session = opteryx.session()
    try:
        rows = 0
        t0 = time.monotonic_ns()
        for morsel in session.execute_to_morsels(sql):
            if morsel is not None:
                rows += morsel.num_rows
        return (time.monotonic_ns() - t0) / 1e6, rows
    finally:
        session.close()


def main() -> int:
    parser = argparse.ArgumentParser(description="odata_dashboard benchmark vs DuckDB")
    parser.add_argument(
        "--iterations", type=int, default=5, help="Warm iterations per query (default: 5)"
    )
    args = parser.parse_args()

    if not os.path.isdir(_DATA_DIR):
        print(f"ERROR: dataset not found at {_DATA_DIR}")
        print("       run dev/odata_benchmark/fetch_snapshots.py first")
        return 1

    queries = [(name, body.format(**_TABLES)) for name, body in QUERIES]

    duckdb_min, duckdb_machine = load_duckdb_baseline(
        os.path.join(_DUCKDB_DIR, "results.local.json")
    )

    print("Warming up (cold start)...")
    start = time.monotonic_ns()
    warm_session = None
    try:
        warm_session = opteryx.session()
        for _ in warm_session.execute_to_morsels("SELECT COUNT(*) FROM testdata.public.gdelt_events;"):
            pass
        cold_time_ms = (time.monotonic_ns() - start) / 1e6
        print(f"Cold start: {cold_time_ms:.2f}ms\n")
    except Exception as e:
        print(f"Cold start failed: {e}\n")
    finally:
        if warm_session is not None:
            warm_session.close()

    print_banner(
        title="ODATA_DASHBOARD BENCHMARK",
        opteryx_version=opteryx.__version__,
        metadata=[
            ("Data", "testdata/public/*"),
            ("Queries", str(len(queries))),
            ("Iterations", f"{args.iterations} warm runs per query"),
        ],
        duckdb_machine=duckdb_machine if duckdb_min else None,
        duckdb_query_count=len(duckdb_min) if duckdb_min else None,
    )

    print_header("Query", args.iterations, has_baseline=bool(duckdb_min))

    csv_writer, csv_path, csv_handle = open_results_csv(
        _RESULTS_DIR,
        fieldnames=["query", "run", "status", "elapsed_ms", "row_count", "duckdb_min_ms", "error"],
    )

    passed = 0
    failed = 0
    failures: list[tuple[str, str]] = []
    suite_start = time.monotonic_ns()
    opteryx_total_min = 0.0
    duckdb_total_min = 0.0
    compared_queries = 0

    try:
        for name, sql in queries:
            d_ms = duckdb_min.get(name) if duckdb_min else None
            times: list[float] = []
            row_count = 0
            query_failed = False
            for run_ix in range(1, args.iterations + 1):
                try:
                    elapsed_ms, rows = _run_query(sql)
                    times.append(elapsed_ms)
                    row_count = rows
                    csv_writer.writerow(
                        {
                            "query": name,
                            "run": run_ix,
                            "status": "ok",
                            "elapsed_ms": f"{elapsed_ms:.3f}",
                            "row_count": rows,
                            "duckdb_min_ms": f"{d_ms:.3f}" if d_ms is not None else "",
                            "error": "",
                        }
                    )
                    csv_handle.flush()
                except Exception as err:
                    msg = f"{type(err).__name__}: {err}"
                    failures.append((name, msg))
                    failed += 1
                    query_failed = True
                    csv_writer.writerow(
                        {
                            "query": name,
                            "run": run_ix,
                            "status": "error",
                            "elapsed_ms": "",
                            "row_count": 0,
                            "duckdb_min_ms": f"{d_ms:.3f}" if d_ms is not None else "",
                            "error": msg,
                        }
                    )
                    csv_handle.flush()
                    print_error_row(name, msg)
                    break

            if query_failed or not times:
                continue
            passed += 1
            print_row(name, times, args.iterations, d_ms)
            if d_ms is not None:
                opteryx_total_min += min(times)
                duckdb_total_min += d_ms
                compared_queries += 1
    finally:
        csv_handle.close()

    print("─" * 100)
    if compared_queries:
        print_total_row(opteryx_total_min, duckdb_total_min, compared_queries, args.iterations)
    print()

    elapsed_s = (time.monotonic_ns() - suite_start) / 1e9
    print(
        f"\033[38;2;26;185;67m{passed} passed\033[0m, "
        f"\033[38;2;255;121;198m{failed} failed\033[0m   "
        f"({elapsed_s:.1f}s)"
    )
    print(f"  results: {os.path.relpath(csv_path, _REPO_ROOT)}")

    if failures:
        print()
        print("\033[38;2;139;233;253m\033[3mFAILURES\033[0m")
        for name, err in failures:
            print(f"  {name}: {err}")

    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
