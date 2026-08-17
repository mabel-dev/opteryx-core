#!/usr/bin/env python3
"""
TPC-DS smoke runner.

Not a performance benchmark (yet) — see `make tpch` / `make clickbench` for
that shape. This runs each of the 99 TPC-DS queries once against Opteryx and
reports pass/fail, so the suite's *coverage* (how much of TPC-DS the engine
currently accepts and executes) is visible before anyone chases its numbers.

Each query runs in its own subprocess (`_query_worker.py`) with a hard
wall-clock timeout (default 30s, --timeout to change): a query that never
yields control back to Python — e.g. a missing join-key detection turning a
6-table query into a real cross join — can't be stopped by an in-process
signal-based timeout, only by killing the process. See _query_worker.py's
docstring for why.

Usage:
    make tpcds
    python tests/performance/tpcds/runner.py
    python tests/performance/tpcds/runner.py --scale 1
    python tests/performance/tpcds/runner.py --query 23   # run one query
    python tests/performance/tpcds/runner.py --timeout 60

Inputs:
    tests/performance/tpcds/opteryx/queries/*.sql   — query bodies (dev/tpcds/generate_queries.py)
    testdata/tpcds_<scale>/<table>/*.parquet        — data (dev/tpcds/generate_data.py)
"""

from __future__ import annotations

import argparse
import glob
import json
import os
import subprocess
import sys
import time

_REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
sys.path.insert(0, _REPO_ROOT)

sys.path.insert(0, os.path.join(_REPO_ROOT, "tests", "performance"))
from _common import (  # noqa: E402
    open_results_csv,
    print_banner,
    print_error_row,
)

import opteryx  # noqa: E402 — only for opteryx.__version__ in the banner; queries run in _query_worker.py

_QUERY_DIR = os.path.join(os.path.dirname(__file__), "opteryx", "queries")
_RESULTS_DIR = os.path.join(os.path.dirname(__file__), "results")
_WORKER_PATH = os.path.join(os.path.dirname(__file__), "_query_worker.py")


def _load_queries(scale: str, only: str = "") -> list[tuple[str, str]]:
    dataset = f"testdata.tpcds_{scale}"
    queries: list[tuple[str, str]] = []
    for path in sorted(glob.glob(os.path.join(_QUERY_DIR, "query*.sql"))):
        name = os.path.splitext(os.path.basename(path))[0]
        if name.startswith("query") and name[5:].isdigit():
            name = f"Q{int(name[5:]):02d}"
        if only and name != f"Q{int(only):02d}":
            continue
        body = open(path).read().replace("testdata.tpcds_tiny.", f"{dataset}.")
        queries.append((name, body))
    return queries


def _run_query(sql: str, timeout_s: float) -> tuple[str, float, int, str]:
    """Run one query in a subprocess. Returns (status, elapsed_ms, rows, error)
    with status in {"ok", "error", "timeout"}."""
    t0 = time.monotonic_ns()
    try:
        proc = subprocess.run(
            [sys.executable, _WORKER_PATH],
            input=sql,
            capture_output=True,
            text=True,
            timeout=timeout_s,
        )
    except subprocess.TimeoutExpired:
        elapsed_ms = (time.monotonic_ns() - t0) / 1e6
        return "timeout", elapsed_ms, 0, f"exceeded {timeout_s:.0f}s timeout"

    elapsed_ms = (time.monotonic_ns() - t0) / 1e6
    last_line = proc.stdout.strip().splitlines()[-1] if proc.stdout.strip() else ""
    try:
        data = json.loads(last_line)
    except json.JSONDecodeError:
        stderr_tail = proc.stderr.strip().splitlines()[-1] if proc.stderr.strip() else "no output"
        return "error", elapsed_ms, 0, f"worker crashed (exit {proc.returncode}): {stderr_tail}"

    if data["ok"]:
        return "ok", data["elapsed_ms"], data["rows"], ""
    return "error", elapsed_ms, 0, data["error"]


def main() -> int:
    parser = argparse.ArgumentParser(description="TPC-DS smoke runner")
    parser.add_argument(
        "--scale", type=str, default="1", help="Scale factor suffix matching testdata/tpcds_<scale> (default: 1)"
    )
    parser.add_argument("--query", type=str, default="", help="Run a single query number (e.g. 23)")
    parser.add_argument(
        "--timeout", type=float, default=30.0, help="Per-query wall-clock timeout in seconds (default: 30)"
    )
    args = parser.parse_args()

    dataset_path = os.path.join(_REPO_ROOT, "testdata", f"tpcds_{args.scale}")
    if not os.path.isdir(dataset_path):
        print(f"ERROR: dataset not found at {dataset_path}")
        print(f"       generate it: python dev/tpcds/generate_data.py --scale {args.scale}")
        return 1

    queries = _load_queries(args.scale, args.query)
    if not queries:
        print(f"ERROR: no matching .sql files found in {_QUERY_DIR}")
        return 1

    print_banner(
        title="TPC-DS SMOKE RUN",
        opteryx_version=opteryx.__version__,
        metadata=[
            ("Scale factor", f"{args.scale}  (testdata.tpcds_{args.scale})"),
            ("Queries", str(len(queries))),
        ],
    )

    csv_writer, csv_path, csv_handle = open_results_csv(
        _RESULTS_DIR,
        fieldnames=["scale", "query", "status", "elapsed_ms", "row_count", "error"],
    )

    passed = 0
    failed = 0
    timed_out = 0
    failures: list[tuple[str, str]] = []
    suite_start = time.monotonic_ns()

    try:
        for name, sql in queries:
            status, elapsed_ms, rows, err = _run_query(sql, args.timeout)
            if status == "ok":
                passed += 1
                print(f"{name:<8} \033[38;2;26;185;67mOK\033[0m     {elapsed_ms:>10.1f}ms   {rows:>12,} rows")
            elif status == "timeout":
                timed_out += 1
                failures.append((name, f"TIMEOUT: {err}"))
                print(f"{name:<8} \033[38;2;255;184;108mTIMEOUT\033[0m  {err}")
            else:
                failed += 1
                failures.append((name, err))
                print_error_row(name, err)
            csv_writer.writerow(
                {
                    "scale": args.scale,
                    "query": name,
                    "status": status,
                    "elapsed_ms": f"{elapsed_ms:.3f}" if status == "ok" else "",
                    "row_count": rows,
                    "error": err,
                }
            )
            csv_handle.flush()
    finally:
        csv_handle.close()

    elapsed_s = (time.monotonic_ns() - suite_start) / 1e9
    print("─" * 100)
    print(
        f"\033[38;2;26;185;67m{passed} passed\033[0m, "
        f"\033[38;2;255;121;198m{failed} failed\033[0m, "
        f"\033[38;2;255;184;108m{timed_out} timed out\033[0m   "
        f"({elapsed_s:.1f}s)"
    )
    print(f"  results: {os.path.relpath(csv_path, _REPO_ROOT)}")

    if failures:
        print()
        print("\033[38;2;139;233;253m\033[3mFAILURES\033[0m")
        for name, err in failures:
            print(f"  {name}: {err}")

    return 1 if (failed or timed_out) else 0


if __name__ == "__main__":
    sys.exit(main())
