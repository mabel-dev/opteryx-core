#!/usr/bin/env python3
"""
ClickBench benchmark + DuckDB comparison runner.

Runs the ClickBench query battery against Opteryx (warm, multi-iteration),
compares each query's best time to the DuckDB baseline, and writes
per-iteration results to `results/<sha>-<ts>.csv`.

The query bodies (`STATEMENTS`) and the pytest entry point live in
`opteryx/runner.py` — this file is the benchmark + comparison front-end.

Usage:
    make clickbench
    python tests/performance/clickbench/runner.py
    python tests/performance/clickbench/runner.py --iterations 3
    python tests/performance/clickbench/runner.py --duckdb-baseline path/to.json
"""

from __future__ import annotations

import argparse
import gc
import os
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
_REPO_ROOT = os.path.abspath(os.path.join(HERE, "..", "..", ".."))
sys.path.insert(0, _REPO_ROOT)
sys.path.insert(0, os.path.join(_REPO_ROOT, "tests", "performance"))

# Load STATEMENTS + Dataset from the pytest entry-point file without putting
# its parent dir on sys.path — the `opteryx/` subdirectory there would shadow
# the real `opteryx` package.
import importlib.util  # noqa: E402

_pytest_runner_path = os.path.join(HERE, "opteryx", "runner.py")
_spec = importlib.util.spec_from_file_location("_clickbench_pytest_runner", _pytest_runner_path)
_pytest_runner = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_pytest_runner)
STATEMENTS = _pytest_runner.STATEMENTS
DATASET = _pytest_runner.DATASET

import opteryx  # noqa: E402

from _common import (  # noqa: E402
    open_results_csv,
    print_banner,
    print_error_row,
    print_header,
    print_row,
    print_total_row,
)

_DUCKDB_DIR = os.path.join(HERE, "duckdb")
_RESULTS_DIR = os.path.join(HERE, "results")


def _load_clickbench_baseline(path: str) -> tuple[dict[str, float], str | None]:
    """Load the positional ClickBench DuckDB baseline.

    The upstream format is `{"result": [[run1, run2, run3], ...]}` indexed by
    query position; we map it to `Q01`..`QNN` so the comparison logic matches
    the named TPC-H/JOB/H2O baselines.

    Times in the JSON are in seconds; we return ms.
    """
    if not os.path.exists(path):
        return {}, None
    try:
        import json

        with open(path) as f:
            data = json.load(f)
    except Exception:
        return {}, None
    by_name: dict[str, float] = {}
    for ix, runs in enumerate(data.get("result", []), start=1):
        if not runs:
            continue
        # Use warm2 (second warm) to match the existing ClickBench convention.
        warm = runs[2] if len(runs) >= 3 else runs[-1]
        by_name[f"Q{ix:02d}"] = float(warm) * 1000.0
    return by_name, data.get("machine")


def _resolve_baseline_path(explicit: str | None) -> str:
    if explicit:
        return explicit
    local = os.path.join(_DUCKDB_DIR, "results.local.json")
    if os.path.exists(local):
        return local
    return os.path.join(_DUCKDB_DIR, "results.c6a.4xlarge.json")


def _run_query(sql: str) -> tuple[float, int]:
    gc.collect()
    session = opteryx.session()
    try:
        rows = 0
        t0 = time.monotonic_ns()
        for morsel in session.execute_to_morsels(sql):
            if morsel is not None and hasattr(morsel, "num_rows"):
                rows += morsel.num_rows
        return (time.monotonic_ns() - t0) / 1e6, rows
    finally:
        session.close()


def main() -> int:
    parser = argparse.ArgumentParser(description="ClickBench benchmark vs DuckDB")
    parser.add_argument(
        "--iterations",
        type=int,
        default=2,
        help="Warm iterations per query (default: 2)",
    )
    parser.add_argument(
        "--duckdb-baseline",
        type=str,
        default=None,
        help="Path to DuckDB baseline JSON (default: duckdb/results.local.json, "
        "fallback duckdb/results.c6a.4xlarge.json)",
    )
    args = parser.parse_args()

    baseline_path = _resolve_baseline_path(args.duckdb_baseline)
    duckdb_min, duckdb_machine = _load_clickbench_baseline(baseline_path)

    # Cold start
    print("Warming up (cold start)...")
    start = time.monotonic_ns()
    warm_session = None
    try:
        warm_session = opteryx.session()
        for _ in warm_session.execute_to_morsels(f"SELECT COUNT(*) FROM {DATASET.value};"):
            pass
        cold_time_ms = (time.monotonic_ns() - start) / 1e6
        print(f"Cold start: {cold_time_ms:.2f}ms\n")
    except Exception as e:
        print(f"Cold start failed: {e}\n")
    finally:
        if warm_session is not None:
            warm_session.close()

    print_banner(
        title="CLICKBENCH BENCHMARK",
        opteryx_version=opteryx.__version__,
        metadata=[
            ("Dataset", f"{DATASET.name} ({DATASET.value})"),
            ("Queries", str(len(STATEMENTS))),
            ("Iterations", f"{args.iterations} warm runs per query"),
        ],
        duckdb_machine=duckdb_machine if duckdb_min else None,
        duckdb_query_count=len(duckdb_min) if duckdb_min else None,
    )

    print_header("Query", args.iterations, has_baseline=bool(duckdb_min))

    csv_writer, csv_path, csv_handle = open_results_csv(
        _RESULTS_DIR,
        fieldnames=[
            "query",
            "run",
            "status",
            "elapsed_ms",
            "row_count",
            "duckdb_min_ms",
            "error",
        ],
    )

    passed = 0
    failed = 0
    skipped = 0
    failures: list[tuple[str, str]] = []
    suite_start = time.monotonic_ns()
    opteryx_total_min = 0.0
    duckdb_total_min = 0.0
    compared = 0

    try:
        for index, (statement_template, _expected_err) in enumerate(STATEMENTS, start=1):
            name = f"Q{index:02d}"
            # Skip commented-out statements (e.g. `--/* 34 */ ...`)
            if statement_template.lstrip().startswith("--"):
                skipped += 1
                csv_writer.writerow(
                    {
                        "query": name,
                        "run": 0,
                        "status": "skip",
                        "elapsed_ms": "",
                        "row_count": 0,
                        "duckdb_min_ms": "",
                        "error": "commented out",
                    }
                )
                csv_handle.flush()
                continue
            sql = statement_template.replace("{DATASET}", DATASET.value)
            d_ms = duckdb_min.get(name) if duckdb_min else None
            times: list[float] = []
            had_failure = False
            for run_ix in range(1, args.iterations + 1):
                try:
                    elapsed_ms, rows = _run_query(sql)
                except Exception as err:
                    msg = f"{type(err).__name__}: {err}"
                    failures.append((name, msg))
                    failed += 1
                    had_failure = True
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
                times.append(elapsed_ms)
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
            if had_failure or not times:
                continue
            passed += 1
            print_row(name, times, args.iterations, d_ms)
            if d_ms is not None:
                opteryx_total_min += min(times)
                duckdb_total_min += d_ms
                compared += 1
    finally:
        csv_handle.close()

    print("─" * 100)
    if compared:
        print_total_row(opteryx_total_min, duckdb_total_min, compared, args.iterations)
    print()

    elapsed_s = (time.monotonic_ns() - suite_start) / 1e9
    print(
        f"\033[38;2;26;185;67m{passed} passed\033[0m, "
        f"\033[38;2;255;121;198m{failed} failed\033[0m, "
        f"\033[38;2;128;128;128m{skipped} skipped\033[0m   "
        f"({elapsed_s:.1f}s)"
    )
    print(f"  results: {os.path.relpath(csv_path, _REPO_ROOT)}")
    if duckdb_min:
        print(f"  baseline: {os.path.relpath(baseline_path, _REPO_ROOT)}")

    if failures:
        print()
        print("\033[38;2;139;233;253m\033[3mFAILURES\033[0m")
        for name, err in failures:
            print(f"  {name}: {err}")

    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
