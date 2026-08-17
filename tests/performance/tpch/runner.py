#!/usr/bin/env python3
"""
TPC-H benchmark + DuckDB comparison runner.

Runs each TPC-H query against Opteryx (warm, multi-iteration), compares the
best Opteryx time to the DuckDB baseline at the same scale factor, and writes
per-iteration results to `results/<sha>-<ts>.csv`.

Usage:
    make tpch                                  # default: SF=1, 3 warm iterations
    python tests/performance/tpch/runner.py
    python tests/performance/tpch/runner.py --scale 001
    python tests/performance/tpch/runner.py --iterations 5

Inputs:
    tests/integration/sql_battery/test_data/tests/tpch/*.sql   — query bodies
    tests/performance/tpch/duckdb/results.sf{scale}.json       — DuckDB baseline

The DuckDB baseline is regenerated separately via `make tpch-bench-duckdb`.
"""

from __future__ import annotations

import argparse
import gc
import glob
import os
import sys
import time

# Repo root on sys.path so `import opteryx` resolves to the source tree.
_REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
sys.path.insert(0, _REPO_ROOT)

# Performance helpers (shared display + CSV layout)
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


_QUERY_DIR = os.path.join(os.path.dirname(__file__), "opteryx", "queries")
_DUCKDB_DIR = os.path.join(os.path.dirname(__file__), "duckdb")
_RESULTS_DIR = os.path.join(os.path.dirname(__file__), "results")


def _dataset_suffix(scale: str, variant: str) -> str:
    """`tpch_<scale>` or `tpch_<scale>_<variant>` (e.g. variant `skene`)."""
    return f"tpch_{scale}_{variant}" if variant else f"tpch_{scale}"


def _scale_to_dataset(scale: str, variant: str = "") -> str:
    """Map CLI scale token (`1`, `001`, …) to the testdata workspace path."""
    return f"testdata.{_dataset_suffix(scale, variant)}"


def _load_queries(scale: str, variant: str = "") -> list[tuple[str, str]]:
    """[(name, sql), ...] sorted by name; placeholder table prefixes rewritten."""
    dataset = _scale_to_dataset(scale, variant)
    queries: list[tuple[str, str]] = []
    for path in sorted(glob.glob(os.path.join(_QUERY_DIR, "query*.sql"))):
        name = os.path.splitext(os.path.basename(path))[0]
        if name.startswith("query") and name[5:].isdigit():
            name = f"Q{int(name[5:]):02d}"
        body = open(path).read()
        body = body.replace("testdata.tpch_tiny.", f"{dataset}.")
        body = body.replace("testdata.tpch.", f"{dataset}.")
        queries.append((name, body))
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
            if morsel is not None and hasattr(morsel, "num_rows"):
                rows += morsel.num_rows
                if cols == 0:
                    cols = len(morsel.column_names)
        return (time.monotonic_ns() - t0) / 1e6, rows, cols
    finally:
        session.close()


def main() -> int:
    parser = argparse.ArgumentParser(description="TPC-H benchmark vs DuckDB")
    parser.add_argument(
        "--scale",
        type=str,
        default="10",
        # SF10, not SF1. At SF1 planning is 19.1% of the suite's total work, so
        # the benchmark substantially measures the PLANNER and dilutes any engine
        # change by ~1.2x before it can be seen; at SF10 planning is 2.6%. SF1
        # also puts each table in one parquet file, so the scan's per-file
        # parallelism is barely exercised, and the whole dataset is page-cache
        # resident, which makes codec measurements say the opposite of what they
        # say at a realistic size. Smaller scales stay available via --scale.
        help="Scale factor suffix matching testdata/tpch_<scale> (default: 10)",
    )
    parser.add_argument(
        "--iterations",
        type=int,
        default=3,
        help="Warm iterations per query (default: 3)",
    )
    parser.add_argument(
        "--variant",
        type=str,
        default="",
        help="Dataset format variant: runs against testdata/tpch_<scale>_<variant> "
        "(e.g. `skene` for the skene mirror; default: the parquet dataset)",
    )
    args = parser.parse_args()

    suffix = _dataset_suffix(args.scale, args.variant)
    dataset = _scale_to_dataset(args.scale, args.variant)
    dataset_path = os.path.join(_REPO_ROOT, "testdata", suffix)
    if not os.path.isdir(dataset_path):
        print(f"ERROR: dataset not found at {dataset_path}")
        print(f"       expected: testdata/{suffix}")
        if args.variant:
            print(f"       generate it: python dev/parquet_to_skene.py "
                  f"testdata/tpch_{args.scale} testdata/{suffix}")
        return 1

    queries = _load_queries(args.scale, args.variant)
    if not queries:
        print(f"ERROR: no .sql files found in {_QUERY_DIR}")
        return 1

    duckdb_baseline_path = os.path.join(_DUCKDB_DIR, f"results.sf{args.scale}.json")
    duckdb_min, duckdb_machine = load_duckdb_baseline(duckdb_baseline_path)
    duckdb_shapes = load_duckdb_shapes(duckdb_baseline_path)

    # Cold start
    print("Warming up (cold start)...")
    start = time.monotonic_ns()
    warm_session = None
    try:
        warm_session = opteryx.session()
        for _ in warm_session.execute_to_morsels(
            f"SELECT COUNT(*) FROM testdata.{suffix};"
        ):
            pass
        cold_time_ms = (time.monotonic_ns() - start) / 1e6
        print(f"Cold start: {cold_time_ms:.2f}ms\n")
    except Exception as e:
        print(f"Cold start failed: {e}\n")
    finally:
        if warm_session is not None:
            warm_session.close()

    print_banner(
        title="TPC-H BENCHMARK",
        opteryx_version=opteryx.__version__,
        metadata=[
            ("Scale factor", f"{args.scale}  ({dataset})"),
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
        fieldnames=[
            "scale",
            "variant",
            "query",
            "run",
            "status",
            "elapsed_ms",
            "row_count",
            "col_count",
            "duckdb_min_ms",
            "duckdb_rows",
            "duckdb_cols",
            "error",
        ],
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
            d_shape = duckdb_shapes.get(name)
            times: list[float] = []
            row_count = 0
            col_count = 0
            query_failed = False
            for run_ix in range(1, args.iterations + 1):
                try:
                    elapsed_ms, rows, cols = _run_query(sql)
                    times.append(elapsed_ms)
                    row_count = rows
                    col_count = cols
                    csv_writer.writerow(
                        {
                            "scale": args.scale,
                            "variant": args.variant or "parquet",
                            "query": name,
                            "run": run_ix,
                            "status": "ok",
                            "elapsed_ms": f"{elapsed_ms:.3f}",
                            "row_count": rows,
                            "col_count": cols,
                            "duckdb_min_ms": f"{d_ms:.3f}" if d_ms is not None else "",
                            "duckdb_rows": d_shape[0] if d_shape is not None else "",
                            "duckdb_cols": d_shape[1] if d_shape is not None else "",
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
                            "scale": args.scale,
                            "variant": args.variant or "parquet",
                            "query": name,
                            "run": run_ix,
                            "status": "error",
                            "elapsed_ms": "",
                            "row_count": 0,
                            "col_count": 0,
                            "duckdb_min_ms": f"{d_ms:.3f}" if d_ms is not None else "",
                            "duckdb_rows": d_shape[0] if d_shape is not None else "",
                            "duckdb_cols": d_shape[1] if d_shape is not None else "",
                            "error": msg,
                        }
                    )
                    csv_handle.flush()
                    print_error_row(name, msg)
                    break

            if query_failed or not times:
                continue

            # A DuckDB timing baseline only proves Opteryx was fast — not that
            # it was RIGHT. When the baseline JSON also recorded a result shape
            # (see load_duckdb_shapes), a mismatch here is a correctness
            # regression wearing a passing benchmark: fail loud rather than
            # report green on a wrong answer.
            if d_shape is not None and (row_count, col_count) != d_shape:
                failed += 1
                shape_msg = (
                    f"SHAPE MISMATCH: opteryx {row_count} rows/{col_count} cols "
                    f"vs duckdb {d_shape[0]} rows/{d_shape[1]} cols"
                )
                failures.append((name, shape_msg))
                print_row(name, times, args.iterations, d_ms)
                print(f"          \033[38;2;255;69;69m⚠ {shape_msg}\033[0m")
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
