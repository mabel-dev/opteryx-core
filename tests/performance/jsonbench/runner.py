#!/usr/bin/env python3
"""
JSONBench benchmark + DuckDB comparison runner.

Runs the 5 upstream JSONBench queries (github.com/ClickHouse/JSONBench)
against rugo's JSONL reader (warm, multi-iteration; see ./rugo/runner.py for
why this is a hand-written Python scan-and-aggregate rather than SQL —
neither Opteryx nor rugo can run SQL against JSON), compares each query's
best time to the DuckDB baseline, and writes per-iteration results to
`results/<sha>-<ts>.csv`.

This is NOT an apples-to-apples engine comparison like ClickBench/TPC-H: the
DuckDB baseline times query execution against an already-loaded native-
storage table (load time excluded), while the rugo numbers are a full
scan-and-parse of the raw NDJSON on every iteration (there is no persisted
storage to query against). See README.md for the full set of caveats. The
purpose of this benchmark is to gauge the scale of that gap and inform
whether Opteryx should gain native JSON reading.

Usage:
    make jsonbench                              # default: 10m rows, 2 warm iterations
    python tests/performance/jsonbench/runner.py
    python tests/performance/jsonbench/runner.py --size 100 --iterations 1
"""

from __future__ import annotations

import argparse
import gc
import glob
import os
import sys
import time

_HERE = os.path.dirname(os.path.abspath(__file__))
_REPO_ROOT = os.path.abspath(os.path.join(_HERE, "..", "..", ".."))
sys.path.insert(0, _REPO_ROOT)
sys.path.insert(0, os.path.join(_REPO_ROOT, "tests", "performance"))

# Load fetch_data.py + rugo/runner.py by explicit file path rather than putting this
# directory on sys.path — this dir's `rugo/` subpackage would otherwise shadow the real
# installed `rugo` package that rugo/runner.py itself needs to import (same trap as
# clickbench/runner.py's comment on opteryx/runner.py).
import importlib.util  # noqa: E402


def _load_module(name: str, rel_path: str):
    path = os.path.join(_HERE, rel_path)
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


fetch = _load_module("_jsonbench_fetch_data", "fetch_data.py").fetch
QUERIES = _load_module("_jsonbench_rugo_runner", os.path.join("rugo", "runner.py")).QUERIES

from _common import (  # noqa: E402
    open_results_csv,
    print_banner,
    print_error_row,
    print_header,
    print_row,
    print_total_row,
)

_DUCKDB_DIR = os.path.join(_HERE, "duckdb")
_RESULTS_DIR = os.path.join(_HERE, "results")
_JSONL_DIR = os.path.join(_REPO_ROOT, "testdata", "_downloads", "jsonbench", "decompressed")


def _load_jsonbench_baseline(path: str) -> tuple[dict[str, float], str | None]:
    """Load our local DuckDB JSONBench baseline: positional `result: [[t1,t2,t3], ...]`
    (matching the upstream JSONBench/ClickBench convention), mapped to `Q1`.. by index."""
    if not os.path.exists(path):
        return {}, None
    import json

    with open(path) as f:
        data = json.load(f)
    by_name: dict[str, float] = {}
    for ix, runs in enumerate(data.get("result", []), start=1):
        valid = [t for t in runs if t is not None]
        if valid:
            by_name[f"Q{ix}"] = min(valid) * 1000.0
    return by_name, data.get("machine")


def _run_query(fn, paths) -> tuple[float, int]:
    gc.collect()
    t0 = time.monotonic_ns()
    rows = fn(paths)
    elapsed_ms = (time.monotonic_ns() - t0) / 1e6
    return elapsed_ms, len(rows)


def main() -> int:
    parser = argparse.ArgumentParser(description="JSONBench benchmark vs DuckDB (rugo JSONL reader)")
    parser.add_argument("--size", type=int, default=10, choices=(1, 10, 100), help="Dataset size in millions of rows (default: 10)")
    parser.add_argument("--iterations", type=int, default=2, help="Warm iterations per query (default: 2)")
    parser.add_argument("--skip-fetch", action="store_true", help="Don't fetch/decompress data; fail if missing")
    parser.add_argument(
        "--duckdb-baseline",
        type=str,
        default=None,
        help="Path to DuckDB baseline JSON (default: duckdb/results.local.<size>m.json)",
    )
    args = parser.parse_args()

    if args.skip_fetch:
        paths = sorted(glob.glob(os.path.join(_JSONL_DIR, "file_*.jsonl")))[: args.size]
        if len(paths) < args.size:
            print(f"ERROR: expected {args.size} decompressed shard(s) in {_JSONL_DIR}, found {len(paths)}")
            return 1
    else:
        paths = fetch(args.size)

    baseline_path = args.duckdb_baseline or os.path.join(_DUCKDB_DIR, f"results.local.{args.size}m.json")
    duckdb_min, duckdb_machine = _load_jsonbench_baseline(baseline_path)

    data_size = sum(os.path.getsize(p) for p in paths)
    print_banner(
        title="JSONBENCH BENCHMARK (rugo)",
        opteryx_version="n/a — rugo has no SQL layer; queries are hand-written Python scans",
        metadata=[
            ("Dataset", f"Bluesky NDJSON, {args.size}m rows ({data_size / 1e9:.2f}GB decompressed)"),
            ("Queries", str(len(QUERIES))),
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
    rugo_total_min = 0.0
    duckdb_total_min = 0.0
    compared = 0

    try:
        for name, fn in QUERIES:
            d_ms = duckdb_min.get(name) if duckdb_min else None
            times: list[float] = []
            had_failure = False
            for run_ix in range(1, args.iterations + 1):
                try:
                    elapsed_ms, rows = _run_query(fn, paths)
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
                rugo_total_min += min(times)
                duckdb_total_min += d_ms
                compared += 1
    finally:
        csv_handle.close()

    print("─" * 100)
    if compared:
        print_total_row(rugo_total_min, duckdb_total_min, compared, args.iterations)
    print()

    elapsed_s = (time.monotonic_ns() - suite_start) / 1e9
    print(
        f"\033[38;2;26;185;67m{passed} passed\033[0m, "
        f"\033[38;2;255;121;198m{failed} failed\033[0m   "
        f"({elapsed_s:.1f}s)"
    )
    print(f"  results: {os.path.relpath(csv_path, _REPO_ROOT)}")
    if duckdb_min:
        print(f"  baseline: {os.path.relpath(baseline_path, _REPO_ROOT)}")
    else:
        print(f"  baseline: none found at {os.path.relpath(baseline_path, _REPO_ROOT)} (run `make jsonbench-duckdb`)")

    if failures:
        print()
        print("\033[38;2;139;233;253m\033[3mFAILURES\033[0m")
        for name, err in failures:
            print(f"  {name}: {err}")

    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
