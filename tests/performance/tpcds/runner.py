#!/usr/bin/env python3
"""
TPC-DS runner.

Runs each of the 99 TPC-DS queries against Opteryx, reports pass/fail/timeout
(the suite's *coverage*) and compares each query's time to the DuckDB baseline
in `duckdb/results.sf<scale>.json` (`make tpcds-sf<scale>-duckdb` regenerates
it). Each query runs --iterations times (default 3); its reported time is the
min, and the suite total is the sum of those mins — the same min-of-N the
TPC-H and SSB runners report. With no baseline file the comparison column is
omitted.

Queries run in a worker subprocess (`_query_worker.py`) with a hard wall-clock
timeout (default 30s, --timeout to change): a query that never yields control
back to Python — e.g. a missing join-key detection turning a 6-table query into
a real cross join — can't be stopped by an in-process signal-based timeout, only
by killing the process. See _query_worker.py's docstring for why.

The worker is REUSED across queries and replaced only after a timeout or a
crash, so the suite's wall time is query execution, not 99 interpreter starts and
`import opteryx` (which was ~85% of an SF1 run). Consequence: the engine's
in-process caches (footers, chunks) persist from one query to the next, as they
would in a server — later queries are not cold-process runs.

Usage:
    make tpcds-sf1 | tpcds-sf001
    python tests/performance/tpcds/runner.py --scale 1 --variant skene
    python tests/performance/tpcds/runner.py --scale 1
    python tests/performance/tpcds/runner.py --query 23   # run one query
    python tests/performance/tpcds/runner.py --timeout 60

Inputs:
    tests/performance/tpcds/opteryx/queries/*.sql        — query bodies (dev/tpcds/generate_queries.py)
    testdata/tpcds_<scale>/<table>/*.parquet             — parquet source (dev/tpcds/generate_data.py)
    testdata/tpcds_<scale>_skene/<table>/*.skene         — skene v2 mirror (dev/parquet_to_skene.py)

The make targets run the SKENE MIRROR; the parquet source is the intermediate
it is built from and stays reachable by dropping --variant.
"""

from __future__ import annotations

import argparse
import glob
import json
import os
import select
import subprocess
import sys
import tempfile
import time

_REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
sys.path.insert(0, _REPO_ROOT)

sys.path.insert(0, os.path.join(_REPO_ROOT, "tests", "performance"))
from _common import (  # noqa: E402
    AISTOR_WORKSPACE,
    aistor_network_path,
    load_duckdb_baseline,
    open_results_csv,
    print_banner,
    print_error_row,
    print_header,
    print_row,
    print_skip_row,
    print_total_row,
    print_total_row_no_baseline,
)

import opteryx  # noqa: E402 — only for opteryx.__version__ in the banner; queries run in _query_worker.py

_QUERY_DIR = os.path.join(os.path.dirname(__file__), "opteryx", "queries")
_RESULTS_DIR = os.path.join(os.path.dirname(__file__), "results")
_WORKER_PATH = os.path.join(os.path.dirname(__file__), "_query_worker.py")


def _dataset_suffix(scale: str, variant: str) -> str:
    """`tpcds_<scale>` or `tpcds_<scale>_<variant>` (e.g. variant `skene`)."""
    return f"tpcds_{scale}_{variant}" if variant else f"tpcds_{scale}"


def _load_queries(scale: str, only: str = "", variant: str = "") -> list[tuple[str, str]]:
    # `aistor` is the remote Iceberg copy of the parquet source, not a local mirror.
    if variant == AISTOR_WORKSPACE:
        dataset = f"{AISTOR_WORKSPACE}.tpcds_sf{scale}"
    else:
        dataset = f"testdata.{_dataset_suffix(scale, variant)}"
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


class _Worker:
    """One long-lived `_query_worker.py` process. `run()` sends a query and waits up
    to the timeout for its result line; on a timeout or a dead process the worker is
    killed and the next `run()` starts a fresh one."""

    def __init__(self, variant: str):
        self._args = ["--aistor"] if variant == AISTOR_WORKSPACE else []
        self._proc = None
        self._stderr = None
        self.starts = 0

    def _start(self) -> None:
        # stderr to a file, not a pipe: a chatty worker must not block on a full pipe
        # the runner is not reading, and a crash's last line is still recoverable.
        self._stderr = tempfile.TemporaryFile(mode="w+")
        self._proc = subprocess.Popen(
            [sys.executable, _WORKER_PATH, *self._args],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=self._stderr,
            text=True,
            bufsize=1,
        )
        self.starts += 1

    def _stderr_tail(self) -> str:
        self._stderr.seek(0)
        lines = self._stderr.read().strip().splitlines()
        return lines[-1] if lines else "no output"

    def stop(self) -> None:
        if self._proc is None:
            return
        if self._proc.poll() is None:
            self._proc.kill()
        self._proc.wait()
        self._stderr.close()
        self._proc = None

    def run(self, sql: str, iterations: int, timeout_s: float) -> tuple[str, list, int, str]:
        """Runs `sql` `iterations` times. Returns (status, runs_ms, rows, error), status
        in {"ok", "error", "timeout"}. `timeout_s` is PER RUN: the wait for the whole
        request is iterations x timeout_s."""
        if self._proc is None or self._proc.poll() is not None:
            self.stop()
            self._start()
        try:
            self._proc.stdin.write(json.dumps({"sql": sql, "iterations": iterations}) + "\n")
            self._proc.stdin.flush()
        except BrokenPipeError:
            tail = self._stderr_tail()
            code = self._proc.poll()
            self.stop()
            return "error", [], 0, f"worker crashed (exit {code}): {tail}"
        ready, _, _ = select.select([self._proc.stdout], [], [], timeout_s * iterations)
        if not ready:
            self.stop()
            return "timeout", [], 0, (f"exceeded {timeout_s:.0f}s per run "
                                      f"({iterations} runs)")
        line = self._proc.stdout.readline()
        if not line:
            tail = self._stderr_tail()
            code = self._proc.wait()
            self.stop()
            return "error", [], 0, f"worker crashed (exit {code}): {tail}"
        data = json.loads(line)
        if data["ok"]:
            return "ok", data["runs_ms"], data["rows"], ""
        return "error", [], 0, data["error"]


def main() -> int:
    parser = argparse.ArgumentParser(description="TPC-DS runner")
    parser.add_argument(
        "--scale", type=str, default="1", help="Scale factor suffix matching testdata/tpcds_<scale> (default: 1)"
    )
    parser.add_argument("--query", type=str, default="", help="Run a single query number (e.g. 23)")
    parser.add_argument(
        "--variant",
        type=str,
        default="",
        help="Dataset format variant: runs against testdata/tpcds_<scale>_<variant> "
        "(e.g. `skene` for the skene v2 mirror; default: the parquet dataset). "
        "`aistor` runs against the parquet source on AIStor (aistor.tpcds_sf<scale>).",
    )
    parser.add_argument(
        "--timeout", type=float, default=30.0, help="Per-run wall-clock timeout in seconds (default: 30)"
    )
    parser.add_argument(
        "--iterations", type=int, default=3,
        help="Timed runs per query; the min is reported and summed (default: 3)",
    )
    args = parser.parse_args()

    suffix = _dataset_suffix(args.scale, args.variant)
    dataset_path = os.path.join(_REPO_ROOT, "testdata", suffix)
    # Remote existence is the catalog's to answer, per query, in the worker.
    if args.variant != AISTOR_WORKSPACE and not os.path.isdir(dataset_path):
        print(f"ERROR: dataset not found at {dataset_path}")
        print(f"       expected: testdata/{suffix}")
        if args.variant:
            print(f"       generate it: python dev/parquet_to_skene.py "
                  f"testdata/tpcds_{args.scale} testdata/{suffix} lz4")
        else:
            print(f"       generate it: python dev/tpcds/generate_data.py --scale {args.scale}")
        return 1

    queries = _load_queries(args.scale, args.query, args.variant)
    if not queries:
        print(f"ERROR: no matching .sql files found in {_QUERY_DIR}")
        return 1

    duckdb_min, duckdb_machine = load_duckdb_baseline(
        os.path.join(os.path.dirname(__file__), "duckdb", f"results.sf{args.scale}.json")
    )

    network = aistor_network_path() if args.variant == AISTOR_WORKSPACE else "local"

    print_banner(
        title="TPC-DS",
        opteryx_version=opteryx.__version__,
        metadata=[
            ("Scale factor", f"{args.scale}  (testdata.{suffix})"),
            ("Format", args.variant or "parquet"),
            ("Network", network),
            ("Queries", str(len(queries))),
            ("Iterations", f"{args.iterations} runs per query; reported = min, total = sum of mins"),
            ("Worker", "one process reused across queries (in-process caches stay warm); "
                       "replaced after a timeout or crash"),
        ],
        duckdb_machine=duckdb_machine if duckdb_min else None,
        duckdb_query_count=len(duckdb_min) if duckdb_min else None,
    )

    csv_writer, csv_path, csv_handle = open_results_csv(
        _RESULTS_DIR,
        fieldnames=[
            "scale", "variant", "query", "status", "elapsed_ms", "runs_ms", "row_count", "duckdb_min_ms", "network", "error",
        ],
    )

    print_header("Query", args.iterations, has_baseline=bool(duckdb_min))

    worker = _Worker(args.variant)
    passed = 0
    failed = 0
    total_min_ms = 0.0
    duckdb_total_ms = 0.0
    compared_opteryx_ms = 0.0
    compared = 0
    timed_out = 0
    failures: list[tuple[str, str]] = []
    suite_start = time.monotonic_ns()

    try:
        for name, sql in queries:
            status, runs_ms, rows, err = worker.run(sql, args.iterations, args.timeout)
            elapsed_ms = min(runs_ms) if runs_ms else 0.0
            d_ms = duckdb_min.get(name)
            if status == "ok":
                passed += 1
                total_min_ms += elapsed_ms
                print_row(name, runs_ms, args.iterations, d_ms)
                if d_ms is not None:
                    compared_opteryx_ms += elapsed_ms
                    duckdb_total_ms += d_ms
                    compared += 1
            elif status == "timeout":
                timed_out += 1
                failures.append((name, f"TIMEOUT: {err}"))
                print_skip_row(name, f"timeout: {err}")
            else:
                failed += 1
                failures.append((name, err))
                print_error_row(name, err)
            csv_writer.writerow(
                {
                    "scale": args.scale,
                    "variant": args.variant or "parquet",
                    "query": name,
                    "status": status,
                    "elapsed_ms": f"{elapsed_ms:.3f}" if status == "ok" else "",
                    "runs_ms": ";".join(f"{r:.3f}" for r in runs_ms),
                    "row_count": rows,
                    "duckdb_min_ms": f"{d_ms:.3f}" if d_ms is not None else "",
                    "network": network,
                    "error": err,
                }
            )
            csv_handle.flush()
    finally:
        worker.stop()
        csv_handle.close()

    elapsed_s = (time.monotonic_ns() - suite_start) / 1e9
    print("─" * 100)
    if compared:
        # Both totals sum only the queries that have a baseline, so the ratio compares like with like.
        print_total_row(compared_opteryx_ms, duckdb_total_ms, compared, args.iterations)
    else:
        print_total_row_no_baseline(total_min_ms, passed, args.iterations)
    print(
        f"\033[38;2;26;185;67m{passed} passed\033[0m, "
        f"\033[38;2;255;121;198m{failed} failed\033[0m, "
        f"\033[38;2;255;184;108m{timed_out} timed out\033[0m   "
        f"   sum of min-of-{args.iterations}: {total_min_ms / 1000:.2f}s over {passed} queries"
        f"   (wall {elapsed_s:.1f}s, worker processes: {worker.starts})"
    )
    print(f"  network: {network}")
    print(f"  results: {os.path.relpath(csv_path, _REPO_ROOT)}")

    if failures:
        print()
        print("\033[38;2;139;233;253m\033[3mFAILURES\033[0m")
        for name, err in failures:
            print(f"  {name}: {err}")

    return 1 if (failed or timed_out) else 0


if __name__ == "__main__":
    sys.exit(main())
