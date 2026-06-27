"""
H2O db-benchmark runner for Opteryx.

Executes the H2O groupby (g1..g10) and join (j1..j5) query suites against
Opteryx, mirroring tests/performance/job/run.py. For each query:

  - rewrites bare H2O table names (`x`, `small`, `medium`, `big`) to
    `testdata.h2o.<size>.<table>` so Opteryx can resolve them through the
    dataset registry,
  - opens a fresh Opteryx session per RUN,
  - executes via session.execute_to_morsels() and drains the iterator,
  - times wall-clock,
  - enforces a per-query timeout (default 600s) — recorded as a timeout,
    not a crash,
  - runs each query TWICE (cold / warm) per the upstream H2O convention.

Output:
    tests/performance/h2o/results/<git-sha>-<timestamp>.csv

Columns: workload, size, query, run, status, elapsed_ms, row_count, error_msg.
"""

from __future__ import annotations

import argparse
import csv
import re
import statistics
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(1, str(HERE.parents[2]))

import opteryx  # noqa: E402

JOIN_TABLES = ("x", "small", "medium", "big")
GROUPBY_TABLES = ("x",)  # rewritten to `x_groupby` (see generator)

QUERY_RE = re.compile(r"^([gj])([0-9]+)\.sql$")


def _query_sort_key(path: Path):
    m = QUERY_RE.match(path.name)
    if not m:
        return ("z", 10**9)
    return (m.group(1), int(m.group(2)))


def _rewrite_query(sql: str, size: str, workload: str) -> str:
    """Replace bare table names with `testdata.h2o.<size>.<table>`.

    Group-by queries use `x_groupby` (string ids first); join queries use
    the join schema tables (`x`, `small`, `medium`, `big`).
    """
    prefix = f"testdata.h2o.{size}."

    if workload == "groupby":
        return re.sub(r"(?<![\w.])x\b", prefix + "x_groupby", sql)

    out = sql
    for table in sorted(JOIN_TABLES, key=len, reverse=True):
        pattern = re.compile(r"(?<![\w.])(" + re.escape(table) + r")\b")
        out = pattern.sub(prefix + table, out)
    return out


def _git_sha() -> str:
    try:
        sha = subprocess.check_output(
            ["git", "rev-parse", "--short", "HEAD"],
            cwd=str(HERE),
            stderr=subprocess.DEVNULL,
        ).decode().strip()
        return sha or "nogit"
    except Exception:
        return "nogit"


def _run_one(sql: str, timeout_s: float) -> tuple[str, float, int, str]:
    """Execute one query with a wall-clock timeout, in-process.

    Same approach as the JOB runner: a hard interrupt of in-flight Cython
    is not possible, so the deadline is checked between morsels.
    """
    start = time.monotonic()
    deadline = start + timeout_s
    rows = 0
    session = None
    try:
        session = opteryx.session()
        morsels = session.execute_to_morsels(sql)
        for morsel in morsels:
            try:
                rows += morsel.num_rows
            except AttributeError:
                try:
                    rows += len(morsel)
                except Exception:
                    pass
            if time.monotonic() > deadline:
                elapsed_ms = (time.monotonic() - start) * 1000.0
                return ("timeout", elapsed_ms, rows, f"exceeded {timeout_s:.0f}s")
        elapsed_ms = (time.monotonic() - start) * 1000.0
        return ("ok", elapsed_ms, rows, "")
    except Exception as e:
        elapsed_ms = (time.monotonic() - start) * 1000.0
        msg = f"{type(e).__name__}: {e}"
        return ("error", elapsed_ms, rows, msg[:500])
    finally:
        if session is not None:
            try:
                session.close()
            except Exception:
                pass


def _collect(workload: str, queries_dir: Path) -> list[Path]:
    prefix = "g" if workload == "groupby" else "j"
    files = [
        p for p in queries_dir.glob(f"{prefix}*.sql")
        if QUERY_RE.match(p.name)
    ]
    return sorted(files, key=_query_sort_key)


def main() -> int:
    parser = argparse.ArgumentParser(description="H2O db-benchmark runner for Opteryx")
    parser.add_argument(
        "--workload",
        choices=["groupby", "join", "both"],
        default="both",
    )
    parser.add_argument(
        "--size",
        choices=["small", "medium", "large"],
        default="small",
    )
    parser.add_argument(
        "--queries-dir",
        type=Path,
        default=HERE / "queries",
    )
    parser.add_argument(
        "--results-dir",
        type=Path,
        default=HERE / "results",
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
        help="run only queries whose stem matches this regex (e.g. '^g[12]$')",
    )
    parser.add_argument(
        "--runs",
        type=int,
        default=2,
        help="how many times to run each query (default: 2 — cold + warm)",
    )
    args = parser.parse_args()

    workloads = ["groupby", "join"] if args.workload == "both" else [args.workload]

    args.results_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%dT%H%M%S")
    results_path = args.results_dir / f"{_git_sha()}-{ts}.csv"

    print("=" * 80)
    print(f"H2O db-benchmark — Opteryx {opteryx.__version__}")
    print(f"Size:    {args.size}")
    print(f"Workloads: {', '.join(workloads)}")
    print(f"Runs/query: {args.runs}  (1=cold, 2=warm)")
    print(f"Timeout: {args.timeout:.0f}s/query")
    print(f"Output:  {results_path}")
    print("=" * 80)
    print(f"{'Workload':<8} {'Query':<6} {'Run':<3} {'Status':<8} {'Elapsed':>12} {'Rows':>12}  Note")
    print("-" * 80)

    suite_start = time.monotonic()
    counts = {"ok": 0, "timeout": 0, "error": 0}
    ok_times: list[float] = []

    with open(results_path, "w", newline="") as fh:
        writer = csv.writer(fh)
        writer.writerow([
            "workload", "size", "query", "run",
            "status", "elapsed_ms", "row_count", "error_msg",
        ])

        for workload in workloads:
            queries = _collect(workload, args.queries_dir)
            if args.filter:
                flt = re.compile(args.filter)
                queries = [q for q in queries if flt.match(q.stem)]
            if not queries:
                print(f"[!] no queries matched for workload={workload}")
                continue

            for path in queries:
                stem = path.stem
                raw_sql = path.read_text()
                sql = _rewrite_query(raw_sql, args.size, workload)

                for run_idx in range(1, args.runs + 1):
                    status, elapsed_ms, rows, msg = _run_one(sql, args.timeout)
                    counts[status] += 1
                    if status == "ok":
                        ok_times.append(elapsed_ms)

                    note = ""
                    if status == "ok":
                        colour = "\033[32mok\033[0m"
                    elif status == "timeout":
                        colour = "\033[33mtimeout\033[0m"
                        note = msg
                    else:
                        colour = "\033[31merror\033[0m"
                        note = msg

                    print(
                        f"{workload:<8} {stem:<6} {run_idx:<3} {colour:<17} "
                        f"{elapsed_ms:>10.1f}ms {rows:>12,}  {note[:60]}"
                    )

                    writer.writerow([
                        workload, args.size, stem, run_idx,
                        status, f"{elapsed_ms:.3f}", rows, msg,
                    ])
                    fh.flush()

    suite_elapsed = time.monotonic() - suite_start

    print("-" * 80)
    print(
        f"Total: ok={counts['ok']}  timeout={counts['timeout']}  error={counts['error']}"
        f"  total_wall={suite_elapsed:.1f}s"
    )
    if ok_times:
        med = statistics.median(ok_times)
        p95 = (
            statistics.quantiles(ok_times, n=100)[94]
            if len(ok_times) >= 20
            else max(ok_times)
        )
        print(f"       median(ok)={med:.1f}ms  p95(ok)={p95:.1f}ms")
    print(f"results: {results_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
