#!/usr/bin/env python3
"""
H2O db-benchmark runner for Opteryx.

Executes the H2O groupby (g1..g10) and join (j1..j5) query suites against
Opteryx. For each query:

  - rewrites bare H2O table names (`x`, `small`, `medium`, `big`) to
    `testdata.h2o.<size>.<table>` so Opteryx can resolve them through the
    dataset registry,
  - opens a fresh Opteryx session per iteration,
  - executes via session.execute_to_morsels() and drains the iterator,
  - times wall-clock,
  - enforces a per-query timeout (default 600s),
  - compares min time to the DuckDB baseline (if present),
  - writes per-iteration rows to results/<sha>-<ts>.csv.

The query name in the comparison column is `<workload>/<query>` (e.g.
`groupby/g1`) so the same DuckDB baseline file can hold both workloads.
"""

from __future__ import annotations

import argparse
import gc
import os
import re
import sys
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
_REPO_ROOT = HERE.parents[2]
sys.path.insert(0, str(_REPO_ROOT))
sys.path.insert(0, str(_REPO_ROOT / "tests" / "performance"))

from _common import (  # noqa: E402
    load_duckdb_baseline,
    open_results_csv,
    print_banner,
    print_error_row,
    print_header,
    print_row,
    print_skip_row,
    print_total_row,
)

import opteryx  # noqa: E402

JOIN_TABLES = ("x", "small", "medium", "big")
QUERY_RE = re.compile(r"^([gj])([0-9]+)\.sql$")


def _query_sort_key(path: Path):
    m = QUERY_RE.match(path.name)
    if not m:
        return ("z", 10**9)
    return (m.group(1), int(m.group(2)))


def _dataset_prefix(variant: str, size: str) -> str:
    """`testdata.h2o_skene.` or `testdata.h2o.<size>.` — never a silent fallback.

    The skene mirror is built at one size (medium), so it carries no size level:
    `testdata.h2o_skene.<table>`. The parquet tree keeps its per-size layout.
    An unknown variant is a hard failure rather than a fallback to the other
    corpus.
    """
    if variant not in ("skene", "parquet"):
        raise ValueError(f"unknown variant {variant!r}; expected 'skene' or 'parquet'")
    return "testdata.h2o_skene." if variant == "skene" else f"testdata.h2o.{size}."


def _rewrite_query(sql: str, size: str, workload: str, variant: str = "skene") -> str:
    """Replace bare table names with the dataset-qualified form.

    Group-by queries use `x_groupby` (string ids first); join queries use
    the join schema tables (`x`, `small`, `medium`, `big`). The two workloads
    have different schemas behind the same bare `x`, so the prefix alone is not
    enough — the workload decides which table `x` means.
    """
    prefix = _dataset_prefix(variant, size)
    if workload == "groupby":
        return re.sub(r"(?<![\w.])x\b", prefix + "x_groupby", sql)
    out = sql
    for table in sorted(JOIN_TABLES, key=len, reverse=True):
        pattern = re.compile(r"(?<![\w.])(" + re.escape(table) + r")\b")
        out = pattern.sub(prefix + table, out)
    return out


def _run_one(sql: str, timeout_s: float) -> tuple[str, float, int, str]:
    gc.collect()
    t0 = time.monotonic_ns()
    deadline = time.monotonic() + timeout_s
    rows = 0
    session = None
    try:
        session = opteryx.session()
        for morsel in session.execute_to_morsels(sql):
            if morsel is not None and hasattr(morsel, "num_rows"):
                rows += morsel.num_rows
            if time.monotonic() > deadline:
                elapsed_ms = (time.monotonic_ns() - t0) / 1e6
                return ("timeout", elapsed_ms, rows, f"exceeded {timeout_s:.0f}s")
        return ("ok", (time.monotonic_ns() - t0) / 1e6, rows, "")
    except Exception as e:
        msg = f"{type(e).__name__}: {e}"
        return ("error", (time.monotonic_ns() - t0) / 1e6, rows, msg[:500])
    finally:
        if session is not None:
            try:
                session.close()
            except Exception:
                pass


def _collect(workload: str, queries_dir: Path) -> list[Path]:
    prefix = "g" if workload == "groupby" else "j"
    files = [p for p in queries_dir.glob(f"{prefix}*.sql") if QUERY_RE.match(p.name)]
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
        # `small` is gone. At 1e7 rows it is 630MB, which sits entirely in page
        # cache on any development machine, so it measured compute with the
        # storage layer removed — and the skene mirror is built at medium only.
        choices=["medium", "large"],
        default="medium",
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
        "--variant",
        type=str,
        default="skene",
        choices=("skene", "parquet"),
        help="dataset format: runs against testdata/h2o_skene or testdata/h2o/<size> "
             "(default: skene)",
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
        "--iterations",
        type=int,
        default=2,
        help="iterations per query (default: 2 — cold + warm, H2O convention)",
    )
    args = parser.parse_args()

    workloads = ["groupby", "join"] if args.workload == "both" else [args.workload]

    warm_table = (
        f"testdata.h2o.{args.size}.x_groupby"
        if "groupby" in workloads
        else f"testdata.h2o.{args.size}.x"
    )
    print("Warming up (cold start)...")
    start = time.monotonic()
    warm_session = None
    try:
        warm_session = opteryx.session()
        for _ in warm_session.execute_to_morsels(f"SELECT COUNT(*) FROM {warm_table};"):
            pass
        cold_time_ms = (time.monotonic() - start) * 1000.0
        print(f"Cold start: {cold_time_ms:.2f}ms\n")
    except Exception as e:
        print(f"Cold start failed: {e}\n")
    finally:
        if warm_session is not None:
            warm_session.close()

    duckdb_min, duckdb_machine = load_duckdb_baseline(
        str(HERE / "duckdb" / f"results.{args.size}.json")
    )

    print_banner(
        title="H2O db-benchmark",
        opteryx_version=opteryx.__version__,
        metadata=[
            ("Size", args.size),
            ("Workloads", ", ".join(workloads)),
            ("Iterations", f"{args.iterations} per query (1=cold, 2=warm)"),
            ("Timeout", f"{args.timeout:.0f}s/query"),
        ],
        duckdb_machine=duckdb_machine if duckdb_min else None,
        duckdb_query_count=len(duckdb_min) if duckdb_min else None,
    )

    print_header("Workload/Query", args.iterations, has_baseline=bool(duckdb_min))

    csv_writer, csv_path, csv_handle = open_results_csv(
        str(args.results_dir),
        fieldnames=[
            "workload",
            "size",
            "query",
            "run",
            "status",
            "elapsed_ms",
            "row_count",
            "duckdb_min_ms",
            "error",
        ],
    )

    counts = {"ok": 0, "timeout": 0, "error": 0}
    suite_start = time.monotonic()
    opteryx_total_min = 0.0
    duckdb_total_min = 0.0
    compared = 0

    try:
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
                label = f"{workload[:3]}/{stem}"  # e.g. gro/g1, joi/j1
                d_ms = duckdb_min.get(label) if duckdb_min else None
                if d_ms is None and duckdb_min:
                    # Also accept bare query name in the baseline.
                    d_ms = duckdb_min.get(stem)

                raw_sql = path.read_text()
                sql = _rewrite_query(raw_sql, args.size, workload, args.variant)

                run_times: list[float] = []
                had_failure = False
                for run_ix in range(1, args.iterations + 1):
                    status, elapsed_ms, rows, msg = _run_one(sql, args.timeout)
                    counts[status] += 1
                    csv_writer.writerow(
                        {
                            "workload": workload,
                            "size": args.size,
                            "query": stem,
                            "run": run_ix,
                            "status": status,
                            "elapsed_ms": f"{elapsed_ms:.3f}",
                            "row_count": rows,
                            "duckdb_min_ms": f"{d_ms:.3f}" if d_ms is not None else "",
                            "error": msg,
                        }
                    )
                    csv_handle.flush()
                    if status == "ok":
                        run_times.append(elapsed_ms)
                    else:
                        if status == "timeout":
                            print_skip_row(label, f"timeout: {msg}")
                        else:
                            print_error_row(label, msg)
                        had_failure = True
                        break
                if had_failure or not run_times:
                    continue
                print_row(label, run_times, args.iterations, d_ms)
                if d_ms is not None:
                    opteryx_total_min += min(run_times)
                    duckdb_total_min += d_ms
                    compared += 1
    finally:
        csv_handle.close()

    suite_elapsed = time.monotonic() - suite_start

    print("─" * 100)
    if compared:
        print_total_row(opteryx_total_min, duckdb_total_min, compared, args.iterations)
    print()

    elapsed_s = suite_elapsed
    print(
        f"\033[38;2;26;185;67m{counts['ok']} passed\033[0m, "
        f"\033[38;2;255;121;198m{counts['error']} failed\033[0m, "
        f"\033[38;2;255;165;0m{counts['timeout']} timeout\033[0m   "
        f"({elapsed_s:.1f}s)"
    )
    print(f"  results: {os.path.relpath(csv_path, _REPO_ROOT)}")
    return 1 if counts["error"] else 0


if __name__ == "__main__":
    sys.exit(main())
