#!/usr/bin/env python3
"""
Join Order Benchmark (JOB) runner for Opteryx.

Walks all 113 .sql query files under tests/performance/job/queries/ in canonical
order (1a, 1b, 1c, 2a, ... 33c). For each query:

  - rewrites bare IMDB table names to `testdata.job.<table>` so Opteryx can
    resolve them through the dataset registry (rewrite is scoped to the FROM
    clause to avoid clobbering identical column / alias names),
  - opens a fresh Opteryx session per iteration,
  - executes via session.execute_to_morsels() and drains the iterator,
  - times wall-clock,
  - enforces a per-query timeout (default 300s),
  - compares min time to the DuckDB baseline (if present),
  - writes per-iteration rows to results/<sha>-<ts>.csv.
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

# 21 IMDB tables; longest-first so partial-prefix matches don't fire
# (e.g. `movie_info_idx` must be matched before `movie_info`).
TABLES = [
    "comp_cast_type",
    "company_name",
    "company_type",
    "complete_cast",
    "movie_companies",
    "movie_info_idx",
    "movie_keyword",
    "movie_info",
    "movie_link",
    "person_info",
    "char_name",
    "info_type",
    "kind_type",
    "link_type",
    "role_type",
    "aka_name",
    "aka_title",
    "cast_info",
    "keyword",
    "title",
    "name",
]

DATASET_PREFIX = "testdata.job."
QUERY_RE = re.compile(r"^([0-9]+)([a-z])\.sql$")
_TABLE_ALT = "|".join(re.escape(t) for t in TABLES)
# Rewrite is scoped to the FROM clause: only tokens immediately following
# `FROM` or a comma get prefixed. This avoids clobbering identical names
# used as column aliases (e.g. `MIN(k.keyword) AS movie_keyword`) or as
# bare column references in WHERE/SELECT.
_FROM_TABLE_RE = re.compile(
    r"(?P<lead>(?:\bFROM\b|,)\s*)(?P<tbl>" + _TABLE_ALT + r")\b",
    re.IGNORECASE,
)


def _query_sort_key(path: Path):
    m = QUERY_RE.match(path.name)
    if not m:
        return (10**9, "z")
    return (int(m.group(1)), m.group(2))


def _rewrite_query(sql: str) -> str:
    return _FROM_TABLE_RE.sub(
        lambda m: m.group("lead") + DATASET_PREFIX + m.group("tbl"),
        sql,
    )


def _run_one(sql: str, timeout_s: float) -> tuple[str, float, int, str]:
    """Execute one query with a wall-clock deadline checked between morsels.

    A query that blocks inside a single C/Cython call cannot be interrupted
    by this loop — the timeout still bounds Python-side iteration, which
    in practice covers most JOB long-runners.
    """
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
                return ("timeout", (time.monotonic_ns() - t0) / 1e6, rows, f"exceeded {timeout_s:.0f}s")
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


def main() -> int:
    parser = argparse.ArgumentParser(description="JOB benchmark vs DuckDB")
    parser.add_argument(
        "--queries-dir",
        type=Path,
        default=HERE / "queries",
        help="directory containing 113 .sql files",
    )
    parser.add_argument(
        "--results-dir",
        type=Path,
        default=HERE / "results",
        help="directory for the output CSV",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=300.0,
        help="per-query wall-clock timeout in seconds (default: 300)",
    )
    parser.add_argument(
        "--filter",
        type=str,
        default=None,
        help="run only queries whose stem matches this regex (e.g. '^1[abc]$')",
    )
    parser.add_argument(
        "--iterations",
        type=int,
        default=2,
        help="warm iterations per query (default: 2)",
    )
    args = parser.parse_args()

    queries = sorted(args.queries_dir.glob("*.sql"), key=_query_sort_key)
    queries = [q for q in queries if QUERY_RE.match(q.name)]
    if not queries:
        sys.exit(
            f"No JOB query files in {args.queries_dir}. "
            f"Run: python tests/performance/job/fetch_data.py --queries"
        )
    if args.filter:
        flt = re.compile(args.filter)
        queries = [q for q in queries if flt.match(q.stem)]
        if not queries:
            sys.exit(f"--filter {args.filter!r} matched zero queries")

    print("Warming up (cold start)...")
    start = time.monotonic()
    warm_session = None
    try:
        warm_session = opteryx.session()
        for _ in warm_session.execute_to_morsels(f"SELECT COUNT(*) FROM {DATASET_PREFIX}title;"):
            pass
        cold_time_ms = (time.monotonic() - start) * 1000.0
        print(f"Cold start: {cold_time_ms:.2f}ms\n")
    except Exception as e:
        print(f"Cold start failed: {e}\n")
    finally:
        if warm_session is not None:
            warm_session.close()

    duckdb_min, duckdb_machine = load_duckdb_baseline(
        str(HERE / "duckdb" / "results.json")
    )

    print_banner(
        title="JOB BENCHMARK",
        opteryx_version=opteryx.__version__,
        metadata=[
            ("Dataset", "testdata.job"),
            ("Queries", str(len(queries))),
            ("Iterations", f"{args.iterations} per query"),
            ("Timeout", f"{args.timeout:.0f}s/query"),
        ],
        duckdb_machine=duckdb_machine if duckdb_min else None,
        duckdb_query_count=len(duckdb_min) if duckdb_min else None,
    )

    print_header("Query", args.iterations, has_baseline=bool(duckdb_min))

    csv_writer, csv_path, csv_handle = open_results_csv(
        str(args.results_dir),
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

    counts = {"ok": 0, "timeout": 0, "error": 0}
    suite_start = time.monotonic()
    opteryx_total_min = 0.0
    duckdb_total_min = 0.0
    compared = 0

    try:
        for path in queries:
            stem = path.stem
            sql = _rewrite_query(path.read_text())
            d_ms = duckdb_min.get(stem) if duckdb_min else None
            run_times: list[float] = []
            had_failure = False
            for run_ix in range(1, args.iterations + 1):
                status, elapsed_ms, rows, msg = _run_one(sql, args.timeout)
                counts[status] += 1
                csv_writer.writerow(
                    {
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
                        print_skip_row(stem, f"timeout: {msg}")
                    else:
                        print_error_row(stem, msg)
                    had_failure = True
                    break
            if had_failure or not run_times:
                continue
            print_row(stem, run_times, args.iterations, d_ms)
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

    print(
        f"\033[38;2;26;185;67m{counts['ok']} passed\033[0m, "
        f"\033[38;2;255;121;198m{counts['error']} failed\033[0m, "
        f"\033[38;2;255;165;0m{counts['timeout']} timeout\033[0m   "
        f"({suite_elapsed:.1f}s)"
    )
    print(f"  results: {os.path.relpath(csv_path, _REPO_ROOT)}")
    return 1 if counts["error"] else 0


if __name__ == "__main__":
    sys.exit(main())
