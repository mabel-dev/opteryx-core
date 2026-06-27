"""
Join Order Benchmark (JOB) runner for Opteryx.

Walks all 113 .sql query files under tests/performance/job/queries/ in the
canonical order (1a, 1b, 1c, 2a, ... 33c). For each query:

  - rewrites bare IMDB table names to `testdata.job.<table>` so Opteryx can
    resolve them through the existing dataset registry (rewrite is scoped to
    the FROM clause to avoid clobbering identical column / alias names),
  - opens a fresh Opteryx session,
  - executes via session.execute_to_morsels() and drains the iterator,
  - times wall-clock,
  - enforces a per-query timeout (default 300s) — recorded as a timeout, not
    a crash.

Writes per-query results to:
    tests/performance/job/results/<git-sha>-<timestamp>.csv

Columns: query, status (ok|timeout|error), elapsed_ms, row_count, error_msg.

Prints a summary at the end (counts, total wall, median, p95).
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
# bare column references in WHERE/SELECT. JOB's queries always use
# comma-joined table lists, never explicit JOIN.
_FROM_TABLE_RE = re.compile(
    r"(?P<lead>(?:\bFROM\b|,)\s*)(?P<tbl>" + _TABLE_ALT + r")\b",
    re.IGNORECASE,
)


def query_sort_key(path: Path):
    m = QUERY_RE.match(path.name)
    if not m:
        return (10**9, "z")
    return (int(m.group(1)), m.group(2))


def rewrite_query(sql: str) -> str:
    """Prefix bare IMDB table names with `testdata.job.` in the FROM clause.

    Aliases (`title AS t`) survive because they follow whitespace, not a
    comma; column refs (`mi.info`) survive because they don't follow `FROM`
    or a comma; column aliases (`AS movie_keyword`) survive for the same
    reason.
    """
    return _FROM_TABLE_RE.sub(
        lambda m: m.group("lead") + DATASET_PREFIX + m.group("tbl"),
        sql,
    )


def git_sha() -> str:
    try:
        sha = subprocess.check_output(
            ["git", "rev-parse", "--short", "HEAD"],
            cwd=str(HERE),
            stderr=subprocess.DEVNULL,
        ).decode().strip()
        return sha or "nogit"
    except Exception:
        return "nogit"


def run_one(sql: str, timeout_s: float) -> tuple[str, float, int, str]:
    """Execute one query with a wall-clock deadline checked between morsels.

    Returns (status, elapsed_ms, row_count, error_msg).
    A query that blocks inside a single C/Cython call cannot be interrupted
    by this loop — the timeout still bounds Python-side iteration, which
    in practice covers most JOB long-runners.
    """
    start = time.monotonic()
    deadline = start + timeout_s
    rows = 0
    session = None
    try:
        session = opteryx.session()
        for morsel in session.execute_to_morsels(sql):
            try:
                rows += morsel.num_rows
            except AttributeError:
                try:
                    rows += len(morsel)
                except Exception:
                    pass
            if time.monotonic() > deadline:
                return (
                    "timeout",
                    (time.monotonic() - start) * 1000.0,
                    rows,
                    f"exceeded {timeout_s:.0f}s",
                )
        return ("ok", (time.monotonic() - start) * 1000.0, rows, "")
    except Exception as e:
        msg = f"{type(e).__name__}: {e}"
        return ("error", (time.monotonic() - start) * 1000.0, rows, msg[:500])
    finally:
        if session is not None:
            try:
                session.close()
            except Exception:
                pass


def main() -> int:
    parser = argparse.ArgumentParser(description="JOB runner for Opteryx")
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
    args = parser.parse_args()

    queries = sorted(args.queries_dir.glob("*.sql"), key=query_sort_key)
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

    args.results_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%dT%H%M%S")
    results_path = args.results_dir / f"{git_sha()}-{ts}.csv"

    print("=" * 80)
    print(f"JOB BENCHMARK — {len(queries)} queries")
    print(f"Opteryx: {opteryx.__version__}")
    print(f"Dataset: {DATASET_PREFIX[:-1]}")
    print(f"Timeout: {args.timeout:.0f}s/query")
    print(f"Output:  {results_path}")
    print("=" * 80)
    print(f"{'Query':<8} {'Status':<8} {'Elapsed':>12} {'Rows':>12}  Note")
    print("-" * 80)

    suite_start = time.monotonic()
    ok_times: list[float] = []
    counts = {"ok": 0, "timeout": 0, "error": 0}

    with open(results_path, "w", newline="") as fh:
        writer = csv.writer(fh)
        writer.writerow(["query", "status", "elapsed_ms", "row_count", "error_msg"])

        for path in queries:
            stem = path.stem
            sql = rewrite_query(path.read_text())
            status, elapsed_ms, rows, msg = run_one(sql, args.timeout)
            counts[status] += 1
            if status == "ok":
                ok_times.append(elapsed_ms)

            colour = {
                "ok": "\033[32mok\033[0m",
                "timeout": "\033[33mtimeout\033[0m",
                "error": "\033[31merror\033[0m",
            }[status]
            note = "" if status == "ok" else msg
            print(
                f"{stem:<8} {colour:<17} {elapsed_ms:>10.1f}ms {rows:>12,}  {note[:60]}"
            )
            writer.writerow([stem, status, f"{elapsed_ms:.3f}", rows, msg])
            fh.flush()

    suite_elapsed = time.monotonic() - suite_start
    print("-" * 80)
    print(
        f"{'Total':<8} ok={counts['ok']}  timeout={counts['timeout']}  "
        f"error={counts['error']}  total_wall={suite_elapsed:.1f}s"
    )
    if ok_times:
        med = statistics.median(ok_times)
        p95 = (
            statistics.quantiles(ok_times, n=100)[94]
            if len(ok_times) >= 20
            else max(ok_times)
        )
        print(f"         median(ok)={med:.1f}ms  p95(ok)={p95:.1f}ms")
    print(f"results: {results_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
