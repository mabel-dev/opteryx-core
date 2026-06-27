"""
Medicare1 benchmark runner for Opteryx.

Walks all 10 .sql query files under tests/performance/medicare1/queries/ in
numerical order (1, 2, ..., 10). For each query:

  - rewrites Medicare1 table names to `testdata.medicare1.<table>` and
    strips double-quotes from both table and column references,
  - opens a fresh Opteryx session,
  - executes via session.execute_to_morsels() and drains the iterator,
  - times wall-clock,
  - enforces a per-query timeout (default 300s) — recorded as a timeout,
    not a crash.

Writes per-query results to:
    tests/performance/medicare1/results/<git-sha>-<timestamp>.csv

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
PERF_DIR = HERE.parent  # tests/performance
sys.path.insert(1, str(HERE.parents[2]))
sys.path.insert(1, str(PERF_DIR))

import opteryx  # noqa: E402

# Import _common from parent directory
import importlib.util
common_path = PERF_DIR / "_common.py"
spec = importlib.util.spec_from_file_location("_common", common_path)
_common = importlib.util.module_from_spec(spec)
spec.loader.exec_module(_common)
load_duckdb_baseline = _common.load_duckdb_baseline
print_header = _common.print_header
print_row = _common.print_row
print_error_row = _common.print_error_row
print_total_row = _common.print_total_row

TABLES = ["Medicare1_1", "Medicare1_2"]
DATASET_PREFIX = "testdata.medicare1."

QUERY_RE = re.compile(r"^(\d+)\.sql$")


def query_sort_key(path: Path):
    m = QUERY_RE.match(path.name)
    if not m:
        return 10**9
    return int(m.group(1))


def rewrite_query(sql: str) -> str:
    """Rewrite Medicare1 table names and fix Tableau-generated identifiers.

    Medicare1 queries use Tableau-generated SQL with:
    - Fully-qualified column refs like `"Medicare1_1"."COLUMN"` with double quotes everywhere
    - Aliases with colons like `"avg:Calculation_X:ok"` (invalid in standard SQL)

    Opteryx's parser doesn't support quoted identifiers after dots (e.g., `table."column"`),
    so we:
    1. Replace Tableau aliases with colons -> remove colons (replace with underscores)
    2. Remove ALL double quotes from the SQL
    3. Rewrite table names and FROM clauses to add dataset prefixes
    """
    # Step 1: Handle problematic Tableau aliases inside double quotes by replacing
    # the offending characters with underscores:
    #   "avg:Calc:ok"   -> avg_Calc_ok      (colons)
    #   "$__alias__0"    -> ___alias__0      (dollar — DuckDB reads a bare $name as a
    #                                         bind parameter; sanitising keeps both
    #                                         engines parsing the SAME identifier)
    # Done before quote-stripping so the replacement is identical wherever the alias
    # is defined or referenced, keeping Opteryx and DuckDB queries comparable.
    def fix_problem_identifiers(match):
        return match.group(1).replace(':', '_').replace('$', '_')

    sql = re.sub(r'"([^"]*[:$][^"]*)"', fix_problem_identifiers, sql)

    # Step 2: Remove ALL remaining double quotes (Opteryx parser limitation)
    sql = sql.replace('"', '')

    # Step 3: Rewrite table names in FROM clauses to add dataset prefix with alias
    for table in TABLES:
        # Match FROM or JOIN followed by whitespace and the table name
        pattern = rf'\b(FROM|JOIN)\s+{re.escape(table)}\b'
        replacement = rf'\1 {DATASET_PREFIX}{table} AS {table}'
        sql = re.sub(pattern, replacement, sql, flags=re.IGNORECASE)

    return sql


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
    in practice covers most long-runners.
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
    parser = argparse.ArgumentParser(description="Medicare1 runner for Opteryx")
    parser.add_argument(
        "--queries-dir",
        type=Path,
        default=HERE / "queries",
        help="directory containing 10 .sql files",
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
        help="run only queries whose stem matches this regex (e.g. '^[1-5]$')",
    )
    parser.add_argument(
        "--iterations",
        type=int,
        default=1,
        help="number of times to run each query (default: 1)",
    )
    args = parser.parse_args()

    queries = sorted(args.queries_dir.glob("*.sql"), key=query_sort_key)
    queries = [q for q in queries if QUERY_RE.match(q.name)]
    if not queries:
        sys.exit(
            f"No Medicare1 query files in {args.queries_dir}. "
            f"Run: python tests/performance/medicare1/fetch_data.py --queries"
        )

    if args.filter:
        flt = re.compile(args.filter)
        queries = [q for q in queries if flt.match(q.stem)]
        if not queries:
            sys.exit(f"--filter {args.filter!r} matched zero queries")

    args.results_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%dT%H%M%S")
    results_path = args.results_dir / f"{git_sha()}-{ts}.csv"

    # Load DuckDB baseline for comparison
    duckdb_baseline_path = HERE / "duckdb" / "results.json"
    duckdb_results, duckdb_machine = load_duckdb_baseline(str(duckdb_baseline_path))
    has_baseline = bool(duckdb_results)

    print("=" * 100)
    print(f"MEDICARE1 BENCHMARK — {len(queries)} queries × {args.iterations} iteration(s)")
    print(f"Opteryx: {opteryx.__version__}")
    print(f"Dataset: {DATASET_PREFIX[:-1]}")
    print(f"Timeout: {args.timeout:.0f}s/query")
    if has_baseline:
        print(f"DuckDB baseline: {duckdb_machine or 'unknown machine'}")
    print(f"Output:  {results_path}")
    print("=" * 100)

    # Print header with appropriate columns
    rule_width = print_header("Query", args.iterations, has_baseline)

    suite_start = time.monotonic()
    ok_times: list[float] = []
    counts = {"ok": 0, "timeout": 0, "error": 0}
    duckdb_total_ms = 0.0
    opteryx_total_ms = 0.0
    n_compared = 0

    with open(results_path, "w", newline="") as fh:
        writer = csv.writer(fh)
        writer.writerow(["query", "status", "elapsed_ms", "row_count", "error_msg"])

        for path in queries:
            stem = path.stem
            sql = rewrite_query(path.read_text())

            iteration_times = []
            status_final = None
            rows_final = 0
            msg_final = ""

            for iteration in range(args.iterations):
                status, elapsed_ms, rows, msg = run_one(sql, args.timeout)
                counts[status] += 1
                status_final = status
                rows_final = rows
                msg_final = msg

                if status == "ok":
                    ok_times.append(elapsed_ms)
                    iteration_times.append(elapsed_ms)

                query_label = f"{stem}" if args.iterations == 1 else f"{stem}.{iteration+1}"
                writer.writerow([query_label, status, f"{elapsed_ms:.3f}", rows, msg])
                fh.flush()

            # Print the row using _common helpers
            if status_final == "ok":
                duckdb_ms = duckdb_results.get(stem)
                if duckdb_ms is not None:
                    duckdb_total_ms += duckdb_ms
                    n_compared += 1
                if iteration_times:
                    opteryx_total_ms += min(iteration_times)
                print_row(stem, iteration_times, args.iterations, duckdb_ms)
            elif status_final == "error":
                print_error_row(stem, msg_final)
            else:
                print_error_row(stem, f"{status_final}: {msg_final[:80]}")

    print("─" * rule_width)
    if duckdb_total_ms > 0 and n_compared > 0:
        print_total_row(opteryx_total_ms, duckdb_total_ms, n_compared, args.iterations)
    else:
        suite_elapsed = time.monotonic() - suite_start
        print(
            f"{'TOTAL':<10} ok={counts['ok']}  timeout={counts['timeout']}  "
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

    print(f"\nresults: {results_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
