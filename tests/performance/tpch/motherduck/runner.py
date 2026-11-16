"""
TPC-H on MotherDuck (engine time)
=================================
Rough comparison point for the opteryx.app SQLAlchemy runner
(tests/performance/clickbench/sqlalchemy/runner.py): the same 22 query bodies, the
same 3 tries per query, the same summary table and SUM OF MINIMUMS.

Queries: tests/performance/tpch/opteryx/queries/query01..22.sql - the bodies the
opteryx.app views benchmarks.tpch_sf<N>.q_NN came from (dev/tpch/web_views.py):
block comments stripped, `testdata.tpch.` -> `tpch_sf<N>.main.`. DuckDB dialect
changes, the minimum needed to parse:
    Q11  `q11_part_tmp_cached join q11_sum_tmp_cached`  -> `cross join` (DuckDB needs ON for JOIN)
    Q22  `q22_customer_tmp1_cached ct1 join (`          -> `cross join (`  (same)

Data: one MotherDuck database per scale, tpch_sf<N>, generated in place with DuckDB's
tpch extension (`--generate`, refuses if the database already exists).

Each try records ENGINE time, not what the client waited: after the rows are fetched,
the query's MD_LAST_QUERY_ID() is looked up in MD_INFORMATION_SCHEMA.RECENT_QUERIES and
its EXECUTION_TIME ("duration where the query is actively executing") is the time -
WAIT_TIME (queued on resources), network and result download are outside it. The
duckling each try ran on (INSTANCE_TYPE) is recorded in the JSON and the summary.

The token never touches the repo or the result file - it is read from Vaultwarden
(dev/secret_store.py):

    MOTHERDUCK_TOKEN   MotherDuck access token

Output: results.local.sf<N>.json here (ClickBench result format; a failed try is null
and the run exits non-zero).

Needs DuckDB 1.5.x (MotherDuck refuses 2.0), so run it through `make tpch-sf<N>-motherduck`,
which provides its own uv env.

Usage:
    make tpch-sf1-motherduck [MD_ARGS=--generate]
"""

import argparse
import datetime
import glob
import importlib.util
import json
import os
import re
import sys
import time

import duckdb

HERE = os.path.dirname(__file__)
REPO = os.path.join(HERE, "..", "..", "..", "..")
sys.path.insert(1, os.path.join(REPO, "dev"))
from secret_store import get_secret  # noqa: E402

# Summary/table output is shared with the OData runner so the reports line up.
_spec = importlib.util.spec_from_file_location(
    "odata_runner", os.path.join(REPO, "tests", "performance", "clickbench", "odata", "runner.py")
)
odata_runner = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(odata_runner)

QUERY_DIR = os.path.join(REPO, "tests", "performance", "tpch", "opteryx", "queries")
TRIES = odata_runner.TRIES
# Lite (free) plan: 10 GB storage; SF10 is ~2.8 GB as a DuckDB database, SF100 does not fit.
SCALES = {"001": 0.01, "01": 0.1, "1": 1, "10": 10}
# DuckDB needs ON for JOIN; these are the only two bodies that rely on Opteryx's bare JOIN.
DIALECT = {
    11: ("q11_part_tmp_cached join q11_sum_tmp_cached", "q11_part_tmp_cached cross join q11_sum_tmp_cached"),
    22: ("q22_customer_tmp1_cached ct1 join (", "q22_customer_tmp1_cached ct1 cross join ("),
}
# RECENT_QUERIES "is updated every couple of seconds".
HISTORY_WAIT_S = 60


def queries(database: str) -> list:
    """(number, sql) for the 22 bodies, rewritten for DuckDB as listed in the module docstring."""
    paths = sorted(glob.glob(os.path.join(QUERY_DIR, "query*.sql")))
    assert len(paths) == 22, f"expected 22 TPC-H queries in {QUERY_DIR}, found {len(paths)}"
    out = []
    for path in paths:
        number = int(os.path.basename(path)[5:7])
        body = re.sub(r"/\*.*?\*/", "", open(path).read(), flags=re.DOTALL).strip().rstrip(";").strip()
        if number in DIALECT:
            old, new = DIALECT[number]
            assert body.count(old) == 1, f"Q{number:02d}: dialect rewrite target not found once"
            body = body.replace(old, new)
        out.append((number, body.replace("testdata.tpch.", f"{database}.main.")))
    return out


def engine_time(conn, query_id) -> tuple[float, str]:
    """(EXECUTION_TIME seconds, INSTANCE_TYPE) for a finished query, from RECENT_QUERIES."""
    deadline = time.monotonic() + HISTORY_WAIT_S
    while time.monotonic() < deadline:
        row = conn.execute(
            "SELECT epoch(execution_time), instance_type, error_message "
            "FROM md_information_schema.recent_queries WHERE query_id = ? AND end_time IS NOT NULL",
            [query_id],
        ).fetchone()
        if row is not None:
            assert row[2] is None, f"query {query_id} finished with error: {row[2]}"
            return row[0], row[1]
        time.sleep(1)
    raise RuntimeError(f"query {query_id} not in recent_queries after {HISTORY_WAIT_S}s")


def run_benchmark(conn, database: str) -> tuple[list, list, set]:
    """(per-query try engine times in seconds, None for a failed try; failure messages; instance types)."""
    results = []
    failures = []
    instances = set()
    for number, sql in queries(database):
        query_num = f"Q{number:02d}"
        times = []
        errors = []
        print(f"  {query_num} {TRIES} runs (", end="", flush=True)
        for attempt in range(TRIES):
            try:
                conn.execute(sql).fetchall()
                query_id = conn.execute("SELECT md_last_query_id()").fetchone()[0]
            except duckdb.Error as exc:
                errors.append(f"{query_num} try {attempt + 1}: {type(exc).__name__}: {exc}")
                elapsed = None
            else:
                seconds, instance = engine_time(conn, query_id)
                instances.add(instance)
                elapsed = round(seconds, 3)
            times.append(elapsed)
            status = f"{elapsed:5.2f}" if elapsed is not None else "  ERR"
            print(f"{', ' if attempt else ''}{status}", end="", flush=True)
        results.append(times)
        if errors:
            # A query with any failed try is a failure, never a fast time.
            failures.extend(errors)
            print(f"\r\033[K  {query_num} FAILED: {errors[0][:90]}")
            continue
        fastest = times.index(min(times))
        slowest = times.index(max(times))
        cells = []
        for try_ix, seconds in enumerate(times):
            cell = f"{seconds:5.2f}"
            if try_ix == fastest:
                cell = f"\033[38;2;34;197;94m{cell}\033[0m"
            elif try_ix == slowest:
                cell = f"\033[38;2;255;69;69m{cell}\033[0m"
            cells.append(cell)
        print(f"\r\033[K  {query_num} {TRIES} runs ({', '.join(cells)})")
    return results, failures, instances


def generate(conn, database: str, sf: float) -> None:
    existing = conn.execute("SELECT database_name FROM duckdb_databases() WHERE database_name = ?", [database]).fetchall()
    assert not existing, f"{database} already exists in MotherDuck - drop it first to regenerate"
    print(f"generating {database} (CALL dbgen(sf={sf})) ...", flush=True)
    t0 = time.monotonic()
    conn.execute(f"CREATE DATABASE {database}")
    conn.execute("INSTALL tpch")
    conn.execute("LOAD tpch")
    conn.execute(f"CALL dbgen(sf={sf}, catalog='{database}')")
    print(f"generated {database} in {time.monotonic() - t0:.1f}s")


def main() -> int:
    parser = argparse.ArgumentParser(description="TPC-H against MotherDuck (engine time)")
    parser.add_argument("--scale", required=True, choices=sorted(SCALES), help="scale suffix: 001 01 1 10")
    parser.add_argument("--generate", action="store_true", help="create tpch_sf<N> in MotherDuck first")
    parser.add_argument("--output", help="Output JSON path (default: results.local.sf<N>.json here)")
    args = parser.parse_args()
    database = f"tpch_sf{args.scale}"
    output = args.output or os.path.join(HERE, f"results.local.sf{args.scale}.json")
    numbers = list(range(1, 23))

    conn = duckdb.connect("md:", config={"motherduck_token": get_secret("MOTHERDUCK_TOKEN")})
    if args.generate:
        generate(conn, database, SCALES[args.scale])
    conn.execute(f"USE {database}")

    print(f"TPC-H {database} — MotherDuck, ENGINE time (recent_queries.execution_time)")
    print(f"  DuckDB client  : {duckdb.__version__}")
    print(f"  Database       : md:{database}")
    print(f"  Tries per query: {TRIES}")
    print(f"  Output         : {output}")
    print()

    start = time.monotonic()
    results, failures, instances = run_benchmark(conn, database)
    wall_seconds = time.monotonic() - start
    conn.close()

    instance = ", ".join(sorted(instances)) or "unknown (every try failed)"
    payload = {
        "system": "MotherDuck (engine time)",
        "date": datetime.date.today().isoformat(),
        "machine": f"MotherDuck {instance}",
        "instance_type": sorted(instances),
        "scale_factor": SCALES[args.scale],
        "cluster_size": 1,
        "proprietary": "yes",
        "hardware": "cpu",
        "tuned": "no",
        "tags": ["C++", "column-oriented", "serverless", "managed", "DuckDB"],
        "load_time": 0,
        "data_size": 0,
        "result": results,
    }
    with open(output, "w") as f:
        json.dump(payload, f, indent=2)
    print(f"\nresults written to {output}")
    print(f"  Instance       : {instance}")

    odata_runner.print_summary(results, failures, wall_seconds, numbers)

    if failures:
        print(f"ERROR: {len(failures)} of {len(numbers) * TRIES} tries failed", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
