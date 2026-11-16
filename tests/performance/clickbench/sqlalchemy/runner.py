"""
SQLAlchemy Benchmark (opteryx.app)
==================================
Runs a benchmark's queries against opteryx.app through the opteryx-sqlalchemy
dialect, reading the same views as the OData runner (see its --collection):

    SELECT * FROM benchmarks.<collection>.q_NN

Each try records ENGINE time, not what the client waited: after the rows are
fetched, the job's verbose results (GET jobs.opteryx.app/api/v1/jobs/<id>/results
?verbose=true) give `time_taken_s` - the engine's `time_total`, which opens when the
worker starts the statement, so queueing, submit, polling and result download are
outside it. The worker's own result-part writes to GCS fall inside that window, so
`worker_timings.write_results_in_stream` + `write_results_after_stream` are
subtracted.

Credentials never touch the repo or the result file - they are read from
Vaultwarden (dev/secret_store.py):

    CLIENT_ID      opteryx.app client id
    CLIENT_SECRET  opteryx.app client secret

Output: sqlalchemy/results.local.json for clickbench, results.local.<collection>.json
otherwise (ClickBench result format; a failed try is null and the run exits non-zero).

Usage:
    python tests/performance/clickbench/sqlalchemy/runner.py [--collection tpch_sf10]
"""

import argparse
import datetime
import importlib.util
import json
import os
import sys
import time
import urllib.error
import urllib.request

from sqlalchemy import create_engine
from sqlalchemy import text
from sqlalchemy.engine import URL
from sqlalchemy_dialect.dbapi import Error as DialectError

HERE = os.path.dirname(__file__)
sys.path.insert(1, os.path.join(HERE, "..", "..", "..", "..", "dev"))
from secret_store import get_secret  # noqa: E402

# Summary/table output is shared with the OData runner so the two reports line up.
_spec = importlib.util.spec_from_file_location("odata_runner", os.path.join(HERE, "..", "odata", "runner.py"))
odata_runner = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(odata_runner)

HOST = "opteryx.app"
JOBS_URL = "https://jobs.opteryx.app/api/v1/jobs"
TRIES = odata_runner.TRIES
TIMEOUT = odata_runner.TIMEOUT


def engine_seconds(statement_handle: str, token) -> float:
    """The job's engine time: time_total less the worker's result writes inside that window."""
    req = urllib.request.Request(
        f"{JOBS_URL}/{statement_handle}/results?verbose=true&num_rows=100",
        headers={"Accept": "application/json", "Authorization": token.header()},
    )
    with urllib.request.urlopen(req, timeout=60) as resp:
        payload = json.loads(resp.read())
    writes = payload["worker_timings"]
    return payload["time_taken_s"] - writes["write_results_in_stream"] - writes["write_results_after_stream"]


def run_benchmark(conn, token, collection: str) -> tuple[list, list]:
    """Returns (per-query try engine times in seconds, None for a failed try; failure messages)."""
    results = []
    failures = []
    for idx in odata_runner.view_numbers(collection):
        query_num = f"Q{idx:02d}"
        sql = f"SELECT * FROM benchmarks.{collection}.q_{idx:02d}"
        times = []
        errors = []
        print(f"  {query_num} {TRIES} runs (", end="", flush=True)
        for attempt in range(TRIES):
            try:
                # The dialect's DBAPI cursor: its statement_handle names the job to read the time from.
                cursor = conn.connection.cursor()
                cursor.execute(sql)
                cursor.fetchall()
                elapsed = round(engine_seconds(cursor.statement_handle, token), 3)
            except DialectError as exc:
                errors.append(f"{query_num} try {attempt + 1}: {type(exc).__name__}: {exc}")
                elapsed = None
            except (urllib.error.URLError, TimeoutError) as exc:
                errors.append(f"{query_num} try {attempt + 1}: {odata_runner.describe_error(exc)}")
                elapsed = None
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
    return results, failures


def main() -> int:
    parser = argparse.ArgumentParser(description="SQLAlchemy benchmark against opteryx.app (engine time)")
    parser.add_argument("--collection", default="clickbench", help="benchmarks.<collection> to read")
    parser.add_argument("--output", help="Output JSON path (default: results.local[.<collection>].json here)")
    args = parser.parse_args()
    numbers = odata_runner.view_numbers(args.collection)
    output = args.output or odata_runner.default_output(HERE, args.collection)
    token = odata_runner.Token(get_secret("CLIENT_ID"), get_secret("CLIENT_SECRET"))
    # TPC-H compares to MotherDuck and Snowflake the way the local runners compare to DuckDB
    # (make tpch-sf<N>-motherduck / -snowflake); a scale with no saved run has no baseline.
    baselines = []
    baseline_descs = []
    for label, system_dir in (("MotherDuck", "motherduck"), ("Snowflake", "snowflake")):
        baseline_path = os.path.join(
            HERE, "..", "..", "tpch", system_dir, f"results.local.{args.collection.replace('tpch_', '')}.json"
        )
        if args.collection != "clickbench" and os.path.exists(baseline_path):
            with open(baseline_path) as f:
                saved = json.load(f)
            baselines.append((label, saved["result"]))
            baseline_descs.append(f"{saved['system']}, {saved['machine']} ({saved['date']})")
        else:
            baseline_descs.append(f"none (no {os.path.normpath(baseline_path)})")

    url = URL.create(
        "opteryx",
        username=get_secret("CLIENT_ID"),
        password=get_secret("CLIENT_SECRET"),
        host=HOST,
        port=443,
        database="default",
        query={"ssl": "true", "timeout": str(TIMEOUT)},
    )
    engine = create_engine(url)

    with engine.connect() as conn:
        # authenticate up front: a credential failure stops the run before any timing
        conn.execute(text("SELECT 1")).fetchall()

        print(f"Opteryx {args.collection} — SQLAlchemy (opteryx.app), ENGINE time")
        print(f"  Host           : {HOST}")
        print(f"  Views          : benchmarks.{args.collection}.q_NN")
        print(f"  Tries per query: {TRIES}")
        print(f"  Output         : {output}")
        for desc in baseline_descs:
            print(f"  Baseline       : {desc}")
        print()

        start = time.monotonic()
        results, failures = run_benchmark(conn, token, args.collection)
        wall_seconds = time.monotonic() - start

    payload = {
        "system": "Opteryx (opteryx.app, SQLAlchemy, engine time)",
        "date": datetime.date.today().isoformat(),
        "machine": "opteryx.app",
        "cluster_size": "serverless",
        "proprietary": "no",
        "hardware": "cpu",
        "tuned": "no",
        "tags": ["C++", "column-oriented", "serverless", "managed", "no-cold"],
        "load_time": 0,
        "data_size": 0,
        "result": results,
    }
    with open(output, "w") as f:
        json.dump(payload, f, indent=2)
    print(f"\nresults written to {output}")

    odata_runner.print_summary(results, failures, wall_seconds, numbers, baselines)

    if failures:
        print(f"ERROR: {len(failures)} of {len(numbers) * TRIES} tries failed", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
