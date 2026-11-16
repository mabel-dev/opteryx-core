"""
TPC-H on Snowflake (comparison runner)
======================================
Runs the 22 TPC-H queries against Snowflake's shared sample data, like-for-like with
the opteryx.app SQLAlchemy runner (tests/performance/clickbench/sqlalchemy/runner.py):
3 tries per query, ENGINE time only, the same summary table and SUM OF MINIMUMS.

Data: <database>.TPCH_SF<N> - SNOWFLAKE_SAMPLE_DATA for SF1..SF1000 (the share has
nothing smaller), OPTERYX_TPCH for SF0.01 / SF0.1 (loaded by load.py here). The
schema must exist - the runner lists what the database has and refuses one it lacks.

Query text: tests/performance/tpch/opteryx/queries/query*.sql - the bodies the
opteryx.app views benchmarks.tpch_sf<N>.q_NN were generated from (dev/tpch/web_views.py):
block comments stripped, testdata.tpch. -> <database>.TPCH_SF<N>.

Engine time: Snowflake's own EXECUTION_TIME for the statement, read back from
INFORMATION_SCHEMA.QUERY_HISTORY_BY_SESSION by query id. Queueing (QUEUED_*) and
COMPILATION_TIME are outside it; compilation is kept per try in the output as
"compilation" since Opteryx's engine time includes its planning. The result cache
is off (USE_CACHED_RESULT = FALSE); the warehouse's local disk cache is not.

Credentials never touch the repo or the result file: the connection is the
connector's own named connection "opteryx_tpch" in ~/.snowflake/connections.toml
(account, user, key-pair auth with the private key in ~/.config/opteryx-secrets/).

Output: results.local.sf<N>.json here (ClickBench result format, plus the warehouse
size; a failed try is null and the run exits non-zero).

Usage:
    python tests/performance/tpch/snowflake/runner.py --scale 10 [--warehouse COMPUTE_WH]
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

import snowflake.connector
from snowflake.connector.errors import Error as SnowflakeError

HERE = os.path.dirname(__file__)
CONNECTION = "opteryx_tpch"

# Summary/table output is shared with the OData runner so the reports line up.
_spec = importlib.util.spec_from_file_location(
    "odata_runner", os.path.join(HERE, "..", "..", "clickbench", "odata", "runner.py")
)
odata_runner = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(odata_runner)

QUERY_DIR = os.path.join(HERE, "..", "opteryx", "queries")
TRIES = odata_runner.TRIES
TIMEOUT = odata_runner.TIMEOUT
# QUERY_HISTORY_BY_SESSION can trail the statement by a moment; wait this long for its row.
HISTORY_WAIT_S = 30


def load_queries(database: str, schema: str) -> list:
    """(number, sql) for the 22 bodies, rewritten exactly as dev/tpch/web_views.py does."""
    paths = sorted(glob.glob(os.path.join(QUERY_DIR, "query*.sql")))
    assert len(paths) == 22, f"expected 22 TPC-H queries in {QUERY_DIR}, found {len(paths)}"
    queries = []
    for path in paths:
        body = re.sub(r"/\*.*?\*/", "", open(path).read(), flags=re.DOTALL).strip().rstrip(";").strip()
        assert ";" not in body and "--" not in body, f"{path}: unexpected statement separator or line comment"
        queries.append((int(os.path.basename(path)[5:7]), body.replace("testdata.tpch.", f"{database}.{schema}.")))
    return queries


def history(cur, query_id: str) -> tuple:
    """(execution_ms, compilation_ms, queued_ms, warehouse_size) for a finished statement."""
    deadline = time.monotonic() + HISTORY_WAIT_S
    while True:
        cur.execute(
            "SELECT execution_status, execution_time, compilation_time,"
            " queued_provisioning_time + queued_repair_time + queued_overload_time, warehouse_size"
            " FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY_BY_SESSION(RESULT_LIMIT => 1000))"
            " WHERE query_id = %s",
            (query_id,),
        )
        row = cur.fetchone()
        if row is not None and row[0] == "SUCCESS":
            return row[1], row[2], row[3], row[4]
        if row is not None and row[0] != "RUNNING":
            raise RuntimeError(f"query {query_id} history status {row[0]}")
        if time.monotonic() >= deadline:
            raise RuntimeError(f"query {query_id} not in QUERY_HISTORY_BY_SESSION after {HISTORY_WAIT_S}s")
        time.sleep(0.5)


def run_benchmark(conn, queries: list) -> tuple[list, list, list, set]:
    """Returns (per-query execution seconds, None for a failed try; compilation seconds; failures; sizes seen)."""
    results = []
    compilation = []
    failures = []
    sizes = set()
    cur = conn.cursor()
    for idx, sql in queries:
        query_num = f"Q{idx:02d}"
        times = []
        compiles = []
        errors = []
        print(f"  {query_num} {TRIES} runs (", end="", flush=True)
        for attempt in range(TRIES):
            try:
                cur.execute(sql)
                cur.fetchall()
                exec_ms, compile_ms, _queued_ms, size = history(cur, cur.sfqid)
                elapsed = round(exec_ms / 1000, 3)
                compiles.append(round(compile_ms / 1000, 3))
                sizes.add(size)
            except (SnowflakeError, RuntimeError) as exc:
                errors.append(f"{query_num} try {attempt + 1}: {type(exc).__name__}: {exc}")
                elapsed = None
                compiles.append(None)
            times.append(elapsed)
            status = f"{elapsed:5.2f}" if elapsed is not None else "  ERR"
            print(f"{', ' if attempt else ''}{status}", end="", flush=True)
        results.append(times)
        compilation.append(compiles)
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
    return results, compilation, failures, sizes


def main() -> int:
    parser = argparse.ArgumentParser(description="TPC-H against Snowflake sample data (engine time)")
    parser.add_argument("--scale", required=True, help="TPC-H scale: <database>.TPCH_SF<scale>")
    parser.add_argument("--database", default="SNOWFLAKE_SAMPLE_DATA", help="database holding TPCH_SF<scale>")
    parser.add_argument("--warehouse", default="COMPUTE_WH", help="warehouse to run on")
    parser.add_argument("--output", help="Output JSON path (default: results.local.sf<scale>.json here)")
    args = parser.parse_args()
    schema = f"TPCH_SF{args.scale}"
    output = args.output or os.path.join(HERE, f"results.local.sf{args.scale}.json")

    conn = snowflake.connector.connect(
        connection_name=CONNECTION,
        warehouse=args.warehouse,
        database=args.database,
        session_parameters={"USE_CACHED_RESULT": False, "STATEMENT_TIMEOUT_IN_SECONDS": TIMEOUT},
    )
    cur = conn.cursor()

    cur.execute(f"SHOW SCHEMAS LIKE 'TPCH_SF%' IN DATABASE {args.database}")
    available = sorted(row[1] for row in cur.fetchall())
    if schema not in available:
        raise SystemExit(f"{args.database} has no {schema}; it has: {', '.join(available)}")

    cur.execute(f"SHOW WAREHOUSES LIKE '{args.warehouse}'")
    columns = [d[0] for d in cur.description]
    rows = cur.fetchall()
    if len(rows) != 1:
        raise SystemExit(f"warehouse {args.warehouse!r} not found (SHOW WAREHOUSES returned {len(rows)} rows)")
    warehouse = dict(zip(columns, rows[0]))
    size = warehouse["size"]
    clusters = f"{warehouse['min_cluster_count']}-{warehouse['max_cluster_count']}"

    queries = load_queries(args.database, schema)
    numbers = [number for number, _ in queries]

    print(f"TPC-H {schema} — Snowflake, ENGINE time (EXECUTION_TIME)")
    print(f"  Data           : {args.database}.{schema}")
    print(f"  Warehouse      : {args.warehouse} ({size}, clusters {clusters})")
    print("  Result cache   : off")
    print(f"  Tries per query: {TRIES}")
    print(f"  Output         : {output}")
    print()

    start = time.monotonic()
    results, compilation, failures, sizes = run_benchmark(conn, queries)
    wall_seconds = time.monotonic() - start
    conn.close()

    # The size is checked against what every statement actually ran on, not just the config read up front.
    if sizes - {size}:
        failures.append(f"warehouse size moved during the run: configured {size}, statements ran on {sorted(sizes)}")

    payload = {
        "system": "Snowflake (engine time)",
        "date": datetime.date.today().isoformat(),
        "machine": f"Snowflake {size} warehouse",
        "warehouse_size": size,
        "cluster_size": clusters,
        "proprietary": "yes",
        "hardware": "cpu",
        "tuned": "no",
        "tags": ["managed", "column-oriented", "no-cold"],
        "load_time": 0,
        "data_size": 0,
        "result": results,
        "compilation": compilation,
    }
    with open(output, "w") as f:
        json.dump(payload, f, indent=2)
    print(f"\nresults written to {output}")
    print(f"\nWarehouse size: {size}")

    odata_runner.print_summary(results, failures, wall_seconds, numbers)

    ok = [(t, c) for t, c in zip(results, compilation) if None not in t]
    with_compile = sum(min(e + p for e, p in zip(t, c)) for t, c in ok)
    print(f"  incl. compilation: {with_compile:.2f}s over {len(ok)}/{len(numbers)} queries")

    if failures:
        print(f"ERROR: {len(failures)} of {len(numbers) * TRIES} tries failed", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
