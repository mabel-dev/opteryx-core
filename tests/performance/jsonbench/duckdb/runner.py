"""
DuckDB Local JSONBench Benchmark
==================================
Runs the 5 upstream JSONBench queries (github.com/ClickHouse/JSONBench,
`duckdb/queries.sql`) against DuckDB reading the same decompressed Bluesky
NDJSON shards used by the rugo benchmark, loaded into a single `j JSON`
column table (matching the upstream `duckdb/ddl.sql`).

Output: duckdb/results.local.json

Usage:
    python tests/performance/jsonbench/duckdb/runner.py [--size 10] [--jsonl-dir ...]

The result file is written to the same directory as this script so that
../runner.py can reference it as the local baseline.
"""

from __future__ import annotations

import argparse
import glob
import json
import os
import platform
import socket
import time

import duckdb

_HERE = os.path.dirname(os.path.abspath(__file__))
_REPO_ROOT = os.path.abspath(os.path.join(_HERE, "..", "..", "..", ".."))
_DEFAULT_JSONL_DIR = os.path.join(_REPO_ROOT, "testdata", "_downloads", "jsonbench", "decompressed")

# Upstream duckdb/queries.sql, verbatim (operates on a single `j JSON` column table).
QUERIES = [
    "SELECT j->>'$.commit.collection' AS event,count() AS count FROM bluesky GROUP BY event ORDER BY count DESC;",
    "SELECT j->>'$.commit.collection' AS event,count() AS count,count(DISTINCT j->>'$.did') AS users FROM bluesky WHERE (j->>'$.kind' = 'commit') AND (j->>'$.commit.operation' = 'create') GROUP BY event ORDER BY count DESC;",
    "SELECT j->>'$.commit.collection' AS event,hour(TO_TIMESTAMP(CAST(j->>'$.time_us' AS BIGINT) / 1000000)) as hour_of_day,count() AS count FROM bluesky WHERE (j->>'$.kind' = 'commit') AND (j->>'$.commit.operation' = 'create') AND (j->>'$.commit.collection' in ['app.bsky.feed.post', 'app.bsky.feed.repost', 'app.bsky.feed.like']) GROUP BY event, hour_of_day ORDER BY hour_of_day, event;",
    "SELECT j->>'$.did'::String as user_id,TO_TIMESTAMP(CAST(MIN(j->>'$.time_us') AS BIGINT) / 1000000) AS first_post_date FROM bluesky WHERE (j->>'$.kind' = 'commit') AND (j->>'$.commit.operation' = 'create')   AND (j->>'$.commit.collection' = 'app.bsky.feed.post') GROUP BY user_id ORDER BY first_post_date ASC LIMIT 3;",
    "SELECT j->>'$.did'::String as user_id,date_diff('milliseconds', TO_TIMESTAMP(CAST(MIN(j->>'$.time_us') AS BIGINT) / 1000000),TO_TIMESTAMP(CAST(MAX(j->>'$.time_us') AS BIGINT) / 1000000)) AS activity_span FROM bluesky WHERE (j->>'$.kind' = 'commit') AND (j->>'$.commit.operation' = 'create') AND (j->>'$.commit.collection' = 'app.bsky.feed.post') GROUP BY user_id ORDER BY activity_span DESC LIMIT 3;",
]

TRIES = 3


def machine_description() -> str:
    node = socket.gethostname()
    cpu = platform.processor() or platform.machine()
    return f"{node} ({cpu})"


def load_table(con: duckdb.DuckDBPyConnection, jsonl_paths: list[str]) -> tuple[float, int]:
    """Load all shards into `bluesky (j JSON)`, matching upstream ddl.sql.

    Uses `ignore_errors=true`, unlike upstream's `load_data.sh` (which uses
    `ignore_errors=false` per 100k-line chunk with no exit-code check — a
    chunk containing a malformed line is silently dropped in its entirety,
    losing up to 100k rows per bad line with no record of it happening). The
    Bluesky dataset does contain malformed lines (observed: a raw unescaped
    newline embedded in a string field splits one JSON record across two
    physical lines) — `ignore_errors=true` recovers every well-formed row
    instead of losing a whole chunk for one bad one. See README.md.

    Returns (load_seconds, rows_loaded).
    """
    con.execute("CREATE TABLE bluesky (j JSON)")
    t0 = time.monotonic()
    for path in jsonl_paths:
        con.execute(
            "INSERT INTO bluesky SELECT * FROM read_ndjson_objects(?, ignore_errors=true, maximum_object_size=1048576000)",
            [path],
        )
    load_s = time.monotonic() - t0
    rows = con.execute("SELECT count(*) FROM bluesky").fetchone()[0]
    return load_s, rows


def run_benchmark(con: duckdb.DuckDBPyConnection) -> list:
    results = []
    total = len(QUERIES)
    for idx, query in enumerate(QUERIES, start=1):
        times = []
        print(f"  Q{idx}/{total} ", end="", flush=True)
        for attempt in range(TRIES):
            t0 = time.monotonic()
            try:
                con.execute(query).fetchall()
                elapsed = round(time.monotonic() - t0, 3)
            except Exception as exc:
                print(f"\n    ERROR on attempt {attempt + 1}: {exc}")
                elapsed = None
            times.append(elapsed)
            status = f"{elapsed:.3f}s" if elapsed is not None else "ERR"
            print(status, end="  " if attempt < TRIES - 1 else "\n", flush=True)
        results.append(times)
    return results


def main() -> int:
    parser = argparse.ArgumentParser(description="DuckDB local JSONBench benchmark")
    parser.add_argument("--size", type=int, default=10, choices=(1, 10, 100), help="Dataset size in millions of rows (default: 10)")
    parser.add_argument("--jsonl-dir", default=_DEFAULT_JSONL_DIR, help="Directory of decompressed file_NNNN.jsonl shards")
    parser.add_argument("--output", default=None, help="Output JSON path (default: results.local.json next to this script)")
    args = parser.parse_args()

    all_shards = sorted(glob.glob(os.path.join(args.jsonl_dir, "file_*.jsonl")))
    shards = all_shards[: args.size]
    if len(shards) < args.size:
        print(
            f"ERROR: need {args.size} decompressed shard(s) in {args.jsonl_dir}, found {len(all_shards)}.\n"
            f"Run: python tests/performance/jsonbench/fetch_data.py --size {args.size}"
        )
        return 1

    data_size = sum(os.path.getsize(p) for p in shards)
    output_path = args.output or os.path.join(_HERE, f"results.local.{args.size}m.json")

    print("DuckDB JSONBench — local benchmark")
    print(f"  DuckDB version : {duckdb.__version__}")
    print(f"  Machine        : {machine_description()}")
    print(f"  Shards         : {len(shards)} x 1,000,000 rows ({data_size / 1e9:.2f}GB decompressed NDJSON)")
    print(f"  Tries per query: {TRIES}")
    print(f"  Output         : {output_path}")
    print()

    con = duckdb.connect()
    load_s, rows_loaded = load_table(con, shards)
    expected_rows = args.size * 1_000_000
    print(f"Load time: {load_s:.1f}s  ({rows_loaded:,} rows loaded of {expected_rows:,} expected)")
    if rows_loaded < expected_rows:
        print(f"  NOTE: {expected_rows - rows_loaded} malformed row(s) skipped (ignore_errors=true) — see README.md")

    start = time.monotonic()
    results = run_benchmark(con)
    total_elapsed = time.monotonic() - start
    con.close()

    print(f"\nTotal query time: {total_elapsed:.1f}s")

    payload = {
        "system": "DuckDB (NDJSON, single JSON column)",
        "date": __import__("datetime").date.today().isoformat(),
        "machine": machine_description(),
        "size_millions": args.size,
        "load_time_s": round(load_s, 3),
        "rows_loaded": rows_loaded,
        "rows_expected": expected_rows,
        "data_size": data_size,
        "result": results,
    }
    with open(output_path, "w") as f:
        json.dump(payload, f, indent=2)
    print(f"Results written to {output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
