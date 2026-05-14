"""
DuckDB Local ClickBench Benchmark — single-threaded
====================================================
Identical to runner.py but forces DuckDB to use a single thread. This isolates
the per-core engine work from threading speedup so the result is directly
comparable to Opteryx's serial execution.

Output: duckdb/results.local.single_thread.json

Usage:
    python tests/performance/clickbench/duckdb/runner_single_thread.py [--parquet-dir scratch/hits]
"""

import argparse
import json
import os
import platform
import socket
import sys
import time

import duckdb

# Reuse the canonical query list to keep results comparable to the threaded run.
sys.path.insert(0, os.path.dirname(__file__))
from runner import QUERIES, TRIES, machine_description  # noqa: E402


def run_benchmark(parquet_glob: str) -> list:
    """
    Run all queries TRIES times each, single-threaded.
    Returns a list of [cold, warm1, warm2] timing tuples (seconds, 3 d.p.).
    """
    con = duckdb.connect()
    con.execute("SET threads TO 1")
    con.execute("SET parquet_metadata_cache=true")
    con.execute(f"""
        CREATE VIEW hits AS
        SELECT * REPLACE (make_date(EventDate) AS EventDate)
        FROM read_parquet('{parquet_glob}', binary_as_string=True)
    """)
    con.execute("CREATE MACRO toDateTime(t) AS epoch_ms(CAST(t AS BIGINT) * 1000)")

    # Confirm the setting actually took.
    threads_in_use = con.execute("SELECT current_setting('threads')").fetchone()[0]
    print(f"  DuckDB threads in use: {threads_in_use}")

    results = []
    total = len(QUERIES)

    for idx, query in enumerate(QUERIES, start=1):
        times = []
        print(f"  Q{idx:02d}/{total} ", end="", flush=True)
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

    con.close()
    return results


def main():
    parser = argparse.ArgumentParser(description="DuckDB single-threaded ClickBench")
    parser.add_argument(
        "--parquet-dir",
        default="scratch/hits",
        help="Directory containing hits_*.parquet files (default: scratch/hits)",
    )
    parser.add_argument(
        "--output",
        default=None,
        help="Output JSON path (default: duckdb/results.local.single_thread.json)",
    )
    args = parser.parse_args()

    parquet_dir = args.parquet_dir
    if not os.path.isabs(parquet_dir):
        candidates = [
            parquet_dir,
            os.path.join(os.path.dirname(__file__), "..", "..", "..", parquet_dir),
        ]
        for c in candidates:
            if os.path.isdir(c):
                parquet_dir = os.path.realpath(c)
                break

    parquet_glob = os.path.join(parquet_dir, "hits_*.parquet")
    parquet_files = [
        f for f in os.listdir(parquet_dir) if f.startswith("hits_") and f.endswith(".parquet")
    ]

    if not parquet_files:
        print(f"ERROR: No hits_*.parquet files found in {parquet_dir}", file=sys.stderr)
        sys.exit(1)

    data_size = sum(os.path.getsize(os.path.join(parquet_dir, f)) for f in parquet_files)

    output_path = args.output or os.path.join(
        os.path.dirname(__file__), "results.local.single_thread.json"
    )

    print(f"DuckDB ClickBench — single-threaded local benchmark")
    print(f"  DuckDB version : {duckdb.__version__}")
    print(f"  Machine        : {machine_description()}")
    print(
        f"  Parquet dir    : {parquet_dir}  ({len(parquet_files)} files, {data_size / 1e9:.1f} GB)"
    )
    print(f"  Tries per query: {TRIES}")
    print(f"  Output         : {output_path}")
    print()

    start = time.monotonic()
    results = run_benchmark(parquet_glob)
    total_elapsed = time.monotonic() - start

    print(f"\nTotal benchmark time: {total_elapsed:.1f}s")

    payload = {
        "system": "DuckDB (Parquet, partitioned, single-threaded)",
        "date": __import__("datetime").date.today().isoformat(),
        "machine": machine_description(),
        "cluster_size": 1,
        "proprietary": "no",
        "hardware": "cpu",
        "tuned": "no",
        "tags": ["C++", "column-oriented", "embedded", "stateless", "single-threaded"],
        "threads": 1,
        "load_time": 0,
        "data_size": data_size,
        "result": results,
    }

    with open(output_path, "w") as f:
        json.dump(payload, f, indent=2)

    print(f"Results written to {output_path}")


if __name__ == "__main__":
    main()
