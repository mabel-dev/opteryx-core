"""
ClickHouse Local ClickBench Benchmark over the Avro shards
===========================================================
Runs the ClickBench query suite with `clickhouse local` reading the Avro shards
`make clickbench-avro` converts (scratch/hits_avro, dev/clickbench/parquet_to_avro.py)
through its `file(..., 'Avro')` table function - the second engine comparison for
`make clickbench-avro`, beside DuckDB's avro extension.

The queries are the DuckDB runner's (tests/performance/clickbench/duckdb/runner.py),
which are written in ClickHouse's style; STRLEN becomes ClickHouse's `length`. The
`hits` view converts EventDate (days) to a Date, as the DuckDB view does. Strings
stay as ClickHouse String, which is bytes.

Each query runs in its own `clickhouse local` process; the time is ClickHouse's own
`--time` figure for the query (process start and the view are not counted).

Output: clickhouse/results.local.avro.json (DuckDB runner's result format).

Usage:
    python tests/performance/clickbench/clickhouse/runner.py --binary scratch/clickhouse/clickhouse \\
        [--avro-dir scratch/hits_avro] [--tries 1]
"""

import argparse
import datetime
import json
import os
import platform
import socket
import subprocess
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "duckdb"))

from runner import QUERIES as DUCKDB_QUERIES  # noqa: E402

QUERIES = [q.replace("STRLEN(", "length(") for q in DUCKDB_QUERIES]


def main():
    parser = argparse.ArgumentParser(description="ClickHouse local ClickBench over Avro")
    parser.add_argument("--binary", required=True, help="Path to the clickhouse binary")
    parser.add_argument("--avro-dir", default="scratch/hits_avro")
    parser.add_argument("--tries", type=int, default=1)
    parser.add_argument("--output", default=None)
    args = parser.parse_args()

    avro_dir = os.path.realpath(args.avro_dir)
    files = [f for f in os.listdir(avro_dir) if f.startswith("hits_") and f.endswith(".avro")]
    if not files:
        sys.exit(f"ERROR: No hits_*.avro files found in {avro_dir}")
    data_size = sum(os.path.getsize(os.path.join(avro_dir, f)) for f in files)
    version = subprocess.run([args.binary, "local", "--version"], capture_output=True, text=True, check=True).stdout.strip()
    output_path = args.output or os.path.join(os.path.dirname(os.path.abspath(__file__)), "results.local.avro.json")
    view = (
        "CREATE VIEW hits AS SELECT * REPLACE (toDate(EventDate) AS EventDate) "
        f"FROM file('{avro_dir}/hits_*.avro', 'Avro');"
    )

    print("ClickHouse ClickBench — local benchmark (Avro)")
    print(f"  ClickHouse     : {version}")
    print(f"  Avro dir       : {avro_dir}  ({len(files)} files, {data_size / 1e9:.1f} GB)")
    print(f"  Tries per query: {args.tries}")
    print(f"  Output         : {output_path}\n")

    results = []
    for index, query in enumerate(QUERIES):
        print(f"  Q{index:02d}/{len(QUERIES)} ", end="", flush=True)
        proc = subprocess.run(
            [args.binary, "local", "--time", "--query", view + (query + ";") * args.tries],
            capture_output=True,
            text=True,
        )
        if proc.returncode:
            print(f"ERROR: {proc.stderr.strip().splitlines()[0]}")
            results.append([None] * args.tries)
            continue
        # --time writes one elapsed line per statement to stderr: the view, then each try.
        times = [round(float(line), 3) for line in proc.stderr.split() if line.replace(".", "", 1).isdigit()][1:]
        if len(times) != args.tries:
            sys.exit(f"Q{index:02d}: expected {args.tries} timings from --time, got {proc.stderr!r}")
        print("  ".join(f"{t:.3f}s" for t in times), flush=True)
        results.append(times)

    payload = {
        "system": "ClickHouse local (Avro, file())",
        "date": datetime.date.today().isoformat(),
        "machine": f"{socket.gethostname()} ({platform.processor() or platform.machine()})",
        "version": version,
        "tries": args.tries,
        "data_size": data_size,
        "result": results,
    }
    with open(output_path, "w") as f:
        json.dump(payload, f, indent=2)
    print(f"\nResults written to {output_path}")


if __name__ == "__main__":
    main()
