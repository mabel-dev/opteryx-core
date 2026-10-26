"""
ClickHouse Local ClickBench Benchmark over the Avro or ORC shards
==================================================================
Runs the ClickBench query suite with `clickhouse local` reading the Avro shards
`make clickbench-avro` converts (scratch/hits_avro, dev/clickbench/parquet_to_avro.py)
through its `file(..., 'Avro')` table function - the second engine comparison for
`make clickbench-avro`, beside DuckDB's avro extension. With `--orc-dir` it reads the ORC
shards `make clickbench-orc` converts (scratch/hits_orc, dev/clickbench/parquet_to_orc.py)
through `file(..., 'ORC')` and writes clickhouse/results.local.orc.json - the ratio column of
`make clickbench-orc`.

The queries are the DuckDB runner's (tests/performance/clickbench/duckdb/runner.py),
which are written in ClickHouse's style; STRLEN becomes ClickHouse's `length`. The
`hits` view converts EventDate (days) to a Date, as the DuckDB view does. Strings
stay as ClickHouse String, which is bytes.

Each query runs in its own `clickhouse local` process; the time is ClickHouse's own
`--time` figure for the query (process start and the view are not counted).

Output: clickhouse/results.local.avro.json, or results.local.orc.json (DuckDB runner's result format).

Usage:
    python tests/performance/clickbench/clickhouse/runner.py --binary scratch/clickhouse/clickhouse \\
        [--avro-dir scratch/hits_avro | --orc-dir scratch/hits_orc] [--tries 1]
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
    parser = argparse.ArgumentParser(description="ClickHouse local ClickBench over Avro or ORC")
    parser.add_argument("--binary", required=True, help="Path to the clickhouse binary")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--avro-dir", default=None)
    group.add_argument("--orc-dir", default=None)
    parser.add_argument("--tries", type=int, default=1)
    parser.add_argument("--output", default=None)
    args = parser.parse_args()

    if args.orc_dir:
        label, extension, data_dir = "ORC", "orc", os.path.realpath(args.orc_dir)
    else:
        label, extension, data_dir = "Avro", "avro", os.path.realpath(args.avro_dir or "scratch/hits_avro")
    files = [f for f in os.listdir(data_dir) if f.startswith("hits_") and f.endswith("." + extension)]
    if not files:
        sys.exit(f"ERROR: No hits_*.{extension} files found in {data_dir}")
    data_size = sum(os.path.getsize(os.path.join(data_dir, f)) for f in files)
    version = subprocess.run([args.binary, "local", "--version"], capture_output=True, text=True, check=True).stdout.strip()
    output_path = args.output or os.path.join(os.path.dirname(os.path.abspath(__file__)), f"results.local.{extension}.json")
    view = (
        "CREATE VIEW hits AS SELECT * REPLACE (toDate(EventDate) AS EventDate) "
        f"FROM file('{data_dir}/hits_*.{extension}', '{label}');"
    )

    print(f"ClickHouse ClickBench — local benchmark ({label})")
    print(f"  ClickHouse     : {version}")
    print(f"  {label} dir".ljust(17) + f": {data_dir}  ({len(files)} files, {data_size / 1e9:.1f} GB)")
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
        "system": f"ClickHouse local ({label}, file())",
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
