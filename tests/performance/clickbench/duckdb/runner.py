"""
DuckDB Local ClickBench Benchmark
==================================
Runs the ClickBench query suite against DuckDB reading the same partitioned
parquet files used by the Opteryx clickbench benchmark.

Output: duckdb/results.local.json

Usage:
    python tests/performance/clickbench/duckdb/runner.py [--parquet-dir scratch/hits]

The result file is written to the same directory as this script so that
opteryx/runner.py can reference it as the local baseline.
"""

import argparse
import json
import os
import platform
import socket
import sys
import time

import duckdb

# ---------------------------------------------------------------------------
# 43 ClickBench queries translated for DuckDB against the `hits` view.
# Queries 34 and 35 are commented-out in the Opteryx benchmark; we still run
# them here so the result array index lines up with the upstream spec.
# ---------------------------------------------------------------------------
QUERIES = [
    # 01
    "SELECT COUNT(*) FROM hits",
    # 02
    "SELECT COUNT(*) FROM hits WHERE AdvEngineID <> 0",
    # 03
    "SELECT SUM(AdvEngineID), COUNT(*), AVG(ResolutionWidth) FROM hits",
    # 04
    "SELECT AVG(UserID) FROM hits",
    # 05
    "SELECT COUNT(DISTINCT UserID) FROM hits",
    # 06
    "SELECT COUNT(DISTINCT SearchPhrase) FROM hits",
    # 07
    "SELECT MIN(EventDate), MAX(EventDate) FROM hits",
    # 08
    "SELECT AdvEngineID, COUNT(*) FROM hits WHERE AdvEngineID <> 0 GROUP BY AdvEngineID ORDER BY COUNT(*) DESC",
    # 09
    "SELECT RegionID, COUNT(DISTINCT UserID) AS u FROM hits GROUP BY RegionID ORDER BY u DESC LIMIT 10",
    # 10
    "SELECT RegionID, SUM(AdvEngineID), COUNT(*) AS c, AVG(ResolutionWidth), COUNT(DISTINCT UserID) FROM hits GROUP BY RegionID ORDER BY c DESC LIMIT 10",
    # 11
    "SELECT MobilePhoneModel, COUNT(DISTINCT UserID) AS u FROM hits WHERE MobilePhoneModel <> '' GROUP BY MobilePhoneModel ORDER BY u DESC LIMIT 10",
    # 12
    "SELECT MobilePhone, MobilePhoneModel, COUNT(DISTINCT UserID) AS u FROM hits WHERE MobilePhoneModel <> '' GROUP BY MobilePhone, MobilePhoneModel ORDER BY u DESC LIMIT 10",
    # 13
    "SELECT SearchPhrase, COUNT(*) AS c FROM hits WHERE SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY c DESC LIMIT 10",
    # 14
    "SELECT SearchPhrase, COUNT(DISTINCT UserID) AS u FROM hits WHERE SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY u DESC LIMIT 10",
    # 15
    "SELECT SearchEngineID, SearchPhrase, COUNT(*) AS c FROM hits WHERE SearchPhrase <> '' GROUP BY SearchEngineID, SearchPhrase ORDER BY c DESC LIMIT 10",
    # 16
    "SELECT UserID, COUNT(*) FROM hits GROUP BY UserID ORDER BY COUNT(*) DESC LIMIT 10",
    # 17
    "SELECT UserID, SearchPhrase, COUNT(*) FROM hits GROUP BY UserID, SearchPhrase ORDER BY COUNT(*) DESC LIMIT 10",
    # 18
    "SELECT UserID, SearchPhrase, COUNT(*) FROM hits GROUP BY UserID, SearchPhrase LIMIT 10",
    # 19
    "SELECT UserID, extract(minute FROM toDateTime(EventTime)) AS m, SearchPhrase, COUNT(*) FROM hits GROUP BY UserID, m, SearchPhrase ORDER BY COUNT(*) DESC LIMIT 10",
    # 20
    "SELECT UserID FROM hits WHERE UserID = 435090932899640449",
    # 21
    "SELECT COUNT(*) FROM hits WHERE URL LIKE '%google%'",
    # 22
    "SELECT SearchPhrase, MIN(URL), COUNT(*) AS c FROM hits WHERE URL LIKE '%google%' AND SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY c DESC LIMIT 10",
    # 23
    "SELECT SearchPhrase, MIN(URL), MIN(Title), COUNT(*) AS c, COUNT(DISTINCT UserID) FROM hits WHERE Title LIKE '%Google%' AND URL NOT LIKE '%.google.%' AND SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY c DESC LIMIT 10",
    # 24
    "SELECT * FROM hits WHERE URL LIKE '%google%' ORDER BY EventTime LIMIT 10",
    # 25
    "SELECT SearchPhrase FROM hits WHERE SearchPhrase <> '' ORDER BY EventTime LIMIT 10",
    # 26
    "SELECT SearchPhrase FROM hits WHERE SearchPhrase <> '' ORDER BY SearchPhrase LIMIT 10",
    # 27
    "SELECT SearchPhrase FROM hits WHERE SearchPhrase <> '' ORDER BY EventTime, SearchPhrase LIMIT 10",
    # 28
    "SELECT CounterID, AVG(STRLEN(URL)) AS l, COUNT(*) AS c FROM hits WHERE URL <> '' GROUP BY CounterID HAVING COUNT(*) > 100000 ORDER BY l DESC LIMIT 25",
    # 29
    r"SELECT REGEXP_REPLACE(Referer, '^https?://(?:www\.)?([^/]+)/.*$', '\1') AS k, AVG(STRLEN(Referer)) AS l, COUNT(*) AS c, MIN(Referer) FROM hits WHERE Referer <> '' GROUP BY k HAVING COUNT(*) > 100000 ORDER BY l DESC LIMIT 25",
    # 30
    "SELECT SUM(ResolutionWidth), SUM(ResolutionWidth + 1), SUM(ResolutionWidth + 2), SUM(ResolutionWidth + 3), SUM(ResolutionWidth + 4), SUM(ResolutionWidth + 5), SUM(ResolutionWidth + 6), SUM(ResolutionWidth + 7), SUM(ResolutionWidth + 8), SUM(ResolutionWidth + 9), SUM(ResolutionWidth + 10), SUM(ResolutionWidth + 11), SUM(ResolutionWidth + 12), SUM(ResolutionWidth + 13), SUM(ResolutionWidth + 14), SUM(ResolutionWidth + 15), SUM(ResolutionWidth + 16), SUM(ResolutionWidth + 17), SUM(ResolutionWidth + 18), SUM(ResolutionWidth + 19), SUM(ResolutionWidth + 20), SUM(ResolutionWidth + 21), SUM(ResolutionWidth + 22), SUM(ResolutionWidth + 23), SUM(ResolutionWidth + 24), SUM(ResolutionWidth + 25), SUM(ResolutionWidth + 26), SUM(ResolutionWidth + 27), SUM(ResolutionWidth + 28), SUM(ResolutionWidth + 29), SUM(ResolutionWidth + 30), SUM(ResolutionWidth + 31), SUM(ResolutionWidth + 32), SUM(ResolutionWidth + 33), SUM(ResolutionWidth + 34), SUM(ResolutionWidth + 35), SUM(ResolutionWidth + 36), SUM(ResolutionWidth + 37), SUM(ResolutionWidth + 38), SUM(ResolutionWidth + 39), SUM(ResolutionWidth + 40), SUM(ResolutionWidth + 41), SUM(ResolutionWidth + 42), SUM(ResolutionWidth + 43), SUM(ResolutionWidth + 44), SUM(ResolutionWidth + 45), SUM(ResolutionWidth + 46), SUM(ResolutionWidth + 47), SUM(ResolutionWidth + 48), SUM(ResolutionWidth + 49), SUM(ResolutionWidth + 50), SUM(ResolutionWidth + 51), SUM(ResolutionWidth + 52), SUM(ResolutionWidth + 53), SUM(ResolutionWidth + 54), SUM(ResolutionWidth + 55), SUM(ResolutionWidth + 56), SUM(ResolutionWidth + 57), SUM(ResolutionWidth + 58), SUM(ResolutionWidth + 59), SUM(ResolutionWidth + 60), SUM(ResolutionWidth + 61), SUM(ResolutionWidth + 62), SUM(ResolutionWidth + 63), SUM(ResolutionWidth + 64), SUM(ResolutionWidth + 65), SUM(ResolutionWidth + 66), SUM(ResolutionWidth + 67), SUM(ResolutionWidth + 68), SUM(ResolutionWidth + 69), SUM(ResolutionWidth + 70), SUM(ResolutionWidth + 71), SUM(ResolutionWidth + 72), SUM(ResolutionWidth + 73), SUM(ResolutionWidth + 74), SUM(ResolutionWidth + 75), SUM(ResolutionWidth + 76), SUM(ResolutionWidth + 77), SUM(ResolutionWidth + 78), SUM(ResolutionWidth + 79), SUM(ResolutionWidth + 80), SUM(ResolutionWidth + 81), SUM(ResolutionWidth + 82), SUM(ResolutionWidth + 83), SUM(ResolutionWidth + 84), SUM(ResolutionWidth + 85), SUM(ResolutionWidth + 86), SUM(ResolutionWidth + 87), SUM(ResolutionWidth + 88), SUM(ResolutionWidth + 89) FROM hits",
    # 31
    "SELECT SearchEngineID, ClientIP, COUNT(*) AS c, SUM(IsRefresh), AVG(ResolutionWidth) FROM hits WHERE SearchPhrase <> '' GROUP BY SearchEngineID, ClientIP ORDER BY c DESC LIMIT 10",
    # 32
    "SELECT WatchID, ClientIP, COUNT(*) AS c, SUM(IsRefresh), AVG(ResolutionWidth) FROM hits WHERE SearchPhrase <> '' GROUP BY WatchID, ClientIP ORDER BY c DESC LIMIT 10",
    # 33
    "SELECT WatchID, ClientIP, COUNT(*) AS c, SUM(IsRefresh), AVG(ResolutionWidth) FROM hits GROUP BY WatchID, ClientIP ORDER BY c DESC LIMIT 10",
    # 34
    "SELECT URL, COUNT(*) AS c FROM hits GROUP BY URL ORDER BY c DESC LIMIT 10",
    # 35
    "SELECT 1, URL, COUNT(*) AS c FROM hits GROUP BY 1, URL ORDER BY c DESC LIMIT 10",
    # 36
    "SELECT ClientIP, ClientIP - 1, ClientIP - 2, ClientIP - 3, COUNT(*) AS c FROM hits GROUP BY ClientIP, ClientIP - 1, ClientIP - 2, ClientIP - 3 ORDER BY c DESC LIMIT 10",
    # 37
    "SELECT URL, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= '2013-07-01' AND EventDate <= '2013-07-31' AND DontCountHits = 0 AND IsRefresh = 0 AND URL <> '' GROUP BY URL ORDER BY PageViews DESC LIMIT 10",
    # 38
    "SELECT Title, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= '2013-07-01' AND EventDate <= '2013-07-31' AND DontCountHits = 0 AND IsRefresh = 0 AND Title <> '' GROUP BY Title ORDER BY PageViews DESC LIMIT 10",
    # 39
    "SELECT URL, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= '2013-07-01' AND EventDate <= '2013-07-31' AND IsRefresh = 0 AND IsLink <> 0 AND IsDownload = 0 GROUP BY URL ORDER BY PageViews DESC LIMIT 10 OFFSET 1000",
    # 40
    "SELECT TraficSourceID, SearchEngineID, AdvEngineID, CASE WHEN (SearchEngineID = 0 AND AdvEngineID = 0) THEN Referer ELSE '' END AS Src, URL AS Dst, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= '2013-07-01' AND EventDate <= '2013-07-31' AND IsRefresh = 0 GROUP BY TraficSourceID, SearchEngineID, AdvEngineID, Src, Dst ORDER BY PageViews DESC LIMIT 10 OFFSET 1000",
    # 41
    "SELECT URLHash, EventDate, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= '2013-07-01' AND EventDate <= '2013-07-31' AND IsRefresh = 0 AND TraficSourceID IN (-1, 6) AND RefererHash = 3594120000172545465 GROUP BY URLHash, EventDate ORDER BY PageViews DESC LIMIT 10 OFFSET 100",
    # 42
    "SELECT WindowClientWidth, WindowClientHeight, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= '2013-07-01' AND EventDate <= '2013-07-31' AND IsRefresh = 0 AND DontCountHits = 0 AND URLHash = 2868770270353813622 GROUP BY WindowClientWidth, WindowClientHeight ORDER BY PageViews DESC LIMIT 10 OFFSET 10000",
    # 43
    "SELECT DATE_TRUNC('minute', toDateTime(EventTime)) AS M, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= '2013-07-14' AND EventDate <= '2013-07-15' AND IsRefresh = 0 AND DontCountHits = 0 GROUP BY DATE_TRUNC('minute', toDateTime(EventTime)) ORDER BY DATE_TRUNC('minute', toDateTime(EventTime)) LIMIT 10 OFFSET 1000",
]

TRIES = 3


def machine_description() -> str:
    """Return a short human-readable machine identifier."""
    node = socket.gethostname()
    cpu = platform.processor() or platform.machine()
    return f"{node} ({cpu})"


def run_benchmark(parquet_glob: str) -> list:
    """
    Run all queries TRIES times each.
    Returns a list of [cold, warm1, warm2] timing tuples (seconds, 3 d.p.).
    """
    con = duckdb.connect()
    # Mirror the upstream create.sql setup
    con.execute("SET parquet_metadata_cache=true")
    con.execute(f"""
        CREATE VIEW hits AS
        SELECT * REPLACE (make_date(EventDate) AS EventDate)
        FROM read_parquet('{parquet_glob}', binary_as_string=True)
    """)
    con.execute("CREATE MACRO toDateTime(t) AS epoch_ms(CAST(t AS BIGINT) * 1000)")

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
    parser = argparse.ArgumentParser(description="DuckDB local ClickBench benchmark")
    parser.add_argument(
        "--parquet-dir",
        default="scratch/hits",
        help="Directory containing hits_*.parquet files (default: scratch/hits)",
    )
    parser.add_argument(
        "--output",
        default=None,
        help="Output JSON path (default: same dir as this script / duckdb.local.json)",
    )
    args = parser.parse_args()

    # Resolve parquet glob relative to the repo root (cwd when running via make)
    parquet_dir = args.parquet_dir
    if not os.path.isabs(parquet_dir):
        # Support running from repo root or from the script's own directory
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

    output_path = args.output or os.path.join(os.path.dirname(__file__), "results.local.json")

    print(f"DuckDB ClickBench — local benchmark")
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
        "system": "DuckDB (Parquet, partitioned)",
        "date": __import__("datetime").date.today().isoformat(),
        "machine": machine_description(),
        "cluster_size": 1,
        "proprietary": "no",
        "hardware": "cpu",
        "tuned": "no",
        "tags": ["C++", "column-oriented", "embedded", "stateless"],
        "load_time": 0,
        "data_size": data_size,
        "result": results,
    }

    with open(output_path, "w") as f:
        json.dump(payload, f, indent=2)

    print(f"Results written to {output_path}")


if __name__ == "__main__":
    main()
