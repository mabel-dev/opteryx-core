"""
DuckDB Local ClickBench Benchmark
==================================
Runs the ClickBench query suite against DuckDB reading the same partitioned
parquet files used by the Opteryx clickbench benchmark.

Output: duckdb/results.local.json

Usage:
    python tests/performance/clickbench/duckdb/runner.py [--parquet-dir scratch/hits]
    python tests/performance/clickbench/duckdb/runner.py --avro-dir scratch/hits_avro

`--avro-dir` reads the Avro shards `make clickbench-avro` converts
(dev/clickbench/parquet_to_avro.py) through DuckDB's avro extension
(`INSTALL avro` once; it is not built in) and writes results.local.avro.json -
the like-for-like engine comparison for `make clickbench-avro`.

`--aistor-table` reads an Iceberg table in the AIStor `clickbench` namespace
(e.g. hits_rugo_typed_split_10) - the SAME table the opteryx
`--variant aistor-*` runs read - and writes results.aistor.<table>.json, the
like-for-like engine comparison for those variants. DuckDB's REST catalog
client cannot SigV4-sign a non-AWS host ("Could not parse AWS service from
host"), so the catalog lookup alone goes through pyiceberg, once, before any
timing; DuckDB's iceberg_scan then reads the metadata JSON, manifests and data
files over S3 itself. Opteryx's per-query times include its catalog lookup;
these do not. Iceberg types the string columns BINARY, so they are decode()d to
VARCHAR in the view, as the Avro path does.

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
# Queries 33 and 34 are commented-out in the Opteryx benchmark; we still run
# them here so the result array index lines up with the upstream spec.
# ---------------------------------------------------------------------------
QUERIES = [
    # 00
    "SELECT COUNT(*) FROM hits",
    # 01
    "SELECT COUNT(*) FROM hits WHERE AdvEngineID <> 0",
    # 02
    "SELECT SUM(AdvEngineID), COUNT(*), AVG(ResolutionWidth) FROM hits",
    # 03
    "SELECT AVG(UserID) FROM hits",
    # 04
    "SELECT COUNT(DISTINCT UserID) FROM hits",
    # 05
    "SELECT COUNT(DISTINCT SearchPhrase) FROM hits",
    # 06
    "SELECT MIN(EventDate), MAX(EventDate) FROM hits",
    # 07
    "SELECT AdvEngineID, COUNT(*) FROM hits WHERE AdvEngineID <> 0 GROUP BY AdvEngineID ORDER BY COUNT(*) DESC",
    # 08
    "SELECT RegionID, COUNT(DISTINCT UserID) AS u FROM hits GROUP BY RegionID ORDER BY u DESC LIMIT 10",
    # 09
    "SELECT RegionID, SUM(AdvEngineID), COUNT(*) AS c, AVG(ResolutionWidth), COUNT(DISTINCT UserID) FROM hits GROUP BY RegionID ORDER BY c DESC LIMIT 10",
    # 10
    "SELECT MobilePhoneModel, COUNT(DISTINCT UserID) AS u FROM hits WHERE MobilePhoneModel <> '' GROUP BY MobilePhoneModel ORDER BY u DESC LIMIT 10",
    # 11
    "SELECT MobilePhone, MobilePhoneModel, COUNT(DISTINCT UserID) AS u FROM hits WHERE MobilePhoneModel <> '' GROUP BY MobilePhone, MobilePhoneModel ORDER BY u DESC LIMIT 10",
    # 12
    "SELECT SearchPhrase, COUNT(*) AS c FROM hits WHERE SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY c DESC LIMIT 10",
    # 13
    "SELECT SearchPhrase, COUNT(DISTINCT UserID) AS u FROM hits WHERE SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY u DESC LIMIT 10",
    # 14
    "SELECT SearchEngineID, SearchPhrase, COUNT(*) AS c FROM hits WHERE SearchPhrase <> '' GROUP BY SearchEngineID, SearchPhrase ORDER BY c DESC LIMIT 10",
    # 15
    "SELECT UserID, COUNT(*) FROM hits GROUP BY UserID ORDER BY COUNT(*) DESC LIMIT 10",
    # 16
    "SELECT UserID, SearchPhrase, COUNT(*) FROM hits GROUP BY UserID, SearchPhrase ORDER BY COUNT(*) DESC LIMIT 10",
    # 17
    "SELECT UserID, SearchPhrase, COUNT(*) FROM hits GROUP BY UserID, SearchPhrase LIMIT 10",
    # 18
    "SELECT UserID, extract(minute FROM toDateTime(EventTime)) AS m, SearchPhrase, COUNT(*) FROM hits GROUP BY UserID, m, SearchPhrase ORDER BY COUNT(*) DESC LIMIT 10",
    # 19
    "SELECT UserID FROM hits WHERE UserID = 435090932899640449",
    # 20
    "SELECT COUNT(*) FROM hits WHERE URL LIKE '%google%'",
    # 21
    "SELECT SearchPhrase, MIN(URL), COUNT(*) AS c FROM hits WHERE URL LIKE '%google%' AND SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY c DESC LIMIT 10",
    # 22
    "SELECT SearchPhrase, MIN(URL), MIN(Title), COUNT(*) AS c, COUNT(DISTINCT UserID) FROM hits WHERE Title LIKE '%Google%' AND URL NOT LIKE '%.google.%' AND SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY c DESC LIMIT 10",
    # 23
    "SELECT * FROM hits WHERE URL LIKE '%google%' ORDER BY EventTime LIMIT 10",
    # 24
    "SELECT SearchPhrase FROM hits WHERE SearchPhrase <> '' ORDER BY EventTime LIMIT 10",
    # 25
    "SELECT SearchPhrase FROM hits WHERE SearchPhrase <> '' ORDER BY SearchPhrase LIMIT 10",
    # 26
    "SELECT SearchPhrase FROM hits WHERE SearchPhrase <> '' ORDER BY EventTime, SearchPhrase LIMIT 10",
    # 27
    "SELECT CounterID, AVG(STRLEN(URL)) AS l, COUNT(*) AS c FROM hits WHERE URL <> '' GROUP BY CounterID HAVING COUNT(*) > 100000 ORDER BY l DESC LIMIT 25",
    # 28
    r"SELECT REGEXP_REPLACE(Referer, '^https?://(?:www\.)?([^/]+)/.*$', '\1') AS k, AVG(STRLEN(Referer)) AS l, COUNT(*) AS c, MIN(Referer) FROM hits WHERE Referer <> '' GROUP BY k HAVING COUNT(*) > 100000 ORDER BY l DESC LIMIT 25",
    # 29
    "SELECT SUM(ResolutionWidth), SUM(ResolutionWidth + 1), SUM(ResolutionWidth + 2), SUM(ResolutionWidth + 3), SUM(ResolutionWidth + 4), SUM(ResolutionWidth + 5), SUM(ResolutionWidth + 6), SUM(ResolutionWidth + 7), SUM(ResolutionWidth + 8), SUM(ResolutionWidth + 9), SUM(ResolutionWidth + 10), SUM(ResolutionWidth + 11), SUM(ResolutionWidth + 12), SUM(ResolutionWidth + 13), SUM(ResolutionWidth + 14), SUM(ResolutionWidth + 15), SUM(ResolutionWidth + 16), SUM(ResolutionWidth + 17), SUM(ResolutionWidth + 18), SUM(ResolutionWidth + 19), SUM(ResolutionWidth + 20), SUM(ResolutionWidth + 21), SUM(ResolutionWidth + 22), SUM(ResolutionWidth + 23), SUM(ResolutionWidth + 24), SUM(ResolutionWidth + 25), SUM(ResolutionWidth + 26), SUM(ResolutionWidth + 27), SUM(ResolutionWidth + 28), SUM(ResolutionWidth + 29), SUM(ResolutionWidth + 30), SUM(ResolutionWidth + 31), SUM(ResolutionWidth + 32), SUM(ResolutionWidth + 33), SUM(ResolutionWidth + 34), SUM(ResolutionWidth + 35), SUM(ResolutionWidth + 36), SUM(ResolutionWidth + 37), SUM(ResolutionWidth + 38), SUM(ResolutionWidth + 39), SUM(ResolutionWidth + 40), SUM(ResolutionWidth + 41), SUM(ResolutionWidth + 42), SUM(ResolutionWidth + 43), SUM(ResolutionWidth + 44), SUM(ResolutionWidth + 45), SUM(ResolutionWidth + 46), SUM(ResolutionWidth + 47), SUM(ResolutionWidth + 48), SUM(ResolutionWidth + 49), SUM(ResolutionWidth + 50), SUM(ResolutionWidth + 51), SUM(ResolutionWidth + 52), SUM(ResolutionWidth + 53), SUM(ResolutionWidth + 54), SUM(ResolutionWidth + 55), SUM(ResolutionWidth + 56), SUM(ResolutionWidth + 57), SUM(ResolutionWidth + 58), SUM(ResolutionWidth + 59), SUM(ResolutionWidth + 60), SUM(ResolutionWidth + 61), SUM(ResolutionWidth + 62), SUM(ResolutionWidth + 63), SUM(ResolutionWidth + 64), SUM(ResolutionWidth + 65), SUM(ResolutionWidth + 66), SUM(ResolutionWidth + 67), SUM(ResolutionWidth + 68), SUM(ResolutionWidth + 69), SUM(ResolutionWidth + 70), SUM(ResolutionWidth + 71), SUM(ResolutionWidth + 72), SUM(ResolutionWidth + 73), SUM(ResolutionWidth + 74), SUM(ResolutionWidth + 75), SUM(ResolutionWidth + 76), SUM(ResolutionWidth + 77), SUM(ResolutionWidth + 78), SUM(ResolutionWidth + 79), SUM(ResolutionWidth + 80), SUM(ResolutionWidth + 81), SUM(ResolutionWidth + 82), SUM(ResolutionWidth + 83), SUM(ResolutionWidth + 84), SUM(ResolutionWidth + 85), SUM(ResolutionWidth + 86), SUM(ResolutionWidth + 87), SUM(ResolutionWidth + 88), SUM(ResolutionWidth + 89) FROM hits",
    # 30
    "SELECT SearchEngineID, ClientIP, COUNT(*) AS c, SUM(IsRefresh), AVG(ResolutionWidth) FROM hits WHERE SearchPhrase <> '' GROUP BY SearchEngineID, ClientIP ORDER BY c DESC LIMIT 10",
    # 31
    "SELECT WatchID, ClientIP, COUNT(*) AS c, SUM(IsRefresh), AVG(ResolutionWidth) FROM hits WHERE SearchPhrase <> '' GROUP BY WatchID, ClientIP ORDER BY c DESC LIMIT 10",
    # 32
    "SELECT WatchID, ClientIP, COUNT(*) AS c, SUM(IsRefresh), AVG(ResolutionWidth) FROM hits GROUP BY WatchID, ClientIP ORDER BY c DESC LIMIT 10",
    # 33
    "SELECT URL, COUNT(*) AS c FROM hits GROUP BY URL ORDER BY c DESC LIMIT 10",
    # 34
    "SELECT 1, URL, COUNT(*) AS c FROM hits GROUP BY 1, URL ORDER BY c DESC LIMIT 10",
    # 35
    "SELECT ClientIP, ClientIP - 1, ClientIP - 2, ClientIP - 3, COUNT(*) AS c FROM hits GROUP BY ClientIP, ClientIP - 1, ClientIP - 2, ClientIP - 3 ORDER BY c DESC LIMIT 10",
    # 36
    "SELECT URL, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= '2013-07-01' AND EventDate <= '2013-07-31' AND DontCountHits = 0 AND IsRefresh = 0 AND URL <> '' GROUP BY URL ORDER BY PageViews DESC LIMIT 10",
    # 37
    "SELECT Title, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= '2013-07-01' AND EventDate <= '2013-07-31' AND DontCountHits = 0 AND IsRefresh = 0 AND Title <> '' GROUP BY Title ORDER BY PageViews DESC LIMIT 10",
    # 38
    "SELECT URL, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= '2013-07-01' AND EventDate <= '2013-07-31' AND IsRefresh = 0 AND IsLink <> 0 AND IsDownload = 0 GROUP BY URL ORDER BY PageViews DESC LIMIT 10 OFFSET 1000",
    # 39
    "SELECT TraficSourceID, SearchEngineID, AdvEngineID, CASE WHEN (SearchEngineID = 0 AND AdvEngineID = 0) THEN Referer ELSE '' END AS Src, URL AS Dst, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= '2013-07-01' AND EventDate <= '2013-07-31' AND IsRefresh = 0 GROUP BY TraficSourceID, SearchEngineID, AdvEngineID, Src, Dst ORDER BY PageViews DESC LIMIT 10 OFFSET 1000",
    # 40
    "SELECT URLHash, EventDate, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= '2013-07-01' AND EventDate <= '2013-07-31' AND IsRefresh = 0 AND TraficSourceID IN (-1, 6) AND RefererHash = 3594120000172545465 GROUP BY URLHash, EventDate ORDER BY PageViews DESC LIMIT 10 OFFSET 100",
    # 41
    "SELECT WindowClientWidth, WindowClientHeight, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= '2013-07-01' AND EventDate <= '2013-07-31' AND IsRefresh = 0 AND DontCountHits = 0 AND URLHash = 2868770270353813622 GROUP BY WindowClientWidth, WindowClientHeight ORDER BY PageViews DESC LIMIT 10 OFFSET 10000",
    # 42
    "SELECT DATE_TRUNC('minute', toDateTime(EventTime)) AS M, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= '2013-07-14' AND EventDate <= '2013-07-15' AND IsRefresh = 0 AND DontCountHits = 0 GROUP BY DATE_TRUNC('minute', toDateTime(EventTime)) ORDER BY DATE_TRUNC('minute', toDateTime(EventTime)) LIMIT 10 OFFSET 1000",
]

# ---------------------------------------------------------------------------
# The 15 queries proposed in ClickHouse/ClickBench#1489 (OPEN, not merged as of
# 2026-10-23), verbatim from its duckdb-parquet/queries.sql. The PR numbers them
# Q44-Q58 (1-based); here they continue the 0-based numbering, as in
# opteryx/runner.py's EXTENDED_STATEMENTS. `--extended` appends them.
# ---------------------------------------------------------------------------
EXTENDED_QUERIES = [
    # 43
    "SELECT UserID, RANK() OVER (ORDER BY EventTime) AS r FROM hits ORDER BY r DESC LIMIT 10",
    # 44
    "SELECT * FROM hits ORDER BY ClientIP, EventTime LIMIT 100",
    # 45
    "SELECT CounterID, avg(length(Referer)) AS l FROM hits WHERE Referer <> '' GROUP BY CounterID HAVING count(*) > 100000 ORDER BY l DESC LIMIT 25",
    # 46
    "SELECT BrowserCountry, BrowserLanguage, count(*) AS c FROM hits GROUP BY BrowserCountry, BrowserLanguage ORDER BY c DESC LIMIT 10",
    # 47
    "SELECT Title, URL, count(*) AS c FROM hits GROUP BY Title, URL ORDER BY c DESC LIMIT 10",
    # 48
    "SELECT count(*) FILTER (WHERE NOT REGEXP_MATCHES(URL, '^https')) FROM hits",
    # 49
    "SELECT count(*) FILTER (WHERE strpos(URL, 'google') > 0) FROM hits",
    # 50
    "SELECT CounterID, COALESCE(EventTime - lag(EventTime) OVER (PARTITION BY CounterID ORDER BY EventTime), EventTime) AS dt FROM hits WHERE CounterID = 62 ORDER BY dt DESC LIMIT 10",
    # 51
    "SELECT min(URL), max(URL) FROM hits",
    # 52
    "SELECT URL, length(URL) AS l FROM hits ORDER BY l DESC LIMIT 10",
    # 53
    "SELECT substr(URL, 1, 8) AS p, count(*) AS c FROM hits GROUP BY p ORDER BY c DESC LIMIT 10",
    # 54
    "SELECT percentile_cont(0.1) WITHIN GROUP (ORDER BY ResponseEndTiming), percentile_cont(0.5) WITHIN GROUP (ORDER BY ResponseEndTiming), percentile_cont(0.9) WITHIN GROUP (ORDER BY ResponseEndTiming) FROM hits WHERE ResponseEndTiming > 0",
    # 55
    "SELECT percentile_cont(0.99) WITHIN GROUP (ORDER BY ResponseEndTiming) FROM hits WHERE ResponseEndTiming > 0",
    # 56
    "SELECT hour(toDateTime(EventTime)) AS h, count(*) AS c FROM hits GROUP BY h ORDER BY h",
    # 57
    "SELECT EventTime, count(*) AS c FROM hits GROUP BY EventTime ORDER BY c DESC LIMIT 10",
]

TRIES = 3


def machine_description() -> str:
    """Return a short human-readable machine identifier."""
    node = socket.gethostname()
    cpu = platform.processor() or platform.machine()
    return f"{node} ({cpu})"


AISTOR_REGION = "us-east-1"


def resolve_aistor_table(table_name: str) -> tuple[str, int]:
    """(metadata_location, data_size) of `clickbench.<table_name>` in the AIStor
    Iceberg REST catalog - the catalog tests/performance/_common.py binds the
    opteryx `aistor` workspace to, configured identically. A missing AISTOR_*
    key raises KeyError: no remote configured means no remote run."""
    from pyiceberg.catalog import load_catalog

    endpoint = os.environ["AISTOR_ENDPOINT"].rstrip("/")
    # pyiceberg's SigV4 signer takes its credentials from the process environment.
    os.environ["AWS_ACCESS_KEY_ID"] = os.environ["AISTOR_ACCESS_KEY_ID"]
    os.environ["AWS_SECRET_ACCESS_KEY"] = os.environ["AISTOR_SECRET_ACCESS_KEY"]
    os.environ["AWS_REGION"] = AISTOR_REGION
    catalog = load_catalog(
        "aistor",
        **{
            "type": "rest",
            "uri": f"{endpoint}/_iceberg",
            "warehouse": "benchmarks",
            "rest.sigv4-enabled": "true",
            "rest.signing-name": "s3tables",
            "rest.signing-region": AISTOR_REGION,
            "s3.endpoint": endpoint,
            "s3.access-key-id": os.environ["AISTOR_ACCESS_KEY_ID"],
            "s3.secret-access-key": os.environ["AISTOR_SECRET_ACCESS_KEY"],
            "s3.region": AISTOR_REGION,
        },
    )
    table = catalog.load_table(("clickbench", table_name))
    data_size = sum(task.file.file_size_in_bytes for task in table.scan().plan_files())
    return table.metadata_location, data_size


def run_benchmark(queries: list, parquet_glob: str = None, avro_glob: str = None, iceberg_metadata: str = None) -> list:
    """
    Run all queries TRIES times each, over the parquet, the Avro files, OR the
    AIStor Iceberg table whose metadata JSON is `iceberg_metadata`.
    Returns a list of [cold, warm1, warm2] timing tuples (seconds, 3 d.p.).
    """
    con = duckdb.connect()
    to_date_time = "epoch_ms(CAST(t AS BIGINT) * 1000)"
    if iceberg_metadata is not None:
        endpoint = os.environ["AISTOR_ENDPOINT"].rstrip("/")
        scheme, host = endpoint.split("://", 1)
        for extension in ("httpfs", "iceberg"):
            con.execute(f"INSTALL {extension}")
            con.execute(f"LOAD {extension}")
        con.execute(f"""
            CREATE SECRET aistor (
                TYPE s3,
                KEY_ID '{os.environ["AISTOR_ACCESS_KEY_ID"]}',
                SECRET '{os.environ["AISTOR_SECRET_ACCESS_KEY"]}',
                REGION '{AISTOR_REGION}',
                ENDPOINT '{host}',
                URL_STYLE 'path',
                USE_SSL {str(scheme == "https").lower()}
            )
        """)
        columns = con.execute(f"DESCRIBE SELECT * FROM iceberg_scan('{iceberg_metadata}')").fetchall()
        column_types = {name: column_type for name, column_type, *_ in columns}
        replace = [f"decode({name}) AS {name}" for name, column_type in column_types.items() if column_type == "BLOB"]
        # The canon split carries upstream's integer EventDate/EventTime; the
        # rugo typed split carries DATE/TIMESTAMP.
        if column_types["EventDate"] != "DATE":
            replace.append("make_date(EventDate) AS EventDate")
        if column_types["EventTime"] == "TIMESTAMP":
            to_date_time = "t"
        con.execute(
            f"CREATE VIEW hits AS SELECT * REPLACE ({', '.join(replace)}) FROM iceberg_scan('{iceberg_metadata}')"
        )
    elif avro_glob is not None:
        # The shards keep the source's binary strings as Avro `bytes` (BLOB here);
        # decode() is read_parquet's binary_as_string=True, so the queries see
        # the same VARCHAR columns as the parquet view.
        con.execute("LOAD avro")
        columns = con.execute(f"DESCRIBE SELECT * FROM read_avro('{avro_glob}')").fetchall()
        blobs = [name for name, column_type, *_ in columns if column_type == "BLOB"]
        replace = ", ".join(
            ["make_date(EventDate) AS EventDate"] + [f"decode({name}) AS {name}" for name in blobs]
        )
        con.execute(f"CREATE VIEW hits AS SELECT * REPLACE ({replace}) FROM read_avro('{avro_glob}')")
    else:
        # Mirror the upstream create.sql setup
        con.execute("SET parquet_metadata_cache=true")
        con.execute(f"""
            CREATE VIEW hits AS
            SELECT * REPLACE (make_date(EventDate) AS EventDate)
            FROM read_parquet('{parquet_glob}', binary_as_string=True)
        """)
    con.execute(f"CREATE MACRO toDateTime(t) AS {to_date_time}")

    results = []
    total = len(queries)

    for idx, query in enumerate(queries):
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
        "--avro-dir",
        default=None,
        help="Read hits_*.avro from this directory through DuckDB's avro extension instead of parquet "
        "(default output: results.local.avro.json)",
    )
    parser.add_argument(
        "--output",
        default=None,
        help="Output JSON path (default: same dir as this script / duckdb.local.json)",
    )
    parser.add_argument(
        "--aistor-table",
        default=None,
        help="Read Iceberg table clickbench.<name> on AIStor (e.g. hits_rugo_typed_split_10) instead of parquet "
        "(default output: results.aistor.<name>.json)",
    )
    parser.add_argument(
        "--extended",
        action="store_true",
        default=False,
        help="Append the 15 queries proposed in ClickHouse/ClickBench#1489 (Q43-Q57); parquet only "
        "(default output: results.local.extended.json)",
    )
    args = parser.parse_args()

    if args.extended and (args.aistor_table is not None or args.avro_dir is not None):
        parser.error("--extended is parquet only: it is the baseline for opteryx `--extended` on the local split")
    queries = QUERIES + EXTENDED_QUERIES if args.extended else QUERIES

    if args.aistor_table is not None:
        import dotenv

        # AISTOR_* live in the repo-root .env (make runs from the root).
        dotenv.load_dotenv(os.path.join(os.getcwd(), ".env"))
        metadata_location, data_size = resolve_aistor_table(args.aistor_table)
        output_path = args.output or os.path.join(
            os.path.dirname(__file__), f"results.aistor.{args.aistor_table}.json"
        )
        print("DuckDB ClickBench — AIStor Iceberg benchmark")
        print(f"  DuckDB version : {duckdb.__version__}")
        print(f"  Machine        : {machine_description()}")
        print(f"  Table          : clickbench.{args.aistor_table}  ({data_size / 1e9:.2f} GB)")
        print(f"  Metadata       : {metadata_location}")
        print(f"  Tries per query: {TRIES}")
        print(f"  Output         : {output_path}")
        print()
        start = time.monotonic()
        results = run_benchmark(queries, iceberg_metadata=metadata_location)
        print(f"\nTotal benchmark time: {time.monotonic() - start:.1f}s")
        payload = {
            "system": "DuckDB (Iceberg on AIStor, iceberg_scan)",
            "date": __import__("datetime").date.today().isoformat(),
            "machine": machine_description(),
            "cluster_size": 1,
            "proprietary": "no",
            "hardware": "cpu",
            "tuned": "no",
            "tags": ["C++", "column-oriented", "embedded", "stateless"],
            "load_time": 0,
            "data_size": data_size,
            "table": f"clickbench.{args.aistor_table}",
            "result": results,
        }
        with open(output_path, "w") as f:
            json.dump(payload, f, indent=2)
        print(f"Results written to {output_path}")
        return

    if args.avro_dir is not None:
        avro_dir = os.path.realpath(args.avro_dir)
        avro_files = [f for f in os.listdir(avro_dir) if f.startswith("hits_") and f.endswith(".avro")]
        if not avro_files:
            print(f"ERROR: No hits_*.avro files found in {avro_dir}", file=sys.stderr)
            sys.exit(1)
        data_size = sum(os.path.getsize(os.path.join(avro_dir, f)) for f in avro_files)
        output_path = args.output or os.path.join(os.path.dirname(__file__), "results.local.avro.json")
        print("DuckDB ClickBench — local benchmark (Avro)")
        print(f"  DuckDB version : {duckdb.__version__}")
        print(f"  Machine        : {machine_description()}")
        print(f"  Avro dir       : {avro_dir}  ({len(avro_files)} files, {data_size / 1e9:.1f} GB)")
        print(f"  Tries per query: {TRIES}")
        print(f"  Output         : {output_path}")
        print()
        start = time.monotonic()
        results = run_benchmark(queries, avro_glob=os.path.join(avro_dir, "hits_*.avro"))
        print(f"\nTotal benchmark time: {time.monotonic() - start:.1f}s")
        payload = {
            "system": "DuckDB (Avro, avro extension)",
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
        return

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

    output_path = args.output or os.path.join(
        os.path.dirname(__file__), "results.local.extended.json" if args.extended else "results.local.json"
    )

    print(f"DuckDB ClickBench — local benchmark")
    print(f"  DuckDB version : {duckdb.__version__}")
    print(f"  Machine        : {machine_description()}")
    print(
        f"  Parquet dir    : {parquet_dir}  ({len(parquet_files)} files, {data_size / 1e9:.1f} GB)"
    )
    print(f"  Queries        : {len(queries)}{' (extended, ClickBench#1489)' if args.extended else ''}")
    print(f"  Tries per query: {TRIES}")
    print(f"  Output         : {output_path}")
    print()

    start = time.monotonic()
    results = run_benchmark(queries, parquet_glob=parquet_glob)
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
