#!/usr/bin/env python3
"""
JSONBench, the legal form: the five queries over ONE `data` VARIANT column (./load.py).

Each query is JSONBench's, with DuckDB's `j->>'$.commit.collection'` written as
`data->'commit'->>'collection'`. Every query alternates with DuckDB's own query
(upstream duckdb/queries.sql, verbatim, as ../duckdb/runner.py runs them) in this one
process, so drift on the box hits both alike (./../parquet/runner.py explains why
remembered numbers are noise). DuckDB loads the same JSONL into `bluesky (j JSON)` first.

Every Opteryx answer is checked against DuckDB's before any time is reported: a
wrong answer fails the run instead of posting a time.

Usage:
    python tests/performance/jsonbench/variant/runner.py --size 10
    python tests/performance/jsonbench/variant/runner.py --sources a.jsonl --parquet DIR
"""

from __future__ import annotations

import argparse
import datetime
import importlib.util
import json
import os
import platform
import sys
import time

_HERE = os.path.dirname(os.path.abspath(__file__))
_REPO_ROOT = os.path.abspath(os.path.join(_HERE, "..", "..", "..", ".."))
sys.path.insert(0, _REPO_ROOT)

_spec = importlib.util.spec_from_file_location("variant_load", os.path.join(_HERE, "load.py"))
load = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(load)

_POSTS = "data->'commit'->>'collection' = 'app.bsky.feed.post'"
_CREATES = "data->>'kind' = 'commit' AND data->'commit'->>'operation' = 'create'"
_TIME = "CAST(data->>'time_us' AS INT64)"

OPTERYX = [
    """SELECT data->'commit'->>'collection' AS event, COUNT(*) AS count
       FROM {t} GROUP BY event ORDER BY count DESC""",
    f"""SELECT data->'commit'->>'collection' AS event, COUNT(*) AS count,
              COUNT(DISTINCT data->>'did') AS users
       FROM {{t}} WHERE {_CREATES} GROUP BY event ORDER BY count DESC""",
    f"""SELECT data->'commit'->>'collection' AS event,
              EXTRACT(HOUR FROM CAST({_TIME} AS TIMESTAMP[us])) AS hour_of_day, COUNT(*) AS count
       FROM {{t}}
       WHERE {_CREATES} AND data->'commit'->>'collection'
             IN ('app.bsky.feed.post', 'app.bsky.feed.repost', 'app.bsky.feed.like')
       GROUP BY event, hour_of_day ORDER BY hour_of_day, event""",
    f"""SELECT data->>'did' AS user_id, MIN({_TIME}) AS first_post_us
       FROM {{t}} WHERE {_CREATES} AND {_POSTS}
       GROUP BY user_id ORDER BY first_post_us ASC LIMIT 3""",
    f"""SELECT data->>'did' AS user_id, (MAX({_TIME}) - MIN({_TIME})) / 1000 AS activity_span
       FROM {{t}} WHERE {_CREATES} AND {_POSTS}
       GROUP BY user_id ORDER BY activity_span DESC LIMIT 3""",
]

# Upstream duckdb/queries.sql, verbatim (as ../duckdb/runner.py).
DUCKDB = [
    "SELECT j->>'$.commit.collection' AS event,count() AS count FROM bluesky GROUP BY event ORDER BY count DESC;",
    "SELECT j->>'$.commit.collection' AS event,count() AS count,count(DISTINCT j->>'$.did') AS users FROM bluesky WHERE (j->>'$.kind' = 'commit') AND (j->>'$.commit.operation' = 'create') GROUP BY event ORDER BY count DESC;",
    "SELECT j->>'$.commit.collection' AS event,hour(TO_TIMESTAMP(CAST(j->>'$.time_us' AS BIGINT) / 1000000)) as hour_of_day,count() AS count FROM bluesky WHERE (j->>'$.kind' = 'commit') AND (j->>'$.commit.operation' = 'create') AND (j->>'$.commit.collection' in ['app.bsky.feed.post', 'app.bsky.feed.repost', 'app.bsky.feed.like']) GROUP BY event, hour_of_day ORDER BY hour_of_day, event;",
    "SELECT j->>'$.did'::String as user_id,TO_TIMESTAMP(CAST(MIN(j->>'$.time_us') AS BIGINT) / 1000000) AS first_post_date FROM bluesky WHERE (j->>'$.kind' = 'commit') AND (j->>'$.commit.operation' = 'create')   AND (j->>'$.commit.collection' = 'app.bsky.feed.post') GROUP BY user_id ORDER BY first_post_date ASC LIMIT 3;",
    "SELECT j->>'$.did'::String as user_id,date_diff('milliseconds', TO_TIMESTAMP(CAST(MIN(j->>'$.time_us') AS BIGINT) / 1000000),TO_TIMESTAMP(CAST(MAX(j->>'$.time_us') AS BIGINT) / 1000000)) AS activity_span FROM bluesky WHERE (j->>'$.kind' = 'commit') AND (j->>'$.commit.operation' = 'create') AND (j->>'$.commit.collection' = 'app.bsky.feed.post') GROUP BY user_id ORDER BY activity_span DESC LIMIT 3;",
]


def _opteryx(sql: str) -> list[tuple]:
    import opteryx

    rows: list[tuple] = []
    for morsel in opteryx.session().execute_to_morsels(sql):
        rows.extend(zip(*[morsel.column(c).to_pylist() for c in morsel.column_names]))
    return rows


def _same(q: int, ours: list[tuple], theirs: list[tuple]) -> bool:
    """Opteryx's answer against DuckDB's. Q1/Q2 order only by count, so ties may come in
    either order; Q4 compares DuckDB's timestamp as epoch microseconds; Q5's span is
    whole milliseconds on DuckDB's side (date_diff), exact microseconds / 1000 on ours."""
    if q in (0, 1):
        return sorted(ours, key=repr) == sorted(theirs, key=repr) and [r[1] for r in ours] == [r[1] for r in theirs]
    if q == 3:
        theirs = [(u, int(ts.timestamp() * 1_000_000)) for u, ts in theirs]
        return ours == theirs
    if q == 4:
        return len(ours) == len(theirs) and all(
            u == v and abs(a - b) < 1 for (u, a), (v, b) in zip(ours, theirs)
        )
    return ours == theirs


def main() -> int:
    parser = argparse.ArgumentParser(description="JSONBench over one VARIANT column, vs DuckDB")
    parser.add_argument("--size", type=int, default=10)
    parser.add_argument("--sources", nargs="+", help="JSONL files DuckDB loads instead of the shards")
    parser.add_argument("--parquet", help="Directory ./load.py wrote (default: variant/<size>shards)")
    parser.add_argument("--iterations", type=int, default=3)
    args = parser.parse_args()

    import duckdb

    sources = args.sources or load.shard_sources(args.size)
    parquet_dir = args.parquet or load.target_dir(args.size)
    table = f"READ_PARQUET('{parquet_dir}/*.parquet')"

    con = duckdb.connect()
    con.execute("SET TimeZone = 'UTC'")
    con.execute("CREATE TABLE bluesky (j JSON)")
    started = time.monotonic()
    for path in sources:
        # ignore_errors keeps a malformed line as a NULL row; we drop it. Both answer over
        # the same valid records (the NULLs would otherwise join Q1's NULL group).
        con.execute(
            "INSERT INTO bluesky SELECT * FROM read_ndjson_objects(?, ignore_errors=true, maximum_object_size=1048576000) WHERE json IS NOT NULL",
            [path],
        )
    duck_load = time.monotonic() - started
    duck_rows = con.execute("SELECT count(*) FROM bluesky").fetchone()[0]
    our_rows = _opteryx(f"SELECT COUNT(*) FROM {table}")[0][0]
    print(f"rows: Opteryx {our_rows:,}  DuckDB {duck_rows:,}  (DuckDB load {duck_load:.1f}s)")
    if our_rows != duck_rows:
        print("\033[31mrow counts differ\033[0m")
        return 1

    results = []
    wrong = 0
    print(f"{'query':<6}{'opteryx ms':>30}{'duckdb ms':>30}{'ratio':>8}")
    for q, (ours_sql, duck_sql) in enumerate(zip(OPTERYX, DUCKDB)):
        sql = ours_sql.format(t=table)
        o_ms, d_ms = [], []
        for _ in range(args.iterations):
            t0 = time.perf_counter()
            ours = _opteryx(sql)
            o_ms.append((time.perf_counter() - t0) * 1000)
            t0 = time.perf_counter()
            theirs = con.execute(duck_sql).fetchall()
            d_ms.append((time.perf_counter() - t0) * 1000)
        ok = _same(q, ours, theirs)
        wrong += not ok
        results.append([round(ms / 1000, 3) for ms in o_ms])
        mark = "" if ok else "  \033[31mWRONG\033[0m"
        print(
            f"Q{q + 1:<5}{' '.join(f'{m:8.0f}' for m in o_ms):>30}{' '.join(f'{m:8.0f}' for m in d_ms):>30}"
            f"{min(o_ms) / min(d_ms):8.2f}{mark}"
        )
        if not ok:
            print(f"  ours:   {ours[:5]}\n  duckdb: {theirs[:5]}")
    if wrong:
        print(f"\033[31m{wrong} query(ies) differ from DuckDB — no result written\033[0m")
        return 1

    if not args.sources:
        out = os.path.join(_HERE, f"results.local.{args.size}m.json")
        data_size = sum(os.path.getsize(os.path.join(parquet_dir, f)) for f in os.listdir(parquet_dir))
        with open(out, "w") as f:
            json.dump(
                {
                    "system": "Opteryx (Parquet, one shredded VARIANT column)",
                    "date": datetime.date.today().isoformat(),
                    "machine": f"{platform.node()} ({platform.machine()})",
                    "size_millions": args.size,
                    "rows_loaded": our_rows,
                    "data_size": data_size,
                    "result": results,
                },
                f,
                indent=2,
            )
        print(f"  {os.path.relpath(out, _REPO_ROOT)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
