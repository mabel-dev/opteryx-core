"""
JSONBench queries against Opteryx SQL, via READ_JSONL(path) + the `->`/`->>` JSON
extraction operators (opteryx/planner/binder/operator_map.py; see
docs/json_variant_type_plan.md for the VARIANT type these operate on).

This is the real thing the older `../rugo/runner.py` hand-written scan-and-aggregate
existed to approximate: as of this file, READ_JSONL supports the VARIANT (nested
object) and ARRAY column types the Bluesky dataset's `commit` field needs, so the
5 upstream JSONBench queries can be written as actual SQL and run through the same
planner/optimizer/native-execution path as every other Opteryx query -- filtering,
grouping, and JSON-key extraction are all native (no per-row Python), not just the
extraction step `rugo/runner.py` already pushed into a native kernel by hand.

The queries read the REAL, UNMODIFIED downloaded shards. Nothing here rewrites,
cleans, or pre-filters the dataset -- an earlier version of this file did (the
Bluesky dump contains genuinely malformed records, see below), which meant the
benchmark was quietly measuring a doctored copy of the data rather than the data.
That workaround is gone: rugo's JSONL parser now handles malformed records itself
(drops the bad record, counts it, keeps going), so the benchmark can point straight
at what was downloaded.

Multiple shards, one relation
------------------------------
DuckDB's baseline loads every shard into a single `bluesky` table. The equivalent
here is one `READ_JSONL('<dir>/*.jsonl')` over a glob. Globbing measured ~2x faster
than the `UNION ALL`-per-shard form this file used first -- one scan node over N
files, rather than N scan nodes the planner then has to union together.

A glob straight at the shared `decompressed/` directory would be wrong, though:
JSONBench sizes are prefix-nested (1m's shard IS 10m's first shard IS 100m's
first shard), so that directory accumulates every shard any past run fetched, and
`--size 1` after a prior `--size 10` run would silently read all 10 while
reporting "1m rows". `shard_glob` below scopes the glob to exactly this run's
shard set via a directory of hard links, so the glob physically cannot over-match.

Known upstream data-quality defect (see ../README.md's "Known data-quality defect"
section): at 10m-row scale the Bluesky dump contains a few records with a raw,
unescaped control character (a newline) inside a nested string field, splitting one
JSON record across two physical lines -- genuinely invalid JSON, confirmed
independently by orjson and by DuckDB's own loader. rugo's parser detects these
(rugo/src/jsonl/core/interpreter.cpp), drops the affected record, resyncs at the
next physical line boundary, and reports the count via `malformed_count` -- so the
row counts here match orjson's own valid-line count exactly (999,998 of 1,000,000
on each of shards 5/6/7 at the 10m size). DuckDB's baseline reaches the same place
from the other direction with `read_ndjson_objects(..., ignore_errors=true)`.
"""

from __future__ import annotations

import os
import shutil
import sys
from typing import Callable, Iterable

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..", "..")))

import opteryx  # noqa: E402

_GLOB_DIR_NAME = "_by_size_for_sql"


def shard_glob(paths: Iterable[str]) -> str:
    """Return a glob path matching exactly `paths` and nothing else.

    Builds `<shard-dir>/_by_size_for_sql/<n>shards/` as a directory of HARD LINKS to
    the original, unmodified shard files (see module docstring for why a glob at
    the shared decompressed/ directory would silently over-match across --size
    values). Hard links, not copies: the same inode, so the bytes read are literally
    the downloaded bytes, nothing duplicated (~5GB saved at the 10m size) and nothing
    altered. The directory is rebuilt from scratch on every call so it can never
    carry a stale shard over from a differently-sized past run.

    Hard links rather than SYMlinks specifically: opteryx's native directory lister
    (opteryx/compiled/io/disk_reader.list_files_info, behind
    LocalFileSystem.list_files -> _resolve_glob_files) reports only regular files
    and silently skips symlinks, so a symlinked shard would vanish from the glob
    with no error -- verified directly. A hard link to a regular file IS a regular
    file, so it lists normally. (That the lister silently drops symlinked files is
    arguably a wart in READ_JSONL/READ_PARQUET globbing in its own right, but it is
    out of scope here and not something this benchmark should paper over quietly.)

    Grouped by shard COUNT rather than a caller-supplied size label -- JSONBench's
    three sizes (1/10/100 shards) already have distinct, unambiguous counts, so
    there is no label for a caller to get wrong.
    """
    paths = list(paths)
    if not paths:
        raise ValueError("shard_glob: paths must be non-empty")

    group_dir = os.path.join(os.path.dirname(paths[0]), _GLOB_DIR_NAME, f"{len(paths)}shards")
    if os.path.isdir(group_dir):
        shutil.rmtree(group_dir)
    os.makedirs(group_dir)
    for path in paths:
        os.link(os.path.abspath(path), os.path.join(group_dir, os.path.basename(path)))

    return os.path.join(group_dir, "*.jsonl")


def _from(glob_path: str) -> str:
    """The shared FROM clause: one glob scan over this run's shards.

    `ignore_errors => true` (rugo's fail_on_error=False) is what makes a malformed
    record get DROPPED and counted rather than aborting the query -- see the module
    docstring's data-quality note. It is set here, in one place, rather than repeated
    across the five queries where it could silently drift out of sync between them.
    Matches DuckDB's baseline, which loads the same dump with
    `read_ndjson_objects(..., ignore_errors=true)` for the identical reason.
    """
    return f"READ_JSONL('{glob_path}', ignore_errors => true) AS bluesky"


def _run(sql: str) -> list[tuple]:
    session = opteryx.session()
    rows: list[tuple] = []
    for morsel in session.execute_to_morsels(sql):
        rows.extend(zip(*[morsel.column(c) for c in morsel.column_names]))
    return rows


def q1_events_by_collection(glob_path: str) -> list[tuple]:
    """SELECT commit.collection, count(*) FROM bluesky GROUP BY 1 ORDER BY 2 DESC"""
    return _run(f"""
        SELECT commit ->> 'collection' AS collection, COUNT(*) AS n
        FROM {_from(glob_path)}
        GROUP BY collection
        ORDER BY n DESC
    """)


def q2_creates_by_collection(glob_path: str) -> list[tuple]:
    """SELECT collection, count(*), count(DISTINCT did) WHERE kind='commit' AND op='create' GROUP BY 1 ORDER BY 2 DESC"""
    return _run(f"""
        SELECT commit ->> 'collection' AS collection, COUNT(*) AS n, COUNT(DISTINCT did) AS users
        FROM {_from(glob_path)}
        WHERE kind = 'commit' AND commit ->> 'operation' = 'create'
        GROUP BY collection
        ORDER BY n DESC
    """)


def q3_hourly_activity(glob_path: str) -> list[tuple]:
    """post/repost/like creates, grouped by (collection, hour_of_day), ordered by hour then collection

    hour_of_day is `EXTRACT(HOUR FROM CAST(time_us AS TIMESTAMP[us]))` -- time_us is
    epoch microseconds, TIMESTAMP[us] interprets an INT64 as exactly that with no
    timezone shift, so this is UTC hour-of-day, matching ../rugo/runner.py's own
    arithmetic (`(t // 1_000_000 // 3600) % 24`) and DuckDB's `hour(TO_TIMESTAMP(...))`.
    Confirmed to return identical rows to the raw-arithmetic form across all 10 shards;
    kept as EXTRACT/CAST because it reads as "hour of day" rather than needing a comment
    to justify the arithmetic -- NOT for performance, the two forms benchmarked within
    noise of each other (~2945ms vs ~2932ms over 3 runs at 10m rows): this query's cost
    is dominated by the `->>` JSON-key extraction and scan, not by integer divisions.
    """
    return _run(f"""
        SELECT commit ->> 'collection' AS collection,
               EXTRACT(HOUR FROM CAST(time_us AS TIMESTAMP[us])) AS hour_of_day,
               COUNT(*) AS n
        FROM {_from(glob_path)}
        WHERE kind = 'commit'
          AND commit ->> 'operation' = 'create'
          AND commit ->> 'collection' IN ('app.bsky.feed.post', 'app.bsky.feed.repost', 'app.bsky.feed.like')
        GROUP BY collection, hour_of_day
        ORDER BY hour_of_day, collection
    """)


def q4_first_post(glob_path: str) -> list[tuple]:
    """did, MIN(time_us) for app.bsky.feed.post creates, grouped by did, top 3 earliest"""
    return _run(f"""
        SELECT did, MIN(time_us) AS first_ts
        FROM {_from(glob_path)}
        WHERE kind = 'commit' AND commit ->> 'operation' = 'create' AND commit ->> 'collection' = 'app.bsky.feed.post'
        GROUP BY did
        ORDER BY first_ts ASC
        LIMIT 3
    """)


def q5_activity_span(glob_path: str) -> list[tuple]:
    """did, (MAX(time_us) - MIN(time_us)) in ms for app.bsky.feed.post creates, top 3 longest span"""
    return _run(f"""
        SELECT did, (MAX(time_us) - MIN(time_us)) / 1000.0 AS span_ms
        FROM {_from(glob_path)}
        WHERE kind = 'commit' AND commit ->> 'operation' = 'create' AND commit ->> 'collection' = 'app.bsky.feed.post'
        GROUP BY did
        ORDER BY span_ms DESC
        LIMIT 3
    """)


QUERIES: list[tuple[str, Callable[[str], list]]] = [
    ("Q1", q1_events_by_collection),
    ("Q2", q2_creates_by_collection),
    ("Q3", q3_hourly_activity),
    ("Q4", q4_first_post),
    ("Q5", q5_activity_span),
]
