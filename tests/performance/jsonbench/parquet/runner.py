#!/usr/bin/env python3
"""
JSONBench, Parquet-backed: the same 5 queries against a LOADED columnar table.

What this measures, and why it is a different question from ../runner.py
-----------------------------------------------------------------------
The upstream JSONBench query column times an already-loaded table. ClickHouse,
Doris and StarRocks all shred each document into typed columnar subcolumns at
INSERT; at query time no document is parsed. ../runner.py measures the opposite
— a full scan-and-parse of raw NDJSON, every iteration, no persisted storage.
Both are real numbers; only this one is the same operation the leaderboard's
query column reports. See ./convert.py for the load side (and its cost).

Interleaved A/B, not two separate runs
--------------------------------------
Every query runs its JSONL form and its Parquet form ALTERNATELY, within one
process, and only the ratio of their per-query minima is trusted. This box
drifts ~30% within a session (thermal, and other work landing on it), which is
easily enough to invent or erase the entire effect if the two forms are timed in
separate runs. Do not "simplify" this into a Parquet-only runner and diff it
against a remembered `make jsonbench` number — that comparison is noise.

The Parquet queries are the SAME queries, with `commit ->> 'x'` replaced by the
`commit_x` column ./convert.py materialised at load. Nothing else differs: same
predicates, same grouping, same ordering, same planner, same native execution.
Row counts are asserted equal between the two forms per query, so a rewrite that
silently changed the answer fails the run rather than posting a fast time.

Usage:
    python tests/performance/jsonbench/parquet/runner.py --size 10
    python tests/performance/jsonbench/parquet/runner.py --size 10 --variant narrow
    python tests/performance/jsonbench/parquet/runner.py --size 10 --no-jsonl  # Parquet only
"""

from __future__ import annotations

import argparse
import gc
import glob
import importlib.util
import json
import os
import sys
import time
from typing import Callable

_HERE = os.path.dirname(os.path.abspath(__file__))
_JSONBENCH_DIR = os.path.abspath(os.path.join(_HERE, ".."))
_REPO_ROOT = os.path.abspath(os.path.join(_HERE, "..", "..", "..", ".."))
sys.path.insert(0, _REPO_ROOT)


def _load_module(name: str, path: str):
    """Load by explicit path — ../opteryx/ would otherwise shadow the real
    `opteryx` package (same trap ../runner.py documents)."""
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


_jsonl_runner = _load_module("_jsonbench_jsonl_runner", os.path.join(_JSONBENCH_DIR, "opteryx", "runner.py"))
_convert = _load_module("_jsonbench_convert", os.path.join(_HERE, "convert.py"))

_JSONL_DIR = os.path.join(_REPO_ROOT, "testdata", "_downloads", "jsonbench", "decompressed")
_DUCKDB_DIR = os.path.join(_JSONBENCH_DIR, "duckdb")


def _run(sql: str) -> list[tuple]:
    import opteryx

    session = opteryx.session()
    rows: list[tuple] = []
    for morsel in session.execute_to_morsels(sql):
        rows.extend(zip(*[morsel.column(c) for c in morsel.column_names]))
    return rows


def _from(glob_path: str) -> str:
    return f"READ_PARQUET('{glob_path}') AS bluesky"


def q1(glob_path: str) -> list[tuple]:
    return _run(f"""
        SELECT commit_collection AS collection, COUNT(*) AS n
        FROM {_from(glob_path)}
        GROUP BY collection
        ORDER BY n DESC
    """)


def q2(glob_path: str) -> list[tuple]:
    return _run(f"""
        SELECT commit_collection AS collection, COUNT(*) AS n, COUNT(DISTINCT did) AS users
        FROM {_from(glob_path)}
        WHERE kind = 'commit' AND commit_operation = 'create'
        GROUP BY collection
        ORDER BY n DESC
    """)


def q3(glob_path: str) -> list[tuple]:
    return _run(f"""
        SELECT commit_collection AS collection,
               EXTRACT(HOUR FROM CAST(time_us AS TIMESTAMP[us])) AS hour_of_day,
               COUNT(*) AS n
        FROM {_from(glob_path)}
        WHERE kind = 'commit'
          AND commit_operation = 'create'
          AND commit_collection IN ('app.bsky.feed.post', 'app.bsky.feed.repost', 'app.bsky.feed.like')
        GROUP BY collection, hour_of_day
        ORDER BY hour_of_day, collection
    """)


def q4(glob_path: str) -> list[tuple]:
    return _run(f"""
        SELECT did, MIN(time_us) AS first_ts
        FROM {_from(glob_path)}
        WHERE kind = 'commit' AND commit_operation = 'create' AND commit_collection = 'app.bsky.feed.post'
        GROUP BY did
        ORDER BY first_ts ASC
        LIMIT 3
    """)


def q5(glob_path: str) -> list[tuple]:
    return _run(f"""
        SELECT did, (MAX(time_us) - MIN(time_us)) / 1000.0 AS span_ms
        FROM {_from(glob_path)}
        WHERE kind = 'commit' AND commit_operation = 'create' AND commit_collection = 'app.bsky.feed.post'
        GROUP BY did
        ORDER BY span_ms DESC
        LIMIT 3
    """)


QUERIES: list[tuple[str, Callable[[str], list]]] = [
    ("Q1", q1),
    ("Q2", q2),
    ("Q3", q3),
    ("Q4", q4),
    ("Q5", q5),
]


def _duckdb_baseline(size: int) -> tuple[dict[str, float], str | None]:
    path = os.path.join(_DUCKDB_DIR, f"results.local.{size}m.json")
    if not os.path.exists(path):
        return {}, None
    with open(path) as f:
        data = json.load(f)
    by_name: dict[str, float] = {}
    for ix, runs in enumerate(data.get("result", []), start=1):
        valid = [t for t in runs if t is not None]
        if valid:
            by_name[f"Q{ix}"] = min(valid) * 1000.0
    return by_name, data.get("machine")


def _time(fn, path) -> tuple[float, int]:
    gc.collect()
    t0 = time.monotonic_ns()
    rows = fn(path)
    return (time.monotonic_ns() - t0) / 1e6, len(rows)


_DIM = "\033[2m"
_OFF = "\033[0m"
_GREEN = "\033[38;2;26;185;67m"
_PINK = "\033[38;2;255;121;198m"


def _speedup(jsonl_ms: float, parquet_ms: float) -> str:
    ratio = jsonl_ms / parquet_ms
    colour = _GREEN if ratio >= 1.0 else _PINK
    return f"{colour}{ratio:5.1f}x{_OFF}"


def main() -> int:
    parser = argparse.ArgumentParser(description="JSONBench against loaded Parquet, A/B vs raw JSONL")
    parser.add_argument("--size", type=int, default=10, choices=(1, 10, 100))
    parser.add_argument("--variant", choices=_convert.VARIANTS, default="full")
    parser.add_argument("--iterations", type=int, default=3, help="Warm iterations per form per query")
    parser.add_argument("--no-jsonl", action="store_true", help="Skip the JSONL A/B leg")
    args = parser.parse_args()

    parquet_dir = _convert.target_dir(args.size, args.variant)
    parquet_files = sorted(glob.glob(os.path.join(parquet_dir, "*.parquet")))
    if len(parquet_files) < args.size:
        print(
            f"ERROR: expected {args.size} Parquet shard(s) in {os.path.relpath(parquet_dir, _REPO_ROOT)}, "
            f"found {len(parquet_files)}\n"
            f"       run: python {os.path.relpath(__file__, _REPO_ROOT)}/../convert.py "
            f"--size {args.size} --variant {args.variant}"
        )
        return 1
    parquet_glob = os.path.join(parquet_dir, "*.parquet")

    jsonl_paths = sorted(glob.glob(os.path.join(_JSONL_DIR, "file_*.jsonl")))[: args.size]
    run_jsonl = not args.no_jsonl and len(jsonl_paths) == args.size
    jsonl_glob = _jsonl_runner.shard_glob(jsonl_paths) if run_jsonl else None

    duckdb_min, duckdb_machine = _duckdb_baseline(args.size)

    import opteryx

    jsonl_bytes = sum(os.path.getsize(p) for p in jsonl_paths)
    parquet_bytes = sum(os.path.getsize(p) for p in parquet_files)

    print()
    print(f"\033[1mJSONBENCH — LOADED PARQUET vs RAW JSONL\033[0m   opteryx {opteryx.__version__}")
    print(f"  Dataset     Bluesky, {args.size}m rows")
    print(f"  JSONL       {jsonl_bytes / 1e9:.2f}GB raw NDJSON, parsed every query")
    print(f"  Parquet     {parquet_bytes / 1e9:.2f}GB columnar ({args.variant}), parsed once at load")
    print(f"  Iterations  {args.iterations} warm runs per form, INTERLEAVED per query")
    if duckdb_min:
        print(f"  DuckDB      {duckdb_machine} (loaded table, load time excluded)")
    print()

    header = f"  {'Query':<6} {'Parquet':>10} {'JSONL':>10} {'speedup':>9} {'DuckDB':>10}   rows"
    print(f"{_DIM}{header}{_OFF}")
    print(f"{_DIM}  {'─' * 62}{_OFF}")

    totals = {"parquet": 0.0, "jsonl": 0.0, "duckdb": 0.0}
    mismatches: list[str] = []

    for (name, pq_fn), (_, jl_fn) in zip(QUERIES, _jsonl_runner.QUERIES):
        pq_times: list[float] = []
        jl_times: list[float] = []
        pq_rows = jl_rows = None

        # Alternate the two forms so any drift over the run hits both equally.
        for _ in range(args.iterations):
            ms, pq_rows = _time(pq_fn, parquet_glob)
            pq_times.append(ms)
            if run_jsonl:
                ms, jl_rows = _time(jl_fn, jsonl_glob)
                jl_times.append(ms)

        pq_best = min(pq_times)
        totals["parquet"] += pq_best

        if run_jsonl and jl_rows != pq_rows:
            mismatches.append(f"{name}: Parquet returned {pq_rows} rows, JSONL returned {jl_rows}")

        jl_cell = f"{min(jl_times):9.0f}ms" if jl_times else f"{'—':>11}"
        speed_cell = _speedup(min(jl_times), pq_best) if jl_times else f"{'—':>9}"
        if jl_times:
            totals["jsonl"] += min(jl_times)

        d_ms = duckdb_min.get(name)
        d_cell = f"{d_ms:9.0f}ms" if d_ms is not None else f"{'—':>11}"
        if d_ms is not None:
            totals["duckdb"] += d_ms

        print(f"  {name:<6} {pq_best:8.0f}ms {jl_cell} {speed_cell} {d_cell}   {pq_rows}")

    print(f"{_DIM}  {'─' * 62}{_OFF}")
    jl_total = f"{totals['jsonl']:9.0f}ms" if totals["jsonl"] else f"{'—':>11}"
    speed_total = _speedup(totals["jsonl"], totals["parquet"]) if totals["jsonl"] else f"{'—':>9}"
    d_total = f"{totals['duckdb']:9.0f}ms" if totals["duckdb"] else f"{'—':>11}"
    print(f"  {'TOTAL':<6} {totals['parquet']:8.0f}ms {jl_total} {speed_total} {d_total}")
    print()

    if totals["duckdb"]:
        print(f"  vs DuckDB   Parquet {totals['parquet'] / totals['duckdb']:.2f}x", end="")
        if totals["jsonl"]:
            print(f"   JSONL {totals['jsonl'] / totals['duckdb']:.2f}x", end="")
        print()

    if mismatches:
        print(f"\n{_PINK}ROW COUNT MISMATCH — the Parquet rewrite is not the same query:{_OFF}")
        for line in mismatches:
            print(f"  {line}")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
