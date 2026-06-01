#!/usr/bin/env python3
"""
GROUP BY phase-breakdown profiler — Phase 0 of GROUPBY_PERF_PLAN.md.

Runs every GROUP BY query in the ClickBench suite against the tiny dataset,
prints a per-phase time breakdown for each query, and ends with a phase-summary
table ranked by total time spent in each phase across all queries.

Usage:
    python dev/groupby_phase_profile.py
    python dev/groupby_phase_profile.py --dataset testdata.clickbench_tiny
    python dev/groupby_phase_profile.py --warmup 1 --runs 2

Telemetry keys extracted (all times in milliseconds):
    hash         — hash_keys() on group columns
    lookup       — CarcharIndex / ParviMap find-or-insert
    store_keys   — key storage for newly-seen groups
    grow         — collector buffer growth
    accumulate   — per-row value accumulation into collectors
    reconstruct  — key-column finalization (fixed / string / multi)
    build_morsel — assembling the output morsel
    slice_output — slicing output into CHUNK_SIZE chunks
    (finalize = reconstruct + build_morsel + slice_output overhead)
    eval_exprs   — GROUP BY expression pre-evaluation (outside the engine)
"""

from __future__ import annotations

import gc
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, ".."))

import argparse
import time

import opteryx

# ---------------------------------------------------------------------------
# Query corpus — GROUP BY queries only, pulled from the ClickBench STATEMENTS
# list. Indexed by their ClickBench query number (1-based).
# ---------------------------------------------------------------------------

_GROUP_BY_QUERIES = {
     8: "SELECT AdvEngineID, COUNT(*) FROM {DS} WHERE AdvEngineID <> 0 GROUP BY AdvEngineID ORDER BY COUNT(*) DESC",
     9: "SELECT RegionID, COUNT(DISTINCT UserID) AS u FROM {DS} GROUP BY RegionID ORDER BY u DESC LIMIT 10",
    10: "SELECT RegionID, SUM(AdvEngineID), COUNT(*) AS c, AVG(ResolutionWidth), COUNT(DISTINCT UserID) FROM {DS} GROUP BY RegionID ORDER BY c DESC LIMIT 10",
    11: "SELECT MobilePhoneModel, COUNT(DISTINCT UserID) AS u FROM {DS} WHERE MobilePhoneModel <> '' GROUP BY MobilePhoneModel ORDER BY u DESC LIMIT 10",
    12: "SELECT MobilePhone, MobilePhoneModel, COUNT(DISTINCT UserID) AS u FROM {DS} WHERE MobilePhoneModel <> '' GROUP BY MobilePhone, MobilePhoneModel ORDER BY u DESC LIMIT 10",
    13: "SELECT SearchPhrase, COUNT(*) AS c FROM {DS} WHERE SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY c DESC LIMIT 10",
    14: "SELECT SearchPhrase, COUNT(DISTINCT UserID) AS u FROM {DS} WHERE SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY u DESC LIMIT 10",
    15: "SELECT SearchEngineID, SearchPhrase, COUNT(*) AS c FROM {DS} WHERE SearchPhrase <> '' GROUP BY SearchEngineID, SearchPhrase ORDER BY c DESC LIMIT 10",
    16: "SELECT UserID, COUNT(*) FROM {DS} GROUP BY UserID ORDER BY COUNT(*) DESC LIMIT 10",
    17: "SELECT UserID, SearchPhrase, COUNT(*) FROM {DS} GROUP BY UserID, SearchPhrase ORDER BY COUNT(*) DESC LIMIT 10",
    18: "SELECT UserID, SearchPhrase, COUNT(*) FROM {DS} GROUP BY UserID, SearchPhrase LIMIT 10",
    19: "SELECT UserID, extract(minute FROM EventTime::TIMESTAMP[ms]) AS m, SearchPhrase, COUNT(*) FROM {DS} GROUP BY UserID, extract(minute FROM EventTime::TIMESTAMP[ms]), SearchPhrase ORDER BY COUNT(*) DESC LIMIT 10",
    22: "SELECT SearchPhrase, MIN(URL), COUNT(*) AS c FROM {DS} WHERE URL LIKE '%google%' AND SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY c DESC LIMIT 10",
    23: "SELECT SearchPhrase, MIN(URL), MIN(Title), COUNT(*) AS c, COUNT(DISTINCT UserID) FROM {DS} WHERE Title LIKE '%Google%' AND URL NOT LIKE '%.google.%' AND SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY c DESC LIMIT 10",
    28: "SELECT CounterID, AVG(length(URL)) AS l, COUNT(*) AS c FROM {DS} WHERE URL <> '' GROUP BY CounterID HAVING COUNT(*) > 100000 ORDER BY l DESC LIMIT 25",
    29: "SELECT REGEXP_REPLACE(Referer, b'^https?://(?:www\\.)?([^/]+)/.*$', r'\\1') AS k, AVG(length(Referer)) AS l, COUNT(*) AS c, MIN(Referer) FROM {DS} WHERE Referer <> '' GROUP BY REGEXP_REPLACE(Referer, b'^https?://(?:www\\.)?([^/]+)/.*$', r'\\1') HAVING COUNT(*) > 100000 ORDER BY l DESC LIMIT 25",
    31: "SELECT SearchEngineID, ClientIP, COUNT(*) AS c, SUM(IsRefresh), AVG(ResolutionWidth) FROM {DS} WHERE SearchPhrase <> '' GROUP BY SearchEngineID, ClientIP ORDER BY c DESC LIMIT 10",
    32: "SELECT WatchID, ClientIP, COUNT(*) AS c, SUM(IsRefresh), AVG(ResolutionWidth) FROM {DS} WHERE SearchPhrase <> '' GROUP BY WatchID, ClientIP ORDER BY c DESC LIMIT 10",
    33: "SELECT WatchID, ClientIP, COUNT(*) AS c, SUM(IsRefresh), AVG(ResolutionWidth) FROM {DS} GROUP BY WatchID, ClientIP ORDER BY c DESC LIMIT 10",
    34: "SELECT URL, COUNT(*) AS c FROM {DS} GROUP BY URL ORDER BY c DESC LIMIT 10",
    35: "SELECT 1, URL, COUNT(*) AS c FROM {DS} GROUP BY 1, URL ORDER BY c DESC LIMIT 10",
    36: "SELECT ClientIP, ClientIP - 1, ClientIP - 2, ClientIP - 3, COUNT(*) AS c FROM {DS} GROUP BY ClientIP, ClientIP - 1, ClientIP - 2, ClientIP - 3 ORDER BY c DESC LIMIT 10",
    37: "SELECT URL, COUNT(*) AS PageViews FROM {DS} WHERE CounterID = 62 AND EventDate >= '2013-07-01'::DATE AND EventDate <= '2013-07-31'::DATE AND DontCountHits = 0 AND IsRefresh = 0 AND URL <> '' GROUP BY URL ORDER BY PageViews DESC LIMIT 10",
    38: "SELECT Title, COUNT(*) AS PageViews FROM {DS} WHERE CounterID = 62 AND EventDate >= '2013-07-01'::DATE AND EventDate <= '2013-07-31'::DATE AND DontCountHits = 0 AND IsRefresh = 0 AND Title <> '' GROUP BY Title ORDER BY PageViews DESC LIMIT 10",
    40: "SELECT TraficSourceID, SearchEngineID, AdvEngineID, CASE WHEN (SearchEngineID = 0 AND AdvEngineID = 0) THEN Referer ELSE '' END AS Src, URL AS Dst, COUNT(*) AS PageViews FROM {DS} WHERE CounterID = 62 AND EventDate >= '2013-07-01'::DATE AND EventDate <= '2013-07-31'::DATE AND IsRefresh = 0 GROUP BY TraficSourceID, SearchEngineID, AdvEngineID, CASE WHEN (SearchEngineID = 0 AND AdvEngineID = 0) THEN Referer ELSE '' END, URL ORDER BY PageViews DESC LIMIT 10 OFFSET 1000",
    41: "SELECT URLHash, EventDate, COUNT(*) AS PageViews FROM {DS} WHERE CounterID = 62 AND EventDate >= '2013-07-01'::DATE AND EventDate <= '2013-07-31'::DATE AND IsRefresh = 0 AND TraficSourceID IN (-1, 6) AND RefererHash = 3594120000172545465 GROUP BY URLHash, EventDate ORDER BY PageViews DESC LIMIT 10 OFFSET 100",
    42: "SELECT WindowClientWidth, WindowClientHeight, COUNT(*) AS PageViews FROM {DS} WHERE CounterID = 62 AND EventDate >= '2013-07-01'::DATE AND EventDate <= '2013-07-31'::DATE AND IsRefresh = 0 AND DontCountHits = 0 AND URLHash = 2868770270353813622 GROUP BY WindowClientWidth, WindowClientHeight ORDER BY PageViews DESC LIMIT 10 OFFSET 10000",
    43: "SELECT TRUNC(EventTime::TIMESTAMP[ms], 'minute') AS M, COUNT(*) AS PageViews FROM {DS} WHERE CounterID = 62 AND EventDate >= '2013-07-14'::DATE AND EventDate <= '2013-07-15'::DATE AND IsRefresh = 0 AND DontCountHits = 0 GROUP BY TRUNC(EventTime::TIMESTAMP[ms], 'minute') ORDER BY M LIMIT 10 OFFSET 1000",
}

# Phase keys (in display order) — these match the telemetry dict keys without
# the "time_aggregate_" prefix, plus "total_ms" for the wall-clock total.
_PHASES = [
    ("eval_exprs",    "eval_exprs"),
    ("hash",          "hash"),
    ("lookup",        "lookup"),
    ("store_keys",    "store_keys"),
    ("grow",          "grow"),
    ("accumulate",    "accumulate"),
    ("reconstruct",   "reconstruct"),
    ("build_morsel",  "build_morsel"),
    ("slice_output",  "slice_output"),
]

_AGG_PREFIX = "time_aggregate_"

# Map from display name → telemetry dict key (without prefix)
_PHASE_KEYS = {
    "eval_exprs":  "evaluations",
    "hash":        "hash",
    "lookup":      "lookup",
    "store_keys":  "store_keys",
    "grow":        "grow",
    "accumulate":  "accumulate",
    "reconstruct": "reconstruct",
    "build_morsel":"build_morsel",
    "slice_output":"slice_output",
}


def _run_query(sql: str) -> tuple[float, dict]:
    """Run SQL, drain morsels, return (wall_ms, sensors_dict).

    Aggregate phase timings live in the plan's per-node sensors (nanoseconds),
    not in session.telemetry (which only covers query-level planning/execution).
    We harvest them from session._plan.sensors() after draining all morsels.
    """
    gc.collect()
    session = opteryx.session()
    t0 = time.monotonic_ns()
    for morsel in session.execute_to_morsels(sql):
        pass
    wall_ms = (time.monotonic_ns() - t0) / 1e6
    # Merge all per-node sensor readings into a single flat dict.
    merged: dict = {}
    for node_readings in session._plan.sensors().values():
        merged.update(node_readings)
    session.close()
    return wall_ms, merged


def _extract_phases(sensors: dict) -> dict[str, float]:
    """Pull aggregate phase times (nanoseconds → ms) from the merged sensors dict."""
    out = {}
    for display, key in _PHASE_KEYS.items():
        full_key = _AGG_PREFIX + key
        val = sensors.get(full_key, 0)
        out[display] = val / 1e6  # ns → ms
    return out


def _bar(frac: float, width: int = 20) -> str:
    filled = round(frac * width)
    return "█" * filled + "░" * (width - filled)


def _print_query_result(
    qnum: int,
    wall_ms: float,
    phases: dict[str, float],
) -> None:
    agg_total = sum(phases.values())
    print(f"\n  Q{qnum:02d}  wall={wall_ms:.1f}ms  agg_total={agg_total:.1f}ms")
    print(f"  {'phase':<14} {'ms':>8}  {'%agg':>6}  bar")
    print(f"  {'-'*14} {'-'*8}  {'-'*6}  {'-'*20}")
    for display, _ in _PHASES:
        ms = phases[display]
        pct = (ms / agg_total * 100) if agg_total > 0 else 0.0
        bar = _bar(pct / 100)
        print(f"  {display:<14} {ms:>8.2f}  {pct:>5.1f}%  {bar}")


def _print_summary(results: list[tuple[int, float, dict]]) -> None:
    print("\n\n" + "=" * 70)
    print("  PHASE SUMMARY — total ms across all GROUP BY queries")
    print("=" * 70)

    phase_totals: dict[str, float] = {d: 0.0 for d, _ in _PHASES}
    total_wall = 0.0
    for _, wall_ms, phases in results:
        total_wall += wall_ms
        for d, _ in _PHASES:
            phase_totals[d] += phases[d]

    grand_agg = sum(phase_totals.values())
    ranked = sorted(phase_totals.items(), key=lambda x: -x[1])

    print(f"\n  total wall time : {total_wall:.1f} ms")
    print(f"  total agg time  : {grand_agg:.1f} ms")
    print(f"\n  {'phase':<14} {'total ms':>10}  {'% of agg':>9}  bar")
    print(f"  {'-'*14} {'-'*10}  {'-'*9}  {'-'*24}")
    for display, ms in ranked:
        pct = (ms / grand_agg * 100) if grand_agg > 0 else 0.0
        bar = _bar(pct / 100, width=24)
        print(f"  {display:<14} {ms:>10.2f}  {pct:>8.1f}%  {bar}")

    print()
    print("  Interpretation:")
    top = ranked[0][0] if ranked else "n/a"
    print(f"    dominant phase: {top} — fix this first")
    print("    phases < 1% are noise; ignore until the big ones shrink")
    print()


def main() -> None:
    parser = argparse.ArgumentParser(description="GROUP BY phase profiler")
    parser.add_argument(
        "--dataset",
        default="scratch.hits",
        help="Dataset to query (default: scratch.hits ~100M rows)",
    )
    parser.add_argument(
        "--warmup",
        type=int,
        default=1,
        help="Warmup runs per query (not measured, default: 1)",
    )
    parser.add_argument(
        "--runs",
        type=int,
        default=1,
        help="Measured runs per query — best is used (default: 1)",
    )
    parser.add_argument(
        "--queries",
        type=str,
        default=None,
        help="Comma-separated query numbers to run, e.g. --queries 9,14,17",
    )
    args = parser.parse_args()

    selected: set[int] | None = None
    if args.queries:
        selected = {int(q.strip()) for q in args.queries.split(",")}

    ds = args.dataset
    queries = {
        qnum: sql
        for qnum, sql in _GROUP_BY_QUERIES.items()
        if selected is None or qnum in selected
    }

    print(f"GROUP BY phase profiler  ({len(queries)} queries on {ds})")
    print(f"warmup={args.warmup}  runs={args.runs}")
    print("=" * 70)

    results: list[tuple[int, float, dict]] = []
    errors: list[tuple[int, str]] = []

    for qnum in sorted(queries):
        sql = queries[qnum].replace("{DS}", ds)

        # Warmup
        for _ in range(args.warmup):
            try:
                _run_query(sql)
            except Exception:
                pass

        # Measured run(s) — keep best wall time and its telemetry
        best_wall: float | None = None
        best_phases: dict | None = None

        for _ in range(args.runs):
            try:
                wall_ms, telem = _run_query(sql)
            except Exception as exc:
                errors.append((qnum, str(exc)))
                break
            phases = _extract_phases(telem)
            if best_wall is None or wall_ms < best_wall:
                best_wall = wall_ms
                best_phases = phases

        if best_wall is None or best_phases is None:
            # Already recorded in errors list; keep running
            continue

        _print_query_result(qnum, best_wall, best_phases)
        results.append((qnum, best_wall, best_phases))

    if results:
        _print_summary(results)

    if errors:
        print("ERRORS:")
        for qnum, msg in errors:
            print(f"  Q{qnum:02d}: {msg}")


if __name__ == "__main__":
    main()
