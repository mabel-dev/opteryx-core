import enum
import os
import sys

import pytest

os.environ.pop("OPTERYX_DEBUG", None)

sys.path.insert(1, os.path.join(sys.path[0], "../../../.."))

from typing import Optional

import opteryx
from opteryx.utils.formatter import format_sql


class Dataset(enum.Enum):
    TINY = "testdata.clickbench_tiny"
    MID = "scratch.hits_mid"
    FULL_SPLIT = "scratch.hits"
    FULL_SPLIT_RUGO_262K = "scratch.hits_rugo_262k" # preferred
    FULL_SINGLE = "scratch.hits_single"
    FULL_SPLIT_SKENE = "scratch.hits_skene"


DATASET = Dataset.FULL_SPLIT_SKENE

# Format variant -> dataset. `--variant skene` runs the identical battery against
# the skene mirror (built by dev/parquet_to_skene.py) so the two formats are
# compared on the same queries, same machine, same iteration count.
#
# ⛔ Both entries pointed at FULL_SPLIT (parquet) until 2026-08-14, so
# `make clickbench-skene` built the skene mirror, announced "(skene)", and then
# benchmarked parquet. Every skene-vs-parquet figure produced before that date is
# parquet-vs-parquet and must not be quoted. The path assertion in
# `resolve_dataset_path` below is what stops a mis-wired variant recurring
# silently — a variant that cannot be located is a hard failure, never a
# fallback to another dataset.
VARIANT_DATASETS = {
    "": DATASET,
    "skene": Dataset.FULL_SPLIT_SKENE,
}

# Queries whose per-round spread exceeds this fraction of their own minimum are
# reported as UNSTABLE. An unstable query's minimum is not a usable signal: the
# machine moved under it, so a change measured against it is measuring the
# machine.
UNSTABLE_SPREAD = 0.45


def resolve_dataset_path(dataset: Dataset) -> str:
    """Map a Dataset's dotted relation name onto its on-disk directory.

    Raises rather than returning a sentinel: a benchmark that cannot find its
    dataset must stop, not fall through to whatever else is lying around. The
    repository root is derived from this file's location, not the working
    directory, so the answer does not change with where make was invoked from.
    """
    repo_root = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), "../../../.."))
    path = os.path.join(repo_root, *dataset.value.split("."))
    if not os.path.isdir(path):
        raise FileNotFoundError(
            f"dataset {dataset.name} ({dataset.value}) resolves to {path}, which does not exist"
        )
    entries = os.listdir(path)
    if not entries:
        raise FileNotFoundError(
            f"dataset {dataset.name} ({dataset.value}) resolves to {path}, which is empty"
        )
    return path

# fmt:off
STATEMENTS = [

        ("/* 01 */ SELECT COUNT(*) FROM {DATASET};", None),
        ("/* 02 */ SELECT COUNT(*) FROM {DATASET} WHERE AdvEngineID <> 0;", None),
        ("/* 03 */ SELECT SUM(AdvEngineID), COUNT(*), AVG(ResolutionWidth) FROM {DATASET};", None),
        ("/* 04 */ SELECT AVG(UserID) FROM {DATASET};", None),
        ("/* 05 */ SELECT COUNT(DISTINCT UserID) FROM {DATASET};", None),
        ("/* 06 */ SELECT COUNT(DISTINCT SearchPhrase) FROM {DATASET};", None),
        ("/* 07 */ SELECT MIN(EventDate), MAX(EventDate) FROM {DATASET};", None),
        ("/* 08 */ SELECT AdvEngineID, COUNT(*) FROM {DATASET} WHERE AdvEngineID <> 0 GROUP BY AdvEngineID ORDER BY COUNT(*) DESC;", None),
        ("/* 09 */ SELECT RegionID, COUNT(DISTINCT UserID) AS u FROM {DATASET} GROUP BY RegionID ORDER BY u DESC LIMIT 10;", None),
        ("/* 10 */ SELECT RegionID, SUM(AdvEngineID), COUNT(*) AS c, AVG(ResolutionWidth), COUNT(DISTINCT UserID) FROM {DATASET} GROUP BY RegionID ORDER BY c DESC LIMIT 10;", None),
        ("/* 11 */ SELECT MobilePhoneModel, COUNT(DISTINCT UserID) AS u FROM {DATASET} WHERE MobilePhoneModel <> '' GROUP BY MobilePhoneModel ORDER BY u DESC LIMIT 10;", None),
        ("/* 12 */ SELECT MobilePhone, MobilePhoneModel, COUNT(DISTINCT UserID) AS u FROM {DATASET} WHERE MobilePhoneModel <> '' GROUP BY MobilePhone, MobilePhoneModel ORDER BY u DESC LIMIT 10;", None),
        ("/* 13 */ SELECT SearchPhrase, COUNT(*) AS c FROM {DATASET} WHERE SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY c DESC LIMIT 10;", None),
        ("/* 14 */ SELECT SearchPhrase, COUNT(DISTINCT UserID) AS u FROM {DATASET} WHERE SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY u DESC LIMIT 10;", None),
        ("/* 15 */ SELECT SearchEngineID, SearchPhrase, COUNT(*) AS c FROM {DATASET} WHERE SearchPhrase <> '' GROUP BY SearchEngineID, SearchPhrase ORDER BY c DESC LIMIT 10;", None),
        ("/* 16 */ SELECT UserID, COUNT(*) FROM {DATASET} GROUP BY UserID ORDER BY COUNT(*) DESC LIMIT 10;", None),
        ("/* 17 */ SELECT UserID, SearchPhrase, COUNT(*) FROM {DATASET} GROUP BY UserID, SearchPhrase ORDER BY COUNT(*) DESC LIMIT 10;", None),
        ("/* 18 */ SELECT UserID, SearchPhrase, COUNT(*) FROM {DATASET} GROUP BY UserID, SearchPhrase LIMIT 10;", None),
        ("/* 19 */ SELECT UserID, extract(minute FROM EventTime::TIMESTAMP[s]) AS m, SearchPhrase, COUNT(*) FROM {DATASET} GROUP BY UserID, extract(minute FROM EventTime::TIMESTAMP[s]), SearchPhrase ORDER BY COUNT(*) DESC LIMIT 10;", None),
        ("/* 20 */ SELECT UserID FROM {DATASET} WHERE UserID = 435090932899640449;", None),
        ("/* 21 */ SELECT COUNT(*) FROM {DATASET} WHERE URL LIKE '%google%';", None),
        ("/* 22 */ SELECT SearchPhrase, MIN(URL), COUNT(*) AS c FROM {DATASET} WHERE URL LIKE '%google%' AND SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY c DESC LIMIT 10;", None),
        ("/* 23 */ SELECT SearchPhrase, MIN(URL), MIN(Title), COUNT(*) AS c, COUNT(DISTINCT UserID) FROM {DATASET} WHERE Title LIKE '%Google%' AND URL NOT LIKE '%.google.%' AND SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY c DESC LIMIT 10;", None),
        ("/* 24 */ SELECT * FROM {DATASET} WHERE URL LIKE '%google%' ORDER BY EventTime LIMIT 10;", None),
        ("/* 25 */ SELECT SearchPhrase FROM {DATASET} WHERE SearchPhrase <> '' ORDER BY EventTime LIMIT 10;", None),
        ("/* 26 */ SELECT SearchPhrase FROM {DATASET} WHERE SearchPhrase <> '' ORDER BY SearchPhrase LIMIT 10;", None),
        ("/* 27 */ SELECT SearchPhrase FROM {DATASET} WHERE SearchPhrase <> '' ORDER BY EventTime, SearchPhrase LIMIT 10;", None),
        ("/* 28 */ SELECT CounterID, AVG(length(URL)) AS l, COUNT(*) AS c FROM {DATASET} WHERE URL <> '' GROUP BY CounterID HAVING COUNT(*) > 100000 ORDER BY l DESC LIMIT 25;", None),
        ("/* 29 */ SELECT REGEXP_REPLACE(Referer, b'^https?://(?:www\\.)?([^/]+)/.*$', r'\\1') AS k, AVG(length(Referer)) AS l, COUNT(*) AS c, MIN(Referer) FROM {DATASET} WHERE Referer <> '' GROUP BY REGEXP_REPLACE(Referer, b'^https?://(?:www\\.)?([^/]+)/.*$', r'\\1') HAVING COUNT(*) > 100000 ORDER BY l DESC LIMIT 25;", None),
        ("/* 30 */ SELECT SUM(ResolutionWidth), SUM(ResolutionWidth + 1), SUM(ResolutionWidth + 2), SUM(ResolutionWidth + 3), SUM(ResolutionWidth + 4), SUM(ResolutionWidth + 5), SUM(ResolutionWidth + 6), SUM(ResolutionWidth + 7), SUM(ResolutionWidth + 8), SUM(ResolutionWidth + 9), SUM(ResolutionWidth + 10), SUM(ResolutionWidth + 11), SUM(ResolutionWidth + 12), SUM(ResolutionWidth + 13), SUM(ResolutionWidth + 14), SUM(ResolutionWidth + 15), SUM(ResolutionWidth + 16), SUM(ResolutionWidth + 17), SUM(ResolutionWidth + 18), SUM(ResolutionWidth + 19), SUM(ResolutionWidth + 20), SUM(ResolutionWidth + 21), SUM(ResolutionWidth + 22), SUM(ResolutionWidth + 23), SUM(ResolutionWidth + 24), SUM(ResolutionWidth + 25), SUM(ResolutionWidth + 26), SUM(ResolutionWidth + 27), SUM(ResolutionWidth + 28), SUM(ResolutionWidth + 29), SUM(ResolutionWidth + 30), SUM(ResolutionWidth + 31), SUM(ResolutionWidth + 32), SUM(ResolutionWidth + 33), SUM(ResolutionWidth + 34), SUM(ResolutionWidth + 35), SUM(ResolutionWidth + 36), SUM(ResolutionWidth + 37), SUM(ResolutionWidth + 38), SUM(ResolutionWidth + 39), SUM(ResolutionWidth + 40), SUM(ResolutionWidth + 41), SUM(ResolutionWidth + 42), SUM(ResolutionWidth + 43), SUM(ResolutionWidth + 44), SUM(ResolutionWidth + 45), SUM(ResolutionWidth + 46), SUM(ResolutionWidth + 47), SUM(ResolutionWidth + 48), SUM(ResolutionWidth + 49), SUM(ResolutionWidth + 50), SUM(ResolutionWidth + 51), SUM(ResolutionWidth + 52), SUM(ResolutionWidth + 53), SUM(ResolutionWidth + 54), SUM(ResolutionWidth + 55), SUM(ResolutionWidth + 56), SUM(ResolutionWidth + 57), SUM(ResolutionWidth + 58), SUM(ResolutionWidth + 59), SUM(ResolutionWidth + 60), SUM(ResolutionWidth + 61), SUM(ResolutionWidth + 62), SUM(ResolutionWidth + 63), SUM(ResolutionWidth + 64), SUM(ResolutionWidth + 65), SUM(ResolutionWidth + 66), SUM(ResolutionWidth + 67), SUM(ResolutionWidth + 68), SUM(ResolutionWidth + 69), SUM(ResolutionWidth + 70), SUM(ResolutionWidth + 71), SUM(ResolutionWidth + 72), SUM(ResolutionWidth + 73), SUM(ResolutionWidth + 74), SUM(ResolutionWidth + 75), SUM(ResolutionWidth + 76), SUM(ResolutionWidth + 77), SUM(ResolutionWidth + 78), SUM(ResolutionWidth + 79), SUM(ResolutionWidth + 80), SUM(ResolutionWidth + 81), SUM(ResolutionWidth + 82), SUM(ResolutionWidth + 83), SUM(ResolutionWidth + 84), SUM(ResolutionWidth + 85), SUM(ResolutionWidth + 86), SUM(ResolutionWidth + 87), SUM(ResolutionWidth + 88), SUM(ResolutionWidth + 89) FROM {DATASET};", None),
        ("/* 31 */ SELECT SearchEngineID, ClientIP, COUNT(*) AS c, SUM(IsRefresh), AVG(ResolutionWidth) FROM {DATASET} WHERE SearchPhrase <> '' GROUP BY SearchEngineID, ClientIP ORDER BY c DESC LIMIT 10;", None),
        ("/* 32 */ SELECT WatchID, ClientIP, COUNT(*) AS c, SUM(IsRefresh), AVG(ResolutionWidth) FROM {DATASET} WHERE SearchPhrase <> '' GROUP BY WatchID, ClientIP ORDER BY c DESC LIMIT 10;", None),
        ("/* 33 */ SELECT WatchID, ClientIP, COUNT(*) AS c, SUM(IsRefresh), AVG(ResolutionWidth) FROM {DATASET} GROUP BY WatchID, ClientIP ORDER BY c DESC LIMIT 10;", None),
        ("/* 34 */ SELECT URL, COUNT(*) AS c FROM {DATASET} GROUP BY URL ORDER BY c DESC LIMIT 10;", None),
        ("/* 35 */ SELECT 1, URL, COUNT(*) AS c FROM {DATASET} GROUP BY 1, URL ORDER BY c DESC LIMIT 10;", None),
        ("/* 36 */ SELECT ClientIP, ClientIP - 1, ClientIP - 2, ClientIP - 3, COUNT(*) AS c FROM {DATASET} GROUP BY ClientIP, ClientIP - 1, ClientIP - 2, ClientIP - 3 ORDER BY c DESC LIMIT 10;", None),
        ("/* 37 */ SELECT URL, COUNT(*) AS PageViews FROM {DATASET} WHERE CounterID = 62 AND EventDate::DATE >= '2013-07-01'::DATE AND EventDate::DATE <= '2013-07-31'::DATE AND DontCountHits = 0 AND IsRefresh = 0 AND URL <> '' GROUP BY URL ORDER BY PageViews DESC LIMIT 10;", None),
        ("/* 38 */ SELECT Title, COUNT(*) AS PageViews FROM {DATASET} WHERE CounterID = 62 AND EventDate::DATE >= '2013-07-01'::DATE AND EventDate::DATE <= '2013-07-31'::DATE AND DontCountHits = 0 AND IsRefresh = 0 AND Title <> '' GROUP BY Title ORDER BY PageViews DESC LIMIT 10;", None),
        ("/* 39 */ SELECT URL, COUNT(*) AS PageViews FROM {DATASET} WHERE CounterID = 62 AND EventDate::DATE >= '2013-07-01'::DATE AND EventDate::DATE <= '2013-07-31'::DATE AND IsRefresh = 0 AND IsLink <> 0 AND IsDownload = 0 GROUP BY URL ORDER BY PageViews DESC LIMIT 10 OFFSET 1000;", None),
        ("/* 40 */ SELECT TraficSourceID, SearchEngineID, AdvEngineID, CASE WHEN (SearchEngineID = 0 AND AdvEngineID = 0) THEN Referer ELSE '' END AS Src, URL AS Dst, COUNT(*) AS PageViews FROM {DATASET} WHERE CounterID = 62 AND EventDate::DATE >= '2013-07-01'::DATE AND EventDate::DATE <= '2013-07-31'::DATE AND IsRefresh = 0 GROUP BY TraficSourceID, SearchEngineID, AdvEngineID, CASE WHEN (SearchEngineID = 0 AND AdvEngineID = 0) THEN Referer ELSE '' END, URL ORDER BY PageViews DESC LIMIT 10 OFFSET 1000;", None),
        ("/* 41 */ SELECT URLHash, EventDate, COUNT(*) AS PageViews FROM {DATASET} WHERE CounterID = 62 AND EventDate::DATE >= '2013-07-01'::DATE AND EventDate::DATE <= '2013-07-31'::DATE AND IsRefresh = 0 AND TraficSourceID IN (-1, 6) AND RefererHash = 3594120000172545465 GROUP BY URLHash, EventDate ORDER BY PageViews DESC LIMIT 10 OFFSET 100;", None),
        ("/* 42 */ SELECT WindowClientWidth, WindowClientHeight, COUNT(*) AS PageViews FROM {DATASET} WHERE CounterID = 62 AND EventDate::DATE >= '2013-07-01'::DATE AND EventDate::DATE <= '2013-07-31'::DATE AND IsRefresh = 0 AND DontCountHits = 0 AND URLHash = 2868770270353813622 GROUP BY WindowClientWidth, WindowClientHeight ORDER BY PageViews DESC LIMIT 10 OFFSET 10000;", None),
        ("/* 43 */ SELECT TRUNC(EventTime::TIMESTAMP[s], 'minute') AS M, COUNT(*) AS PageViews FROM {DATASET} WHERE CounterID = 62 AND EventDate::DATE >= '2013-07-14'::DATE AND EventDate::DATE <= '2013-07-15'::DATE AND IsRefresh = 0 AND DontCountHits = 0 GROUP BY TRUNC(EventTime::TIMESTAMP[s], 'minute') ORDER BY M LIMIT 10 OFFSET 1000;", None),
]
# fmt:on


@pytest.mark.parametrize("statement, exception", STATEMENTS)
def test_sql_battery(statement: str, exception: Optional[Exception]):
    """
    Test an battery of statements
    """

    from opteryx.exceptions import MissingSqlStatement, UnsupportedSyntaxError

    session = None
    try:
        # execute_to_morsels avoids Arrow conversion overhead
        session = opteryx.session()
        for _ in session.execute_to_morsels(statement):
            pass
        assert exception is None, (
            f"Exception {exception} not raised but expected\n{format_sql(statement)}"
        )
    except AssertionError as error:
        raise error
    except UnsupportedSyntaxError:
        # Draken flag is on; unsupported shapes are allowed to error
        pytest.skip("query not supported by Draken aggregator")
    except MissingSqlStatement:
        # comment-only or empty statements can be skipped
        pytest.skip("no actual SQL statement")
    except Exception as error:
        if not type(error) == exception:
            raise ValueError(
                f"{format_sql(statement)}\nQuery failed with error {type(error)} but error {exception} was expected"
            ) from error
    finally:
        if session is not None:
            session.close()


if __name__ == "__main__":  # pragma: no cover
    # Running in the IDE we do some formatting - it's not functional but helps when reading the outputs.

    import argparse
    import gc
    import json
    import statistics
    import subprocess
    import time

    parser = argparse.ArgumentParser(description="ClickBench Performance Test")
    parser.add_argument(
        "--iterations",
        type=int,
        default=3,
        help="Rounds of the full battery (default: 3)",
    )
    parser.add_argument(
        "--json",
        type=str,
        default=None,
        help="Write timings and provenance to this path (default: write nothing)",
    )
    parser.add_argument(
        "--duckdb-baseline",
        type=str,
        default=None,
        help="Path to DuckDB baseline results JSON (defaults to duckdb.local.json if present, then duckdb.c6a.4xlarge.json)",
    )
    parser.add_argument(
        "--profile",
        action="store_true",
        default=False,
        help="After the benchmark, run a tracing pass (EXPLAIN ANALYZE) and report "
        "per-operator self-time, per-query and aggregated across the suite.",
    )
    parser.add_argument(
        "--variant",
        type=str,
        default="",
        choices=sorted(VARIANT_DATASETS),
        help="Dataset format variant: `skene` runs against the skene mirror "
        "(default: the parquet dataset)",
    )
    args = parser.parse_args()

    DATASET = VARIANT_DATASETS[args.variant]
    # Hard-fails if the variant's dataset is absent or empty. `--variant skene`
    # silently ran against parquet for as long as the mapping was wrong; the only
    # defence against the next mis-wiring is refusing to run on a dataset we
    # cannot locate.
    dataset_path = resolve_dataset_path(DATASET)
    repo_root = os.path.abspath(
        os.path.join(os.path.dirname(os.path.abspath(__file__)), "../../../..")
    )

    # Resolve DuckDB baseline path: prefer local results if present
    _script_dir = os.path.dirname(os.path.abspath(__file__))
    _duckdb_dir = os.path.join(_script_dir, "..", "duckdb")
    _local = os.path.join(_duckdb_dir, "results.local.json")
    # _local = os.path.join(_duckdb_dir, "results.local.single_thread.json")
    _remote = os.path.join(_duckdb_dir, "results.c6a.4xlarge.json")
    if args.duckdb_baseline is None:
        if os.path.exists(_local):
            args.duckdb_baseline = _local
        else:
            args.duckdb_baseline = _remote

    # Load DuckDB baseline results
    duckdb_results = None
    duckdb_machine = None
    if os.path.exists(args.duckdb_baseline):
        with open(args.duckdb_baseline, "r") as f:
            duckdb_data = json.load(f)
            # Use the warm2 (second warm run) for comparison
            duckdb_results = [result[2] for result in duckdb_data.get("result", [])]
            duckdb_machine = duckdb_data.get("machine", args.duckdb_baseline)

    def format_ratio(opteryx_ms: float, duckdb_ms: float) -> str:
        """Format ratio with color coding based on performance."""
        ratio = opteryx_ms / duckdb_ms
        ratio_str = f"[{ratio:.2f}x]"

        # Color codes based on ratio thresholds
        if ratio <= 1.02:
            # Deep green: faster or parity than DuckDB
            return f"\033[1;38;2;34;197;94m{ratio_str}\033[0m"  # Pure bright green
        elif ratio <= 3.0:
            # Light green: within 300%
            return f"\033[38;2;72;209;204m{ratio_str}\033[0m"
        elif ratio <= 7.0:
            # Orange: 700% slower
            return f"\033[38;2;255;165;0m{ratio_str}\033[0m"
        else:
            # Red: 1000%+ slower
            return f"\033[38;2;255;69;69m{ratio_str}\033[0m"

    def git_fact(*argv: str) -> str:
        """Read-only git probe. Reports unavailability rather than hiding it."""
        proc = subprocess.run(["git", *argv], capture_output=True, text=True, cwd=repo_root)
        if proc.returncode != 0:
            return "unavailable"
        return proc.stdout.strip()

    # Provenance. A timing without this is not reproducible and must not be
    # quoted; collecting it is cheap and it is printed whether or not --json is
    # given. Build flags are read from the environment as a statement of what
    # this shell would build with — the compiled extensions do not record the
    # flags they were built with, so this is NOT proof of how they were built.
    git_sha = git_fact("rev-parse", "--short", "HEAD")
    git_dirty = git_fact("status", "--porcelain")
    provenance = {
        "opteryx_version": opteryx.__version__,
        "opteryx_build": opteryx.__build__,
        "git_sha": git_sha,
        "git_clean": (git_dirty == "") if git_dirty != "unavailable" else "unavailable",
        "python": sys.version.split()[0],
        "gil_enabled": sys._is_gil_enabled(),
        "cpu_count": os.cpu_count(),
        "dataset": DATASET.name,
        "dataset_relation": DATASET.value,
        "dataset_path": dataset_path,
        "dataset_entries": len(os.listdir(dataset_path)),
        "rounds": args.iterations,
        "preload": os.environ.get("DYLD_INSERT_LIBRARIES") or os.environ.get("LD_PRELOAD") or "none",
        "env_lto": os.environ.get("OPTERYX_ENABLE_LTO", "unset"),
        "env_pgo": os.environ.get("OPTERYX_ENABLE_PGO", "unset"),
    }

    start_suite = time.monotonic_ns()
    passed: int = 0
    failed: int = 0
    sum_min_ms: float = 0.0  # Σ per-query best (minimum) time — the headline total
    sum_duckdb_min_ms: float = 0.0  # Σ DuckDB baseline over the same queries
    failures = []

    print(f"{'=' * 88}")
    print("CLICKBENCH WARM PERFORMANCE BENCHMARK")
    print(f"{'=' * 88}")
    for key, value in provenance.items():
        print(f"  {key:<20} {value}")
    if duckdb_results:
        print(f"  {'duckdb_baseline':<20} {duckdb_machine} (warm2 times)")
        print(
            "  ⚠ the DuckDB column is ORIENTATION ONLY — a stored baseline from another\n"
            "    session and thermal state. It is not an interleaved A/B and must not be\n"
            "    quoted as one."
        )
    print(f"{'=' * 88}\n")

    # Cold start, outside the measured battery: the first query through the
    # process pays module-level lazy imports and first-touch allocation that no
    # subsequent query pays. Reported, never folded into a query's timing.
    print("Warming up (cold start)...")
    start = time.monotonic_ns()
    warm_session = opteryx.session()
    try:
        for _ in warm_session.execute_to_morsels(f"SELECT COUNT(*) FROM {DATASET.value};"):
            pass
        cold_time_ms = (time.monotonic_ns() - start) / 1e6
        print(f"Cold start: {cold_time_ms:.2f}ms\n")
    finally:
        warm_session.close()

    # Round-robin over the battery rather than all rounds of Q1, then all of Q2.
    # Thermal drift over a suite this long is real; running each query once per
    # round spreads it across every query instead of concentrating it on
    # whichever queries happened to run during the machine's ramp.
    print(f"RUNNING CLICKBENCH BATTERY OF {len(STATEMENTS)} QUERIES × {args.iterations} ROUNDS\n")
    timings: dict = {index: [] for index in range(len(STATEMENTS))}
    dead: set = set()

    for round_no in range(args.iterations):
        round_start = time.monotonic_ns()
        for index, (statement, _err) in enumerate(STATEMENTS):
            if index in dead:
                continue
            statement = statement.replace("{DATASET}", f"{DATASET.value}")
            query_num = f"Q{(index + 1):02d}"

            gc.collect()
            # Session construction is NOT engine work and is NOT on the clock.
            # It stays per-query so query isolation is unchanged from before.
            session = opteryx.session()
            try:
                start = time.monotonic_ns()
                for _ in session.execute_to_morsels(statement):
                    pass
                timings[index].append((time.monotonic_ns() - start) / 1e6)
            except Exception as error:
                # A query that cannot run is a failure with its error attached,
                # never a skip and never a fast time. Drop it from later rounds
                # so one broken query does not cost three rounds of noise.
                dead.add(index)
                timings[index] = []
                failures.append((statement, error))
                failed += 1
                print(f"  {query_num} FAILED (round {round_no + 1}): {type(error).__name__}: {str(error)[:70]}")
            finally:
                session.close()
        print(
            f"  round {round_no + 1}/{args.iterations} complete "
            f"({(time.monotonic_ns() - round_start) / 1e9:.2f}s)"
        )

    print()
    header = f"{'Query':<7} {'Min':>11} {'Median':>11} {'Max':>11} {'Spread':>9}  {'Rounds':<8}"
    if duckdb_results:
        header += " vs DuckDB"
    print(header)
    print("-" * (len(header) + 4))

    unstable = []
    for index in range(len(STATEMENTS)):
        query_num = f"Q{(index + 1):02d}"
        times = timings[index]
        if not times:
            print(f"{query_num:<7} {'FAILED':>11}")
            continue

        min_time = min(times)
        max_time = max(times)
        med_time = statistics.median(times)
        spread = (max_time - min_time) / min_time if min_time > 0 else 0.0
        spread_str = f"{spread * 100:>8.1f}%"
        if spread > UNSTABLE_SPREAD:
            unstable.append((query_num, spread))
            spread_str = f"\033[38;2;255;165;0m{spread * 100:>8.1f}%\033[0m"

        row = (
            f"{query_num:<7} {min_time:>9.2f}ms {med_time:>9.2f}ms {max_time:>9.2f}ms "
            f"{spread_str}  {len(times):<8}"
        )
        if duckdb_results and index < len(duckdb_results):
            duckdb_ms = duckdb_results[index] * 1000  # baseline JSON is in seconds
            row += f" {format_ratio(min_time, duckdb_ms)}"
            sum_duckdb_min_ms += duckdb_ms
        print(row)

        sum_min_ms += min_time
        passed += 1

    print("--- ✅ \033[0;32mdone\033[0m")

    if unstable:
        print(
            f"\n\033[38;2;255;165;0m{len(unstable)} UNSTABLE QUERIES\033[0m "
            f"(spread > {UNSTABLE_SPREAD * 100:.0f}% of their own minimum — the machine moved "
            f"under them, so their minimum is not a usable signal):"
        )
        if args.iterations < 3:
            print(
                "  ⚠ with 2 rounds, spread is one difference rather than a distribution and\n"
                "    over-reports. Re-run at --iterations 3 or more before acting on this list."
            )
        for query_num, spread in sorted(unstable, key=lambda x: -x[1]):
            print(f"  {query_num}  {spread * 100:.1f}%")

    if args.profile:
        import collections

        from opteryx.utils import mermaid as _mermaid
        from opteryx.operators._operators import get_groupby_telemetry
        from opteryx.operators._operators import reset_groupby_telemetry
        from rugo.rugo_native import get_cpp_telemetry
        from rugo.rugo_native import reset_cpp_telemetry

        # Real per-operator self-time only exists once the query has actually run:
        # the physical-plan Python objects never execute on the native engine (the
        # C++ engine does), so their own execution_time/sensors() counters stay
        # zero — mermaid._collect_node_stats() is what overlays the native engine's
        # per-identity readings (telemetry._reading["native_op_stats"]) back onto
        # the plan nodes; it's the same lookup EXPLAIN ANALYZE (TEXT format) uses
        # for its own self-time column. We drive each query via EXPLAIN ANALYZE in
        # a SEPARATE pass — the benchmark numbers above stay tracing-free and honest.
        print(f"\n{'=' * 80}")
        print("PER-OPERATOR PROFILE (tracing pass — EXPLAIN ANALYZE self-time)")
        print(f"{'=' * 80}\n")

        suite_self = collections.defaultdict(int)  # operator name -> self_time ns
        suite_plan_ns = 0
        suite_exec_ns = 0
        per_query_rows = []  # (query_num, exec_ms, top_operator, top_share)
        # Sub-phase breakdowns for the two operators that dominate suite_self above —
        # groupby_tel (src/cpp/engine/groupby_tel.hpp) and rugo_tel (the Parquet
        # decoder's existing phase accumulators). Both are global process-wide
        # atomics, so reset before / read after each query in this already-serial,
        # one-query-at-a-time tracing pass gives a clean per-query attribution.
        suite_gb_phase = collections.defaultdict(float)  # groupby phase -> seconds
        suite_pq_phase = collections.defaultdict(float)  # parquet decode phase -> seconds

        for index, (statement, _err) in enumerate(STATEMENTS):
            statement = statement.replace("{DATASET}", f"{DATASET.value}")
            query_num = f"Q{(index + 1):02d}"
            gc.collect()
            session = None
            reset_groupby_telemetry()
            reset_cpp_telemetry()
            try:
                session = opteryx.session()
                for _ in session.execute_to_morsels(f"EXPLAIN ANALYZE {statement}"):
                    pass

                node_stats_by_nid, _, _ = _mermaid._collect_node_stats(session._plan)
                op_self = collections.defaultdict(int)
                for nid in session._plan.nodes():
                    node = session._plan[nid]
                    if node is None or node.name in ("Explain", "Exit"):
                        continue
                    stat = node_stats_by_nid.get(nid)
                    if stat is None:
                        continue
                    op_self[node.name] += stat.get("self_time", 0)

                for name, t in op_self.items():
                    suite_self[name] += t
                suite_plan_ns += session._telemetry.time_planning
                suite_exec_ns += session._telemetry.time_executing

                gb_tel = get_groupby_telemetry()
                for phase in ("hash_s", "probe_s", "apply_s"):
                    suite_gb_phase[phase] += gb_tel[phase]
                pq_tel = get_cpp_telemetry()
                for phase, seconds in pq_tel.items():
                    if phase.endswith("_s"):
                        suite_pq_phase[phase] += seconds

                total = sum(op_self.values()) or 1
                top_name, top_t = max(op_self.items(), key=lambda x: x[1], default=("-", 0))
                per_query_rows.append(
                    (query_num, total / 1e6, top_name, 100.0 * top_t / total)
                )
            except opteryx.exceptions.MissingSqlStatement:
                continue
            except Exception as e:  # noqa: BLE001 - profiling is best-effort
                per_query_rows.append((query_num, 0.0, f"ERROR: {str(e)[:40]}", 0.0))
            finally:
                if session is not None:
                    session.close()

        # Per-query: where each query spends its time.
        print(f"{'Query':<8} {'ExecSelf':>12}   {'Dominant operator':<32} {'Share':>7}")
        print("-" * 70)
        for query_num, exec_ms, top_name, top_share in per_query_rows:
            print(f"{query_num:<8} {exec_ms:>9.2f}ms   {top_name:<32} {top_share:>6.1f}%")

        # Suite-wide: where the whole battery spends its time.
        suite_total = sum(suite_self.values()) or 1
        print(f"\n{'=' * 80}")
        print("SUITE-WIDE OPERATOR SELF-TIME (sum across all queries)")
        print(f"{'=' * 80}\n")
        print(f"{'Operator':<34} {'Self time':>12} {'Share':>8}")
        print("-" * 56)
        for name, t in sorted(suite_self.items(), key=lambda x: -x[1]):
            print(f"{name:<34} {t / 1e6:>9.2f}ms {100.0 * t / suite_total:>6.1f}%")
        print("-" * 56)
        print(f"{'TOTAL operator self-time':<34} {suite_total / 1e6:>9.2f}ms")
        print(
            f"\n{'Planning (total)':<34} {suite_plan_ns / 1e6:>9.2f}ms"
            f"\n{'Execution (total, traced)':<34} {suite_exec_ns / 1e6:>9.2f}ms"
        )

        # Sub-phase breakdown within Grouped Aggregate (Hashed) — where suite_self
        # above only shows it as one number. hash_s = key hashing (Pass A), probe_s
        # = hash-table find_or_insert + lane growth (Pass B), apply_s = per-aggregate-
        # function state update (Pass C). See groupby_tel.hpp.
        gb_total = sum(suite_gb_phase.values()) or 1
        print(f"\n{'=' * 80}")
        print("GROUPED AGGREGATE PHASE BREAKDOWN (hash / probe / apply)")
        print(f"{'=' * 80}\n")
        print(f"{'Phase':<20} {'Time':>12} {'Share':>8}")
        print("-" * 42)
        gb_labels = {"hash_s": "Hash keys (A)", "probe_s": "Probe/insert (B)", "apply_s": "Apply aggs (C)"}
        for phase, seconds in sorted(suite_gb_phase.items(), key=lambda x: -x[1]):
            print(f"{gb_labels.get(phase, phase):<20} {seconds * 1000:>9.2f}ms {100.0 * seconds / gb_total:>6.1f}%")
        print("-" * 42)
        print(f"{'TOTAL':<20} {gb_total * 1000:>9.2f}ms")

        # Sub-phase breakdown within Parquet Read's decode step — already computed by
        # the existing rugo_tel accumulators (rugo/src/parquet/telemetry.hpp), just
        # not previously surfaced anywhere.
        pq_total = sum(suite_pq_phase.values()) or 1
        print(f"\n{'=' * 80}")
        print("PARQUET READ DECODE PHASE BREAKDOWN")
        print(f"{'=' * 80}\n")
        print(f"{'Phase':<20} {'Time':>12} {'Share':>8}")
        print("-" * 42)
        for phase, seconds in sorted(suite_pq_phase.items(), key=lambda x: -x[1]):
            print(f"{phase:<20} {seconds * 1000:>9.2f}ms {100.0 * seconds / pq_total:>6.1f}%")
        print("-" * 42)
        print(f"{'TOTAL':<20} {pq_total * 1000:>9.2f}ms")

    if failed > 0:
        print("\n\033[38;2;139;233;253m\033[3mFAILURES\033[0m")
        for statement, err in failures:
            print(err)

    # Headline total: the sum of each query's BEST (minimum) time. This is OUR
    # metric, not the published ClickBench one — ClickBench normalises per query
    # against the best system and has cold/hot semantics this runner does not
    # reproduce. Never compare this number to a ×N figure from the public chart.
    #
    # Queries that FAILED contribute nothing, so a suite with failures has a
    # smaller total than a working one. The failure count below is part of the
    # headline, not a footnote to it.
    summary = f"\n\033[38;2;139;233;253m\033[3mSUM OF MINIMUMS\033[0m  {sum_min_ms / 1000:.2f}s ({sum_min_ms:.0f}ms) over {passed}/{len(STATEMENTS)} queries"
    if failed > 0:
        summary += f"\n  \033[0;31m{failed} queries failed and contribute 0ms — this total is not comparable to a clean run\033[0m"
    if sum_duckdb_min_ms > 0:
        summary += (
            f"\n  vs DuckDB {sum_duckdb_min_ms / 1000:.2f}s  "
            f"{format_ratio(sum_min_ms, sum_duckdb_min_ms)}  (orientation only)"
        )
    print(summary)

    if args.json:
        payload = {
            "provenance": provenance,
            "cold_start_ms": cold_time_ms,
            "unstable_spread_threshold": UNSTABLE_SPREAD,
            "queries": [
                {
                    "query": f"Q{(index + 1):02d}",
                    "times_ms": timings[index],
                    "failed": index in dead,
                }
                for index in range(len(STATEMENTS))
            ],
            "sum_of_minimums_ms": sum_min_ms,
            "passed": passed,
            "failed": failed,
        }
        with open(args.json, "w") as results_file:
            json.dump(payload, results_file, indent=2)
        print(f"\nresults written to {args.json}")

    print(
        f"\n\033[38;2;139;233;253m\033[3mCOMPLETE\033[0m ({((time.monotonic_ns() - start_suite) / 1e9):.2f} seconds wall)\n"
        f"  \033[38;2;26;185;67m{passed} passed ({(passed * 100) // (passed + failed)}%)\033[0m\n"
        f"  \033[38;2;255;121;198m{failed} failed\033[0m"
    )
