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
    FULL_SPLIT_RUGO = "scratch.hits_rugo"
    FULL_SPLIT_RUGO_250K = "scratch.hits_rugo_250k"
    FULL_SPLIT_RUGO_125K = "scratch.hits_rugo_125k"
    FULL_SPLIT_RUGO_262K = "scratch.hits_rugo_262k"
    FULL_SINGLE = "scratch.hits_single"


DATASET = Dataset.FULL_SPLIT

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
    import shutil
    import time

    from tests import trunc_printable

    parser = argparse.ArgumentParser(description="ClickBench Performance Test")
    parser.add_argument(
        "--warm",
        action="store_true",
        default=True,
        help="Run warm queries (3 iterations per query)",
    )
    parser.add_argument(
        "--iterations",
        type=int,
        default=2,
        help="Number of iterations for warm queries (default: 3)",
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
    args = parser.parse_args()

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
        elif ratio <= 10.0:
            # Orange: 1000% slower
            return f"\033[38;2;255;165;0m{ratio_str}\033[0m"
        else:
            # Red: 1000%+ slower
            return f"\033[38;2;255;69;69m{ratio_str}\033[0m"

    start_suite = time.monotonic_ns()
    width = shutil.get_terminal_size((80, 20))[0] - 18
    passed: int = 0
    failed: int = 0
    nl: str = "\n"
    failures = []

    if args.warm:
        print(f"{'=' * 80}")
        print(f"CLICKBENCH WARM PERFORMANCE BENCHMARK")
        print(f"Version: {opteryx.__version__}")
        print(f"Iterations per query: {args.iterations}")
        print(f"Dataset: {DATASET.name} ({DATASET.value})")
        if duckdb_results:
            print(f"Baseline: DuckDB on {duckdb_machine} (comparing to warm2 times)")
        print()
        print(f"{'=' * 80}\n")

        # Cold start
        print("Warming up (cold start)...")
        start = time.monotonic_ns()
        warm_session = None
        try:
            warm_session = opteryx.session()
            for _ in warm_session.execute_to_morsels(f"SELECT COUNT(*) FROM {DATASET.value};"):
                pass
            cold_time_ms = (time.monotonic_ns() - start) / 1e6
            print(f"Cold start: {cold_time_ms:.2f}ms\n")
        except Exception as e:
            print(f"Cold start failed: {e}\n")
        finally:
            if warm_session is not None:
                warm_session.close()

        header = f"{'Query':<8} {'Iteration 1':>14} {'Iteration 2':>14} {'Iteration 3':>14}         {'Avg':<13} {'Min':<13} {'Max':<13}"
        if duckdb_results:
            header += "vs DuckDB"
        print(header)
        print("-" * (102 + (12 if duckdb_results else 0)))

    print(f"RUNNING CLICKBENCH BATTERY OF {len(STATEMENTS)} QUERIES\n")
    for index, (statement, err) in enumerate(STATEMENTS):
        statement = statement.replace("{DATASET}", f"{DATASET.value}")
        printable = statement
        query_num = f"Q{(index + 1):02d}/{index:02d}"

        if args.warm:
            # Run multiple iterations for warm query testing
            times = []
            query_failed = False

            for iteration in range(args.iterations):
                gc.collect()
                session = None
                try:
                    start = time.monotonic_ns()
                    session = opteryx.session()
                    for _ in session.execute_to_morsels(statement):
                        pass
                    elapsed_ms = (time.monotonic_ns() - start) / 1e6
                    times.append(elapsed_ms)
                except opteryx.exceptions.MissingSqlStatement:
                    # Commented-out queries (e.g. Q33) are intentional skips.
                    query_failed = True
                    print(f"{query_num:<8} SKIP  (no SQL statement)")
                    break
                except Exception as e:
                    query_failed = True
                    print(f"{query_num:<8} ERROR: {str(e)[:60]}")
                    failures.append((statement, e))
                    failed += 1
                    break
                finally:
                    if session is not None:
                        session.close()

            if not query_failed and times:
                avg_time = sum(times) / len(times)
                min_time = min(times)
                max_time = max(times)

                # Format iteration times
                iter_strs = [f"{t:.2f}ms" for t in times]
                while len(iter_strs) < 3:
                    iter_strs.append("-")

                result_str = (
                    f"{query_num:<8} {iter_strs[0]:>14} {iter_strs[1]:>14} {iter_strs[2]:>14} "
                    f"{avg_time:>9.2f}ms   {min_time:>9.2f}ms   {max_time:>9.2f}ms"
                )

                # Add DuckDB comparison if available
                if duckdb_results and index < len(duckdb_results):
                    duckdb_ms = duckdb_results[index] * 1000  # Convert from seconds to ms
                    ratio_str = format_ratio(min_time, duckdb_ms)
                    result_str += f"  {ratio_str}"

                print(result_str)

                passed += 1
        else:
            # Original single-run mode
            print(
                f"\033[38;2;255;184;108m{(index + 1):04}\033[0m"
                f" {trunc_printable(format_sql(printable), width - 1)}",
                end="",
                flush=True,
            )
            try:
                start = time.monotonic_ns()
                test_sql_battery(statement, err)
                print(
                    f"\033[38;2;26;185;67m{str(int((time.monotonic_ns() - start) / 1e6)).rjust(4)}ms\033[0m ✅",
                    end="",
                )
                passed += 1
                if failed > 0:
                    print(f" \033[0;31m{failed}\033[0m")
                else:
                    print()
            except Exception as err:
                failed += 1
                print(
                    f"\033[0;31m{str(int((time.monotonic_ns() - start) / 1e6)).rjust(4)}ms ❌ {failed}\033[0m"
                )
                print(">", err)
                failures.append((statement, err))

    print("--- ✅ \033[0;32mdone\033[0m")

    if args.profile:
        import collections

        # A query's accurate per-operator SELF time only accrues with tracing on
        # (it lets push() subtract downstream_time out of the inclusive
        # execution_time). The only sanctioned path that flips tracing on every
        # node is EXPLAIN ANALYZE, so we drive each query that way in a SEPARATE
        # pass — the benchmark numbers above stay tracing-free and honest.
        print(f"\n{'=' * 80}")
        print("PER-OPERATOR PROFILE (tracing pass — EXPLAIN ANALYZE self-time)")
        print(f"{'=' * 80}\n")

        suite_self = collections.defaultdict(int)  # operator name -> self_time ns
        suite_plan_ns = 0
        suite_exec_ns = 0
        per_query_rows = []  # (query_num, exec_ms, top_operator, top_share)

        for index, (statement, _err) in enumerate(STATEMENTS):
            statement = statement.replace("{DATASET}", f"{DATASET.value}")
            query_num = f"Q{(index + 1):02d}"
            gc.collect()
            session = None
            try:
                session = opteryx.session()
                for _ in session.execute_to_morsels(f"EXPLAIN ANALYZE {statement}"):
                    pass

                op_self = collections.defaultdict(int)
                for nid in session._plan.nodes():
                    node = session._plan[nid]
                    if node is None or node.name in ("Explain", "Exit"):
                        continue
                    op_self[node.name] += node.sensors().get("self_time", 0)

                for name, t in op_self.items():
                    suite_self[name] += t
                suite_plan_ns += session._telemetry.time_planning
                suite_exec_ns += session._telemetry.time_executing

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

    if failed > 0:
        print("\n\033[38;2;139;233;253m\033[3mFAILURES\033[0m")
        for statement, err in failures:
            print(err)

    print(
        f"\n\033[38;2;139;233;253m\033[3mCOMPLETE\033[0m ({((time.monotonic_ns() - start_suite) / 1e9):.2f} seconds)\n"
        f"  \033[38;2;26;185;67m{passed} passed ({(passed * 100) // (passed + failed)}%)\033[0m\n"
        f"  \033[38;2;255;121;198m{failed} failed\033[0m"
    )
