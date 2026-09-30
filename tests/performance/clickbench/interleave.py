"""
ClickBench Opteryx-vs-DuckDB interleaved A/B
============================================
The stored DuckDB baseline is a different session and thermal state from any
Opteryx run, so it is orientation only. This harness runs BOTH engines in one
process, one machine state, in a counterbalanced order, so the comparison can
be asserted rather than eyeballed.

Schedule, per query N (every query, then the whole sweep LOOPS times):

    [gap] X(N) Y(N)  [gap]  Y(N) X(N)  [gap]  next query ...

The gap is ONE value used before every pair, so whichever engine goes first is
always preceded by the same idle time. (An earlier schedule used 1s before one
engine's first-move and 5s before the other's; Opteryx pays ~1.3x after a 5s
idle and DuckDB ~1.08x, so first-mover and idle time were confounded and the
pooled median leaned on whichever engine happened to sit in the long gap.)
X,Y is O,D on odd loops and D,O on even loops, so each engine is the first
mover in exactly half of all pairs. LOOPS must therefore be even.

The unit of measurement is the PAIR: the O and D runs adjacent in one slot, the
ratio taken inside the pair so machine drift common to both cancels. A query's
figure is the median of its paired ratios.

Both engines read the SAME files (default: scratch/hits_rugo_262k). The Opteryx
queries and the DuckDB queries are imported from their own runners, so there is
one definition of the suite.

Exit status is the assertion. Non-zero when:
  - the upper bound of the 95% bootstrap confidence interval of the geomean
    opteryx/duckdb (resampling each query's pairs; the suite is fixed, so queries are not resampled) exceeds
    --max-ratio: parity is only claimed when it is statistically supported,
  - any query errors on either engine, or has fewer pairs than asked for.
The order/idle-position split (is either engine slower as first mover?) is
printed as a diagnostic; with symmetric gaps a large split is itself a finding.
"""

import argparse
import csv
import gc
import importlib.util
import math
import os
import random
import statistics
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.abspath(os.path.join(HERE, "../../.."))

# This directory contains a `duckdb/` package (the runner), which would shadow the
# real duckdb module if sys.path[0] stayed as the script directory.
sys.path[:] = [p for p in sys.path if os.path.abspath(p or os.getcwd()) != HERE]
sys.path.insert(0, REPO_ROOT)

import duckdb  # noqa: E402
import opteryx  # noqa: E402


def load_module(name: str, relpath: str):
    spec = importlib.util.spec_from_file_location(name, os.path.join(HERE, relpath))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def geomean(values) -> float:
    return math.exp(statistics.fmean(math.log(v) for v in values))


def median_ci(ratios: list, rng: random.Random, iterations: int = 2000) -> tuple:
    """95% CI of one query's median paired ratio, resampling its pairs."""
    estimates = sorted(statistics.median(rng.choices(ratios, k=len(ratios))) for _ in range(iterations))
    return estimates[int(0.025 * iterations)], estimates[int(0.975 * iterations) - 1]


def suite_ci(pair_ratios: dict, rng: random.Random, iterations: int = 10000) -> tuple:
    """95% CI of the geomean of per-query median paired ratios.

    The 43 queries are the fixed suite being asserted on, NOT a sample of some
    wider population, so only the measurement noise is resampled: each query's
    pairs, with replacement. (Resampling queries too would fold query
    heterogeneity, e.g. 0.07x stats-answered queries next to 2x losses, into the
    interval and make it too wide to ever support a claim.) Fixed seed, so the
    same samples always give the same bound.
    """
    queries = list(pair_ratios.values())
    estimates = []
    for _ in range(iterations):
        logs = 0.0
        for ratios in queries:
            logs += math.log(statistics.median(rng.choices(ratios, k=len(ratios))))
        estimates.append(math.exp(logs / len(queries)))
    estimates.sort()
    return estimates[int(0.025 * iterations)], estimates[int(0.975 * iterations) - 1]


def main() -> int:
    parser = argparse.ArgumentParser(description="Interleaved Opteryx vs DuckDB ClickBench")
    parser.add_argument("--loops", type=int, default=4,
                        help="Full sweeps of the 43 queries; must be even (default: 4 -> 8 pairs/query)")
    parser.add_argument("--gap", type=float, default=5.0,
                        help="Idle seconds before EVERY pair, so first-movers see identical idle (default: 5)")
    parser.add_argument("--max-ratio", type=float, default=1.05,
                        help="Fail if the upper 95%% bound of geomean(opteryx/duckdb) exceeds this (default: 1.05)")
    parser.add_argument("--data", default="scratch/hits_rugo_262k",
                        help="Directory of parquet files both engines read (default: scratch/hits_rugo_262k)")
    parser.add_argument("--queries", default="",
                        help="Comma-separated 1-based query numbers to run (default: all)")
    args = parser.parse_args()

    if args.loops < 2 or args.loops % 2:
        print(f"FAIL: --loops must be even and >= 2 (got {args.loops}): each engine must be first mover equally often")
        return 2

    opteryx_suite = load_module("_opteryx_clickbench", "opteryx/runner.py")
    duckdb_suite = load_module("_duckdb_clickbench", "duckdb/runner.py")

    statements = [s for s, _ in opteryx_suite.STATEMENTS]
    duck_queries = duckdb_suite.QUERIES
    if len(statements) != len(duck_queries):
        print(f"FAIL: suites disagree on length: opteryx={len(statements)} duckdb={len(duck_queries)}")
        return 2

    selected = list(range(len(statements)))
    if args.queries:
        selected = [int(q) - 1 for q in args.queries.split(",")]

    data_dir = args.data if os.path.isabs(args.data) else os.path.join(REPO_ROOT, args.data)
    if not os.path.isdir(data_dir):
        print(f"FAIL: data directory {data_dir} does not exist")
        return 2
    parquet_glob = os.path.join(data_dir, "*.parquet")
    relation = ".".join(os.path.relpath(data_dir, REPO_ROOT).split(os.sep))

    con = duckdb.connect()
    con.execute("SET parquet_metadata_cache=true")
    con.execute(f"""
        CREATE VIEW hits AS
        SELECT * REPLACE (make_date(EventDate) AS EventDate)
        FROM read_parquet('{parquet_glob}', binary_as_string=True)
    """)
    con.execute("CREATE MACRO toDateTime(t) AS epoch_ms(CAST(t AS BIGINT) * 1000)")

    def run_opteryx(index: int) -> float:
        statement = statements[index].replace("{DATASET}", relation)
        gc.collect()
        session = opteryx.session()  # construction is not engine work, not on the clock
        try:
            start = time.monotonic_ns()
            for _ in session.execute_to_morsels(statement):
                pass
            return (time.monotonic_ns() - start) / 1e6
        finally:
            session.close()

    def run_duckdb(index: int) -> float:
        gc.collect()
        start = time.monotonic_ns()
        con.execute(duck_queries[index]).fetchall()
        return (time.monotonic_ns() - start) / 1e6

    engines = {"O": run_opteryx, "D": run_duckdb}
    pairs_per_query = args.loops * 2

    print("=" * 96)
    print("CLICKBENCH INTERLEAVED  opteryx vs duckdb")
    print("=" * 96)
    print(f"  opteryx {opteryx.__version__}   duckdb {duckdb.__version__}")
    print(f"  data        {data_dir}  (both engines)")
    print(f"  loops       {args.loops} (alternating O-first / D-first)   gap {args.gap}s before every pair")
    print(f"  queries     {len(selected)}   paired samples per query: {pairs_per_query}")
    print(f"  assertion   upper 95% bound of geomean(opteryx/duckdb) <= {args.max_ratio}")
    print("=" * 96)

    # Cold start outside the measured schedule, both engines, then the same idle gap.
    print("Warming up (first query through each engine)...")
    for engine in engines.values():
        engine(0)
    time.sleep(args.gap)

    # pairs[index] = list of (first_engine, o_ms, d_ms)
    pairs = {index: [] for index in selected}
    errors = []
    results_dir = os.path.join(HERE, "opteryx", "results", "INTERLEAVED")
    os.makedirs(results_dir, exist_ok=True)
    csv_path = os.path.join(results_dir, time.strftime("%Y%m%dT%H%M%S") + ".csv")
    handle = open(csv_path, "w", newline="")
    writer = csv.writer(handle)
    writer.writerow(["loop", "query", "pair", "first", "o_ms", "d_ms", "status", "error"])

    def measure(loop: int, index: int, pair_no: int, order: str) -> None:
        got = {}
        failure = ""
        for engine_key in order:
            try:
                got[engine_key] = engines[engine_key](index)
            except Exception as error:
                errors.append((index, engine_key, error))
                failure = f"{engine_key}: {error!r}"
        label = f"Q{index + 1:02d}"
        if failure:
            writer.writerow([loop, label, pair_no, order[0], "", "", "error", failure])
        else:
            pairs[index].append((order[0], got["O"], got["D"]))
            writer.writerow([loop, label, pair_no, order[0], f"{got['O']:.3f}", f"{got['D']:.3f}", "ok", ""])
        handle.flush()

    for loop in range(1, args.loops + 1):
        forward = "OD" if loop % 2 else "DO"
        for index in selected:
            measure(loop, index, 1, forward)
            time.sleep(args.gap)
            measure(loop, index, 2, forward[::-1])
            time.sleep(args.gap)
            ratios = [o / d for _f, o, d in pairs[index][-2:]]
            print(f"  loop {loop}/{args.loops} Q{index + 1:02d}  pair ratios O/D: "
                  + "  ".join(f"{r:5.2f}" for r in ratios), flush=True)
    handle.close()

    print()
    print(f"{'query':<6}{'O med ms':>10}{'D med ms':>10}{'ratio':>8}{'  O-1st':>8}{'  O-2nd':>8}  pairs O<D  verdict")
    print("-" * 96)
    pair_ratios = {}
    wins = losses = ties = 0
    first_split = {"O": [], "D": []}
    sum_o = sum_d = 0.0
    for index in selected:
        rows = pairs[index]
        if len(rows) != pairs_per_query:
            print(f"Q{index + 1:02d}   INCOMPLETE  {len(rows)}/{pairs_per_query} pairs")
            continue
        ratios = [o / d for _f, o, d in rows]
        pair_ratios[index] = ratios
        o_med = statistics.median(o for _f, o, _d in rows)
        d_med = statistics.median(d for _f, _o, d in rows)
        sum_o += o_med
        sum_d += d_med
        ratio = statistics.median(ratios)
        o_first = statistics.median(o for f, o, _d in rows if f == "O")
        o_second = statistics.median(o for f, o, _d in rows if f == "D")
        d_first = statistics.median(d for f, _o, d in rows if f == "D")
        d_second = statistics.median(d for f, _o, d in rows if f == "O")
        first_split["O"].append(o_first / o_second)
        first_split["D"].append(d_first / d_second)
        below = sum(1 for r in ratios if r < 1.0)
        q_low, q_high = median_ci(ratios, random.Random(index))
        if q_high < 1.0:
            verdict, wins = "OPTERYX", wins + 1
        elif q_low > 1.0:
            verdict, losses = "duckdb", losses + 1
        else:
            verdict, ties = "tie", ties + 1
        print(f"Q{index + 1:02d}  {o_med:10.1f}{d_med:10.1f}{ratio:8.2f}{o_first:8.1f}{o_second:8.1f}  "
              f"{below}/{len(ratios)}       {verdict}")

    print("-" * 96)
    failed = False
    for index, engine_key, error in errors:
        print(f"ERROR Q{index + 1:02d} {engine_key}: {error!r}")
        failed = True
    if len(pair_ratios) != len(selected):
        print(f"FAIL: only {len(pair_ratios)}/{len(selected)} queries produced a full pair set")
        failed = True

    if pair_ratios:
        point = geomean(statistics.median(r) for r in pair_ratios.values())
        low, high = suite_ci(pair_ratios, random.Random(0))
        print(f"geomean opteryx/duckdb     : {point:.3f}   95% CI [{low:.3f}, {high:.3f}]   (<1 = opteryx faster)")
        print(f"sum of medians             : opteryx {sum_o / 1000:.2f}s   duckdb {sum_d / 1000:.2f}s   ratio {sum_o / sum_d:.3f}")
        print(f"per-query (CI excludes 1)  : opteryx {wins}   duckdb {losses}   tie {ties}")
        print(f"first-mover penalty        : opteryx {geomean(first_split['O']):.3f}x   duckdb {geomean(first_split['D']):.3f}x"
              "   (geomean of first/second; ~1.0 = no order effect)")
        if geomean(first_split["O"]) > 1.10 or geomean(first_split["D"]) > 1.10:
            print("NOTE: a >10% first-mover penalty persists with symmetric gaps: that engine pays for idle/cold state.")
        if high > args.max_ratio:
            print(f"FAIL: upper 95% bound {high:.3f} exceeds --max-ratio {args.max_ratio}")
            failed = True
        elif not failed:
            print(f"PASS: upper 95% bound {high:.3f} <= {args.max_ratio}")
    print(f"samples written to {csv_path}")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
