#!/usr/bin/env python3
"""
TPC-H benchmark + DuckDB comparison runner.

Runs each TPC-H query against Opteryx (warm, multi-iteration), compares the
best Opteryx time to the DuckDB baseline at the same scale factor, and writes
per-iteration results to `results/<sha>-<ts>.csv`.

Usage:
    make tpch-sf1 | tpch-sf10 | tpch-sf100     # skene v2 mirror, 3 warm iterations
    python tests/performance/tpch/runner.py --scale 1 --variant skene
    python tests/performance/tpch/runner.py --scale 001
    python tests/performance/tpch/runner.py --iterations 5

Inputs:
    tests/performance/tpch/opteryx/queries/query*.sql          — query bodies
    tests/performance/tpch/duckdb/results.sf{scale}.json       — DuckDB baseline

The DuckDB baseline is regenerated separately, per scale, via
`make tpch-sf1-duckdb` / `tpch-sf10-duckdb` / `tpch-sf100-duckdb`.
"""

from __future__ import annotations

import argparse
import gc
import glob
import os
import sys
import time

# Repo root on sys.path so `import opteryx` resolves to the source tree.
_REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
sys.path.insert(0, _REPO_ROOT)

# Performance helpers (shared display + CSV layout)
sys.path.insert(0, os.path.join(_REPO_ROOT, "tests", "performance"))
from _common import (  # noqa: E402
    load_duckdb_baseline,
    load_duckdb_shapes,
    open_results_csv,
    print_banner,
    print_error_row,
    print_header,
    print_row,
    print_total_row,
)

import opteryx  # noqa: E402
from opteryx.connectors import DiskConnector  # noqa: E402

opteryx.register_workspace("testdata", DiskConnector)


_QUERY_DIR = os.path.join(os.path.dirname(__file__), "opteryx", "queries")
_DUCKDB_DIR = os.path.join(os.path.dirname(__file__), "duckdb")
_RESULTS_DIR = os.path.join(os.path.dirname(__file__), "results")


def _dataset_suffix(scale: str, variant: str) -> str:
    """`tpch_<scale>` or `tpch_<scale>_<variant>` (e.g. variant `skene`)."""
    return f"tpch_{scale}_{variant}" if variant else f"tpch_{scale}"


def _scale_to_dataset(scale: str, variant: str = "") -> str:
    """Map CLI scale token (`1`, `001`, …) to the testdata workspace path."""
    return f"testdata.{_dataset_suffix(scale, variant)}"


def _load_queries(scale: str, variant: str = "") -> list[tuple[str, str]]:
    """[(name, sql), ...] sorted by name; placeholder table prefixes rewritten."""
    dataset = _scale_to_dataset(scale, variant)
    queries: list[tuple[str, str]] = []
    for path in sorted(glob.glob(os.path.join(_QUERY_DIR, "query*.sql"))):
        name = os.path.splitext(os.path.basename(path))[0]
        if name.startswith("query") and name[5:].isdigit():
            name = f"Q{int(name[5:]):02d}"
        body = open(path).read()
        body = body.replace("testdata.tpch_tiny.", f"{dataset}.")
        body = body.replace("testdata.tpch.", f"{dataset}.")
        queries.append((name, body))
    return queries


def _run_query(sql: str) -> tuple[float, int, int]:
    """Run one query, return (elapsed_ms, row_count, col_count)."""
    gc.collect()
    session = opteryx.session()
    try:
        rows = 0
        cols = 0
        t0 = time.monotonic_ns()
        for morsel in session.execute_to_morsels(sql):
            if morsel is not None and hasattr(morsel, "num_rows"):
                rows += morsel.num_rows
                if cols == 0:
                    cols = len(morsel.column_names)
        return (time.monotonic_ns() - t0) / 1e6, rows, cols
    finally:
        session.close()


def _profile_pass(queries: list[tuple[str, str]]) -> None:
    """Per-operator self-time for each query, from a SEPARATE tracing pass.

    Real per-operator self-time only exists once the query has actually run: the
    physical-plan Python objects never execute on the native engine (the C++ engine
    does), so their own execution_time/sensors() counters stay zero.
    `mermaid._collect_node_stats()` is what overlays the native engine's per-identity
    readings (`telemetry._reading["native_op_stats"]`) back onto the plan nodes - the
    same lookup EXPLAIN ANALYZE (TEXT format) uses for its own self-time column.

    Driven via EXPLAIN ANALYZE in a pass of its own so the benchmark timings above
    stay tracing-free and honest.

    Pin the width before reading any of this: `self_time` and `cpu_time_ms` are
    thread-summed across every dop worker, so they inflate with parallelism and are
    not comparable between two runs at different widths. Run the profile under
    `MAX_EXECUTION_WORKERS=1` to get per-operator numbers that add up.
    """
    import collections

    from opteryx.operators._operators import get_groupby_telemetry
    from opteryx.operators._operators import reset_groupby_telemetry
    from rugo.rugo_native import get_cpp_telemetry
    from rugo.rugo_native import reset_cpp_telemetry

    from opteryx.utils import mermaid as _mermaid

    print(f"\n{'=' * 100}")
    print("PER-OPERATOR PROFILE (tracing pass - EXPLAIN ANALYZE self-time)")
    print(f"  width: MAX_EXECUTION_WORKERS={os.environ.get('MAX_EXECUTION_WORKERS', 'auto')}"
          "   (self_time/cpu_time are THREAD-SUMMED - pin to 1 to compare)")
    print(f"{'=' * 100}")

    suite_self: dict = collections.defaultdict(int)  # operator label -> self_time ns
    suite_gb_phase: dict = collections.defaultdict(float)  # groupby phase -> seconds
    suite_pq_phase: dict = collections.defaultdict(float)  # parquet decode phase -> seconds
    summary_rows: list[tuple[str, float, float, float, str, float]] = []

    for name, sql in queries:
        gc.collect()
        session = None
        reset_groupby_telemetry()
        reset_cpp_telemetry()
        try:
            session = opteryx.session()
            for _ in session.execute_to_morsels(f"EXPLAIN ANALYZE {sql}"):
                pass

            node_stats_by_nid, _, _ = _mermaid._collect_node_stats(session._plan)

            rows = []
            op_self: dict = collections.defaultdict(int)
            for nid in session._plan.nodes():
                node = session._plan[nid]
                if node is None or node.name in ("Explain", "Exit"):
                    continue
                stat = node_stats_by_nid.get(nid)
                if stat is None:
                    continue
                label = stat.get("operator") or node.name
                self_ns = stat.get("self_time", 0)
                op_self[label] += self_ns
                rows.append(
                    (
                        label,
                        self_ns,
                        stat.get("cpu_time_ms", 0.0),
                        stat.get("merge_time", 0),
                        stat.get("dop", 0),
                        stat.get("records_out", 0) or 0,
                        str(stat.get("config", "") or "")[:44],
                    )
                )

            for label, self_ns in op_self.items():
                suite_self[label] += self_ns

            plan_ms = session._telemetry.time_planning / 1e6
            exec_ms = session._telemetry.time_executing / 1e6
            total_self = sum(op_self.values())

            print(f"\n{name}   planning {plan_ms:.1f}ms   execution {exec_ms:.1f}ms   "
                  f"operator self-time {total_self / 1e6:.1f}ms (thread-summed)")
            print(f"  {'Operator':<26} {'Self':>10} {'CPU':>10} {'Merge':>9} {'dop':>4} "
                  f"{'Rows out':>12}  {'Config':<44}")
            print("  " + "-" * 96)
            for label, self_ns, cpu_ms, merge_ns, dop, rows_out, config in sorted(
                rows, key=lambda r: -r[1]
            ):
                print(
                    f"  {label:<26} {self_ns / 1e6:>8.1f}ms {cpu_ms:>8.1f}ms "
                    f"{merge_ns / 1e6:>7.1f}ms {dop:>4} {rows_out:>12,}  {config:<44}"
                )

            gb_tel = get_groupby_telemetry()
            for phase in ("hash_s", "probe_s", "apply_s"):
                suite_gb_phase[phase] += gb_tel[phase]
            pq_tel = get_cpp_telemetry()
            for phase, seconds in pq_tel.items():
                if phase.endswith("_s"):
                    suite_pq_phase[phase] += seconds

            top_label, top_ns = max(
                op_self.items(), key=lambda x: x[1], default=("-", 0)
            )
            summary_rows.append(
                (
                    name,
                    plan_ms,
                    exec_ms,
                    total_self / 1e6,
                    top_label,
                    100.0 * top_ns / (total_self or 1),
                )
            )
        finally:
            if session is not None:
                session.close()

    # Planning vs execution, per query. Planning is Python and does not scale with
    # the data, so on a small/slow box it is a FLAT tax that a ratio against DuckDB
    # hides - this column is what says whether a gap is engine work or fixed cost.
    print(f"\n{'=' * 100}")
    print("PLANNING vs EXECUTION")
    print(f"{'=' * 100}\n")
    print(f"{'Query':<8} {'Planning':>11} {'Execution':>11} {'Plan share':>11}   "
          f"{'Dominant operator':<28} {'Share':>7}")
    print("-" * 88)
    for name, plan_ms, exec_ms, self_ms, top_label, top_share in summary_rows:
        share = 100.0 * plan_ms / ((plan_ms + exec_ms) or 1)
        print(f"{name:<8} {plan_ms:>9.1f}ms {exec_ms:>9.1f}ms {share:>10.1f}%   "
              f"{top_label:<28} {top_share:>6.1f}%")

    suite_total = sum(suite_self.values()) or 1
    print(f"\n{'=' * 100}")
    print("OPERATOR SELF-TIME, SUMMED OVER THE PROFILED QUERIES")
    print(f"{'=' * 100}\n")
    print(f"{'Operator':<34} {'Self time':>12} {'Share':>8}")
    print("-" * 56)
    for label, self_ns in sorted(suite_self.items(), key=lambda x: -x[1]):
        print(f"{label:<34} {self_ns / 1e6:>9.1f}ms {100.0 * self_ns / suite_total:>6.1f}%")
    print("-" * 56)
    print(f"{'TOTAL operator self-time':<34} {suite_total / 1e6:>9.1f}ms")

    # Sub-phase breakdown within Grouped Aggregate (Hashed), where the table above
    # only shows it as one number. hash_s = key hashing (Pass A), probe_s = hash-table
    # find_or_insert + lane growth (Pass B), apply_s = per-aggregate-function state
    # update (Pass C). See src/cpp/engine/groupby_tel.hpp.
    gb_total = sum(suite_gb_phase.values())
    print(f"\n{'=' * 100}")
    print("GROUPED AGGREGATE PHASE BREAKDOWN (hash / probe / apply)")
    print(f"{'=' * 100}\n")
    gb_labels = {
        "hash_s": "Hash keys (A)",
        "probe_s": "Probe/insert (B)",
        "apply_s": "Apply aggs (C)",
    }
    if gb_total == 0:
        # A zero total means no grouped aggregate ran in this pass. Say that,
        # rather than dividing by a fabricated denominator and printing a table
        # of 0.0% rows under a total that was never measured.
        print("  no grouped aggregate ran in the profiled queries")
    else:
        print(f"{'Phase':<20} {'Time':>12} {'Share':>8}")
        print("-" * 42)
        for phase, seconds in sorted(suite_gb_phase.items(), key=lambda x: -x[1]):
            print(f"{gb_labels.get(phase, phase):<20} {seconds * 1000:>9.1f}ms "
                  f"{100.0 * seconds / gb_total:>6.1f}%")
        print("-" * 42)
        print(f"{'TOTAL':<20} {gb_total * 1000:>9.1f}ms")

    # Parquet decode phases - already accumulated by rugo's own telemetry
    # (rugo/src/parquet/telemetry.hpp), just surfaced here.
    pq_total = sum(suite_pq_phase.values())
    print(f"\n{'=' * 100}")
    print("PARQUET READ DECODE PHASE BREAKDOWN")
    print(f"{'=' * 100}\n")
    if pq_total == 0:
        # Zero across every phase means nothing read parquet - the `skene` variant
        # is the usual reason. Saying so beats a table of 0.0% shares under a
        # denominator that was never measured.
        print("  no parquet decode in the profiled queries (skene variant reads no parquet)")
    else:
        print(f"{'Phase':<20} {'Time':>12} {'Share':>8}")
        print("-" * 42)
        for phase, seconds in sorted(suite_pq_phase.items(), key=lambda x: -x[1]):
            print(f"{phase:<20} {seconds * 1000:>9.1f}ms {100.0 * seconds / pq_total:>6.1f}%")
        print("-" * 42)
        print(f"{'TOTAL':<20} {pq_total * 1000:>9.1f}ms")


def main() -> int:
    parser = argparse.ArgumentParser(description="TPC-H benchmark vs DuckDB")
    parser.add_argument(
        "--scale",
        type=str,
        default="10",
        # SF10, not SF1. At SF1 planning is 19.1% of the suite's total work, so
        # the benchmark substantially measures the PLANNER and dilutes any engine
        # change by ~1.2x before it can be seen; at SF10 planning is 2.6%. SF1
        # also puts each table in one parquet file, so the scan's per-file
        # parallelism is barely exercised, and the whole dataset is page-cache
        # resident, which makes codec measurements say the opposite of what they
        # say at a realistic size. Smaller scales stay available via --scale.
        help="Scale factor suffix matching testdata/tpch_<scale> (default: 10)",
    )
    parser.add_argument(
        "--iterations",
        type=int,
        default=3,
        help="Warm iterations per query (default: 3)",
    )
    parser.add_argument(
        "--variant",
        type=str,
        default="",
        help="Dataset format variant: runs against testdata/tpch_<scale>_<variant> "
        "(e.g. `skene` for the skene mirror; default: the parquet dataset)",
    )
    parser.add_argument(
        "--queries",
        type=str,
        default="",
        help="Comma-separated query names to run (e.g. `Q01,Q09,Q13`). Default: all 22. "
        "Applies to the benchmark pass AND the --profile pass.",
    )
    parser.add_argument(
        "--profile",
        action="store_true",
        help="After the benchmark, run a SEPARATE tracing pass (EXPLAIN ANALYZE) and "
        "report per-operator self-time, the planning/execution split per query, and "
        "the grouped-aggregate and parquet-decode sub-phase breakdowns.",
    )
    args = parser.parse_args()

    suffix = _dataset_suffix(args.scale, args.variant)
    dataset = _scale_to_dataset(args.scale, args.variant)
    dataset_path = os.path.join(_REPO_ROOT, "testdata", suffix)
    if not os.path.isdir(dataset_path):
        print(f"ERROR: dataset not found at {dataset_path}")
        print(f"       expected: testdata/{suffix}")
        if args.variant:
            print(f"       generate it: python dev/parquet_to_skene.py "
                  f"testdata/tpch_{args.scale} testdata/{suffix}")
        return 1

    queries = _load_queries(args.scale, args.variant)
    if not queries:
        print(f"ERROR: no .sql files found in {_QUERY_DIR}")
        return 1

    if args.queries:
        wanted = [q.strip().upper() for q in args.queries.split(",") if q.strip()]
        available = {name for name, _ in queries}
        # An unknown name is a typo, not a query that ran zero times - refuse
        # rather than silently profiling a shorter suite than was asked for.
        unknown = [q for q in wanted if q not in available]
        if unknown:
            print(f"ERROR: unknown query name(s): {', '.join(unknown)}")
            print(f"       available: {', '.join(sorted(available))}")
            return 1
        order = {name: ix for ix, name in enumerate(wanted)}
        queries = sorted(
            (entry for entry in queries if entry[0] in order), key=lambda e: order[e[0]]
        )

    duckdb_baseline_path = os.path.join(_DUCKDB_DIR, f"results.sf{args.scale}.json")
    duckdb_min, duckdb_machine = load_duckdb_baseline(duckdb_baseline_path)
    duckdb_shapes = load_duckdb_shapes(duckdb_baseline_path)

    # Cold start
    print("Warming up (cold start)...")
    start = time.monotonic_ns()
    warm_session = None
    try:
        warm_session = opteryx.session()
        for _ in warm_session.execute_to_morsels(
            f"SELECT COUNT(*) FROM testdata.{suffix};"
        ):
            pass
        cold_time_ms = (time.monotonic_ns() - start) / 1e6
        print(f"Cold start: {cold_time_ms:.2f}ms\n")
    except Exception as e:
        print(f"Cold start failed: {e}\n")
    finally:
        if warm_session is not None:
            warm_session.close()

    print_banner(
        title="TPC-H BENCHMARK",
        opteryx_version=opteryx.__version__,
        metadata=[
            ("Scale factor", f"{args.scale}  ({dataset})"),
            ("Format", args.variant or "parquet"),
            ("Queries", str(len(queries))),
            ("Iterations", f"{args.iterations} warm runs per query"),
        ],
        duckdb_machine=duckdb_machine if duckdb_min else None,
        duckdb_query_count=len(duckdb_min) if duckdb_min else None,
    )

    print_header("Query", args.iterations, has_baseline=bool(duckdb_min))

    csv_writer, csv_path, csv_handle = open_results_csv(
        _RESULTS_DIR,
        fieldnames=[
            "scale",
            "variant",
            "query",
            "run",
            "status",
            "elapsed_ms",
            "row_count",
            "col_count",
            "duckdb_min_ms",
            "duckdb_rows",
            "duckdb_cols",
            "error",
        ],
    )

    passed = 0
    failed = 0
    failures: list[tuple[str, str]] = []
    suite_start = time.monotonic_ns()
    opteryx_total_min = 0.0
    duckdb_total_min = 0.0
    compared_queries = 0

    try:
        for name, sql in queries:
            d_ms = duckdb_min.get(name) if duckdb_min else None
            d_shape = duckdb_shapes.get(name)
            times: list[float] = []
            row_count = 0
            col_count = 0
            query_failed = False
            for run_ix in range(1, args.iterations + 1):
                try:
                    elapsed_ms, rows, cols = _run_query(sql)
                    times.append(elapsed_ms)
                    row_count = rows
                    col_count = cols
                    csv_writer.writerow(
                        {
                            "scale": args.scale,
                            "variant": args.variant or "parquet",
                            "query": name,
                            "run": run_ix,
                            "status": "ok",
                            "elapsed_ms": f"{elapsed_ms:.3f}",
                            "row_count": rows,
                            "col_count": cols,
                            "duckdb_min_ms": f"{d_ms:.3f}" if d_ms is not None else "",
                            "duckdb_rows": d_shape[0] if d_shape is not None else "",
                            "duckdb_cols": d_shape[1] if d_shape is not None else "",
                            "error": "",
                        }
                    )
                    csv_handle.flush()
                except Exception as err:
                    msg = f"{type(err).__name__}: {err}"
                    failures.append((name, msg))
                    failed += 1
                    query_failed = True
                    csv_writer.writerow(
                        {
                            "scale": args.scale,
                            "variant": args.variant or "parquet",
                            "query": name,
                            "run": run_ix,
                            "status": "error",
                            "elapsed_ms": "",
                            "row_count": 0,
                            "col_count": 0,
                            "duckdb_min_ms": f"{d_ms:.3f}" if d_ms is not None else "",
                            "duckdb_rows": d_shape[0] if d_shape is not None else "",
                            "duckdb_cols": d_shape[1] if d_shape is not None else "",
                            "error": msg,
                        }
                    )
                    csv_handle.flush()
                    print_error_row(name, msg)
                    break

            if query_failed or not times:
                continue

            # A DuckDB timing baseline only proves Opteryx was fast — not that
            # it was RIGHT. When the baseline JSON also recorded a result shape
            # (see load_duckdb_shapes), a mismatch here is a correctness
            # regression wearing a passing benchmark: fail loud rather than
            # report green on a wrong answer.
            if d_shape is not None and (row_count, col_count) != d_shape:
                failed += 1
                shape_msg = (
                    f"SHAPE MISMATCH: opteryx {row_count} rows/{col_count} cols "
                    f"vs duckdb {d_shape[0]} rows/{d_shape[1]} cols"
                )
                failures.append((name, shape_msg))
                print_row(name, times, args.iterations, d_ms)
                print(f"          \033[38;2;255;69;69m⚠ {shape_msg}\033[0m")
                continue

            passed += 1
            print_row(name, times, args.iterations, d_ms)
            if d_ms is not None:
                opteryx_total_min += min(times)
                duckdb_total_min += d_ms
                compared_queries += 1
    finally:
        csv_handle.close()

    print("─" * 100)
    if compared_queries:
        print_total_row(opteryx_total_min, duckdb_total_min, compared_queries, args.iterations)
    print()

    elapsed_s = (time.monotonic_ns() - suite_start) / 1e9
    print(
        f"\033[38;2;26;185;67m{passed} passed\033[0m, "
        f"\033[38;2;255;121;198m{failed} failed\033[0m   "
        f"({elapsed_s:.1f}s)"
    )
    print(f"  results: {os.path.relpath(csv_path, _REPO_ROOT)}")

    if args.profile:
        _profile_pass(queries)

    if failures:
        print()
        print("\033[38;2;139;233;253m\033[3mFAILURES\033[0m")
        for name, err in failures:
            print(f"  {name}: {err}")

    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
