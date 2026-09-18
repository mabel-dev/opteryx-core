#!/usr/bin/env python3
"""
Standalone TPC-H per-operator profiler.

Self-contained on purpose: it imports `opteryx` and NOTHING else from this
repo, so it runs unchanged against a pip-installed `opteryx_core` wheel on a
machine that has no checkout (Colab, a bare VM, the x86 repro box). The
in-tree equivalent is `tests/performance/tpch/runner.py --profile`, which
needs testdata/, tests/performance/_common.py and a DuckDB baseline JSON -
none of which exist off the tree.

The queries are EMBEDDED (Q01, Q09, Q13 - the three with the largest absolute
gap to DuckDB at SF5) with `testdata.tpch` as a placeholder prefix that
`--dataset` rewrites. `--sql-dir` overrides them with a directory of
`query*.sql` if you want the full 22.

Usage (Colab, data already registered by the notebook):

    !MAX_EXECUTION_WORKERS=1 python tpch_profile.py --dataset tpch_5

Usage (local parquet/skene directories on disk):

    MAX_EXECUTION_WORKERS=1 python dev/tpch_profile.py \
        --register testdata --dataset testdata.tpch_5_skene

Or from inside a notebook that already registers its own connectors:

    from tpch_profile import profile_queries
    profile_queries([("Q01", sql), ...])

WIDTH. `self_time` and `cpu_time` are summed across every dop worker thread,
so they inflate with parallelism and two runs at different widths are not
comparable. Pin `MAX_EXECUTION_WORKERS=1` in the ENVIRONMENT (config is read
at import, so setting it in Python after `import opteryx` does nothing).
"""

from __future__ import annotations

import argparse
import collections
import gc
import glob
import os
import sys
import time

import opteryx

# The placeholder prefix inside the embedded SQL below, rewritten by --dataset.
_PLACEHOLDER = "testdata.tpch"

_QUERIES: dict[str, str] = {
    "Q01": """
select
    l_returnflag,
    l_linestatus,
    sum(l_quantity) as sum_qty,
    sum(l_extendedprice) as sum_base_price,
    sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
    sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
    avg(l_quantity) as avg_qty,
    avg(l_extendedprice) as avg_price,
    avg(l_discount) as avg_disc,
    count(*) as count_order
from
    testdata.tpch.lineitem
where
    l_shipdate <= '1998-09-16'::DATE
group by l_returnflag, l_linestatus
order by l_returnflag, l_linestatus
""",
    "Q09": """
select
    nation,
    o_year,
    sum(amount) as sum_profit
from
    (
        select
            n_name as nation,
            EXTRACT(YEAR FROM o_orderdate) as o_year,
            l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity as amount
        from
            testdata.tpch.part,
            testdata.tpch.supplier,
            testdata.tpch.lineitem,
            testdata.tpch.partsupp,
            testdata.tpch.orders,
            testdata.tpch.nation
        where
            s_suppkey = l_suppkey
            and ps_suppkey = l_suppkey
            and ps_partkey = l_partkey
            and p_partkey = l_partkey
            and o_orderkey = l_orderkey
            and s_nationkey = n_nationkey
            and p_name like '%plum%'
    ) as profit
group by
    nation,
    o_year
order by
    nation,
    o_year desc
""",
    # Canonical Q13 filters o_comment inside the LEFT OUTER JOIN's ON clause;
    # Opteryx supports equality predicates only in ON, so orders is pre-filtered
    # in a derived table instead. That reproduces the ON-clause semantics exactly
    # (moving the predicate to WHERE would NOT - it runs after the join and drops
    # the c_count=0 bucket). Kept in step with
    # tests/performance/tpch/opteryx/queries/query13.sql.
    "Q13": """
SELECT
  c_count,
  Count(*) AS custdist
FROM
  (
    SELECT
      c_custkey,
      Count(o_orderkey) AS c_count
    FROM
      testdata.tpch.customer
      LEFT OUTER JOIN (
        SELECT
          *
        FROM
          testdata.tpch.orders
        WHERE
          o_comment NOT LIKE '%unusual%accounts%'
      ) AS t ON c_custkey = t.o_custkey
    GROUP BY
      c_custkey
  ) c_orders
GROUP BY
  c_count
ORDER BY
  custdist DESC,
  c_count DESC
""",
}


def _time_query(sql: str) -> tuple[float, int]:
    """Run one query untraced; return (wall_ms, rows). The number to compare
    against the benchmark chart - the traced pass below is NOT that number."""
    gc.collect()
    session = opteryx.session()
    try:
        rows = 0
        start = time.monotonic_ns()
        for morsel in session.execute_to_morsels(sql):
            if morsel is not None:
                rows += morsel.num_rows
        return (time.monotonic_ns() - start) / 1e6, rows
    finally:
        session.close()


def profile_queries(pairs: list[tuple[str, str]], iterations: int = 3) -> None:
    """Per-operator self-time for each (name, sql), from a tracing pass.

    Real per-operator self-time only exists once the query has actually run:
    the physical-plan Python objects never execute on the native engine (the
    C++ engine does), so their own execution_time/sensors() counters stay zero.
    `mermaid._collect_node_stats()` is what overlays the native engine's
    per-identity readings (`telemetry._reading["native_op_stats"]`) back onto
    the plan nodes - the same lookup EXPLAIN ANALYZE (TEXT format) uses for its
    own self-time column.

    Each query is run untraced `iterations` times first and the MINIMUM is
    reported, matching the benchmark chart's min-of-N-warm methodology. The
    first run of a query is cold, so at iterations=1 the reported wall time is
    a COLD number - keep it at 3 or more for anything you intend to compare.
    """
    from opteryx.operators._operators import get_groupby_telemetry
    from opteryx.operators._operators import reset_groupby_telemetry
    from opteryx.utils import mermaid as _mermaid
    from rugo.rugo_native import get_cpp_telemetry
    from rugo.rugo_native import reset_cpp_telemetry

    print(f"\n{'=' * 100}")
    print("PER-OPERATOR PROFILE (tracing pass - EXPLAIN ANALYZE self-time)")
    print(f"  opteryx {opteryx.__version__}   "
          f"MAX_EXECUTION_WORKERS={os.environ.get('MAX_EXECUTION_WORKERS', 'auto')}"
          "   (self_time/cpu_time are THREAD-SUMMED - pin to 1 to compare)")
    print(f"{'=' * 100}")

    suite_self: dict = collections.defaultdict(int)  # operator label -> self_time ns
    suite_gb_phase: dict = collections.defaultdict(float)  # groupby phase -> seconds
    suite_pq_phase: dict = collections.defaultdict(float)  # decode phase -> seconds
    summary_rows: list[tuple[str, float, float, float, float, str, float]] = []

    for name, sql in pairs:
        walls: list[float] = []
        rows_out = 0
        for _ in range(max(1, iterations)):
            elapsed_ms, rows_out = _time_query(sql)
            walls.append(elapsed_ms)
        warm_ms = min(walls)

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

            print(f"\n{name}   min wall {warm_ms:.1f}ms of {len(walls)} ({rows_out:,} rows)   "
                  f"planning {plan_ms:.1f}ms   execution {exec_ms:.1f}ms   "
                  f"operator self-time {total_self / 1e6:.1f}ms (thread-summed)")
            print(f"  {'Operator':<26} {'Self':>10} {'CPU':>10} {'Merge':>9} {'dop':>4} "
                  f"{'Rows out':>12}  {'Config':<44}")
            print("  " + "-" * 96)
            for label, self_ns, cpu_ms, merge_ns, dop, n_out, config in sorted(
                rows, key=lambda r: -r[1]
            ):
                print(
                    f"  {label:<26} {self_ns / 1e6:>8.1f}ms {cpu_ms:>8.1f}ms "
                    f"{merge_ns / 1e6:>7.1f}ms {dop:>4} {n_out:>12,}  {config:<44}"
                )

            gb_tel = get_groupby_telemetry()
            for phase in ("hash_s", "probe_s", "apply_s"):
                suite_gb_phase[phase] += gb_tel[phase]
            pq_tel = get_cpp_telemetry()
            for phase, seconds in pq_tel.items():
                if phase.endswith("_s"):
                    suite_pq_phase[phase] += seconds

            top_label, top_ns = max(op_self.items(), key=lambda x: x[1], default=("-", 0))
            summary_rows.append(
                (
                    name,
                    warm_ms,
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

    # Planning is Python and does not scale with the data, so on a slow box it is
    # a FLAT tax that a ratio against DuckDB hides. This column is what says
    # whether a query's gap is engine work or fixed cost.
    print(f"\n{'=' * 100}")
    print("PLANNING vs EXECUTION")
    print(f"{'=' * 100}\n")
    print(f"{'Query':<8} {'Min wall':>11} {'Planning':>11} {'Execution':>11} "
          f"{'Plan share':>11}   {'Dominant operator':<28} {'Share':>7}")
    print("-" * 100)
    for name, warm_ms, plan_ms, exec_ms, _self_ms, top_label, top_share in summary_rows:
        share = 100.0 * plan_ms / ((plan_ms + exec_ms) or 1)
        print(f"{name:<8} {warm_ms:>9.1f}ms {plan_ms:>9.1f}ms {exec_ms:>9.1f}ms "
              f"{share:>10.1f}%   {top_label:<28} {top_share:>6.1f}%")

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

    # Sub-phase breakdown within Grouped Aggregate (Hashed), where the table
    # above only shows it as one number. hash_s = key hashing (Pass A), probe_s
    # = hash-table find_or_insert + lane growth (Pass B), apply_s = per-aggregate-
    # function state update (Pass C). See src/cpp/engine/groupby_tel.hpp.
    gb_total = sum(suite_gb_phase.values())
    print(f"\n{'=' * 100}")
    print("GROUPED AGGREGATE PHASE BREAKDOWN (hash / probe / apply)")
    print(f"{'=' * 100}\n")
    if gb_total == 0:
        # A zero total means no grouped aggregate ran. Say that, rather than
        # dividing by a fabricated denominator and printing a table of 0.0%
        # rows under a total that was never measured.
        print("  no grouped aggregate ran in the profiled queries")
    else:
        gb_labels = {
            "hash_s": "Hash keys (A)",
            "probe_s": "Probe/insert (B)",
            "apply_s": "Apply aggs (C)",
        }
        print(f"{'Phase':<20} {'Time':>12} {'Share':>8}")
        print("-" * 42)
        for phase, seconds in sorted(suite_gb_phase.items(), key=lambda x: -x[1]):
            print(f"{gb_labels.get(phase, phase):<20} {seconds * 1000:>9.1f}ms "
                  f"{100.0 * seconds / gb_total:>6.1f}%")
        print("-" * 42)
        print(f"{'TOTAL':<20} {gb_total * 1000:>9.1f}ms")

    # Parquet decode phases - accumulated by rugo's own telemetry
    # (rugo/src/parquet/telemetry.hpp), just surfaced here.
    pq_total = sum(suite_pq_phase.values())
    print(f"\n{'=' * 100}")
    print("PARQUET READ DECODE PHASE BREAKDOWN")
    print(f"{'=' * 100}\n")
    if pq_total == 0:
        # Zero across every phase means nothing read parquet - a skene dataset is
        # the usual reason. Saying so beats a table of 0.0% shares under a
        # denominator that was never measured.
        print("  no parquet decode in the profiled queries (a skene dataset reads no parquet)")
    else:
        print(f"{'Phase':<20} {'Time':>12} {'Share':>8}")
        print("-" * 42)
        for phase, seconds in sorted(suite_pq_phase.items(), key=lambda x: -x[1]):
            print(f"{phase:<20} {seconds * 1000:>9.1f}ms {100.0 * seconds / pq_total:>6.1f}%")
        print("-" * 42)
        print(f"{'TOTAL':<20} {pq_total * 1000:>9.1f}ms")


def _load_sql_dir(path: str) -> dict[str, str]:
    """Read `query*.sql` from a directory, keyed Q01..Q22."""
    loaded: dict[str, str] = {}
    for sql_path in sorted(glob.glob(os.path.join(path, "query*.sql"))):
        stem = os.path.splitext(os.path.basename(sql_path))[0]
        if stem[5:].isdigit():
            loaded[f"Q{int(stem[5:]):02d}"] = open(sql_path).read()
    return loaded


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Standalone TPC-H per-operator profiler (no repo checkout needed)"
    )
    parser.add_argument(
        "--dataset",
        type=str,
        default=_PLACEHOLDER,
        help=f"Table prefix to substitute for `{_PLACEHOLDER}` in the SQL "
        "(e.g. `tpch_5`, `testdata.tpch_5_skene`). Default: leave the SQL as-is.",
    )
    parser.add_argument(
        "--register",
        type=str,
        default="",
        help="Register this workspace name against DiskConnector before running. "
        "Omit if the notebook already registers its own connectors.",
    )
    parser.add_argument(
        "--queries",
        type=str,
        default="",
        help="Comma-separated query names (e.g. `Q01,Q09,Q13`). Default: all available.",
    )
    parser.add_argument(
        "--sql-dir",
        type=str,
        default="",
        help="Directory of `query*.sql` to use instead of the embedded Q01/Q09/Q13.",
    )
    parser.add_argument(
        "--iterations",
        type=int,
        default=3,
        help="Untraced runs per query before the traced pass; the MINIMUM is "
        "reported (default: 3, matching the benchmark chart). The first run is "
        "cold, so 1 reports a cold number.",
    )
    args = parser.parse_args()

    if args.register:
        from opteryx.connectors import DiskConnector

        opteryx.register_workspace(args.register, DiskConnector)

    available = _load_sql_dir(args.sql_dir) if args.sql_dir else dict(_QUERIES)
    if not available:
        print(f"ERROR: no query*.sql found in {args.sql_dir}")
        return 1

    if args.queries:
        wanted = [q.strip().upper() for q in args.queries.split(",") if q.strip()]
        # An unknown name is a typo, not a query that ran zero times - refuse
        # rather than silently profiling a shorter set than was asked for.
        unknown = [q for q in wanted if q not in available]
        if unknown:
            print(f"ERROR: unknown query name(s): {', '.join(unknown)}")
            print(f"       available: {', '.join(sorted(available))}")
            return 1
    else:
        wanted = sorted(available)

    pairs = [(name, available[name].replace(_PLACEHOLDER, args.dataset)) for name in wanted]
    profile_queries(pairs, iterations=args.iterations)
    return 0


if __name__ == "__main__":
    sys.exit(main())
