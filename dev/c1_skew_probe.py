#!/usr/bin/env python3
"""
C1 ceiling probe (docs/ARCH_AWARE_PERFORMANCE_TEST_PLAN.md): how much wall time
does uneven work distribution cost on the parquet scan path?

For each ClickBench statement (rugo 262k-row-group parquet by default) or TPC-H
query (testdata/tpch_<scale>), after an
untimed warm pass, runs N timed passes and reports the median of:

  wall_ms      query wall time
  proc_cores   whole-process CPU / wall, from getrusage — includes the rugo
               decode-pool threads, which engine telemetry does not see
  dop          engine workers
  idle_ms      barrier idle summed over pipelines (worker-time spent waiting
               for the slowest worker at the end of each pipeline)
  ceiling      idle_ms / dop / wall_ms — the wall share a PERFECT distribution
               of the same work could remove. An upper bound for C1: finer
               decode claims can only reclaim barrier idle, never more.

Run from a tree root (PYTHONPATH pinned to it, as dev/ab_bench.py does):

    PYTHONPATH=. python dev/c1_skew_probe.py [--passes 3] [--queries 9,10,33]
        [--suite clickbench --dataset scratch.hits_rugo_262k | --suite tpch --scale 1]
"""

from __future__ import annotations

import argparse
import gc
import importlib.util
import json
import os
import resource
import statistics
import sys
import time


def _statements(tree: str, dataset: str | None) -> list[tuple[str, str]]:
    path = os.path.join(tree, "tests", "performance", "clickbench", "opteryx", "runner.py")
    spec = importlib.util.spec_from_file_location("cb_runner", path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)  # type: ignore[union-attr]
    ds = dataset or mod.DATASET.value
    return [(f"Q{i + 1:02d}", s.replace("{DATASET}", ds)) for i, (s, _e) in enumerate(mod.STATEMENTS)]


def _tpch(tree: str, scale: str) -> list[tuple[str, str]]:
    """The TPC-H runner's query files against testdata.tpch_<scale> (same rewrite as dev/ab_bench.py)."""
    import glob

    import opteryx
    from opteryx.connectors import DiskConnector

    opteryx.register_workspace("testdata", DiskConnector)
    out = []
    for path in sorted(glob.glob(os.path.join(tree, "tests", "performance", "tpch", "opteryx", "queries", "query*.sql"))):
        name = os.path.splitext(os.path.basename(path))[0]
        body = open(path).read()
        body = body.replace("testdata.tpch_tiny.", f"testdata.tpch_{scale}.").replace("testdata.tpch.", f"testdata.tpch_{scale}.")
        out.append((f"Q{int(name[5:]):02d}", body))
    return out


def _cpu_s() -> float:
    r = resource.getrusage(resource.RUSAGE_SELF)
    return r.ru_utime + r.ru_stime


def _run(opteryx, sql: str) -> dict:
    gc.collect()
    session = opteryx.session()
    try:
        c0, t0 = _cpu_s(), time.monotonic_ns()
        for _m in session.execute_to_morsels(sql):
            pass
        wall_ns = time.monotonic_ns() - t0
        cpu_s = _cpu_s() - c0
        pipes = session.telemetry["native_pipeline_stats"]
    finally:
        session.close()
    dop = max((p["dop"] for p in pipes), default=0)
    idle_ns = sum(p["barrier_idle_time"] for p in pipes)
    return {
        "wall_ms": wall_ns / 1e6,
        "proc_cores": cpu_s / (wall_ns / 1e9),
        "dop": dop,
        "idle_ms": idle_ns / 1e6,
        "ceiling": (idle_ns / dop / wall_ns) if dop and wall_ns else 0.0,
        "pipes": len(pipes),
    }


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--passes", type=int, default=3)
    ap.add_argument("--queries", default="")
    ap.add_argument("--dataset", default=None, help="ClickBench dataset override")
    ap.add_argument("--suite", choices=("clickbench", "tpch"), default="clickbench")
    ap.add_argument("--scale", default="1", help="TPC-H: testdata/tpch_<scale> (e.g. 1, 10, 1_skene)")
    ap.add_argument("--json", default=None, help="write per-query medians here")
    args = ap.parse_args()

    tree = os.getcwd()
    sys.path.insert(0, tree)
    import opteryx

    if not opteryx.__file__.startswith(tree):
        raise SystemExit(f"opteryx resolved to {opteryx.__file__}, not this tree ({tree})")

    stmts = _statements(tree, args.dataset) if args.suite == "clickbench" else _tpch(tree, args.scale)
    if args.queries:
        wanted = {f"Q{int(q):02d}" for q in args.queries.split(",")}
        stmts = [s for s in stmts if s[0] in wanted]

    for _name, sql in stmts:  # warm pass
        _run(opteryx, sql)

    results = {}
    print(f"{'query':<6}{'wall_ms':>10}{'cores':>7}{'dop':>5}{'idle_ms':>10}{'ceiling':>9}{'pipes':>6}")
    tot_wall = tot_save = 0.0
    for name, sql in stmts:
        runs = [_run(opteryx, sql) for _ in range(args.passes)]
        med = {k: statistics.median(r[k] for r in runs) for k in runs[0]}
        results[name] = med
        tot_wall += med["wall_ms"]
        tot_save += med["ceiling"] * med["wall_ms"]
        print(f"{name:<6}{med['wall_ms']:>10.1f}{med['proc_cores']:>7.2f}{med['dop']:>5.0f}"
              f"{med['idle_ms']:>10.1f}{med['ceiling']:>8.1%}{med['pipes']:>6.0f}")
    print(f"\nsuite wall {tot_wall:.0f} ms; perfect-distribution ceiling {tot_save:.0f} ms "
          f"= {tot_save / tot_wall:.1%} of suite wall (cpu_count={os.cpu_count()})")
    if args.json:
        with open(args.json, "w") as f:
            json.dump(results, f, indent=1)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
