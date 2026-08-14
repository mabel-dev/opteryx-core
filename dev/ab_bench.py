#!/usr/bin/env python3
"""
Interleaved A/B benchmark harness for the low-level optimisation programme.

Runs a named subset of ClickBench / TPC-H queries N times, strictly interleaved
across two compiled working trees (A = baseline, B = candidate), and reports
per-query medians, best times, and the B/A ratio, plus optional per-operator
self-time attribution. Results land in dev/bench_results/ as CSV.

The box drifts (~30% observed), so A and B alternate within every iteration —
never sequential blocks. Each (side, iteration) runs in a fresh subprocess with
PYTHONPATH pinned to that tree (never the installed opteryx) and the same
allocator preload the Makefile bench targets use.

Usage:
    # Compare this tree against a baseline tree, ClickBench Q06/Q19/Q21, 5 rounds
    python dev/ab_bench.py --suite clickbench --queries 6,19,21 \
        --a /path/to/baseline-tree --b . --iterations 5

    # TPC-H Q1/Q6 with per-operator attribution
    python dev/ab_bench.py --suite tpch --queries 1,6 --a ../opteryx-base --b . --profile

    # Single-tree timing (no comparison): omit --a
    python dev/ab_bench.py --suite tpch --queries 6 --b . --iterations 3

Both trees must already be compiled (`make compile` / `make c`) — the harness
never builds. Query text comes from the existing runners
(tests/performance/clickbench/opteryx/runner.py STATEMENTS, and
tests/performance/tpch/opteryx/queries/*.sql), so the harness cannot drift from
the suites it claims to run.
"""

from __future__ import annotations

import argparse
import csv
import datetime
import json
import os
import platform
import statistics
import subprocess
import sys

_HERE = os.path.dirname(os.path.abspath(__file__))
_REPO_ROOT = os.path.abspath(os.path.join(_HERE, ".."))


# ---------------------------------------------------------------------------
# Worker mode — runs inside ONE tree, prints one JSON document to stdout.
# ---------------------------------------------------------------------------


def _worker_load_clickbench(tree: str, dataset: str | None) -> list[tuple[str, str]]:
    """[(name, sql)] from the ClickBench runner's STATEMENTS, dataset substituted."""
    import importlib.util

    path = os.path.join(tree, "tests", "performance", "clickbench", "opteryx", "runner.py")
    spec = importlib.util.spec_from_file_location("cb_runner", path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)  # type: ignore[union-attr]
    ds = dataset or mod.DATASET.value
    out = []
    for index, (statement, _err) in enumerate(mod.STATEMENTS):
        out.append((f"Q{index + 1:02d}", statement.replace("{DATASET}", ds)))
    return out


def _worker_load_tpch(tree: str, scale: str) -> list[tuple[str, str]]:
    """[(name, sql)] mirroring tests/performance/tpch/runner.py's loader."""
    import glob as _glob

    import opteryx
    from opteryx.connectors import DiskConnector

    opteryx.register_workspace("testdata", DiskConnector)
    qdir = os.path.join(tree, "tests", "performance", "tpch", "opteryx", "queries")
    dataset = f"testdata.tpch_{scale}"
    queries = []
    for path in sorted(_glob.glob(os.path.join(qdir, "query*.sql"))):
        name = os.path.splitext(os.path.basename(path))[0]
        if name.startswith("query") and name[5:].isdigit():
            name = f"Q{int(name[5:]):02d}"
        body = open(path).read()
        body = body.replace("testdata.tpch_tiny.", f"{dataset}.")
        body = body.replace("testdata.tpch.", f"{dataset}.")
        queries.append((name, body))
    return queries


def _worker(args: argparse.Namespace) -> None:
    import gc
    import time

    tree = os.getcwd()
    sys.path.insert(0, tree)

    import opteryx  # noqa: E402 — resolves to `tree` via the path insert above

    if args.suite == "clickbench":
        queries = _worker_load_clickbench(tree, args.dataset)
    else:
        queries = _worker_load_tpch(tree, args.scale)

    wanted = {f"Q{int(q):02d}" for q in args.queries.split(",")} if args.queries else None
    if wanted is not None:
        queries = [(n, s) for n, s in queries if n in wanted]
        missing = wanted - {n for n, _ in queries}
        if missing:
            raise SystemExit(f"unknown queries for suite {args.suite}: {sorted(missing)}")

    results: dict = {"tree": tree, "queries": {}, "opteryx_file": opteryx.__file__}
    for name, sql in queries:
        gc.collect()
        session = opteryx.session()
        try:
            rows = 0
            t0 = time.monotonic_ns()
            for morsel in session.execute_to_morsels(sql):
                if morsel is not None:
                    rows += morsel.num_rows
            elapsed_ms = (time.monotonic_ns() - t0) / 1e6
            results["queries"][name] = {"ms": elapsed_ms, "rows": rows}
        finally:
            session.close()

    if args.profile:
        # Separate tracing pass — the timed numbers above stay tracing-free.
        # Same mechanism as the ClickBench runner's --profile: EXPLAIN ANALYZE,
        # then mermaid._collect_node_stats overlays the native engine's
        # per-identity self-time back onto the plan nodes.
        import collections

        from opteryx.utils import mermaid as _mermaid

        for name, sql in queries:
            gc.collect()
            session = opteryx.session()
            try:
                for _ in session.execute_to_morsels(f"EXPLAIN ANALYZE {sql}"):
                    pass
                node_stats_by_nid, _, _ = _mermaid._collect_node_stats(session._plan)
                op_self = collections.defaultdict(int)
                for nid in session._plan.nodes():
                    node = session._plan[nid]
                    if node is None or node.name in ("Explain", "Exit"):
                        continue
                    stat = node_stats_by_nid.get(nid)
                    if stat is not None:
                        op_self[node.name] += stat.get("self_time", 0)
                results["queries"][name]["operators_ns"] = dict(op_self)
            finally:
                session.close()

    print("@@AB_RESULT@@" + json.dumps(results))


# ---------------------------------------------------------------------------
# Orchestrator mode
# ---------------------------------------------------------------------------


def _bench_preload_env() -> dict[str, str]:
    """Replicate the Makefile's BENCH_PRELOAD allocator setup for this platform."""
    env: dict[str, str] = {}
    if platform.system() == "Darwin":
        for cand in ("/opt/homebrew/lib/libjemalloc.dylib", "/usr/local/lib/libjemalloc.dylib"):
            if os.path.exists(cand):
                env["DYLD_INSERT_LIBRARIES"] = cand
                break
    else:
        try:
            out = subprocess.run(
                [sys.executable, "-c", "import draken; print(draken.preload_library_path() or '')"],
                capture_output=True, text=True, timeout=30,
            ).stdout.strip()
            if out:
                env["LD_PRELOAD"] = out
                # 1000, not 100. MEASURED 2026-08-14 on the x86 repro box, full
                # ClickBench hot suite, 43/43 queries, interleaved with arm order
                # alternating per query: PURGE_DELAY=100 → 132.99s,
                # PURGE_DELAY=1000 → 119.43s. 0.898x, faster on 41/43, ZERO
                # regressions beyond 3ms. Plain glibc measures ≈ PD=1000, so 100
                # was the worst of the three settings — every A/B run through
                # this harness was measuring a ~10% handicapped configuration.
                env["MIMALLOC_PURGE_DELAY"] = "1000"
        except Exception:
            pass
    return env


def _run_side(tree: str, args: argparse.Namespace) -> dict:
    """One worker subprocess in `tree`; returns the parsed result document."""
    cmd = [
        sys.executable, os.path.abspath(__file__), "--worker",
        "--suite", args.suite, "--scale", args.scale,
    ]
    if args.queries:
        cmd += ["--queries", args.queries]
    if args.dataset:
        cmd += ["--dataset", args.dataset]
    if args.profile:
        cmd += ["--profile"]
    env = dict(os.environ)
    env.pop("OPTERYX_DEBUG", None)
    env["PYTHONPATH"] = tree  # tree source, never the installed wheel
    env.update(_bench_preload_env())
    proc = subprocess.run(cmd, cwd=tree, env=env, capture_output=True, text=True)
    for line in proc.stdout.splitlines():
        if line.startswith("@@AB_RESULT@@"):
            return json.loads(line[len("@@AB_RESULT@@"):])
    raise RuntimeError(
        f"worker in {tree} produced no result\n--- stdout ---\n{proc.stdout[-4000:]}"
        f"\n--- stderr ---\n{proc.stderr[-4000:]}"
    )


def _median(values: list[float]) -> float:
    return statistics.median(values) if values else float("nan")


def _orchestrate(args: argparse.Namespace) -> None:
    sides: list[tuple[str, str]] = []
    if args.a:
        sides.append(("A", os.path.abspath(args.a)))
    sides.append(("B", os.path.abspath(args.b)))
    for label, tree in sides:
        if not os.path.exists(os.path.join(tree, "opteryx", "__init__.py")):
            raise SystemExit(f"side {label}: {tree} is not an opteryx tree")

    # side -> query -> [ms per iteration]; and operator self-time accumulation
    times: dict[str, dict[str, list[float]]] = {label: {} for label, _ in sides}
    rows_seen: dict[str, dict[str, set]] = {label: {} for label, _ in sides}
    ops: dict[str, dict[str, dict[str, int]]] = {label: {} for label, _ in sides}

    for iteration in range(args.iterations):
        for label, tree in sides:  # strict interleave: A,B within every round
            doc = _run_side(tree, args)
            for qname, q in doc["queries"].items():
                times[label].setdefault(qname, []).append(q["ms"])
                rows_seen[label].setdefault(qname, set()).add(q["rows"])
                for op, ns in q.get("operators_ns", {}).items():
                    ops[label].setdefault(qname, {}).setdefault(op, 0)
                    ops[label][qname][op] += ns
            done = ", ".join(f"{q}={v['ms']:.1f}ms" for q, v in doc["queries"].items())
            print(f"  round {iteration + 1}/{args.iterations} [{label}] {done}")

    # Row-count cross-check: identical inputs must yield identical row counts.
    if len(sides) == 2:
        for qname in times["A"]:
            ra, rb = rows_seen["A"].get(qname), rows_seen["B"].get(qname)
            if ra and rb and ra != rb:
                print(f"!! ROW-COUNT MISMATCH on {qname}: A={sorted(ra)} B={sorted(rb)} "
                      f"— result difference, timing comparison is void")

    os.makedirs(os.path.join(_HERE, "bench_results"), exist_ok=True)
    stamp = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
    out_path = os.path.join(_HERE, "bench_results", f"ab-{args.suite}-{stamp}.csv")

    all_queries = sorted({q for s in times.values() for q in s})
    with open(out_path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["query", "side", "median_ms", "min_ms", "runs_ms", "top_operator", "top_operator_ms_total"])
        for qname in all_queries:
            for label, _tree in sides:
                runs = times[label].get(qname, [])
                top_op, top_ns = "", 0
                for op, ns in ops[label].get(qname, {}).items():
                    if ns > top_ns:
                        top_op, top_ns = op, ns
                w.writerow([qname, label, f"{_median(runs):.3f}", f"{min(runs):.3f}" if runs else "",
                            ";".join(f"{r:.3f}" for r in runs), top_op, f"{top_ns / 1e6:.3f}"])

    print(f"\n{'Query':<8}", end="")
    for label, _ in sides:
        print(f"{label + ' median':>12}{label + ' min':>10}", end="")
    if len(sides) == 2:
        print(f"{'B/A':>8}", end="")
    print()
    for qname in all_queries:
        print(f"{qname:<8}", end="")
        medians = {}
        for label, _ in sides:
            runs = times[label].get(qname, [])
            medians[label] = _median(runs)
            print(f"{medians[label]:>11.2f} {min(runs) if runs else float('nan'):>9.2f}", end="")
        if len(sides) == 2 and medians.get("A"):
            print(f"{medians['B'] / medians['A']:>8.3f}", end="")
        print()

    if args.profile and len(sides) == 2:
        print("\nPer-operator self-time delta (summed over runs, B − A, top movers):")
        for qname in all_queries:
            deltas = []
            for op in set(ops["A"].get(qname, {})) | set(ops["B"].get(qname, {})):
                a_ns = ops["A"].get(qname, {}).get(op, 0)
                b_ns = ops["B"].get(qname, {}).get(op, 0)
                deltas.append((b_ns - a_ns, op, a_ns, b_ns))
            deltas.sort(key=lambda d: abs(d[0]), reverse=True)
            for delta, op, a_ns, b_ns in deltas[:3]:
                print(f"  {qname} {op:<32} A={a_ns / 1e6:>9.2f}ms  B={b_ns / 1e6:>9.2f}ms  "
                      f"Δ={delta / 1e6:>+9.2f}ms")

    print(f"\nresults: {out_path}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--suite", choices=("clickbench", "tpch"), required=True)
    parser.add_argument("--queries", default="", help="comma-separated query numbers, e.g. 6,19,21 (empty = all)")
    parser.add_argument("--a", default=None, help="baseline tree root (omit for single-tree timing)")
    parser.add_argument("--b", default=_REPO_ROOT, help="candidate tree root (default: this repo)")
    parser.add_argument("--iterations", type=int, default=5, help="interleaved rounds per side (default 5)")
    parser.add_argument("--scale", default="1", help="TPC-H scale suffix (testdata/tpch_<scale>, default 1)")
    parser.add_argument("--dataset", default=None, help="ClickBench dataset override (default: runner's DATASET)")
    parser.add_argument("--profile", action="store_true", help="add an EXPLAIN ANALYZE attribution pass per run")
    parser.add_argument("--worker", action="store_true", help=argparse.SUPPRESS)
    args = parser.parse_args()

    if args.worker:
        _worker(args)
    else:
        _orchestrate(args)


if __name__ == "__main__":
    main()
