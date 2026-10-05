#!/usr/bin/env python3
"""
Interleaved A/B benchmark harness for the low-level optimisation programme.

Runs a named subset of ClickBench / TPC-H queries N times, strictly interleaved
across two compiled working trees (A = baseline, B = candidate), and reports
per-query medians, best times, and the B/A ratio, plus optional per-operator
self-time attribution. Results land in dev/bench_results/ as CSV.

The box drifts (~30% observed), so arms interleave within every iteration and
the within-round order alternates (a fixed order penalises the second arm 1-4%).
Arms come from --a/--b or an --arms JSON file; an arm may override env vars or
substitute built extension modules (.so) in its tree, so compile-time constants
can be A/B'd from one tree. Every run also writes a JSON document carrying the
machine fingerprint (CPU, caches, compiler), so results from different
architectures can be laid side by side — compare ratios per machine, never raw
times across machines. Each (side, iteration) runs in a fresh subprocess with
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
import shutil
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


def _worker_load_job(tree: str) -> list[tuple[str, str]]:
    """[(name, sql)] from the JOB runner's own query order and table rewrite (skene)."""
    import importlib.util
    from pathlib import Path

    import opteryx
    from opteryx.connectors import DiskConnector

    opteryx.register_workspace("testdata", DiskConnector)
    path = os.path.join(tree, "tests", "performance", "job", "runner.py")
    spec = importlib.util.spec_from_file_location("job_runner", path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)  # type: ignore[union-attr]
    qdir = Path(tree) / "tests" / "performance" / "job" / "queries"
    out = []
    for qpath in sorted(qdir.glob("*.sql"), key=mod._query_sort_key):
        if not mod.QUERY_RE.match(qpath.name):
            continue
        out.append((f"J{qpath.stem}", mod._rewrite_query(qpath.read_text(), "testdata.job_skene.")))
    return out


def _query_names(spec: str, suite: str) -> set[str]:
    """--queries / --controls → result names: numbers for clickbench/tpch, JOB ids (1a,33c)."""
    if not spec:
        return set()
    if suite == "job":
        return {f"J{q.strip()}" for q in spec.split(",")}
    return {f"Q{int(q):02d}" for q in spec.split(",")}


def _worker(args: argparse.Namespace) -> None:
    import gc
    import time

    tree = os.getcwd()
    sys.path.insert(0, tree)

    import opteryx  # noqa: E402 — resolves to `tree` via the path insert above

    if args.suite == "clickbench":
        queries = _worker_load_clickbench(tree, args.dataset)
    elif args.suite == "job":
        queries = _worker_load_job(tree)
    else:
        queries = _worker_load_tpch(tree, args.scale)

    wanted = _query_names(args.queries, args.suite) or None
    if wanted is not None:
        queries = [(n, s) for n, s in queries if n in wanted]
        missing = wanted - {n for n, _ in queries}
        if missing:
            raise SystemExit(f"unknown queries for suite {args.suite}: {sorted(missing)}")

    results: dict = {"tree": tree, "queries": {}, "opteryx_file": opteryx.__file__}
    # Untimed warm-up passes: a fresh process otherwise charges first-touch costs
    # (imports, footer/metadata caches, allocator growth) to whichever query runs first.
    for _ in range(args.warmup):
        for _name, sql in queries:
            session = opteryx.session()
            try:
                for _morsel in session.execute_to_morsels(sql):
                    pass
            finally:
                session.close()
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
        # then plan_telemetry.collect_plan_telemetry overlays the native engine's
        # per-identity self-time back onto the plan nodes.
        import collections

        from opteryx.utils.plan_telemetry import collect_plan_telemetry

        for name, sql in queries:
            gc.collect()
            session = opteryx.session()
            try:
                for _ in session.execute_to_morsels(f"EXPLAIN ANALYZE {sql}"):
                    pass
                node_stats_by_nid = collect_plan_telemetry(session._plan)
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
            raise RuntimeError("jemalloc not found (brew install jemalloc) — refusing to "
                               "benchmark without the production-like allocator")
    else:
        proc = subprocess.run(
            [sys.executable, "-c", "import draken; print(draken.preload_library_path() or '')"],
            capture_output=True, text=True, timeout=30, cwd=_REPO_ROOT,
            env={**os.environ, "PYTHONPATH": _REPO_ROOT},
        )
        if proc.returncode != 0:
            raise RuntimeError(f"allocator preload lookup failed:\n{proc.stderr[-2000:]}")
        out = proc.stdout.strip()
        if not out:
            raise RuntimeError("draken.preload_library_path() returned nothing — refusing to "
                               "benchmark without the production allocator")
        env["LD_PRELOAD"] = out
        # 1000, not 100. MEASURED 2026-08-14 on the x86 repro box, full
        # ClickBench hot suite, 43/43 queries, interleaved with arm order
        # alternating per query: PURGE_DELAY=100 → 132.99s,
        # PURGE_DELAY=1000 → 119.43s. 0.898x, faster on 41/43, ZERO
        # regressions beyond 3ms. Plain glibc measures ≈ PD=1000, so 100
        # was the worst of the three settings — every A/B run through
        # this harness was measuring a ~10% handicapped configuration.
        env["MIMALLOC_PURGE_DELAY"] = "1000"
    return env


def _run_side(arm: dict, args: argparse.Namespace) -> dict:
    """One worker subprocess for `arm`; returns the parsed result document."""
    tree = arm["tree"]
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
    cmd += ["--warmup", str(args.warmup)]
    env = dict(os.environ)
    env.pop("OPTERYX_DEBUG", None)
    env["PYTHONPATH"] = tree  # tree source, never the installed wheel
    env.update(_bench_preload_env())
    env.update(arm["env"])
    proc = subprocess.run(cmd, cwd=tree, env=env, capture_output=True, text=True)
    for line in proc.stdout.splitlines():
        if line.startswith("@@AB_RESULT@@"):
            return json.loads(line[len("@@AB_RESULT@@"):])
    raise RuntimeError(
        f"worker for arm {arm['label']} in {tree} produced no result (rc {proc.returncode})"
        f"\n--- stdout ---\n{proc.stdout[-4000:]}\n--- stderr ---\n{proc.stderr[-4000:]}"
    )


def _median(values: list[float]) -> float:
    return statistics.median(values) if values else float("nan")


def _sha256(path: str) -> str:
    import hashlib

    digest = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _load_arms(args: argparse.Namespace) -> list[dict]:
    """Arms from --arms (JSON) or the legacy --a/--b pair.

    An arm is {"label", "tree", "env": {VAR: value}, "so": {relpath: built .so}}.
    `so` substitutes extension modules inside `tree` for that arm's runs only —
    the way to A/B compile-time constants without a second tree. Arms sharing a
    tree that do not override a path get the tree's ORIGINAL file (stashed at
    start, restored and hash-verified at the end).
    """
    if args.arms:
        with open(args.arms) as f:
            raw = json.load(f)
    else:
        raw = []
        if args.a:
            raw.append({"label": "A", "tree": args.a})
        raw.append({"label": "B", "tree": args.b})
    arms = []
    for item in raw:
        tree = os.path.abspath(item["tree"])
        if not os.path.exists(os.path.join(tree, "opteryx", "__init__.py")):
            raise SystemExit(f"arm {item['label']}: {tree} is not an opteryx tree")
        so = {}
        for rel, src in item.get("so", {}).items():
            src = os.path.abspath(src)
            if not os.path.isfile(src):
                raise SystemExit(f"arm {item['label']}: substitute {src} does not exist")
            if not os.path.isfile(os.path.join(tree, rel)):
                raise SystemExit(f"arm {item['label']}: {rel} is not a file in {tree}")
            so[rel] = src
        arms.append({"label": item["label"], "tree": tree,
                     "env": {k: str(v) for k, v in item.get("env", {}).items()}, "so": so})
    labels = [a["label"] for a in arms]
    if len(set(labels)) != len(labels):
        raise SystemExit(f"duplicate arm labels: {labels}")
    return arms


class _SoSwapper:
    """Installs each arm's .so substitutes before its run.

    A loaded .so must never be overwritten in place (macOS SIGKILLs later
    launches that map the old inode), so every install is unlink-then-copy,
    giving a new inode. Runs are fresh subprocesses, so nothing holds the old
    mapping across a swap.
    """

    def __init__(self, arms: list[dict], stash_dir: str):
        self.originals: dict[str, str] = {}   # abs dest -> stashed copy
        self.hashes: dict[str, str] = {}
        os.makedirs(stash_dir, exist_ok=True)
        for arm in arms:
            for rel in arm["so"]:
                dest = os.path.join(arm["tree"], rel)
                if dest in self.originals:
                    continue
                stash = os.path.join(stash_dir, f"{len(self.originals)}-{os.path.basename(rel)}")
                shutil.copy2(dest, stash)
                self.originals[dest] = stash
                self.hashes[dest] = _sha256(dest)

    def install(self, arm: dict) -> None:
        for dest, stash in self.originals.items():
            if not dest.startswith(arm["tree"] + os.sep):
                continue
            rel = os.path.relpath(dest, arm["tree"])
            src = arm["so"].get(rel, stash)
            os.unlink(dest)
            shutil.copy2(src, dest)

    def restore(self) -> None:
        for dest, stash in self.originals.items():
            os.unlink(dest)
            shutil.copy2(stash, dest)
            if _sha256(dest) != self.hashes[dest]:
                raise RuntimeError(f"restore of {dest} does not match the original — tree is on the WRONG binary")


def _machine_fingerprint() -> dict:
    """What a result depends on besides the code: CPU, caches, compiler, Python."""
    def sh(cmd: list[str]) -> str:
        proc = subprocess.run(cmd, capture_output=True, text=True)
        return proc.stdout.strip() if proc.returncode == 0 else f"<{cmd[0]} failed rc={proc.returncode}>"

    fp: dict = {
        "host": platform.node(), "system": platform.system(), "machine": platform.machine(),
        "python": sys.version.split()[0], "gil_enabled": sys._is_gil_enabled(),
        "cpu_count": os.cpu_count(),
        "cc": os.environ.get("CC", ""), "cxx": os.environ.get("CXX", ""),
        "cxxflags": os.environ.get("CXXFLAGS", ""), "cppflags": os.environ.get("CPPFLAGS", ""),
    }
    if platform.system() == "Darwin":
        for key in ("machdep.cpu.brand_string", "hw.perflevel0.logicalcpu", "hw.perflevel1.logicalcpu",
                    "hw.perflevel0.l1dcachesize", "hw.perflevel0.l2cachesize", "hw.l2cachesize",
                    "hw.l3cachesize", "hw.memsize"):
            fp[key] = sh(["sysctl", "-n", key])
        fp["compiler"] = sh(["clang", "--version"]).splitlines()[0]
    else:
        lscpu = sh(["lscpu"])
        for line in lscpu.splitlines():
            key, _, value = line.partition(":")
            if key.strip() in ("Model name", "L1d cache", "L2 cache", "L3 cache", "CPU(s)", "Flags"):
                fp[key.strip()] = value.strip()
        fp["compiler"] = sh([os.environ.get("CXX") or "g++", "--version"]).splitlines()[0]
    return fp


def _orchestrate(args: argparse.Namespace) -> None:
    arms = _load_arms(args)
    labels = [a["label"] for a in arms]
    ref = labels[0]
    controls = _query_names(args.controls, args.suite)

    times: dict[str, dict[str, list[float]]] = {label: {} for label in labels}
    rows_seen: dict[str, dict[str, set]] = {label: {} for label in labels}
    ops: dict[str, dict[str, dict[str, int]]] = {label: {} for label in labels}
    order_log: list[list[str]] = []

    os.makedirs(os.path.join(_HERE, "bench_results"), exist_ok=True)
    stamp = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
    host = platform.node().split(".")[0]
    swapper = _SoSwapper(arms, os.path.join(_HERE, "bench_results", f".so-stash-{stamp}"))
    try:
        for iteration in range(args.iterations):
            # Alternate the within-round order: a fixed order penalises whichever
            # arm runs second by 1-4% (measured 2026-08-12).
            ordered = arms if iteration % 2 == 0 else list(reversed(arms))
            order_log.append([a["label"] for a in ordered])
            for arm in ordered:
                swapper.install(arm)
                doc = _run_side(arm, args)
                label = arm["label"]
                for qname, q in doc["queries"].items():
                    times[label].setdefault(qname, []).append(q["ms"])
                    rows_seen[label].setdefault(qname, set()).add(q["rows"])
                    for op, ns in q.get("operators_ns", {}).items():
                        ops[label].setdefault(qname, {}).setdefault(op, 0)
                        ops[label][qname][op] += ns
                total = sum(v["ms"] for v in doc["queries"].values())
                print(f"  round {iteration + 1}/{args.iterations} [{label}] total={total:.1f}ms", flush=True)
    finally:
        swapper.restore()

    # Row-count cross-check: identical inputs must yield identical row counts.
    mismatches = []
    for qname in times[ref]:
        seen = {label: rows_seen[label].get(qname) for label in labels}
        if len({frozenset(v) for v in seen.values() if v}) > 1:
            mismatches.append(qname)
            print(f"!! ROW-COUNT MISMATCH on {qname}: {seen} — result difference, timing comparison is void")

    all_queries = sorted({q for s in times.values() for q in s})
    ordered_queries = sorted(all_queries, key=lambda q: (q not in controls, q))
    base = f"ab-{args.suite}-{host}-{stamp}"
    csv_path = os.path.join(_HERE, "bench_results", base + ".csv")
    json_path = os.path.join(_HERE, "bench_results", base + ".json")
    with open(csv_path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["query", "side", "median_ms", "min_ms", "runs_ms", "top_operator", "top_operator_ms_total"])
        for qname in all_queries:
            for label in labels:
                runs = times[label].get(qname, [])
                top_op, top_ns = "", 0
                for op, ns in ops[label].get(qname, {}).items():
                    if ns > top_ns:
                        top_op, top_ns = op, ns
                w.writerow([qname, label, f"{_median(runs):.3f}", f"{min(runs):.3f}" if runs else "",
                            ";".join(f"{r:.3f}" for r in runs), top_op, f"{top_ns / 1e6:.3f}"])
    with open(json_path, "w") as f:
        json.dump({
            "suite": args.suite, "scale": args.scale, "dataset": args.dataset, "queries": args.queries,
            "iterations": args.iterations, "warmup": args.warmup, "controls": sorted(controls), "order": order_log,
            "arms": [{"label": a["label"], "tree": a["tree"], "env": a["env"],
                      "so": {rel: {"src": src, "sha256": _sha256(src)} for rel, src in a["so"].items()}}
                     for a in arms],
            "machine": _machine_fingerprint(), "row_count_mismatches": mismatches,
            "times_ms": times,
        }, f, indent=1)

    print(f"\n{'Query':<9}", end="")
    for label in labels:
        print(f"{label[:10] + ' med':>15}", end="")
    for label in labels[1:]:
        print(f"{label[:8] + '/' + ref[:6]:>17}", end="")
    print()
    for qname in ordered_queries:
        tag = "*" if qname in controls else " "
        print(f"{qname + tag:<9}", end="")
        medians = {label: _median(times[label].get(qname, [])) for label in labels}
        for label in labels:
            print(f"{medians[label]:>15.2f}", end="")
        for label in labels[1:]:
            ratio = medians[label] / medians[ref] if medians[ref] else float("nan")
            # paired wins: rounds in which this arm beat the reference
            pairs = list(zip(times[label].get(qname, []), times[ref].get(qname, [])))
            wins = sum(1 for b, a in pairs if b < a)
            print(f"{ratio:>11.3f} {wins:>2}/{len(pairs):<2}", end="")
        print()
    if controls:
        print("(* = control query: must read ~1.000 — if it does not, the harness is wrong, not the engine)")

    if args.profile and len(labels) == 2:
        a_label, b_label = labels
        print(f"\nPer-operator self-time delta (summed over runs, {b_label} − {a_label}, top movers):")
        for qname in all_queries:
            deltas = []
            for op in set(ops[a_label].get(qname, {})) | set(ops[b_label].get(qname, {})):
                a_ns = ops[a_label].get(qname, {}).get(op, 0)
                b_ns = ops[b_label].get(qname, {}).get(op, 0)
                deltas.append((b_ns - a_ns, op, a_ns, b_ns))
            deltas.sort(key=lambda d: abs(d[0]), reverse=True)
            for delta, op, a_ns, b_ns in deltas[:3]:
                print(f"  {qname} {op:<32} {a_label}={a_ns / 1e6:>9.2f}ms  {b_label}={b_ns / 1e6:>9.2f}ms  "
                      f"Δ={delta / 1e6:>+9.2f}ms")

    print(f"\nresults: {csv_path}\n         {json_path}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--suite", choices=("clickbench", "tpch", "job"), required=True)
    parser.add_argument("--queries", default="", help="comma-separated query numbers, e.g. 6,19,21 (empty = all)")
    parser.add_argument("--a", default=None, help="baseline tree root (omit for single-tree timing)")
    parser.add_argument("--b", default=_REPO_ROOT, help="candidate tree root (default: this repo)")
    parser.add_argument("--iterations", type=int, default=5, help="interleaved rounds per side (default 5)")
    parser.add_argument("--scale", default="1", help="TPC-H scale suffix (testdata/tpch_<scale>, default 1)")
    parser.add_argument("--dataset", default=None, help="ClickBench dataset override (default: runner's DATASET)")
    parser.add_argument("--profile", action="store_true", help="add an EXPLAIN ANALYZE attribution pass per run")
    parser.add_argument("--arms", default=None,
                        help="JSON file: [{label, tree, env?, so?}] — overrides --a/--b; first arm is the reference")
    parser.add_argument("--controls", default="",
                        help="query numbers the change provably cannot affect; reported first, must read ~1.000")
    parser.add_argument("--warmup", type=int, default=1,
                        help="untimed passes over the query list inside each worker before timing (default 1)")
    parser.add_argument("--worker", action="store_true", help=argparse.SUPPRESS)
    args = parser.parse_args()

    if args.worker:
        _worker(args)
    else:
        _orchestrate(args)


if __name__ == "__main__":
    main()
