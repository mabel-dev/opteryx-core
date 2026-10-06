#!/usr/bin/env python3
"""
Where does a suite's time go? Per query and suite-wide, from the native trace:
thread-time in rugo DECODE spans (row-group read + decompress + decode, on the
decode pool), engine OP_EXEC per operator, SINK per sink, COMBINE/FINALIZE, and
IO/queue waits, against query wall time and whole-process CPU (getrusage).

Thread-time, not wall: categories run concurrently on many threads, so the
columns add to CPU-ish time, not wall. Their SHARES are the ceiling map — a
change to one category can at most remove its share of the work.

Decompress vs decode is NOT split: rugo emits no decode_phase spans (the
category is defined, nothing produces it). Use perf for that split.

    PYTHONPATH=. python dev/suite_time_split.py --suite clickbench [--queries ...]
    PYTHONPATH=. python dev/suite_time_split.py --suite tpch --scale 10_skene
"""

from __future__ import annotations

import argparse
import collections
import gc
import importlib.util
import json
import os
import resource
import sys
import time


def _clickbench(tree: str) -> list[tuple[str, str]]:
    path = os.path.join(tree, "tests", "performance", "clickbench", "opteryx", "runner.py")
    spec = importlib.util.spec_from_file_location("cb_runner", path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)  # type: ignore[union-attr]
    ds = mod.DATASET.value
    return [(f"Q{i + 1:02d}", s.replace("{DATASET}", ds)) for i, (s, _e) in enumerate(mod.STATEMENTS)]


def _tpch(tree: str, scale: str) -> list[tuple[str, str]]:
    import glob

    import opteryx
    from opteryx.connectors import DiskConnector

    opteryx.register_workspace("testdata", DiskConnector)
    out = []
    qdir = os.path.join(tree, "tests", "performance", "tpch", "opteryx", "queries")
    for path in sorted(glob.glob(os.path.join(qdir, "query*.sql"))):
        name = os.path.splitext(os.path.basename(path))[0]
        body = open(path).read()
        body = body.replace("testdata.tpch_tiny.", f"testdata.tpch_{scale}.").replace("testdata.tpch.", f"testdata.tpch_{scale}.")
        out.append((f"Q{int(name[5:]):02d}", body))
    return out


def _cpu_s() -> float:
    r = resource.getrusage(resource.RUSAGE_SELF)
    return r.ru_utime + r.ru_stime


def _bucket(span: dict) -> str:
    cat = span["type"]
    node = span["operator_name"] or ""
    if cat in ("op_exec", "sink"):
        return f"{cat}:{node}" if node else cat
    return cat


def _run(opteryx, interpret_trace, sql: str, traced: bool) -> dict:
    gc.collect()
    session = opteryx.session()
    try:
        if traced:
            for _ in session.execute_to_morsels("SET trace TO true"):
                pass
        c0, t0 = _cpu_s(), time.monotonic_ns()
        for _m in session.execute_to_morsels(sql):
            pass
        wall_ns = time.monotonic_ns() - t0
        cpu_s = _cpu_s() - c0
        buckets: dict = collections.defaultdict(int)
        if traced:
            blob, nodes, files, *_rest = session.trace()
            for sp in interpret_trace(blob, nodes, files):
                buckets[_bucket(sp)] += sp["duration_ns"]
    finally:
        session.close()
    return {"wall_ms": wall_ns / 1e6, "cpu_ms": cpu_s * 1e3, "buckets_ms": {k: v / 1e6 for k, v in buckets.items()}}


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--suite", choices=("clickbench", "tpch"), required=True)
    ap.add_argument("--scale", default="10_skene")
    ap.add_argument("--queries", default="")
    ap.add_argument("--json", default=None)
    args = ap.parse_args()

    tree = os.getcwd()
    sys.path.insert(0, tree)
    import opteryx
    from opteryx.tracing.spans import interpret_trace

    if not opteryx.__file__.startswith(tree):
        raise SystemExit(f"opteryx resolved to {opteryx.__file__}, not this tree ({tree})")
    stmts = _clickbench(tree) if args.suite == "clickbench" else _tpch(tree, args.scale)
    if args.queries:
        wanted = {f"Q{int(q):02d}" for q in args.queries.split(",")}
        stmts = [s for s in stmts if s[0] in wanted]

    results = {}
    suite = collections.defaultdict(float)
    tot_wall = tot_cpu = 0.0
    for name, sql in stmts:
        _run(opteryx, interpret_trace, sql, traced=False)          # warm
        untraced = _run(opteryx, interpret_trace, sql, traced=False)
        traced = _run(opteryx, interpret_trace, sql, traced=True)
        results[name] = {"untraced": untraced, "traced": traced}
        tot_wall += untraced["wall_ms"]
        tot_cpu += untraced["cpu_ms"]
        b = traced["buckets_ms"]
        for k, v in b.items():
            suite[k] += v
        top = sorted(b.items(), key=lambda kv: -kv[1])[:4]
        print(f"{name}: wall {untraced['wall_ms']:.0f} ms (traced {traced['wall_ms']:.0f}), cpu {untraced['cpu_ms']:.0f} ms | "
              + ", ".join(f"{k} {v:.0f}" for k, v in top), flush=True)

    total = sum(suite.values())
    print(f"\nSUITE: wall {tot_wall:.0f} ms, process cpu {tot_cpu:.0f} ms, traced span time {total:.0f} ms")
    print(f"{'bucket':<52}{'ms':>10}{'share':>8}")
    for k, v in sorted(suite.items(), key=lambda kv: -kv[1]):
        if v / total < 0.002:
            continue
        print(f"{k:<52}{v:>10.0f}{v / total:>8.1%}")
    if args.json:
        with open(args.json, "w") as f:
            json.dump({"suite": dict(suite), "queries": results}, f, indent=1)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
