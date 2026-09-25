"""Interleaved ClickBench timing across parquet-layout arms (local disk).

    RUGO_LOCAL_MMAP_CACHE=1 python dev/grouped_layout_clickbench_bench.py \\
        --arm rg256k=scratch.hits_rugo_256k_ab --arm grouped64k=scratch.hits_rugo_262k \\
        [--rounds 3] [--out results.json]

Same method as dev/grouped_layout_tpch_bench.py over the 43 battery statements
in tests/performance/clickbench/opteryx/runner.py: one untimed warm pass per
arm, then N rounds with every arm per query and the arm order rotated each
round; figure = sum over queries of each query's minimum; row counts compared
across arms. Dev tooling only.
"""

import argparse
import gc
import json
import os
import sys
import time

REPO = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
sys.path.insert(1, REPO)
sys.path.insert(1, os.path.join(REPO, "tests/performance/clickbench/opteryx"))
os.chdir(REPO)
sys.argv, _saved = [sys.argv[0]], sys.argv
import runner  # noqa: E402  (module import only defines STATEMENTS; its main is guarded)
sys.argv = _saved


def run(sql):
    import opteryx

    gc.collect()
    session = opteryx.session()
    try:
        rows = 0
        t0 = time.monotonic_ns()
        for m in session.execute_to_morsels(sql):
            rows += m.num_rows
        return (time.monotonic_ns() - t0) / 1e6, rows
    finally:
        session.close()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--arm", action="append", required=True, help="name=scratch.dotted.dataset")
    ap.add_argument("--rounds", type=int, default=3)
    ap.add_argument("--out", default="")
    args = ap.parse_args()
    arms = [a.split("=", 1) for a in args.arm]
    names = [n for n, _ in arms]
    stmts = [(f"Q{i + 1:02d}", s) for i, (s, _) in enumerate(runner.STATEMENTS)]
    print("mmap cache:", os.environ.get("RUGO_LOCAL_MMAP_CACHE", "(default)"), flush=True)

    rowcounts = {}
    print("warm pass", flush=True)
    for n, ds in arms:
        for q, s in stmts:
            _, rows = run(s.replace("{DATASET}", ds))
            rowcounts.setdefault(q, {})[n] = rows
        print("  warmed", n, flush=True)
    mismatched = [q for q, _ in stmts if len(set(rowcounts[q].values())) != 1]
    if mismatched:
        print("ROW COUNT MISMATCH across arms for", mismatched, flush=True)

    res = {n: {q: [] for q, _ in stmts} for n in names}
    for rnd in range(args.rounds):
        for i, (q, s) in enumerate(stmts):
            k = (rnd + i) % len(names)
            for n in names[k:] + names[:k]:
                ds = dict(arms)[n]
                ms, _ = run(s.replace("{DATASET}", ds))
                res[n][q].append(ms)
        print("round", rnd + 1, "done", flush=True)
        if args.out:
            json.dump({"res": res, "rows": rowcounts, "mismatched": mismatched}, open(args.out, "w"))

    print(f"\n{'query':>6}" + "".join(f"{n:>16}" for n in names))
    totals = {n: 0.0 for n in names}
    for q, _ in stmts:
        row = f"{q:>6}"
        for n in names:
            best = min(res[n][q])
            totals[n] += best
            row += f"{best:16.1f}"
        print(row)
    print(f"{'SUM':>6}" + "".join(f"{totals[n]:16.1f}" for n in names))
    base = totals[names[0]]
    print(f"{'vs 1st':>6}" + "".join(f"{(totals[n] / base - 1) * 100:+15.1f}%" for n in names))
    if mismatched:
        print("⛔ ROW COUNT MISMATCH:", mismatched)


if __name__ == "__main__":
    main()
