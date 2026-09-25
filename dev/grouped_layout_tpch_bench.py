"""Interleaved TPC-H timing across parquet-layout arms (local disk).

    RUGO_LOCAL_MMAP_CACHE=1 python dev/grouped_layout_tpch_bench.py \\
        --arm rg256k=testdata.tpch_1_rg256k --arm grouped64k=testdata.tpch_1_grouped64k \\
        [--rounds 5] [--out results.json]

Per query, every arm runs once per round with the arm order rotated each round
(first-arm bias); one untimed warm pass per arm first; the reported figure is
each query's MINIMUM per arm, summed over the 22 queries (the design doc's
§5.5 method). Digests of every query's result are compared across arms so a
layout that changed an ANSWER is reported, not summed. Dev tooling only.
"""

import argparse
import gc
import glob
import hashlib
import json
import os
import sys
import time

REPO = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
sys.path.insert(1, REPO)
os.chdir(REPO)

QUERY_DIR = os.path.join(REPO, "tests/performance/tpch/opteryx/queries")


def load_queries(dataset):
    out = []
    for path in sorted(glob.glob(os.path.join(QUERY_DIR, "query*.sql"))):
        name = os.path.splitext(os.path.basename(path))[0]
        name = f"Q{int(name[5:]):02d}"
        body = open(path).read()
        body = body.replace("testdata.tpch_tiny.", f"{dataset}.").replace("testdata.tpch.", f"{dataset}.")
        out.append((name, body))
    return out


def run(sql):
    import opteryx

    gc.collect()
    session = opteryx.session()
    try:
        h = hashlib.sha256()
        rows = 0
        t0 = time.monotonic_ns()
        for m in session.execute_to_morsels(sql):
            rows += m.num_rows
            for name in m.column_names:
                h.update(repr(m.column(name).to_pylist()).encode())
        ms = (time.monotonic_ns() - t0) / 1e6
        return ms, rows, h.hexdigest()[:16]
    finally:
        session.close()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--arm", action="append", required=True, help="name=testdata.dotted.dataset")
    ap.add_argument("--rounds", type=int, default=3)
    ap.add_argument("--out", default="")
    args = ap.parse_args()
    arms = [a.split("=", 1) for a in args.arm]
    names = [n for n, _ in arms]
    queries = {n: load_queries(ds) for n, ds in arms}
    qnames = [q for q, _ in queries[names[0]]]
    print("mmap cache:", os.environ.get("RUGO_LOCAL_MMAP_CACHE", "(default)"), flush=True)

    digests = {}
    print("warm pass", flush=True)
    for n in names:
        for q, sql in queries[n]:
            _, rows, d = run(sql)
            digests.setdefault(q, {})[n] = (rows, d)
        print("  warmed", n, flush=True)
    mismatched = [q for q in qnames if len(set(digests[q].values())) != 1]
    if mismatched:
        print("RESULT MISMATCH across arms for", mismatched, {q: digests[q] for q in mismatched}, flush=True)

    res = {n: {q: [] for q in qnames} for n in names}
    for rnd in range(args.rounds):
        for i, q in enumerate(qnames):
            k = (rnd + i) % len(names)
            for n in names[k:] + names[:k]:
                sql = dict(queries[n])[q]
                ms, _, _ = run(sql)
                res[n][q].append(ms)
        print("round", rnd + 1, "done", flush=True)
        if args.out:
            json.dump({"res": res, "digests": digests, "mismatched": mismatched}, open(args.out, "w"))

    print(f"\n{'query':>6}" + "".join(f"{n:>16}" for n in names))
    totals = {n: 0.0 for n in names}
    for q in qnames:
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
        print("⛔ RESULT MISMATCH:", mismatched)


if __name__ == "__main__":
    main()
