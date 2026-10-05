#!/usr/bin/env python3
"""
Lay dev/ab_bench.py JSON results from several machines side by side.

Each input is one machine's run of the SAME arms (labels must match). Ratios
are computed per machine — arm median over reference-arm median — and never
across machines: absolute times on different hardware are not comparable,
only the direction and size of each arm's effect on its own box.

    python dev/arch_compare.py dev/bench_results/ab-clickbench-mac-*.json \
        dev/bench_results/ab-clickbench-opteryx-perf-*.json

Per query and arm, prints `ratio wins/rounds` for every machine, then the
suite total ratio per machine. Control queries (as recorded in the JSON) are
listed first and flagged when they move more than 3% in the same direction in
every round — that is a harness fault, not an engine effect. A sign that differs between machines is marked
with `<>`: that is the case the plan's per-architecture decisions are for.
"""

from __future__ import annotations

import json
import statistics
import sys


def _load(path: str) -> dict:
    with open(path) as f:
        doc = json.load(f)
    machine = doc["machine"]
    doc["_name"] = f"{machine['host'].split('.')[0]}/{machine['machine']}"
    return doc


def main() -> int:
    if len(sys.argv) < 2:
        print(__doc__)
        return 2
    docs = [_load(p) for p in sys.argv[1:]]
    labels = [a["label"] for a in docs[0]["arms"]]
    for doc in docs[1:]:
        other = [a["label"] for a in doc["arms"]]
        if other != labels:
            raise SystemExit(f"{doc['_name']}: arms {other} do not match {labels}")
        if doc["suite"] != docs[0]["suite"]:
            raise SystemExit(f"{doc['_name']}: suite {doc['suite']} != {docs[0]['suite']}")
    ref = labels[0]
    controls = set(docs[0]["controls"])
    queries = sorted({q for d in docs for q in d["times_ms"][ref]}, key=lambda q: (q not in controls, q))

    print("machines:")
    for doc in docs:
        m = doc["machine"]
        cpu = m.get("machdep.cpu.brand_string") or m.get("Model name", "?")
        print(f"  {doc['_name']:<32} {cpu} | {m.get('compiler', '?')} | py {m['python']} "
              f"| rounds {doc['iterations']}")
        if doc["row_count_mismatches"]:
            print(f"  !! {doc['_name']}: ROW-COUNT MISMATCH on {doc['row_count_mismatches']} — void")
    print()

    for label in labels[1:]:
        print(f"== {label} / {ref}")
        header = f"{'query':<8}" + "".join(f"{d['_name'][:22]:>24}" for d in docs)
        print(header)
        totals = {d["_name"]: [0.0, 0.0] for d in docs}
        for q in queries:
            cells, signs = [], set()
            for d in docs:
                a = d["times_ms"][ref].get(q, [])
                b = d["times_ms"][label].get(q, [])
                if not a or not b:
                    cells.append(f"{'-':>24}")
                    continue
                ratio = statistics.median(b) / statistics.median(a)
                wins = sum(1 for x, y in zip(b, a) if x < y)
                totals[d["_name"]][0] += statistics.median(a)
                totals[d["_name"]][1] += statistics.median(b)
                flag = ""
                # A control is a harness fault only if it moves CONSISTENTLY: >3% and
                # every round on the same side. Small queries on a noisy box spike
                # 2-5x in single rounds; that moves a median without being a bias.
                if q in controls and abs(ratio - 1) > 0.03 and wins in (0, len(a)):
                    flag = " CTRL!"
                if ratio < 0.97:
                    signs.add("-")
                elif ratio > 1.03:
                    signs.add("+")
                cells.append(f"{ratio:>12.3f} {wins:>2}/{len(a):<2}{flag:>6}")
            mark = " <>" if len(signs) > 1 else ""
            tag = "*" if q in controls else " "
            print(f"{q + tag:<8}" + "".join(cells) + mark)
        print(f"{'TOTAL':<8}" + "".join(
            f"{(t[1] / t[0] if t[0] else float('nan')):>12.3f}{'':>12}" for t in totals.values()))
        print()
    print("* control query   CTRL! control moved >3% in the same direction every round (harness fault)   <> sign differs between machines")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
