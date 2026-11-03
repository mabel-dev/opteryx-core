#!/usr/bin/env python3
"""
Fit the plan-time execution-time tree (opteryx/planner/time_estimate.py) from
production job docs, score it, and write opteryx/planner/time_estimate_tree.py.

Two steps, so a fit can be repeated without re-reading Firestore:

    # 1. export COMPLETED jobs (read-only) to JSONL
    python fit_time_bins.py export --project mabeldev --out jobs.jsonl

    # 2. fit, score on a time split, and (with --write) regenerate the tree
    python fit_time_bins.py fit jobs.jsonl --write

Needs scikit-learn (fit) and google-cloud-firestore (export); neither is an
engine dependency, and the engine only ever reads the generated tree.

Features come from `time_estimate.plan_features` - the function the engine
serves with - loaded from its file so this script needs no built opteryx. The
target is the job's `time_total`. The split is by time (train on the earliest
70%, test on the rest): a random split would put repeats of the same scheduled
statement on both sides and flatter the score.
"""

import argparse
import importlib.util
import json
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
PLANNER = os.path.join(HERE, "..", "..", "..", "opteryx", "planner")


def _load_time_estimate():
    spec = importlib.util.spec_from_file_location("time_estimate", os.path.join(PLANNER, "time_estimate.py"))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def export(args):
    from google.cloud import firestore

    db = firestore.Client(project=args.project, database=args.database)
    query = (
        db.collection("jobs")
        .where(filter=firestore.FieldFilter("status", "==", "COMPLETED"))
        .select(["sql_text", "finished_at", "telemetry"])
    )
    n = 0
    with open(args.out, "w") as out:
        for doc in query.stream(timeout=1200):
            job = doc.to_dict()
            finished = job.get("finished_at")
            job["finished_at"] = finished.isoformat() if finished is not None else None
            job["id"] = doc.id
            out.write(json.dumps(job, default=str) + "\n")
            n += 1
    print(f"exported {n} jobs to {args.out}")


def _bin_index(seconds, bins):
    return 0 if seconds < 1 else 1 if seconds < 5 else 2 if seconds < 15 else 3


def _rows(path, te):
    rows = []
    for line in open(path):
        job = json.loads(line)
        t = job.get("telemetry") or {}
        if t.get("time_total") is None or not job.get("finished_at"):
            continue
        features = te.plan_features(
            time_planning_s=t.get("time_planning") or 0,
            billing_bytes=t.get("billing_bytes") or 0,
            estimated_row_counts=t.get("estimated_row_counts"),
            join_estimates=t.get("join_estimates"),
            sql=job.get("sql_text") or "",
        )
        rows.append((job["finished_at"], job.get("sql_text") or "", t["time_total"], features))
    rows.sort(key=lambda r: r[0])
    return rows


def _tree_literal(tree, names, bins):
    t = tree.tree_

    def node(i):
        if t.children_left[i] == -1:
            return bins[int(t.value[i][0].argmax())]
        return (names[t.feature[i]], round(float(t.threshold[i]), 6), node(t.children_left[i]), node(t.children_right[i]))

    return node(0)


def _collapse(node):
    """Merge a split whose two sides answer the same - sklearn keeps those."""
    if isinstance(node, str):
        return node
    feature, threshold, le, gt = node
    le, gt = _collapse(le), _collapse(gt)
    return le if le == gt else (feature, threshold, le, gt)


def fit(args):
    import numpy as np
    from sklearn.tree import DecisionTreeClassifier

    te = _load_time_estimate()
    bins = te.BINS
    rows = _rows(args.jobs, te)
    names = list(rows[0][3])
    X = np.array([[r[3][n] for n in names] for r in rows], dtype=float)
    y = np.array([_bin_index(r[2], bins) for r in rows])
    cut = int(len(rows) * 0.7)
    seen = {r[1] for r in rows[:cut]}
    unseen = np.array([r[1] not in seen for r in rows[cut:]])

    model = DecisionTreeClassifier(max_depth=args.depth, min_samples_leaf=args.min_leaf, random_state=0)
    model.fit(X[:cut], y[:cut])
    tree = _collapse(_tree_literal(model, names, bins))
    # Score the literal the engine will serve, not the sklearn object.
    pred = np.array([bins.index(te.estimate_time_bin(dict(zip(names, x)), tree)) for x in X[cut:]])
    truth = y[cut:]
    mode = np.bincount(y[:cut]).argmax()

    def line(label, p, t):
        return f"  {label:28s} exact {(p == t).mean():6.1%}  within one bin {(abs(p - t) <= 1).mean():6.1%}"

    print(f"{len(rows)} jobs, {rows[0][0][:10]} .. {rows[-1][0][:10]}; test = last {len(rows) - cut} ({rows[cut][0][:10]} on)")
    print(line(f"always {bins[mode]!r}", np.full(len(truth), mode), truth))
    print(line(f"tree depth {args.depth}", pred, truth))
    print(line("  ...SQL unseen in training", pred[unseen], truth[unseen]))
    print("\n  actual \\ predicted  " + "  ".join(f"{b:>6s}" for b in bins))
    for i, b in enumerate(bins):
        print(f"  {b:>18s}  " + "  ".join(f"{((truth == i) & (pred == j)).sum():6d}" for j in range(len(bins))))

    if args.write:
        # Refit on everything for the shipped tree; the score above is the held-out one.
        model.fit(X, y)
        tree = _collapse(_tree_literal(model, names, bins))
        target = os.path.normpath(os.path.join(PLANNER, "time_estimate_tree.py"))
        with open(target, "w") as out:
            out.write(
                "# Licensed under the Apache License, Version 2.0 (the \"License\");\n"
                "# you may not use this file except in compliance with the License.\n"
                "# See the License at http://www.apache.org/licenses/LICENSE-2.0\n"
                "# Distributed on an \"AS IS\" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.\n\n"
                "# GENERATED by tests/performance/estimation/fit_time_bins.py - do not edit.\n"
                f"# Fitted on {len(rows)} jobs, {rows[0][0][:10]} .. {rows[-1][0][:10]};"
                f" held-out exact {(pred == truth).mean():.1%}, within one bin {(abs(pred - truth) <= 1).mean():.1%}.\n"
                "# A node is (feature, threshold, if_le, if_gt); a leaf is a bin label.\n\n"
                f"TREE = {_pretty(tree)}\n"
            )
        print(f"\nwrote {target}")


def _pretty(node, indent=1):
    if isinstance(node, str):
        return repr(node)
    pad = "    " * indent
    feature, threshold, le, gt = node
    return f"(\n{pad}{feature!r},\n{pad}{threshold!r},\n{pad}{_pretty(le, indent + 1)},\n{pad}{_pretty(gt, indent + 1)},\n{'    ' * (indent - 1)})"


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="command", required=True)
    p = sub.add_parser("export")
    p.add_argument("--project", required=True)
    p.add_argument("--database", default="(default)")
    p.add_argument("--out", required=True)
    p.set_defaults(run=export)
    p = sub.add_parser("fit")
    p.add_argument("jobs")
    p.add_argument("--depth", type=int, default=4)
    p.add_argument("--min-leaf", type=int, default=40)
    p.add_argument("--write", action="store_true")
    p.set_defaults(run=fit)
    args = parser.parse_args()
    args.run(args)


if __name__ == "__main__":
    sys.exit(main())
