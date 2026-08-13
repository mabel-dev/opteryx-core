#!/usr/bin/env python3
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""Interleaved A/B benchmark for the CROSS JOIN UNNEST optimisation programme.

CROSS JOIN UNNEST explodes: rows out is the SUM of array lengths. Everything on
the far side of that fan-out is paid for per EXPANDED row, which is why the
previous engine folded filters and DISTINCT into the operator and why dead
parent columns replicated across it are worth removing. This harness measures
those three changes over `testdata/flat/unnest_bench`
(see dev/generate_unnest_bench_data.py).

Method — the box drifts ~9.5% UPWARD across repeated runs of an UNCHANGED
binary, monotonically, so a baseline captured before a change and compared to a
run after it measures thermal state, not the change:

  * both arms are built from ONE tree behind a runtime switch, and
  * arms are run A/B/A/B INTERLEAVED within a single session, never in blocks,
  * each (arm, round) is a FRESH SUBPROCESS so no warm cache or allocator state
    leaks between arms,
  * MEDIANS are compared, plus per-round pairing — a mean is destroyed by one
    outlier, and a sign that does not hold per-round is not a result.

Usage:
    # single-arm baseline on the untouched tree (do this BEFORE the first edit)
    python dev/bench_unnest.py --label baseline --rounds 5

    # interleaved A/B once the change sits behind its switch
    python dev/bench_unnest.py --ab OPTERYX_SOME_TEMPORARY_SWITCH --rounds 5

`--ab VAR` runs arm A with VAR=0 (legacy) and arm B with VAR=1 (new), which is
the shape every switch in this programme uses. No such switch is named here on
purpose: each one is deleted as soon as its measurement is banked, so a real name
in this docstring would be a dangling reference within the day. Results land in
dev/bench_results/ as CSV.
"""

from __future__ import annotations

import argparse
import csv
import datetime
import json
import os
import statistics
import subprocess
import sys

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RESULTS_DIR = os.path.join(REPO, "dev", "bench_results")
TABLE = "testdata.flat.unnest_bench"

# Two routes into the same data, because the three optimisations do not all bite
# on the same shape:
#   SPLIT route  — computed source; the source string is live BELOW the unnest and
#                  dead ABOVE it, so it is replicated across the fan-out and then
#                  discarded. This is the shape item 1 (dead parent columns) fixes.
#   ARRAY route  — stored ARRAY column; already compiles to a 1-column unnest, so
#                  it isolates items 2 (filter fold) and 3 (DISTINCT fold) from
#                  any column-width effect.
# `plain` in both routes is the control: no filter, no DISTINCT, so a change that
# claims to speed up only the filtered/distincted shapes must leave it flat.
RARE_TAG = "tag-0499-xx"

QUERIES = {
    "split_plain": f"SELECT part FROM {TABLE} CROSS JOIN UNNEST(SPLIT(csv_tags,',')) AS part",
    "split_filter": (
        f"SELECT part FROM {TABLE} CROSS JOIN UNNEST(SPLIT(csv_tags,',')) AS part "
        f"WHERE part = '{RARE_TAG}'"
    ),
    "split_distinct": (
        f"SELECT DISTINCT part FROM {TABLE} CROSS JOIN UNNEST(SPLIT(csv_tags,',')) AS part"
    ),
    "array_plain": f"SELECT tag FROM {TABLE} CROSS JOIN UNNEST(tags) AS tag",
    "array_filter": (
        f"SELECT tag FROM {TABLE} CROSS JOIN UNNEST(tags) AS tag WHERE tag = '{RARE_TAG}'"
    ),
    "array_distinct": f"SELECT DISTINCT tag FROM {TABLE} CROSS JOIN UNNEST(tags) AS tag",
    # LIVE PARENT COLUMNS + a selective filter. This is where folding the filter into
    # the unnest actually pays, and the plain `array_filter` above hides it: the
    # dead-parent-column prune already narrows that one to a single column, so the
    # only work the fold saves there is materialising the target. Here `label` (~24
    # bytes) is genuinely read by the SELECT, so nothing can prune it — unfolded, it
    # is replicated across 1.69M rows to keep 472.
    "array_filter_wide": (
        f"SELECT tag, label FROM {TABLE} CROSS JOIN UNNEST(tags) AS tag "
        f"WHERE tag = '{RARE_TAG}'"
    ),
    # Same, with three live parent columns instead of one — separates "the fold helps"
    # from "the fold helps in proportion to what it stops replicating".
    "array_filter_wide3": (
        f"SELECT tag, label, payload_a, payload_b FROM {TABLE} "
        f"CROSS JOIN UNNEST(tags) AS tag WHERE tag = '{RARE_TAG}'"
    ),
    # ADVERSARIAL for the DISTINCT fold: HIGH cardinality. `array_distinct` above
    # collapses 1.69M elements to 500 (a 3390x reduction — the fold's best case).
    # Here 600K elements hold 200K distinct values, so the pre-reduction pays a hash
    # and a set insert per element and removes almost nothing, and the set it grows is
    # 400x larger. If folding DISTINCT is ever a net loss, it is here.
    "split_distinct_hi": (
        f"SELECT DISTINCT part FROM {TABLE} CROSS JOIN UNNEST(SPLIT(label,'-')) AS part"
    ),
    # ADVERSARIAL: a filter that keeps EVERY row. The fold builds a mask over all
    # 1.69M elements and saves nothing, so this is where it could be a net LOSS. A
    # change that only ever gets measured on the shapes it was designed for is a
    # change whose regression case is unknown.
    "array_filter_loose": (
        f"SELECT tag, label FROM {TABLE} CROSS JOIN UNNEST(tags) AS tag "
        f"WHERE tag LIKE 'tag-0%'"
    ),
}

# Row counts every arm must reproduce. A faster arm that returns a different
# number of rows is not a faster arm, and folding a filter or a DISTINCT into an
# operator is exactly the kind of change that can silently drop rows — so this is
# asserted on every round of every arm, not spot-checked once.
EXPECTED_ROWS = {
    "split_plain": 1_695_884,
    "split_filter": 472,
    "split_distinct": 501,
    "array_plain": 1_694_861,
    "array_filter": 472,
    "array_distinct": 500,
    "array_filter_wide": 472,
    "array_filter_wide3": 472,
    "array_filter_loose": 1_694_861,
    "split_distinct_hi": 200_002,
}

# ---------------------------------------------------------------------------
# The HASHTAGS suite — a real ~1GB corpus (9.67M tweets, 49 files) rather than a
# generated one. It exists because the synthetic corpus above is too small and too
# tame to conclude anything about the DISTINCT fold:
#
#   synthetic : 1.69M elements,     500 distinct  (0.03% distinct, 3390x collapse)
#   hashtags  : 1.95M elements, 151,682 distinct  (7.8%  distinct,   12.9x collapse)
#
# 151,682 live set entries is a real hash set with real cache behaviour; 500 fits in
# L1 and makes any dedup look free. The distinct-value COUNT is the variable the
# DISTINCT fold is most sensitive to, and the synthetic corpus pins it at a value no
# production query has.
# ---------------------------------------------------------------------------
HT = "scratch.parquet"
HOT_TAG = "PremiosMTVMIAW"          # 39,270 occurrences — the most common tag
IN_TAGS = "'COVID19','Bitcoin'"     #  9,920 occurrences combined

HASHTAG_QUERIES = {
    # control: no filter, no DISTINCT — must stay flat under every fold
    "ht_plain": f"SELECT tag FROM {HT} CROSS JOIN UNNEST(hash_tags) AS tag",
    # the DISTINCT fold at realistic cardinality
    "ht_distinct": f"SELECT DISTINCT tag FROM {HT} CROSS JOIN UNNEST(hash_tags) AS tag",
    "ht_distinct_wide": (
        f"SELECT DISTINCT tag FROM {HT} CROSS JOIN UNNEST(hash_tags) AS tag "
        f"WHERE tag LIKE 'M%'"
    ),
    # Eq and IN — the two forms a hash-compare fusion could possibly answer
    "ht_filter_eq": (
        f"SELECT tag FROM {HT} CROSS JOIN UNNEST(hash_tags) AS tag WHERE tag = '{HOT_TAG}'"
    ),
    "ht_filter_in": (
        f"SELECT tag FROM {HT} CROSS JOIN UNNEST(hash_tags) AS tag WHERE tag IN ({IN_TAGS})"
    ),
    # Eq with a LIVE parent column — the shape the filter fold helps most
    "ht_eq_wide": (
        f"SELECT tag, screen_name FROM {HT} CROSS JOIN UNNEST(hash_tags) AS tag "
        f"WHERE tag = '{HOT_TAG}'"
    ),
    # BOTH folds on one unnest — the only shape a DISTINCT+Eq fusion could serve
    "ht_distinct_eq": (
        f"SELECT DISTINCT tag FROM {HT} CROSS JOIN UNNEST(hash_tags) AS tag "
        f"WHERE tag IN ({IN_TAGS})"
    ),
}

# ---------------------------------------------------------------------------
# The CEILING suite — what could a DISTINCT+Eq hash fusion possibly buy?
#
# The most such a fusion can save is ONE pass of the predicate bytecode over the
# child vector (it would answer Eq/IN from the hash it computes for DISTINCT
# anyway). So rather than write the fused path and then discover it was pointless,
# measure the pass by DELETING it: arm B sets OPTERYX_UNNEST_SKIP_FILTER_KERNEL.
#
# Every predicate here matches EVERY element on purpose. A selective predicate
# would make the skipping arm emit 1.95M rows instead of 39K and the measurement
# would compare two different amounts of downstream work rather than the kernel —
# a confound seen and rejected while building this. Matching everything keeps the
# output identical (asserted below, same as any other suite), so the ONLY
# difference between the arms is whether the kernel ran.
#
# This measures the fusion's CEILING, not the fusion. A ceiling in the noise means
# no implementation of it can pay, whatever its correctness cost.
# ---------------------------------------------------------------------------
CEILING_QUERIES = {
    "ceil_ne": (
        f"SELECT tag FROM {HT} CROSS JOIN UNNEST(hash_tags) AS tag WHERE tag != '\\x01zzz'"
    ),
    "ceil_ge": f"SELECT tag FROM {HT} CROSS JOIN UNNEST(hash_tags) AS tag WHERE tag >= ''",
    "ceil_like": f"SELECT tag FROM {HT} CROSS JOIN UNNEST(hash_tags) AS tag WHERE tag LIKE '%'",
    # with DISTINCT also folded — the only shape the fusion would ever serve
    "ceil_ne_distinct": (
        f"SELECT DISTINCT tag FROM {HT} CROSS JOIN UNNEST(hash_tags) AS tag "
        f"WHERE tag != '\\x01zzz'"
    ),
}

# ---------------------------------------------------------------------------
# The SCALE suite — items 1 and 2 re-measured on the real corpus, because their
# original numbers came from the 11MB synthetic one AND from a harness that ran
# A-then-B in a fixed order (a ~1-4% penalty on whichever arm went second, since
# corrected). Both shapes the earlier run could not exercise are here:
#   * COMPUTED sources (SPLIT), the only shape item 1 affects — a stored ARRAY
#     column is already narrowed to one column by scan-level projection pushdown,
#     so `hash_tags` alone would measure nothing.
#   * LIVE parent columns of real width, which is what item 2's saving scales with.
# ---------------------------------------------------------------------------
SCALE_QUERIES = {
    # control
    "s_plain": f"SELECT tag FROM {HT} CROSS JOIN UNNEST(hash_tags) AS tag",
    # item 1: computed source => a dead parent column rides the fan-out
    "s_split_text": (
        f"SELECT w FROM {HT} CROSS JOIN UNNEST(SPLIT(text,' ')) AS w WHERE w = 'the'"
    ),
    "s_split_sn": f"SELECT p FROM {HT} CROSS JOIN UNNEST(SPLIT(screen_name,'_')) AS p",
    # item 2: selective filter with 0, 1 and 3 LIVE parent columns
    "s_eq": (
        f"SELECT tag FROM {HT} CROSS JOIN UNNEST(hash_tags) AS tag WHERE tag = '{HOT_TAG}'"
    ),
    "s_eq_wide": (
        f"SELECT tag, screen_name FROM {HT} CROSS JOIN UNNEST(hash_tags) AS tag "
        f"WHERE tag = '{HOT_TAG}'"
    ),
    "s_eq_wide3": (
        f"SELECT tag, screen_name, user_name, text FROM {HT} "
        f"CROSS JOIN UNNEST(hash_tags) AS tag WHERE tag = '{HOT_TAG}'"
    ),
}

SCALE_EXPECTED = {
    "s_plain": 1_953_648,
    "s_split_text": 5_333_644,
    "s_split_sn": 12_015_876,
    "s_eq": 39_270,
    "s_eq_wide": 39_270,
    "s_eq_wide3": 39_270,
}

CEILING_EXPECTED = {
    "ceil_ne": 1_953_648,
    "ceil_ge": 1_953_648,
    "ceil_like": 1_953_648,
    "ceil_ne_distinct": 151_682,
}

HASHTAG_EXPECTED = {
    "ht_plain": 1_953_648,
    "ht_distinct": 151_682,
    "ht_distinct_wide": 5_795,
    "ht_filter_eq": 39_270,
    "ht_filter_in": 9_920,
    "ht_eq_wide": 39_270,
    "ht_distinct_eq": 2,
}

# Runs inside the child process: import opteryx, time each query, print JSON.
CHILD = r"""
import json, os, sys, time
sys.path.insert(1, %(repo)r)
import opteryx

QUERIES = json.loads(os.environ["_BENCH_QUERIES"])
EXPECTED = json.loads(os.environ["_BENCH_EXPECTED"])
REPEATS = int(os.environ["_BENCH_REPEATS"])

session = opteryx.session()
out = {}
for name, sql in QUERIES.items():
    # One discarded warmup: the first execution of a query pays plan compilation
    # and the first touch of the parquet footer, neither of which is what this
    # measures. Every arm pays it identically, but it is variance, so drop it.
    for _ in range(1):
        rows = 0
        for morsel in session.execute_to_morsels(sql):
            rows += morsel.num_rows
    if rows != EXPECTED[name]:
        print(json.dumps({"error": f"{name}: {rows} rows, expected {EXPECTED[name]}"}))
        sys.exit(3)
    timings = []
    for _ in range(REPEATS):
        start = time.perf_counter()
        rows = 0
        for morsel in session.execute_to_morsels(sql):
            rows += morsel.num_rows
        timings.append(1000.0 * (time.perf_counter() - start))
        if rows != EXPECTED[name]:
            print(json.dumps({"error": f"{name}: {rows} rows, expected {EXPECTED[name]}"}))
            sys.exit(3)
    # Best-of within the round: the round is the unit of comparison, and taking the
    # minimum inside it strips scheduler noise without hiding drift ACROSS rounds,
    # which is what the interleaving is there to expose.
    out[name] = min(timings)
print(json.dumps(out))
"""


def run_round(env_overrides, repeats):
    """One fresh subprocess: every query, `repeats` times, best-of each."""
    env = dict(os.environ)
    env["_BENCH_QUERIES"] = json.dumps(QUERIES)
    env["_BENCH_EXPECTED"] = json.dumps(EXPECTED_ROWS)
    env["_BENCH_REPEATS"] = str(repeats)
    env.update({k: str(v) for k, v in env_overrides.items()})
    # PYTHONPATH pinned to this tree so the child can never pick up an installed
    # opteryx — a benchmark that silently measured the wheel would be worthless.
    env["PYTHONPATH"] = REPO

    proc = subprocess.run(
        [sys.executable, "-c", CHILD % {"repo": REPO}],
        cwd=REPO, env=env, capture_output=True, text=True,
    )
    if proc.returncode != 0:
        sys.stderr.write(proc.stdout + "\n" + proc.stderr + "\n")
        raise SystemExit(f"benchmark child failed (rc={proc.returncode})")
    return json.loads(proc.stdout.strip().splitlines()[-1])


def summarise(name, samples):
    return {
        "arm": name,
        "median": statistics.median(samples),
        "best": min(samples),
        "worst": max(samples),
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--rounds", type=int, default=5, help="interleaved rounds per arm")
    parser.add_argument("--repeats", type=int, default=3, help="timed executions per query per round")
    parser.add_argument("--ab", help="env var switching the change; A=0, B=1")
    parser.add_argument("--label", default="run", help="label for the results file")
    parser.add_argument("--suite", choices=("synthetic", "hashtags", "ceiling", "scale"),
                        default="synthetic",
                        help="synthetic = generated corpus; hashtags = the ~1GB real one; "
                             "ceiling = isolate the predicate-kernel pass")
    args = parser.parse_args()

    # Rebind the module-level query set the child process reads. The two suites are
    # deliberately NOT run together: the hashtags corpus is ~1GB and the synthetic one
    # is 11MB, so mixing them in one round buries the small-corpus signal in I/O.
    global QUERIES, EXPECTED_ROWS
    if args.suite == "hashtags":
        QUERIES = HASHTAG_QUERIES
        EXPECTED_ROWS = HASHTAG_EXPECTED
    elif args.suite == "ceiling":
        QUERIES = CEILING_QUERIES
        EXPECTED_ROWS = CEILING_EXPECTED
    elif args.suite == "scale":
        QUERIES = SCALE_QUERIES
        EXPECTED_ROWS = SCALE_EXPECTED

    os.makedirs(RESULTS_DIR, exist_ok=True)
    stamp = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
    path = os.path.join(RESULTS_DIR, f"unnest-{args.label}-{stamp}.csv")

    arms = [("A-legacy", {args.ab: 0}), ("B-new", {args.ab: 1})] if args.ab else [(args.label, {})]

    # arm -> query -> [per-round ms]
    samples = {arm: {q: [] for q in QUERIES} for arm, _ in arms}

    for rnd in range(args.rounds):
        # ALTERNATE the within-round order: A,B then B,A then A,B ...
        #
        # Interleaving across rounds is not enough on its own. With a fixed A-then-B
        # order, whichever arm runs second inherits the machine state the first left
        # behind, and this box ramps: a run measured a ~1-4% penalty on the second arm
        # across queries the switch provably could not affect (controls with no
        # DISTINCT in them at all). That artefact is the same size as the effects being
        # measured, so it can invent or erase a result. Alternating cancels it to first
        # order — each arm runs first in half the rounds — and the controls staying
        # flat is what says it worked.
        ordered = arms if rnd % 2 == 0 else list(reversed(arms))
        for arm, overrides in ordered:
            result = run_round(overrides, args.repeats)
            for query, ms in result.items():
                samples[arm][query].append(ms)
            shown = "  ".join(f"{q}={result[q]:.1f}" for q in QUERIES)
            print(f"round {rnd + 1}/{args.rounds}  {arm:9s}  {shown}", flush=True)

    print()
    with open(path, "w", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["query", "arm", "median_ms", "best_ms", "worst_ms", "rounds"])

        for query in QUERIES:
            stats = {arm: summarise(arm, samples[arm][query]) for arm, _ in arms}
            for arm, _ in arms:
                s = stats[arm]
                writer.writerow([query, arm, f"{s['median']:.2f}", f"{s['best']:.2f}",
                                 f"{s['worst']:.2f}", args.rounds])

            if len(arms) == 2:
                a, b = stats["A-legacy"], stats["B-new"]
                ratio = b["median"] / a["median"]
                # A per-round win, not an aggregate one: the sign has to hold in
                # every pairing or the result is noise wearing a median's clothes.
                paired = sum(
                    1 for i in range(args.rounds)
                    if samples["B-new"][query][i] < samples["A-legacy"][query][i]
                )
                separated = "yes" if b["worst"] < a["best"] or a["worst"] < b["best"] else "no"
                print(f"{query:16s} A={a['median']:8.1f}ms  B={b['median']:8.1f}ms  "
                      f"B/A={ratio:5.3f}  won {paired}/{args.rounds}  ranges separate: {separated}")
            else:
                s = stats[arms[0][0]]
                print(f"{query:16s} median={s['median']:8.1f}ms  "
                      f"best={s['best']:8.1f}ms  worst={s['worst']:8.1f}ms")

    print(f"\nwrote {path}")


if __name__ == "__main__":
    main()
