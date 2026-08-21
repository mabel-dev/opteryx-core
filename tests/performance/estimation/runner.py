#!/usr/bin/env python3
"""
Cardinality-estimation q-error harness (Phase 0 of the estimator remediation).

Runs TPC-H SF1, TPC-DS SF1 and ClickBench once each, collects every plan
operator's (planner-estimated, actually-emitted) row-count pair from query
telemetry, and reports q-error per operator, per query and per suite as JSON.
This is a MEASUREMENT harness, not a benchmark: one run per query, no timing
comparisons, subprocess-per-query so a hang or native crash costs one query,
not the run (same shape as tpcds/_query_worker.py).

q-error = max(est/actual, actual/est), both sides clamped to >= 1 row first —
the standard convention (Moerkotte et al.), which also makes zero-row actuals
well-defined: an estimate of 100 against an empty result scores 100, and
0-vs-0 scores a perfect 1.0. Raw counts are kept alongside so the clamping is
auditable. Operators the statistics refresh never reached carry no estimate
and are counted as "unestimated" (coverage), never scored.

Operator identity
-----------------
Every recorded operator carries a ``key`` describing its ROLE in the plan, not
its position in a list. Position is worthless for comparison: insert one node
and every operator after it shifts, and a naive index-aligned diff then reports
movement that never happened. The key is::

    LABEL[relation multiset beneath it]#rank.ordinal

built from the plan DAG (the worker emits the edge list alongside the operator
records). ``rank`` counts how many operators with the SAME label-and-relations
sit strictly below this one, which separates a stacked chain (FILTER over
FILTER over the same relation) without using depth-from-root — an unrelated
node inserted ABOVE would shift depth, but leaves rank alone. ``ordinal`` is
the last resort for parallel siblings that are still identical after all that,
assigned in a deterministic structural order.

What this key does and does not promise:
  - stable across runs of an unchanged plan: yes — nothing in it comes from a
    per-run identifier (plan nids are regenerated every run) or from a measured
    value (an estimate that moves must not move the key that names it).
  - unchanged by unrelated edits elsewhere in the plan: yes — every component
    is derived from the operator's own subtree.
  - honest about new operators: yes — a genuinely new operator produces a key
    no baseline operator has, and compare mode reports it as ADDED. Keys are
    matched by equality only; nothing is ever paired by proximity or position.
  - the one case it cannot separate: two operators that are the same label over
    the same relations at the same rank with identical child roles — the two
    scans of a self-join, say. They are distinguished only by ``ordinal``, and
    since they are structurally interchangeable, pairing either way is equally
    valid. When such a group GROWS, the surplus member is reported ADDED, never
    silently absorbed.

An estimate carried as the ``_UNKNOWN_ROW_COUNT`` stand-in (1,000,000 — the
fallback for a relation that cannot report its size) is flagged ``stand_in:
"direct"``, and every operator above one is flagged ``"inherited"``. These are
fabrications, not estimates: scoring them scores the fallback constant, so the
summaries carry the geomean both with and without them.

Usage:
    python tests/performance/estimation/runner.py                 # all suites
    python tests/performance/estimation/runner.py --suite tpch
    python tests/performance/estimation/runner.py --suite tpch,tpcds
    python tests/performance/estimation/runner.py --query Q06     # one query
    python tests/performance/estimation/runner.py --out path.json
    python tests/performance/estimation/runner.py --timeout 120
    python tests/performance/estimation/runner.py --compare base.json cand.json

Gating a Phase 3 change with this tool
--------------------------------------
This harness measures; it does not decide. A Phase 3 estimator change gates
itself like this:

 1. BEFORE touching anything, on a clean tree::

        python tests/performance/estimation/runner.py --out /tmp/before.json

    Capture it first, from the tree you are actually about to change. Do not
    reuse ``results.local.json`` as the "before" unless you have confirmed the
    tree still matches it — other work lands concurrently, and a baseline from
    a different tree attributes someone else's movement to your change.

 2. Make the change, rebuild (``make c``), and run again::

        python tests/performance/estimation/runner.py --out /tmp/after.json

 3. Compare::

        python tests/performance/estimation/runner.py --compare /tmp/before.json /tmp/after.json

 4. Read it in this order, and be able to answer each before claiming a win:

    - ``queries_failed`` must not rise. A query that stopped running is not an
      improved estimate; it is a broken one, and it silently withdraws every
      operator it used to contribute.
    - ``pairs`` and ``coverage``: if the pair count moved, the geomean change
      is not the sum of the per-query contributions — the denominator moved.
      The report says so explicitly; do not quote the geomean past that line
      without explaining the count change.
    - ``standin_operators``: a fall here IS the result for a change whose job
      is to give a relation a real row count. Read it before the geomeans —
      turning a fabrication into a real-but-still-imperfect estimate moves
      operators INTO the scored set and can raise ``qerror_geomean_ex_standin``
      while being unambiguously correct.
    - per-query movement, ranked by contribution: the queries your change
      actually moved. If a query you did not expect dominates the list, find
      out why before landing.
    - ADDED / REMOVED operators: these are plan-SHAPE changes. An estimator
      change that reshapes plans is doing two things at once; say so, or split
      it.

 5. ``results.local.json`` is the committed baseline and the architect decides
    when it moves. Land your change against your own before/after pair; do not
    refresh the committed file as part of the change.


Output (run mode): tests/performance/estimation/results.local.json — a stable,
sorted JSON document so two runs can be diffed. The committed baseline that
gates estimator changes lives next to it; regenerate with --out and diff.

Inputs:
    tests/performance/tpch/opteryx/queries/query*.sql   → testdata.tpch_1
    tests/performance/tpcds/opteryx/queries/query*.sql  → testdata.tpcds_1
    tests/performance/clickbench/opteryx/runner.py      → scratch.hits_rugo_262k
"""

from __future__ import annotations

import argparse
import glob
import json
import math
import os
import subprocess
import sys
import time

_HERE = os.path.dirname(os.path.abspath(__file__))
_REPO_ROOT = os.path.abspath(os.path.join(_HERE, "..", "..", ".."))
sys.path.insert(0, _REPO_ROOT)

_WORKER_PATH = os.path.join(_HERE, "_query_worker.py")
_TPCH_QUERY_DIR = os.path.join(_REPO_ROOT, "tests", "performance", "tpch", "opteryx", "queries")
_TPCDS_QUERY_DIR = os.path.join(_REPO_ROOT, "tests", "performance", "tpcds", "opteryx", "queries")
_CLICKBENCH_RUNNER = os.path.join(
    _REPO_ROOT, "tests", "performance", "clickbench", "opteryx", "runner.py"
)
_DEFAULT_OUT = os.path.join(_HERE, "results.local.json")

_TPCH_DATASET = "testdata.tpch_1"
_TPCDS_DATASET = "testdata.tpcds_1"


def _load_tpch_queries() -> list[tuple[str, str]]:
    queries = []
    for path in sorted(glob.glob(os.path.join(_TPCH_QUERY_DIR, "query*.sql"))):
        name = os.path.splitext(os.path.basename(path))[0]
        if name.startswith("query") and name[5:].isdigit():
            name = f"Q{int(name[5:]):02d}"
        body = open(path).read()
        body = body.replace("testdata.tpch_tiny.", f"{_TPCH_DATASET}.")
        body = body.replace("testdata.tpch.", f"{_TPCH_DATASET}.")
        queries.append((name, body))
    return queries


def _load_tpcds_queries() -> list[tuple[str, str]]:
    queries = []
    for path in sorted(glob.glob(os.path.join(_TPCDS_QUERY_DIR, "query*.sql"))):
        name = os.path.splitext(os.path.basename(path))[0]
        if name.startswith("query") and name[5:].isdigit():
            name = f"Q{int(name[5:]):02d}"
        body = open(path).read().replace("testdata.tpcds_tiny.", f"{_TPCDS_DATASET}.")
        queries.append((name, body))
    return queries


def _load_clickbench_queries() -> tuple[list[tuple[str, str]], str]:
    """Load STATEMENTS from the ClickBench pytest entry point.

    Imported by file path, not sys.path — the `opteryx/` subdirectory next to
    it would shadow the real package (same trick as clickbench/runner.py).
    """
    import importlib.util

    spec = importlib.util.spec_from_file_location("_clickbench_pytest_runner", _CLICKBENCH_RUNNER)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    dataset = module.DATASET.value
    queries = []
    for index, (template, _expected_err) in enumerate(module.STATEMENTS, start=1):
        if template.lstrip().startswith("--"):  # commented-out statements
            continue
        queries.append((f"Q{index:02d}", template.format(DATASET=dataset)))
    return queries, dataset


# ---------------------------------------------------------------- identity --

# The stand-in row count `statistics_refresh._UNKNOWN_ROW_COUNT` substitutes
# for a relation that cannot report a size. Scoring it as an estimate scores a
# fabrication, so the report flags it rather than silently folding it in.
_UNKNOWN_ROW_COUNT = 1_000_000


def _plan_identity(operators: list, edges: list) -> dict:
    """Derive a structural key (and stand-in taint) per operator, by nid.

    See "Operator identity" in the module docstring for what the key means and
    the one case it cannot separate. Returns ``{nid: {"key": str,
    "stand_in": "direct"|"inherited"|None}}``; nids absent from the plan DAG
    (a legacy worker record with no ``nid``) simply do not appear.
    """
    ops = {op["nid"]: op for op in operators if op.get("nid")}
    if not ops:
        return {}

    producers: dict = {nid: [] for nid in ops}  # consumer nid -> producer nids
    leg_in: dict = {}  # producer nid -> the leg label on its outgoing edge
    for edge in edges:
        src, dst = edge.get("from"), edge.get("to")
        if src in ops and dst in ops:
            producers[dst].append(src)
            if edge.get("leg"):
                leg_in[src] = edge["leg"]

    # --- relation multiset beneath each node (the "what it operates over") ---
    _rels: dict = {}

    def rels(nid: str, guard: frozenset = frozenset()) -> dict:
        if nid in _rels:
            return _rels[nid]
        if nid in guard:  # plans are DAGs; refuse to loop rather than recurse
            return {}
        children = producers[nid]
        if not children:
            # A source node: its `config` is the relation it reads. Anything
            # else childless (a values/function source) still has a stable
            # config string, which is exactly as good a role descriptor.
            counts = {str(ops[nid].get("config") or ops[nid].get("type") or "?"): 1}
        else:
            counts = {}
            for child in children:
                for name, count in rels(child, guard | {nid}).items():
                    counts[name] = counts.get(name, 0) + count
        _rels[nid] = counts
        return counts

    def rels_str(nid: str) -> str:
        return ",".join(
            f"{name}*{count}" if count > 1 else name for name, count in sorted(rels(nid).items())
        )

    def label(nid: str) -> str:
        op = ops[nid]
        return str(op.get("operator") or op.get("type") or "?")

    base = {nid: f"{label(nid)}[{rels_str(nid)}]" for nid in ops}

    # --- rank: how many same-role operators sit BELOW this one -------------
    # Separates a stacked chain (FILTER over FILTER over the same relation)
    # without using depth-from-root, which an unrelated insertion above would
    # shift. Inserting an unrelated node inside the chain leaves ranks alone;
    # inserting a THIRD same-role operator changes them, which is honest —
    # the chain really did change.
    _sub: dict = {}

    def subtree(nid: str, guard: frozenset = frozenset()) -> frozenset:
        if nid in _sub:
            return _sub[nid]
        if nid in guard:
            return frozenset()
        acc = {nid}
        for child in producers[nid]:
            acc |= subtree(child, guard | {nid})
        result = frozenset(acc)
        _sub[nid] = result
        return result

    rank = {
        nid: sum(1 for other in subtree(nid) if other != nid and base[other] == base[nid])
        for nid in ops
    }

    # --- ordinal: the last resort, for interchangeable parallel siblings ---
    groups: dict = {}
    for nid in ops:
        groups.setdefault((base[nid], rank[nid]), []).append(nid)

    def signature(nid: str) -> tuple:
        return (
            tuple(sorted(f"{base[child]}#{rank[child]}" for child in producers[nid])),
            leg_in.get(nid, ""),
            str(ops[nid].get("config") or ""),
        )

    keys: dict = {}
    for (base_key, node_rank), members in groups.items():
        for ordinal, nid in enumerate(sorted(members, key=signature)):
            keys[nid] = f"{base_key}#{node_rank}.{ordinal}"

    # --- stand-in taint ---------------------------------------------------
    direct = {
        nid
        for nid, op in ops.items()
        if isinstance(op.get("est_rows"), int)
        and op["est_rows"] >= _UNKNOWN_ROW_COUNT
        and op["est_rows"] % _UNKNOWN_ROW_COUNT == 0
    }
    identity: dict = {}
    for nid in ops:
        if nid in direct:
            taint = "direct"
        elif direct & subtree(nid):
            taint = "inherited"
        else:
            taint = None
        identity[nid] = {"key": keys[nid], "stand_in": taint}
    return identity


def _qerror(est: int, actual: int) -> float:
    """max(est/actual, actual/est), both clamped to >= 1 (see module docstring)."""
    e = max(1.0, float(est))
    a = max(1.0, float(actual))
    return max(e / a, a / e)


def _run_one(sql: str, timeout: float) -> dict:
    """Run one query in a worker subprocess; return the worker's JSON record."""
    try:
        proc = subprocess.run(
            [sys.executable, _WORKER_PATH],
            input=sql.encode("utf-8"),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=timeout,
            cwd=_REPO_ROOT,
        )
    except subprocess.TimeoutExpired:
        return {"status": "error", "error": f"timeout after {timeout:.0f}s"}
    if proc.returncode != 0:
        tail = proc.stderr.decode("utf-8", "replace").strip().splitlines()[-3:]
        return {
            "status": "error",
            "error": f"worker exited {proc.returncode}: {' | '.join(tail) or 'no stderr'}",
        }
    # The result is the LAST stdout line: the query itself may print (warnings).
    lines = [line for line in proc.stdout.decode("utf-8", "replace").splitlines() if line.strip()]
    if not lines:
        return {"status": "error", "error": "worker produced no output"}
    try:
        return json.loads(lines[-1])
    except json.JSONDecodeError:
        return {"status": "error", "error": f"unparseable worker output: {lines[-1][:200]}"}


def _score_query(result: dict) -> dict:
    """Fold a worker record into the per-query report entry."""
    if result.get("status") != "ok":
        return {"status": "error", "error": result.get("error", "unknown")}

    identity = _plan_identity(result["operators"], result.get("edges") or [])

    operators = []
    qerrors = []
    unestimated = 0
    standin = 0
    for op in result["operators"]:
        est = op.get("est_rows")
        actual = op.get("actual_rows")
        info = identity.get(op.get("nid")) or {}
        entry = {
            "key": info.get("key"),
            "operator": op.get("operator"),
            "type": op.get("type"),
            "est_rows": est,
            "est_rows_kind": op.get("est_rows_kind"),
            "actual_rows": actual,
        }
        if info.get("stand_in"):
            entry["stand_in"] = info["stand_in"]
            standin += 1
        if est is None or actual is None:
            unestimated += 1
        else:
            entry["qerror"] = round(_qerror(est, actual), 4)
            qerrors.append(entry["qerror"])
        operators.append(entry)

    # Deterministic order regardless of dict/nid ordering, so runs diff cleanly.
    # The structural key is the primary sort now: two reports of the same plan
    # list their operators in the same order, and one shifted operator no longer
    # drags every later entry with it. The old tie-breakers stay as a fallback
    # for a record with no key (nothing else in the plan DAG to place it by).
    operators.sort(
        key=lambda o: (
            o["key"] or "",
            o["operator"] or "",
            o["est_rows"] or 0,
            o["actual_rows"] or 0,
        )
    )

    entry = {
        "status": "ok",
        "pairs": len(qerrors),
        "unestimated_operators": unestimated,
        "standin_operators": standin,
        "operators": operators,
    }
    if qerrors:
        ordered = sorted(qerrors)
        entry["qerror_median"] = round(ordered[len(ordered) // 2], 4)
        entry["qerror_max"] = round(ordered[-1], 4)
    return entry


def _summarise(query_entries: dict) -> dict:
    """Suite-level aggregate over every scored operator pair."""
    qerrors = []
    real_qerrors = []  # excluding operators carrying/inheriting a stand-in
    pairs = 0
    unestimated = 0
    standin = 0
    ok = failed = 0
    for entry in query_entries.values():
        if entry.get("status") != "ok":
            failed += 1
            continue
        ok += 1
        pairs += entry["pairs"]
        unestimated += entry["unestimated_operators"]
        standin += entry.get("standin_operators") or 0
        for op in entry["operators"]:
            if "qerror" not in op:
                continue
            qerrors.append(op["qerror"])
            if not op.get("stand_in"):
                real_qerrors.append(op["qerror"])

    summary = {
        "queries_ok": ok,
        "queries_failed": failed,
        "pairs": pairs,
        "unestimated_operators": unestimated,
        "standin_operators": standin,
        "coverage": round(pairs / (pairs + unestimated), 4) if (pairs + unestimated) else None,
    }
    if qerrors:
        ordered = sorted(qerrors)
        summary["qerror_median"] = round(ordered[len(ordered) // 2], 4)
        summary["qerror_p90"] = round(ordered[int(len(ordered) * 0.9)], 4)
        summary["qerror_max"] = round(ordered[-1], 4)
        summary["qerror_geomean"] = round(
            math.exp(sum(math.log(q) for q in ordered) / len(ordered)), 4
        )
    # Same convention, same operators, one exclusion: the fabricated stand-ins.
    # Reported ALONGSIDE the full figure, never instead of it — see the
    # stand-in note in the module docstring for how to read the pair.
    if real_qerrors:
        summary["qerror_geomean_ex_standin"] = round(
            math.exp(sum(math.log(q) for q in real_qerrors) / len(real_qerrors)), 4
        )
    return summary


# ----------------------------------------------------------------- compare --


def _load_report(path: str) -> dict:
    with open(path) as handle:
        return json.load(handle)


def _query_rows(report: dict) -> dict:
    """Flatten a report to {"suite:QNN": query_entry}."""
    rows: dict = {}
    for suite_name, suite in (report.get("suites") or {}).items():
        for query_name, entry in (suite.get("queries") or {}).items():
            rows[f"{suite_name}:{query_name}"] = entry
    return rows


def _has_keys(report: dict) -> bool:
    """True if this report carries structural operator keys (see Task A)."""
    for entry in _query_rows(report).values():
        for op in entry.get("operators") or []:
            if op.get("key"):
                return True
    return False


def _log_sum(entry: dict) -> tuple:
    """(sum of ln(qerror), pair count) for one query entry."""
    total = 0.0
    count = 0
    for op in entry.get("operators") or []:
        if "qerror" in op:
            total += math.log(max(1.0, op["qerror"]))
            count += 1
    return total, count


def _fmt_delta(before, after, higher_is_worse: bool = True) -> str:
    if before is None and after is None:
        return "     —"
    if before is None:
        return f"    — → {after}"
    if after is None:
        return f"{before} →     —"
    arrow = "="
    if after > before:
        arrow = "worse" if higher_is_worse else "better"
    elif after < before:
        arrow = "better" if higher_is_worse else "worse"
    return f"{before} → {after}  ({after - before:+.4g}, {arrow})"


def _compare_summaries(label: str, base_entries: dict, cand_entries: dict, lines: list) -> None:
    base = _summarise(base_entries)
    cand = _summarise(cand_entries)
    lines.append(f"  {label}")
    for field, worse_up in (
        ("qerror_geomean", True),
        ("qerror_median", True),
        ("qerror_p90", True),
        ("qerror_geomean_ex_standin", True),
        ("pairs", False),
        ("coverage", False),
        ("standin_operators", True),
        ("queries_ok", False),
        ("queries_failed", True),
    ):
        if base.get(field) is None and cand.get(field) is None:
            continue
        lines.append(f"    {field:<28} {_fmt_delta(base.get(field), cand.get(field), worse_up)}")


def _compare(base_path: str, cand_path: str, top: int) -> dict:
    base_report = _load_report(base_path)
    cand_report = _load_report(cand_path)
    base_rows = _query_rows(base_report)
    cand_rows = _query_rows(cand_report)

    keyed = _has_keys(base_report) and _has_keys(cand_report)

    lines: list = []
    lines.append("═══ estimation compare ═══")
    lines.append(f"  baseline  {base_path}")
    lines.append(f"            {(base_report.get('meta') or {}).get('generated', '?')}"
                 f"  opteryx {(base_report.get('meta') or {}).get('opteryx_version', '?')}")
    lines.append(f"  candidate {cand_path}")
    lines.append(f"            {(cand_report.get('meta') or {}).get('generated', '?')}"
                 f"  opteryx {(cand_report.get('meta') or {}).get('opteryx_version', '?')}")
    lines.append("")

    # --- 1. aggregates ----------------------------------------------------
    lines.append("── aggregates (recomputed from both reports' operator pairs)")
    _compare_summaries("OVERALL", base_rows, cand_rows, lines)
    for suite_name in sorted(set(base_report.get("suites") or {}) | set(cand_report.get("suites") or {})):
        prefix = f"{suite_name}:"
        _compare_summaries(
            suite_name.upper(),
            {k: v for k, v in base_rows.items() if k.startswith(prefix)},
            {k: v for k, v in cand_rows.items() if k.startswith(prefix)},
            lines,
        )
    lines.append("")

    # --- 2. per-query movement -------------------------------------------
    # Ranked by the query's contribution to the change in sum(ln qerror) — the
    # numerator of the geomean. When the pair COUNT also moved the geomean is
    # not a clean sum of these contributions; that is called out below rather
    # than hidden inside a single number.
    movement: list = []
    base_pairs_total = cand_pairs_total = 0
    for name in sorted(set(base_rows) | set(cand_rows)):
        b = base_rows.get(name) or {}
        c = cand_rows.get(name) or {}
        b_sum, b_n = _log_sum(b) if b.get("status") == "ok" else (0.0, 0)
        c_sum, c_n = _log_sum(c) if c.get("status") == "ok" else (0.0, 0)
        base_pairs_total += b_n
        cand_pairs_total += c_n
        status = ""
        if b.get("status") != c.get("status"):
            status = f"{b.get('status', 'absent')}→{c.get('status', 'absent')}"
        if abs(c_sum - b_sum) < 1e-12 and b_n == c_n and not status:
            continue
        movement.append(
            {
                "query": name,
                "delta_log_sum": c_sum - b_sum,
                "pairs_before": b_n,
                "pairs_after": c_n,
                "geomean_before": round(math.exp(b_sum / b_n), 4) if b_n else None,
                "geomean_after": round(math.exp(c_sum / c_n), 4) if c_n else None,
                "status_change": status,
            }
        )
    movement.sort(key=lambda m: -abs(m["delta_log_sum"]))

    lines.append(f"── per-query movement (top {top} by contribution to Σln q-error)")
    if not movement:
        lines.append("  no query moved")
    if base_pairs_total != cand_pairs_total:
        lines.append(
            f"  NOTE: scored pairs moved {base_pairs_total} → {cand_pairs_total}; the geomean "
            "change is NOT the sum of these contributions (the denominator moved too)."
        )
    for entry in movement[:top]:
        note = f"  [{entry['status_change']}]" if entry["status_change"] else ""
        lines.append(
            f"  {entry['delta_log_sum']:+9.3f}  {entry['query']:<20} "
            f"geomean {entry['geomean_before']} → {entry['geomean_after']}  "
            f"pairs {entry['pairs_before']}→{entry['pairs_after']}{note}"
        )
    lines.append("")

    # --- 3. operators added / removed ------------------------------------
    added: list = []
    removed: list = []
    moved: list = []
    if keyed:
        for name in sorted(set(base_rows) | set(cand_rows)):
            b_ops = {op["key"]: op for op in ((base_rows.get(name) or {}).get("operators") or []) if op.get("key")}
            c_ops = {op["key"]: op for op in ((cand_rows.get(name) or {}).get("operators") or []) if op.get("key")}
            for key in sorted(set(c_ops) - set(b_ops)):
                added.append({"query": name, "key": key, "est_rows": c_ops[key].get("est_rows"),
                              "actual_rows": c_ops[key].get("actual_rows")})
            for key in sorted(set(b_ops) - set(c_ops)):
                removed.append({"query": name, "key": key, "est_rows": b_ops[key].get("est_rows"),
                                "actual_rows": b_ops[key].get("actual_rows")})
            for key in sorted(set(b_ops) & set(c_ops)):
                b_q = b_ops[key].get("qerror")
                c_q = c_ops[key].get("qerror")
                if b_q is None or c_q is None:
                    if b_q != c_q:
                        moved.append({"query": name, "key": key, "qerror_before": b_q,
                                      "qerror_after": c_q, "delta_log": 0.0,
                                      "est_before": b_ops[key].get("est_rows"),
                                      "est_after": c_ops[key].get("est_rows")})
                    continue
                delta = math.log(max(1.0, c_q)) - math.log(max(1.0, b_q))
                if abs(delta) > 1e-12:
                    moved.append({"query": name, "key": key, "qerror_before": b_q,
                                  "qerror_after": c_q, "delta_log": delta,
                                  "est_before": b_ops[key].get("est_rows"),
                                  "est_after": c_ops[key].get("est_rows")})
        moved.sort(key=lambda m: -abs(m["delta_log"]))
        lines.append(f"── operators: {len(added)} added, {len(removed)} removed, {len(moved)} matched-and-moved")
        for entry in added[:top]:
            lines.append(f"  ADDED    {entry['query']:<20} {entry['key']}  est={entry['est_rows']} actual={entry['actual_rows']}")
        if len(added) > top:
            lines.append(f"  … {len(added) - top} more added not listed")
        for entry in removed[:top]:
            lines.append(f"  REMOVED  {entry['query']:<20} {entry['key']}  est={entry['est_rows']} actual={entry['actual_rows']}")
        if len(removed) > top:
            lines.append(f"  … {len(removed) - top} more removed not listed")
        lines.append("")
        lines.append(f"── matched operators that moved (top {top} by |Δln q-error|)")
        for entry in moved[:top]:
            lines.append(
                f"  {entry['delta_log']:+9.3f}  {entry['query']:<20} {entry['key']}\n"
                f"             q-error {entry['qerror_before']} → {entry['qerror_after']}"
                f"   est {entry['est_before']} → {entry['est_after']}"
            )
        if len(moved) > top:
            lines.append(f"  … {len(moved) - top} more movers not listed")
    else:
        lines.append("── operators: NOT PAIRED")
        lines.append("  At least one report predates structural operator keys (Task A), so this")
        lines.append("  tool cannot tell which operator in one report is which in the other.")
        lines.append("  It will NOT guess by list position — that is exactly the shift artefact")
        lines.append("  this mode exists to remove. Aggregates and per-query movement above are")
        lines.append("  unaffected (they need no operator identity). Re-run the harness on the")
        lines.append("  baseline tree to get a keyed baseline for operator-level attribution.")
    lines.append("")

    # --- 4. stand-in callout ---------------------------------------------
    def _standins(rows: dict) -> dict:
        out: dict = {}
        for name, entry in rows.items():
            if entry.get("status") != "ok":
                continue
            for op in entry.get("operators") or []:
                if op.get("stand_in"):
                    out.setdefault(name, []).append((op.get("key"), op["stand_in"], op.get("est_rows")))
        return out

    base_standin = _standins(base_rows)
    cand_standin = _standins(cand_rows)
    base_count = sum(len(v) for v in base_standin.values())
    cand_count = sum(len(v) for v in cand_standin.values())

    lines.append(f"── fabricated stand-in estimates (_UNKNOWN_ROW_COUNT = {_UNKNOWN_ROW_COUNT:,} and its multiples)")
    if not _has_keys(base_report):
        lines.append("  baseline predates stand-in flagging — its count below reads 0 because the")
        lines.append("  flag was never recorded, NOT because it carried none.")
    lines.append(f"  operators carrying or inheriting a stand-in: {base_count} → {cand_count}")
    base_sum = _summarise(base_rows)
    cand_sum = _summarise(cand_rows)
    lines.append(
        f"  geomean incl. stand-ins  {base_sum.get('qerror_geomean')} → {cand_sum.get('qerror_geomean')}"
    )
    lines.append(
        f"  geomean excl. stand-ins  {base_sum.get('qerror_geomean_ex_standin')} → "
        f"{cand_sum.get('qerror_geomean_ex_standin')}"
    )
    lines.append("  Read the two together. A change that turns a fabricated estimate into a real")
    lines.append("  one REMOVES rows from the excluded set, so the excl. figure can rise while the")
    lines.append("  estimator got better — the falling stand-in count is the evidence of progress.")
    shown = 0
    for name in sorted(cand_standin):
        was = {k for k, _, _ in base_standin.get(name, [])}
        fresh = [row for row in cand_standin[name] if row[0] not in was]
        for key, taint, est in fresh:
            if shown >= top:
                break
            lines.append(f"  NEW STAND-IN  {name:<20} [{taint}] est={est}  {key}")
            shown += 1
    lines.append("")

    text = "\n".join(lines)
    print(text)
    return {
        "baseline": base_path,
        "candidate": cand_path,
        "operator_keys_available": keyed,
        "overall": {"before": base_sum, "after": cand_sum},
        "query_movement": movement,
        "operators_added": added,
        "operators_removed": removed,
        "operators_moved": moved,
        "standin_operators": {"before": base_count, "after": cand_count},
        "text": text,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Estimate-vs-actual q-error harness")
    parser.add_argument(
        "--suite",
        type=str,
        default="tpch,tpcds,clickbench",
        help="Comma-separated subset of tpch,tpcds,clickbench (default: all)",
    )
    parser.add_argument("--query", type=str, default="", help="Run only this query (e.g. Q06)")
    parser.add_argument(
        "--timeout", type=float, default=120.0, help="Per-query wall-clock timeout seconds"
    )
    parser.add_argument(
        "--out",
        type=str,
        default=None,
        help=f"Output JSON path (run mode default: {os.path.relpath(_DEFAULT_OUT, _REPO_ROOT)}; "
        "compare mode writes nothing unless given)",
    )
    parser.add_argument(
        "--compare",
        nargs=2,
        metavar=("BASELINE", "CANDIDATE"),
        default=None,
        help="Compare two reports instead of running queries",
    )
    parser.add_argument(
        "--top", type=int, default=15, help="Rows per ranked section in compare mode (default 15)"
    )
    args = parser.parse_args()

    if args.compare:
        comparison = _compare(args.compare[0], args.compare[1], args.top)
        if args.out:
            with open(args.out, "w") as handle:
                json.dump(comparison, handle, indent=2, sort_keys=True)
                handle.write("\n")
            print(f"Comparison written to {args.out}")
        return 0

    out_path = args.out or _DEFAULT_OUT

    wanted = {token.strip() for token in args.suite.split(",") if token.strip()}
    unknown = wanted - {"tpch", "tpcds", "clickbench"}
    if unknown:
        print(f"ERROR: unknown suite(s): {', '.join(sorted(unknown))}")
        return 1

    suites: dict[str, tuple[list[tuple[str, str]], str]] = {}
    if "tpch" in wanted:
        suites["tpch"] = (_load_tpch_queries(), _TPCH_DATASET)
    if "tpcds" in wanted:
        suites["tpcds"] = (_load_tpcds_queries(), _TPCDS_DATASET)
    if "clickbench" in wanted:
        queries, dataset = _load_clickbench_queries()
        suites["clickbench"] = (queries, dataset)

    for name, (queries, dataset) in suites.items():
        if not queries:
            print(f"ERROR: no queries found for suite {name}")
            return 1
        dataset_dir = os.path.join(_REPO_ROOT, *dataset.split("."))
        if not os.path.isdir(dataset_dir):
            print(f"ERROR: dataset for suite {name} not found at {dataset_dir}")
            return 1

    import opteryx

    report: dict = {
        "meta": {
            "opteryx_version": opteryx.__version__,
            "generated": time.strftime("%Y-%m-%dT%H:%M:%S"),
            "qerror_convention": "max(est/actual, actual/est), both clamped to >= 1 row",
        },
        "suites": {},
    }

    start = time.monotonic()
    for suite_name, (queries, dataset) in suites.items():
        print(f"── {suite_name}  ({dataset}, {len(queries)} queries)")
        query_entries: dict = {}
        for name, sql in queries:
            if args.query and name != args.query:
                continue
            result = _run_one(sql, args.timeout)
            entry = _score_query(result)
            query_entries[name] = entry
            if entry["status"] == "ok":
                qmax = entry.get("qerror_max")
                detail = f"pairs={entry['pairs']:<3} qerr_max={qmax}" if qmax else "no scored pairs"
                print(f"  {name}  ok     {detail}")
            else:
                print(f"  {name}  ERROR  {entry['error']}")
        report["suites"][suite_name] = {
            "dataset": dataset,
            "summary": _summarise(query_entries),
            "queries": dict(sorted(query_entries.items())),
        }

    # Overall aggregate across every suite's scored pairs.
    merged: dict = {}
    for suite_name, suite in report["suites"].items():
        merged.update({f"{suite_name}:{k}": v for k, v in suite["queries"].items()})
    report["overall"] = _summarise(merged)

    with open(out_path, "w") as handle:
        json.dump(report, handle, indent=2, sort_keys=True)
        handle.write("\n")

    elapsed = time.monotonic() - start
    print(f"\nOverall: {json.dumps(report['overall'])}")
    print(f"Report written to {os.path.relpath(out_path, _REPO_ROOT)}  ({elapsed:.1f}s)")
    failed = sum(s["summary"]["queries_failed"] for s in report["suites"].values())
    if failed:
        print(f"{failed} queries errored — recorded in the report (not an exit failure: "
              "known TPC-DS gaps would otherwise fail every baseline run)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
