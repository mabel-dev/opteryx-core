#!/usr/bin/env python3
"""
Subprocess worker for estimation/runner.py — runs exactly one query and prints
one JSON document with the per-operator (estimate, actual) row-count pairs.

A subprocess per query for the same reason tpcds/_query_worker.py exists: a
query that never yields control back to Python (a missed join key turning into
a real cross join) can only be stopped by killing the process, and a native
crash must take down this worker, not the whole baseline run.

Protocol:
    stdin  — the SQL text (UTF-8)
    stdout — one JSON object:
        {"status": "ok", "operators": [{...}, ...], "edges": [{...}, ...]}
        {"status": "error", "error": "<type>: <message>"}

Each operator carries its plan-node id (``nid``) and the plan's edge list
(``telemetry.edges``, the structured plan DAG — producer ``from``, consumer
``to``) is emitted alongside, so the runner can rebuild the plan shape and
derive a STRUCTURAL operator key. The nids are regenerated on every run and
are NOT stable identities: they exist here only to join these operator
records to those edges, and nothing downstream records them.

Each operator entry carries the fields the q-error report needs, read from
``session.telemetry["operations"]`` (the definitive per-node record — the
native engine's ``records_out`` actuals overlaid with the planner's
``est_rows``/``est_rows_kind`` estimates by ``mermaid._collect_node_stats``).
Operators the statistics refresh never reached have ``est_rows: null`` —
"no estimate was made", never "estimated zero".
"""

from __future__ import annotations

import json
import os
import sys

_REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
sys.path.insert(0, _REPO_ROOT)
# `scratch.*` datasets (ClickBench hits) resolve relative to the working
# directory; anchor it so the answer doesn't depend on where make ran from.
os.chdir(_REPO_ROOT)

import opteryx  # noqa: E402
from opteryx.connectors import DiskConnector  # noqa: E402

opteryx.register_workspace("testdata", DiskConnector)


def main() -> int:
    sql = sys.stdin.read()
    session = opteryx.session()
    try:
        for _ in session.execute_to_morsels(sql):
            pass
        telemetry = session.telemetry
    except Exception as err:  # a failed query is a recorded result, not a crash
        print(json.dumps({"status": "error", "error": f"{type(err).__name__}: {err}"}))
        return 0
    finally:
        session.close()

    operators = []
    for nid, op in (telemetry.get("operations") or {}).items():
        operators.append(
            {
                "nid": nid,
                "operator": op.get("operator"),
                "type": op.get("type"),
                "config": op.get("config"),
                "est_rows": op.get("est_rows"),
                "est_rows_kind": op.get("est_rows_kind"),
                "actual_rows": op.get("records_out"),
            }
        )
    print(
        json.dumps(
            {"status": "ok", "operators": operators, "edges": telemetry.get("edges") or []}
        )
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
