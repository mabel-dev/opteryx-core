#!/usr/bin/env python3
"""
Subprocess worker for tpcds/runner.py — runs exactly one query, prints a
JSON result line, exits.

Why a subprocess at all: a query that never yields control back to the
Python interpreter (a native call that just spins — see the Q13 join-
detection hang) can't be stopped by an in-process signal-based timeout;
CPython only runs a signal handler between bytecode instructions of the
interpreter loop, and a single blocking nogil call never reaches one. Running
each query in its own process lets the parent enforce a real wall-clock
timeout with `subprocess.run(..., timeout=...)`, which SIGKILLs the process
(and everything in it, including any native thread pool) at the OS level —
the only version of "timeout" that's actually true.

Reads the SQL to run from stdin, writes one line of JSON to stdout:
    {"ok": true, "elapsed_ms": ..., "rows": ...}
    {"ok": false, "error": "..."}
"""

from __future__ import annotations

import json
import os
import sys
import time

_REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
sys.path.insert(0, _REPO_ROOT)

import opteryx  # noqa: E402
from opteryx.connectors import DiskConnector  # noqa: E402

opteryx.register_workspace("testdata", DiskConnector)


def main() -> int:
    sql = sys.stdin.read()
    session = opteryx.session()
    try:
        rows = 0
        t0 = time.monotonic_ns()
        for morsel in session.execute_to_morsels(sql):
            if morsel is not None and hasattr(morsel, "num_rows"):
                rows += morsel.num_rows
        elapsed_ms = (time.monotonic_ns() - t0) / 1e6
        print(json.dumps({"ok": True, "elapsed_ms": elapsed_ms, "rows": rows}))
        return 0
    except Exception as err:
        print(json.dumps({"ok": False, "error": f"{type(err).__name__}: {err}"}))
        return 1
    finally:
        session.close()


if __name__ == "__main__":
    sys.exit(main())
