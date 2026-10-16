#!/usr/bin/env python3
"""
Subprocess worker for tpcds/runner.py — a long-lived process that runs queries
one at a time: one JSON request line in, one JSON result line out.

Why a subprocess at all: a query that never yields control back to the
Python interpreter (a native call that just spins — see the Q13 join-
detection hang) can't be stopped by an in-process signal-based timeout;
CPython only runs a signal handler between bytecode instructions of the
interpreter loop, and a single blocking nogil call never reaches one. Running
each query in its own process lets the parent enforce a real wall-clock
timeout with `subprocess.run(..., timeout=...)`, which SIGKILLs the process
(and everything in it, including any native thread pool) at the OS level —
the only version of "timeout" that's actually true.

The process is reused across queries, so the suite pays interpreter startup and
`import opteryx` once, not per query. The timeout still holds: the runner kills
this process (and its native threads) when a query overruns, and starts a fresh
one for the next query.

Protocol, one line each way per query:
    in:  {"sql": "...", "iterations": N}
    out: {"ok": true, "runs_ms": [N timings], "rows": ...}
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

# `--aistor`: the runner's `--variant aistor` - bind the remote Iceberg workspace.
if "--aistor" in sys.argv[1:]:
    sys.path.insert(0, os.path.join(_REPO_ROOT, "tests", "performance"))
    from _common import register_aistor_workspace  # noqa: E402

    register_aistor_workspace()


def main() -> int:
    # One-time engine initialisation (lazy imports, native setup) is paid here, before
    # the first request, instead of landing on whichever query happens to run first:
    # measured ~550-690 ms on the first query of a fresh worker vs ~3 ms after.
    warm = opteryx.session()
    for _ in warm.execute_to_morsels("SELECT COUNT(*) FROM $planets"):
        pass
    warm.close()
    # One JSON request per line on stdin, one JSON result per line on stdout, until EOF.
    # The runner keeps this process for the whole suite (so the suite times queries,
    # not interpreter startup + `import opteryx`), and kills / replaces it only when a
    # query exceeds its timeout or the process dies.
    for line in sys.stdin:
        if not line.strip():
            continue
        request = json.loads(line)
        sql = request["sql"]
        iterations = request["iterations"]
        # `iterations` timed runs, each in a fresh session; the runner keeps the min
        # (the same min-of-N the TPC-H and SSB runners report). The first run is not
        # discarded — the min already excludes its cold cost.
        runs_ms: list = []
        rows = 0
        result = None
        for _ in range(iterations):
            session = opteryx.session()
            try:
                rows = 0
                t0 = time.monotonic_ns()
                for morsel in session.execute_to_morsels(sql):
                    rows += morsel.num_rows
                runs_ms.append((time.monotonic_ns() - t0) / 1e6)
            except Exception as err:
                result = {"ok": False, "error": f"{type(err).__name__}: {err}"}
                break
            finally:
                session.close()
        if result is None:
            result = {"ok": True, "runs_ms": runs_ms, "rows": rows}
        sys.stdout.write(json.dumps(result) + "\n")
        sys.stdout.flush()
    return 0


if __name__ == "__main__":
    sys.exit(main())
