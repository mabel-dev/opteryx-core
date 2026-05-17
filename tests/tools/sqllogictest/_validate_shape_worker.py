#!/usr/bin/env python3
"""Worker for ``convert_shape_tests.py``.

Reads a JSON list of {"ix": int, "sql": str} cases from a file, runs each
through Opteryx, and prints one JSON result line per case to stdout:

    {"ix": int, "verb": "ok", "rows": int, "cols": int}
    {"ix": int, "verb": "error", "detail": "<exc class>"}

Crash-resilient via ``--start-from`` like the run_tests worker.
"""

from __future__ import annotations

import argparse
import json
import os
import pathlib
import sys


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("path", type=pathlib.Path)
    ap.add_argument("--start-from", type=int, default=0)
    args = ap.parse_args()

    workdir = os.environ.get("OPTERYX_HOME")
    if workdir:
        os.chdir(workdir)

    import opteryx  # noqa: E402

    session = opteryx.session(memberships=["Apollo 11", "opteryx"])
    cases = json.loads(args.path.read_text(encoding="utf-8"))
    out = sys.stdout

    for case in cases:
        ix = case["ix"]
        if ix < args.start_from:
            continue
        sql = case["sql"]
        try:
            morsels = list(session.execute_to_morsels(sql))
            rows = sum(m.num_rows for m in morsels if m is not None)
            cols = (
                len(next(m for m in morsels if m is not None).column_names)
                if any(m is not None for m in morsels)
                else 0
            )
            out.write(
                json.dumps({"ix": ix, "verb": "ok", "rows": rows, "cols": cols})
            )
        except BaseException as exc:  # noqa: BLE001
            out.write(
                json.dumps(
                    {"ix": ix, "verb": "error", "detail": type(exc).__name__}
                )
            )
        out.write("\n")
        out.flush()

    return 0


if __name__ == "__main__":
    sys.exit(main())
