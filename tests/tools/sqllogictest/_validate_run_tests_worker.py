#!/usr/bin/env python3
"""Worker for ``convert_run_tests.py``.

Runs the SQL statements from a ``*.run_tests`` file through Opteryx, emitting
one JSON line per statement on stdout. The parent process drives the worker
in a loop with ``--start-from`` so a segfault on one statement only loses
that statement, not the rest of the file.

Each output line:
    {"ix": <line_index>, "verb": "ok"|"error", "detail": "<exc_class_name>"}

stdout is line-buffered so partial output survives a child crash.
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

    import opteryx  # noqa: E402 — must follow chdir.

    session = opteryx.session(memberships=["Apollo 11", "opteryx"])
    out = sys.stdout

    lines = args.path.read_text(encoding="utf-8").splitlines()
    for ix, raw_line in enumerate(lines):
        if ix < args.start_from:
            continue
        line = raw_line.rstrip()
        if not line.strip():
            continue
        stripped = line.lstrip()
        if stripped.startswith("#") or stripped.startswith("--"):
            continue
        sql = line.rstrip(";").rstrip()
        if not sql:
            continue
        try:
            for _ in session.execute_to_morsels(sql):
                pass
            out.write(json.dumps({"ix": ix, "verb": "ok", "detail": ""}))
        except BaseException as exc:  # noqa: BLE001 — mirror engine errors.
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
