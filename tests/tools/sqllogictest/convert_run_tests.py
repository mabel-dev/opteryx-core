#!/usr/bin/env python3
"""Convert Opteryx ``*.run_tests`` files to sqllogictest ``.slt`` files.

A ``*.run_tests`` file is one SQL statement per line; lines starting with ``#``
or ``--`` are comments. The Python harness ``test_run_only_battery.py`` simply
runs each statement and asserts it doesn't error — no result checking.

Each statement is validated against Opteryx at conversion time and emitted as
``statement ok`` if it succeeds, or ``statement error`` (snapshotting the
error class) if it currently fails. This makes regressions visible in both
directions: a previously-passing query that breaks AND a known-broken query
that starts passing both fail the suite.

Usage:
    PYTHONPATH=$OPTERYX_HOME OPTERYX_HOME=$OPTERYX_HOME \
        python3 convert_run_tests.py \
            --src $OPTERYX_HOME/tests/integration/sql_battery/test_data/tests \
            --dest /path/to/sqllogictest/examples/opteryx/tests/run_only \
            [--exclude clickbench --exclude tpch_data]
"""

from __future__ import annotations

import argparse
import json
import os
import pathlib
import re
import subprocess
import sys


def _read_lines(path: pathlib.Path) -> list[str]:
    return path.read_text(encoding="utf-8").splitlines()


def _is_sql(line: str) -> bool:
    s = line.lstrip()
    return bool(s) and not s.startswith("#") and not s.startswith("--")


def _validate_in_subprocess(
    path: pathlib.Path, workdir: pathlib.Path
) -> tuple[dict[int, tuple[str, str]], set[int]]:
    """Run each non-comment line in a child process and return per-line status.

    Drives the worker in a loop, restarting after any abnormal exit (segfault,
    fatal error). Each restart resumes after the last reported line, so a crash
    only loses the statement that caused it.
    """
    helper = pathlib.Path(__file__).with_name("_validate_run_tests_worker.py")
    env = {**os.environ, "OPTERYX_HOME": str(workdir)}

    n_lines = len(path.read_text(encoding="utf-8").splitlines())
    results: dict[int, tuple[str, str]] = {}
    crashes: set[int] = set()
    start_from = 0

    while start_from < n_lines:
        cmd = [
            sys.executable,
            str(helper),
            str(path),
            "--start-from",
            str(start_from),
        ]
        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            env=env,
            cwd=str(workdir),
            text=True,
            bufsize=1,
        )
        last_ix = start_from - 1
        assert proc.stdout is not None
        for raw in proc.stdout:
            raw = raw.strip()
            if not raw:
                continue
            try:
                obj = json.loads(raw)
            except json.JSONDecodeError:
                continue
            results[obj["ix"]] = (obj["verb"], obj.get("detail", ""))
            last_ix = obj["ix"]
        rc = proc.wait()

        if rc == 0:
            break  # Worker finished the file cleanly.
        # Worker died — figure out which line killed it. The next line of SQL
        # after ``last_ix`` is the culprit; mark it crashed and skip past it.
        next_ix = last_ix + 1
        # Find the next SQL line >= next_ix; lines before that are blanks/comments.
        with path.open(encoding="utf-8") as fh:
            file_lines = fh.read().splitlines()
        culprit = None
        for ix in range(next_ix, n_lines):
            if _is_sql(file_lines[ix]):
                culprit = ix
                break
        if culprit is None:
            break
        crashes.add(culprit)
        start_from = culprit + 1

    return results, crashes


def _convert(
    path: pathlib.Path, workdir: pathlib.Path
) -> tuple[str, int, int, int]:
    """Return (slt_text, n_ok, n_error, n_crash)."""
    lines = _read_lines(path)
    results, crashes = _validate_in_subprocess(path, workdir)

    parts: list[str] = [f"# Source: {path.name}", ""]
    n_ok = n_error = n_crash = 0
    for ix, raw_line in enumerate(lines):
        line = raw_line.rstrip()
        if not line.strip():
            parts.append("")
            continue
        if not _is_sql(line):
            stripped = line.lstrip()
            parts.append("# " + stripped.lstrip("#-").strip())
            continue
        sql = line.rstrip(";").rstrip()
        if not sql:
            continue
        if ix in crashes:
            parts.append(f"# crashed Opteryx (excluded): {sql}")
            n_crash += 1
            continue
        if ix not in results:
            parts.append(f"# unreported (excluded): {sql}")
            n_crash += 1
            continue
        verb, detail = results[ix]
        if verb == "ok":
            parts.append("statement ok")
            n_ok += 1
        else:
            parts.append(f"statement error {re.escape(detail)}")
            n_error += 1
        parts.append(sql)
        parts.append("")
    if parts[-1] != "":
        parts.append("")
    return "\n".join(parts), n_ok, n_error, n_crash


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--src", required=True, type=pathlib.Path)
    ap.add_argument("--dest", required=True, type=pathlib.Path)
    ap.add_argument(
        "--exclude",
        action="append",
        default=[],
        help="Stem (filename without .run_tests) to skip; repeatable.",
    )
    ap.add_argument(
        "--workdir",
        type=pathlib.Path,
        help="chdir here before running queries (defaults to $OPTERYX_HOME).",
    )
    args = ap.parse_args()

    workdir = args.workdir or (
        pathlib.Path(os.environ["OPTERYX_HOME"]) if "OPTERYX_HOME" in os.environ else None
    )
    if workdir is None:
        print("--workdir or $OPTERYX_HOME is required", file=sys.stderr)
        return 1

    args.dest.mkdir(parents=True, exist_ok=True)
    files = sorted(args.src.glob("*.run_tests"))
    if not files:
        print(f"no *.run_tests files under {args.src}", file=sys.stderr)
        return 1

    excluded = set(args.exclude)
    converted = 0
    skipped: list[str] = []
    total_ok = total_error = total_crash = 0
    for path in files:
        stem = path.name[: -len(".run_tests")]
        if stem in excluded:
            skipped.append(stem)
            print(f"  excluded {path.name}", flush=True)
            continue
        try:
            text, n_ok, n_err, n_crash = _convert(path, workdir)
        except RuntimeError as e:
            print(f"  FAIL {path.name}: {e}", flush=True)
            continue
        total_ok += n_ok
        total_error += n_err
        total_crash += n_crash
        out_path = args.dest / f"{stem}.slt"
        out_path.write_text(text, encoding="utf-8")
        converted += 1
        print(
            f"  ok {path.name} -> {out_path.name}  "
            f"({n_ok} ok, {n_err} error, {n_crash} crash)",
            flush=True,
        )

    print()
    print(
        f"converted {converted}/{len(files) - len(skipped)} files into {args.dest} "
        f"({total_ok} statement ok, {total_error} statement error, {total_crash} crashed)"
    )
    if skipped:
        print(f"excluded: {', '.join(skipped)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
