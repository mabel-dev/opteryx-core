#!/usr/bin/env python3
"""Convert Opteryx ``*.results_tests`` JSON fixtures to sqllogictest ``.slt`` files.

Each ``*.results_tests`` file looks like::

    {
      "summary": "...",
      "statement": "SELECT ...",
      "result": {"col_a": [v, v, ...], "col_b": [...]}
    }

Opteryx is used as the oracle to determine the *column order* (the JSON dict's
key order is not authoritative — the test runner sorts keys before comparing),
and to verify the test currently passes. If Opteryx's output disagrees with the
recorded ``result`` column-by-column, the test is skipped with a warning rather
than baking a stale value into the ``.slt``.

Usage:
    PYTHONPATH=/path/to/opteryx-core \
        python3 convert_results_tests.py \
            --src /path/to/opteryx-core/tests/integration/sql_battery/test_data/tests/results \
            --dest tests/opteryx/results
"""

from __future__ import annotations

import argparse
import json
import math
import os
import pathlib
import re
import sys
from decimal import Decimal
from typing import Any

import opteryx


def _norm(value: Any) -> Any:
    """Project a value into a comparable canonical form for cross-checking."""
    if isinstance(value, bytes):
        try:
            return value.decode("utf-8")
        except UnicodeDecodeError:
            return value.decode("utf-8", errors="replace")
    if isinstance(value, Decimal):
        return float(value)
    return value


def _format_cell(value: Any) -> str:
    """Render a value using the same rules as opteryx_driver._format_cell."""
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "1" if value else "0"
    if isinstance(value, bytes):
        try:
            value = value.decode("utf-8")
        except UnicodeDecodeError:
            value = value.decode("utf-8", errors="replace")
    if isinstance(value, str):
        return value if value else "(empty)"
    if isinstance(value, Decimal):
        return f"{float(value):.3f}"
    if isinstance(value, float):
        if math.isnan(value):
            return "NaN"
        if math.isinf(value):
            return "Inf" if value > 0 else "-Inf"
        return f"{value:.3f}"
    if isinstance(value, int):
        return str(value)
    if isinstance(value, (list, tuple)):
        return "[" + ",".join(_format_cell(v) for v in value) + "]"
    return str(value)


def _type_letter(values: list) -> str:
    """Pick a sqllogictest type letter for a column based on its values."""
    has_float = False
    has_int_or_bool = False
    for v in values:
        if v is None:
            continue
        if isinstance(v, bool):
            has_int_or_bool = True
        elif isinstance(v, (Decimal, float)):
            has_float = True
        elif isinstance(v, int):
            has_int_or_bool = True
        else:
            return "T"
    if has_float:
        return "R"
    if has_int_or_bool:
        return "I"
    return "T"  # all NULLs — fall back to text


def _has_top_level_order_by(sql: str) -> bool:
    """Heuristic: does the SQL have an ORDER BY outside any parentheses?"""
    depth = 0
    upper = sql.upper()
    i = 0
    while i < len(sql):
        c = sql[i]
        if c == "(":
            depth += 1
        elif c == ")":
            depth -= 1
        elif depth == 0 and upper.startswith("ORDER BY", i):
            # Make sure ORDER is a whole word.
            before = sql[i - 1] if i > 0 else " "
            after_idx = i + len("ORDER BY")
            after = sql[after_idx] if after_idx < len(sql) else " "
            if not before.isalnum() and not after.isalnum():
                return True
            i += len("ORDER BY")
            continue
        i += 1
    return False


def _run(session, sql: str):
    """Run SQL and return (column_names, rows_as_lists). Raises on engine error."""
    morsels = list(session.execute_to_morsels(sql))
    columns: list[str] = []
    rows: list[list[Any]] = []
    for m in morsels:
        if m is None:
            continue
        table = m.to_arrow()
        if not columns:
            columns = list(table.column_names)
        for record in table.to_pylist():
            rows.append([record[c] for c in columns])
    return columns, rows


def _columns_match(actual_cols: list[str], expected_result: dict) -> bool:
    return set(actual_cols) == set(expected_result.keys())


def _values_match(
    actual_cols: list[str], actual_rows: list[list[Any]], expected: dict
) -> bool:
    """Multiset-compare actual rows against the expected per-column lists."""
    if not actual_rows and not any(expected.values()):
        return True
    n_rows = len(actual_rows)
    if any(len(v) != n_rows for v in expected.values()):
        return False

    def canon(row, cols):
        return tuple(_norm(v) for v in (row[cols.index(c)] for c in sorted(cols)))

    sorted_cols = sorted(actual_cols)
    actual_set = sorted(canon(r, actual_cols) for r in actual_rows)
    expected_rows = [
        [expected[c][i] for c in actual_cols] for i in range(n_rows)
    ]
    expected_set = sorted(canon(r, actual_cols) for r in expected_rows)
    return actual_set == expected_set


def convert_one(path: pathlib.Path, session) -> tuple[str | None, str | None]:
    """Return (slt_text, error). Exactly one is non-None."""
    raw = path.read_text(encoding="utf-8")
    # Opteryx's runner uses yyjson (relaxed); permit trailing commas so we can
    # parse files like union_001 with stdlib json.
    cleaned = re.sub(r",(\s*[}\]])", r"\1", raw)
    try:
        doc = json.loads(cleaned, strict=False)
    except json.JSONDecodeError as e:
        return None, f"json parse failed: {e}"

    sql = doc.get("statement", "").strip()
    summary = doc.get("summary", "").strip()
    expected = doc.get("result", {}) or {}
    if not sql or not isinstance(expected, dict):
        return None, "missing statement or result"

    try:
        cols, rows = _run(session, sql)
    except Exception as e:  # noqa: BLE001
        return None, f"opteryx error: {type(e).__name__}: {e}"

    if not _columns_match(cols, expected):
        return None, f"column-name mismatch: got {cols}, expected {list(expected.keys())}"
    if not _values_match(cols, rows, expected):
        return None, "value mismatch vs recorded result"

    type_letters = "".join(
        _type_letter([row[i] for row in rows]) for i in range(len(cols))
    )
    sort_mode = "" if _has_top_level_order_by(sql) else " rowsort"

    formatted = [[_format_cell(v) for v in row] for row in rows]
    if sort_mode:
        # sqllogictest's `rowsort` sorts the engine output but expects the
        # ``.slt`` to already be in sorted order — pre-sort to match.
        formatted.sort()
    formatted_rows = [" ".join(cells) for cells in formatted]

    sql_block = sql.rstrip(";").rstrip()
    parts: list[str] = []
    parts.append(f"# Source: {path.name}")
    if summary:
        parts.append(f"# {summary}")
    parts.append(f"query {type_letters}{sort_mode}")
    parts.append(sql_block)
    parts.append("----")
    parts.extend(formatted_rows)
    parts.append("")
    return "\n".join(parts), None


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--src", required=True, type=pathlib.Path)
    ap.add_argument("--dest", required=True, type=pathlib.Path)
    ap.add_argument(
        "--workdir",
        type=pathlib.Path,
        help="chdir here before running queries (so relative paths like "
        "'testdata/...' resolve). Defaults to $OPTERYX_HOME if set.",
    )
    args = ap.parse_args()
    workdir = args.workdir or (
        pathlib.Path(os.environ["OPTERYX_HOME"]) if "OPTERYX_HOME" in os.environ else None
    )
    if workdir is not None:
        os.chdir(workdir)

    args.dest.mkdir(parents=True, exist_ok=True)
    session_factory = opteryx.session

    files = sorted(args.src.glob("*.results_tests"))
    if not files:
        print(f"no *.results_tests files under {args.src}", file=sys.stderr)
        return 1

    converted = 0
    skipped = []
    for path in files:
        # Fresh session per file matches how the .slt will be run.
        session = session_factory()
        text, err = convert_one(path, session)
        try:
            session.close()
        except Exception:
            pass
        out_name = re.sub(r"\.results_tests$", ".slt", path.name)
        out_path = args.dest / out_name
        if text is not None:
            out_path.write_text(text, encoding="utf-8")
            converted += 1
            print(f"  ok   {path.name} -> {out_name}")
        else:
            skipped.append((path.name, err))
            print(f"  SKIP {path.name}: {err}", file=sys.stderr)

    print()
    print(f"converted {converted}/{len(files)} files into {args.dest}")
    if skipped:
        print(f"skipped {len(skipped)}:")
        for name, err in skipped:
            print(f"  - {name}: {err}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
