#!/usr/bin/env python3
"""Convert Opteryx ``test_shapes_*.py`` (and ``test_casts_battery.py``)
to ``.slt`` files using the new ``query shape`` directive.

Each Python module exposes a ``STATEMENTS`` list of
``(sql, rows, cols, exception_or_None)`` tuples. Conversion is purely a
text transformation — no live Opteryx execution — so it runs in seconds:

* If ``exception`` is ``None``  →  ``query shape <rows> <cols>`` (or
  ``query shape <rows>`` when there are 0 rows, since the Python harness
  does not check the column count in that case).
* If ``exception`` is set       →  ``statement error <ExceptionClassName>``.

The resulting ``.slt`` file carries the same expectations the Python test
encoded; running it through sqllogictest is the validation step. Drift
between the Python-recorded expectation and current Opteryx behavior shows
up as ordinary slt failures, which is exactly the signal you want.

Usage:
    PYTHONPATH=$OPTERYX_HOME \
        python3 convert_shape_tests.py \
            --src-dir $OPTERYX_HOME/tests/integration/sql_battery \
            --dest /path/to/sqllogictest/examples/opteryx/tests/shapes
"""

from __future__ import annotations

import argparse
import importlib
import pathlib
import re
import sys

# Modules to migrate. ``test_shapes_basic`` is intentionally absent — that
# is what ``make q`` runs in opteryx-core, and we keep it there for now.
DEFAULT_MODULES = [
    "test_shapes_aliases_distinct",
    "test_shapes_basic_remote",
    "test_shapes_data_sources",
    "test_shapes_edge_cases",
    "test_shapes_functions_aggregates",
    "test_shapes_joins_subqueries",
    "test_shapes_operators_expressions",
    "test_shapes_split",
    "test_casts_battery",
    "test_exclude_arm",
]


def _load_statements(module_name: str, src_dir: pathlib.Path) -> list[tuple]:
    if str(src_dir) not in sys.path:
        sys.path.insert(0, str(src_dir))
    mod = importlib.import_module(module_name)
    out: list[tuple] = []
    for tup in mod.STATEMENTS:
        if len(tup) != 4:
            continue
        sql, rows, cols, exc = tup
        if isinstance(sql, bytes):
            sql = sql.decode("utf-8")
        out.append((sql, rows, cols, exc))
    return out


def _flatten_sql(sql: str) -> str:
    """Collapse multi-line SQL into a single line.

    sqllogictest treats blank lines as record separators, so multi-line SQL
    can't survive verbatim. ``--`` line comments are stripped first so they
    don't swallow the rest of the statement once newlines are gone.
    """
    cleaned: list[str] = []
    for line in sql.splitlines():
        in_quote = False
        cut = len(line)
        i = 0
        while i < len(line):
            c = line[i]
            if c == "'":
                in_quote = not in_quote
            elif (
                not in_quote
                and c == "-"
                and i + 1 < len(line)
                and line[i + 1] == "-"
            ):
                cut = i
                break
            i += 1
        cleaned.append(line[:cut])
    return re.sub(r"\s+", " ", " ".join(cleaned)).strip().rstrip(";").strip()


def _exc_class_name(exc) -> str:
    return exc.__name__ if isinstance(exc, type) else type(exc).__name__


def _emit(module_name: str, statements: list[tuple]) -> tuple[str, dict[str, int]]:
    parts: list[str] = [f"# Source: {module_name}.py", ""]
    stats = {"shape": 0, "error": 0, "skipped": 0}
    for sql, rows, cols, exc in statements:
        sql = _flatten_sql(sql)
        if not sql:
            stats["skipped"] += 1
            continue
        if exc is not None:
            parts.append(f"statement error {re.escape(_exc_class_name(exc))}")
            stats["error"] += 1
        else:
            row_str = str(rows) if rows is not None else "-"
            # Python harness skips the column check when rows == 0 (no morsel
            # means no observable schema), so do the same here.
            if rows == 0 or cols is None:
                parts.append(f"query shape {row_str}")
            else:
                parts.append(f"query shape {row_str} {cols}")
            stats["shape"] += 1
        parts.append(sql)
        parts.append("")
    if parts[-1] != "":
        parts.append("")
    return "\n".join(parts), stats


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--src-dir", required=True, type=pathlib.Path)
    ap.add_argument("--dest", required=True, type=pathlib.Path)
    ap.add_argument("--module", action="append", help="Restrict to these modules.")
    args = ap.parse_args()

    args.dest.mkdir(parents=True, exist_ok=True)
    modules = args.module or DEFAULT_MODULES

    grand = {"shape": 0, "error": 0, "skipped": 0}
    for module_name in modules:
        try:
            statements = _load_statements(module_name, args.src_dir)
        except Exception as e:  # noqa: BLE001
            print(f"  FAIL {module_name}: import error {e}")
            continue
        if not statements:
            print(f"  skip {module_name}: no STATEMENTS")
            continue
        text, stats = _emit(module_name, statements)
        out_name = module_name.replace("test_", "", 1) + ".slt"
        (args.dest / out_name).write_text(text, encoding="utf-8")
        for k, v in stats.items():
            grand[k] += v
        print(
            f"  ok {module_name}: {len(statements)} cases -> {out_name} "
            f"({stats['shape']} shape, {stats['error']} error, "
            f"{stats['skipped']} skipped)"
        )

    print()
    print(
        f"totals: {grand['shape']} shape, {grand['error']} error, "
        f"{grand['skipped']} skipped"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
