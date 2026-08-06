# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Tests for the Opteryx command line - `python -m opteryx` and the `opteryx` command.

The CLI shipped calling `opteryx.query()`, which this library does not have, so
every invocation failed while `--help` kept working. Nothing covered it. This
covers what the CLI promises:

  - a one-shot query renders a table, with types, and a row/column/timing line
  - the REPL runs statements, reports errors, and keeps going
  - a failed query says so and exits non-zero
  - `--o` writes .jsonl and .md, and rejects anything else
  - `--cycles` reports a timing per cycle instead of a result
  - `--no-stats` and `--max_col_width` do what they say
  - the `opteryx` console script is declared, and points at the CLI

The end-to-end cases run in a subprocess: exit codes, argument parsing and what
actually reaches stdout are the things under test.
"""

import json
import os
import re
import subprocess
import sys
import tempfile
import tomllib

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

from opteryx.utils.display import format_jsonl
from opteryx.utils.display import format_markdown
from opteryx.utils.display import format_table

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))

# the box-drawing characters which start a rendered table's lines
_TABLE_LINE = ("┌", "│", "╞", "└")


def _cli(*argv, stdin=None):
    """Run the CLI the way a user runs it. Returns (exit_code, stdout, stderr)."""
    result = subprocess.run(
        [sys.executable, "-m", "opteryx", *argv],
        cwd=REPO_ROOT,
        input=stdin,
        capture_output=True,
        text=True,
    )
    return result.returncode, result.stdout, result.stderr


def _cells(line):
    """The values of a rendered table row, stripped of their padding."""
    return [cell.strip() for cell in line.split("│")[1:-1]]


def test_one_shot_query_renders_a_table():
    code, out, err = _cli(
        "--no-color", "SELECT name, gravity FROM $planets ORDER BY mass DESC LIMIT 4"
    )
    assert code == 0, err

    lines = out.splitlines()
    assert lines[0].startswith("┌")
    assert _cells(lines[1]) == ["", "name", "gravity"]
    assert _cells(lines[2]) == ["", "VARCHAR", "DECIMAL(3, 1)"]
    assert [_cells(line)[1] for line in lines[4:8]] == [
        "Jupiter",
        "Saturn",
        "Neptune",
        "Uranus",
    ]
    assert [_cells(line)[2] for line in lines[4:8]] == ["23.1", "9.0", "11.0", "8.7"]
    assert lines[8].startswith("└")
    assert "[ 4 rows x 2 columns ]" in out


def test_a_query_matching_nothing_renders_its_columns():
    code, out, err = _cli("--no-color", "SELECT name FROM $planets WHERE name = 'Vulcan'")
    assert code == 0, err
    assert "VARCHAR" in out
    assert "[ 0 rows x 1 columns ]" in out


def test_a_non_tabular_statement_reports_an_outcome():
    """A statement with no relation to show says what it did, not nothing."""
    code, out, err = _cli("--no-color", "SET @answer = 42")
    assert code == 0, err
    assert "rows affected" in out
    # nothing to draw a table from
    assert "\u250c" not in out


def test_repl_runs_statements_and_exits():
    code, out, err = _cli("--no-color", stdin="SELECT name FROM $planets LIMIT 2\n.exit\n")
    assert code == 0, err
    assert "Opteryx version" in out
    assert "Mercury" in out
    assert "Venus" in out
    assert "[ 2 rows x 1 columns ]" in out


def test_repl_reports_an_error_and_keeps_the_session():
    code, out, err = _cli(
        "--no-color",
        stdin="SELECT * FROM $nope\nSELECT name FROM $planets LIMIT 1\n.exit\n",
    )
    assert code == 0, err
    assert "Error" in out
    # the session survived the bad statement
    assert "Mercury" in out


def test_repl_help_lists_the_dot_commands():
    code, out, err = _cli("--no-color", stdin=".help\n.quit\n")
    assert code == 0, err
    assert ".exit" in out
    assert ".help" in out


def test_a_failed_query_reports_and_exits_non_zero():
    code, out, err = _cli("SELECT * FROM $nope")
    assert code == 1
    assert "Error" in out
    assert "$nope" in out


def test_output_to_jsonl():
    with tempfile.TemporaryDirectory() as folder:
        destination = os.path.join(folder, "planets.jsonl")
        code, out, err = _cli("--o", destination, "SELECT name, gravity FROM $planets LIMIT 3")
        assert code == 0, err
        assert "Written result to" in out

        with open(destination, mode="rb") as file:
            rows = [json.loads(line) for line in file if line.strip()]
    assert rows == [
        {"name": "Mercury", "gravity": 3.7},
        {"name": "Venus", "gravity": 8.9},
        {"name": "Earth", "gravity": 9.8},
    ]


def test_output_to_markdown():
    with tempfile.TemporaryDirectory() as folder:
        destination = os.path.join(folder, "planets.md")
        code, out, err = _cli("--o", destination, "SELECT name FROM $planets LIMIT 2")
        assert code == 0, err

        with open(destination, mode="r") as file:
            lines = file.read().splitlines()
    assert lines == ["| name |", "| --- |", "| Mercury |", "| Venus |"]


def test_an_unsupported_output_format_is_rejected():
    with tempfile.TemporaryDirectory() as folder:
        destination = os.path.join(folder, "planets.xlsx")
        code, out, err = _cli("--o", destination, "SELECT name FROM $planets LIMIT 2")
        assert code == 1
        assert "Unsupported output format" in out
        assert not os.path.exists(destination)


def test_cycles_reports_a_timing_for_each_cycle():
    code, out, err = _cli("--cycles", "3", "SELECT COUNT(*) FROM $planets")
    assert code == 0, err
    assert re.fullmatch(r"\[\d+\.\d{3},\d+\.\d{3},\d+\.\d{3}\]\n", out), out


def test_no_stats_suppresses_the_statistics_line():
    code, out, err = _cli("--no-color", "--no-stats", "SELECT name FROM $planets LIMIT 1")
    assert code == 0, err
    assert "Mercury" in out
    assert "rows x" not in out


def test_max_col_width_truncates_the_value():
    code, out, err = _cli(
        "--no-color", "--max_col_width", "10", "SELECT 'antidisestablishmentarianism' AS word"
    )
    assert code == 0, err
    assert "antidises…" in out
    assert "antidisestablishmentarianism" not in out


def test_help_describes_the_options():
    code, out, err = _cli("--help")
    assert code == 0, err
    for option in ("--o", "--no-color", "--no-stats", "--max_col_width", "--cycles"):
        assert option in out


def test_the_console_script_is_declared():
    """`pip install opteryx-core` has to put an `opteryx` command on PATH."""
    with open(os.path.join(REPO_ROOT, "pyproject.toml"), mode="rb") as file:
        metadata = tomllib.load(file)
    assert metadata["project"]["scripts"] == {"opteryx": "opteryx.command:main"}


def test_the_console_script_points_at_the_command_line():
    from opteryx.__main__ import main as module_entry_point
    from opteryx.command import main as script_entry_point

    assert script_entry_point is module_entry_point


def test_a_number_column_right_aligns_and_a_null_column_does_not():
    table = format_table(
        ["n", "v"], ["INT64", "VARCHAR"], [(1, None), (1000, None)], colorize=False
    )
    number, null = table.splitlines()[4].split("│")[2:4]
    assert number.strip() == "1"
    assert number.startswith("  ")
    assert null.strip() == "null"
    assert null.startswith(" null")


def test_columns_which_cannot_fit_are_dropped_and_reported():
    names = [f"column_{index}" for index in range(10)]
    table = format_table(
        names, ["VARCHAR"] * 10, [tuple("value" for _ in names)], display_width=40, colorize=False
    )
    drawn = [line for line in table.splitlines() if line.startswith(_TABLE_LINE)]
    assert max(len(line) for line in drawn) <= 40
    assert "columns too wide to display" in table


def test_markdown_escapes_the_column_separator():
    markdown = format_markdown(["a"], [("one | two",)])
    assert markdown.splitlines()[-1] == "| one \\| two |"


def test_jsonl_encodes_the_types_json_has_no_representation_for():
    import datetime
    import decimal

    row = (decimal.Decimal("3.14"), datetime.date(2025, 1, 31), b"\xff\xfe", None)
    encoded = json.loads(format_jsonl(["d", "when", "blob", "nothing"], row))
    assert encoded == {"d": 3.14, "when": "2025-01-31", "blob": "//4=", "nothing": None}


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
