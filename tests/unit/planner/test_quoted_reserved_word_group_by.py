# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Regression: GROUP BY on a double-quoted reserved-word column.

`testdata.astronauts` has a real column literally named `group` (a reserved
word). Double quotes are keyword/identifier escapes (standard SQL), so `"group"`
names that column. Previously the dialect tokenised `"group"` as a *string*
literal; the GROUP BY then registered a literal (which the aggregate binder
prunes, as it must for `GROUP BY 1`), collapsing to zero group columns and
silently returning a single degenerate row (the string 'group' with a count of
ALL rows).

The fix makes the dialect treat double quotes as an identifier delimiter, so
`"group"` binds to the column. Single quotes remain the only string delimiter.
Answers are checked against DuckDB.
"""

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

import opteryx


def _run(sql):
    session = opteryx.session()
    rows = []
    for morsel in session.execute_to_morsels(sql):
        names = list(morsel.column_names)
        cols = [morsel.column(n).to_pylist() for n in names]
        rows.extend(tuple(c[i] for c in cols) for i in range(morsel.num_rows))
    return rows


def test_group_by_double_quoted_reserved_word():
    sql = 'SELECT "group" g, COUNT(*) c FROM testdata.astronauts GROUP BY "group"'
    rows = sorted(_run(sql), key=lambda r: (r[0] is None, r[0]))

    # DuckDB oracle: one row per distinct value of `group`, plus the NULL group.
    expected = sorted(
        [
            (1.0, 7), (2.0, 9), (3.0, 15), (4.0, 6), (5.0, 19), (6.0, 11),
            (7.0, 7), (8.0, 35), (9.0, 19), (10.0, 17), (11.0, 13), (12.0, 15),
            (13.0, 22), (14.0, 19), (15.0, 19), (16.0, 35), (17.0, 25),
            (18.0, 17), (19.0, 11), (20.0, 9), (None, 27),
        ],
        key=lambda r: (r[0] is None, r[0]),
    )

    assert rows == expected, rows
    # Not the old degenerate single-group collapse.
    assert len(rows) == 21, len(rows)
    assert sum(r[1] for r in rows) == 357


def test_double_quoted_reserved_word_matches_backtick_identifier():
    key = lambda r: (r[0] is None, r[0])
    quoted = sorted(_run('SELECT "group" g, COUNT(*) c FROM testdata.astronauts GROUP BY "group"'), key=key)
    backtick = sorted(_run("SELECT `group` g, COUNT(*) c FROM testdata.astronauts GROUP BY `group`"), key=key)
    assert quoted == backtick, (quoted, backtick)


def test_double_quoted_compound_identifier_resolves():
    # `"table"."col"` previously raised `Unhandled token ... CompoundFieldAccess`
    # because double quotes tokenised as strings. As an identifier escape it now
    # parses as a CompoundIdentifier and resolves to the column.
    rows = _run('SELECT "a"."name" n FROM $planets a ORDER BY "a"."name" LIMIT 3')
    assert rows == [("Earth",), ("Jupiter",), ("Mars",)], rows


def test_genuine_literal_group_by_still_one_group():
    # A constant that does NOT name a column must remain a single literal group.
    assert _run("SELECT 1 k, COUNT(*) c FROM $planets GROUP BY 1") == [(1, 9)]
    assert _run("SELECT 'x' k, COUNT(*) c FROM $planets GROUP BY 'x'") == [("x", 9)]


if __name__ == "__main__":  # pragma: no cover
    test_group_by_double_quoted_reserved_word()
    test_double_quoted_reserved_word_matches_backtick_identifier()
    test_double_quoted_compound_identifier_resolves()
    test_genuine_literal_group_by_still_one_group()
    print("✅ okay")
