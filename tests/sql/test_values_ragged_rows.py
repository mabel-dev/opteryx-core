"""A VALUES list is rectangular, and its columns are all named or none are.

The rule: every row has the same number of values, and a column list names exactly
that many columns. With no column list the columns are `column_1` .. `column_n`.
Anything else is a SqlError from the binder, naming the mismatch.

None of it was checked. A short row was padded with NULL and a long row truncated, both
silently, so `VALUES (1, 'x'), (2)` answered (2, NULL) where the standard and DuckDB
refuse the query. A column list narrower than the rows silently dropped the unnamed
columns. A short FIRST row, or a column list wider than the rows, died at execution as
a bare IndexError. And with no column list at all the query returned NO columns.
"""

import os
import sys

import pytest

sys.path.insert(1, os.path.join(os.path.dirname(__file__), "..", ".."))

import opteryx
from opteryx.exceptions import SqlError


def run(sql):
    for _ in opteryx.session().execute_to_morsels(sql):
        pass


@pytest.mark.parametrize(
    "sql,message",
    [
        # Short row after the first: used to be padded with NULL.
        ("SELECT * FROM (VALUES (1, 'x'), (2)) AS v(a, b)", "row 1 has 2 and row 2 has 1"),
        # Short FIRST row: used to be an IndexError.
        ("SELECT * FROM (VALUES (1), (2, 'y')) AS v(a, b)", "row 1 has 1 and row 2 has 2"),
        # Long row: used to be truncated.
        ("SELECT * FROM (VALUES (1, 'x'), (2, 'y', 3)) AS v(a, b)",
         "row 1 has 2 and row 2 has 3"),
        # The row that differs is named, not just the first mismatch against row 2.
        ("SELECT * FROM (VALUES (1, 'x'), (2, 'y'), (3)) AS v(a, b)",
         "row 1 has 2 and row 3 has 1"),
        # More column names than values: used to be an IndexError.
        ("SELECT * FROM (VALUES (1), (2)) AS v(a, b)", "names 2 columns"),
        # Fewer column names than values: used to drop the unnamed column.
        ("SELECT * FROM (VALUES (1, 'x'), (2, 'y')) AS v(a)", "names 1 column "),
    ],
)
def test_ragged_values_refused(sql, message):
    with pytest.raises(SqlError, match=message):
        run(sql)


def test_rectangular_values_still_run():
    out = {}
    for morsel in opteryx.session().execute_to_morsels(
        "SELECT * FROM (VALUES (1, 'x'), (2, NULL)) AS v(a, b)"
    ):
        for key, values in morsel.to_arrow().to_pydict().items():
            out.setdefault(key, []).extend(values)
    assert out == {"a": [1, 2], "b": ["x", None]}


def test_unnamed_values_columns_are_numbered():
    """No column list: the columns are `column_1` .. `column_n` - it used to be {}."""
    out = {}
    for morsel in opteryx.session().execute_to_morsels(
        "SELECT * FROM (VALUES (1, 'x'), (2, 'y')) AS v"
    ):
        for key, values in morsel.to_arrow().to_pydict().items():
            out.setdefault(key, []).extend(values)
    assert out == {"column_1": [1, 2], "column_2": ["x", "y"]}


def test_unnamed_values_columns_can_be_referenced():
    out = []
    for morsel in opteryx.session().execute_to_morsels(
        "SELECT column_2 FROM (VALUES (1, 'x'), (2, 'y')) AS v WHERE v.column_1 > 1"
    ):
        out.extend(morsel.to_arrow().to_pydict()["column_2"])
    assert out == ["y"]


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
