"""
A projected empty-string comparison keeps its output column.

PredicateRewriter turns `col = ''` / `col <> ''` / `LENGTH(col) = 0` into
IsEmpty / IsNotEmpty. It built the new node with a freshly minted column, but the
rewrite also runs over SELECT lists, which reference the comparison by ITS identity:
`SELECT name, ('' = name) AS f FROM $planets` was refused with "an output column the
engine could not resolve here". In a WHERE nothing references the identity, so only
projections broke. Found by the single-table fuzzer's NoREC oracle, which projects
the WHERE predicate as a value. Counts checked against DuckDB: s_null is '' on 208
rows and NULL on 411 of testdata.fuzzing.mixed's 2,000.
"""

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import pytest

import opteryx


def _rows(sql):
    return [morsel[i] for morsel in opteryx.session().execute_to_morsels(sql) for i in range(len(morsel))]


@pytest.mark.parametrize(
    "expression,expected",
    [
        ("('' = s_null)", {False: 1381, None: 411, True: 208}),
        ("(s_null = '')", {False: 1381, None: 411, True: 208}),
        ("(s_null <> '')", {False: 208, None: 411, True: 1381}),
        ("(LENGTH(s_null) = 0)", {False: 1381, None: 411, True: 208}),
    ],
)
def test_projected_empty_string_compare(expression, expected):
    counts = dict(_rows(f"SELECT {expression} AS f, COUNT(*) FROM testdata.fuzzing.mixed GROUP BY {expression}"))
    assert counts == expected


def test_projected_empty_string_compare_beside_its_column():
    assert _rows("SELECT name, ('' = name) AS f FROM $planets WHERE id = 1") == [("Mercury", False)]


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
