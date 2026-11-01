"""
A NULL operand of LIKE / ILIKE / RLIKE (or a NULL array under ANY / ALL) makes the
predicate NULL on every row, like any other comparison with NULL.

* An untyped NULL reached the draken LIKE kernels as a DRAKEN_NULL vector and failed
  with "draken_like: string operands required" (also the `_STARTS_WITH` / contains
  forms the optimizer rewrites LIKE into).
* A typed NULL pattern crashed the optimizer's LIKE rewrite (`b"%%" in None`), and
  RLIKE refused it ("pattern must be a string constant").
* `x = ANY(CAST(NULL AS ARRAY<T>))` crashed the binder (`tuple(None)`).
`name RLIKE NULL` (untyped) is not here: sqlparser 0.63 swallows the NULL keyword
after RLIKE, so the statement never reaches Opteryx intact.
"""

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import pytest

import opteryx


def _rows(sql):
    return [tuple(m[i]) for m in opteryx.session().execute_to_morsels(sql) for i in range(len(m))]


@pytest.mark.parametrize(
    "expression",
    [
        "'a' LIKE NULL",
        "NULL LIKE 'a'",
        "NULL LIKE NULL",
        "NULL LIKE '%a%'",
        "NULL LIKE 'a%'",
        "NULL ILIKE '%a'",
        "NULL NOT LIKE 'a'",
        "NULL RLIKE 'a'",
        "name LIKE NULL",
        "name NOT LIKE NULL",
        "name ILIKE NULL",
        "name NOT ILIKE NULL",
        "name LIKE CAST(NULL AS VARCHAR)",
        "name NOT ILIKE CAST(NULL AS VARCHAR)",
        "name RLIKE CAST(NULL AS VARCHAR)",
        "name NOT RLIKE CAST(NULL AS VARCHAR)",
        "name = ANY(CAST(NULL AS ARRAY<VARCHAR>))",
        "name != ALL(CAST(NULL AS ARRAY<VARCHAR>))",
        "name LIKE ANY(CAST(NULL AS ARRAY<VARCHAR>))",
        "id = ANY(CAST(NULL AS ARRAY<INT64>))",
    ],
)
def test_null_operand_is_null(expression):
    assert _rows(f"SELECT {expression} AS x FROM $planets") == [(None,)] * 9
    # a WHERE keeps no row, nor does its negation
    assert _rows(f"SELECT COUNT(*) FROM testdata.planets WHERE {expression}") == [(0,)]
    assert _rows(f"SELECT COUNT(*) FROM testdata.planets WHERE NOT ({expression})") == [(0,)]


@pytest.mark.parametrize(
    "sql,expected",
    [
        ("SELECT COUNT(*), COUNT(name LIKE NULL) FROM $planets", [(9, 0)]),
        ("SELECT COUNT(*) FROM testdata.planets WHERE name ILIKE NULL OR id = 1", [(1,)]),
        ("SELECT COUNT(*) FROM testdata.planets WHERE name NOT RLIKE CAST(NULL AS VARCHAR) OR id = 2", [(1,)]),
        ("SELECT name LIKE 'E%' OR name LIKE NULL AS x FROM $planets WHERE id = 3", [(True,)]),
        ("SELECT name LIKE 'E%' OR name LIKE NULL AS x FROM $planets WHERE id = 4", [(None,)]),
    ],
)
def test_null_operand_in_larger_predicates(sql, expected):
    assert _rows(sql) == expected


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
