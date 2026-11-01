"""
A multi-value BOOLEAN IN list runs, with SQL's three-valued result.

The native IN-list lowering had no BOOLEAN arm, so `b IN (FALSE, TRUE)` was refused —
in a filter ("outside the c-native kernel set") and in a scan ("unlowerable_predicate").
Worse, the optimizer MAKES these lists: `b = FALSE OR b = TRUE` and
`b <> FALSE AND NOT b = TRUE` are folded to `b IN [False, True]` / `b NOT IN [...]`, so
queries that ran as written were refused after rewriting. Found by the join and
single-table fuzzers. Expected counts were checked against DuckDB:
testdata.fuzzing.mixed has 2,000 rows, b_null is NULL on 1,016 of them, TRUE on 492.
"""

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import pytest

import opteryx


def _rows(sql):
    return [morsel[i] for morsel in opteryx.session().execute_to_morsels(sql) for i in range(len(morsel))]


@pytest.mark.parametrize(
    "predicate,expected",
    [
        ("b_null IN (FALSE, TRUE)", 984),
        ("b_null NOT IN (FALSE, TRUE)", 0),
        ("b_null IN (TRUE, TRUE)", 492),
        ("b_value IN (FALSE, TRUE)", 2000),
        # optimizer-made lists
        ("b_null = FALSE OR b_null = TRUE", 984),
        ("b_null <> FALSE AND NOT b_null = TRUE", 0),
        ("b_null IN (FALSE, TRUE) OR i_group = 1", 1050),
    ],
)
def test_boolean_in_list_filters(predicate, expected):
    assert _rows(f"SELECT COUNT(*) FROM testdata.fuzzing.mixed WHERE {predicate}") == [(expected,)]


def test_boolean_in_list_is_null_on_a_null_operand():
    result = dict(_rows(
        "SELECT b_null IN (FALSE, TRUE) AS x, COUNT(*) FROM testdata.fuzzing.mixed "
        "GROUP BY b_null IN (FALSE, TRUE)"
    ))
    assert result == {True: 984, None: 1016}


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
