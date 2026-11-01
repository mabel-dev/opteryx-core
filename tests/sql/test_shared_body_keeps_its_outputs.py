"""
A shared CTE body keeps every column its references read.

A window over a grouped, ORDERED relation filtered on an aggregate output reads the
relation twice (the window rewrite copies its source), so the body is shared. Both
references carry the WHERE, so the coordination hoists it onto the body's head — once
the boundary that stated the body's width has been stripped. The body's own projection
pushdown then took the hoisted filter's columns (`a2`) as the whole demand, the Sort
emitted only those, and the reference reading `density` was refused: "a CTE reference
column the shared body does not carry". The body's liveness is now seeded from its
outputs (what its references read). Found by the single-table fuzzer; answers checked
against DuckDB.
"""

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import pytest

import opteryx


def _rows(sql):
    return [morsel[i] for morsel in opteryx.session().execute_to_morsels(sql) for i in range(len(morsel))]


BODY = "SELECT density, MAX(diameter) AS a2 FROM testdata.planets GROUP BY density ORDER BY density"


@pytest.mark.parametrize(
    "statement",
    [
        f"SELECT MIN(density) OVER () AS w FROM ({BODY}) AS c WHERE a2 > 0",
        f"WITH c AS ({BODY}) SELECT MIN(density) OVER () AS w FROM c WHERE a2 > 0",
        f"WITH c AS ({BODY}) SELECT a2, MIN(density) OVER (PARTITION BY a2) AS w FROM c WHERE a2 > 0",
    ],
)
def test_window_over_filtered_ordered_grouped_relation(statement):
    result = _rows(statement)
    assert len(result) == 9
    # every group passes `a2 > 0`, so the overall minimum is the smallest density
    # (Saturn's, 687 — as DuckDB answers)
    if "OVER ()" in statement:
        assert {row[-1] for row in result} == {687}


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
