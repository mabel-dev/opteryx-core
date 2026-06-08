import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

from tests.helpers import execute_and_get_rowcount
import pytest


def test_limit_pushdown_left_outer_join():
    query = (
        "SELECT s.name FROM testdata.satellites AS s "
        "LEFT JOIN testdata.planets AS p ON s.planetId = p.id LIMIT 5;"
    )
    count = execute_and_get_rowcount(query)
    assert count == 5


def test_limit_pushdown_cross_join_not_pushed_to_one_side():
    # A LIMIT over a cross product must not be pushed into a single input:
    # limiting one side to n still yields |other side| * n rows.
    query = (
        "SELECT * FROM testdata.planets AS p CROSS JOIN testdata.satellites AS s LIMIT 5;"
    )
    count = execute_and_get_rowcount(query)
    assert count == 5


def test_limit_pushdown_bare_cross_join_row_count():
    # Regression: bare CROSS JOIN + LIMIT n previously returned |q| * n rows
    # because the limit was pushed to one scan. It must return exactly n.
    query = (
        "SELECT p.name FROM testdata.planets AS p "
        "CROSS JOIN testdata.planets AS q LIMIT 7;"
    )
    count = execute_and_get_rowcount(query)
    assert count == 7

if __name__ == "__main__":  # pragma: no cover
    pytest.main([__file__])
