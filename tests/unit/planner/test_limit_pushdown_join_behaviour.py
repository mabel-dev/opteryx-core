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


def test_limit_pushdown_cross_join_prefers_smaller_side():
    query = (
        "SELECT * FROM testdata.planets AS p CROSS JOIN testdata.satellites AS s LIMIT 5;"
    )
    count = execute_and_get_rowcount(query)
    assert count == 5

if __name__ == "__main__":  # pragma: no cover
    pytest.main([__file__])
