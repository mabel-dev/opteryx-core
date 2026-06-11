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


def test_limit_pushdown_left_outer_one_to_many():
    # Regression: LEFT OUTER JOIN where the *preserved* side joins 1:N against
    # the other side (each planet has many satellites). Limiting the preserved
    # input to n still yields >n output rows, so the LIMIT must stay above the
    # join. Previously the limit was pushed below the join and this returned 72.
    query = (
        "SELECT p.name FROM testdata.planets AS p "
        "LEFT JOIN testdata.satellites AS s ON p.id = s.planetId LIMIT 5;"
    )
    count = execute_and_get_rowcount(query)
    assert count == 5, count


def test_limit_pushdown_right_outer_one_to_many():
    # Mirror of the left-outer case: orders is the preserved (right) side and
    # joins 1:N against lineitem. (Uses tpch rather than planets/satellites: the
    # `satellites RIGHT JOIN planets` direction triggers a pre-existing, LIMIT-
    # independent execution crash that is out of scope for this fix.)
    query = (
        "SELECT o.o_orderkey FROM testdata.tpch_001.lineitem AS l "
        "RIGHT OUTER JOIN testdata.tpch_001.orders AS o "
        "ON o.o_orderkey = l.l_orderkey LIMIT 10;"
    )
    count = execute_and_get_rowcount(query)
    assert count == 10, count


def test_limit_pushdown_full_outer_one_to_many():
    query = (
        "SELECT p.name FROM testdata.planets AS p "
        "FULL OUTER JOIN testdata.satellites AS s ON p.id = s.planetId LIMIT 5;"
    )
    count = execute_and_get_rowcount(query)
    assert count == 5, count


def test_limit_pushdown_inner_join_one_to_many():
    # INNER JOIN is also row-multiplying from a single input; the LIMIT must not
    # be pushed below it.
    query = (
        "SELECT p.name FROM testdata.planets AS p "
        "INNER JOIN testdata.satellites AS s ON p.id = s.planetId LIMIT 5;"
    )
    count = execute_and_get_rowcount(query)
    assert count == 5, count


def test_limit_pushdown_bare_scan_still_pushed():
    # The valid case must keep working: a LIMIT directly over a single scan is
    # pushed into the scan (and the Limit node removed).
    query = "SELECT name FROM testdata.satellites LIMIT 5;"
    count = execute_and_get_rowcount(query)
    assert count == 5, count


def test_limit_pushdown_cross_join_not_pushed_to_one_side():
    # A LIMIT over a cross product must not be pushed into a single input:
    # limiting one side to n still yields |other side| * n rows.
    # (Projects a single qualified column rather than `SELECT *`: planets and
    # satellites share `id`/`name`, so `SELECT *` now raises AmbiguousIdentifierError
    # by design — see exit.pyx. The limit-pushdown behaviour is unchanged.)
    query = (
        "SELECT p.id FROM testdata.planets AS p CROSS JOIN testdata.satellites AS s LIMIT 5;"
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
