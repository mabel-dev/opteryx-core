import pytest

import opteryx


def test_round_function_single_arg():
    """ROUND(x) should round to the nearest integer."""
    table = opteryx.session().execute_to_arrow("SELECT ROUND(1.5) AS r, ROUND(-1.5) AS s")
    assert table.num_rows == 1
    assert table.column(0)[0].as_py() == 2.0
    assert table.column(1)[0].as_py() == -2.0


def test_round_function_two_args():
    """ROUND(x, digits) should round to the specified number of decimal places."""
    table = opteryx.session().execute_to_arrow("SELECT ROUND(1.2345, 2) AS r")
    assert table.num_rows == 1
    assert abs(table.column(0)[0].as_py() - 1.23) < 1e-12


def test_round_function_works_in_predicate():
    """ROUND can be used in WHERE filters and returns expected rows."""
    table = opteryx.session().execute_to_arrow(
        """
        SELECT ROUND(v, 2) AS g
        FROM (VALUES (0.37755102), (0.90816327), (1.0), (0.37755102), (2.35714286)) AS t(v)
        WHERE ROUND(v, 2) > 1.1
        """
    )
    assert table.num_rows == 1
    assert abs(table.column(0)[0].as_py() - 2.36) < 1e-12


def test_round_function_integer_input_is_supported():
    """ROUND should accept integer vectors and still round correctly."""
    table = opteryx.session().execute_to_arrow(
        "SELECT ROUND(density, 2) AS r FROM (VALUES (1, 2), (1, 3)) AS t(a, density)"
    )
    assert table.num_rows == 2
    assert table.column(0)[0].as_py() == 2.0
    assert table.column(0)[1].as_py() == 3.0
