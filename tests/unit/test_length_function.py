"""Tests for the LENGTH() function and its underlying kernel.

These focus on the regression where the LENGTH kernel was the list-oriented
``vector_length`` kernel. That raised `opteryx.exceptions.FunctionExecutionError`
when the argument arrived as a string vector (the vast majority of real queries).

Here we exercise the function via the planner to ensure the fix is end-to-end.
"""

import opteryx


def test_length_function_via_sql():
    session = opteryx.session()
    rows = [
        value
        for morsel in session.execute_to_morsels(
            "SELECT LENGTH(name) AS n FROM $planets WHERE LENGTH(name) > 5"
        )
        for value in morsel.column("n").to_pylist()
    ]
    # Mercury, Jupiter, Saturn, Uranus, Neptune
    assert sorted(rows) == [6, 6, 7, 7, 7], rows
