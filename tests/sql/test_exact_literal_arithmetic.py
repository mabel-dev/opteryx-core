"""Regression: arithmetic between decimal-point literals is folded exactly.

A literal such as `0.06` is a FLOAT64, so folding `0.06 + 0.01` in floating point
produced 0.06999999999999999. As a filter bound that is one ulp below 0.07 and
drops every DECIMAL(15,2) row equal to 0.07. TPC-H Q6

    l_discount between 0.06 - 0.01 and 0.06 + 0.01

therefore returned 75,207,768 instead of 123,141,078 on SF1. The constant folder
(`_fold_exact_literal_arithmetic` in
opteryx/planner/optimizer/strategies/constant_folding.py) now computes literal
<op> literal in exact decimal arithmetic from each literal's text and rounds to
FLOAT64 once, so the folded bound equals the double the literal `0.07` parses to.
"""

import os
import sys

import pytest

sys.path.insert(1, os.path.join(os.path.dirname(__file__), "..", ".."))

import opteryx


def _scalar(sql):
    for morsel in opteryx.session().execute_to_morsels(sql):
        for row in morsel:
            return list(row)[0]
    raise AssertionError("no row returned")


@pytest.mark.parametrize(
    "expression, expected",
    [
        ("0.06 + 0.01", 0.07),
        ("0.06 - 0.01", 0.05),
        ("0.1 + 0.2", 0.3),
        ("0.1 * 3", 0.3),
        ("1.1 * 1.1", 1.21),
        ("3.3 / 1.1", 3.0),
        ("0.3 - 0.1", 0.2),
        ("4.35 * 100", 435.0),
        ("0.06 + 0.01 + 0.02", 0.09),
        ("5 + 2.5", 7.5),
    ],
)
def test_float_literal_arithmetic_is_exact(expression, expected):
    # Compared with ==: the point is the exact double the decimal text names.
    assert _scalar(f"SELECT {expression}") == expected


def test_folded_sum_equals_the_written_literal():
    assert _scalar("SELECT 0.06 + 0.01 = 0.07") is True


@pytest.mark.parametrize(
    "expression, expected",
    [
        ("1 + 2", 3),  # integer arithmetic keeps its own semantics
        ("7 / 2", 3.5),
        ("1e16 + 1.0", 1e16),  # nothing to round: the exact and float results agree
        ("1.5 % 1", 0.5),  # not one of the four operators
    ],
)
def test_other_literal_arithmetic_is_unchanged(expression, expected):
    assert _scalar(f"SELECT {expression}") == expected


@pytest.mark.parametrize(
    "expression, check",
    [
        ("1.0 / 0.0", lambda v: v == float("inf")),
        ("1e308 * 10.0", lambda v: v == float("inf")),
    ],
)
def test_division_by_zero_and_overflow_fall_through_to_the_float_result(expression, check):
    assert check(_scalar(f"SELECT {expression}"))


def test_decimal_column_bounds_keep_the_boundary_rows():
    """The Q6 shape: BETWEEN over a DECIMAL(15,2) column, bounds written as sums.

    Same rows as the bounds written out, and the 0.07 rows are among them.
    """
    table = "testdata.tpch_001.lineitem"
    written = _scalar(f"SELECT COUNT(*) FROM {table} WHERE l_discount BETWEEN 0.05 AND 0.07")
    computed = _scalar(
        f"SELECT COUNT(*) FROM {table} WHERE l_discount BETWEEN 0.06 - 0.01 AND 0.06 + 0.01"
    )
    top_edge = _scalar(f"SELECT COUNT(*) FROM {table} WHERE l_discount = 0.07")
    assert top_edge > 0
    assert computed == written


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
