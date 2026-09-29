"""INT64 add/sub/mul/neg/INT_DIVIDE overflow FAILS LOUD (ruling 2026-09-29).

Both kernel families are exercised: the draken_binop cross-width path
(fixed_int_ops.h) and the planner's literal folding. The contract mirrors SUM:
"never a wrapped answer". Divide / modulo by zero is covered by test_integer_divide_by_zero.py.
"""

import os
import sys

import pytest

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

import opteryx

MAX = 9223372036854775807
MIN = -9223372036854775808

# Two rows so the query runs through the vector kernels, not literal folding.
# The second row is benign; the first carries the operands under test.
TABLE = (
    "(SELECT CAST({a} AS INT64) AS a, CAST({b} AS INT64) AS b, 1 AS k "
    "UNION ALL SELECT CAST(1 AS INT64) AS a, CAST(1 AS INT64) AS b, 2 AS k) AS t"
)


def _rows(sql):
    session = opteryx.session()
    out = []
    for morsel in session.execute_to_morsels(sql):
        out.extend(morsel.to_arrow().to_pylist())
    return out


def _col(expr, a, b):
    return [r["r"] for r in _rows(f"SELECT {expr} AS r FROM {TABLE.format(a=a, b=b)} ORDER BY k")]


OVERFLOWS = [
    ("a + b", MAX, 1, "addition"),
    ("a + b", MIN, -1, "addition"),
    ("a - b", MIN, 1, "subtraction"),
    ("a - b", MAX, -1, "subtraction"),
    ("a * b", MAX, 2, "multiplication"),
    ("a * b", MIN, -1, "multiplication"),
    ("a DIV b", MIN, -1, "division"),
    ("a + 1", MAX, 0, "addition"),
    ("a * 4", 1 << 62, 0, "multiplication"),
    ("-a", MIN, 0, "negation"),
    ("0 - a", MIN, 0, "negation"),
]


@pytest.mark.parametrize("expr,a,b,name", OVERFLOWS)
def test_column_overflow_raises(expr, a, b, name):
    with pytest.raises(Exception, match=f"INT64 {name} overflow"):
        _col(expr, a, b)


@pytest.mark.parametrize(
    "sql,name",
    [
        (f"SELECT {MAX} + 1 AS r", "addition"),
        (f"SELECT {MIN} - 1 AS r", "subtraction"),
        ("SELECT 4611686018427387904 * 4 AS r", "multiplication"),
        (f"SELECT {MIN} DIV -1 AS r", "division"),
    ],
)
def test_literal_folding_overflow_raises(sql, name):
    with pytest.raises(Exception, match=f"INT64 {name} overflow"):
        _rows(sql)


@pytest.mark.parametrize(
    "expr,a,b,expected",
    [
        ("a + b", MAX, 0, MAX),
        ("a + b", MAX - 1, 1, MAX),
        ("a + b", MIN, 0, MIN),
        ("a + b", MIN, MAX, -1),
        ("a - b", MIN, 0, MIN),
        ("a - b", MAX, MAX, 0),
        ("a - b", -1, MAX, MIN),
        ("a * b", MIN, 1, MIN),
        ("a * b", MAX, -1, -MAX),
        ("a * b", 1 << 31, 1 << 31, 1 << 62),
        ("a DIV b", MIN, 1, MIN),
        ("a DIV b", MAX, -1, -MAX),
        ("a % b", MIN, -1, 0),
    ],
)
def test_boundary_values_that_fit_are_exact(expr, a, b, expected):
    assert _col(expr, a, b)[0] == expected


def test_null_operand_never_raises():
    rows = _rows(
        f"SELECT a + b AS r FROM (SELECT CAST({MAX} AS INT64) AS a, CAST(NULL AS INT64) AS b "
        "UNION ALL SELECT CAST(1 AS INT64) AS a, CAST(1 AS INT64) AS b) AS t"
    )
    assert sorted(r["r"] is None for r in rows) == [False, True]


def test_narrow_widths_widen_and_never_overflow():
    # INT32 * INT32 widens to INT64 (D.6): the product cannot overflow.
    rows = _rows(
        "SELECT a * b AS r FROM (SELECT CAST(2147483647 AS INT32) AS a, CAST(2147483647 AS INT32) AS b "
        "UNION ALL SELECT CAST(1 AS INT32) AS a, CAST(1 AS INT32) AS b) AS t"
    )
    assert sorted(r["r"] for r in rows) == [1, 2147483647 * 2147483647]


def test_abs_of_int64_min_raises_column_and_literal():
    with pytest.raises(Exception, match="INT64 absolute value overflow"):
        _col("ABS(a)", MIN, 0)
    with pytest.raises(Exception, match="INT64 absolute value overflow"):
        _rows(f"SELECT ABS({MIN}) AS r")


@pytest.mark.parametrize("a,expected", [(MIN + 1, MAX), (MAX, MAX), (-5, 5), (0, 0)])
def test_abs_values_that_fit_are_exact(a, expected):
    assert _col("ABS(a)", a, 0)[0] == expected


def test_abs_null_row_never_raises():
    rows = _rows(
        "SELECT ABS(a) AS r FROM (SELECT CAST(NULL AS INT64) AS a "
        "UNION ALL SELECT CAST(-3 AS INT64) AS a) AS t"
    )
    assert sorted(r["r"] is None for r in rows) == [False, True]
