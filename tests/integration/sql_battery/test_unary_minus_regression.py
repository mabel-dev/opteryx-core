"""
Regression test: unary minus on a column/expression.

`SELECT -id FROM $planets` previously crashed the planner with
`KeyError: 'Value'` in logical_planner_builders.unary_op, which assumed the
operand was always a numeric literal. Unary minus must now lower to `0 - expr`
for arbitrary expressions, while still constant-folding numeric literals.
"""

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

import opteryx


def _column(sql, name):
    """Return the named column's values across all emitted morsels."""
    key = name.encode() if isinstance(name, str) else name
    values = []
    for morsel in opteryx.session().execute_to_morsels(sql):
        values.extend(morsel.column(key).to_pylist())
    return values


def test_unary_minus_on_column():
    ids = _column("SELECT id FROM $planets ORDER BY id", "id")
    neg = _column("SELECT -id AS neg FROM $planets ORDER BY id", "neg")
    assert neg == [-v for v in ids], neg


def test_unary_minus_on_expression():
    base = _column("SELECT id + 1 AS v FROM $planets ORDER BY id", "v")
    neg = _column("SELECT -(id + 1) AS v FROM $planets ORDER BY id", "v")
    assert neg == [-v for v in base], neg


def test_unary_minus_matches_binary_subtraction():
    unary = _column("SELECT -id AS v FROM $planets ORDER BY id", "v")
    binary = _column("SELECT 0 - id AS v FROM $planets ORDER BY id", "v")
    assert unary == binary, (unary, binary)


def test_unary_plus_is_identity():
    ids = _column("SELECT id FROM $planets ORDER BY id", "id")
    plus = _column("SELECT +id AS v FROM $planets ORDER BY id", "v")
    assert plus == ids, plus


def test_unary_minus_literal_constant_folds():
    assert _column("SELECT -5 AS v", "v") == [-5]
    assert _column("SELECT -3.5 AS v", "v") == [-3.5]


if __name__ == "__main__":  # pragma: no cover
    test_unary_minus_on_column()
    test_unary_minus_on_expression()
    test_unary_minus_matches_binary_subtraction()
    test_unary_plus_is_identity()
    test_unary_minus_literal_constant_folds()
    print("✅ okay")
