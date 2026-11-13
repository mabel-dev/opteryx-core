"""
A CAST of a constant expression to its own type folds, it is not refused.

`CAST(x AS T)` where x is already T runs no kernel: RedundantCastElimination removes
it. That strategy runs after the first ConstantFolding pass, so a CONSTANT one —
`CAST(ABS(1.5) AS FLOAT)` — reached the native evaluator, which has no FLOAT64 ->
FLOAT64 kernel, and the query died with "CAST FLOAT64 → FLOAT64 is not supported".
The same cast over a column always worked. Found by the single-table fuzzer
(2026-11-13, seed 1491173701740002103).
"""

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import pytest

import opteryx


def _rows(sql):
    return [tuple(m[i]) for m in opteryx.session().execute_to_morsels(sql) for i in range(len(m))]


@pytest.mark.parametrize(
    "expression,expected",
    [
        ("CAST(ABS(1.5) AS FLOAT)", 1.5),
        ("CAST(1.5 * 2.0 AS FLOAT)", 3.0),
        ("TRY_CAST(1.5 * 2.0 AS FLOAT)", 3.0),
        ("CAST(ABS(2) AS INTEGER)", 2),
        ("CAST(CAST(ABS(2) AS INTEGER) AS VARCHAR)", "2"),
        # A cast that is NOT an identity still runs its kernel.
        ("CAST(ABS(1.5) AS INTEGER)", 1),
    ],
)
def test_constant_cast_to_own_type_folds(expression, expected):
    assert _rows(f"SELECT {expression} AS x FROM $planets LIMIT 1") == [(expected,)]


def test_constant_identity_cast_in_a_filter():
    assert _rows("SELECT name FROM $planets WHERE id = CAST(ABS(-3) AS INTEGER)") == [("Earth",)]


def test_two_constant_identity_casts_keep_their_own_names_and_values():
    morsels = list(
        opteryx.session().execute_to_morsels(
            "SELECT CAST(ABS(1.5) AS FLOAT), CAST(ABS(2.5) AS FLOAT) FROM $planets LIMIT 1"
        )
    )
    assert [tuple(m[i]) for m in morsels for i in range(len(m))] == [(1.5, 2.5)]
