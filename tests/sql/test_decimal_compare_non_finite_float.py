"""
A DECIMAL compared with a non-finite FLOAT64 constant answers instead of crashing.

The compiler moves a numeric literal onto a DECIMAL column's scale gridline
(logical_type.rescale_decimal_literal). ±Infinity escaped that as a raw
decimal.InvalidOperation, and NaN (`-0.0 * Infinity` folds to it) as
"ValueError: cannot convert NaN to integer". Neither has a gridline point, so the
rescale now declines and the mixed numeric compare runs under the ruled float order
(NaN == NaN, NaN greatest). Found by the reference fuzzer; counts checked against DuckDB.
"""

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import pytest

import opteryx

DEC = "CAST(id AS DECIMAL(12,3))"


@pytest.mark.parametrize(
    "predicate,expected",
    [
        (f"CAST('-Infinity' AS FLOAT64) = {DEC}", 0),
        (f"{DEC} < CAST('Infinity' AS FLOAT64)", 9),
        (f"{DEC} > CAST('-Infinity' AS FLOAT64)", 9),
        (f"{DEC} <= CAST('-0.0' AS FLOAT64) * CAST('Infinity' AS FLOAT64)", 9),
        (f"{DEC} < CAST('NaN' AS FLOAT64)", 9),
        (f"{DEC} = CAST('NaN' AS FLOAT64)", 0),
        (f"{DEC} >= CAST('NaN' AS FLOAT64)", 0),
    ],
)
def test_decimal_against_non_finite_constant(predicate, expected):
    morsels = opteryx.session().execute_to_morsels(f"SELECT name FROM $planets WHERE {predicate}")
    assert sum(len(m) for m in morsels) == expected


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
