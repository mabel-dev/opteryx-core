"""
`x * 0` folds to IFNOTNULL(x, 0) only when that is exact and keeps the product's type.

Constant folding rewrote every `col * 0` to IFNOTNULL(col, 0):
  * DECIMAL(12,3) * 0 is DECIMAL(24,6) (the int128 tier), but IFNOTNULL kept the
    column's int64 tier under that label, so comparing it with another DECIMAL product
    died with "BC_COMPARE fast path declined" (found by the reference fuzzer);
  * FLOAT64: Infinity * 0 and NaN * 0 are NaN, and the fold answered 0.0.
Values checked against DuckDB.
"""

import decimal
import math
import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import pytest

import opteryx

DEC = "(SELECT CAST(id AS DECIMAL(12,3)) AS d FROM $planets) AS s"


def _rows(sql):
    return [tuple(m[i]) for m in opteryx.session().execute_to_morsels(sql) for i in range(len(m))]


@pytest.mark.parametrize(
    "predicate,expected",
    [
        ("(d * CAST('0' AS DECIMAL(12,3))) <= (CAST('0.001' AS DECIMAL(12,3)) * CAST('999999999.999' AS DECIMAL(12,3)))", 9),
        ("(d * CAST('0' AS DECIMAL(12,3))) <= (CAST('0.001' AS DECIMAL(12,3)) * CAST('1.5' AS DECIMAL(12,3)))", 9),
        ("(d * CAST('2' AS DECIMAL(12,3))) > (CAST('0.001' AS DECIMAL(12,3)) * CAST('9000' AS DECIMAL(12,3)))", 5),
    ],
)
def test_decimal_zero_product_compares(predicate, expected):
    assert _rows(f"SELECT COUNT(*) FROM {DEC} WHERE {predicate}") == [(expected,)]


def test_decimal_zero_product_keeps_its_scale():
    assert _rows(f"SELECT SUM(d * CAST('0' AS DECIMAL(12,3))) FROM {DEC}") == [(decimal.Decimal("0.000000"),)]


def test_float_zero_product_is_ieee():
    sql = "SELECT f * 0, i * 0 FROM (SELECT CAST('Infinity' AS FLOAT64) * CAST(id AS FLOAT64) AS f, id AS i FROM $planets) AS s"
    rows = _rows(sql)
    assert len(rows) == 9
    assert all(math.isnan(f) and i == 0 for f, i in rows)


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
