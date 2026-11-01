"""
`WHERE key = <literal> GROUP BY key, other` with a literal of a different type answers.

FilterImpliedGroupKeyReduction strips a group key a filter pins to a literal and
re-emits the literal as the key. It typed the literal as the key but kept the
literal's own Python value, so `dec_key = 3` raised a raw TypeError ("Literal #N of
type DECIMAL(12, 3) holds a int (3)"). A key is now reduced only when the literal
already has the key's type. Found by the reference fuzzer; answers checked against DuckDB.
"""

import decimal
import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import pytest

import opteryx


def _rows(sql):
    return sorted(tuple(m[i]) for m in opteryx.session().execute_to_morsels(sql) for i in range(len(m)))


@pytest.mark.parametrize(
    "key,literal,expected",
    [
        ("CAST(id AS DECIMAL(12,3))", "3", decimal.Decimal("3.000")),
        ("CAST(id AS DECIMAL(12,3))", "3.0", decimal.Decimal("3.000")),
        ("CAST(id AS FLOAT64)", "3", 3.0),
        ("id", "CAST(3 AS DECIMAL(5,1))", 3),
        ("id", "3", 3),
    ],
)
def test_group_key_pinned_by_literal_of_another_type(key, literal, expected):
    sql = (
        f"SELECT k, n, COUNT(*) FROM (SELECT {key} AS k, name AS n FROM $planets) AS s "
        f"WHERE k = {literal} GROUP BY k, n"
    )
    assert _rows(sql) == [(expected, "Earth", 1)]


def test_empty_input_still_empty():
    sql = (
        "SELECT k, n FROM (SELECT CAST(id AS DECIMAL(12,3)) AS k, name AS n FROM $planets "
        "WHERE id > 100) AS s WHERE k = 3 GROUP BY k, n"
    )
    assert _rows(sql) == []


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
