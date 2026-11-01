"""
A parenthesised join key gets the same INT/FLOAT coercion as a bare one.

Mixed numeric join keys are coerced by the compiler (ruled 2026-08-03), and the binder
exempts them from its same-type check — but only when both operands were bare
identifiers. `ON (p.id) = (q.escape_velocity)` wraps each in a NESTED node, so it was
refused ("cannot be compared, because their types do not match") while the
unparenthesised form joined. Found by the reference fuzzer, which parenthesises
every operand; answers checked against DuckDB.
"""

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import pytest

import opteryx


def _rows(sql):
    return sorted(tuple(m[i]) for m in opteryx.session().execute_to_morsels(sql) for i in range(len(m)))


@pytest.mark.parametrize(
    "condition,expected",
    [
        ("(p.id) = (q.escape_velocity)", [("Jupiter", "Mars")]),
        ("(p.id) = q.escape_velocity", [("Jupiter", "Mars")]),
        ("((p.id)) = (q.gravity)", [("Pluto", "Saturn")]),
        ("(p.gravity) = q.id", [("Saturn", "Pluto")]),
    ],
)
def test_parenthesised_mixed_numeric_key_joins(condition, expected):
    sql = f"SELECT p.name, q.name FROM testdata.planets AS p INNER JOIN testdata.planets AS q ON {condition}"
    assert _rows(sql) == expected


@pytest.mark.parametrize("join,expected", [("LEFT OUTER JOIN", 9), ("FULL OUTER JOIN", 17), ("LEFT ANTI JOIN", 8)])
def test_parenthesised_mixed_numeric_key_other_joins(join, expected):
    sql = f"SELECT COUNT(*) FROM testdata.planets AS p {join} testdata.planets AS q ON (p.id) = (q.escape_velocity)"
    assert _rows(sql) == [(expected,)]


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
