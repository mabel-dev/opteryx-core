"""
A NaN join key matches a NaN key in every join type (NaN == NaN, ruled 2026-05-22).

testdata.satellites has 6 NaN albedo and 6 NaN magnitude values and no other match
between the two columns. SEMI/ANTI/OUTER joins already answered under the ruling, but
INNER answered 0 instead of 36: the correlated-filter strategy pushed
`r1.magnitude <= max(r0.albedo)` from statistics, and float statistics omit NaN,
which sorts above every value — so the max is no upper bound (the same rule as the
scan's _nan_invisible_to_bounds). `CROSS JOIN ... WHERE` becomes the same INNER join.
"""

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import pytest

import opteryx

ON = "r0.albedo = r1.magnitude"


def _count(sql):
    return [m[i] for m in opteryx.session().execute_to_morsels(sql) for i in range(len(m))][0][0]


@pytest.mark.parametrize(
    "join,expected",
    [
        ("INNER JOIN", 36),
        ("LEFT SEMI JOIN", 6),
        ("LEFT ANTI JOIN", 171),
        ("LEFT JOIN", 207),
        ("RIGHT JOIN", 207),
        ("FULL JOIN", 378),
    ],
)
def test_nan_keys_match_in_every_join(join, expected):
    sql = f"SELECT COUNT(*) FROM testdata.satellites AS r0 {join} testdata.satellites AS r1 ON {ON}"
    assert _count(sql) == expected


def test_cross_join_where_agrees_with_inner():
    sql = f"SELECT COUNT(*) FROM testdata.satellites AS r0 CROSS JOIN testdata.satellites AS r1 WHERE {ON}"
    assert _count(sql) == 36


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
