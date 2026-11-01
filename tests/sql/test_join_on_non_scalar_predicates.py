"""
IN, NOT IN, LIKE, ILIKE and IS tests in a JOIN's ON clause were refused outright
("Only JOINs with equals comparisons supported") — even on one relation, where the
optimizer pushes them to that leg exactly as it does `s.id > 3`.

Lifting the refusal exposed the reason for it: JoinConditionHoistStrategy kept every
TWO-sided comparison in ON as a per-pair residual, but the join evaluates only `=`
keys and `!= < > <= >=` residuals, so `p.id + s.id IN (5, 6, 7)` was dropped (177
rows, not 2). Those are now hoisted to a WHERE above the join like any other
condition. Expected values from DuckDB 2.0.0 over the same parquet files.

Outer joins keep their existing rules: a filter on the preserved side, or a
two-sided non-equality, is refused by name.
"""

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import pytest

import opteryx
from opteryx.exceptions import UnsupportedSyntaxError


def _summary(sql):
    rows = [tuple(m[i]) for m in opteryx.session().execute_to_morsels(sql) for i in range(len(m))]
    return len(rows), sum(1 for _, s in rows if s is None), sum(s or 0 for _, s in rows)


# (join, condition, (rows, rows without a satellite, sum of satellite ids))
CASES = [
    ("INNER", "s.id IN (1, 2, 3, 50, 100)", (5, 0, 156)),
    ("INNER", "s.id NOT IN (1, 2, 3)", (174, 0, 15747)),
    ("INNER", "s.id IN (1, 2, NULL)", (2, 0, 3)),
    ("INNER", "s.id NOT IN (1, 2, NULL)", (0, 0, 0)),
    ("INNER", "s.name LIKE 'P%'", (17, 0, 1629)),
    ("INNER", "s.name ILIKE 'p%'", (17, 0, 1629)),
    ("INNER", "p.id IN (3, 4)", (3, 0, 6)),
    ("INNER", "p.name LIKE 'M%'", (2, 0, 5)),
    ("INNER", "p.id + s.id IN (5, 6, 7)", (2, 0, 5)),
    ("INNER", "p.id + s.id NOT IN (5, 6, 7)", (175, 0, 15748)),
    ("INNER", "p.name || s.name LIKE '%Moon%'", (1, 0, 1)),
    ("INNER", "s.gm IS NULL", (0, 0, 0)),
    ("LEFT", "s.id IN (1, 2, 3, 50, 100)", (10, 5, 156)),
    ("LEFT", "s.id NOT IN (1, 2, NULL)", (9, 9, 0)),
    ("LEFT", "s.name LIKE 'P%'", (21, 4, 1629)),
    ("LEFT", "s.name IN ('Moon', 'Phobos')", (9, 7, 3)),
]


@pytest.mark.parametrize("join,condition,expected", CASES)
def test_non_scalar_predicate_in_on(join, condition, expected):
    sql = (
        f"SELECT p.id, s.id FROM testdata.planets AS p {join} JOIN testdata.satellites AS s "
        f"ON p.id = s.planetId AND {condition}"
    )
    assert _summary(sql) == expected


@pytest.mark.parametrize(
    "join,condition,message",
    [
        ("LEFT", "p.id IN (3, 4)", "filters only the preserved side"),
        ("LEFT", "p.id + s.id IN (5, 6, 7)", "is not an equality between the two relations"),
        ("FULL", "s.name LIKE 'P%'", "filters only the preserved side"),
    ],
)
def test_outer_join_rules_still_refuse(join, condition, message):
    sql = (
        f"SELECT p.id, s.id FROM testdata.planets AS p {join} JOIN testdata.satellites AS s "
        f"ON p.id = s.planetId AND {condition}"
    )
    with pytest.raises(UnsupportedSyntaxError, match=message):
        _summary(sql)


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
