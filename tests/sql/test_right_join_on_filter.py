"""
An ON-clause condition on the null-supplying (LEFT) leg of a RIGHT JOIN limits which
left rows can match; it never removes right rows. PredicatePushdown handled that for
LEFT JOIN (the right leg's pre-filter) but sent every RIGHT JOIN predicate above the
join, where it applied as a WHERE: `ON p.id = s.planetId AND p.id = 4` answered 2
rows instead of 177. Expected values from DuckDB 2.0.0 over the same parquet files.
"""

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import pytest

import opteryx


def _summary(sql):
    rows = [tuple(m[i]) for m in opteryx.session().execute_to_morsels(sql) for i in range(len(m))]
    return len(rows), sum(1 for p, _ in rows if p is None), sum(p or 0 for p, _ in rows)


# (ON condition, WHERE, (rows, rows without a planet, sum of planet ids))
CASES = [
    ("p.id = 4", "", (177, 175, 8)),
    ("p.id > 5", "", (177, 70, 712)),
    ("p.id != 3", "", (177, 1, 1055)),
    ("p.id IN (3, 4, NULL)", "", (177, 174, 11)),
    ("p.name LIKE 'M%'", "", (177, 175, 8)),
    ("(p.id = 3 OR p.id = 5)", "", (177, 109, 338)),
    ("p.id BETWEEN 3 AND 6", "", (177, 46, 712)),
    ("p.id = 4", "p.id IS NULL", (175, 175, 0)),
    ("p.id > 5", "s.id < 10", (9, 9, 0)),
    ("p.id NOT IN (1, 2)", "p.id > 4 OR s.id < 3", (176, 0, 1054)),
]


@pytest.mark.parametrize("on,where,expected", CASES)
def test_right_join_on_filter_keeps_right_rows(on, where, expected):
    sql = (
        "SELECT p.id, s.id FROM testdata.planets AS p RIGHT JOIN testdata.satellites AS s "
        f"ON p.id = s.planetId AND {on}" + (f" WHERE {where}" if where else "")
    )
    assert _summary(sql) == expected


@pytest.mark.parametrize("on,where,expected", CASES)
def test_same_as_the_left_join_spelling(on, where, expected):
    sql = (
        "SELECT p.id, s.id FROM testdata.satellites AS s LEFT JOIN testdata.planets AS p "
        f"ON p.id = s.planetId AND {on}" + (f" WHERE {where}" if where else "")
    )
    assert _summary(sql) == expected


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
