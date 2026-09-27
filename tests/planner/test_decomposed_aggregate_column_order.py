"""
Regression: decomposed aggregates keep their SELECT-list position.

The logical planner rewrites `SUM(x ± int)` to `SUM(x) ± COUNT(x) * int` and
`MIN/MAX(x op literal)` to `MIN/MAX(x) op literal`. It used to drop the original
from the projection and APPEND the rewrite, so the result's columns came back in
a different order from the SELECT list: `SELECT SUM(id + 1), SUM(id * 2)` gave
`SUM(id * 2), SUM(id + 1)`. Values were right, positions were not — any client
reading columns by position (DB-API cursors, CSV, BI tools) got the wrong number
under each heading.

Each case asserts both the column names and the row values, in order.
"""

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import pytest

import opteryx


def run(sql):
    sess = opteryx.session()
    names = None
    rows = []
    for morsel in sess.execute_to_morsels(sql):
        morsel.materialize()
        names = [n.decode() for n in morsel.column_names]
        columns = [morsel.column(name).to_pylist() for name in morsel.column_names]
        rows.extend(zip(*columns))
    sess.close()
    return names, rows


# $planets.id is 1..9: SUM(id)=45, COUNT=9, MIN=1, MAX=9
CASES = [
    (
        "SELECT SUM(id + 1), SUM(id * 2) FROM $planets",
        ["SUM(id + 1)", "SUM(id * 2)"],
        [(54, 90)],
    ),
    (
        "SELECT SUM(id + 1) AS a, SUM(id * 2) AS b FROM $planets",
        ["a", "b"],
        [(54, 90)],
    ),
    (
        "SELECT SUM(id * 2) AS b, SUM(id + 1) AS a FROM $planets",
        ["b", "a"],
        [(90, 54)],
    ),
    (
        "SELECT MIN(id + 1), MAX(id), SUM(id) FROM $planets",
        ["MIN(id + 1)", "MAX(id)", "SUM(id)"],
        [(2, 9, 45)],
    ),
    (
        "SELECT MAX(id + 1) AS m, SUM(id + 2) AS s, MIN(id * 2) AS n, SUM(id - 1) AS t FROM $planets",
        ["m", "s", "n", "t"],
        [(10, 63, 2, 36)],
    ),
    # Nested inside a larger expression there is no SELECT slot to substitute
    # into, so the aggregate is computed as written. The old path appended a
    # stray column and died at compile time with InvalidInternalStateError.
    (
        "SELECT SUM(id + 1) * 2 AS d, COUNT(*) AS c FROM $planets",
        ["d", "c"],
        [(108, 9)],
    ),
    (
        "SELECT MAX(id + 1) - 1 AS m, SUM(id + 1) AS s, MIN(id * 2) + SUM(id - 1) AS x FROM $planets",
        ["m", "s", "x"],
        [(9, 54, 38)],
    ),
]

GROUPED_CASES = [
    (
        "SELECT COUNT(*) AS c, SUM(id + 1) AS s, name FROM $planets GROUP BY name ORDER BY name",
        ["c", "s", "name"],
    ),
    (
        "SELECT SUM(id - 1) AS z, name, COUNT(*) AS y FROM $planets GROUP BY name ORDER BY name",
        ["z", "name", "y"],
    ),
    (
        "SELECT name, MAX(id + 1) AS m, SUM(id) AS s FROM $planets GROUP BY name ORDER BY name",
        ["name", "m", "s"],
    ),
]


@pytest.mark.parametrize("sql, expected_names, expected_rows", CASES)
def test_ungrouped_decomposed_aggregate_column_order(sql, expected_names, expected_rows):
    names, rows = run(sql)
    assert names == expected_names, f"{sql}\n  got {names}"
    assert rows == expected_rows, f"{sql}\n  got {rows}"


@pytest.mark.parametrize("sql, expected_names", GROUPED_CASES)
def test_grouped_decomposed_aggregate_column_order(sql, expected_names):
    names, rows = run(sql)
    assert names == expected_names, f"{sql}\n  got {names}"
    # the values must follow the headings: each planet is its own group
    by_name = {r[names.index("name")]: dict(zip(names, r)) for r in rows}
    planets = dict(run("SELECT name, id FROM $planets")[1])
    assert len(by_name) == len(planets)
    for planet, pid in planets.items():
        row = by_name[planet]
        if "s" in row:
            assert row["s"] == (pid + 1 if "SUM(id + 1)" in sql else pid)
        if "m" in row:
            assert row["m"] == pid + 1
        if "z" in row:
            assert row["z"] == pid - 1
        for count_col in ("c", "y"):
            if count_col in row:
                assert row[count_col] == 1


if __name__ == "__main__":  # pragma: no cover
    pytest.main([__file__, "-v"])
