"""
A semi/anti join over a CROSS-TYPE key must not emit its internal key cast.

The join hashes a CAST of the narrower key (INTEGER `planetId` -> FLOAT64) appended
to the probe stream after the leg's real columns. A semi/anti join emits the probe
rows themselves, and when every probe column was live it emitted the whole stream —
CAST included — while declaring only the real columns. The next operator's computed
column landed on the CAST's index and read it:

    SELECT r0.year, (r0.gender > 'syny') AS f
    FROM testdata.astronauts AS r0 LEFT ANTI JOIN testdata.satellites AS r1
    ON r0.year = r1.radius                                   -- f = 2004.0

Found by the join fuzzer's NoREC oracle (a BOOLEAN predicate projected as 1.0).
"""

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

import pytest

import opteryx

FLOAT_IDS = "(SELECT CAST(id AS FLOAT64) AS fid FROM testdata.planets WHERE id < 6) AS r1"


def _rows(sql):
    out = []
    for morsel in opteryx.session().execute_to_morsels(sql):
        out.extend(morsel[i] for i in range(len(morsel)))
    return out


@pytest.mark.parametrize(
    "join,expected_planets",
    [("LEFT SEMI JOIN", {3, 4, 5}), ("LEFT ANTI JOIN", {6, 7, 8, 9})],
)
def test_projection_over_a_cross_type_semi_anti_join(join, expected_planets):
    # Every probe column is projected, so nothing is dead and the join emits the
    # whole probe stream — the shape that leaked.
    sql = (
        "SELECT r0.planetId, r0.radius, (r0.radius > 100) AS f "
        f"FROM testdata.satellites AS r0 {join} {FLOAT_IDS} ON r0.planetId = r1.fid"
    )
    rows = _rows(sql)
    assert {planet for planet, _radius, _f in rows} == expected_planets
    for planet, radius, flag in rows:
        assert flag is (radius > 100), (planet, radius, flag)


def test_the_reported_anti_join():
    rows = _rows(
        "SELECT r0.year, (r0.gender > 'syny') AS f FROM testdata.astronauts AS r0 "
        "LEFT ANTI JOIN testdata.satellites AS r1 ON r0.year = r1.radius"
    )
    assert len(rows) == 357
    for year, flag in rows:
        assert year is None or type(year) is int, year
        assert flag is None or type(flag) is bool, flag


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
