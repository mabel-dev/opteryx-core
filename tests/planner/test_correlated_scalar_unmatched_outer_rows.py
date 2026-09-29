"""
Repro for the registered wrong-answer defect
`correlated-scalar-subquery-drops-unmatched-outer-rows`
(tests/fuzzing/subquery_known_gaps.py).

A correlated scalar subquery in WHERE is decorrelated to an INNER join against
`(SELECT key, AGG ... GROUP BY key)`, so an outer row with no matching group is
dropped instead of receiving the aggregate's empty-set value (0 for COUNT, NULL
for every other aggregate).

Expected rows are computed in Python from two plain, uncorrelated scans, so the
oracle does not go through the code under test. These tests are EXPECTED TO FAIL
until the defect is fixed.
"""

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import pytest

import opteryx
from opteryx.connectors import DiskConnector

opteryx.register_workspace("testdata", DiskConnector)


def _rows(sql):
    session = opteryx.session()
    out = []
    for morsel in session.execute_to_morsels(sql):
        for i in range(len(morsel)):
            out.append(tuple(morsel[i]))
    return out


def _ids(sql):
    return sorted(r[0] for r in _rows(sql))


PLANETS = [r[0] for r in _rows("SELECT id FROM testdata.planets")]
SAT = _rows("SELECT planetId, radius FROM testdata.satellites")


def _count(pid):
    return sum(1 for p, _ in SAT if p == pid)


def _max_radius(pid):
    vals = [r for p, r in SAT if p == pid and r is not None]
    return max(vals) if vals else None


def _sum_radius(pid):
    vals = [r for p, r in SAT if p == pid and r is not None]
    return sum(vals) if vals else None


def _expected(pred):
    return sorted(pid for pid in PLANETS if pred(pid))


OUTER = "SELECT sq_o.id FROM testdata.planets AS sq_o WHERE "
CORR = "FROM testdata.satellites AS sq_i WHERE sq_i.planetId = sq_o.id"


def test_oracle_has_unmatched_outer_rows():
    assert [p for p in PLANETS if _count(p) == 0], "test data must have planets with no satellites"


@pytest.mark.parametrize(
    "predicate, expected",
    [
        (f"(SELECT COUNT(*) {CORR}) = 0", lambda p: _count(p) == 0),
        (f"(SELECT COUNT(*) {CORR}) < 1", lambda p: _count(p) < 1),
        (f"(SELECT COUNT(*) {CORR}) <> 3", lambda p: _count(p) != 3),
        (f"sq_o.id > (SELECT COUNT(*) {CORR})", lambda p: p > _count(p)),
        (f"(SELECT COUNT(sq_i.radius) {CORR}) = 0", lambda p: len([1 for q, r in SAT if q == p and r is not None]) == 0),
        (f"(SELECT MAX(sq_i.radius) {CORR}) IS NULL", lambda p: _max_radius(p) is None),
        (f"(SELECT SUM(sq_i.radius) {CORR}) IS NULL", lambda p: _sum_radius(p) is None),
        (f"(SELECT MIN(sq_i.radius) {CORR}) IS NULL", lambda p: _max_radius(p) is None),
        (f"(SELECT AVG(sq_i.radius) {CORR}) IS NULL", lambda p: _max_radius(p) is None),
        (f"COALESCE((SELECT MAX(sq_i.radius) {CORR}), -1.0) < 0.0", lambda p: (_max_radius(p) is None)),
        (f"COALESCE((SELECT SUM(sq_i.radius) {CORR}), -1.0) < 0.0", lambda p: (_sum_radius(p) is None)),
        (f"COALESCE((SELECT COUNT(*) {CORR}), 99) = 0", lambda p: _count(p) == 0),
    ],
)
def test_where_scalar_unmatched_semantics(predicate, expected):
    assert _ids(OUTER + predicate) == _expected(expected)


# Control: NULL-rejecting comparisons on non-COUNT aggregates are already correct
# (INNER join drop == SQL unknown drop). These must keep passing after any fix.
@pytest.mark.parametrize(
    "predicate, expected",
    [
        (f"(SELECT MAX(sq_i.radius) {CORR}) > 0", lambda p: (_max_radius(p) or 0) > 0),
        (f"(SELECT SUM(sq_i.radius) {CORR}) > 0", lambda p: (_sum_radius(p) or 0) > 0),
        (f"(SELECT COUNT(*) {CORR}) > 0", lambda p: _count(p) > 0),
    ],
)
def test_where_scalar_null_rejecting_controls(predicate, expected):
    assert _ids(OUTER + predicate) == _expected(expected)


def test_having_scalar_count_zero():
    sql = (
        "SELECT sq_o.id FROM testdata.planets AS sq_o GROUP BY sq_o.id HAVING "
        f"(SELECT COUNT(*) {CORR}) = 0"
    )
    assert _ids(sql) == _expected(lambda p: _count(p) == 0)


if __name__ == "__main__":
    pytest.main([__file__, "-q", "-x", "--no-header", "-p", "no:cacheprovider"])
