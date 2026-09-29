"""
Repro for the registered wrong-answer defect
`correlated-scalar-subquery-drops-unmatched-outer-rows`
(tests/fuzzing/subquery_known_gaps.py).

A correlated scalar subquery in WHERE is decorrelated to an INNER join against
`(SELECT key, AGG ... GROUP BY key)`, so an outer row with no matching group is
dropped instead of receiving the aggregate's empty-set value (0 for COUNT, NULL
for every other aggregate).

Expected rows are computed in Python from two plain, uncorrelated scans, so the
oracle does not go through the code under test. The fix (architect ruling 2026-09-29): the join is LEFT OUTER, with
`CASE WHEN inner_key IS NULL THEN <empty-set value> ELSE value END` substituted where the
empty-set value is not NULL, EXCEPT where the predicate provably rejects the empty-set value
(TPC-H Q2/Q17/Q20 shapes), where the cheaper INNER join is kept. Skip-level correlation
under a predicate that accepts the empty-set value is refused.
"""

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import pytest

import opteryx
from opteryx.connectors import DiskConnector
from opteryx.exceptions import UnsupportedSyntaxError

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


@pytest.mark.parametrize(
    "predicate, expected",
    [
        # The empty-set value is computed THROUGH the subquery's own expression.
        (f"(SELECT COUNT(*) + 1 {CORR}) = 1", lambda p: _count(p) == 0),
        (f"(SELECT 2 * COUNT(*) {CORR}) = 0", lambda p: _count(p) == 0),
        (f"COALESCE((SELECT SUM(sq_i.radius) {CORR}), 0) = 0", lambda p: _sum_radius(p) is None),
        # A HAVING over the empty aggregate row drops it: NULL, not 0.
        (f"(SELECT COUNT(*) {CORR} HAVING COUNT(*) > 100) IS NULL", lambda p: True),
        # The ORDER BY ... LIMIT 1 form has no aggregate: NULL when unmatched.
        (
            f"(SELECT sq_i.radius {CORR} ORDER BY sq_i.radius DESC LIMIT 1) IS NULL",
            lambda p: _max_radius(p) is None,
        ),
        # Connectives: a NULL-rejecting conjunct is not enough under OR.
        (f"(SELECT COUNT(*) {CORR}) = 0 OR sq_o.id = 5", lambda p: _count(p) == 0 or p == 5),
        (f"(SELECT COUNT(*) {CORR}) = 0 AND sq_o.id > 1", lambda p: _count(p) == 0 and p > 1),
        (f"(SELECT MAX(sq_i.radius) {CORR}) > 0 OR sq_o.id = 1", lambda p: (_max_radius(p) or 0) > 0 or p == 1),
    ],
)
def test_where_scalar_expression_and_connective_shapes(predicate, expected):
    assert _ids(OUTER + predicate) == _expected(expected)


def _join_kinds(sql):
    session = opteryx.session()
    lines = []
    for morsel in session.execute_to_morsels("EXPLAIN " + sql):
        for i in range(len(morsel)):
            name = morsel[i][0]
            lines.append(name.decode() if isinstance(name, bytes) else name)
    return " ".join(lines)


def test_null_rejecting_predicate_keeps_the_inner_join():
    plan = _join_kinds(OUTER + f"(SELECT MAX(sq_i.radius) {CORR}) > 0")
    assert "Inner Join" in plan and "left_outer" not in plan


def test_empty_value_accepting_predicate_uses_left_outer_join():
    plan = _join_kinds(OUTER + f"(SELECT COUNT(*) {CORR}) = 0")
    assert "left_outer" in plan


def test_skip_level_correlation_with_empty_value_predicate_is_refused():
    sql = (
        "SELECT p.id FROM testdata.planets AS p WHERE (SELECT COUNT(*) FROM testdata.satellites AS s "
        "WHERE s.planetId = p.id AND (SELECT COUNT(*) FROM testdata.satellites AS s2 "
        "WHERE s2.planetId = p.id AND s2.id = s.id) = 0) = 0"
    )
    with pytest.raises(UnsupportedSyntaxError):
        _rows(sql)


if __name__ == "__main__":
    pytest.main([__file__, "-q", "-x", "--no-header", "-p", "no:cacheprovider"])
