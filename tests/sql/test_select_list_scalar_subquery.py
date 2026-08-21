"""Regression: uncorrelated scalar subqueries used as a SELECT-list value.

TPC-DS Q09 (tests/performance/tpcds/opteryx/queries/query09.sql) failed with:

    UnsupportedSyntaxError: Scalar subqueries are supported in the WHERE clause
    but not yet in the SELECT list.

Q09's SELECT list is a chain of CASE expressions, each branch its own scalar
subquery, e.g.:

    SELECT CASE
        WHEN (SELECT count(*) FROM store_sales WHERE ...) > 74129
        THEN (SELECT avg(ss_ext_discount_amt) FROM store_sales WHERE ...)
        ELSE (SELECT avg(ss_net_paid) FROM store_sales WHERE ...)
    END bucket1, ...

Every subquery here is UNCORRELATED and a plain ungrouped aggregate — exactly
the single-row-safe shape `decorrelate_subquery.py`'s WHERE-clause rewrite
(`_decorrelate`) already trusts via `_uncorrelated_single_row_proof`. The
restriction was placement, not correlation or cardinality: logical_planner.py
refused any SUBQUERY node found anywhere in the projection, pre-bind, because
DecorrelateSubqueryStrategy only ever inspected Filter conditions — an
unhandled SUBQUERY node in a Project's columns would have bound successfully
(bind_correlated_subquery is generic) and then failed deep in the planner with
an opaque internal error once compilation reached a node type it does not
know.

Fixed by:
  - narrowing the pre-bind guard (logical_planner.py) to only refuse EXISTS/IN
    in the SELECT list — a genuinely different rewrite (a zero-key semi/anti
    join the join compiler does not admit) — and letting a bare SCALAR
    subquery through to binding, where correlation can actually be told apart
    from an uncorrelated one;
  - extending `_find` (decorrelate_subquery.py) to walk a CASE node's
    conditions/results/else_result, which it did not before (only
    left/right/centre/parameters) — needed for Q09's WHEN/THEN/ELSE shape;
  - adding `_decorrelate_projection`, the Project-node sibling of `_decorrelate`:
    same uncorrelated cross-join rewrite, applied to a SELECT-list expression
    instead of a Filter condition. No "narrow back" Project is needed
    afterward — Project already narrows to its own declared columns, unlike
    Filter, which passes everything through;
  - a genuine binder gap this surfaced: a bare top-level `SELECT (subquery)`
    left its schema_column pointing at the identity the subquery's OWN inner
    scope minted (deliberately not merged outward — see
    bind_correlated_subquery's docstring), which project.py's binder assumed
    every top-level column could resolve against `context.schemas` — a
    `SELECT (SELECT ...) AS x` therefore KeyErrored during binding rather than
    the intended `UnsupportedSyntaxError`.

Correlated SELECT-list subqueries remain refused: an outer row with no match
must survive carrying NULL (a LEFT OUTER join), where a WHERE-clause
subquery's INNER join can validly drop the row instead. That is a different
rewrite, out of scope here, and not needed by Q09.
"""

import os
import sys

sys.path.insert(1, os.path.join(os.path.dirname(__file__), "..", ".."))

import pytest

import opteryx
from opteryx.exceptions import DataError
from opteryx.exceptions import UnsupportedSyntaxError

_SESSION = opteryx.session()


def _rows(sql, colnames):
    out = []
    for morsel in _SESSION.execute_to_morsels(sql):
        cols = [morsel.column(c).to_pylist() for c in colnames]
        out.extend(zip(*cols))
    return out


def test_bare_scalar_subquery_in_select_list():
    # $planets' number_of_moons sums to 210 across 9 rows.
    rows = _rows(
        "SELECT id, (SELECT AVG(number_of_moons) FROM $planets) AS avg_moons "
        "FROM $planets WHERE id IN (1, 9) ORDER BY id",
        ["id", "avg_moons"],
    )
    assert rows == pytest.approx([(1, 210 / 9), (9, 210 / 9)])


def test_case_with_scalar_subquery_branches_matches_tpcds_q09_shape():
    # 7 of 9 $planets have number_of_moons > 0, summing to 210: avg == 30.
    # All 9 have number_of_moons >= 0, so COUNT(*) > 8 is true (THEN fires).
    rows = _rows(
        "SELECT CASE "
        "WHEN (SELECT COUNT(*) FROM $planets WHERE number_of_moons >= 0) > 8 "
        "THEN (SELECT AVG(number_of_moons) FROM $planets WHERE number_of_moons > 0) "
        "ELSE (SELECT AVG(mass) FROM $planets) "
        "END AS bucket FROM $planets WHERE id = 1",
        ["bucket"],
    )
    assert rows == pytest.approx([(30.0,)])

    # Same CASE, WHEN threshold raised past the subquery's actual count (9), so
    # the ELSE branch's subquery fires instead of the THEN branch's.
    avg_mass = sum([0.33, 4.87, 5.97, 0.642, 1898.0, 568.0, 86.8, 102.0, 0.0146]) / 9
    rows = _rows(
        "SELECT CASE "
        "WHEN (SELECT COUNT(*) FROM $planets WHERE number_of_moons >= 0) > 100 "
        "THEN (SELECT AVG(number_of_moons) FROM $planets WHERE number_of_moons > 0) "
        "ELSE (SELECT AVG(mass) FROM $planets) "
        "END AS bucket FROM $planets WHERE id = 1",
        ["bucket"],
    )
    assert rows == pytest.approx([(avg_mass,)])


def test_select_list_scalar_subquery_still_refuses_correlation():
    with pytest.raises(UnsupportedSyntaxError, match="must be uncorrelated"):
        _rows(
            "SELECT (SELECT AVG(number_of_moons) FROM $planets p2 WHERE p2.id = p1.id) "
            "AS x FROM $planets p1",
            ["x"],
        )


def test_select_list_scalar_subquery_enforces_cardinality_at_runtime():
    # Statically unprovable single-row shapes now RUN behind a runtime
    # ScalarSubqueryGuard rather than refusing at plan time; a subquery that
    # genuinely returns several rows raises SQL's cardinality violation.
    with pytest.raises(DataError, match="more than one row returned by a subquery"):
        _rows(
            "SELECT (SELECT number_of_moons FROM $planets) AS x FROM $planets WHERE id = 1",
            ["x"],
        )

    # A data-fact single row is admitted and yields the value ...
    rows = _rows(
        "SELECT (SELECT number_of_moons FROM $planets WHERE name = 'Saturn') AS x "
        "FROM $planets WHERE id = 1",
        ["x"],
    )
    assert rows == [(82,)]

    # ... and a zero-row subquery is NULL per outer row, never an emptied result.
    rows = _rows(
        "SELECT (SELECT number_of_moons FROM $planets WHERE name = 'Krypton' LIMIT 1) AS x "
        "FROM $planets WHERE id = 1",
        ["x"],
    )
    assert rows == [(None,)]


def test_select_list_still_refuses_exists_and_in_subqueries():
    with pytest.raises(UnsupportedSyntaxError, match="EXISTS"):
        _rows(
            "SELECT EXISTS(SELECT 1 FROM $planets) AS x FROM $planets WHERE id = 1",
            ["x"],
        )
    with pytest.raises(UnsupportedSyntaxError, match="EXISTS"):
        _rows(
            "SELECT id IN (SELECT id FROM $planets) AS x FROM $planets WHERE id = 1",
            ["x"],
        )
