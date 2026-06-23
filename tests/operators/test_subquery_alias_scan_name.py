"""
Regression tests for subqueries that alias a scan column and then filter on it.

Before the fix, `visit_subquery` (opteryx/planner/binder/subquery.py) renamed the
column's `SchemaColumn` *in place* to the user-facing alias. That object is shared
with the underlying scan, so the scan column lost its physical name (e.g. `id`).
The reader's `normalize_morsel` maps the connector's physically-named data back to
each schema column by (identity, name); with the physical name gone it found
neither and substituted a NULL placeholder of the schema's default width (INT64).

That produced two visible failures whenever the subquery emitted more than one
column AND was filtered (so it wasn't collapsed by redundant-operator removal):

  * a fused CAST projection over the (now INT64) placeholder hit a kernel wired
    for the original narrow int → `ValueError: C kernel error`;
  * a plain filter over the all-NULL placeholder dropped every row.

The fix gives the subquery's OUTPUT column its own object (renamed to the alias)
and leaves the scan column's physical name intact.

Ground-truth values: $planets.id is 1..9.
"""

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import pytest

import opteryx


def _first_col(sql):
    sess = opteryx.session()
    out = []
    for m in sess.execute_to_morsels(sql):
        out += m.column(m.column_names[0]).to_pylist()
    return sorted(out)


# (sql, expected sorted values of the single output column)
CASES = [
    # CAST result filtered through a multi-column aliasing subquery (was: C kernel error)
    (
        "SELECT k FROM (SELECT CAST(id AS INTEGER) AS c, id AS k FROM $planets) t WHERE c > 5",
        [6, 7, 8, 9],
    ),
    # column-vs-column predicate, no literal involved (was: C kernel error)
    (
        "SELECT k FROM (SELECT CAST(id AS INTEGER) AS c, id AS k FROM $planets) t WHERE c > k",
        [],
    ),
    # DOUBLE cast variant (was: C kernel error)
    (
        "SELECT k FROM (SELECT CAST(id AS DOUBLE) AS c, id AS k FROM $planets) t WHERE c > -1",
        [1, 2, 3, 4, 5, 6, 7, 8, 9],
    ),
    # plain filter on the un-cast aliased column, multi-column subquery (was: 0 rows)
    (
        "SELECT k FROM (SELECT id AS k, name FROM $planets) t WHERE k > 5",
        [6, 7, 8, 9],
    ),
    # filter on the cast column itself (was: C kernel error)
    (
        "SELECT k FROM (SELECT CAST(id AS INTEGER) AS c, id AS k FROM $planets) t WHERE k > 5",
        [6, 7, 8, 9],
    ),
    # same underlying column aliased twice, both resolvable from the outer query
    (
        "SELECT x FROM (SELECT id AS x, id AS y, name FROM $planets) t WHERE x > 7",
        [8, 9],
    ),
    # nested two-level rename + filter
    (
        "SELECT kk FROM (SELECT k AS kk FROM (SELECT id AS k, name FROM $planets) a) b WHERE kk > 6",
        [7, 8, 9],
    ),
    # single-column subquery (collapsed by redundant-operator removal) still works
    (
        "SELECT k FROM (SELECT id AS k FROM $planets) t WHERE k > 5",
        [6, 7, 8, 9],
    ),
]


@pytest.mark.parametrize("sql,expected", CASES)
def test_subquery_alias_scan_name(sql, expected):
    assert _first_col(sql) == expected, sql


def test_aliasing_does_not_leak_physical_name_to_outer_query():
    """The pre-alias physical name must NOT be resolvable in the outer query."""
    from opteryx.exceptions import ColumnNotFoundError

    sess = opteryx.session()
    with pytest.raises(ColumnNotFoundError):
        list(sess.execute_to_morsels("SELECT id FROM (SELECT id AS k, name FROM $planets) t"))


if __name__ == "__main__":  # pragma: no cover
    for _sql, _exp in CASES:
        got = _first_col(_sql)
        status = "✅" if got == _exp else "❌"
        print(status, _sql, "->", got)
    test_aliasing_does_not_leak_physical_name_to_outer_query()
    print("✅ outer query cannot resolve pre-alias physical name")
