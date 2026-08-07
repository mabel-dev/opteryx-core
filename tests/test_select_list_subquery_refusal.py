"""
Regression coverage for scalar subqueries in the SELECT list.

DecorrelateSubqueryStrategy owns every subquery form that appears in a
PREDICATE; a SUBQUERY expression node in the PROJECTION is handled nowhere.
It used to survive binding unresolved and crash deep in the binder
(`AttributeError: 'NoneType' object has no attribute 'identity'` in
visit_project) — a raw internal error instead of a refusal. The logical
planner now raises UnsupportedSyntaxError at the first walk of the
projection, per the fail-fast contract: raise, never silently wrong.

Full support is follow-on scope: a SELECT-list scalar subquery decorrelates
to a LEFT OUTER join (outer rows without a match survive with NULL), not the
INNER join the WHERE-clause rewrite builds.
"""

import os
import sys

sys.path.insert(0, os.path.join(sys.path[0], ".."))

import opteryx
from opteryx.connectors import DiskConnector
from opteryx.exceptions import UnsupportedSyntaxError

opteryx.register_workspace("testdata", DiskConnector)

T = "testdata.tpch_001"


def _rows(sql):
    session = opteryx.session()
    return sum(morsel.num_rows for morsel in session.execute_to_morsels(sql))


def _raises_unsupported(sql) -> bool:
    try:
        _rows(sql)
    except UnsupportedSyntaxError:
        return True
    return False


def test_select_list_scalar_subquery_refused():
    """The ORDER BY ... LIMIT 1 form that originally crashed the binder."""
    assert _raises_unsupported(f"""
        SELECT c_custkey,
               (SELECT o_totalprice FROM {T}.orders
                WHERE o_custkey = c_custkey
                ORDER BY o_orderdate DESC LIMIT 1) AS latest
        FROM {T}.customer LIMIT 5
    """)


def test_select_list_aggregate_subquery_refused():
    """Even the aggregate form the WHERE rewrite handles must refuse here."""
    assert _raises_unsupported(f"""
        SELECT c_custkey,
               (SELECT MAX(o_totalprice) FROM {T}.orders
                WHERE o_custkey = c_custkey) AS biggest
        FROM {T}.customer LIMIT 5
    """)


def test_select_list_nested_subquery_refused():
    """A subquery buried inside a projection expression, not a bare column."""
    assert _raises_unsupported(f"""
        SELECT c_custkey,
               1 + (SELECT MAX(o_totalprice) FROM {T}.orders
                    WHERE o_custkey = c_custkey) AS padded
        FROM {T}.customer LIMIT 5
    """)


def test_where_clause_scalar_subquery_still_runs():
    """The refusal must not reach the supported WHERE-clause form."""
    assert (
        _rows(f"""
        SELECT c_custkey FROM {T}.customer
        WHERE c_acctbal > (SELECT AVG(c_acctbal) FROM {T}.customer)
        LIMIT 5
    """)
        == 5
    )


if __name__ == "__main__":
    for name, case in sorted(globals().items()):
        if name.startswith("test_") and callable(case):
            case()
            print(f"{name}: OK")
    print("\nall passed")
