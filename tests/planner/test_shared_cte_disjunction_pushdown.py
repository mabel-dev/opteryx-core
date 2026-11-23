"""
A shared CTE read by references that filter DIFFERENTLY gets the OR of their
filters pushed into its body (TPC-DS Q39: `d_moy = 1` on one reference, `d_moy = 2`
on the other). Each reference keeps its own filter, so the answer is unchanged;
a reference with no filter reads every row, so nothing is pushed.
"""

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import opteryx

CTE = "WITH g AS (SELECT planetId, COUNT(*) AS c FROM testdata.satellites GROUP BY planetId) "


def _run(sql):
    session = opteryx.session()
    rows = []
    for morsel in session.execute_to_morsels(sql):
        if morsel is None or morsel.num_rows == 0:
            continue
        columns = [morsel.column(name).to_pylist() for name in morsel.column_names]
        rows.extend(zip(*columns))
    return rows, session.telemetry.get("optimization_shared_cte_disjunction_pushed")


def test_different_filters_push_their_disjunction():
    rows, pushed = _run(
        CTE + "SELECT a.c, b.c FROM g AS a, g AS b WHERE a.planetId = 5 AND b.planetId = 6"
    )
    assert rows == [(67, 61)]
    assert pushed == 1


def test_each_branch_keeps_all_its_filters():
    rows, pushed = _run(
        CTE + "SELECT a.planetId, b.planetId FROM g AS a, g AS b "
        "WHERE a.c > 1 AND b.c > 1 AND a.planetId = 4 AND b.planetId = 8"
    )
    assert rows == [(4, 8)]
    assert pushed == 1


def test_unfiltered_reference_blocks_the_push():
    rows, pushed = _run(
        CTE + "SELECT a.c, COUNT(*) FROM g AS a, g AS b WHERE a.planetId = 5 GROUP BY a.c"
    )
    assert rows == [(67, 7)]
    assert not pushed


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
