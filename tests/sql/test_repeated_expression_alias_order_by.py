"""ORDER BY the second alias of an expression written twice in the SELECT list.

    SELECT id + 1 AS a, id + 1 AS b FROM $planets ORDER BY b

raised `ColumnNotFoundError: Column b cannot be found`. The first `id + 1` mints a
schema column carrying alias `a`; the second is textually identical, so inner_binder
REUSES that column — and the reuse path was the one bind path that did not publish
the node's alias onto the column (the identifier path and the mint path both do).
visit_project re-publishes only the first node's alias per identity, so `b` never
reached the schema the Order step binds against. Found by the single-table fuzzer
(seed 8866681452490952040, `Column oz_e2 cannot be found`).
"""

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import opteryx


def result(sql):
    """Column names and every row of a result, as tuples in column order."""
    session = opteryx.session()
    names = None
    collected = []
    for morsel in session.execute_to_morsels(sql):
        if morsel is None:
            continue
        table = morsel.to_arrow()
        names = table.column_names
        collected.extend(zip(*table.to_pydict().values()))
    return names, collected


def test_order_by_second_alias_of_repeated_expression():
    names, rows = result("SELECT id + 1 AS a, id + 1 AS b FROM $planets ORDER BY b DESC")
    assert names == ["a", "b"], names
    assert rows == [(i, i) for i in range(10, 1, -1)], rows


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
