"""
The OR -> IN-list rewrite must not fuse point tests on same-named columns of
different relations.

`rewrite_cnf_eq_to_inlist` grouped CNF branches by the RENDERED operand, which
names a column without its relation, so `p.id = 1` and `s.id IN (2, 3)` both keyed
"id" and became one `id IN (1, 2, 3)` applied to a single side. On a self-join of
$planets, `p.name = 'x' OR p.id = 1 OR s.id IN (2, 3)` answered 27 instead of 25,
with no error. Found by the reference (Opteryx vs DuckDB) fuzzer.
"""

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

import pytest

import opteryx


def _scalar(sql):
    morsels = list(opteryx.session().execute_to_morsels(sql))
    assert len(morsels) == 1 and len(morsels[0]) == 1, sql
    return morsels[0][0][0]


@pytest.mark.parametrize(
    "sql,expected",
    [
        # p.id = 1: 9 rows; s.id IN (2, 3): 18 rows; both: 2 rows.
        ("SELECT COUNT(*) FROM $planets AS p CROSS JOIN $planets AS s WHERE p.name = 'x' OR p.id = 1 OR s.id IN (2, 3)", 25),
        ("SELECT COUNT(*) FROM $planets AS p CROSS JOIN $planets AS s WHERE p.name = 'x' OR p.id = 1 OR s.id = 2", 17),
        # 177 satellites: p.id = 1 gives 177, s.id IN (2, 3) gives 9 * 2, overlap 2.
        ("SELECT COUNT(*) FROM $planets AS p CROSS JOIN testdata.satellites AS s WHERE p.name = 'x' OR p.id = 1 OR s.id IN (2, 3)", 193),
    ],
)
def test_same_named_columns_of_different_relations_stay_apart(sql, expected):
    assert _scalar(sql) == expected


def test_same_column_point_tests_still_fuse():
    """The rewrite itself must keep firing for one column of one relation."""
    predicate = "name = 'x' OR id = 1 OR id IN (2, 3)"
    assert _scalar(f"SELECT COUNT(*) FROM $planets WHERE {predicate}") == 3
    explain = opteryx.session().execute_to_morsels(f"EXPLAIN SELECT name FROM $planets WHERE {predicate}")
    plan = [morsel[i][1] for morsel in explain for i in range(len(morsel))]
    assert any("id IN [1, 2, 3]" in step for step in plan), plan


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
