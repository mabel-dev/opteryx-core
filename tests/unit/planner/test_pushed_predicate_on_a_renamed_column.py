"""
A predicate on a RENAMED column, pushed into the scan, must read the real column.

`SELECT COUNT(*) FROM (SELECT id AS k FROM t) AS p WHERE k IS NULL` answered 9 on
the 9-row, NULL-free testdata.planets. The pushed predicate carried the alias row
(the same identity as `id`, named `k`), the readers resolve a pushed predicate's
columns against the file by name, and `k` is not in the file — so parquet read it
as the absent-column all-NULL constant (`k IS NULL` matched every row, `k = 3`
none) and skene refused with "requested column 'k' is not in this file". Only a
predicate-only column broke: a projected one already had the scan's own column in
the read set. Found by the subquery fuzzer's NOT IN oracle, which counts NULL keys
exactly this way.
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
    "relation,column",
    [
        ("testdata.planets", "id"),  # parquet
        ("testdata.fuzzing.wide", "grp_wide"),  # parquet, several row groups
        ("testdata.tpch_100_skene.nation", "n_nationkey"),  # skene
    ],
)
@pytest.mark.parametrize("predicate", ["{c} IS NULL", "{c} IS NOT NULL", "{c} = 3", "{c} > 3"])
def test_renamed_predicate_column_matches_the_original(relation, column, predicate):
    renamed = _scalar(
        f"SELECT COUNT(*) FROM (SELECT {column} AS k FROM {relation}) AS p "
        f"WHERE {predicate.format(c='k')}"
    )
    original = _scalar(f"SELECT COUNT(*) FROM {relation} WHERE {predicate.format(c=column)}")
    assert renamed == original


def test_renamed_predicate_column_beside_a_projected_column():
    """Predicate-only beside a projection: the projected column is in the read set,
    the renamed predicate column is not."""
    sql = (
        "SELECT MAX(m) FROM (SELECT id AS k, number_of_moons AS m FROM testdata.planets) AS p "
        "WHERE k < 5"
    )
    assert _scalar(sql) == _scalar("SELECT MAX(number_of_moons) FROM testdata.planets WHERE id < 5")


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
