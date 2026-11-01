"""
An aggregate window keeps the rows whose PARTITION BY key is NULL.

window_to_join rewrites `agg(x) OVER (PARTITION BY c)` into a join of every row
back to `SELECT c, agg(x) ... GROUP BY c`. The GROUP BY gives the NULL keys one
group, but the join back was a plain equi-join, and `NULL = NULL` matches nothing:
every NULL-keyed row silently vanished. `COUNT(*) OVER (PARTITION BY s_null)` on
testdata.fuzzing.mixed returned 1,589 of its 2,000 rows; over an all-NULL key it
returned none. The join is now "inner not-distinct" (IS NOT DISTINCT FROM).
Found while shrinking a single-table fuzzer seed.
"""

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

import opteryx


def _rows(sql):
    return [morsel[i] for morsel in opteryx.session().execute_to_morsels(sql) for i in range(len(morsel))]


def test_null_partition_rows_are_kept_and_counted_together():
    result = _rows("SELECT s_null, COUNT(*) OVER (PARTITION BY s_null) AS w FROM testdata.fuzzing.mixed")
    assert len(result) == 2000
    null_counts = {w for key, w in result if key is None}
    assert null_counts == {411}, null_counts


def test_all_null_partition_key_is_one_partition():
    result = _rows(
        "SELECT id, COUNT(*) OVER (PARTITION BY x) AS w "
        "FROM (SELECT id, CASE WHEN id > 100 THEN 1 END AS x FROM testdata.planets) AS c"
    )
    assert sorted(result) == [(i, 9) for i in range(1, 10)]


def test_null_and_zero_keys_are_different_partitions():
    """NULL equals NULL here, but must not equal 0 — the value a NULL slot holds."""
    result = _rows(
        "SELECT id, COUNT(*) OVER (PARTITION BY x) AS w "
        "FROM (SELECT id, CASE WHEN id > 5 THEN 0 END AS x FROM testdata.planets) AS c"
    )
    assert sorted(result) == [(i, 5 if i <= 5 else 4) for i in range(1, 10)]


def test_multi_column_partition_with_nulls():
    result = _rows(
        "SELECT s_null, b_value, COUNT(*) OVER (PARTITION BY s_null, b_value) AS w "
        "FROM testdata.fuzzing.mixed"
    )
    assert len(result) == 2000
    groups = {}
    for key, flag, _w in result:
        groups[(key, flag)] = groups.get((key, flag), 0) + 1
    assert all(w == groups[(key, flag)] for key, flag, w in result)


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
