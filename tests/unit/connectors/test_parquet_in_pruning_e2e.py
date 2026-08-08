# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""WP-10: end-to-end correctness of IN/NOT IN row-group pruning.

Row-group pruning runs in the native scan path. The danger is *over*-pruning —
dropping a row group that actually contains matches. The strongest guard is
equivalence: ``col IN (...)`` must return exactly the same rows as the logically
identical OR-chain (which does not push as an InList and so prunes differently).
A mismatch means pruning silently dropped rows.
"""

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

import pytest

import opteryx


def _count(sql):
    value = 0
    for morsel in opteryx.session().execute_to_morsels(sql):
        if morsel.num_rows:
            value = morsel.column(b"c").to_pylist()[0]
    return value


# (label, IN form, equivalent OR form)
_EQUIVALENCE_CASES = [
    (
        "in_subset",
        "SELECT COUNT(*) AS c FROM testdata.tpch_001.nation WHERE n_regionkey IN (1, 2, 3)",
        "SELECT COUNT(*) AS c FROM testdata.tpch_001.nation "
        "WHERE n_regionkey = 1 OR n_regionkey = 2 OR n_regionkey = 3",
    ),
    (
        "in_endpoints",
        "SELECT COUNT(*) AS c FROM testdata.tpch_001.nation WHERE n_regionkey IN (0, 4)",
        "SELECT COUNT(*) AS c FROM testdata.tpch_001.nation "
        "WHERE n_regionkey = 0 OR n_regionkey = 4",
    ),
    (
        "in_none_match",
        "SELECT COUNT(*) AS c FROM testdata.tpch_001.nation WHERE n_regionkey IN (99, 100)",
        "SELECT COUNT(*) AS c FROM testdata.tpch_001.nation "
        "WHERE n_regionkey = 99 OR n_regionkey = 100",
    ),
    (
        "in_orders_custkey",
        "SELECT COUNT(*) AS c FROM testdata.tpch_001.orders WHERE o_custkey IN (1, 2, 3, 4, 5)",
        "SELECT COUNT(*) AS c FROM testdata.tpch_001.orders "
        "WHERE o_custkey = 1 OR o_custkey = 2 OR o_custkey = 3 OR o_custkey = 4 OR o_custkey = 5",
    ),
    (
        "not_in",
        "SELECT COUNT(*) AS c FROM testdata.tpch_001.lineitem WHERE l_linenumber NOT IN (1, 2)",
        "SELECT COUNT(*) AS c FROM testdata.tpch_001.lineitem "
        "WHERE NOT (l_linenumber = 1 OR l_linenumber = 2)",
    ),
]


@pytest.mark.parametrize("label, in_sql, or_sql", _EQUIVALENCE_CASES)
def test_in_pruning_matches_or_equivalent(label, in_sql, or_sql):
    assert _count(in_sql) == _count(or_sql), label


def test_in_out_of_range_returns_zero():
    # All list values below the column's range -> every row group prunes -> 0 rows.
    assert (
        _count(
            "SELECT COUNT(*) AS c FROM testdata.tpch_001.lineitem WHERE l_orderkey IN (-5, -4, -3)"
        )
        == 0
    )


def test_in_in_range_returns_matches():
    # Sanity: in-range values still return their rows (pruning must NOT drop them).
    assert (
        _count(
            "SELECT COUNT(*) AS c FROM testdata.tpch_001.lineitem WHERE l_orderkey IN (1, 2, 3)"
        )
        > 0
    )


if __name__ == "__main__":  # pragma: no cover
    pytest.main([__file__, "-v"])
