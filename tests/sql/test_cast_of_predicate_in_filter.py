"""
CAST of an AND / BETWEEN / NOT predicate inside a WHERE was refused ("a CAST in a
filter predicate ... outside the c-native kernel set"), though the same cast ran in
a projection and over a plain comparison or an OR. The optimizer builds those
predicates anew (`NOT (x > 3)` -> `x <= 3`, BETWEEN -> AND) without a bound column,
so the cast's source had no type and missed the native BOOL cast. A predicate is
BOOL by construction. Found by the DuckDB reference fuzzer; answers checked there.
"""

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import pytest

import opteryx


def _ids(sql):
    return sorted(m[i][0] for m in opteryx.session().execute_to_morsels(sql) for i in range(len(m)))


@pytest.mark.parametrize("source", ["$planets", "testdata.planets"])
@pytest.mark.parametrize(
    "condition,expected",
    [
        ("CAST(NOT (id > 3) AS INT64) < 5", list(range(1, 10))),
        ("CAST(NOT (id > 3) AS INT64) = 1", [1, 2, 3]),
        ("CAST((id > 3 AND id < 6) AS INT64) = 1", [4, 5]),
        ("CAST(id BETWEEN 3 AND 6 AS INT64) = 1", [3, 4, 5, 6]),
        ("CAST((id > 3 OR id < 2) AS INT64) = 1", [1, 4, 5, 6, 7, 8, 9]),
        ("CAST((id > 3 AND id < 6) AS FLOAT64) > 0.5", [4, 5]),
        ("CAST((name = 'Earth' AND id < 6) AS VARCHAR) = 'true'", [3]),
        ("CAST(NOT (id > 3) AS INT64) + CAST((id < 7 AND id > 1) AS INT64) = 2", [2, 3]),
        ("CAST(NOT (NULLIF(id, 2) > 3) AS INT64) = 1", [1, 3]),
    ],
)
def test_cast_of_predicate_in_where(source, condition, expected):
    assert _ids(f"SELECT id FROM {source} WHERE {condition}") == expected


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
